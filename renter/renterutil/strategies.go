package renterutil

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"lukechampine.com/frand"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/merkle"
	"lukechampine.com/us/renter"
	"lukechampine.com/us/renterhost"
)

// A ChunkUploader uploads shards, associating them with a given chunk.
type ChunkUploader interface {
	UploadChunk(db MetaDB, b DBBlob, c DBChunk, shards [][]byte) error
}

// SerialChunkUploader uploads chunks to hosts one shard at a time.
type SerialChunkUploader struct {
	Hosts *HostSet
}

// UploadChunk implements ChunkUploader.
func (scu SerialChunkUploader) UploadChunk(db MetaDB, b DBBlob, c DBChunk, shards [][]byte) error {
	// choose hosts, preserving any that at already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range scu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	skip := make([]bool, len(shards))
	for i, sid := range c.Shards {
		if sid != 0 {
			s, err := db.Shard(sid)
			if err != nil {
				return err
			}
			if scu.Hosts.HasHost(s.HostKey) {
				skip[i] = true
				delete(newHosts, s.HostKey)
			}
		}
	}
	chooseHost := func() (h hostdb.HostPublicKey) {
		for h = range newHosts {
			delete(newHosts, h)
			break
		}
		return
	}

	for i, shard := range shards {
		if skip[i] {
			continue
		}
		hostKey := chooseHost()

		var sb renter.SectorBuilder // TODO: reuse
		offset := uint32(sb.Len())
		sb.Append(shard, b.DeriveKey(c.ID), b.DeriveNonce(c.ID, i))
		sector := sb.Finish()
		h, err := scu.Hosts.acquire(hostKey)
		if err != nil {
			return &HostError{hostKey, err}
		}
		root, err := h.Append(sector)
		scu.Hosts.release(hostKey)
		if err != nil {
			return &HostError{hostKey, err}
		}

		c.Shards[i], err = db.AddShard(DBShard{hostKey, root, offset})
		if err != nil {
			return err
		} else if _, err := db.AddChunk(c); err != nil {
			return err
		}
	}
	return nil
}

// ParallelChunkUploader uploads the shards of a chunk in parallel.
type ParallelChunkUploader struct {
	Hosts *HostSet
}

// UploadChunk implements ChunkUploader.
func (pcu ParallelChunkUploader) UploadChunk(db MetaDB, b DBBlob, c DBChunk, shards [][]byte) error {
	if len(shards) > len(pcu.Hosts.sessions) {
		return errors.New("more shards than hosts")
	}
	// choose hosts, preserving any that at already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range pcu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	skip := make([]bool, len(shards))
	for i, sid := range c.Shards {
		if sid != 0 {
			s, err := db.Shard(sid)
			if err != nil {
				return err
			}
			if pcu.Hosts.HasHost(s.HostKey) {
				skip[i] = true
				delete(newHosts, s.HostKey)
			}
		}
	}
	chooseHost := func() (h hostdb.HostPublicKey) {
		for h = range newHosts {
			delete(newHosts, h)
			break
		}
		return
	}

	// spawn workers
	type req struct {
		shardIndex int
		hostKey    hostdb.HostPublicKey
		shard      *[renterhost.SectorSize]byte
		block      bool // wait to acquire
	}
	type resp struct {
		req     req
		sliceID uint64
		err     error
	}
	reqChan := make(chan req, len(c.Shards))
	respChan := make(chan resp, len(c.Shards))
	for range c.Shards {
		go func() {
			for req := range reqChan {
				sess, err := pcu.Hosts.tryAcquire(req.hostKey)
				if err == errHostAcquired && req.block {
					sess, err = pcu.Hosts.acquire(req.hostKey)
				}
				if err != nil {
					respChan <- resp{req, 0, err}
					continue
				}

				root, err := sess.Append(req.shard)
				pcu.Hosts.release(req.hostKey)
				if err != nil {
					respChan <- resp{req, 0, err}
					continue
				}

				// TODO: need to use sb.Len as offset if reusing sb, i.e. when buffering
				ssid, err := db.AddShard(DBShard{req.hostKey, root, 0})
				respChan <- resp{req, ssid, err}
			}
		}()
	}

	// construct sectors
	sectors := make([]*[renterhost.SectorSize]byte, len(c.Shards))
	for i, shard := range shards {
		if skip[i] {
			continue
		}
		key := b.DeriveKey(c.ID)
		nonce := b.DeriveNonce(c.ID, i)
		var sb renter.SectorBuilder
		sb.Append(shard, key, nonce)
		sectors[i] = sb.Finish()
	}

	// start by requesting uploads to len(c.Shards) hosts, non-blocking.
	need := len(c.Shards)
	for shardIndex := range c.Shards {
		if skip[shardIndex] {
			need--
			continue
		}
		reqChan <- req{
			shardIndex: shardIndex,
			hostKey:    chooseHost(),
			shard:      sectors[shardIndex],
			block:      false,
		}
	}

	// for those that return errors, add the next host to the queue, non-blocking.
	// for those that block, add the same host to the queue, blocking.
	// abort once there are not enough hosts remaining (even if they all succeeded)
	var reqQueue []req
	var errs HostErrorSet
	for need > 0 {
		resp := <-respChan
		if resp.err == nil {
			c.Shards[resp.req.shardIndex] = resp.sliceID
			if _, err := db.AddChunk(c); err != nil {
				return err // TODO: need to wait for outstanding workers
			}
			need--
		} else {
			if resp.err == errHostAcquired {
				// host could not be acquired without blocking; add it to the back
				// of the queue, but next time, block
				resp.req.block = true
				reqQueue = append(reqQueue, resp.req)
			} else {
				// downloading from this host failed; don't try it again
				errs = append(errs, &HostError{resp.req.hostKey, resp.err})
				// add a different host to the queue, if able
				if len(newHosts) > 0 {
					resp.req.hostKey = chooseHost()
					resp.req.block = false
					reqQueue = append(reqQueue, resp.req)
				}
			}
			// try the next host in the queue
			if len(reqQueue) > 0 {
				reqChan <- reqQueue[0]
				reqQueue = reqQueue[1:]
			}
		}
	}
	close(reqChan)
	if need > 0 {
		return fmt.Errorf("could not upload to enough hosts: %w", errs)
	}
	return nil
}

// MinimumChunkUploader uploads shards one at a time, stopping as soon as
// MinShards shards have been uploaded.
type MinimumChunkUploader struct {
	Hosts *HostSet
}

// UploadChunk implements ChunkUploader.
func (mcu MinimumChunkUploader) UploadChunk(db MetaDB, b DBBlob, c DBChunk, shards [][]byte) error {
	// choose hosts, preserving any that at already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range mcu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	rem := c.MinShards
	skip := make([]bool, len(shards))
	for i, sid := range c.Shards {
		if sid != 0 {
			s, err := db.Shard(sid)
			if err != nil {
				return err
			}
			if mcu.Hosts.HasHost(s.HostKey) {
				skip[i] = true
				delete(newHosts, s.HostKey)
			}
		}
	}
	if rem <= 0 {
		return nil // already have minimum
	}
	chooseHost := func() (h hostdb.HostPublicKey) {
		for h = range newHosts {
			delete(newHosts, h)
			break
		}
		return
	}
	for i, shard := range shards {
		if skip[i] {
			continue
		}
		hostKey := chooseHost()

		var sb renter.SectorBuilder // TODO: reuse
		nonce := b.DeriveNonce(c.ID, i)
		offset := uint32(sb.Len())
		sb.Append(shard, b.DeriveKey(c.ID), nonce)
		sector := sb.Finish()
		h, err := mcu.Hosts.acquire(hostKey)
		if err != nil {
			return &HostError{hostKey, err}
		}
		root, err := h.Append(sector)
		mcu.Hosts.release(hostKey)
		if err != nil {
			return &HostError{hostKey, err}
		}

		c.Shards[i], err = db.AddShard(DBShard{hostKey, root, offset})
		if err != nil {
			return err
		} else if _, err := db.AddChunk(c); err != nil {
			return err
		}

		if rem--; rem == 0 {
			break
		}
	}
	return nil
}

// A ChunkDownloader downloads the shards of a chunk.
type ChunkDownloader interface {
	DownloadChunk(db MetaDB, b DBBlob, c DBChunk, off, n int64) ([][]byte, error)
}

// SerialChunkDownloader downloads the shards of a chunk one at a time.
type SerialChunkDownloader struct {
	Hosts *HostSet
}

// DownloadChunk implements ChunkDownloader.
func (scd SerialChunkDownloader) DownloadChunk(db MetaDB, b DBBlob, c DBChunk, off, n int64) ([][]byte, error) {
	minChunkSize := merkle.SegmentSize * int64(c.MinShards)
	shards := make([][]byte, len(c.Shards))
	for i := range shards {
		shards[i] = make([]byte, 0, renterhost.SectorSize)
	}
	var errs HostErrorSet
	need := c.MinShards
	for i, ssid := range c.Shards {
		shard, err := db.Shard(ssid)
		if err != nil {
			return nil, err
		}

		start := (off / minChunkSize) * merkle.SegmentSize
		end := ((off + n) / minChunkSize) * merkle.SegmentSize
		if (off+n)%minChunkSize != 0 {
			end += merkle.SegmentSize
		}
		offset, length := start, end-start

		sess, err := scd.Hosts.acquire(shard.HostKey)
		if err != nil {
			errs = append(errs, &HostError{shard.HostKey, err})
			continue
		}

		buf := bytes.NewBuffer(shards[i])
		err = (&renter.ShardDownloader{
			Downloader: sess,
			Key:        b.DeriveKey(c.ID),
			Slices: []renter.SectorSlice{{
				MerkleRoot:   shard.SectorRoot,
				SegmentIndex: shard.Offset,
				NumSegments:  merkle.SegmentsPerSector - shard.Offset, // inconsequential
				Nonce:        b.DeriveNonce(c.ID, i),
			}},
		}).CopySection(buf, offset, length)
		scd.Hosts.release(shard.HostKey)
		if err != nil {
			errs = append(errs, &HostError{shard.HostKey, err})
			continue
		}
		shards[i] = buf.Bytes()
		if need--; need == 0 {
			break
		}
	}
	if need != 0 {
		return nil, errs
	}
	return shards, nil
}

// ParallelChunkDownloader downloads the shards of a chunk in parallel.
type ParallelChunkDownloader struct {
	Hosts *HostSet
}

// DownloadChunk implements ChunkDownloader.
func (pcd ParallelChunkDownloader) DownloadChunk(db MetaDB, b DBBlob, c DBChunk, off, n int64) ([][]byte, error) {
	minChunkSize := merkle.SegmentSize * int64(c.MinShards)
	start := (off / minChunkSize) * merkle.SegmentSize
	end := ((off + n) / minChunkSize) * merkle.SegmentSize
	if (off+n)%minChunkSize != 0 {
		end += merkle.SegmentSize
	}
	offset, length := start, end-start

	// download shards in parallel, stopping when we have any c.MinShards of
	// them
	shards := make([][]byte, len(c.Shards))
	for i := range shards {
		shards[i] = make([]byte, 0, length)
	}
	type req struct {
		shardIndex int
		block      bool // wait to acquire
	}
	type resp struct {
		shardIndex int
		err        *HostError
	}
	reqChan := make(chan req, c.MinShards)
	respChan := make(chan resp, c.MinShards)
	reqQueue := make([]req, len(c.Shards))
	// initialize queue in random order
	for i, shardIndex := range frand.Perm(len(reqQueue)) {
		reqQueue[i] = req{shardIndex, false}
	}
	for len(reqQueue) > len(c.Shards)-int(c.MinShards) {
		go func() {
			for req := range reqChan {
				shard, err := db.Shard(c.Shards[req.shardIndex])
				if err != nil {
					respChan <- resp{req.shardIndex, &HostError{shard.HostKey, err}}
					continue
				}

				sess, err := pcd.Hosts.tryAcquire(shard.HostKey)
				if err == errHostAcquired && req.block {
					sess, err = pcd.Hosts.acquire(shard.HostKey)
				}
				if err != nil {
					respChan <- resp{req.shardIndex, &HostError{shard.HostKey, err}}
					continue
				}
				buf := bytes.NewBuffer(shards[req.shardIndex])
				err = (&renter.ShardDownloader{
					Downloader: sess,
					Key:        b.DeriveKey(c.ID),
					Slices: []renter.SectorSlice{{
						MerkleRoot:   shard.SectorRoot,
						SegmentIndex: shard.Offset,
						NumSegments:  merkle.SegmentsPerSector - shard.Offset, // inconsequential
						Nonce:        b.DeriveNonce(c.ID, req.shardIndex),
					}},
				}).CopySection(buf, offset, length)
				pcd.Hosts.release(shard.HostKey)
				if err != nil {
					respChan <- resp{req.shardIndex, &HostError{shard.HostKey, err}}
					continue
				}
				shards[req.shardIndex] = buf.Bytes()
				respChan <- resp{req.shardIndex, nil}
			}
		}()
		reqChan <- reqQueue[0]
		reqQueue = reqQueue[1:]
	}

	var goodShards int
	var errs HostErrorSet
	for goodShards < int(c.MinShards) && goodShards+len(errs) < len(c.Shards) {
		resp := <-respChan
		if resp.err == nil {
			goodShards++
		} else {
			if resp.err.Err == errHostAcquired {
				// host could not be acquired without blocking; add it to the back
				// of the queue, but next time, block
				reqQueue = append(reqQueue, req{
					shardIndex: resp.shardIndex,
					block:      true,
				})
			} else {
				// downloading from this host failed; don't try it again
				errs = append(errs, resp.err)
			}
			// try the next host in the queue
			if len(reqQueue) > 0 {
				reqChan <- reqQueue[0]
				reqQueue = reqQueue[1:]
			}
		}
	}
	close(reqChan)
	if goodShards < int(c.MinShards) {
		return nil, fmt.Errorf("too many hosts did not supply their shard (needed %v, got %v): %w", c.MinShards, goodShards, errs)
	}
	return shards, nil
}

// A ChunkUpdater updates or replaces an existing chunk, returning the ID of the
// new chunk.
type ChunkUpdater interface {
	UpdateChunk(db MetaDB, b DBBlob, c DBChunk) (uint64, error)
}

// GenericChunkUpdater updates chunks by downloading them with D and reuploading
// them with U, using erasure-coding parameters M and N.
type GenericChunkUpdater struct {
	D    ChunkDownloader
	U    ChunkUploader
	M, N int
}

// UpdateChunk implements ChunkUpdater.
func (gcu GenericChunkUpdater) UpdateChunk(db MetaDB, b DBBlob, c DBChunk) (uint64, error) {
	// download
	shards, err := gcu.D.DownloadChunk(db, b, c, 0, int64(c.Len))
	if err != nil {
		return 0, err
	}

	// reshard
	var buf bytes.Buffer
	rsc := renter.NewRSCode(int(c.MinShards), len(shards))
	if err := rsc.Recover(&buf, shards, 0, int(c.Len)); err != nil {
		return 0, err
	}
	shards = make([][]byte, gcu.N)
	for i := range shards {
		shards[i] = make([]byte, renterhost.SectorSize)
	}
	rsc = renter.NewRSCode(gcu.M, gcu.N)
	rsc.Encode(buf.Bytes(), shards)

	// upload
	rc := DBChunk{
		ID:        0, // create new chunk
		Shards:    make([]uint64, len(shards)),
		MinShards: uint8(gcu.M),
		Len:       c.Len,
	}
	rid, err := db.AddChunk(rc)
	if err != nil {
		return 0, err
	}
	rc.ID = rid
	if err := gcu.U.UploadChunk(db, b, rc, shards); err != nil {
		return 0, err
	}

	return rid, nil
}

// A BlobUploader uploads a DBBlob.
type BlobUploader interface {
	UploadBlob(db MetaDB, b DBBlob, r io.Reader) error
}

// SerialBlobUploader uploads the chunks of a blob one at a time.
type SerialBlobUploader struct {
	U    ChunkUploader
	M, N int
}

// UploadBlob implements BlobUploader.
func (sbu SerialBlobUploader) UploadBlob(db MetaDB, b DBBlob, r io.Reader) error {
	rsc := renter.NewRSCode(sbu.M, sbu.N)
	shards := make([][]byte, sbu.N)
	for i := range shards {
		shards[i] = make([]byte, renterhost.SectorSize)
	}
	buf := make([]byte, renterhost.SectorSize*sbu.M)
	for {
		chunkLen, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		rsc.Encode(buf[:chunkLen], shards)
		c := DBChunk{
			ID:        0, // create new chunk
			Shards:    make([]uint64, sbu.N),
			MinShards: uint8(sbu.M),
			Len:       uint64(chunkLen),
		}
		cid, err := db.AddChunk(c)
		if err != nil {
			return err
		}
		c.ID = cid
		b.Chunks = append(b.Chunks, c.ID)
		if err := db.AddBlob(b); err != nil {
			return err
		}
		if err := sbu.U.UploadChunk(db, b, c, shards); err != nil {
			return err
		}
	}
	return nil
}

// ParallelBlobUploader uploads the chunks of a blob in parallel.
type ParallelBlobUploader struct {
	U    ChunkUploader
	M, N int
	P    int // degree of parallelism
}

// UploadBlob implements BlobUploader.
func (pbu ParallelBlobUploader) UploadBlob(db MetaDB, b DBBlob, r io.Reader) error {
	// spawn p workers
	type req struct {
		c      DBChunk
		shards [][]byte
	}
	reqChan := make(chan req)
	respChan := make(chan error)
	defer close(reqChan)
	for i := 0; i < pbu.P; i++ {
		go func() {
			for req := range reqChan {
				respChan <- pbu.U.UploadChunk(db, b, req.c, req.shards)
			}
		}()
	}

	var inflight int
	consumeResp := func() error {
		err := <-respChan
		inflight--
		return err
	}
	defer func() {
		for inflight > 0 {
			_ = consumeResp()
		}
	}()

	// read+encode chunks, add to db, send requests to workers
	rsc := renter.NewRSCode(pbu.M, pbu.N)
	buf := make([]byte, renterhost.SectorSize*pbu.M)
	for {
		chunkLen, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}

		shards := make([][]byte, pbu.N)
		for i := range shards {
			shards[i] = make([]byte, renterhost.SectorSize)
		}
		rsc.Encode(buf[:chunkLen], shards)
		c := DBChunk{
			Shards:    make([]uint64, pbu.N),
			MinShards: uint8(pbu.M),
			Len:       uint64(chunkLen),
		}
		cid, err := db.AddChunk(c)
		if err != nil {
			return err
		}
		c.ID = cid
		b.Chunks = append(b.Chunks, c.ID)
		if err := db.AddBlob(b); err != nil {
			return err
		}
		reqChan <- req{c, shards}
		inflight++
		if inflight == pbu.P {
			if err := consumeResp(); err != nil {
				return err
			}
		}
	}
	// all requests have been sent; wait for inflight uploads to complete
	for inflight > 0 {
		if err := consumeResp(); err != nil {
			return err
		}
	}
	return nil
}

// A BlobDownloader downloads blob data, writing it to w.
type BlobDownloader interface {
	DownloadBlob(db MetaDB, b DBBlob, w io.Writer, off, n int64) error
}

// SerialBlobDownloader downloads the chunks of a blob one at a time.
type SerialBlobDownloader struct {
	D ChunkDownloader
}

// DownloadBlob implements BlobDownloader.
func (sbd SerialBlobDownloader) DownloadBlob(db MetaDB, b DBBlob, w io.Writer, off, n int64) error {
	for _, cid := range b.Chunks {
		c, err := db.Chunk(cid)
		if err != nil {
			return err
		}
		if off >= int64(c.Len) {
			off -= int64(c.Len)
			continue
		}

		reqLen := n
		if reqLen < 0 || reqLen > int64(c.Len) {
			reqLen = int64(c.Len)
		}
		shards, err := sbd.D.DownloadChunk(db, b, c, off, reqLen)
		if err != nil {
			return err
		}

		rsc := renter.NewRSCode(int(c.MinShards), len(c.Shards))
		skip := int(off % (merkle.SegmentSize * int64(c.MinShards)))
		if err := rsc.Recover(w, shards, skip, int(reqLen)); err != nil {
			return err
		}
		off = 0
		n -= reqLen
		if n == 0 {
			break
		}
	}
	return nil
}

// ParallelBlobDownloader downloads the chunks of a blob in parallel.
type ParallelBlobDownloader struct {
	D ChunkDownloader
	P int // degree of parallelism
}

// DownloadBlob implements BlobDownloader.
func (pbd ParallelBlobDownloader) DownloadBlob(db MetaDB, b DBBlob, w io.Writer, off, n int64) error {
	// spawn workers
	type req struct {
		c      DBChunk
		off, n int64
		index  int
	}
	type resp struct {
		index int
		chunk []byte
		err   error
	}
	reqChan := make(chan req)
	respChan := make(chan resp)
	defer close(reqChan)
	for i := 0; i < pbd.P; i++ {
		go func() {
			for req := range reqChan {
				shards, err := pbd.D.DownloadChunk(db, b, req.c, req.off, req.n)
				if err != nil {
					respChan <- resp{req.index, nil, err}
					continue
				}
				rsc := renter.NewRSCode(int(req.c.MinShards), len(req.c.Shards))
				skip := int(req.off % (merkle.SegmentSize * int64(req.c.MinShards)))
				var buf bytes.Buffer
				if err := rsc.Recover(&buf, shards, skip, int(req.n)); err != nil {
					respChan <- resp{req.index, nil, err}
					continue
				}
				respChan <- resp{req.index, buf.Bytes(), err}
			}
		}()
	}

	// when a response arrives, write the chunk into a circular buffer, then
	// flush as many chunks as possible
	chunks := make([][]byte, pbd.P)
	pos := 0
	inflight := 0
	consumeResp := func() error {
		resp := <-respChan
		inflight--
		if resp.err != nil {
			return resp.err
		}
		// write + flush
		if chunks[resp.index%pbd.P] != nil {
			panic("refusing to overwrite chunk")
		}
		chunks[resp.index%pbd.P] = resp.chunk
		for i := pos % pbd.P; chunks[i] != nil; i = pos % pbd.P {
			if _, err := w.Write(chunks[i]); err != nil {
				return err
			}
			chunks[i] = nil
			pos++
		}
		return nil
	}

	// if we return early (due to an error), ensure that we consume all
	// outstanding requests
	defer func() {
		for inflight > 0 {
			_ = consumeResp()
		}
	}()

	// request each chunk
	for chunkIndex, cid := range b.Chunks {
		c, err := db.Chunk(cid)
		if err != nil {
			return err
		}
		if off >= int64(c.Len) {
			off -= int64(c.Len)
			continue
		}
		reqLen := n
		if reqLen < 0 || reqLen > int64(c.Len) {
			reqLen = int64(c.Len)
		}
		reqChan <- req{c, off, reqLen, chunkIndex}
		inflight++

		// clear offset (as it only applies to the first chunk) and break early
		// if all necessary chunks have been requested
		off = 0
		n -= reqLen
		if n == 0 {
			break
		}

		// if all workers are busy, wait for one to finish before proceeding
		if inflight == pbd.P {
			if err := consumeResp(); err != nil {
				return err
			}
		}
	}

	// all requests have been sent; wait for inflight downloads to complete
	for inflight > 0 {
		if err := consumeResp(); err != nil {
			return err
		}
	}

	return nil
}

// A BlobUpdater updates the contents of a blob.
type BlobUpdater interface {
	UpdateBlob(db MetaDB, b DBBlob) error
}

// SerialBlobUpdater uploads the chunks of a blob one at a time.
type SerialBlobUpdater struct {
	U ChunkUpdater
}

// UpdateBlob implements BlobUpdater.
func (sbu SerialBlobUpdater) UpdateBlob(db MetaDB, b DBBlob) error {
	for i, cid := range b.Chunks {
		c, err := db.Chunk(cid)
		if err != nil {
			return err
		}
		id, err := sbu.U.UpdateChunk(db, b, c)
		if err != nil {
			return err
		}
		if cid != id {
			b.Chunks[i] = id
			if err := db.AddBlob(b); err != nil {
				return err
			}
		}
	}
	return nil
}

// A SectorDeleter deletes sectors from hosts.
type SectorDeleter interface {
	DeleteSectors(db MetaDB, sectors map[hostdb.HostPublicKey][]crypto.Hash) error
}

// SerialSectorDeleter deletes sectors from hosts, one host at a time.
type SerialSectorDeleter struct {
	Hosts *HostSet
}

// DeleteSectors implements SectorDeleter.
func (ssd SerialSectorDeleter) DeleteSectors(db MetaDB, sectors map[hostdb.HostPublicKey][]crypto.Hash) error {
	for hostKey, roots := range sectors {
		h, err := ssd.Hosts.acquire(hostKey)
		if err != nil {
			return err
		}
		err = h.DeleteSectors(roots) // TODO: no-op if roots already deleted
		ssd.Hosts.release(hostKey)
		if err != nil {
			return err
		}
		// TODO: mark sectors as deleted in db
	}
	return nil
}
