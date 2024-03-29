package renterutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules/host/contractmanager"
	"go.uber.org/multierr"
	"lukechampine.com/frand"

	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/merkle"
	"lukechampine.com/us/renter"
	"lukechampine.com/us/renter/proto"
	"lukechampine.com/us/renterhost"
)

var errMigrationSkipped = errors.New("migration is skipped")

func acquireCtx(ctx context.Context, hosts *HostSet, hostKey hostdb.HostPublicKey, block bool) (*proto.Session, error) {
	// NOTE: we can't smear ctx throughout the HostSet without changing a LOT of
	// code, so this "leaky" approach will have to do for now. (It's "leaky"
	// because the goroutine can stick around long after the ctx is canceled.)

	if ctx.Err() != nil {
		return nil, &HostError{HostKey: hostKey, Err: ctx.Err()}
	}
	var sess *proto.Session
	var err error
	done := make(chan struct{})
	go func() {
		sess, err = hosts.tryAcquire(hostKey)
		if err == ErrHostAcquired && block {
			sess, err = hosts.acquire(ctx, hostKey)
		}
		select {
		case done <- struct{}{}:
		case <-ctx.Done():
			if err == nil {
				hosts.release(hostKey)
			}
		}
	}()
	select {
	case <-ctx.Done():
		return nil, &HostError{HostKey: hostKey, Err: ctx.Err()}
	case <-done:
		if err != nil {
			return nil, &HostError{HostKey: hostKey, Err: err}
		}
		return sess, nil
	}
}

func uploadCtx(ctx context.Context, sess *proto.Session, shard *[renterhost.SectorSize]byte) (crypto.Hash, error) {
	if ctx.Err() != nil {
		return crypto.Hash{}, &HostError{HostKey: sess.HostKey(), Err: ctx.Err()}
	}
	var (
		root crypto.Hash
		err  error
	)
	done := make(chan struct{})
	go func() {
		root, err = sess.Append(shard)
		close(done)
	}()
	select {
	case <-ctx.Done():
		sess.Interrupt()
		<-done // wait for goroutine to exit
		err = ctx.Err()
	case <-done:
	}
	if err != nil {
		return crypto.Hash{}, &HostError{HostKey: sess.HostKey(), Err: err}
	}
	return root, nil
}

func downloadCtx(ctx context.Context, sess *proto.Session, key renter.KeySeed, shard DBShard, offset, length int64) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, &HostError{HostKey: sess.HostKey(), Err: ctx.Err()}
	}
	var (
		section []byte
		err     error
	)
	done := make(chan struct{})
	go func() {
		var buf bytes.Buffer
		err = (&renter.ShardDownloader{
			Downloader: sess,
			Key:        key,
			Slices: []renter.SectorSlice{{
				MerkleRoot:   shard.SectorRoot,
				SegmentIndex: shard.Offset,
				NumSegments:  merkle.SegmentsPerSector - shard.Offset, // inconsequential
				Nonce:        shard.Nonce,
			}},
		}).CopySection(&buf, offset, length)
		section = buf.Bytes()
		close(done)
	}()
	select {
	case <-ctx.Done():
		sess.Interrupt()
		<-done // wait for goroutine to exit
		err = ctx.Err()
	case <-done:
	}
	if err != nil {
		return nil, &HostError{HostKey: sess.HostKey(), Err: err}
	}
	return section, nil
}

func deleteCtx(ctx context.Context, sess *proto.Session, roots []crypto.Hash) error {
	if ctx.Err() != nil {
		return &HostError{HostKey: sess.HostKey(), Err: ctx.Err()}
	}
	done := make(chan error)
	go func() {
		done <- sess.DeleteSectors(roots)
	}()
	select {
	case <-ctx.Done():
		sess.Interrupt()
		<-done // wait for goroutine to exit
		return &HostError{HostKey: sess.HostKey(), Err: ctx.Err()}
	case err := <-done:
		if err != nil {
			return &HostError{HostKey: sess.HostKey(), Err: err}
		}
		return nil
	}
}

// A ChunkUploader uploads shards, associating them with a given chunk.
type ChunkUploader interface {
	UploadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, shards [][]byte) error
}

// SerialChunkUploader uploads chunks to hosts one shard at a time.
type SerialChunkUploader struct {
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// UploadChunk implements ChunkUploader.
func (scu SerialChunkUploader) UploadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, shards [][]byte) error {
	// choose hosts, preserving any that are already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range scu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	need := len(shards)
	skip := make([]bool, len(shards))
	for i, sid := range c.Shards {
		if sid != 0 {
			s, err := db.Shard(sid)
			if err != nil {
				return err
			}
			if scu.Hosts.HasHost(s.HostKey) {
				skip[i] = true
				need--
				delete(newHosts, s.HostKey)
			}
		}
	}
	if need > len(newHosts) {
		return errors.New("fewer hosts than shards")
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
		nonce := renter.RandomNonce()
		sb.Append(shard, key, nonce)
		sector := sb.Finish()

		var h *proto.Session
		err := scu.Executor.Execute(ctx, func(ctx context.Context) (err error) {
			h, err = acquireCtx(ctx, scu.Hosts, hostKey, true)
			return err
		})
		if err != nil {
			return err
		}
		h.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, scu.Recorder))

		var root crypto.Hash
		err = scu.Executor.Execute(ctx, func(ctx context.Context) error {
			root, err = uploadCtx(ctx, h, sector)
			return err
		})
		scu.Hosts.release(hostKey)
		if err != nil {
			return err
		}

		sid, err := db.AddShard(DBShard{hostKey, root, offset, nonce})
		if err != nil {
			return &HostError{HostKey: hostKey, Err: err}
		}
		if err := db.SetChunkShard(c.ID, i, sid); err != nil {
			return &HostError{HostKey: hostKey, Err: err}
		}
	}
	return nil
}

// ParallelChunkUploader uploads the shards of a chunk in parallel.
type ParallelChunkUploader struct {
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// UploadChunk implements ChunkUploader.
func (pcu ParallelChunkUploader) UploadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, shards [][]byte) error {
	if len(shards) > len(pcu.Hosts.sessions) {
		return errors.New("more shards than hosts")
	}
	// choose hosts, preserving any that are already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range pcu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	rem := len(shards)
	skip := make([]bool, len(shards))
	for i, sid := range c.Shards {
		if sid != 0 {
			s, err := db.Shard(sid)
			if err != nil {
				return err
			}
			if pcu.Hosts.HasHost(s.HostKey) {
				skip[i] = true
				rem--
				delete(newHosts, s.HostKey)
			}
		}
	}
	if rem > len(newHosts) {
		rem = len(newHosts)
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
		nonce      [24]byte
		block      bool // wait to acquire
	}
	type resp struct {
		req     req
		sliceID uint64
		err     error
	}
	reqChan := make(chan req, rem)
	respChan := make(chan resp, rem)
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < rem; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqChan {
				var sess *proto.Session
				err := pcu.Executor.Execute(ctx, func(ctx context.Context) (err error) {
					sess, err = acquireCtx(ctx, pcu.Hosts, req.hostKey, req.block)
					return err
				})
				if err != nil {
					respChan <- resp{req, 0, err}
					continue
				}
				sess.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, pcu.Recorder))

				var root crypto.Hash
				err = pcu.Executor.Execute(ctx, func(ctx context.Context) error {
					root, err = uploadCtx(ctx, sess, req.shard)
					return err
				})
				pcu.Hosts.release(req.hostKey)
				if err != nil {
					respChan <- resp{req, 0, err}
					continue
				}

				// TODO: need to use sb.Len as offset if reusing sb, i.e. when buffering
				ssid, err := db.AddShard(DBShard{req.hostKey, root, 0, req.nonce})
				if err != nil {
					respChan <- resp{req, ssid, &HostError{HostKey: req.hostKey, Err: err}}
					continue
				}
				respChan <- resp{req, ssid, nil}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		for req := range reqChan {
			respChan <- resp{req, 0, &HostError{HostKey: req.hostKey, Err: ctx.Err()}}
		}
	}()

	// construct sectors
	sectors := make([]*[renterhost.SectorSize]byte, len(c.Shards))
	nonces := make([][24]byte, len(sectors))
	for i, shard := range shards {
		if skip[i] {
			continue
		}
		nonces[i] = renter.RandomNonce()
		var sb renter.SectorBuilder
		sb.Append(shard, key, nonces[i])
		sectors[i] = sb.Finish()
	}

	// start by requesting uploads to rem hosts, non-blocking.
	var inflight int
	defer func() {
		for inflight > 0 {
			<-respChan
			inflight--
		}
		close(reqChan)
	}()

	for shardIndex := range c.Shards {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if skip[shardIndex] || len(newHosts) == 0 {
			continue
		}
		reqChan <- req{
			shardIndex: shardIndex,
			hostKey:    chooseHost(),
			shard:      sectors[shardIndex],
			nonce:      nonces[shardIndex],
			block:      false,
		}
		inflight++
	}

	// for those that return errors, add the next host to the queue, non-blocking.
	// for those that block, add the same host to the queue, blocking.
	var reqQueue []req
	var errs error
	for inflight > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		resp := <-respChan
		inflight--
		if resp.err == nil {
			if err := db.SetChunkShard(c.ID, resp.req.shardIndex, resp.sliceID); err != nil {
				// NOTE: in theory, we could attempt to continue storing the
				// remaining successful shards, but in practice, if
				// SetChunkShards fails, it indicates a serious problem with the
				// db, and subsequent calls to SetChunkShards are not likely to
				// succeed.
				for inflight > 0 {
					<-respChan
					inflight--
				}
				return err
			}
			rem--
		} else {
			if resp.err == ErrHostAcquired {
				// host could not be acquired without blocking; add it to the back
				// of the queue, but next time, block
				resp.req.block = true
				reqQueue = append(reqQueue, resp.req)
			} else {
				// uploading to this host failed; don't try it again
				errs = multierr.Append(errs, resp.err)
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
				inflight++
			}
		}
	}
	if rem > 0 {
		return fmt.Errorf("could not upload to enough hosts: %w", errs)
	}
	return nil
}

// A ChunkDownloader downloads the shards of a chunk.
type ChunkDownloader interface {
	DownloadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, off, n int64) ([][]byte, error)
}

// SerialChunkDownloader downloads the shards of a chunk one at a time.
type SerialChunkDownloader struct {
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// DownloadChunk implements ChunkDownloader.
func (scd SerialChunkDownloader) DownloadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, off, n int64) ([][]byte, error) {
	minChunkSize := merkle.SegmentSize * int64(c.MinShards)
	shards := make([][]byte, len(c.Shards))
	for i := range shards {
		shards[i] = make([]byte, 0, renterhost.SectorSize)
	}
	var errs error
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

		var sess *proto.Session
		err = scd.Executor.Execute(ctx, func(ctx context.Context) error {
			sess, err = acquireCtx(ctx, scd.Hosts, shard.HostKey, true)
			return err
		})
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		sess.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, scd.Recorder))

		var section []byte
		err = scd.Executor.Execute(ctx, func(ctx context.Context) error {
			section, err = downloadCtx(ctx, sess, key, shard, offset, length)
			return err
		})
		scd.Hosts.release(shard.HostKey)
		if err != nil {
			errs = multierr.Append(errs, err)
			continue
		}
		shards[i] = section
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
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// DownloadChunk implements ChunkDownloader.
func (pcd ParallelChunkDownloader) DownloadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, off, n int64) ([][]byte, error) {
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
		err        error
	}
	reqChan := make(chan req, c.MinShards)
	respChan := make(chan resp, c.MinShards)
	reqQueue := make([]req, len(c.Shards))
	// initialize queue in random order
	for i, shardIndex := range frand.Perm(len(reqQueue)) {
		reqQueue[i] = req{shardIndex, false}
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for len(reqQueue) > len(c.Shards)-int(c.MinShards) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqChan {
				shard, err := db.Shard(c.Shards[req.shardIndex])
				if err != nil {
					respChan <- resp{req.shardIndex, &HostError{shard.HostKey, err}}
					continue
				}

				var sess *proto.Session
				err = pcd.Executor.Execute(ctx, func(ctx context.Context) error {
					sess, err = acquireCtx(ctx, pcd.Hosts, shard.HostKey, req.block)
					return err
				})
				if err != nil {
					respChan <- resp{req.shardIndex, err}
					continue
				}
				sess.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, pcd.Recorder))

				var section []byte
				err = pcd.Executor.Execute(ctx, func(ctx context.Context) error {
					section, err = downloadCtx(ctx, sess, key, shard, offset, length)
					return err
				})
				pcd.Hosts.release(shard.HostKey)
				if err != nil {
					respChan <- resp{req.shardIndex, err}
					continue
				}
				shards[req.shardIndex] = section
				respChan <- resp{req.shardIndex, nil}
			}
		}()
		reqChan <- reqQueue[0]
		reqQueue = reqQueue[1:]
	}

	var goodShards int
	var errs error
	for goodShards < int(c.MinShards) && goodShards+len(multierr.Errors(errs)) < len(c.Shards) {
		resp := <-respChan
		if resp.err == nil {
			goodShards++
		} else {
			if errors.Is(resp.err, ErrHostAcquired) {
				// host could not be acquired without blocking; add it to the back
				// of the queue, but next time, block
				reqQueue = append(reqQueue, req{
					shardIndex: resp.shardIndex,
					block:      true,
				})
			} else {
				// downloading from this host failed; don't try it again
				errs = multierr.Append(errs, resp.err)
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

// OverdriveChunkDownloader downloads the shards of a chunk in parallel.
type OverdriveChunkDownloader struct {
	Hosts     *HostSet
	Overdrive int
	Executor  RequestExecutor
	Recorder  RPCStatsRecorder
}

// DownloadChunk implements ChunkDownloader.
func (ocd OverdriveChunkDownloader) DownloadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, off, n int64) ([][]byte, error) {
	if ocd.Overdrive < 0 {
		panic("overdrive cannot be negative")
	}
	numWorkers := int(c.MinShards) + ocd.Overdrive
	if numWorkers > len(c.Shards) {
		numWorkers = len(c.Shards)
	}

	minChunkSize := merkle.SegmentSize * int64(c.MinShards)
	start := (off / minChunkSize) * merkle.SegmentSize
	end := ((off + n) / minChunkSize) * merkle.SegmentSize
	if (off+n)%minChunkSize != 0 {
		end += merkle.SegmentSize
	}
	offset, length := start, end-start

	// download shards in parallel, stopping when we have any c.MinShards of
	// them
	type req struct {
		shardIndex int
		block      bool // wait to acquire
	}
	type resp struct {
		req   req
		shard []byte
		err   error
	}
	reqChan := make(chan req, numWorkers)
	respChan := make(chan resp, numWorkers)
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		close(reqChan)
		cancel()
		wg.Wait()
	}()
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for req := range reqChan {
				shard, err := db.Shard(c.Shards[req.shardIndex])
				if err != nil {
					respChan <- resp{req, nil, &HostError{shard.HostKey, err}}
					continue
				}

				var sess *proto.Session
				err = ocd.Executor.Execute(ctx, func(ctx context.Context) error {
					sess, err = acquireCtx(ctx, ocd.Hosts, shard.HostKey, req.block)
					return err
				})
				if err != nil {
					respChan <- resp{req, nil, err}
					continue
				}
				sess.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, ocd.Recorder))

				var section []byte
				err = ocd.Executor.Execute(ctx, func(ctx context.Context) error {
					section, err = downloadCtx(ctx, sess, key, shard, offset, length)
					return err
				})
				ocd.Hosts.release(shard.HostKey)
				if err != nil {
					respChan <- resp{req, nil, err}
					continue
				}
				respChan <- resp{req, section, nil}
			}
		}()
	}

	// initialize queue in random order
	reqQueue := make([]req, len(c.Shards))
	for i, shardIndex := range frand.Perm(len(reqQueue)) {
		reqQueue[i] = req{shardIndex, false}
	}
	// send initial requests
	for _, req := range reqQueue[:numWorkers] {
		reqChan <- req
	}
	reqQueue = reqQueue[numWorkers:]

	// await responses and replace failed requests as necessary
	shards := make([][]byte, len(c.Shards))
	for i := range shards {
		shards[i] = make([]byte, 0, length)
	}
	var goodShards int
	var errs error
	for goodShards < int(c.MinShards) && goodShards+len(multierr.Errors(errs)) < len(c.Shards) {
		resp := <-respChan
		if resp.err == nil {
			goodShards++
			shards[resp.req.shardIndex] = resp.shard
		} else {
			if errors.Is(resp.err, ErrHostAcquired) {
				// host could not be acquired without blocking; add it to the back
				// of the queue, but next time, block
				resp.req.block = true
				reqQueue = append(reqQueue, resp.req)
			} else {
				// downloading from this host failed; don't try it again
				errs = multierr.Append(errs, resp.err)
			}
			// try the next host in the queue
			if len(reqQueue) > 0 {
				reqChan <- reqQueue[0]
				reqQueue = reqQueue[1:]
			}
		}
	}
	if goodShards < int(c.MinShards) {
		return nil, fmt.Errorf("too many hosts did not supply their shard (needed %v, got %v): %w", c.MinShards, goodShards, errs)
	}
	return shards, nil
}

// A ChunkUpdater updates or replaces an existing chunk, returning the ID of the
// new chunk.
type ChunkUpdater interface {
	UpdateChunk(ctx context.Context, db MetaDB, b DBBlob, c DBChunk, shouldUpdate func(MetaDB, DBChunk) (bool, error)) (uint64, error)
}

// GenericChunkUpdater updates chunks by downloading them with D and reuploading
// them with U, using erasure-coding parameters M and N.
type GenericChunkUpdater struct {
	D    ChunkDownloader
	U    ChunkUploader
	M, N int

	// If true, the chunk's encoding parameters are used, and the chunk is
	// updated directly instead of a new chunk being added to the DB.
	InPlace bool
}

var _ ChunkUpdater = (*GenericChunkUpdater)(nil)

// UpdateChunk implements ChunkUpdater.
func (gcu GenericChunkUpdater) UpdateChunk(
	ctx context.Context, db MetaDB, b DBBlob, c DBChunk, shouldUpdate func(MetaDB, DBChunk) (bool, error),
) (uint64, error) {
	if shouldUpdate != nil {
		update, err := shouldUpdate(db, c)
		if err != nil {
			return 0, err
		} else if !update {
			return 0, errMigrationSkipped
		}
	}

	// download
	shards, err := gcu.D.DownloadChunk(ctx, db, c, b.Seed, 0, int64(c.Len))
	if err != nil {
		return 0, err
	}

	// reshard
	m, n := gcu.M, gcu.N
	if gcu.InPlace {
		m, n = int(c.MinShards), len(c.Shards)
	}
	if m == int(c.MinShards) && n == len(c.Shards) {
		if err := renter.NewRSCode(m, n).Reconstruct(shards); err != nil {
			return 0, err
		}
	} else {
		var buf bytes.Buffer
		if err := renter.NewRSCode(int(c.MinShards), len(c.Shards)).Recover(&buf, shards, 0, int(c.Len)); err != nil {
			return 0, err
		}
		shards = make([][]byte, n)
		for i := range shards {
			shards[i] = make([]byte, renterhost.SectorSize)
		}
		renter.NewRSCode(m, n).Encode(buf.Bytes(), shards)
	}

	// upload
	if !gcu.InPlace {
		c, err = db.AddChunk(m, n, c.Len)
		if err != nil {
			return 0, err
		}
	}
	if err := gcu.U.UploadChunk(ctx, db, c, b.Seed, shards); err != nil {
		return 0, err
	}
	return c.ID, nil
}

// NewMigrationWhitelist returns a filter for use with GenericChunkUpdater. It
// returns true for chunks that store all of their shards on whitelisted host.
func NewMigrationWhitelist(whitelist []hostdb.HostPublicKey) func(MetaDB, DBChunk) (bool, error) {
	return func(db MetaDB, c DBChunk) (bool, error) {
		for _, id := range c.Shards {
			if id == 0 {
				continue
			}
			s, err := db.Shard(id)
			if err != nil {
				return false, err
			}
			whitelisted := false
			for _, h := range whitelist {
				whitelisted = whitelisted || h == s.HostKey
			}
			if !whitelisted {
				return true, nil
			}
		}
		return false, nil
	}
}

// NewMigrationBlacklist returns a filter for use with GenericChunkUpdater. It
// returns true for chunks that store any of their shards on a blacklisted host.
func NewMigrationBlacklist(blacklist []hostdb.HostPublicKey) func(MetaDB, DBChunk) (bool, error) {
	return func(db MetaDB, c DBChunk) (bool, error) {
		for _, id := range c.Shards {
			if id == 0 {
				continue
			}
			s, err := db.Shard(id)
			if err != nil {
				return false, err
			}
			for _, h := range blacklist {
				if h == s.HostKey {
					return true, nil
				}
			}
		}
		return false, nil
	}
}

// A BlobUploader uploads a DBBlob.
type BlobUploader interface {
	UploadBlob(ctx context.Context, db MetaDB, b DBBlob, r io.Reader) error
}

// SerialBlobUploader uploads the chunks of a blob one at a time.
type SerialBlobUploader struct {
	U    ChunkUploader
	M, N int
}

var _ BlobUploader = (*SerialBlobUploader)(nil)

// UploadBlob implements BlobUploader.
func (sbu SerialBlobUploader) UploadBlob(ctx context.Context, db MetaDB, b DBBlob, r io.Reader) error {
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
		c, err := db.AddChunk(sbu.M, sbu.N, uint64(chunkLen))
		if err != nil {
			return err
		}
		b.Chunks = append(b.Chunks, c.ID)
		if err := db.AddBlob(b); err != nil {
			return err
		}
		if err := sbu.U.UploadChunk(ctx, db, c, b.Seed, shards); err != nil {
			return fmt.Errorf("failed to upload a chunk: %w", err)
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

var _ BlobUploader = (*ParallelBlobUploader)(nil)

// UploadBlob implements BlobUploader.
func (pbu ParallelBlobUploader) UploadBlob(ctx context.Context, db MetaDB, b DBBlob, r io.Reader) (err error) {
	// spawn p workers
	type req struct {
		c      DBChunk
		shards [][]byte
	}
	reqChan := make(chan req)
	respChan := make(chan error)
	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithCancel(ctx)

	for i := 0; i < pbu.P; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqChan {
				respChan <- pbu.U.UploadChunk(ctx, db, req.c, b.Seed, req.shards)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		for range reqChan {
			respChan <- ctx.Err()
		}
	}()

	var inflight int
	consumeResp := func() error {
		err := <-respChan
		inflight--
		return err
	}
	defer func() {
		for inflight > 0 {
			if e := consumeResp(); e != nil {
				err = multierr.Append(err, e)
			}
		}
		close(reqChan)
	}()

	defer cancel()

	// read+encode chunks, add to db, send requests to workers
	rsc := renter.NewRSCode(pbu.M, pbu.N)
	buf := make([]byte, renterhost.SectorSize*pbu.M)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

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
		c, err := db.AddChunk(pbu.M, pbu.N, uint64(chunkLen))
		if err != nil {
			return err
		}
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
	DownloadBlob(ctx context.Context, db MetaDB, b DBBlob, w io.Writer, off, n int64) error
}

// SerialBlobDownloader downloads the chunks of a blob one at a time.
type SerialBlobDownloader struct {
	D ChunkDownloader
}

// DownloadBlob implements BlobDownloader.
func (sbd SerialBlobDownloader) DownloadBlob(ctx context.Context, db MetaDB, b DBBlob, w io.Writer, off, n int64) error {
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
		shards, err := sbd.D.DownloadChunk(ctx, db, c, b.Seed, off, reqLen)
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
func (pbd ParallelBlobDownloader) DownloadBlob(ctx context.Context, db MetaDB, b DBBlob, w io.Writer, off, n int64) error {
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
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < pbd.P; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqChan {
				shards, err := pbd.D.DownloadChunk(ctx, db, req.c, b.Seed, req.off, req.n)
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
		close(reqChan)
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
	UpdateBlob(ctx context.Context, db MetaDB, b DBBlob, shouldUpdate func(MetaDB, DBChunk) (bool, error)) error
}

// SerialBlobUpdater uploads the chunks of a blob one at a time.
type SerialBlobUpdater struct {
	U ChunkUpdater
}

var _ BlobUpdater = (*SerialBlobUpdater)(nil)

// UpdateBlob implements BlobUpdater.
func (sbu SerialBlobUpdater) UpdateBlob(ctx context.Context, db MetaDB, b DBBlob, shouldUpdate func(MetaDB, DBChunk) (bool, error)) error {
	for i, cid := range b.Chunks {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c, err := db.Chunk(cid)
		if err != nil {
			return err
		}
		id, err := sbu.U.UpdateChunk(ctx, db, b, c, shouldUpdate)
		if err != nil {
			if errors.Is(err, errMigrationSkipped) {
				continue
			}
			return err
		}
		if cid != id {
			b.Chunks[i] = id
		}
		// Note: need to update the blob info regardless of whether the chunk ID is updated
		// so that the last modified timestamp is updated.
		if err := db.AddBlob(b); err != nil {
			return err
		}
	}
	return nil
}

// A SectorDeleter deletes sectors from hosts.
type SectorDeleter interface {
	DeleteSectors(ctx context.Context, db MetaDB, sectors map[hostdb.HostPublicKey][]crypto.Hash) error
}

// SerialSectorDeleter deletes sectors from hosts, one host at a time.
type SerialSectorDeleter struct {
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// DeleteSectors implements SectorDeleter.
func (ssd SerialSectorDeleter) DeleteSectors(ctx context.Context, db MetaDB, sectors map[hostdb.HostPublicKey][]crypto.Hash) error {
	for hostKey, roots := range sectors {
		var h *proto.Session
		err := ssd.Executor.Execute(ctx, func(ctx context.Context) (err error) {
			h, err = acquireCtx(ctx, ssd.Hosts, hostKey, true)
			return err
		})
		if err != nil {
			return err
		}
		h.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, ssd.Recorder))

		err = ssd.Executor.Execute(ctx, func(ctx context.Context) error {
			return deleteCtx(ctx, h, roots)
		})
		ssd.Hosts.release(hostKey)
		if err != nil && !errors.Is(err, contractmanager.ErrSectorNotFound) {
			return err
		}
		// TODO: mark sectors as deleted in db
	}
	return nil
}

// ParallelSectorDeleter deletes sectors from hosts in parallel.
type ParallelSectorDeleter struct {
	Hosts    *HostSet
	Executor RequestExecutor
	Recorder RPCStatsRecorder
}

// DeleteSectors implements SectorDeleter.
func (psd ParallelSectorDeleter) DeleteSectors(ctx context.Context, db MetaDB, sectors map[hostdb.HostPublicKey][]crypto.Hash) error {
	errCh := make(chan error)
	for hostKey, roots := range sectors {
		go func(hostKey hostdb.HostPublicKey, roots []crypto.Hash) {
			errCh <- func() error {
				var h *proto.Session
				err := psd.Executor.Execute(ctx, func(ctx context.Context) (err error) {
					h, err = acquireCtx(ctx, psd.Hosts, hostKey, true)
					return err
				})
				if err != nil {
					return err
				}
				h.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, psd.Recorder))

				err = psd.Executor.Execute(ctx, func(ctx context.Context) error {
					return deleteCtx(ctx, h, roots)
				})
				psd.Hosts.release(hostKey)
				if err != nil && !errors.Is(err, contractmanager.ErrSectorNotFound) {
					return err
				}
				// TODO: mark sectors as deleted in db
				return nil
			}()
		}(hostKey, roots)
	}
	var errs error
	for range sectors {
		if err := <-errCh; err != nil {
			errs = multierr.Append(errs, err)
		}
	}
	if errs != nil {
		return fmt.Errorf("could not delete from all hosts: %w", errs)
	}
	return nil
}
