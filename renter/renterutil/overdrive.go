package renterutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"go.uber.org/multierr"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter"
	"lukechampine.com/us/renter/proto"
	"lukechampine.com/us/renterhost"
)

// OverdriveBlobUploader uploads the chunks of a blob one at a time.
type OverdriveBlobUploader struct {
	M         int
	N         int
	Overdrive int
	Hosts     *HostSet
	Executor  RequestExecutor
	Recorder  RPCStatsRecorder
}

var _ BlobUploader = (*OverdriveBlobUploader)(nil)

// UploadBlob implements BlobUploader.
func (obu OverdriveBlobUploader) UploadBlob(ctx context.Context, db MetaDB, b DBBlob, r io.Reader) error {
	rsc := renter.NewRSCode(obu.M, obu.N+obu.Overdrive)
	shards := make([][]byte, obu.N+obu.Overdrive)
	for i := range shards {
		shards[i] = make([]byte, renterhost.SectorSize)
	}
	buf := make([]byte, renterhost.SectorSize*obu.M)
	for {
		chunkLen, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			return err
		}
		rsc.Encode(buf[:chunkLen], shards)
		c, err := db.AddChunk(obu.M, obu.N+obu.Overdrive, uint64(chunkLen))
		if err != nil {
			return err
		}
		b.Chunks = append(b.Chunks, c.ID)
		if err := db.AddBlob(b); err != nil {
			return err
		}
		if err := obu.UploadChunk(ctx, db, c, b.Seed, shards); err != nil {
			return fmt.Errorf("failed to upload a chunk: %w", err)
		}
	}
	return nil
}

func (obu OverdriveBlobUploader) UploadChunk(ctx context.Context, db MetaDB, c DBChunk, key renter.KeySeed, shards [][]byte) error {
	if len(shards) > len(obu.Hosts.sessions) {
		return errors.New("more shards than hosts")
	}
	// choose hosts, preserving any that are already present
	newHosts := make(map[hostdb.HostPublicKey]struct{})
	for h := range obu.Hosts.sessions {
		newHosts[h] = struct{}{}
	}
	rem := len(shards)
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

	var inflight int
	defer func() {
		for inflight > 0 {
			<-respChan
			inflight--
		}
		close(reqChan)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < rem; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for req := range reqChan {
				var sess *proto.Session
				err := obu.Executor.Execute(ctx, func(ctx context.Context) (err error) {
					sess, err = acquireCtx(ctx, obu.Hosts, req.hostKey, req.block)
					return err
				})
				if err != nil {
					respChan <- resp{req: req, err: err}
					continue
				}
				sess.SetRPCStatsRecorder(newRPCStatsRecorder(ctx, obu.Recorder))

				var root crypto.Hash
				err = obu.Executor.Execute(ctx, func(ctx context.Context) error {
					root, err = uploadCtx(ctx, sess, req.shard)
					return err
				})
				obu.Hosts.release(req.hostKey)
				if err != nil {
					respChan <- resp{req: req, err: err}
					continue
				}

				ssid, err := db.AddShard(DBShard{HostKey: req.hostKey, SectorRoot: root, Nonce: req.nonce})
				if err != nil {
					respChan <- resp{req: req, sliceID: ssid, err: &HostError{HostKey: req.hostKey, Err: err}}
					continue
				}

				respChan <- resp{req: req, sliceID: ssid}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		for req := range reqChan {
			respChan <- resp{req: req, err: &HostError{HostKey: req.hostKey, Err: ctx.Err()}}
		}
	}()

	// construct sectors
	sectors := make([]*[renterhost.SectorSize]byte, len(shards))
	nonces := make([][24]byte, len(sectors))
	for i, shard := range shards {
		nonces[i] = renter.RandomNonce()
		var sb renter.SectorBuilder
		sb.Append(shard, key, nonces[i])
		sectors[i] = sb.Finish()
	}

	// start by requesting uploads to rem hosts, non-blocking.
	for shardIndex := range shards {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if len(newHosts) == 0 {
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
	var storedShards int
	for inflight > 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		resp := <-respChan
		inflight--
		if resp.err == nil {
			if err := db.SetChunkShard(c.ID, resp.req.shardIndex, resp.sliceID); err != nil {
				return err
			}
			rem--

			storedShards++
			if storedShards == obu.N {
				return nil
			}
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
