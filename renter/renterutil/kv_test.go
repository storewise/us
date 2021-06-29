package renterutil

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"lukechampine.com/frand"
	"lukechampine.com/us/ghost"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renterhost"
)

func createTestingKV(tb testing.TB, numHosts, m, n int) PseudoKV {
	tb.Helper()
	hosts := make([]*ghost.Host, numHosts)
	hkr := make(testHKR)
	hs := NewHostSet(hkr, 0)
	var cleanups []func()
	for i := range hosts {
		h, c := createHostWithContract(tb)
		hosts[i] = h
		hkr[h.PublicKey] = h.Settings.NetAddress
		hs.AddHost(c)
		cleanups = append(cleanups, func() {
			if err := h.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				tb.Error(err)
			}
		})
	}

	dbName := filepath.Join(tb.TempDir(), "kv.db")
	db, err := NewBoltMetaDB(dbName)
	if err != nil {
		tb.Fatal(err)
	}
	uploader := ParallelChunkUploader{
		Hosts:    hs,
		Executor: DefaultExecutor,
	}
	downloader := ParallelChunkDownloader{
		Hosts:    hs,
		Executor: DefaultExecutor,
	}

	kv := PseudoKV{
		DB: db,
		Uploader: SerialBlobUploader{
			U: uploader,
			M: m,
			N: n,
		},
		Downloader: SerialBlobDownloader{
			D: downloader,
		},
		Deleter: ParallelSectorDeleter{
			Hosts:    hs,
			Executor: DefaultExecutor,
		},
		Updater: SerialBlobUpdater{
			U: GenericChunkUpdater{
				U: uploader,
				D: downloader,
				M: m,
				N: n,
			},
		},
	}
	tb.Cleanup(func() {
		if err := kv.Close(); err != nil {
			tb.Error(err)
		}
	})

	return kv
}

func TestKVPutGet(t *testing.T) {
	ctx := context.Background()
	kv := createTestingKV(t, 3, 2, 3)

	err := kv.PutBytes(ctx, []byte("foo"), []byte("bar"))
	if err != nil {
		t.Fatal(err)
	}
	data, err := kv.GetBytes(ctx, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "bar" {
		t.Fatalf("bad data: %q", data)
	}

	// large value, using streaming API
	bigdata := frand.Bytes(renterhost.SectorSize * 4)
	err = kv.Put(ctx, []byte("foo"), bytes.NewReader(bigdata))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	err = kv.Get(ctx, []byte("foo"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), bigdata) {
		t.Fatal("bad data")
	}

	// range request
	buf.Reset()
	off, n := int64(renterhost.SectorSize+10), int64(497)
	err = kv.GetRange(ctx, []byte("foo"), &buf, off, n)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), bigdata[off:][:n]) {
		t.Fatal("bad range data", len(buf.Bytes()), bytes.Index(bigdata, buf.Bytes()))
	}
}

func TestKVBufferHosts(t *testing.T) {
	ctx := context.Background()
	kv := createTestingKV(t, 6, 2, 3) // 3 buffer hosts

	bigdata := frand.Bytes(renterhost.SectorSize * 6)
	err := kv.Put(ctx, []byte("foo"), bytes.NewReader(bigdata))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	err = kv.Get(ctx, []byte("foo"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), bigdata) {
		t.Fatal("bad data")
	}

	// check that chunks are stored on different hosts
	var chunkHosts []string
	b, err := kv.DB.Blob([]byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	for _, cid := range b.Chunks {
		c, err := kv.DB.Chunk(cid)
		if err != nil {
			t.Fatal(err)
		}
		var hosts string
		for _, ssid := range c.Shards {
			s, err := kv.DB.Shard(ssid)
			if err != nil {
				t.Fatal(err)
			}
			hosts += s.HostKey.ShortKey()
		}
		chunkHosts = append(chunkHosts, hosts)
	}
	allEqual := true
	if len(chunkHosts) == 0 {
		t.Fatal("chunkHosts are empty")
	}
	for i := range chunkHosts[1:] {
		allEqual = allEqual && chunkHosts[i] == chunkHosts[i+1]
	}
	if allEqual {
		t.Fatal("all chunks stored on the same host set")
	}
}

func TestKVMigrate(t *testing.T) {
	ctx := context.Background()
	kv := createTestingKV(t, 3, 2, 3)

	bigdata := frand.Bytes(renterhost.SectorSize * 4)
	err := kv.PutBytes(ctx, []byte("foo"), bigdata)
	if err != nil {
		t.Fatal(err)
	}

	// replace a host in the set
	hs := kv.Deleter.(ParallelSectorDeleter).Hosts
	for hostKey := range hs.sessions {
		s, _ := hs.acquire(ctx, hostKey)
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		hs.release(hostKey)
		delete(hs.sessions, hostKey)
		break
	}
	h, c := createHostWithContract(t)
	defer func(h *ghost.Host) {
		err := h.Close()
		if err != nil {
			t.Error(err)
		}
	}(h)
	hs.hkr.(testHKR)[h.PublicKey] = h.Settings.NetAddress
	hs.AddHost(c)

	// migrate
	whitelist := make([]hostdb.HostPublicKey, 0, len(hs.sessions))
	for hostKey := range hs.sessions {
		whitelist = append(whitelist, hostKey)
	}
	err = kv.Migrate(ctx, []byte("foo"), whitelist)
	if err != nil {
		t.Fatal(err)
	}

	data, err := kv.GetBytes(ctx, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, bigdata) {
		t.Fatal("bad data", data, bigdata)
	}
}

func TestKVPutGetParallel(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}
	ctx := context.Background()
	kv := createTestingKV(t, 6, 2, 3)

	var kvs [5]struct {
		smallKey []byte
		smallVal []byte
		largeKey []byte
		largeVal []byte
	}
	for i := range kvs {
		kvs[i].smallKey = []byte("small" + strconv.Itoa(i))
		kvs[i].smallVal = []byte("value" + strconv.Itoa(i))
		kvs[i].largeKey = []byte("large" + strconv.Itoa(i))
		kvs[i].largeVal = frand.Bytes(renterhost.SectorSize * 4)
	}
	// spawn multiple goroutines uploading in parallel
	errCh := make(chan error)
	for i := range kvs {
		go func(i int) {
			errCh <- func() error {
				err := kv.PutBytes(ctx, kvs[i].smallKey, kvs[i].smallVal)
				if err != nil {
					return err
				}
				return kv.Put(ctx, kvs[i].largeKey, bytes.NewReader(kvs[i].largeVal))
			}()
		}(i)
	}
	for range kvs {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	// spawn multiple goroutines downloading in parallel
	// TODO: make one host fail
	for i := range kvs {
		go func(i int) {
			errCh <- func() error {
				data, err := kv.GetBytes(ctx, kvs[i].smallKey)
				if err != nil {
					return err
				} else if !bytes.Equal(data, kvs[i].smallVal) {
					return fmt.Errorf("bad data: %q", data)
				}
				var buf bytes.Buffer
				err = kv.Get(ctx, kvs[i].largeKey, &buf)
				if err != nil {
					return err
				} else if !bytes.Equal(buf.Bytes(), kvs[i].largeVal) {
					return fmt.Errorf("bad data")
				}
				// range request
				buf.Reset()
				off, n := int64(renterhost.SectorSize+10*(i+1)), int64(497*(i+1))
				err = kv.GetRange(ctx, kvs[i].largeKey, &buf, off, n)
				if err != nil {
					return err
				} else if !bytes.Equal(buf.Bytes(), kvs[i].largeVal[off:][:n]) {
					return fmt.Errorf("bad range data")
				}
				return nil
			}()
		}(i)
	}
	for range kvs {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

func wasCanceled(err error) bool {
	var he *HostError
	if errors.As(err, &he) {
		return wasCanceled(he.Err)
	}
	var hes HostErrorSet
	if errors.As(err, &hes) {
		return wasCanceled(hes[0].Err)
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func TestKVCancel(t *testing.T) {
	kv := createTestingKV(t, 3, 2, 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := kv.PutBytes(ctx, []byte("foo"), []byte("bar"))
	if !wasCanceled(err) {
		t.Fatal("expected cancel, got", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	err = kv.PutBytes(ctx, []byte("foo"), []byte("bar"))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = kv.GetBytes(ctx, []byte("foo"))
	if !wasCanceled(err) {
		t.Fatal("expected cancel, got", err)
	}
	ctx = context.Background()
	data, err := kv.GetBytes(ctx, []byte("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "bar" {
		t.Fatalf("bad data: %q", data)
	}

	// large value, using streaming API
	bigdata := frand.Bytes(renterhost.SectorSize * 4)
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	err = kv.Put(ctx, []byte("foo"), bytes.NewReader(bigdata))
	if !wasCanceled(err) {
		t.Fatal("expected cancel, got", err)
	}
	cancel()
	ctx = context.Background()
	err = kv.Put(ctx, []byte("foo"), bytes.NewReader(bigdata))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	var buf bytes.Buffer
	err = kv.Get(ctx, []byte("foo"), &buf)
	if !wasCanceled(err) {
		t.Fatal("expected cancel, got", err)
	}
	cancel()
	ctx = context.Background()
	buf.Reset()
	err = kv.Get(ctx, []byte("foo"), &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), bigdata) {
		t.Fatal("bad data")
	}
}

func BenchmarkKVPut(b *testing.B) {
	ctx := context.Background()
	kv := createTestingKV(b, 3, 2, 3)
	data := frand.Bytes(renterhost.SectorSize * 2)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := kv.PutBytes(ctx, []byte("foo"), data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVGet(b *testing.B) {
	ctx := context.Background()
	kv := createTestingKV(b, 3, 2, 3)
	data := frand.Bytes(renterhost.SectorSize * 2)
	err := kv.PutBytes(ctx, []byte("foo"), data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := kv.Get(ctx, []byte("foo"), ioutil.Discard)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVPutParallel(b *testing.B) {
	ctx := context.Background()
	kv := createTestingKV(b, 3, 2, 3)
	data := frand.Bytes(renterhost.SectorSize * 2)

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	const p = 4
	errCh := make(chan error, p)
	for j := 0; j < p; j++ {
		go func() {
			var err error
			for i := 0; i < b.N/p; i++ {
				err = kv.PutBytes(ctx, []byte("foo"), data)
				if err != nil {
					break
				}
			}
			errCh <- err
		}()
	}
	for j := 0; j < p; j++ {
		if err := <-errCh; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkKVGetParallel(b *testing.B) {
	ctx := context.Background()
	kv := createTestingKV(b, 3, 2, 3)
	data := frand.Bytes(renterhost.SectorSize * 2)
	err := kv.PutBytes(ctx, []byte("foo"), data)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	const p = 4
	errCh := make(chan error, p)
	for j := 0; j < p; j++ {
		go func() {
			var err error
			for i := 0; i < b.N/p; i++ {
				err = kv.Get(ctx, []byte("foo"), ioutil.Discard)
				if err != nil {
					break
				}
			}
			errCh <- err
		}()
	}
	for j := 0; j < p; j++ {
		if err := <-errCh; err != nil {
			b.Fatal(err)
		}
	}
}
