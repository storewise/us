package renterutil

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"lukechampine.com/us/renter"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/Sia/encoding"
	bolt "go.etcd.io/bbolt"
	"lukechampine.com/us/hostdb"
)

func newTempBoldMetaDB(t *testing.T) *BoltMetaDB {
	t.Helper()

	file, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatal("failed to create a temporary file:", err)
	}
	t.Cleanup(func() {
		if err := os.Remove(file.Name()); err != nil {
			t.Error("failed to remove a temporary file:", err)
		}
	})

	db, err := NewBoltMetaDB(file.Name())
	if err != nil {
		t.Fatal("failed to create a bold meta DB:", err)
	}
	return db
}

func randomShard(t *testing.T) DBShard {
	t.Helper()

	pk, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal("failed to generate a key:", err)
	}

	root := make([]byte, 32)
	_, err = rand.Reader.Read(root)
	if err != nil {
		t.Fatal("failed to read from random source:", err)
	}

	var nonce [24]byte
	_, err = rand.Reader.Read(nonce[:])
	if err != nil {
		t.Fatal("failed to read from random source:", err)
	}

	return DBShard{
		HostKey:    hostdb.HostKeyFromPublicKey(pk),
		SectorRoot: crypto.HashBytes(root),
		Offset:     0,
		Nonce:      nonce,
	}
}

func randomSeed(t *testing.T) renter.KeySeed {
	t.Helper()

	var seed [32]byte
	_, err := rand.Reader.Read(seed[:])
	if err != nil {
		t.Fatal("failed to read from random source:", err)
	}
	return seed
}

func TestBoltMetaDB_AddShard(t *testing.T) {
	db := newTempBoldMetaDB(t)
	shard := randomShard(t)

	sid, err := db.AddShard(shard)
	if err != nil {
		t.Error("failed to add a shard:", err)
	}

	if err := db.bdb.View(func(tx *bolt.Tx) error {
		var res DBShard
		if err := encoding.Unmarshal(tx.Bucket(bucketShards).Get(idToKey(sid)), &res); err != nil {
			t.Error("failed to unmarshal:", err)
		}
		if !reflect.DeepEqual(res, shard) {
			t.Errorf("expect %v, got %v", shard, res)
		}
		return nil
	}); err != nil {
		t.Error("View returns an error:", err)
	}
}

func TestBoltMetaDB_Shard(t *testing.T) {
	db := newTempBoldMetaDB(t)
	shard := randomShard(t)

	sid, err := db.AddShard(shard)
	if err != nil {
		t.Error("failed to add a shard:", err)
	}

	res, err := db.Shard(sid)
	if err != nil {
		t.Error("failed to get a shard:", err)
	}

	if !reflect.DeepEqual(res, shard) {
		t.Errorf("expect %v, got %v", shard, res)
	}
}

func TestBoltMetaDB_AddChunk(t *testing.T) {
	db := newTempBoldMetaDB(t)
	m := 10
	n := 5
	length := uint64(30 * 1024 * 1024)
	chunk, err := db.AddChunk(m, n, length)
	if err != nil {
		t.Error("failed to add a chunk:", err)
	}

	if int(chunk.MinShards) != m {
		t.Errorf("expect %v, got %v", m, chunk.MinShards)
	}
	if chunk.Len != length {
		t.Errorf("expect %v, got %v", length, chunk.Len)
	}

	if err := db.bdb.View(func(tx *bolt.Tx) error {
		var res DBChunk
		if err := encoding.Unmarshal(tx.Bucket(bucketChunks).Get(idToKey(chunk.ID)), &res); err != nil {
			t.Error("failed to unmarshal:", err)
		}
		if !reflect.DeepEqual(res, chunk) {
			t.Errorf("expect %v, got %v", chunk, res)
		}
		return nil
	}); err != nil {
		t.Error("View returns an error:", err)
	}
}

func TestBoltMetaDB_Chunk(t *testing.T) {
	db := newTempBoldMetaDB(t)
	m := 10
	n := 5
	length := uint64(30 * 1024 * 1024)
	chunk, err := db.AddChunk(m, n, length)
	if err != nil {
		t.Error("failed to add a chunk:", err)
	}

	res, err := db.Chunk(chunk.ID)
	if err != nil {
		t.Error("failed to get a chunk:", err)
	}

	if !reflect.DeepEqual(res, chunk) {
		t.Errorf("expect %v, got %v", chunk, res)
	}
}

func TestBoltMetaDB_AddBlob(t *testing.T) {
	db := newTempBoldMetaDB(t)
	blob := DBBlob{
		Key:    []byte("dir/file1"),
		Chunks: []uint64{1, 3, 5},
		Seed:   randomSeed(t),
	}
	if err := db.AddBlob(blob); err != nil {
		t.Error("failed to add a blob:", err)
	}

	if err := db.bdb.View(func(tx *bolt.Tx) error {
		var res DBBlob
		if err := encoding.UnmarshalAll(tx.Bucket(bucketBlobs).Get(blob.Key), &res.Chunks, &res.Seed); err != nil {
			t.Error("failed to unmarshal:", err)
		}
		res.Key = blob.Key
		if !reflect.DeepEqual(res, blob) {
			t.Errorf("expect %v, got %v", blob, res)
		}
		return nil
	}); err != nil {
		t.Error("View returns an error:", err)
	}
}

func TestBoltMetaDB_Blob(t *testing.T) {
	db := newTempBoldMetaDB(t)
	t.Run("exist", func(t *testing.T) {
		blob := DBBlob{
			Key:    []byte("dir/file1"),
			Chunks: []uint64{1, 3, 5},
			Seed:   randomSeed(t),
		}
		if err := db.AddBlob(blob); err != nil {
			t.Error("failed to add a blob:", err)
		}

		res, err := db.Blob(blob.Key)
		if err != nil {
			t.Error("failed to get a blob:", err)
		}

		if !reflect.DeepEqual(res, blob) {
			t.Errorf("expect %v, got %v", blob, res)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, err := db.Blob([]byte("non-existing-key"))
		if !errors.Is(err, ErrKeyNotFound) {
			t.Errorf("expect %v, got %v", ErrKeyNotFound, err)
		}
	})
}

func TestBoltMetaDB_RenameBlob(t *testing.T) {
	db := newTempBoldMetaDB(t)
	blob := DBBlob{
		Key:    []byte("dir/file1"),
		Chunks: []uint64{1, 3, 5},
		Seed:   randomSeed(t),
	}
	if err := db.AddBlob(blob); err != nil {
		t.Error("failed to add a blob:", err)
	}

	newKey := []byte("dir2/file2")
	if err := db.RenameBlob(blob.Key, newKey); err != nil {
		t.Error("failed to rename a blob:", err)
	}

	res, err := db.Blob(newKey)
	if err != nil {
		t.Error("failed to get a blob:", err)
	}

	expect := blob
	expect.Key = newKey
	if !reflect.DeepEqual(res, expect) {
		t.Errorf("expect %v, got %v", expect, res)
	}

	_, err = db.Blob(blob.Key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expect %v, got %v", ErrKeyNotFound, err)
	}
}

func TestBoltMetaDB_DeleteBlob(t *testing.T) {
	chunkSize := 10
	m := 10
	n := 5
	length := uint64(30 * 1024 * 1024)

	db := newTempBoldMetaDB(t)
	sectors := make(map[hostdb.HostPublicKey][]crypto.Hash)
	var (
		chunkIDs []uint64
		shardIDs []uint64
	)
	blob := DBBlob{
		Key:  []byte("key"),
		Seed: randomSeed(t),
	}
	for i := 0; i != chunkSize; i += 1 {
		chunk, err := db.AddChunk(m, n, length)
		if err != nil {
			t.Error("failed to add a chunk:", err)
		}
		chunkIDs = append(chunkIDs, chunk.ID)

		for j := 0; j != n; j += 1 {
			shard := randomShard(t)
			sectors[shard.HostKey] = append(sectors[shard.HostKey], shard.SectorRoot)

			sid, err := db.AddShard(shard)
			if err != nil {
				t.Error("failed to add a shard:", err)
			}
			if err := db.SetChunkShard(chunk.ID, j, sid); err != nil {
				t.Error("failed to set chunk shard:", err)
			}
			shardIDs = append(shardIDs, sid)
		}
		blob.Chunks = append(blob.Chunks, chunk.ID)
	}
	if err := db.AddBlob(blob); err != nil {
		t.Error("failed to add a blob:", err)
	}

	res, err := db.DeleteBlob(blob.Key)
	if err != nil {
		t.Error("failed to delete a blob:", err)
	}
	if !reflect.DeepEqual(res, sectors) {
		t.Errorf("expect %v, got %v", sectors, res)
	}

	_, err = db.Blob(blob.Key)
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("expect %v, got %v", ErrKeyNotFound, err)
	}

	if err := db.bdb.View(func(tx *bolt.Tx) error {
		for _, cid := range chunkIDs {
			if len(tx.Bucket(bucketChunks).Get(idToKey(cid))) != 0 {
				t.Errorf("chunk %v still exists", cid)
			}
		}
		for _, sid := range shardIDs {
			if len(tx.Bucket(bucketShards).Get(idToKey(sid))) != 0 {
				t.Errorf("shard %v still exists", sid)
			}
		}
		return nil
	}); err != nil {
		t.Error("View returns an error:", err)
	}
}
