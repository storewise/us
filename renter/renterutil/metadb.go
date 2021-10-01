package renterutil

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	bolt "go.etcd.io/bbolt"
	"go.sia.tech/siad/crypto"
	"go.uber.org/multierr"

	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter"
)

var (
	// ErrKeyNotFound is returned when a key is not found in a MetaDB.
	ErrKeyNotFound = errors.New("key not found")
	// errChunkNotFound is returned when a requested chunk is not found.
	errChunkNotFound = errors.New("chunk not found")
	// errShardNotFound is returned when a requested shard is not found.
	errShardNotFound = errors.New("shard not found")
)

// A DBBlob is the concatenation of one or more chunks.
type DBBlob struct {
	Key    []byte
	Chunks []uint64
	Seed   renter.KeySeed
}

// A DBChunk is a set of erasure-encoded shards.
type DBChunk struct {
	ID        uint64
	Shards    []uint64
	MinShards uint8
	Len       uint64 // of chunk, before erasure encoding
}

// A DBShard is a piece of data stored on a Sia host.
type DBShard struct {
	HostKey    hostdb.HostPublicKey
	SectorRoot crypto.Hash
	Offset     uint32
	Nonce      [24]byte
	// NOTE: Length is not stored, as it can be derived from the DBChunk.Len
}

// A MetaDB stores the metadata of blobs stored on Sia hosts.
type MetaDB interface {
	io.Closer

	AddBlob(b DBBlob) error
	Blob(key []byte) (DBBlob, error)
	RenameBlob(oldKey, newKey []byte) error
	DeleteBlob(key []byte) error
	ForEachBlob(func(key []byte) error) error

	AddChunk(m, n int, length uint64) (DBChunk, error)
	Chunk(id uint64) (DBChunk, error)
	SetChunkShard(id uint64, i int, s uint64) error

	AddShard(s DBShard) (uint64, error)
	Shard(id uint64) (DBShard, error)

	Sectors(key []byte) (map[hostdb.HostPublicKey][]crypto.Hash, error)

	AddMetadata(key, val []byte) error
	Metadata(key []byte) ([]byte, error)
	DeleteMetadata(key []byte) error
	RenameMetadata(oldKey, newKey []byte) error
}

// BoltMetaDB implements MetaDB with a Bolt database.
type BoltMetaDB struct {
	bdb *bolt.DB
}

var _ MetaDB = (*BoltMetaDB)(nil)

var (
	bucketBlobs  = []byte("blobs")
	bucketChunks = []byte("chunks")
	bucketShards = []byte("shards")
	bucketMeta   = []byte("meta")
)

// AddShard implements MetaDB.
func (db *BoltMetaDB) AddShard(s DBShard) (id uint64, err error) {
	err = db.bdb.Update(func(tx *bolt.Tx) error {
		id, err = db.addShard(tx, s)
		return err
	})
	return
}

func (db *BoltMetaDB) addShard(tx *bolt.Tx, s DBShard) (id uint64, err error) {
	id, err = tx.Bucket(bucketShards).NextSequence()
	if err != nil {
		return 0, err
	}
	err = tx.Bucket(bucketShards).Put(idToKey(id), encoding.Marshal(s))
	if err != nil {
		return 0, err
	}
	return id, nil
}

// Shard implements MetaDB.
func (db *BoltMetaDB) Shard(id uint64) (s DBShard, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		s, err = db.shard(tx, id)
		return err
	})
	if errors.Is(err, errShardNotFound) {
		return DBShard{}, os.ErrNotExist
	}

	return
}

func (db *BoltMetaDB) shard(tx *bolt.Tx, id uint64) (DBShard, error) {
	shard := tx.Bucket(bucketShards).Get(idToKey(id))
	if shard == nil {
		return DBShard{}, errShardNotFound
	}

	var s DBShard
	if err := encoding.Unmarshal(shard, &s); err != nil {
		return DBShard{}, err
	}
	return s, nil
}

func (db *BoltMetaDB) deleteShard(tx *bolt.Tx, id uint64) error {
	return tx.Bucket(bucketShards).Delete(idToKey(id))
}

// AddChunk implements MetaDB.
func (db *BoltMetaDB) AddChunk(m, n int, length uint64) (c DBChunk, err error) {
	err = db.bdb.Update(func(tx *bolt.Tx) error {
		c, err = db.addChunk(tx, m, length, make([]uint64, n))
		return err
	})
	return
}

func (db *BoltMetaDB) addChunk(tx *bolt.Tx, m int, length uint64, shards []uint64) (c DBChunk, err error) {
	id, err := tx.Bucket(bucketChunks).NextSequence()
	if err != nil {
		return DBChunk{}, err
	}
	c = DBChunk{
		ID:        id,
		Shards:    shards,
		MinShards: uint8(m),
		Len:       length,
	}
	err = tx.Bucket(bucketChunks).Put(idToKey(id), encoding.Marshal(c))
	if err != nil {
		return DBChunk{}, err
	}
	return c, nil
}

// SetChunkShard implements MetaDB.
func (db *BoltMetaDB) SetChunkShard(id uint64, i int, s uint64) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		var c DBChunk
		key := idToKey(id)
		if err := encoding.Unmarshal(tx.Bucket(bucketChunks).Get(key), &c); err != nil {
			return err
		}
		c.Shards[i] = s
		return tx.Bucket(bucketChunks).Put(key, encoding.Marshal(c))
	})
}

func (db *BoltMetaDB) AddChunkAndShards(m int, length uint64, ss []*DBShard) (c DBChunk, err error) {
	err = db.bdb.Update(func(tx *bolt.Tx) error {
		shards := make([]uint64, len(ss))
		for i, s := range ss {
			if s == nil {
				continue
			}
			id, err := db.addShard(tx, *s)
			if err != nil {
				return nil
			}
			shards[i] = id
		}
		c, err = db.addChunk(tx, m, length, shards)
		return err
	})
	return c, err
}

// Chunk implements MetaDB.
func (db *BoltMetaDB) Chunk(id uint64) (c DBChunk, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		c, err = db.chunk(tx, id)
		return err
	})
	if errors.Is(err, errChunkNotFound) {
		return DBChunk{}, os.ErrNotExist
	}

	return
}

func (db *BoltMetaDB) chunk(tx *bolt.Tx, id uint64) (DBChunk, error) {
	chunk := tx.Bucket(bucketChunks).Get(idToKey(id))
	if chunk == nil {
		return DBChunk{}, errChunkNotFound
	}

	var c DBChunk
	if err := encoding.Unmarshal(chunk, &c); err != nil {
		return DBChunk{}, err
	}
	return c, nil
}

func (db *BoltMetaDB) deleteChunk(tx *bolt.Tx, id uint64) error {
	return tx.Bucket(bucketChunks).Delete(idToKey(id))
}

// AddBlob implements MetaDB.
func (db *BoltMetaDB) AddBlob(b DBBlob) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBlobs).Put(b.Key, encoding.MarshalAll(b.Chunks, b.Seed))
	})
}

// Blob implements MetaDB.
func (db *BoltMetaDB) Blob(key []byte) (b DBBlob, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		b, err = db.blob(tx, key)
		return err
	})
	return
}

func (db *BoltMetaDB) blob(tx *bolt.Tx, key []byte) (DBBlob, error) {
	blobBytes := tx.Bucket(bucketBlobs).Get(key)
	if len(blobBytes) == 0 {
		return DBBlob{}, ErrKeyNotFound
	}

	var b DBBlob
	if err := encoding.UnmarshalAll(blobBytes, &b.Chunks, &b.Seed); err != nil {
		return DBBlob{}, err
	}
	b.Key = key
	return b, nil
}

// RenameBlob renames a blob from oldKey to newKey.
func (db *BoltMetaDB) RenameBlob(oldKey, newKey []byte) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketBlobs)

		blobBytes := bucket.Get(oldKey)
		if len(blobBytes) == 0 {
			return ErrKeyNotFound
		}
		err := bucket.Put(newKey, blobBytes)
		if err != nil {
			return err
		}

		return bucket.Delete(oldKey)
	})
}

func (db *BoltMetaDB) Sectors(key []byte) (map[hostdb.HostPublicKey][]crypto.Hash, error) {
	sectors := make(map[hostdb.HostPublicKey][]crypto.Hash)
	if err := db.bdb.View(func(tx *bolt.Tx) error {
		blob, err := db.blob(tx, key)
		if err != nil {
			return err
		}
		for _, cid := range blob.Chunks {
			chunk, e := db.chunk(tx, cid)
			if errors.Is(e, errChunkNotFound) {
				continue
			} else if e != nil {
				err = multierr.Append(err, e)
				continue
			}
			for _, sid := range chunk.Shards {
				shard, e := db.shard(tx, sid)
				if errors.Is(e, errShardNotFound) {
					continue
				} else if e != nil {
					err = multierr.Append(err, e)
					continue
				}
				sectors[shard.HostKey] = append(sectors[shard.HostKey], shard.SectorRoot)
			}
		}
		return err
	}); err != nil {
		return nil, err
	}
	return sectors, nil
}

// DeleteBlob implements MetaDB.
func (db *BoltMetaDB) DeleteBlob(key []byte) error {
	if err := db.bdb.Update(func(tx *bolt.Tx) error {
		blob, err := db.blob(tx, key)
		if err != nil {
			return err
		}
		for _, cid := range blob.Chunks {
			chunk, e := db.chunk(tx, cid)
			if errors.Is(e, errChunkNotFound) {
				continue
			}
			if e != nil {
				err = multierr.Append(err, e)
				continue
			}
			for _, sid := range chunk.Shards {
				if e = db.deleteShard(tx, sid); e != nil && !errors.Is(e, errShardNotFound) {
					err = multierr.Append(err, e)
				}
			}
			if e = db.deleteChunk(tx, cid); e != nil {
				err = multierr.Append(err, e)
			}
		}
		if e := tx.Bucket(bucketBlobs).Delete(key); e != nil {
			err = multierr.Append(err, e)
		}
		return err
	}); err != nil {
		return err
	}
	return nil
}

// ForEachBlob implements MetaDB.
func (db *BoltMetaDB) ForEachBlob(fn func(key []byte) error) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBlobs).ForEach(func(k, _ []byte) error {
			return fn(k)
		})
	})
}

// AddMetadata adds metadata that associated with the key.
func (db *BoltMetaDB) AddMetadata(key, val []byte) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		return db.addMetadata(tx, key, val)
	})
}

func (db *BoltMetaDB) addMetadata(tx *bolt.Tx, key, val []byte) error {
	return tx.Bucket(bucketMeta).Put(key, val)
}

// Metadata returns metadata associated with the key.
func (db *BoltMetaDB) Metadata(key []byte) (val []byte, err error) {
	err = db.bdb.View(func(tx *bolt.Tx) error {
		val, err = db.metadata(tx, key)
		return err
	})
	return
}

func (db *BoltMetaDB) metadata(tx *bolt.Tx, key []byte) ([]byte, error) {
	res := tx.Bucket(bucketMeta).Get(key)
	if res == nil {
		return nil, ErrKeyNotFound
	}
	return res, nil
}

// DeleteMetadata remove metadata associated with the given key.
func (db *BoltMetaDB) DeleteMetadata(key []byte) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		return db.deleteMetadata(tx, key)
	})
}

func (db *BoltMetaDB) deleteMetadata(tx *bolt.Tx, key []byte) error {
	return tx.Bucket(bucketMeta).Delete(key)
}

// RenameMetadata rename metadata.
func (db *BoltMetaDB) RenameMetadata(oldKey, newKey []byte) error {
	return db.bdb.Update(func(tx *bolt.Tx) error {
		data, err := db.metadata(tx, oldKey)
		if err != nil {
			return err
		}
		err = db.addMetadata(tx, newKey, data)
		if err != nil {
			return err
		}
		return db.deleteMetadata(tx, oldKey)
	})
}

// Close implements MetaDB.
func (db *BoltMetaDB) Close() error {
	return db.bdb.Close()
}

// NewBoltMetaDB initializes a MetaDB backed by a Bolt database.
func NewBoltMetaDB(path string) (*BoltMetaDB, error) {
	bdb, err := bolt.Open(path, 0660, &bolt.Options{
		Timeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	db := &BoltMetaDB{
		bdb: bdb,
	}
	// initialize
	err = bdb.Update(func(tx *bolt.Tx) error {
		for _, bucket := range [][]byte{
			bucketBlobs,
			bucketChunks,
			bucketShards,
			bucketMeta,
		} {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

func idToKey(id uint64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	return key
}
