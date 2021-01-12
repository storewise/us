package renterutil

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/Sia/crypto"
	"gitlab.com/NebulousLabs/encoding"
	bolt "go.etcd.io/bbolt"
	"lukechampine.com/us/hostdb"
	"lukechampine.com/us/renter"
)

// ErrKeyNotFound is returned when a key is not found in a MetaDB.
var ErrKeyNotFound = errors.New("key not found")

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
	AddBlob(b DBBlob) error
	Blob(key []byte) (DBBlob, error)
	RenameBlob(oldKey, newKey []byte) error
	DeleteBlob(key []byte) (map[hostdb.HostPublicKey][]crypto.Hash, error)
	ForEachBlob(func(key []byte) error) error

	AddChunk(m, n int, length uint64) (DBChunk, error)
	Chunk(id uint64) (DBChunk, error)
	SetChunkShard(id uint64, i int, s uint64) error

	AddShard(s DBShard) (uint64, error)
	Shard(id uint64) (DBShard, error)

	UnreferencedSectors() (map[hostdb.HostPublicKey][]crypto.Hash, error)

	AddMetadata(key, val []byte) error
	Metadata(key []byte) ([]byte, error)
	DeleteMetadata(key []byte) error
	RenameMetadata(oldKey, newKey []byte) error

	Close() error
}

// EphemeralMetaDB implements MetaDB in memory.
type EphemeralMetaDB struct {
	shards []DBShard
	chunks []DBChunk
	blobs  map[string]DBBlob
	refs   map[uint64]int
	meta   map[string]string
	mu     sync.Mutex
}

var _ MetaDB = (*EphemeralMetaDB)(nil)

// AddShard implements MetaDB.
func (db *EphemeralMetaDB) AddShard(s DBShard) (uint64, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.shards = append(db.shards, s)
	return uint64(len(db.shards)), nil
}

// Shard implements MetaDB.
func (db *EphemeralMetaDB) Shard(id uint64) (DBShard, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if id == 0 {
		return DBShard{}, ErrKeyNotFound
	}
	return db.shards[id-1], nil
}

// AddChunk implements MetaDB.
func (db *EphemeralMetaDB) AddChunk(m, n int, length uint64) (DBChunk, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	c := DBChunk{
		ID:        uint64(len(db.chunks)) + 1,
		Shards:    make([]uint64, n),
		MinShards: uint8(m),
		Len:       length,
	}
	db.chunks = append(db.chunks, c)
	return c, nil
}

// SetChunkShard implements MetaDB.
func (db *EphemeralMetaDB) SetChunkShard(id uint64, i int, s uint64) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.refs[db.chunks[id-1].Shards[i]]--
	db.chunks[id-1].Shards[i] = s
	db.refs[s]++
	return nil
}

func (db *EphemeralMetaDB) AddChunkAndShards(m int, length uint64, ss []*DBShard) (DBChunk, error) {
	shards := make([]uint64, len(ss))
	for i, s := range ss {
		id, err := db.AddShard(*s)
		if err != nil {
			return DBChunk{}, err
		}
		shards[i] = id
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	c := DBChunk{
		ID:        uint64(len(db.chunks)) + 1,
		Shards:    shards,
		MinShards: uint8(m),
		Len:       length,
	}
	db.chunks = append(db.chunks, c)
	return c, nil
}

// Chunk implements MetaDB.
func (db *EphemeralMetaDB) Chunk(id uint64) (DBChunk, error) {
	if id == 0 {
		panic("GetChunk: unset id")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.chunks[id-1], nil
}

// AddBlob implements MetaDB.
func (db *EphemeralMetaDB) AddBlob(b DBBlob) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.blobs[string(b.Key)] = b
	return nil
}

// Blob implements MetaDB.
func (db *EphemeralMetaDB) Blob(key []byte) (DBBlob, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	b, ok := db.blobs[string(key)]
	if !ok {
		return DBBlob{}, ErrKeyNotFound
	}
	return b, nil
}

// RenameBlob renames a blob from oldKey to newKey.
func (db *EphemeralMetaDB) RenameBlob(oldKey, newKey []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	b, ok := db.blobs[string(oldKey)]
	if !ok {
		return ErrKeyNotFound
	}
	db.blobs[string(newKey)] = b
	delete(db.blobs, string(oldKey))

	return nil
}

// DeleteBlob implements MetaDB.
func (db *EphemeralMetaDB) DeleteBlob(key []byte) (map[hostdb.HostPublicKey][]crypto.Hash, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	b, ok := db.blobs[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	for _, cid := range b.Chunks {
		for _, sid := range db.chunks[cid-1].Shards {
			db.refs[sid]--
		}
	}
	delete(db.blobs, string(key))
	return db.UnreferencedSectors()
}

// ForEachBlob implements MetaDB.
func (db *EphemeralMetaDB) ForEachBlob(fn func(key []byte) error) error {
	db.mu.Lock()
	var keys []string
	for key := range db.blobs {
		keys = append(keys, key)
	}
	db.mu.Unlock()
	sort.Strings(keys)
	for _, key := range keys {
		if err := fn([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

// UnreferencedSectors returns all sectors that are not referenced by any blob
// in the db.
func (db *EphemeralMetaDB) UnreferencedSectors() (map[hostdb.HostPublicKey][]crypto.Hash, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	m := make(map[hostdb.HostPublicKey][]crypto.Hash)
	for sid, n := range db.refs {
		if n == 0 {
			s := db.shards[sid-1]
			m[s.HostKey] = append(m[s.HostKey], s.SectorRoot)
		}
	}
	return m, nil
}

// AddMetadata implements MetaDB.
func (db *EphemeralMetaDB) AddMetadata(key, val []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.meta[string(key)] = string(val)
	return nil
}

// Metadata implements MetaDB.
func (db *EphemeralMetaDB) Metadata(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	md, ok := db.meta[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return []byte(md), nil
}

func (db *EphemeralMetaDB) DeleteMetadata(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.meta, string(key))
	return nil
}

func (db *EphemeralMetaDB) RenameMetadata(oldKey, newKey []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.meta[string(newKey)] = db.meta[string(oldKey)]
	delete(db.meta, string(oldKey))
	return nil
}

// Close implements MetaDB.
func (db *EphemeralMetaDB) Close() error {
	return nil
}

// NewEphemeralMetaDB initializes an EphemeralMetaDB.
func NewEphemeralMetaDB() *EphemeralMetaDB {
	db := &EphemeralMetaDB{
		refs:  make(map[uint64]int),
		blobs: make(map[string]DBBlob),
		meta:  make(map[string]string),
	}
	return db
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
	return
}

func (db *BoltMetaDB) shard(tx *bolt.Tx, id uint64) (DBShard, error) {
	var s DBShard
	if err := encoding.Unmarshal(tx.Bucket(bucketShards).Get(idToKey(id)), &s); err != nil {
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
	return
}

func (db *BoltMetaDB) chunk(tx *bolt.Tx, id uint64) (DBChunk, error) {
	var c DBChunk
	if err := encoding.Unmarshal(tx.Bucket(bucketChunks).Get(idToKey(id)), &c); err != nil {
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

// DeleteBlob implements MetaDB.
func (db *BoltMetaDB) DeleteBlob(key []byte) (map[hostdb.HostPublicKey][]crypto.Hash, error) {
	sectors := make(map[hostdb.HostPublicKey][]crypto.Hash)
	if err := db.bdb.Update(func(tx *bolt.Tx) error {
		blob, err := db.blob(tx, key)
		if err != nil {
			return err
		}
		for _, cid := range blob.Chunks {
			chunk, err := db.chunk(tx, cid)
			if err != nil {
				return err
			}
			for _, sid := range chunk.Shards {
				shard, err := db.shard(tx, sid)
				if err != nil {
					return err
				}
				sectors[shard.HostKey] = append(sectors[shard.HostKey], shard.SectorRoot)
				if err := db.deleteShard(tx, sid); err != nil {
					return err
				}
			}
			if err := db.deleteChunk(tx, cid); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketBlobs).Delete(key)
	}); err != nil {
		return nil, err
	}
	return sectors, nil
}

// ForEachBlob implements MetaDB.
func (db *BoltMetaDB) ForEachBlob(fn func(key []byte) error) error {
	return db.bdb.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBlobs).ForEach(func(k, _ []byte) error {
			return fn(k)
		})
	})
}

// UnreferencedSectors returns all sectors that are not referenced by any blob
// in the db.
func (db *BoltMetaDB) UnreferencedSectors() (map[hostdb.HostPublicKey][]crypto.Hash, error) {
	return nil, nil // TODO
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
