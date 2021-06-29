package renterutil

import (
	"bytes"
	"context"
	"io"

	"lukechampine.com/frand"
	"lukechampine.com/us/hostdb"
)

// PseudoKV implements a key-value store by uploading and downloading data from Sia
// hosts.
type PseudoKV struct {
	DB         MetaDB
	Deleter    SectorDeleter
	Uploader   BlobUploader
	Downloader BlobDownloader
	Updater    BlobUpdater
}

// Put uploads r to hosts and associates it with the specified key. Any existing
// data associated with the key will be overwritten.
func (kv PseudoKV) Put(ctx context.Context, key []byte, r io.Reader) error {
	b := DBBlob{Key: key}
	if _, err := frand.Read(b.Seed[:]); err != nil {
		return err
	}
	if err := kv.DB.AddBlob(b); err != nil {
		return err
	}
	return kv.Uploader.UploadBlob(ctx, kv.DB, b, r)
}

// PutBytes uploads val to hosts and associates it with the specified key.
func (kv PseudoKV) PutBytes(ctx context.Context, key []byte, val []byte) error {
	return kv.Put(ctx, key, bytes.NewReader(val))
}

// GetRange downloads a range of bytes within the value associated with key and
// writes it to w.
func (kv PseudoKV) GetRange(ctx context.Context, key []byte, w io.Writer, off, n int64) error {
	b, err := kv.DB.Blob(key)
	if err != nil {
		return err
	}
	return kv.Downloader.DownloadBlob(ctx, kv.DB, b, w, off, n)
}

// Get downloads the value associated with key and writes it to w.
func (kv PseudoKV) Get(ctx context.Context, key []byte, w io.Writer) error {
	return kv.GetRange(ctx, key, w, 0, -1)
}

// GetBytes downloads the value associated with key and returns it as a []byte.
func (kv PseudoKV) GetBytes(ctx context.Context, key []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := kv.Get(ctx, key, &buf)
	return buf.Bytes(), err
}

// Migrate updates an existing key, migrating each each of its chunks to the
// provided HostSet.
func (kv PseudoKV) Migrate(ctx context.Context, key []byte, whitelist []hostdb.HostPublicKey) error {
	b, err := kv.DB.Blob(key)
	if err != nil {
		return err
	}
	return kv.Updater.UpdateBlob(ctx, kv.DB, b, NewMigrationWhitelist(whitelist))
}

// Delete deletes the value associated with key.
func (kv PseudoKV) Delete(ctx context.Context, key []byte) error {
	sectors, err := kv.DB.Sectors(key)
	if err != nil {
		return err
	}
	if err := kv.Deleter.DeleteSectors(ctx, kv.DB, sectors); err != nil {
		return err
	}
	err = kv.DB.DeleteBlob(key)
	return err
}

// Rename renames a blob.
func (kv PseudoKV) Rename(oldKey, newKey []byte) error {
	return kv.DB.RenameBlob(oldKey, newKey)
}

// Close implements io.Closer.
func (kv PseudoKV) Close() error {
	return kv.DB.Close()
}
