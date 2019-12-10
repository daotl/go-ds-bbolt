package dsbbolt

import (
	"os"

	"bytes"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.etcd.io/bbolt"
	//	"github.com/ipfs/go-datastore"
)

var (
	defaultBucket                    = []byte("datastore")
	_             datastore.Batching = (*Datastore)(nil)
)

// Datastore implements an ipfs datastore
// backed by a bbolt db
type Datastore struct {
	db       *bbolt.DB
	bucket   []byte
	withSync bool
}

// NewDatastore is used to instantiate our datastore
func NewDatastore(path string, opts *bbolt.Options, bucket []byte) (*Datastore, error) {
	if opts == nil {
		opts = bbolt.DefaultOptions
	}
	db, err := bbolt.Open(path, os.FileMode(0640), opts)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		bucket = defaultBucket
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &Datastore{db, bucket, !opts.NoSync}, nil
}

// Put is used to store something in our underlying datastore
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Put(key.Bytes(), value)
	})
}

// Delete removes a key/value pair from our datastore
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Delete(key.Bytes())
	})
}

// Get is used to retrieve a value from the datastore
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	var data []byte
	if err := d.db.View(func(tx *bbolt.Tx) error {
		// taken from https://github.com/ipfs/go-ds-bolt/blob/master/datastore.go#L54
		value := tx.Bucket(d.bucket).Get(key.Bytes())
		if value == nil {
			return datastore.ErrNotFound
		}
		data = make([]byte, len(value))
		copy(data, value)
		return nil
	}); err != nil {
		return nil, err
	}
	return data, nil
}

// Has returns whether the key is present in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	data, err := d.Get(key)
	if err != nil {
		return false, err
	}
	return data != nil, nil
}

// GetSize returns the size of the value referenced by key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	return datastore.GetBackedSize(d, key)
}

// Query performs a complex search query on the underlying datastore
// For more information see :
// https://github.com/ipfs/go-datastore/blob/aa9190c18f1576be98e974359fd08c64ca0b5a94/examples/fs.go#L96
// https://github.com/etcd-io/bbolt#prefix-scans
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	var entries []query.Entry
	if err := d.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(d.bucket).Cursor()
		if q.Prefix == "" {
			for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
				var entry query.Entry
				entry.Key = string(k)
				if !q.KeysOnly {
					entry.Value = v
					entry.Size = int(len(entry.Value))
				}
				entries = append(entries, entry)
			}
			return nil
		}
		pref := []byte(q.Prefix)
		for k, v := cursor.Seek(pref); k != nil && bytes.HasPrefix(k, pref); k, v = cursor.Next() {
			var entry query.Entry
			entry.Key = string(k)
			if !q.KeysOnly {
				entry.Value = v
			}
			entries = append(entries, entry)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	results := query.ResultsWithEntries(q, entries)
	// close the result builder since we are done using it
	return results, nil
}

// Sync is used to manually trigger syncing db contents to disk.
// This call is only usable when synchronous writes aren't enabled
func (d *Datastore) Sync(prefix datastore.Key) error {
	if d.withSync {
		return nil
	}
	return d.db.Sync()
}

// Batch returns a basic batched bolt datastore wrapper
// it is a temporary method until we implement a proper
// transactional batched datastore
func (d *Datastore) Batch() (datastore.Batch, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &bboltBatch{
		tx:  tx,
		bkt: tx.Bucket(d.bucket),
	}, nil
}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}

// implements batching capabilities
type bboltBatch struct {
	tx  *bbolt.Tx
	bkt *bbolt.Bucket
}

// Commit the underlying batched transactions
func (bb *bboltBatch) Commit() error {
	return bb.tx.Commit()
}

// Add delete operation to the batch
func (bb *bboltBatch) Delete(key datastore.Key) error {
	return bb.bkt.Delete(key.Bytes())
}

// Add a put operation to the batch
func (bb *bboltBatch) Put(key datastore.Key, val []byte) error {
	return bb.bkt.Put(key.Bytes(), val)
}
