package dsbbolt

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
	"go.etcd.io/bbolt"
)

var ErrKeyTypeNotMatch = errors.New("key type does not match")

var (
	defaultBucket                        = []byte("datastore")
	_             datastore.TxnDatastore = (*Datastore)(nil)
)

// Datastore implements a daotl datastore
// backed by a bbolt db, only byteskey is supported now
type Datastore struct {
	db     *bbolt.DB
	bucket []byte // only use one bucket?
	ktype  dskey.KeyType
}

// Sync is not required for boltdb, so no op
func (d *Datastore) Sync(ctx context.Context, prefix dskey.Key) error {
	return nil
}

// NewDatastore is used to instantiate our datastore
func NewDatastore(path string, opts *bbolt.Options, bucket []byte, keytype dskey.KeyType) (*Datastore, error) {
	if keytype != dskey.KeyTypeBytes {
		return nil, ErrKeyTypeNotMatch
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
		db.Close()
		return nil, err
	}
	ds := &Datastore{db: db, bucket: bucket, ktype: keytype}
	return ds, nil
}

// Put is used to store something in our underlying datastore
func (d *Datastore) Put(ctx context.Context, key dskey.Key, value []byte) error {
	if key.KeyType() != d.ktype {
		return ErrKeyTypeNotMatch
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Put(key.Bytes(), value)
	})
}

// Delete removes a key/value pair from our datastore
func (d *Datastore) Delete(ctx context.Context, key dskey.Key) error {
	if key.KeyType() != d.ktype {
		return ErrKeyTypeNotMatch
	}
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Delete(key.Bytes())
	})
}

// Get is used to retrieve a value from the datastore
func (d *Datastore) Get(ctx context.Context, key dskey.Key) ([]byte, error) {
	if key.KeyType() != d.ktype {
		return nil, ErrKeyTypeNotMatch
	}
	var result []byte
	if err := d.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(d.bucket).Get(key.Bytes())
		if data == nil {
			return datastore.ErrNotFound
		}
		result = copyBytes(data)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

// Has returns whether the key is present in our datastore
func (d *Datastore) Has(ctx context.Context, key dskey.Key) (bool, error) {
	if key.KeyType() != d.ktype {
		return false, ErrKeyTypeNotMatch
	}
	return datastore.GetBackedHas(ctx, d, key)
}

// GetSize returns the size of the value referenced by key
func (d *Datastore) GetSize(ctx context.Context, key dskey.Key) (int, error) {
	if key.KeyType() != d.ktype {
		return -1, ErrKeyTypeNotMatch
	}

	return datastore.GetBackedSize(ctx, d, key)
}

// return true if type mismatch
func keyTypeMismatch(q dskey.Key, keyType dskey.KeyType) bool {
	if q != nil && q.KeyType() != keyType {
		return true
	}
	return false
}

func queryWithCursor(cursor *bbolt.Cursor, q query.Query, ktype dskey.KeyType) (query.Results, error) {
	if keyTypeMismatch(q.Prefix, ktype) ||
		keyTypeMismatch(q.Range.Start, ktype) ||
		keyTypeMismatch(q.Range.End, ktype) {
		return nil, ErrKeyTypeNotMatch
	}

	var cursorStart []byte = []byte{}
	checkPrefix := false
	var pref []byte

	if q.Prefix != nil {
		checkPrefix = true
		pref = q.Prefix.Bytes()

		switch ktype {
		case dskey.KeyTypeBytes:
			cursorStart = pref
		case dskey.KeyTypeString:
			// not supported now
			return nil, ErrKeyTypeNotMatch
		}
	}

	// cursor starting from max(prefix, range.start)
	if q.Range.Start != nil {
		rangeStartKey := q.Range.Start
		switch ktype {
		case dskey.KeyTypeBytes:
			rangeStartBytes := rangeStartKey.Bytes()
			if bytes.Compare(cursorStart, rangeStartBytes) < 0 {
				cursorStart = rangeStartBytes
			}
		case dskey.KeyTypeString:
			// not supported now
			return nil, ErrKeyTypeNotMatch
		}
	}
	checkRangeEnd := false
	var end []byte
	if q.Range.End != nil {
		checkRangeEnd = true
		end = q.Range.End.Bytes()
	}

	var entries []query.Entry

	for k, v := cursor.Seek(cursorStart); k != nil; k, v = cursor.Next() {
		if checkPrefix && !bytes.HasPrefix(k, pref) {
			break
		}
		// strictly equal to prefix is not allowed
		if checkPrefix && bytes.Equal(k, pref) {
			continue
		}
		if checkRangeEnd && bytes.Compare(end, k) <= 0 {
			break
		}
		entries = append(entries, toQueryEntry(k, v, q.KeysOnly))
	}
	results := query.ResultsWithEntries(q, entries)
	results = query.NaiveQueryApply(q, results)
	return results, nil
}

// Query performs a complex search query on the underlying datastore
// For more information see :
// https://github.com/ipfs/go-datastore/blob/aa9190c18f1576be98e974359fd08c64ca0b5a94/examples/fs.go#L96
// https://github.com/etcd-io/bbolt#prefix-scans
func (d *Datastore) Query(ctx context.Context, q query.Query) (query.Results, error) {
	var results query.Results
	err := d.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(d.bucket).Cursor()
		var err error
		results, err = queryWithCursor(cursor, q, d.ktype)
		return err
	})

	return results, err
}

// Batch returns a basic batched bolt datastore wrapper
// it is a temporary method until we implement a proper
// transactional batched datastore
//func (d *Datastore) Batch(ctx context.Context) (datastore.Batch, error) {
//	return datastore.NewBasicBatch(d), nil
//}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}
