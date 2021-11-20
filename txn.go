package dsbbolt

import (
	"context"
	"fmt"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
	"go.etcd.io/bbolt"
)

func (d *Datastore) NewTransaction(ctx context.Context, readOnly bool) (datastore.Txn, error) {
	tx, err := d.db.Begin(readOnly)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket(d.bucket)

	return &txn{tx: tx, ktype: d.ktype, bucket: bucket}, nil
}

type txn struct {
	tx     *bbolt.Tx
	bucket *bbolt.Bucket
	ktype  dskey.KeyType
}

func (b *txn) Get(ctx context.Context, key dskey.Key) (value []byte, err error) {
	if key.KeyType() != b.ktype {
		return nil, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return nil, datastore.ErrNotFound
	}
	return copyBytes(value), nil
}

func (b *txn) Has(ctx context.Context, key dskey.Key) (exists bool, err error) {
	if key.KeyType() != b.ktype {
		return false, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return false, nil
	}
	return true, nil
}

func (b *txn) GetSize(ctx context.Context, key dskey.Key) (size int, err error) {
	if key.KeyType() != b.ktype {
		return -1, ErrKeyTypeNotMatch
	}

	data := b.bucket.Get(key.Bytes())
	if data == nil {
		return -1, datastore.ErrNotFound
	}
	return len(data), nil
}

func (b *txn) Query(ctx context.Context, q query.Query) (query.Results, error) {
	cursor := b.bucket.Cursor()
	return queryWithCursor(cursor, q, b.ktype)
}

func (b *txn) Put(ctx context.Context, key dskey.Key, value []byte) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	return b.bucket.Put(key.Bytes(), value)
}

func (b *txn) Delete(ctx context.Context, key dskey.Key) error {
	if key.KeyType() != b.ktype {
		return ErrKeyTypeNotMatch
	}
	return b.bucket.Delete(key.Bytes())
}

func (b *txn) Commit(ctx context.Context) error {
	return b.tx.Commit()
}

func (b *txn) Discard(ctx context.Context) {
	err := b.tx.Rollback()
	if err != nil {
		fmt.Println("bolt rollback err", err)
	}
	return
}
