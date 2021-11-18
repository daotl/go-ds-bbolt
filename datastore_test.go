package dsbbolt

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/daotl/go-datastore"
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

func Test_NewDatastore(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Success", args{filepath.Join(t.TempDir(), "bolt")}, false},
		{"Fail", args{"/root/toor"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ds, err := NewDatastore(tt.args.path, nil, nil, dskey.KeyTypeBytes); (err != nil) != tt.wantErr {
				t.Fatalf("NewDatastore() err = %v, wantErr %v", err, tt.wantErr)
			} else if !tt.wantErr {
				if err := ds.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func Test_Datastore(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "bolt")
	ds, err := NewDatastore(tmpFile, nil, nil, dskey.KeyTypeBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	key := dskey.NewBytesKeyFromString("keks")
	key2 := dskey.NewBytesKeyFromString("keks2")
	if err := ds.Put(context.Background(), key, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := ds.Put(context.Background(), key2, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	data, err := ds.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data, []byte("hello world")) {
		t.Fatal("bad data")
	}
	if has, err := ds.Has(context.Background(), key); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Fatal("should have key")
	}
	if size, err := ds.GetSize(context.Background(), key); err != nil {
		t.Fatal(err)
	} else if size != len([]byte("hello world")) {
		t.Fatal("incorrect data size")
	}
	// test a query where we specify a search key
	rs, err := ds.Query(context.Background(), query.Query{Prefix: key})
	if err != nil {
		t.Fatal(err)
	}
	res, err := rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		fmt.Printf("only found %v results \n", len(res))
		for _, v := range res {
			fmt.Printf("%+v\n", v)
		}
		t.Fatal("bad number of results")
	}
	// test a query where we dont specify a search key
	rs, err = ds.Query(context.Background(), query.Query{Prefix: dskey.EmptyBytesKey})
	if err != nil {
		t.Fatal(err)
	}
	res, err = rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("bad number of results")
	}
	// test a query where we specify a partial prefix
	rs, err = ds.Query(context.Background(), query.Query{Prefix: dskey.NewBytesKeyFromString("kek")})
	if err != nil {
		t.Fatal(err)
	}
	res, err = rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("bad number of results")
	}
	if err := ds.Delete(nil, key); err != nil {
		t.Fatal(err)
	}
	if has, err := ds.Has(context.Background(), key); err != nil {
		if err != datastore.ErrNotFound {
			t.Fatal(err)
		}
	} else if has {
		t.Fatal("should not have key")
	}
	if size, err := ds.GetSize(context.Background(), key); err != nil {
		if err != datastore.ErrNotFound {
			t.Fatal(err)
		}
	} else if size != 0 {
		t.Fatal("bad size")
	}

}
