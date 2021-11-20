package dsbbolt

import (
	dskey "github.com/daotl/go-datastore/key"
	"github.com/daotl/go-datastore/query"
)

func copyBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func toQueryEntry(k []byte, v []byte, KeysOnly bool) query.Entry {
	var entry query.Entry
	entry.Key = dskey.NewBytesKey(copyBytes(k))
	if !KeysOnly {
		entry.Value = copyBytes(v)
	}
	entry.Size = len(v)
	return entry
}
