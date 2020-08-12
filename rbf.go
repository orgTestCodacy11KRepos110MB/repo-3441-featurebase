// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/txpath"
	"github.com/pkg/errors"
)

// RbfDBWrapper wraps an *rbf.DB
type RbfDBWrapper struct {
	Path string
	db   *rbf.DB
	reg  *rbfDBRegistrar
	muDb sync.Mutex

	// make Close() idempotent, avoiding panic on double Close()
	closed bool

	//DeleteEmptyContainer bool // needed for roaring compat?
}

// rbfDBRegistrar also allows opening the same path twice to
// result in sharing the same open database handle, and
// thus the same transactional guarantees.
//
type rbfDBRegistrar struct {
	mu sync.Mutex
	mp map[*RbfDBWrapper]bool

	path2db map[string]*RbfDBWrapper
}

var globalRbfDBReg *rbfDBRegistrar = newRbfDBRegistrar()

func newRbfDBRegistrar() *rbfDBRegistrar {
	return &rbfDBRegistrar{
		mp:      make(map[*RbfDBWrapper]bool),
		path2db: make(map[string]*RbfDBWrapper),
	}
}

// register each rbf.DB created, so we dedup and can
// can clean them up. This is called by openRbfDB() while
// holding the r.mu.Lock, since it needs to atomically
// check the registry and make a new instance only
// if one does not exist for its path, and otherwise
// return the existing instance.
func (r *rbfDBRegistrar) unprotectedRegister(w *RbfDBWrapper) {
	r.mp[w] = true
	r.path2db[w.Path] = w
}

// unregister removes w from r
func (r *rbfDBRegistrar) unregister(w *RbfDBWrapper) {
	r.mu.Lock()
	delete(r.mp, w)
	delete(r.path2db, w.Path)
	r.mu.Unlock()
}

// rbfPath is a helper for determining the full directory
// in which the RBF database will be stored.
func rbfPath(path string) string {
	if !strings.HasSuffix(path, "-rbfdb") {
		return path + "-rbfdb"
	}
	return path
}

// openRbfDB opens the database in the path directoy
// without deleting any prior content. Any
// database directory will have the "-rbfdb" suffix.
//
// openRbfDB will check the registry and make a new instance only
// if one does not exist for its path. Otherwise it returns
// the existing instance. This insures only one RbfDBWrapper
// per bpath in this pilosa node.
func (r *rbfDBRegistrar) openRbfDB(path0 string) (*RbfDBWrapper, error) {
	path := rbfPath(path0)
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.path2db[path]
	if ok {
		// creates the effect of having only one DB open per pilosa node.
		return w, nil
	}
	db := rbf.NewDB(path)

	w = &RbfDBWrapper{
		reg:  r,
		Path: path,
		db:   db,
	}

	r.unprotectedRegister(w)

	err := db.Open()
	if err != nil {
		panic(fmt.Sprintf("cannot open rbfDB at path '%v': '%v'", path, err))
	}
	return w, nil
}

type RBFTx struct {
	// initialIndex is only a debugging aid. Transactions
	// can cross indexes. It can be left empty without consequence.
	initialIndex string
	frag         *fragment
	tx           *rbf.Tx
}

func (tx *RBFTx) DBPath() string {
	return tx.tx.DBPath()
}

func (tx *RBFTx) Type() string {
	return RBFTxn
}

func (tx *RBFTx) Rollback() {
	tx.tx.Rollback()
}

func (tx *RBFTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *RBFTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	return tx.tx.RoaringBitmap(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	return tx.tx.Container(rbfName(index, field, view, shard), key)
}

func (tx *RBFTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	return tx.tx.PutContainer(rbfName(index, field, view, shard), key, c)
}

func (tx *RBFTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	return tx.tx.RemoveContainer(rbfName(index, field, view, shard), key)
}

func (tx *RBFTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	return tx.tx.Add(rbfName(index, field, view, shard), a...)
}

func (tx *RBFTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.tx.Remove(rbfName(index, field, view, shard), a...)
}

func (tx *RBFTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	return tx.tx.Contains(rbfName(index, field, view, shard), v)
}

func (tx *RBFTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	return tx.tx.ContainerIterator(rbfName(index, field, view, shard), key)
}

func (tx *RBFTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	return tx.tx.ForEach(rbfName(index, field, view, shard), fn)
}

func (tx *RBFTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	return tx.tx.ForEachRange(rbfName(index, field, view, shard), start, end, fn)
}

func (tx *RBFTx) Count(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Count(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Max(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Max(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	return tx.tx.Min(rbfName(index, field, view, shard))
}

func (tx *RBFTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	return tx.tx.UnionInPlace(rbfName(index, field, view, shard), others...)
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *RBFTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	if tx.frag == nil {
		return tx.tx.CountRange(rbfName(index, field, view, shard), start, end)
	}

	// For speed, exploit the fact that on startup the rowCache will
	// have already loaded fragments.
	rowID := start / ShardWidth
	row, err := tx.frag.unprotectedRow(tx, rowID)
	if err != nil {
		return 0, err
	}
	return row.Count(), nil
}

func (tx *RBFTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	return tx.tx.OffsetRange(rbfName(index, field, view, shard), offset, start, end)
}

func (tx *RBFTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {}

func (tx *RBFTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	return tx.tx.ImportRoaringBits(rbfName(index, field, view, shard), rit, clear, log, rowSize, data)
}

func (tx *RBFTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	if err != nil {
		return nil, -1, errors.Wrap(err, "RoaringBitmapReader RoaringBitmap")
	}
	var buf bytes.Buffer
	sz, err = rbm.WriteTo(&buf)
	if err != nil {
		return nil, -1, errors.Wrap(err, "RoaringBitmapReader rbm.WriteTo(buf)")
	}
	return ioutil.NopCloser(&buf), sz, err
}

func (tx *RBFTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {

	prefix := string(txpath.AllShardPrefix(index, field, view))

	names, err := tx.tx.BitmapNames()
	if err != nil {
		return nil, err
	}

	// Iterate over shard names and collect shards from matching field/view prefix.
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		shard := txpath.ShardFromPrefix([]byte(name))
		sliceOfShards = append(sliceOfShards, shard)
	}
	return sliceOfShards, nil
}

func (tx *RBFTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	b, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)
	return b.Iterator()
}

func (tx *RBFTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

func (tx *RBFTx) Dump() {
	tx.tx.Dump()
}

// Readonly is true if the transaction is not read-and-write, but only doing reads.
func (tx *RBFTx) Readonly() bool {
	return !tx.tx.Writable()
}

func (tx *RBFTx) UseRowCache() bool {
	// since RFB returns memory mapped data, we can't use
	// the rowCache without first making a copy.
	// So we only use the rowCache if the copy is
	// enabled.
	return rbf.EnableRowCache
}

// rbfName returns a NULL-separated key used for identifying bitmap maps in RBF.
func rbfName(index, field, view string, shard uint64) string {
	//return fmt.Sprintf("%s\x00%s\x00%s\x00%d", index, field, view, shard)
	return string(txpath.Prefix(index, field, view, shard))
}

// rbfFieldPrefix returns a prefix for field keys in RBF.
func rbfFieldPrefix(index, field string) string {
	//return fmt.Sprintf("%s\x00%s\x00", index, field)
	return string(txpath.FieldPrefix(index, field))
}

func (w *RbfDBWrapper) DeleteField(index, field, fieldPath string) error {
	w.muDb.Lock()
	defer w.muDb.Unlock()

	if err := os.RemoveAll(fieldPath); err != nil {
		return errors.Wrap(err, "removing directory")
	}

	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.DeleteBitmapsWithPrefix(rbfFieldPrefix(index, field)); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *RbfDBWrapper) DeleteIndex(indexName string) error {

	if strings.Contains(indexName, "'") {
		return fmt.Errorf("error: bad indexName `%v` in RbfDBWrapper.DeleteIndex() call: indexName cannot contain apostrophes/single quotes.", indexName)
	}
	prefix := txpath.IndexOnlyPrefix(indexName)

	w.muDb.Lock()
	defer w.muDb.Unlock()

	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.DeleteBitmapsWithPrefix(string(prefix)); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *RbfDBWrapper) Close() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		w.reg.unregister(w)
		w.closed = true
	}
	return w.db.Close()
}

func (w *RbfDBWrapper) NewRBFTx(write bool, initialIndex string, frag *fragment) (*RBFTx, error) {
	tx, err := w.db.Begin(write)
	if err != nil {
		return nil, err
	}
	return &RBFTx{tx: tx, initialIndex: initialIndex, frag: frag}, nil
}

func (w *RbfDBWrapper) DeleteFragment(index, field, view string, shard uint64, frag *fragment) error {
	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.DeleteBitmapsWithPrefix(rbfName(index, field, view, shard))
	if err != nil {
		return err
	}
	return tx.Commit()
}