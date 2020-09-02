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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/testhook"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// View layout modes.
const (
	viewStandard = "standard"

	viewBSIGroupPrefix = "bsig_"
)

// view represents a container for field data.
type view struct {
	mu            sync.RWMutex
	path          string
	index         string
	field         string
	name          string
	qualifiedName string

	holder *Holder
	idx    *Index

	fieldType string
	cacheType string
	cacheSize uint32

	// Fragments by shard.
	fragments map[uint64]*fragment

	broadcaster  broadcaster
	stats        stats.StatsClient
	rowAttrStore AttrStore

	knownShards       *roaring.Bitmap
	knownShardsCopied uint32
}

// newView returns a new instance of View.
func newView(holder *Holder, path, index, field, name string, fieldOptions FieldOptions) *view {
	panicOn(validateName(name))

	return &view{
		path:          path,
		index:         index,
		field:         field,
		name:          name,
		qualifiedName: FormatQualifiedViewName(index, field, name),

		holder: holder,

		fieldType: fieldOptions.Type,
		cacheType: fieldOptions.CacheType,
		cacheSize: fieldOptions.CacheSize,

		fragments: make(map[uint64]*fragment),

		broadcaster: NopBroadcaster,
		stats:       stats.NopStatsClient,
		knownShards: roaring.NewSliceBitmap(),
	}
}

// addKnownShard adds a known shard to v, which you should only do when
// holding the lock -- but that's probably a given, since you're presumably
// calling it because you were potentially altering the shard list. Since
// you have the write lock, availableShards() can't be happening right now.
// Either it'll get the previous value or the next value of knownShards,
// and either is probably fine.
//
// This means that we only copy the (probably tiny) bitmap if we're
// modifying it after it's been read. If it never gets read, knownShardsCopied
// never changes. If it gets read, then we treat that one as immutable --
// we never modify it again, because the field code might be reading it, so
// we make a fresh copy. Since shards almost never change, the expected
// behavior is that we call addKnownShard a lot during initial startup,
// when knownShardsCopied is 0, and then after that calls to availableShards
// return that bitmap, and set knownShardsCopied to 1, but we rarely modify
// the list.
func (v *view) addKnownShard(shard uint64) {
	v.notifyIfNewShard(shard)
	if atomic.LoadUint32(&v.knownShardsCopied) == 1 {
		v.knownShards = v.knownShards.Clone()
		atomic.StoreUint32(&v.knownShardsCopied, 0)
	}
	_, err := v.knownShards.Add(shard)
	panicOn(err)
}

// removeKnownShard removes a known shard from v. See the notes on addKnownShard.
func (v *view) removeKnownShard(shard uint64) {
	if atomic.LoadUint32(&v.knownShardsCopied) == 1 {
		v.knownShards = v.knownShards.Clone()
		atomic.StoreUint32(&v.knownShardsCopied, 0)
	}
	_, _ = v.knownShards.Remove(shard)
}

// open opens and initializes the view.
func (v *view) open() error {
	if v.knownShards == nil {
		v.knownShards = roaring.NewSliceBitmap()
	}

	// Never keep a cache for field views.
	if strings.HasPrefix(v.name, viewBSIGroupPrefix) {
		v.cacheType = CacheTypeNone
	}

	if err := func() error {
		// Ensure the view's path exists.
		v.holder.Logger.Debugf("ensure view path exists: %s", v.path)
		err := os.MkdirAll(v.path, 0777)
		if err != nil {
			return errors.Wrap(err, "creating view directory")
		}
		err = os.MkdirAll(filepath.Join(v.path, "fragments"), 0777)
		if err != nil {
			return errors.Wrap(err, "creating fragments directory")
		}

		v.holder.Logger.Debugf("open fragments for index/field/view: %s/%s/%s", v.index, v.field, v.name)

		if err := v.openFragmentsInTx(); err != nil {
			return errors.Wrap(err, "opening fragments")
		}

		return nil
	}(); err != nil {
		v.close()
		return err
	}

	_ = testhook.Opened(v.holder.Auditor, v, nil)
	v.holder.Logger.Debugf("successfully opened index/field/view: %s/%s/%s", v.index, v.field, v.name)
	return nil
}

var workQueue = make(chan struct{}, runtime.NumCPU()*2)

// replaces v.openFragments() with Tx generic code.
func (v *view) openFragmentsInTx() error {

	// we think this is correct for dbpershard, but might be slower. TODO
	shards, err := DBPerShardGetShardsForIndex(v.idx, v.path)
	if err != nil {
		return errors.Wrap(err, "DBPerShardGetShardsForIndex()")
	}
	eg, ctx := errgroup.WithContext(context.Background())
	var mu sync.Mutex

	shardCh := make(chan uint64, len(shards))
	for i := range shards {
		shardCh <- shards[i]
	}

shardLoop:
	for range shards {
		select {
		case <-ctx.Done():
			break shardLoop
		default:

			workQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-workQueue
				}()

				var shard uint64
				select {
				case shard = <-shardCh:
				default:
					return nil // no more work
				}
				v.holder.Logger.Debugf("open index/field/view/fragment: %s/%s/%s/%d", v.index, v.field, v.name, shard)

				frag := v.newFragment(v.fragmentPath(shard), shard)
				if err := frag.Open(); err != nil {
					return fmt.Errorf("open fragment: shard=%d, err=%s", frag.shard, err)
				}
				frag.RowAttrStore = v.rowAttrStore
				v.holder.Logger.Debugf("add index/field/view/fragment to view.fragments: %s/%s/%s/%d", v.index, v.field, v.name, shard)
				mu.Lock()
				v.fragments[frag.shard] = frag
				v.addKnownShard(shard)
				mu.Unlock()
				return nil
			})
		}
	}
	return eg.Wait()
}

// close closes the view and its fragments.
func (v *view) close() error {
	v.mu.Lock()
	defer v.mu.Unlock()
	defer func() {
		_ = testhook.Closed(v.holder.Auditor, v, nil)
	}()

	// Close all fragments.
	eg, ctx := errgroup.WithContext(context.Background())
fragLoop:
	for _, loopFrag := range v.fragments {
		select {
		case <-ctx.Done():
			break fragLoop
		default:
			frag := loopFrag
			workQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-workQueue
				}()

				if err := frag.Close(); err != nil {
					return errors.Wrap(err, "closing fragment")
				}
				return nil
			})
		}
	}
	err := eg.Wait()
	v.fragments = make(map[uint64]*fragment)
	v.knownShards = nil
	return err
}

// flags returns a set of flags for the underlying fragments.
func (v *view) flags() byte {
	var flag byte
	if v.fieldType == FieldTypeInt || v.fieldType == FieldTypeDecimal {
		flag |= roaringFlagBSIv2
	}
	return flag
}

// availableShards returns a bitmap of shards which contain data.
func (v *view) availableShards() *roaring.Bitmap {
	// A read lock prevents anything with the write lock from being
	// active, so anything that's calling add/removeKnownShard won't
	// be doing it here. But we do need to indicate that we came
	// through, but we don't want to block on a write lock. So we
	// use an atomic for that.
	v.mu.RLock()
	defer v.mu.RUnlock()
	atomic.StoreUint32(&v.knownShardsCopied, 1)
	return v.knownShards
}

// fragmentPath returns the path to a fragment in the view.
func (v *view) fragmentPath(shard uint64) string {
	return filepath.Join(v.path, "fragments", strconv.FormatUint(shard, 10))
}

// Fragment returns a fragment in the view by shard.
func (v *view) Fragment(shard uint64) *fragment {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.fragments[shard]
}

// allFragments returns a list of all fragments in the view.
func (v *view) allFragments() []*fragment {
	v.mu.Lock()
	defer v.mu.Unlock()

	other := make([]*fragment, 0, len(v.fragments))
	for _, fragment := range v.fragments {
		other = append(other, fragment)
	}
	return other
}

// recalculateCaches recalculates the cache on every fragment in the view.
func (v *view) recalculateCaches() {
	for _, fragment := range v.allFragments() {
		fragment.RecalculateCache()
	}
}

// CreateFragmentIfNotExists returns a fragment in the view by shard.
func (v *view) CreateFragmentIfNotExists(shard uint64) (*fragment, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	// Find fragment in cache first.
	if frag := v.fragments[shard]; frag != nil {
		return frag, nil
	}

	// Initialize and open fragment.
	frag := v.newFragment(v.fragmentPath(shard), shard)
	if err := frag.Open(); err != nil {
		return nil, errors.Wrap(err, "opening fragment")
	}
	frag.RowAttrStore = v.rowAttrStore

	v.fragments[shard] = frag
	v.addKnownShard(shard)
	return frag, nil
}

func (v *view) notifyIfNewShard(shard uint64) {

	if v.knownShards.Contains(shard) { //checks the fields remoteShards bitmap to see if broadcast needed
		return
	}

	broadcastChan := make(chan struct{})

	go func() {
		msg := &CreateShardMessage{
			Index: v.index,
			Field: v.field,
			Shard: shard,
		}
		// Broadcast a message that a new max shard was just created.
		err := v.broadcaster.SendSync(msg)
		if err != nil {
			v.holder.Logger.Printf("broadcasting create shard: %v", err)
		}
		close(broadcastChan)
	}()

	// We want to wait until the broadcast is complete, but what if it
	// takes a really long time? So we time out.
	select {
	case <-broadcastChan:
	case <-time.After(50 * time.Millisecond):
		v.holder.Logger.Debugf("broadcasting create shard took >50ms")
	}
}

func (v *view) newFragment(path string, shard uint64) *fragment {

	if v.holder != nil && v.idx != nil {
		// A view must have its v.idx *Index registered with its holder.
		// Otherwise TestField_AvailableShards crashes, as one example.
		hIdx := v.holder.Index(v.idx.name)
		if hIdx == nil && v.idx != nil {
			v.holder.addIndexFromField(v.idx)
		}
	}

	frag := newFragment(v.holder, path, v.index, v.field, v.name, shard, v.flags())
	frag.CacheType = v.cacheType
	frag.CacheSize = v.cacheSize
	frag.stats = v.stats
	if v.fieldType == FieldTypeMutex {
		frag.mutexVector = newRowsVector(frag)
	} else if v.fieldType == FieldTypeBool {
		frag.mutexVector = newBoolVector(frag)
	}
	return frag
}

// deleteFragment removes the fragment from the view.
func (v *view) deleteFragment(shard uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	f := v.fragments[shard]
	if f == nil {
		return ErrFragmentNotFound
	}

	v.holder.Logger.Printf("delete fragment: (%s/%s/%s) %d", v.index, v.field, v.name, shard)

	idx := f.holder.Index(v.index)
	f.Close()
	if err := idx.Txf.DeleteFragmentFromStore(f.index, f.field, f.view, f.shard, f); err != nil {
		return errors.Wrap(err, "DeleteFragment")
	}
	delete(v.fragments, shard)
	v.removeKnownShard(shard)

	return nil
}

// row returns a row for a shard of the view.
func (v *view) row(txOrig Tx, rowID uint64) (*Row, error) {
	row := NewRow()
	for _, frag := range v.allFragments() {

		tx := txOrig
		if NilInside(tx) {
			tx = v.idx.Txf.NewTx(Txo{Write: !writable, Index: v.idx, Fragment: frag, Shard: frag.shard})
			defer tx.Rollback()
		}

		fr, err := frag.row(tx, rowID)
		if err != nil {
			return nil, err
		} else if fr == nil {
			continue
		}
		row.Merge(fr)
	}
	return row, nil

}

// setBit sets a bit within the view.
func (v *view) setBit(txOrig Tx, rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	var frag *fragment
	frag, err = v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.Txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				panicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}
	return frag.setBit(tx, rowID, columnID)
}

// clearBit clears a bit within the view.
func (v *view) clearBit(txOrig Tx, rowID, columnID uint64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag := v.Fragment(shard)
	if frag == nil {
		return false, nil
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.Txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				panicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}

	return frag.clearBit(tx, rowID, columnID)
}

// value uses a column of bits to read a multi-bit value.
func (v *view) value(txOrig Tx, columnID uint64, bitDepth uint) (value int64, exists bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return value, exists, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = frag.idx.Txf.NewTx(Txo{Write: !writable, Index: frag.idx, Fragment: frag, Shard: frag.shard})
		defer tx.Rollback()
	}

	return frag.value(tx, columnID, bitDepth)
}

// setValue uses a column of bits to set a multi-bit value.
func (v *view) setValue(txOrig Tx, columnID uint64, bitDepth uint, value int64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return changed, err
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.Txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				panicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}

	return frag.setValue(tx, columnID, bitDepth, value)
}

// clearValue removes a specific value assigned to columnID
func (v *view) clearValue(txOrig Tx, columnID uint64, bitDepth uint, value int64) (changed bool, err error) {
	shard := columnID / ShardWidth
	frag := v.Fragment(shard)
	if frag == nil {
		return false, nil
	}

	tx := txOrig
	if NilInside(tx) {
		tx = v.idx.Txf.NewTx(Txo{Write: writable, Index: v.idx, Fragment: frag, Shard: shard})
		defer func() {
			if err == nil {
				panicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}
	return frag.clearValue(tx, columnID, bitDepth, value)
}

// rangeOp returns rows with a field value encoding matching the predicate.
func (v *view) rangeOp(qcx *Qcx, op pql.Token, bitDepth uint, predicate int64) (_ *Row, err0 error) {
	r := NewRow()
	for _, frag := range v.allFragments() {

		tx, finisher := qcx.GetTx(Txo{Write: !writable, Index: v.idx, Shard: frag.shard})
		defer finisher(&err0)

		other, err := frag.rangeOp(tx, op, bitDepth, predicate)
		if err != nil {
			return nil, err
		}
		r = r.Union(other)
	}
	return r, nil
}

// upgradeViewBSIv2 upgrades the fragments of v. Returns ok true if any fragment upgraded.
func upgradeViewBSIv2(v *view, bitDepth uint) (ok bool, _ error) {
	// If reading from an old formatted BSI roaring bitmap, upgrade and reload.
	for _, frag := range v.allFragments() {
		if frag.storage.Flags&roaringFlagBSIv2 == 1 {
			continue // already upgraded, skip
		}
		ok = true // mark as upgraded, requires reload

		if tmpPath, err := upgradeRoaringBSIv2(frag, bitDepth); err != nil {
			return ok, errors.Wrap(err, "upgrading bsi v2")
		} else if err := frag.closeStorage(); err != nil {
			return ok, errors.Wrap(err, "closing after bsi v2 upgrade")
		} else if err := os.Rename(tmpPath, frag.path); err != nil {
			return ok, errors.Wrap(err, "renaming after bsi v2 upgrade")
		} else if err := frag.openStorage(true); err != nil {
			return ok, errors.Wrap(err, "re-opening after bsi v2 upgrade")
		}
	}
	return ok, nil
}

// ViewInfo represents schema information for a view.
type ViewInfo struct {
	Name string `json:"name"`
}

type viewInfoSlice []*ViewInfo

func (p viewInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p viewInfoSlice) Len() int           { return len(p) }
func (p viewInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FormatQualifiedViewName generates a qualified name for the view to be used with Tx operations.
func FormatQualifiedViewName(index, field, view string) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00", index, field, view)
}
