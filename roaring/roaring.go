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

// Package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math/bits"
	"reflect"
	"sort"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// MagicNumber is an identifier, in bytes 0-1 of the file.
	MagicNumber = uint32(12348)

	// storageVersion indicates the storage version, in byte 2.
	storageVersion = uint32(0)

	// NOTE: byte 3 stores user-defined flags.

	// cookie is the first 3 bytes in a roaring bitmap file,
	// formed by joining MagicNumber and storageVersion
	cookie = MagicNumber + storageVersion<<16

	// headerBaseSize is the size in bytes of the cookie, flags, and key count
	// at the beginning of a file.
	headerBaseSize = 3 + 1 + 4

	// runCountHeaderSize is the size in bytes of the run count stored
	// at the beginning of every serialized run container.
	runCountHeaderSize = 2

	// interval16Size is the size of a single run in a container.runs.
	interval16Size = 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64

	maxContainerVal = 0xffff

	// maxContainerKey is the key representing the last container in a full row.
	// It is the full bitmap space (2^64) divided by container width (2^16).
	maxContainerKey = (1 << 48) - 1
)

const (
	containerNil    byte = iota // no container
	containerArray              // slice of bit position values
	containerBitmap             // slice of 1024 uint64s
	containerRun                // container of run-encoded bits
)

// map used for a more descriptive print
var containerTypeNames = map[byte]string{
	containerArray:  "array",
	containerBitmap: "bitmap",
	containerRun:    "run",
}

var fullContainer = NewContainerRun([]interval16{{start: 0, last: maxContainerVal}}).Freeze()

// AdvisoryError is used for the special case where we probably want to *report*
// an error reading a file, but don't want to actually count the file as not
// being read. For instance, a partial ops-log entry is *probably* harmless;
// we probably crashed while writing (?) and as such didn't report the write
// as successful. We hope.
type AdvisoryError interface {
	error
	AdvisoryOnly()
}

type advisoryError struct {
	e error
}

func (a advisoryError) Error() string {
	return a.e.Error()
}

// This marks the error as safe to ignore.
func (a advisoryError) AdvisoryOnly() {
}

type FileShouldBeTruncatedError interface {
	AdvisoryError
	SuggestedLength() int64
}

type fileShouldBeTruncatedError struct {
	advisoryError
	offset int64
}

func (f *fileShouldBeTruncatedError) SuggestedLength() int64 {
	return f.offset
}

func newFileShouldBeTruncatedError(err error, offset int64) *fileShouldBeTruncatedError {
	return &fileShouldBeTruncatedError{advisoryError: advisoryError{e: err}, offset: offset}
}

type Containers interface {
	// Get returns nil if the key does not exist.
	Get(key uint64) *Container

	// Put adds the container at key.
	Put(key uint64, c *Container)

	// PutContainerValues updates an existing container at key.
	// If a container does not exist for key, a new one is allocated.
	// TODO(2.0) make n  int32
	PutContainerValues(key uint64, typ byte, n int, mapped bool)

	// Remove takes the container at key out.
	Remove(key uint64)

	// GetOrCreate returns the container at key, creating a new empty container if necessary.
	GetOrCreate(key uint64) *Container

	// Clone does a deep copy of Containers, including cloning all containers contained.
	Clone() Containers

	// Freeze creates a shallow copy of Containers, freezing all the containers
	// contained. The new copy is a distinct Containers, but the individual containers
	// are shared (but marked as frozen).
	Freeze() Containers

	// Last returns the highest key and associated container.
	Last() (key uint64, c *Container)

	// Size returns the number of containers stored.
	Size() int

	// Update calls fn (existing-container, existed), and expects
	// (new-container, write). If write is true, the container is used to
	// replace the given container.
	Update(key uint64, fn func(*Container, bool) (*Container, bool))

	// UpdateEvery calls fn (existing-container, existed), and expects
	// (new-container, write). If write is true, the container is used to
	// replace the given container.
	UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool))

	// Iterator returns a ContainterIterator which after a call to Next(), a call to Value() will
	// return the first container at or after key. found will be true if a
	// container is found at key.
	Iterator(key uint64) (citer ContainerIterator, found bool)

	Count() uint64

	// Reset clears the containers collection to allow for recycling during snapshot
	Reset()
	// ResetN clears the collection but hints at a needed size.
	ResetN(int)

	// Repair will repair the cardinality of any containers whose cardinality were corrupted
	// due to optimized operations.
	Repair()
}

type ContainerIterator interface {
	Next() bool
	Value() (uint64, *Container)
}

// Bitmap represents a roaring bitmap.
type Bitmap struct {
	Containers Containers
	Source     Source

	// User-defined flags.
	Flags byte
	// should we try to keep things mapped?
	preferMapping bool

	// Number of bit change operations written to the writer. Some operations
	// contain multiple values, so "ops" represents the number of distinct
	// operations, while "opN" represents expected bit changes.
	ops int
	opN int

	// Writer where operations are appended to.
	OpWriter io.Writer
}

// NewBitmap returns a Bitmap with an initial set of values.
func NewBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	// We have no way to report this. We aren't in a server context
	// so we haven't got a logger, nothing is checking for nil returns
	// from this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause AddN() to return an error.
	// Therefore, it's safe to swallow this error.
	_, _ = b.AddN(a...)
	return b
}

// NewSliceBitmap makes a new bitmap, explicitly selecting the slice containers
// type, which performs better in cases where we expect a contiguous block of
// containers added in ascending order, such as when extracting a range from
// another bitmap.
func NewSliceBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	// We have no way to report this. We aren't in a server context
	// so we haven't got a logger, nothing is checking for nil returns
	// from this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause AddN() to return an error.
	// Therefore, it's safe to swallow this error.
	_, _ = b.AddN(a...)
	return b
}

// NewFileBitmap returns a Bitmap with an initial set of values, used for file storage.
// By default, this is a copy of NewBitmap, but is replaced with B+Tree in server/enterprise.go
var NewFileBitmap func(a ...uint64) *Bitmap = NewBTreeBitmap

// Clone returns a heap allocated copy of the bitmap.
// Note: The OpWriter IS NOT copied to the new bitmap.
func (b *Bitmap) Clone() *Bitmap {
	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		Containers: b.Containers.Clone(),
	}

	return other
}

// Freeze returns a shallow copy of the bitmap. The new bitmap
// is a distinct bitmap, with a new Containers object, but the
// actual containers it holds are the same as the parent's
// containers, but have been frozen.
func (b *Bitmap) Freeze() *Bitmap {
	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		Containers: b.Containers.Freeze(),
		Source:     b.Source,
	}

	return other
}

// Add adds values to the bitmap. TODO(2.0) deprecate - use the more general
// AddN (though be aware that it modifies 'a' in place).
func (b *Bitmap) Add(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeAdd, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply to the in-memory bitmap.
		if b.DirectAdd(v) {
			changed = true
		}
	}

	return changed, nil
}

// AddN adds values to the bitmap, appending them all to the op log in a batched
// write. It returns the number of changed bits.
func (b *Bitmap) AddN(a ...uint64) (changed int, err error) {
	if len(a) == 0 {
		return 0, nil
	}

	changed = b.DirectAddN(a...) // modifies a in-place

	if b.OpWriter != nil && changed > 0 {
		op := &op{
			typ:    opTypeAddBatch,
			values: a[:changed],
		}
		if err := b.writeOp(op); err != nil {
			b.DirectRemoveN(op.values...) // reset data since we're returning an error
			return 0, errors.Wrap(err, "writing to op log")
		}
	}

	return changed, nil
}

// DirectAddN sets multiple bits in the bitmap, returning how many changed. It
// modifies the slice 'a' in place such that once it's complete a[:changed] will
// be list of changed bits. It is more efficient than repeated calls to
// DirectAdd for semi-dense sorted data because it reuses the container from the
// previous value if the new value has the same highbits instead of looking it
// up each time. TODO: if Containers implementations cached the last few
// Container objects returned from calls like Get and GetOrCreate, this
// optimization would be less useful.
func (b *Bitmap) DirectAddN(a ...uint64) (changed int) {
	return b.directOpN((*Container).add, a...)
}

// DirectRemoveN behaves analgously to DirectAddN.
func (b *Bitmap) DirectRemoveN(a ...uint64) (changed int) {
	return b.directOpN((*Container).remove, a...)
}

// directOpN contains the logic for DirectAddN and DirectRemoveN. Theoretically,
// it could be used by anything that wanted to apply a boolean-returning
// container level operation across a list of values and return the number of
// trues while modifying the list of values in place to contain the
// true-returning values in order.
func (b *Bitmap) directOpN(op func(c *Container, v uint16) (*Container, bool), a ...uint64) (changed int) {
	hb := uint64(0xFFFFFFFFFFFFFFFF) // impossible sentinel value
	var cont *Container
	for _, v := range a {
		if newhb := highbits(v); newhb != hb {
			hb = newhb
			cont = b.Containers.GetOrCreate(hb)
		}
		newC, change := op(cont, lowbits(v))
		if change {
			a[changed] = v
			changed++
		}
		if newC != cont {
			b.Containers.Put(hb, newC)
			cont = newC
		}
	}
	return changed
}

// DirectAdd adds a value to the bitmap by bypassing the op log. TODO(2.0)
// deprecate in favor of DirectAddN.
func (b *Bitmap) DirectAdd(v uint64) bool {
	cont := b.Containers.GetOrCreate(highbits(v))
	newC, changed := cont.add(lowbits(v))
	if newC != cont {
		b.Containers.Put(highbits(v), newC)
	}
	return changed
}

// Contains returns true if v is in the bitmap.
func (b *Bitmap) Contains(v uint64) bool {
	c := b.Containers.Get(highbits(v))
	if c == nil {
		return false
	}
	return c.Contains(lowbits(v))
}

// Remove removes values from the bitmap (writing to the op log if available).
// TODO(2.0) deprecate - use the more general RemoveN (though be aware that it
// modifies 'a' in place).
func (b *Bitmap) Remove(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeRemove, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply operation to the bitmap.
		if op.apply(b) {
			changed = true
		}
	}
	return changed, nil
}

// RemoveN behaves analagously to AddN.
func (b *Bitmap) RemoveN(a ...uint64) (changed int, err error) {
	if len(a) == 0 {
		return 0, nil
	}

	changed = b.DirectRemoveN(a...) // modifies a in-place

	if b.OpWriter != nil && changed > 0 {
		op := &op{
			typ:    opTypeRemoveBatch,
			values: a[:changed],
		}
		if err := b.writeOp(op); err != nil {
			b.DirectAddN(op.values...) // reset data since we're returning an error
			return 0, errors.Wrap(err, "writing to op log")
		}
	}

	return changed, nil
}

func (b *Bitmap) remove(v uint64) bool {
	c := b.Containers.Get(highbits(v))
	newC, changed := c.remove(lowbits(v))
	if newC != c {
		if newC != nil {
			b.Containers.Put(highbits(v), newC)
		} else {
			b.Containers.Remove(highbits(v))
		}
	}
	return changed
}

// Min returns the lowest value in the bitmap.
// Second return value is true if containers exist in the bitmap.
func (b *Bitmap) Min() (uint64, bool) {
	v, eof := b.Iterator().Next()
	return v, !eof
}

// MinAt returns the lowest value in the bitmap at least equal to its argument.
// Second return value is true if containers exist in the bitmap.
func (b *Bitmap) MinAt(start uint64) (uint64, bool) {
	v, eof := b.IteratorAt(start).Next()
	return v, !eof
}

// Max returns the highest value in the bitmap.
// Returns zero if the bitmap is empty.
func (b *Bitmap) Max() uint64 {
	if b.Containers.Size() == 0 {
		return 0
	}

	hb, c := b.Containers.Last()
	lb := c.max()
	return hb<<16 | uint64(lb)
}

// Count returns the number of bits set in the bitmap.
func (b *Bitmap) Count() (n uint64) {
	return b.Containers.Count()
}

// Any returns "b.Count() > 0"... but faster than doing that.
func (b *Bitmap) Any() bool {
	iter, _ := b.Containers.Iterator(0)
	// TODO (jaffee) I'm not sure if it's possible/legal to have an empty
	// container, so this loop may be totally unnecessary. In theory, any empty
	// container should be removed from the bitmap though.
	for iter.Next() {
		_, c := iter.Value()
		if c.N() > 0 {
			return true
		}
	}
	return false
}

// Size returns the number of bytes required for the bitmap.
func (b *Bitmap) Size() int {
	numbytes := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		numbytes += c.size()
	}
	return numbytes
}

// CountRange returns the number of bits set between [start, end).
func (b *Bitmap) CountRange(start, end uint64) (n uint64) {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}

	if b.Containers.Size() == 0 {
		return
	}

	skey := highbits(start)
	ekey := highbits(end)

	citer, found := b.Containers.Iterator(highbits(start))
	// If range is entirely in one container then just count that range.
	if found && skey == ekey {
		citer.Next()
		_, c := citer.Value()
		return uint64(c.countRange(int32(lowbits(start)), int32(lowbits(end))))
	}

	for citer.Next() {
		k, c := citer.Value()
		if k < skey {
			// TODO remove once we've validated this stuff works
			panic("should be impossible for k to be less than skey")
		}

		// k > ekey handles the case when start > end and where start and end
		// are in different containers. Same container case is already handled above.
		if k > ekey {
			break
		}
		if k == skey {
			n += uint64(c.countRange(int32(lowbits(start)), maxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.N())
			continue
		}
		if k == ekey {
			n += uint64(c.countRange(0, int32(lowbits(end))))
			break
		}
	}
	return n
}

// Slice returns a slice of all integers in the bitmap.
func (b *Bitmap) Slice() []uint64 {
	var a []uint64
	itr := b.Iterator()
	itr.Seek(0)

	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// SliceRange returns a slice of integers between [start, end).
func (b *Bitmap) SliceRange(start, end uint64) []uint64 {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("getting slice in range but %v > %v", start, end))
		}
	}
	var a []uint64
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// ForEach executes fn for each value in the bitmap.
func (b *Bitmap) ForEach(fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		fn(v)
	}
}

// ForEachRange executes fn for each value in the bitmap between [start, end).
func (b *Bitmap) ForEachRange(start, end uint64, fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		fn(v)
	}
}

// OffsetRange returns a new bitmap with a containers offset by start.
// The containers themselves are shared, so they get frozen so it will
// be safe to interact with them.
func (b *Bitmap) OffsetRange(offset, start, end uint64) *Bitmap {
	if lowbits(offset) != 0 {
		panic("offset must not contain low bits")
	}
	if lowbits(start) != 0 {
		panic("range start must not contain low bits")
	}
	if lowbits(end) != 0 {
		panic("range end must not contain low bits")
	}

	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(end)
	citer, _ := b.Containers.Iterator(hi0)
	other := NewSliceBitmap()
	mappedAny := false
	for citer.Next() {
		k, c := citer.Value()
		if k >= hi1 {
			break
		}
		if c.Mapped() {
			mappedAny = true
		}
		other.Containers.Put(off+(k-hi0), c.Freeze())
	}
	// if b.Source != nil && mappedAny {
	if b.Source != nil && (generationDebug || mappedAny) {
		other.Source = b.Source
	}
	return other
}

// container returns the container with the given key.
func (b *Bitmap) container(key uint64) *Container {
	return b.Containers.Get(key)
}

// IntersectionCount returns the number of set bits that would result in an
// intersection between b and other. It is more efficient than actually
// intersecting the two and counting the result.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	var n uint64
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else {
			n += uint64(intersectionCount(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return n
}

// Intersect returns the intersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	output := NewBitmap()
	usedB, usedOther := false, false
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			newC := intersect(ci, cj)
			if newC == ci {
				usedB = true
			}
			if newC == cj {
				usedOther = true
			}
			output.Containers.Put(ki, newC)
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	switch {
	case usedB && usedOther:
		output.Source = MergeSources(b.Source, other.Source)
	case usedB:
		output.Source = b.Source
	case usedOther:
		output.Source = other.Source
	}
	return output
}

type mutableContainersIterator struct {
	c Containers

	cit ContainerIterator
	sit *sliceIterator
}

func newMutableContainersIterator(cs Containers, key uint64) *mutableContainersIterator {
	it := &mutableContainersIterator{c: cs}

	if sc, ok := cs.(*sliceContainers); ok {
		if i, found := sc.seek(key); found {
			it.sit = &sliceIterator{e: sc, i: i, index: i}
			return it
		}
	}

	it.cit, _ = cs.Iterator(key)
	return it
}

func (it *mutableContainersIterator) update(key uint64, newCont *Container) {
	if it.sit != nil {
		if key == it.sit.key {
			it.sit.e.containers[it.sit.index] = newCont
		}
	}

	it.c.Update(key, func(_ *Container, _ bool) (*Container, bool) {
		return newCont, true
	})
}

func (it *mutableContainersIterator) Next() bool {
	if it.sit != nil {
		return it.sit.Next()
	}

	return it.cit.Next()
}

func (it *mutableContainersIterator) Value() (uint64, *Container) {
	if it.sit != nil {
		return it.sit.Value()
	}

	return it.cit.Value()
}

// IntersectInPlace returns the bitwise intersection of b and others,
// modifying b in place.
func (b *Bitmap) IntersectInPlace(others ...*Bitmap) {
	var bSize int
	if bSize = b.Size(); bSize == 0 {
		// If b doesn't have any containers then return early.
		return
	}

	otherIters := make(handledIters, 0, len(others))
	for _, other := range others {
		it, _ := other.Containers.Iterator(0)
		if !it.Next() {
			// An empty bitmap - reset all
			b.Containers.Reset()
			return
		}

		otherIters = append(otherIters, handledIter{
			iter:    it,
			hasNext: true,
		})
	}

	bIter := newMutableContainersIterator(b.Containers, 0)
	for bIter.Next() {
		bKey, bCont := bIter.Value()
		if bCont.N() == 0 {
			// No point in intersecting things from an empty container.
			bIter.update(bKey, nil)
			continue
		}

		// Loop until every iters current value has been handled.
		for _, otherIter := range otherIters {
			if !otherIter.hasNext {
				continue
			}

			otherKey, otherCont := otherIter.iter.Value()
			for otherKey < bKey {
				otherIter.hasNext = otherIter.iter.Next()
				if !otherIter.hasNext {
					break
				}
				otherKey, otherCont = otherIter.iter.Value()
			}

			if bKey == otherKey {
				// Note: a nil container is valid, and has N == 0.
				if otherCont.N() != 0 {
					if bCont.frozen() {
						bCont = bCont.Clone()
						b.Containers.Put(bKey, bCont)
					}
					bCont = bCont.intersectInPlace(otherCont)
					bIter.update(bKey, bCont)
					if bCont == nil || bCont.N() == 0 {
						break
					}

					otherIter.hasNext = otherIter.iter.Next()
					continue
				}
			}

			bIter.update(bKey, nil)
			break
		}
	}

	b.Containers.Repair()
}

func (c *Container) intersectInPlace(other *Container) *Container {
	// short-circuit the trivial cases
	if c == nil || other == nil || c.N() == 0 || other.N() == 0 {
		c = nil
		return c
	}
	cFull, otherFull := (c.N() == maxContainerVal+1), (other.N() == maxContainerVal+1)
	if cFull && otherFull {
		return c
	}
	if cFull {
		return c.copyInPlace(other)
	}
	if otherFull {
		return c
	}

	switch c.typ() {
	case containerArray:
		switch other.typ() {
		case containerArray:
			return intersectArrayArrayInPlace(c, other)
		case containerBitmap:
			return intersectArrayBitmapInPlace(c, other)
		case containerRun:
			return intersectArrayRunInPlace(c, other)
		}

	case containerBitmap:
		switch other.typ() {
		case containerArray:
			return intersectBitmapArrayInPlace(c, other)
		case containerBitmap:
			return intersectBitmapBitmapInPlace(c, other)
		case containerRun:
			return intersectBitmapRunInPlace(c, other)
		}

	case containerRun:
		switch other.typ() {
		case containerArray:
			return intersectRunArrayInPlace(c, other)
		case containerBitmap:
			return intersectRunBitmapInPlace(c, other)
		case containerRun:
			return intersectRunRunInPlace(c, other)
		}
	}

	if roaringParanoia {
		panic(fmt.Sprintf("invalid intersect op: unknown types %d/%d", c.typ(), other.typ()))
	}
	return nil
}

func (c *Container) copyInPlace(other *Container) *Container {
	switch other.typ() {
	case containerArray:
		c.setTyp(containerArray)
		c.setArrayMaybeCopy(other.array(), true)
		c.setN(other.N())

	case containerBitmap:
		bmp := make([]uint64, bitmapN)
		copy(bmp, other.bitmap())
		c.setTyp(containerBitmap)
		c.setBitmap(bmp)
		c.setN(other.N())

	case containerRun:
		c.setTyp(containerRun)
		c.setRunsMaybeCopy(other.runs(), true)
		c.setN(other.N())
	}

	return c
}

func intersectArrayArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayArray")

	a = a.Thaw()
	aa, ba := a.array(), b.array()
	an, bn := len(aa), len(ba)
	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := aa[i], ba[j]

		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			aa[n] = va
			n, i, j = n+1, i+1, j+1
		}
	}
	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectArrayRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayRun")

	a = a.Thaw()
	aa, br := a.array(), b.runs()
	an, bn := len(aa), len(br)

	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := aa[i], br[j]
		if va < vb.start {
			i++
		} else if va > vb.last {
			j++
		} else {
			aa[n] = va
			n++
			i++
		}
	}

	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectArrayBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayBitmap")

	a = a.Thaw()
	aa := a.array()
	bb := b.bitmap()

	n := 0
	for _, va := range aa {
		b := bb[va>>6]
		bidx := va % 64
		mask := uint64(1) << bidx
		if b&mask > 0 {
			aa[n] = va
			n++
		}
	}

	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectBitmapBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapBitmap")

	a = a.Thaw()
	ab := a.bitmap()[:bitmapN]
	bb := b.bitmap()[:bitmapN]

	n := int32(0)
	for i := 0; i < bitmapN; i += 4 {
		// unrolling is still effective in go
		ptr := (*[4]uint64)(unsafe.Pointer(&bb[i]))
		ab[i] &= ptr[0]
		ab[i+1] &= ptr[1]
		ab[i+2] &= ptr[2]
		ab[i+3] &= ptr[3]

		n += int32(popcount(ab[i])) +
			int32(popcount(ab[i+1])) +
			int32(popcount(ab[i+2])) +
			int32(popcount(ab[i+3]))
	}
	a.setN(n)

	return a
}

func intersectBitmapArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapArray")

	a = a.Thaw()
	ab := a.bitmap()
	ba := b.array()

	bn := len(ba)
	array := make([]uint16, bn)
	n := int32(0)
	for _, vb := range ba {
		i := vb >> 6
		mask := uint64(1) << uint(vb%64)
		if ab[i]&mask > 0 {
			array[n] = vb
			n++
		}
	}
	array = array[:n]
	a.setTyp(containerArray)
	a.setArray(array)
	a.setN(n)

	return a
}

func intersectBitmapRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapRun")

	a = a.Thaw()
	ab := a.bitmap()
	br := b.runs()
	an := len(ab)
	bitmap := make([]uint64, an)

	n := int32(0)
	for _, vb := range br {
		i := vb.start >> 6 // index into a

		vastart := i << 6
		valast := vastart + 63
		for valast >= vb.start && vastart <= vb.last && int(i) < an {
			if vastart >= vb.start && valast <= vb.last { // a within b
				bitmap[i] = ab[i]
				n += int32(popcount(ab[i]))
			} else if vb.start >= vastart && vb.last <= valast { // b within a
				var mask uint64 = ((1 << (vb.last - vb.start + 1)) - 1) << (vb.start - vastart)
				bits := ab[i] & mask
				bitmap[i] |= bits
				n += int32(popcount(bits))
			} else if vastart < vb.start { // a overlaps front of b
				offset := 64 - (1 + valast - vb.start)
				bits := (ab[i] >> offset) << offset
				bitmap[i] |= bits
				n += int32(popcount(bits))
			} else if vb.start < vastart { // b overlaps front of a
				offset := 64 - (1 + vb.last - vastart)
				bits := (ab[i] << offset) >> offset
				bitmap[i] |= bits
				n += int32(popcount(bits))
			}

			i++
			vastart = i << 6
			valast = vastart + 63
		}
	}
	a.setBitmap(bitmap)
	a.setN(n)

	return a
}

func intersectRunRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunRun")

	a = a.Thaw()
	ar, br := a.runs(), b.runs()
	an, bn := len(ar), len(br)

	var runs []interval16
	if an > bn {
		runs = make([]interval16, 0, an)
	} else {
		runs = make([]interval16, 0, bn)
	}

	n := int32(0)
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := ar[i], br[j]

		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if vb.last < va.start {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			runs = append(runs, interval16{start: va.start, last: vb.last})
			n += int32(vb.last-va.start) + 1
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			runs = append(runs, vb)
			n += int32(vb.last-vb.start) + 1
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			runs = append(runs, va)
			n += int32(va.last-va.start) + 1
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			runs = append(runs, interval16{start: vb.start, last: va.last})
			n += int32(va.last-vb.start) + 1
			i++
		}
	}
	a.setRuns(runs)
	a.setN(n)

	return a
}

func intersectRunArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunArray")

	a = a.Thaw()
	ar, ba := a.runs(), b.array()
	an, bn := len(ar), len(ba)

	array := make([]uint16, bn)
	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := ar[i], ba[j]
		if vb < va.start {
			j++
		} else if vb > va.last {
			i++
		} else {
			array[n] = vb
			n++
			j++
		}
	}

	array = array[:n]
	a.setTyp(containerArray)
	a.setArray(array)

	return a
}

func intersectRunBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunBitmap")

	// TODO(@kuba--):
	// Figure out how to efficiently intersect dense runs with bitmaps.
	// So far we convert run to bitmap and intersect bitmaps - it's much faster
	// than naive O(n^2) algorithm.
	a = a.runToBitmap()
	return intersectBitmapBitmapInPlace(a, b)
}

// Union returns the bitwise union of b and others as a new bitmap.
func (b *Bitmap) Union(others ...*Bitmap) *Bitmap {
	if len(others) == 1 {
		output := NewBitmap()
		b.unionIntoTargetSingle(output, others[0])
		return output
	}
	// It may seem counterintuitive to freeze this, but the result is
	// a new bitmap which can be safely modified, but postponing any
	// allocations until an actual write to any given container.
	output := b.Freeze()
	output.UnionInPlace(others...)
	return output
}

// UnionInPlace returns the bitwise union of b and others, modifying
// b in place.
func (b *Bitmap) UnionInPlace(others ...*Bitmap) {
	b.unionInPlace(others...)
}

func (b *Bitmap) unionIntoTargetSingle(target *Bitmap, other *Bitmap) {
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	usedB, usedOther := false, false
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			target.Containers.Put(ki, ci.Freeze())
			usedB = true
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			target.Containers.Put(kj, cj.Freeze())
			usedOther = true
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			newC := union(ci, cj)
			target.Containers.Put(ki, newC)
			if newC == ci {
				usedB = true
			}
			if newC == cj {
				usedOther = true
			}
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	switch {
	case usedB && usedOther:
		target.Source = MergeSources(b.Source, other.Source)
	case usedB:
		target.Source = b.Source
	case usedOther:
		target.Source = other.Source
	}
}

// unionInPlace stores the union of b and others into b. The others will
// be left unchanged.
//
// This function performs an n-way union of n bitmaps. It performs this in an
// optimized manner looping through all the bitmaps and performing unions one
// container at a time. As a result, instead of generating many intermediary
// containers for each union operation for a given container key, only one
// new container needs to be allocated (or re-used) regardless of how many bitmaps
// participate in the union. This significantly reduces allocations. In addition,
// because we perform the unions one container at a time across all the bitmaps, we
// can calculate summary statistics that allow us to make more efficient decisions
// up front. For instance, if we have a non-bitmap target container, but we
// expect more than ArrayMaxSize bits, we can convert to bitmap preemptively.
// This will sometimes be wrong (we can't really tell how many bits we'll have
// after a union) but is probably close enough to be useful. This will save
// some reallocations for cases where several consecutive ops have array
// representations, and we expect to have to convert to a bitmap eventually;
// we don't allocate larger and larger array slices before doing that.
//
// An additional optimization that this function makes is that it recognizes that even when
// CPU support is present, performing the popcount() operation isn't free. Imagine a scenario
// where 10 bitset containers are being unioned together one after the next. If every
// bitset<->bitset union operation needs to keep the containers' cardinality up to date, then
// the algorithm will waste a lot of time performing intermediary popcount() operations that
// will immediately be invalidated by the next union operation. As a result, we allow the cardinality
// of containers to degrade when we perform the in-place union operations, and then when the algorithm
// completes we "repair" all the containers by performing the popcount() operation one time. This means
// that we only ever have to do O(1) popcount operations per container instead of O(n) where n is the
// number of containers with the same key that are being unioned together.
//
// The algorithm works by iterating through all of the containers in all of the bitmaps concurrently.
// At every "tick" of the outermost loop, we increment our pointer into the bitmaps list of containers
// by 1 (if we haven't reached the end of the containers for that bitmap.)
//
// We then loop through all of the "current" values(containers) for all of the bitmaps
// and for each container with a specific key that we encounter, we scan forward to see if any of the
// other bitmaps have a container for the same key. If so, we calculate some summary statistics and
// then use that information to make a decision about how to union all of the containers with the same
// key together, perform the union, mark the unioned containers as "handled" and then move on to the next
// batch of containers that share the same key.
//
// We repeat this process until every single bitmaps current container has been "handled". Then we start the
// outer loop over again and the process repeats until we've iterated through every container in every bitmap
// and unioned everything into a single target bitmap.
//
// The diagram below shows the iteration state of four different bitmaps as the algorithm progresses them.
// The diagrams should BE interpreted from left -> right, top -> bottom. The X's represent a container in
// the bitmap at a specific key,  ^ symbol represents the bitmaps current container iteration position,
// and the - symbol represents a container that is at the current iteration position, but has been marked as "handled".
//
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//              ^                             |          _                             |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 2 |_______X________X______X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//                  ^                         |              ^                         |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//                  ^                         |              ^                         |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//              ^                             |          _                             |
// ------------------------------------------------------------------------------------------------------------------------
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//              _                             |                       ^                |                       _
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 2 |_______X_______________X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//                  _                         |                              ^         |                              ^
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//                  _                         |                                        |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//              _
func (b *Bitmap) unionInPlace(others ...*Bitmap) {
	const staticSize = 20
	var (
		requiredSliceSize = len(others)
		// To avoid having to allocate a slice every time, if the number of bitmaps
		// being unioned is small enough (i.e. smaller than staticSize), we can just
		// use this stack-allocated array.
		staticHandledIters = [staticSize]handledIter{}
		bitmapIters        handledIters
		target             = b
	)

	if requiredSliceSize <= staticSize {
		bitmapIters = staticHandledIters[:0]
	} else {
		bitmapIters = make(handledIters, 0, requiredSliceSize)
	}

	var sources []Source
	if b.Source != nil {
		sources = append(sources, b.Source)
	}
	for _, other := range others {
		if other.Source != nil {
			sources = append(sources, other.Source)
		}
		otherIter, _ := other.Containers.Iterator(0)
		if otherIter.Next() {
			bitmapIters = append(bitmapIters, handledIter{
				iter:    otherIter,
				hasNext: true,
				handled: false,
			})
		}
	}
	// new bitmap might have containers from any of those bitmaps in it
	b.Source = MergeSources(sources...)

	// Loop until we've exhausted every iter.
	hasNext := true
	for hasNext {
		// Loop until every iters current value has been handled.
		for i, iIter := range bitmapIters {
			if !iIter.hasNext || iIter.handled {
				// Either we've exhausted this iter (it has no more containers), or
				// we've already handled the current container by unioning it with
				// one of the containers we encountered earlier.
				continue
			}

			iKey, iContainer := iIter.iter.Value()
			expectedN := int64(0)

			// determine whether we have a target to union into.
			tContainer := target.Containers.Get(iKey)
			// if the target's full, short-circuit out.
			if tContainer != nil {
				if tContainer.N() == maxContainerVal+1 {
					bitmapIters.markItersWithKeyAsHandled(i, iKey)
					continue
				}
				expectedN = int64(tContainer.N())
			}
			// Check i and later iters for any max-range containers, and
			// find out how many there are.
			summaryStats := bitmapIters[i:].calculateSummaryStats(iKey)
			if summaryStats.hasMaxRange {
				// One (or more) of the containers represented the maximum possible
				// range that a container can store, so instead of calculating a
				// union we can generate an RLE container that represents the entire
				// range.
				tContainer = fullContainer
				target.Containers.Put(iKey, tContainer)
				bitmapIters.markItersWithKeyAsHandled(i, iKey)
				continue
			}
			expectedN += summaryStats.n
			var itersToUnion handledIters
			// Overview: We know that we have at least one "other" container
			// to union in, and we may have a target container already. We want
			// to shortcut easy cases ("no target container, exactly one
			// other container").
			if tContainer == nil {
				// No existing target container.
				if summaryStats.c == 1 {
					// There's no target and we have only one container, we
					// can just reuse it instead of unioning.
					statsHit("unionInPlace/reuse")
					target.Containers.Put(iKey, iContainer.Freeze())
					bitmapIters[i].handled = true
					continue
				}
				// We have at least two other containers. We can union
				// everything together. We can union everything but
				// the first other container into a clone of the
				// first other container, but for some cases, that will
				// result in cloning a non-bitmap, then converting it
				// to a bitmap, and this will be expensive...
				if expectedN >= 512 && iContainer.typ() != containerBitmap {
					// copying the non-bitmap, then converting it,
					// is expensive.
					statsHit("unionInPlace/newBitmap")
					tContainer = NewContainerBitmapN(nil, 0)
					itersToUnion = bitmapIters[i:]
				} else {
					// either N will be small or iContainer is a
					// bitmap, so we can skip one union op by copying it.
					// And we can just freeze it, and the copy will
					// happen later if it's needed...
					statsHit("unionInPlace/clone")
					tContainer = iContainer.Freeze()
					itersToUnion = bitmapIters[i+1:]
				}
			} else {
				// we have an existing target container. If we're
				// going to end up wanting it to be a bitmap, we
				// convert it preemptively, because union into a
				// bitmap is nearly always faster.
				itersToUnion = bitmapIters[i:]
				if expectedN >= 512 && tContainer.typ() != containerBitmap {
					statsHit("unionInPlace/convertToBitmap")
					switch tContainer.typ() {
					case containerArray:
						tContainer = tContainer.arrayToBitmap()
					case containerRun:
						tContainer = tContainer.runToBitmap()
					}
				}
			}

			// Now we union all remaining containers with this key
			// together.
			for j, iter := range itersToUnion {
				jKey, jContainer := iter.iter.Value()

				if iKey == jKey {
					tContainer = tContainer.Thaw()
					tContainer.unionInPlace(jContainer)
					// "iter" is a local copy from the range
					// loop, not the actual slice member.
					itersToUnion[j].handled = true
				}
			}

			// Now that we've calculated a container that is a union of all the containers
			// with the same key across all the bitmaps, we store it in the list of containers
			// for the target.
			target.Containers.Put(iKey, tContainer)
		}

		hasNext = bitmapIters.next()
	}

	// Performing the popcount() operation with every union is wasteful because
	// its likely the value will be invalidated by the next union operation. As
	// a result, when we're performing all of our in-place unions we allow the value of
	// n (container cardinality) to fall out of sync, and then at the very end we perform
	// a "Repair" to recalculate all the container values. That way we never popcount()
	// an entire bitmap container more than once per bulk union operation.
	target.Containers.Repair()
}

// Difference returns the difference of b and other.
func (b *Bitmap) Difference(other ...*Bitmap) *Bitmap {
	output := b.singleDifference(other[0])
	if len(other) > 1 {
		output.DifferenceInPlace(other[1:]...)
	}
	return output
}

func (b *Bitmap) singleDifference(other *Bitmap) *Bitmap {
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Freeze())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, difference(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Xor returns the bitwise exclusive or of b and other.
func (b *Bitmap) Xor(other *Bitmap) *Bitmap {
	output := NewBitmap()
	// Xor can end up with containers from either parent if the other
	// had no container or an empty container.
	output.Source = MergeSources(b.Source, other.Source)

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Freeze())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			output.Containers.Put(kj, cj.Freeze())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, xor(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Shift shifts the contents of b by 1.
func (b *Bitmap) Shift(n int) (*Bitmap, error) {
	if n != 1 {
		return nil, errors.New("cannot shift by a value other than 1")
	}
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	lastCarry := false
	lastKey := uint64(0)
	for iiter.Next() {
		ki, ci := iiter.Value()
		if lastCarry && ki > lastKey+1 {
			extra := NewContainerArray([]uint16{0})
			output.Containers.Put(lastKey+1, extra)
			lastCarry = false
		}
		o, carry := shift(ci)
		if lastCarry {
			o.add(0)
		}
		if o.N() > 0 {
			output.Containers.Put(ki, o)
		}
		lastCarry = carry
		lastKey = ki
	}
	// As long as the carry wasn't from the max container,
	// append a new container and add the carried bit.
	if lastCarry && lastKey != maxContainerKey {
		extra := NewContainerArray([]uint16{0})
		output.Containers.Put(lastKey+1, extra)
	}

	return output, nil
}

// removeEmptyContainers deletes all containers that have a count of zero.
func (b *Bitmap) removeEmptyContainers() {
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if c.N() == 0 {
			b.Containers.Remove(k)
		}
	}
}
func (b *Bitmap) countEmptyContainers() int {
	result := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() == 0 {
			result++
		}
	}
	return result
}

// Optimize converts array and bitmap containers to run containers as necessary.
func (b *Bitmap) Optimize() {
	b.Containers.UpdateEvery(func(key uint64, c *Container, existed bool) (*Container, bool) {
		return c.optimize(), true
	})
}

type errWriter struct {
	w   io.Writer
	err error
	n   int
}

func (ew *errWriter) WriteUint16(b []byte, v uint16) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint16(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}
func (ew *errWriter) WriteUint32(b []byte, v uint32) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint32(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

func (ew *errWriter) WriteUint64(b []byte, v uint64) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint64(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

// WriteTo writes b to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	b.Optimize()
	return b.writeToUnoptimized(w)
}

// writeToUnoptimized is a WriteTo without the Optimize path. We need
// this because otherwise we can't do some of our marshal/unmarshal tests
// safely.
func (b *Bitmap) writeToUnoptimized(w io.Writer) (n int64, err error) {
	// Remove empty containers before persisting.
	//b.removeEmptyContainers()

	containerCount := b.Containers.Size() - b.countEmptyContainers()
	headerSize := headerBaseSize
	byte2 := make([]byte, 2)
	byte4 := make([]byte, 4)
	byte8 := make([]byte, 8)

	// Build header before writing individual container blocks.
	// Metadata for each container is 8+2+2+4 = sizeof(key) + sizeof(type)+sizeof(cardinality) + sizeof(file offset)
	// Type is stored as 2 bytes, even though it's only got values 1..3.
	// Cookie header section.
	ew := &errWriter{
		w: w,
		n: 0,
	}

	ew.WriteUint32(byte4, cookie|(uint32(b.Flags)<<24))
	ew.WriteUint32(byte4, uint32(containerCount))

	// Descriptive header section: encode keys and cardinality.
	// Key and cardinality are stored interleaved here, 12 bytes per container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		key, c := citer.Value()
		// Verify container count before writing.
		// TODO: instead of commenting this out, we need to make it a configuration option
		//count := c.count()
		//assert(c.count() == c.n, "cannot write container count, mismatch: count=%d, n=%d", count, c.n)
		if c.N() > 0 {
			ew.WriteUint64(byte8, key)
			ew.WriteUint16(byte2, uint16(c.typ()))
			ew.WriteUint16(byte2, uint16(c.N()-1))
		}

	}

	// Offset header section: write the offset for each container block.
	// 4 bytes per container.
	offset := uint32(headerSize + (containerCount * (8 + 2 + 2 + 4)))
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() > 0 {
			ew.WriteUint32(byte4, offset)
			offset += uint32(c.size())
		}

	}
	if ew.err != nil {
		return int64(ew.n), ew.err
	}

	n = int64(headerSize + (containerCount * (8 + 2 + 2 + 4)))

	// Container storage section: write each container block.
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() > 0 {
			nn, err := c.WriteTo(w)
			n += nn
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// roaringIterator represents something which can iterate through a roaring
// bitmap and yield information about containers, including type, size, and
// the location of their data structures.
type roaringIterator interface {
	// Next yields the information about the next container
	Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error)
	// Remaining yields the bytes left over past the end of the roaring data,
	// which is typically an ops log in our case.
	Remaining() []byte
}

// baseRoaringIterator holds values used by both Pilosa and official Roaring
// iterators.
type baseRoaringIterator struct {
	data              []byte
	keys              int64
	headers           []byte
	offsets           []byte
	currentKey        uint64
	currentIdx        int64
	currentType       byte
	currentN          int
	currentLen        int
	currentPointer    *uint16
	currentDataOffset uint64
	prevOffset32      uint32
	chunkOffset       uint64
	lastDataOffset    int64
	lastErr           error
}

// okay, then
func (b *baseRoaringIterator) SilenceLint() {
	// these are actually used in pilosaRoaringIterator or officialRoaringIterator
	// but structcheck doesn't know that
	_ = b.data
	_ = b.keys
	_ = b.offsets
	_ = b.headers
	_ = b.currentIdx
	_ = b.chunkOffset
	_ = b.prevOffset32
}

type pilosaRoaringIterator struct {
	baseRoaringIterator
}

type officialRoaringIterator struct {
	baseRoaringIterator
	containerTyper func(index uint, card int) byte
	haveRuns       bool
}

func newOfficialRoaringIterator(data []byte) (*officialRoaringIterator, error) {
	r := &officialRoaringIterator{}
	r.data = data

	// share code with the existing unmarshal code
	var offsetOffset, headerOffset int
	var err error
	var keys uint32

	// we ignore the flags, since we don't have to process them for anything.
	keys, r.containerTyper, headerOffset, offsetOffset, r.haveRuns, err = readOfficialHeader(data)
	if err != nil {
		return nil, fmt.Errorf("reading official header: %v", err)
	}
	if keys == 0 {
		// not an error, exactly. it's valid and well-formed, we just have nothing to do
		r.Done(io.EOF)
		return r, nil
	}
	r.keys = int64(keys)
	r.headers = data[headerOffset:offsetOffset]
	// note: offsets are only actually used with the no-run headers.
	if r.haveRuns {
		r.currentDataOffset = uint64(offsetOffset)
	} else {
		if len(r.data) < offsetOffset+int(r.keys*4) {
			return nil, fmt.Errorf("insufficient data for offsets (need %d bytes, found %d)",
				r.keys*4, len(r.data)-offsetOffset)
		}
		r.offsets = data[offsetOffset : offsetOffset+int(r.keys*4)]
		r.currentDataOffset = uint64(offsetOffset)
	}
	// set key to -1; user should call Next first.
	r.currentIdx = -1
	r.currentKey = ^uint64(0)
	r.lastErr = errors.New("tried to read iterator without calling Next first")
	return r, nil
}

func newPilosaRoaringIterator(data []byte) (*pilosaRoaringIterator, error) {
	fileVersion := uint32(data[2])
	if fileVersion != storageVersion {
		return nil, fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, storageVersion)
	}
	r := &pilosaRoaringIterator{}
	r.data = data
	// Read key count in bytes sizeof(cookie)+sizeof(flag):(sizeof(cookie)+sizeof(uint32)).
	r.keys = int64(binary.LittleEndian.Uint32(data[3+1 : 8]))
	// it could happen
	if r.keys == 0 {
		// special case: what if we have zero containers, but a valid ops log after them?
		// set currentDataOffset so that Done will set lastDataOffset and Remaining() will
		// work.
		if len(data) > headerBaseSize {
			r.currentDataOffset = headerBaseSize
		}
		// not an error, exactly. it's valid and well-formed, we just have nothing to do
		r.Done(io.EOF)
		return r, nil
	}
	if int64(len(data)) < int64(headerBaseSize+(r.keys*16)) {
		return nil, fmt.Errorf("insufficient data for header + offsets: want %d bytes, got %d",
			headerBaseSize+(r.keys*16), len(data))
	}

	headerStart := int64(headerBaseSize)
	headerEnd := headerStart + (r.keys * 12)
	offsetStart := headerEnd
	offsetEnd := offsetStart + (r.keys * 4)
	r.headers = data[headerStart:headerEnd]
	r.offsets = data[offsetStart:offsetEnd]
	// if there's no containers, we want to act as though data started at the end
	// of the list of offsets, which was also empty, so we don't think the entire thing
	// is actually a malformed op
	r.prevOffset32 = uint32(offsetEnd)
	r.currentDataOffset = uint64(offsetEnd)
	// it's possible that there's so many headers that we're actually over
	// 4GB into the file already.
	r.chunkOffset = r.currentDataOffset &^ ((1 << 32) - 1)
	// set key to -1; user should call Next first.
	r.currentIdx = -1
	r.currentKey = ^uint64(0)
	r.lastErr = errors.New("tried to read iterator without calling Next first")
	return r, nil
}

func newRoaringIterator(data []byte) (roaringIterator, error) {
	if len(data) < headerBaseSize {
		return nil, errors.New("invalid data: not long enough to be a roaring header")
	}
	// Verify the first two bytes are a valid MagicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	switch fileMagic {
	case serialCookie, serialCookieNoRunContainer:
		return newOfficialRoaringIterator(data)
	case MagicNumber:
		return newPilosaRoaringIterator(data)
	}
	return nil, fmt.Errorf("unknown roaring magic number %d", fileMagic)
}

// Done marks the iterator as complete, recording err as the reason why
func (r *baseRoaringIterator) Done(err error) {
	r.lastErr = err
	r.currentKey = ^uint64(0)
	r.currentType = 0
	r.currentN = 0
	r.currentLen = 0
	r.currentPointer = nil
	r.lastDataOffset = int64(r.currentDataOffset)
	r.currentDataOffset = 0
}

func (r *baseRoaringIterator) Remaining() []byte {
	if r.lastDataOffset == 0 {
		return nil
	}
	return r.data[r.lastDataOffset:]
}

func (r *pilosaRoaringIterator) Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	if r.currentIdx >= r.keys {
		// we're already done
		return r.Current()
	}
	r.currentIdx++
	if r.currentIdx == r.keys {
		// this is the last key. transition state to the finalized state
		r.Done(io.EOF)
		return r.Current()
	}
	header := r.headers[r.currentIdx*12:]
	r.currentKey = binary.LittleEndian.Uint64(header[0:8])
	r.currentType = byte(binary.LittleEndian.Uint16(header[8:10]))
	r.currentN = int(binary.LittleEndian.Uint16(header[10:12])) + 1
	offset32 := binary.LittleEndian.Uint32(r.offsets[r.currentIdx*4:])
	if offset32 < r.prevOffset32 {
		r.chunkOffset += (1 << 32)
	}
	r.prevOffset32 = offset32
	r.currentDataOffset = r.chunkOffset + uint64(offset32)

	// a run container keeps its data after an initial 2 byte length header
	var runCount uint16
	if r.currentType == containerRun {
		runCount = binary.LittleEndian.Uint16(r.data[r.currentDataOffset : r.currentDataOffset+runCountHeaderSize])
		r.currentDataOffset += 2
	}
	if r.currentDataOffset > uint64(len(r.data)) || r.currentDataOffset < headerBaseSize {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, len(r.data)))
		return r.Current()
	}
	r.currentPointer = (*uint16)(unsafe.Pointer(&r.data[r.currentDataOffset]))
	var size int
	switch r.currentType {
	case containerArray:
		r.currentLen = r.currentN
		size = r.currentLen * 2
	case containerBitmap:
		r.currentLen = 1024
		size = 8192
	case containerRun:
		r.currentLen = int(runCount)
		size = r.currentLen * 4
	}
	if int64(r.currentDataOffset)+int64(size) > int64(len(r.data)) {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d+%d size, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, size, len(r.data)))
		return r.Current()
	}
	r.currentDataOffset += uint64(size)
	r.lastErr = nil
	return r.Current()
}

func (r *officialRoaringIterator) Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	if r.currentIdx >= r.keys {
		// we're already done
		return r.Current()
	}
	r.currentIdx++
	if r.currentIdx == r.keys {
		// this is the last key. transition state to the finalized state
		r.Done(io.EOF)
		return r.Current()
	}
	header := r.headers[r.currentIdx*4:]
	r.currentKey = uint64(binary.LittleEndian.Uint16(header[0:2]))
	r.currentN = int(binary.LittleEndian.Uint16(header[2:4])) + 1
	r.currentType = r.containerTyper(uint(r.currentIdx), r.currentN)
	// with runs, we can't actually look up offsets; the format just stores
	// things sequentially. so we have to actually track the offset in that case.
	if !r.haveRuns {
		r.currentDataOffset = uint64(binary.LittleEndian.Uint32(r.offsets[r.currentIdx*4:]))
	}
	// a run container keeps its data after an initial 2 byte length header
	var runCount uint16
	if r.currentType == containerRun {
		if int(r.currentDataOffset)+2 > len(r.data) {
			r.Done(fmt.Errorf("insufficient data for offsets container %d/%d, expect run length at %d/%d bytes",
				r.currentIdx, r.keys, r.currentDataOffset, len(r.data)))
			return r.Current()
		}
		runCount = binary.LittleEndian.Uint16(r.data[r.currentDataOffset : r.currentDataOffset+runCountHeaderSize])
		r.currentDataOffset += 2
	}
	if r.currentDataOffset > uint64(len(r.data)) || r.currentDataOffset < headerBaseSize {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, len(r.data)))
		return r.Current()
	}
	r.currentPointer = (*uint16)(unsafe.Pointer(&r.data[r.currentDataOffset]))
	var size int
	switch r.currentType {
	case containerArray:
		r.currentLen = r.currentN
		size = r.currentLen * 2
	case containerBitmap:
		r.currentLen = 1024
		size = 8192
	case containerRun:
		// official format stores runs as start/len, we want to convert, but since
		// they might be mmapped, we can't write to that memory
		newRuns := make([]interval16, runCount)
		oldRuns := (*[65536]interval16)(unsafe.Pointer(r.currentPointer))[:runCount:runCount]
		copy(newRuns, oldRuns)
		for i := range newRuns {
			newRuns[i].last += newRuns[i].start
		}
		r.currentPointer = (*uint16)(unsafe.Pointer(&newRuns[0]))
		r.currentLen = int(runCount)
		size = r.currentLen * 4
	}
	if int64(r.currentDataOffset)+int64(size) > int64(len(r.data)) {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d+%d size, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, size, len(r.data)))
		return r.Current()
	}
	r.currentDataOffset += uint64(size)
	r.lastErr = nil
	return r.Current()
}

func (r *baseRoaringIterator) Current() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	return r.currentKey, r.currentType, r.currentN, r.currentLen, r.currentPointer, r.lastErr
}

// SanityCheckMapping is a debugging function which checks whether containers
// are *correctly* recorded as mapped or unmapped.
func (b *Bitmap) SanityCheckMapping(from, to uintptr) (mappedIn int64, mappedOut int64, unmappedIn int64, errs int, err error) {
	b.Containers.UpdateEvery(func(key uint64, c *Container, existed bool) (*Container, bool) {
		dptr := uintptr(unsafe.Pointer(c.pointer))
		if dptr >= from && dptr < to {
			if c.Mapped() {
				mappedIn++
			} else {
				err = fmt.Errorf("container key %d, addr %x, inside %x+%d",
					key, dptr, from, to-from)
				errs++
				unmappedIn++
			}
		} else {
			if c.Mapped() {
				err = fmt.Errorf("container key %d, addr %x, outside %x+%d, but mapped",
					key, dptr, from, to-from)
				errs++
				mappedOut++
			}
		}
		return c, false
	})
	return mappedIn, mappedOut, unmappedIn, errs, err
}

// RemapRoaringStorage tries to update all containers to refer to
// the roaring bitmap in the provided []byte. If any containers are
// marked as mapped, but do not match the provided storage, they will
// be unmapped. The boolean return indicates whether or not any
// containers were mapped to the given storage.
//
// Regardless, after this function runs, no containers have
// mapped storage which does not refer to data; either they got mapped
// to the new storage, or storage was allocated for them.
func (b *Bitmap) RemapRoaringStorage(data []byte) (mappedAny bool, returnErr error) {
	if b.Containers == nil {
		return false, nil
	}
	var itr roaringIterator
	var err error
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrPointer *uint16
	var itrErr error

	// If we got no data, we don't want to do the actual mapping, just
	// the unmapping. If preferMapping is false, we also don't want to
	// map to the data. We still need to do the UpdateEvery loop, we
	// just won't have an iterator for it.
	if data != nil && b.preferMapping {
		itr, err = newRoaringIterator(data)
	}
	// don't return early: we still have to do the unmapping
	if err != nil {
		returnErr = err
	}

	if itr != nil {
		itrKey, itrCType, itrN, _, itrPointer, itrErr = itr.Next()
	}
	if itrErr != nil {
		// iterator errored out, so we won't check it in the loop below
		itr = nil
	}

	b.Containers.UpdateEvery(func(key uint64, oldC *Container, existed bool) (newC *Container, write bool) {
		if itr != nil {
			for itrKey < key && itrErr == nil {
				itrKey, itrCType, itrN, _, itrPointer, itrErr = itr.Next()
			}
			if itrErr != nil {
				itr = nil
			}
			// container might be similar enough that we should trust it:
			if itrKey == key && itrCType == oldC.typ() && itrN == int(oldC.N()) {
				if oldC.frozen() {
					// we don't use Clone, because that would copy the
					// storage, and we don't need that.
					halfCopy := *oldC
					halfCopy.flags &^= flagFrozen
					newC = &halfCopy
				} else {
					newC = oldC
				}
				mappedAny = true
				newC.pointer = itrPointer
				newC.flags |= flagMapped
				return newC, true
			}
		}
		// if the container isn't mapped, we don't need to do anything
		if !oldC.Mapped() {
			return oldC, false
		}
		// forcibly unmap it, so the old mapping can be unmapped safely.
		newC = oldC.unmapOrClone()
		return newC, true
	})
	return mappedAny, returnErr
}

// ImportRoaringBits sets-or-clears bits based on a provided Roaring bitmap.
// This should be equivalent to unmarshalling the bitmap, then executing
// either `b = Union(b, newB)` or `b = Difference(b, newB)`, but with lower
// overhead. The log parameter controls whether to write to the op log; the
// answer should always be yes, except if you're calling using this to apply
// the op log.
//
// If rowSize is non-zero, we should return a map of rows we altered,
// where "rows" are sets of rowSize containers. Otherwise the map isn't used.
// (This allows ImportRoaring to update caches; see fragment.go.)
func (b *Bitmap) ImportRoaringBits(data []byte, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	if data == nil {
		return 0, nil, errors.New("no roaring bitmap provided")
	}
	var itr roaringIterator
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error

	itr, err = newRoaringIterator(data)
	if err != nil {
		return 0, nil, err
	}
	if itr == nil {
		return 0, nil, errors.New("failed to create roaring iterator, but don't know why")
	}

	rowSet = make(map[uint64]int)

	var synthC Container
	var importUpdater func(*Container, bool) (*Container, bool)
	var currRow uint64
	if clear {
		importUpdater = func(oldC *Container, existed bool) (newC *Container, write bool) {
			existN := oldC.N()
			if existN == 0 || !existed {
				return nil, false
			}
			newC = difference(oldC, &synthC)
			if newC.N() != existN {
				changes := int(existN - newC.N())
				changed += changes
				rowSet[currRow] -= changes
				return newC, true
			}
			return oldC, false
		}
	} else {
		importUpdater = func(oldC *Container, existed bool) (newC *Container, write bool) {
			existN := oldC.N()
			if existN == maxContainerVal+1 {
				return oldC, false
			}
			if existN == 0 {
				newerC := synthC.Clone()
				changed += int(newerC.N())
				rowSet[currRow] += int(newerC.N())
				return newerC, true
			}
			newC = oldC.unionInPlace(&synthC)
			if newC.typeID == containerBitmap {
				newC.Repair()
			}
			if newC.N() != existN {
				changes := int(newC.N() - existN)
				changed += changes
				rowSet[currRow] += changes
				return newC, true
			}
			return oldC, false
		}
	}
	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		synthC.typeID = itrCType
		synthC.n = int32(itrN)
		synthC.len = int32(itrLen)
		synthC.cap = int32(itrLen)
		synthC.pointer = itrPointer
		if rowSize != 0 {
			currRow = itrKey / rowSize
		}
		b.Containers.Update(itrKey, importUpdater)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	// note: if we get a non-EOF err, it's possible that we made SOME
	// changes but didn't log them. I don't have a good solution to this.
	if itrErr != io.EOF {
		return changed, rowSet, itrErr
	}
	err = nil
	if log && changed > 0 {
		op := op{opN: changed, roaring: data}
		if clear {
			op.typ = opTypeRemoveRoaring
		} else {
			op.typ = opTypeAddRoaring
		}
		err = b.writeOp(&op)
	}
	return changed, rowSet, err
}

func (b *Bitmap) PreferMapping(preferred bool) {
	b.preferMapping = preferred
}

// writeOp writes op to the OpWriter, if available.
func (b *Bitmap) writeOp(op *op) error {
	if b.OpWriter == nil {
		return nil
	}

	if _, err := op.WriteTo(b.OpWriter); err != nil {
		return err
	}
	b.opN += op.count()
	b.ops++

	return nil
}

// Iterator returns a new iterator for the bitmap.
func (b *Bitmap) Iterator() *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(0)
	return itr
}

func (b *Bitmap) IteratorAt(start uint64) *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(start)
	return itr
}

// Ops returns the number of write ops the bitmap is aware of in its ops
// log, and their total bit count.
func (b *Bitmap) Ops() (ops int, opN int) {
	return b.ops, b.opN
}

// SetOps lets us reset the operation count in the weird case where we know
// we've changed an underlying file, without actually refreshing the bitmap.
func (b *Bitmap) SetOps(ops int, opN int) {
	b.ops, b.opN = ops, opN
}

// RoaringToBitmaps yields a series of bitmaps with specified shard
// keys, based on a single roaring file, with splits at multiples of
// shardWidth, which should be a multiple of container size.
func RoaringToBitmaps(data []byte, shardWidth uint64) ([]*Bitmap, []uint64) {
	if data == nil {
		return nil, nil
	}
	var itr roaringIterator
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error
	currentShard := ^uint64(0)
	var currentBitmap *Bitmap
	var bitmaps []*Bitmap
	var shards []uint64
	keysPerShard := shardWidth >> 16

	itr, err := newRoaringIterator(data)
	if err != nil || itr == nil {
		return nil, nil
	}

	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		newC := &Container{
			typeID:  itrCType,
			n:       int32(itrN),
			len:     int32(itrLen),
			cap:     int32(itrLen),
			pointer: itrPointer,
			flags:   flagMapped,
		}
		shard := itrKey / keysPerShard
		if shard != currentShard {
			if currentBitmap != nil {
				bitmaps = append(bitmaps, currentBitmap)
				shards = append(shards, currentShard)
			}
			currentBitmap = NewFileBitmap()
			currentShard = shard
		}
		currentBitmap.Containers.Put(itrKey, newC)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	if currentBitmap != nil {
		bitmaps = append(bitmaps, currentBitmap)
		shards = append(shards, currentShard)
	}
	// we don't support ops logs for this
	return bitmaps, shards
}

// BitmapsToRoaring renders a series of non-overlapping bitmaps as a
// unified roaring file.
func BitmapsToRoaring(bitmaps []*Bitmap) []byte {
	count := int64(0)
	size := int64(0)
	for i, bm := range bitmaps {
		c, s := bm.roaringSize()
		// skip this bitmap during the next pass, since it's empty
		if c == 0 {
			bitmaps[i] = nil
			continue
		}
		count += c
		size += s
	}
	if count == 0 {
		return nil
	}
	// we have count headers, which need 12 bytes, plus a magic number,
	// plus offsets (4 bytes per container), plus size bytes of data to
	// write.
	out := make([]byte, headerBaseSize+(12*count)+(4*count)+size)
	binary.LittleEndian.PutUint16(out[0:2], uint16(MagicNumber))
	out[3] = byte(storageVersion)
	binary.LittleEndian.PutUint32(out[4:8], uint32(count))
	headerEnd := 8 + (12 * count)
	offsetEnd := headerEnd + (4 * count)
	headers := out[8:headerEnd]
	offsets := out[headerEnd:offsetEnd]
	data := out[offsetEnd:]
	headerOffset := 0
	offsetOffset := 0
	dataOffset := 0
	prevKey := uint64(0)
	for _, bm := range bitmaps {
		if bm == nil {
			continue
		}
		citer, _ := bm.Containers.Iterator(0)
		for citer.Next() {
			k, c := citer.Value()
			n := c.N()
			if n == 0 {
				continue
			}
			if roaringParanoia {
				if k < prevKey {
					panic("unsorted keys in multiple-bitmap roaring conversion")
				}
			}
			// place header at header offset, and data at data
			// offset
			header := headers[headerOffset : headerOffset+12]
			offset := offsets[offsetOffset : offsetOffset+4]
			headerOffset += 12
			offsetOffset += 4
			binary.LittleEndian.PutUint64(header[0:8], k)
			binary.LittleEndian.PutUint16(header[8:10], uint16(c.typeID))
			binary.LittleEndian.PutUint16(header[10:12], uint16(n-1))
			binary.LittleEndian.PutUint32(offset[0:4], uint32(dataOffset+int(offsetEnd)))
			nextData := data[dataOffset:]
			switch c.typeID {
			case containerArray:
				asUint16 := *(*[]uint16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&nextData[0])), Len: int(c.len), Cap: int(c.len)}))
				copy(asUint16, c.array())
				dataOffset += 2 * int(c.len)
			case containerBitmap:
				asUint64 := *(*[]uint64)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&nextData[0])), Len: 1024, Cap: 1024}))
				copy(asUint64, c.bitmap())
				dataOffset += 8192
			case containerRun:
				asInterval16 := *(*[]interval16)(unsafe.Pointer(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&nextData[2])), Len: int(c.len), Cap: int(c.len)}))
				copy(asInterval16, c.runs())
				binary.LittleEndian.PutUint16(nextData[0:2], uint16(c.len))
				dataOffset += int(4*c.len) + 2
			}
		}
	}
	return out
}

// roaringSize yields the count of non-empty containers, and the size
// of the storage *only* -- not the headers.
func (b *Bitmap) roaringSize() (int64, int64) {
	count := int64(0)
	size := int64(0)
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() == 0 {
			continue
		}
		count++
		switch c.typeID {
		case containerArray:
			size += 2 * int64(c.N())
		case containerBitmap:
			size += 8192
		case containerRun:
			// 2 bytes for the count of runs, plus 4 bytes per run
			size += 2 + (4 * int64(c.len))
		}
	}
	return count, size
}

// Info returns stats for the bitmap.
func (b *Bitmap) Info() bitmapInfo {
	info := bitmapInfo{
		OpN:        b.opN,
		Ops:        b.ops,
		Containers: make([]containerInfo, 0, b.Containers.Size()),
	}

	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		ci := c.info()
		ci.Key = k
		info.Containers = append(info.Containers, ci)
	}
	return info
}

// Check performs a consistency check on the bitmap. Returns nil if consistent.
func (b *Bitmap) Check() error {
	var a ErrorList

	// Check each container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if err := c.check(); err != nil {
			a.AppendWithPrefix(err, fmt.Sprintf("%d/", k))
		}
	}

	if len(a) == 0 {
		return nil
	}
	return a
}

// Flip performs a logical negate of the bits in the range [start,end].
func (b *Bitmap) Flip(start, end uint64) *Bitmap {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("flipping in range but %v > %v", start, end))
		}
	}
	result := NewBitmap()
	itr := b.Iterator()
	v, eof := itr.Next()
	//copy over previous bits.
	for v < start && !eof {
		result.DirectAdd(v)
		v, eof = itr.Next()
	}
	//flip bits in range .
	for i := start; i <= end; i++ {
		if eof {
			result.DirectAdd(i)
		} else if v == i {
			v, eof = itr.Next()
		} else {
			result.DirectAdd(i)
		}
	}
	//add remaining.
	for !eof {
		result.DirectAdd(v)
		v, eof = itr.Next()
	}
	return result
}

// bitmapInfo represents a point-in-time snapshot of bitmap stats.
type bitmapInfo struct {
	OpN        int
	Ops        int
	Containers []containerInfo
}

// Iterator represents an iterator over a Bitmap.
type Iterator struct {
	bitmap *Bitmap
	citer  ContainerIterator
	key    uint64
	c      *Container
	j, k   int32 // i: container; j: array index, bit index, or run index; k: offset within the run
}

// Seek moves to the first value equal to or greater than `seek`.
func (itr *Iterator) Seek(seek uint64) {
	// k should always be -1 unless we're seeking into a run container. Then the
	// "if c.isRun" section will take care of it.
	itr.k = -1

	// Move to the correct container.
	itr.citer, _ = itr.bitmap.Containers.Iterator(highbits(seek))
	if !itr.citer.Next() {
		itr.c = nil
		return // eof
	}
	itr.key, itr.c = itr.citer.Value()
	if roaringParanoia {
		if itr.c == nil {
			panic("seeking iterator got a nil container when Next() was true")
		}
	}

	// Move to the correct value index inside the container.
	lb, hb := lowbits(seek), highbits(seek)
	if itr.c.isArray() {
		// Seek is smaller than min(itr.c).
		if itr.key > hb {
			itr.j = -1
			return
		}

		// Find index in the container.
		itr.j = search32(itr.c.array(), lb)
		if itr.j < 0 {
			itr.j = -itr.j - 1
		}
		if itr.j < int32(len(itr.c.array())) {
			itr.j--
			return
		}

		// If it's at the end of the container then move to the next one.
		if !itr.citer.Next() {
			itr.c = nil
			return
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
		return
	}

	if itr.c.isRun() {
		// Seek is smaller than min(itr.c).
		if itr.key > hb {
			itr.j = 0
			itr.k = -1
			return
		}

		j, contains := binSearchRuns(lb, itr.c.runs())
		if contains {
			itr.j = j
			itr.k = int32(lb) - int32(itr.c.runs()[j].start) - 1
			return
		}
		// If seek is larger than all elements, return.
		if j >= int32(len(itr.c.runs())) {
			if !itr.citer.Next() {
				itr.c = nil
				return
			}
			itr.key, itr.c = itr.citer.Value()
			itr.j = -1
			return
		}
		// Set iterator to next value in the Bitmap.
		itr.j = j
		itr.k = -1
		return
	}

	// If it's a bitmap container then move to index before the value.
	if itr.key > hb {
		itr.j = -1
		return
	}
	itr.j = int32(lb) - 1
}

// Next returns the next value in the bitmap.
// Returns eof as true if there are no values left in the iterator.
func (itr *Iterator) Next() (v uint64, eof bool) {
	if itr.c == nil {
		return 0, true
	}
	// Iterate over containers until we find the next value or EOF.
	for {
		if itr.c.isArray() {
			if itr.j >= itr.c.N()-1 {
				// Reached end of array, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}
			itr.j++
			return itr.peek(), false
		}

		if itr.c.isRun() {
			// Because itr.j for an array container defaults to -1
			// but defaults to 0 for a run container, we need to
			// standardize on treating -1 as our default value for itr.j.
			// Note that this is easier than changing the default to 0
			// because the array logic uses the negative number space
			// to represent offsets to an array position that isn't filled
			// (-1 being the first empty space in an array, or 0).
			if itr.j == -1 {
				itr.j++
			}

			// If the container is empty, move to the next container.
			if len(itr.c.runs()) == 0 {
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			r := itr.c.runs()[itr.j]
			runLength := int32(r.last - r.start)

			if itr.k >= runLength {
				// Reached end of run, move to the next run.
				itr.j, itr.k = itr.j+1, -1
			}

			if itr.j >= int32(len(itr.c.runs())) {
				// Reached end of runs, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			itr.k++
			return itr.peek(), false
		}

		// Move to the next possible index in the bitmap container.
		itr.j++

		// Find first non-zero bit in current bitmap, if possible.
		hb := itr.j >> 6

		if hb >= int32(len(itr.c.bitmap())) {
			if !itr.citer.Next() {
				itr.c = nil
				return 0, true
			}
			itr.key, itr.c = itr.citer.Value()
			itr.j = -1
			continue
		}
		lb := itr.c.bitmap()[hb] >> (uint(itr.j) % 64)
		if lb != 0 {
			itr.j = itr.j + int32(trailingZeroN(lb))
			return itr.peek(), false
		}

		// Otherwise iterate through remaining bitmaps to find next bit.
		for hb++; hb < int32(len(itr.c.bitmap())); hb++ {
			if itr.c.bitmap()[hb] != 0 {
				itr.j = hb<<6 + int32(trailingZeroN(itr.c.bitmap()[hb]))
				return itr.peek(), false
			}
		}

		// If no bits found then move to the next container.
		if !itr.citer.Next() {
			itr.c = nil
			return 0, true
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
	}
}

// peek returns the current value.
func (itr *Iterator) peek() uint64 {
	if itr.c == nil {
		return 0
	}
	if itr.c.isArray() {
		return itr.key<<16 | uint64(itr.c.array()[itr.j])
	}
	if itr.c.isRun() {
		return itr.key<<16 | uint64(itr.c.runs()[itr.j].start+uint16(itr.k))
	}
	return itr.key<<16 | uint64(itr.j)
}

// ArrayMaxSize represents the maximum size of array containers.
const ArrayMaxSize = 4096

// runMaxSize represents the maximum size of run length encoded containers.
const runMaxSize = 2048

type interval16 struct {
	start uint16
	last  uint16
}

// runlen returns the count of integers in the interval.
func (iv interval16) runlen() int32 {
	return 1 + int32(iv.last-iv.start)
}

// count counts all bits in the container.
func (c *Container) count() (n int32) {
	return c.countRange(0, maxContainerVal+1)
}

// countRange counts the number of bits set between [start, end).
func (c *Container) countRange(start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	if c == nil {
		return 0
	}
	if c.isArray() {
		return c.arrayCountRange(start, end)
	} else if c.isRun() {
		return c.runCountRange(start, end)
	}
	return c.bitmapCountRange(start, end)
}

func (c *Container) arrayCountRange(start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	array := c.array()
	i := int32(sort.Search(len(array), func(i int) bool { return int32(array[i]) >= start }))
	for ; i < int32(len(array)); i++ {
		v := int32(array[i])
		if v >= end {
			break
		}
		n++
	}
	return n
}

func (c *Container) bitmapCountRange(start, end int32) int32 {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	var n uint64
	i, j := start/64, end/64
	// Special case when start and end fall in the same word.
	bitmap := c.bitmap()
	if i == j {
		offi, offj := uint(start%64), uint(64-end%64)
		n += popcount((bitmap[i] >> offi) << (offj + offi))
		return int32(n)
	}

	// Count partial starting word.
	if off := uint(start) % 64; off != 0 {
		n += popcount(bitmap[i] >> off)
		i++
	}

	// Count words in between.
	for ; i < j; i++ {
		n += popcount(bitmap[i])
	}

	// Count partial ending word.
	if j < int32(len(bitmap)) {
		off := 64 - (uint(end) % 64)
		n += popcount(bitmap[j] << off)
	}

	return int32(n)
}

func (c *Container) runCountRange(start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	runs := c.runs()
	for _, iv := range runs {
		// iv is before range
		if int32(iv.last) < start {
			continue
		}
		// iv is after range
		if end < int32(iv.start) {
			break
		}
		// iv is superset of range
		if int32(iv.start) <= start && int32(iv.last) >= end {
			return end - start
		}
		// iv is subset of range
		if int32(iv.start) >= start && int32(iv.last) <= end {
			n += iv.runlen()
		}
		// iv overlaps beginning of range without being a subset
		if int32(iv.start) < start && int32(iv.last) < end {
			n += int32(iv.last) - start + 1
		}
		// iv overlaps end of range without being a subset
		if int32(iv.start) > start && int32(iv.last) >= end {
			n += end - int32(iv.start)
		}
	}
	return n
}

// add adds a value to the container.
func (c *Container) add(v uint16) (newC *Container, added bool) {
	if c == nil {
		return NewContainerArray([]uint16{v}), true
	}
	if c.isArray() {
		return c.arrayAdd(v)
	} else if c.isRun() {
		return c.runAdd(v)
	} else {
		return c.bitmapAdd(v)
	}
}

func (c *Container) arrayAdd(v uint16) (*Container, bool) {
	// Optimize appending to the end of an array container.
	array := c.array()
	if c.N() > 0 && c.N() < ArrayMaxSize && c.isArray() && array[c.N()-1] < v {
		statsHit("arrayAdd/append")
		c = c.Thaw()
		array = append(c.array(), v)
		c.setArray(array)
		return c, true
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search32(array, v)
	if i >= 0 {
		return c, false
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.N() >= ArrayMaxSize {
		statsHit("arrayAdd/arrayToBitmap")
		c = c.arrayToBitmap()
		return c.bitmapAdd(v)
	}

	// Otherwise insert into array.
	statsHit("arrayAdd/insert")
	c = c.Thaw()
	i = -i - 1
	array = append(c.array(), 0)
	copy(array[i+1:], array[i:])
	array[i] = v
	c.setArray(array)
	return c, true

}

func (c *Container) bitmapAdd(v uint16) (*Container, bool) {
	if c == nil {
		c = NewContainerBitmapN(nil, 1)
		c.bitmap()[v/64] |= (1 << uint64(v%64))
		return c, true
	}
	if c.bitmapContains(v) {
		return c, false
	}
	c = c.Thaw()
	c.bitmap()[v/64] |= (1 << uint64(v%64))
	c.setN(c.N() + 1)
	return c, true
}

func (c *Container) runAdd(v uint16) (*Container, bool) {
	runs := c.runs()

	if len(runs) == 0 {
		c = c.Thaw()
		c.setRuns([]interval16{{start: v, last: v}})
		c.setN(1)
		return c, true
	}

	i := sort.Search(len(runs),
		func(i int) bool { return runs[i].last >= v })

	if i == len(runs) {
		i--
	}

	iv := runs[i]
	if v >= iv.start && iv.last >= v {
		return c, false
	}

	c = c.Thaw()
	runs = c.runs()
	if iv.last < v {
		if iv.last == v-1 {
			runs[i].last++
		} else {
			runs = append(runs, interval16{start: v, last: v})
		}
	} else if v+1 == iv.start {
		// combining two intervals
		if i > 0 && runs[i-1].last == v-1 {
			runs[i-1].last = iv.last
			runs = append(runs[:i], runs[i+1:]...)
			c.setRuns(runs)
			c.setN(c.N() + 1)
			return c, true
		}
		// just before an interval
		runs[i].start--
	} else if i > 0 && v-1 == runs[i-1].last {
		// just after an interval
		runs[i-1].last++
	} else {
		// alone
		newIv := interval16{start: v, last: v}
		runs = append(runs[:i], append([]interval16{newIv}, runs[i:]...)...)
	}
	c.setRuns(runs)
	c.setN(c.N() + 1)
	return c, true
}

// Contains returns true if v is in the container.
func (c *Container) Contains(v uint16) bool {
	if c == nil {
		return false
	}
	if c.isArray() {
		return c.arrayContains(v)
	} else if c.isRun() {
		return c.runContains(v)
	} else {
		return c.bitmapContains(v)
	}
}

func (c *Container) bitmapCountRuns() (r int32) {
	return bitmapCountRuns(c.bitmap())
}

func bitmapCountRuns(bitmap []uint64) (r int32) {
	for i := 0; i < 1023; i++ {
		v, v1 := bitmap[i], bitmap[i+1]
		r = r + int32(popcount((v<<1)&^v)+((v>>63)&^v1))
	}
	vl := bitmap[len(bitmap)-1]
	r = r + int32(popcount((vl<<1)&^vl)+vl>>63)
	return r
}

func arrayCountRuns(array []uint16) (r int32) {
	prev := int32(-2)
	for _, v := range array {
		if prev+1 != int32(v) {
			r++
		}
		prev = int32(v)
	}
	return r
}

func (c *Container) arrayCountRuns() (r int32) {
	return arrayCountRuns(c.array())
}

func (c *Container) countRuns() (r int32) {
	if c.isArray() {
		return c.arrayCountRuns()
	} else if c.isBitmap() {
		return c.bitmapCountRuns()
	} else if c.isRun() {
		return int32(len(c.runs()))
	}

	// sure hope this never happens
	return 0
}

// optimize converts the container to the type which will take up the least
// amount of space.
func (c *Container) optimize() *Container {
	if c.N() == 0 {
		statsHit("optimize/empty")
		return nil
	}
	runs := c.countRuns()

	var newType byte
	if runs <= runMaxSize && runs <= c.N()/2 {
		newType = containerRun
	} else if c.N() < ArrayMaxSize {
		newType = containerArray
	} else {
		newType = containerBitmap
	}

	// Then convert accordingly.
	if c.isArray() {
		if newType == containerBitmap {
			statsHit("optimize/arrayToBitmap")
			c = c.arrayToBitmap()
		} else if newType == containerRun {
			statsHit("optimize/arrayToRun")
			c = c.arrayToRun(runs)
		} else {
			statsHit("optimize/arrayUnchanged")
		}
	} else if c.isBitmap() {
		if newType == containerArray {
			statsHit("optimize/bitmapToArray")
			c = c.bitmapToArray()
		} else if newType == containerRun {
			statsHit("optimize/bitmapToRun")
			c = c.bitmapToRun(runs)
		} else {
			statsHit("optimize/bitmapUnchanged")
		}
	} else if c.isRun() {
		if newType == containerBitmap {
			statsHit("optimize/runToBitmap")
			c = c.runToBitmap()
		} else if newType == containerArray {
			statsHit("optimize/runToArray")
			c = c.runToArray()
		} else {
			statsHit("optimize/runUnchanged")
		}
	}
	return c
}

// unionInPlace does not necessarily preserve container's N; it's expected
// to be used when running a sequence of unions, after which you should
// call Repair(). (As of this writing, that only matters for bitmaps.)
//
// If called on a frozen container, or a container of the wrong sort,
// it is possible that the returned container will not actually be the
// original container; in-place is a suggestion.
func (c *Container) unionInPlace(other *Container) *Container {
	if c == nil {
		return other.Freeze()
	}
	if other == nil {
		return c
	}
	// short-circuit the trivial cases
	if c.N() == maxContainerVal+1 || other.N() == maxContainerVal+1 {
		return fullContainer
	}
	switch c.typ() {
	case containerBitmap:
		switch other.typ() {
		case containerBitmap:
			return unionBitmapBitmapInPlace(c, other)
		case containerArray:
			return unionBitmapArrayInPlace(c, other)
		case containerRun:
			return unionBitmapRunInPlace(c, other)

		}
	case containerArray:
		switch other.typ() {
		case containerBitmap:
			c = c.arrayToBitmap()
			return unionBitmapBitmapInPlace(c, other)
		case containerArray:
			return unionArrayArrayInPlace(c, other)
		case containerRun:
			c = c.arrayToBitmap()
			return unionBitmapRunInPlace(c, other)
		}
	case containerRun:
		switch other.typ() {
		case containerBitmap:
			c = c.runToBitmap()
			return unionBitmapBitmapInPlace(c, other)
		case containerArray:
			c = c.runToBitmap()
			return unionBitmapArrayInPlace(c, other)
		case containerRun:
			c = c.runToBitmap()
			return unionBitmapRunInPlace(c, other)
		}
	}
	if roaringParanoia {
		panic(fmt.Sprintf("invalid union op: unknown types %d/%d", c.typ(), other.typ()))
	}
	return c
}

func (c *Container) arrayContains(v uint16) bool {
	return search32(c.array(), v) >= 0
}

func (c *Container) bitmapContains(v uint16) bool {
	return (c.bitmap()[v/64] & (1 << uint64(v%64))) != 0
}

// binSearchRuns returns the index of the run containing v, and true, when v is contained;
// or the index of the next run starting after v, and false, when v is not contained.
func binSearchRuns(v uint16, a []interval16) (int32, bool) {
	i := int32(sort.Search(len(a),
		func(i int) bool { return a[i].last >= v }))
	if i < int32(len(a)) {
		return i, (v >= a[i].start) && (v <= a[i].last)
	}

	return i, false
}

// runContains determines if v is in the container assuming c is a run
// container.
func (c *Container) runContains(v uint16) bool {
	_, found := binSearchRuns(v, c.runs())
	return found
}

// remove removes a value from the container.
func (c *Container) remove(v uint16) (newC *Container, removed bool) {
	if c == nil {
		return nil, false
	}
	if c.isArray() {
		return c.arrayRemove(v)
	} else if c.isRun() {
		return c.runRemove(v)
	} else {
		return c.bitmapRemove(v)
	}
}

func (c *Container) arrayRemove(v uint16) (*Container, bool) {
	array := c.array()
	i := search32(array, v)
	if i < 0 {
		return c, false
	}
	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		c.n = 0
		return nil, true
	}
	c = c.Thaw()
	array = c.array()

	array = append(array[:i], array[i+1:]...)
	c.setArray(array)
	return c, true
}

func (c *Container) bitmapRemove(v uint16) (*Container, bool) {
	if !c.bitmapContains(v) {
		return c, false
	}
	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		c.n = 0
		return nil, true
	}
	c = c.Thaw()

	// Lower count and remove element.
	c.bitmap()[v/64] &^= (uint64(1) << uint(v%64))
	c.setN(c.N() - 1)

	// Convert to array if we go below the threshold.
	if c.N() == ArrayMaxSize {
		statsHit("bitmapRemove/bitmapToArray")
		c = c.bitmapToArray()
	}
	return c, true
}

// runRemove removes v from a run container, and returns true if v was removed.
func (c *Container) runRemove(v uint16) (*Container, bool) {
	runs := c.runs()
	i, contains := binSearchRuns(v, runs)
	if !contains {
		return c, false
	}
	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		c.n = 0
		return nil, true
	}
	c = c.Thaw()
	runs = c.runs()
	if v == runs[i].last && v == runs[i].start {
		runs = append(runs[:i], runs[i+1:]...)
	} else if v == runs[i].last {
		runs[i].last--
	} else if v == runs[i].start {
		runs[i].start++
	} else if v > runs[i].start {
		last := runs[i].last
		runs[i].last = v - 1
		runs = append(runs, interval16{})
		copy(runs[i+2:], runs[i+1:])
		runs[i+1] = interval16{start: v + 1, last: last}
		// runs = append(runs[:i+1], append([]interval16{{start: v + 1, last: last}}, runs[i+1:]...)...)
	}
	c.setN(c.N() - 1)
	c.setRuns(runs)
	return c, true
}

// max returns the maximum value in the container.
func (c *Container) max() uint16 {
	if c == nil || c.N() == 0 {
		// probably wrong, but prevents a crash elsewhere
		return 0
	}
	if c.isArray() {
		return c.arrayMax()
	} else if c.isRun() {
		return c.runMax()
	} else {
		return c.bitmapMax()
	}
}

func (c *Container) arrayMax() uint16 {
	array := c.array()
	return array[len(array)-1]
}

func (c *Container) bitmapMax() uint16 {
	// Search bitmap in reverse order.
	bitmap := c.bitmap()
	for i := len(bitmap); i > 0; i-- {
		// If value is zero then skip.
		v := bitmap[i-1]
		if v != 0 {
			r := bits.LeadingZeros64(v)
			return uint16((i-1)*64 + 63 - r)
		}

	}
	return 0
}

func (c *Container) runMax() uint16 {
	runs := c.runs()
	if len(runs) == 0 {
		return 0
	}
	return runs[len(runs)-1].last
}

// bitmapToArray converts from bitmap format to array format.
func (c *Container) bitmapToArray() *Container {
	statsHit("bitmapToArray")
	if c == nil {
		if roaringParanoia {
			panic("nil container for bitmapToArray")
		}
		return nil
	}
	// If c is frozen, we'll be making a new array container. Otherwise,
	// we'll convert this container.
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerArray(nil)
		}
		c.setTyp(containerArray)
		c.setArray(nil)
		return c
	}
	bitmap := c.bitmap()
	n := int32(0)

	array := make([]uint16, c.N())
	for i, word := range bitmap {
		for word != 0 {
			t := word & -word
			if roaringParanoia {
				if n >= c.N() {
					panic("bitmap has more bits set than container.n")
				}
			}
			array[n] = uint16((i*64 + int(popcount(t-1))))
			n++
			word ^= t
		}
	}
	if roaringParanoia {
		if n != c.N() {
			panic("bitmap has fewer bits set than container.n")
		}
	}
	if c.frozen() {
		return NewContainerArray(array)
	}
	c.setTyp(containerArray)
	c.setMapped(false)
	c.setArray(array)
	return c
}

// arrayToBitmap converts from array format to bitmap format.
func (c *Container) arrayToBitmap() *Container {
	statsHit("arrayToBitmap")
	if c == nil {
		if roaringParanoia {
			panic("nil container for arrayToBitmap")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerBitmap(0, nil)
		}
		c.setTyp(containerBitmap)
		c.setBitmap(make([]uint64, bitmapN))
		return c
	}

	bitmap := make([]uint64, bitmapN)
	for _, v := range c.array() {
		bitmap[int(v)/64] |= (uint64(1) << uint(v%64))
	}
	if c.frozen() {
		return NewContainerBitmapN(bitmap, c.N())
	}
	c.setTyp(containerBitmap)
	c.setMapped(false)
	c.setBitmap(bitmap)
	return c
}

// runToBitmap converts from RLE format to bitmap format.
func (c *Container) runToBitmap() *Container {
	statsHit("runToBitmap")
	if c == nil {
		if roaringParanoia {
			panic("nil container for runToBitmap")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerBitmap(0, nil)
		}
		c.setTyp(containerBitmap)
		c.setBitmap(make([]uint64, bitmapN))
		return c
	}
	bitmap := make([]uint64, bitmapN)
	for _, iv := range c.runs() {
		w1, w2 := iv.start/64, iv.last/64
		b1, b2 := iv.start&63, iv.last&63
		// a mask for everything under bit X looks like
		// (1 << x) - 1. Say b1 is 4; our mask will want
		// to have the bottom 4 bits be zero, so we shift
		// left 4, getting 10000, then subtract 1, and
		// get 01111, which is the mask to *remove*.
		m1 := (uint64(1) << b1) - 1
		// inclusive mask: same thing, then shift left 1 and
		// or in 1. So for 4, we'd get 011111, which is the
		// mask to *keep*.
		m2 := (((uint64(1) << b2) - 1) << 1) | 1
		if w1 == w2 {
			// If we only had bit 4 in the range, this would
			// end up being 011111 &^ 01111, or 010000.
			bitmap[w1] |= (m2 &^ m1)
			continue
		}
		// for w2, the "To" field, we want to set the bottom N
		// bits. For w1, the "From" word, we want to set all *but*
		// the bottom N bits.
		bitmap[w2] |= m2
		bitmap[w1] |= ^m1
		words := bitmap[w1+1 : w2]
		// set every bit between them
		for i := range words {
			words[i] = ^uint64(0)
		}
	}
	if c.frozen() {
		return NewContainerBitmapN(bitmap, c.N())
	}
	c.setTyp(containerBitmap)
	c.setMapped(false)
	c.setBitmap(bitmap)
	return c
}

// bitmapToRun converts from bitmap format to RLE format.
func (c *Container) bitmapToRun(numRuns int32) *Container {
	statsHit("bitmapToRun")
	if c == nil {
		if roaringParanoia {
			panic("nil container for bitmapToRun")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerRun(nil)
		}
		c.setTyp(containerRun)
		c.setRuns(nil)
		return c
	}

	bitmap := c.bitmap()
	if numRuns == 0 {
		numRuns = bitmapCountRuns(bitmap)
	}
	runs := make([]interval16, 0, numRuns)

	current := bitmap[0]
	var i, start, last uint16
	for {
		// skip while empty
		for current == 0 && i < bitmapN-1 {
			i++
			current = bitmap[i]
		}

		if current == 0 {
			break
		}
		currentStart := uint16(trailingZeroN(current))
		start = 64*i + currentStart

		// pad LSBs with 1s
		current = current | (current - 1)

		// find next 0
		for current == maxBitmap && i < bitmapN-1 {
			i++
			current = bitmap[i]
		}

		if current == maxBitmap {

			// bitmap[1023] == maxBitmap
			runs = append(runs, interval16{start, maxContainerVal})
			break
		}
		currentLast := uint16(trailingZeroN(^current))
		last = 64*i + currentLast
		runs = append(runs, interval16{start, last - 1})

		// pad LSBs with 0s
		current = current & (current + 1)
	}
	if c.frozen() {
		return NewContainerRunN(runs, c.N())
	}
	c.setTyp(containerRun)
	c.setRuns(runs)
	c.setMapped(false)
	return c
}

// arrayToRun converts from array format to RLE format.
func (c *Container) arrayToRun(numRuns int32) *Container {
	statsHit("arrayToRun")
	if c == nil {
		if roaringParanoia {
			panic("nil container for arrayToRun")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerRun(nil)
		}
		c.setTyp(containerRun)
		c.setRuns(nil)
		return c
	}

	array := c.array()

	if numRuns == 0 {
		numRuns = arrayCountRuns(array)
	}

	runs := make([]interval16, 0, numRuns)
	start := array[0]
	for i, v := range array[1:] {
		if v-array[i] > 1 {
			// if current-previous > 1, one run ends and another begins
			runs = append(runs, interval16{start, array[i]})
			start = v
		}
	}
	// append final run
	runs = append(runs, interval16{start, array[c.N()-1]})
	if c.frozen() {
		return NewContainerRunN(runs, c.N())
	}
	c.setTyp(containerRun)
	c.setMapped(false)
	c.setRuns(runs)
	return c
}

// runToArray converts from RLE format to array format.
func (c *Container) runToArray() *Container {
	statsHit("runToArray")
	if c == nil {
		if roaringParanoia {
			panic("nil container for runToArray")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerArray(nil)
		}
		c.setTyp(containerArray)
		c.setArray(nil)
		return c
	}

	runs := c.runs()

	array := make([]uint16, c.N())
	n := int32(0)
	for _, r := range runs {
		for v := int(r.start); v <= int(r.last); v++ {
			array[n] = uint16(v)
			n++
		}
	}
	if roaringParanoia {
		if n != c.N() {
			panic("run has fewer bits set than container.n")
		}
	}
	if c.frozen() {
		return NewContainerArray(array)
	}
	c.setTyp(containerArray)
	c.setMapped(false)
	c.setArray(array)
	return c
}

// Clone returns a copy of c.
func (c *Container) Clone() (out *Container) {
	statsHit("Container/Clone")
	if c == nil {
		return nil
	}
	switch c.typ() {
	case containerArray:
		statsHit("Container/Clone/Array")
		out = NewContainerArrayCopy(c.array())
	case containerBitmap:
		statsHit("Container/Clone/Bitmap")
		other := NewContainerBitmapN(nil, c.N())
		copy(other.bitmap(), c.bitmap())
		out = other
	case containerRun:
		statsHit("Container/Clone/Run")
		out = NewContainerRunCopy(c.runs())
	default:
		panic(fmt.Sprintf("cloning a container of unknown type %d", c.typ()))
	}
	// this should probably never happen
	if roaringParanoia {
		if out.N() != out.count() {
			panic("cloned container has wrong n")
		}
	}
	return out
}

// WriteTo writes c to w.
func (c *Container) WriteTo(w io.Writer) (n int64, err error) {
	if c == nil {
		return 0, nil
	}
	if c.isArray() {
		return c.arrayWriteTo(w)
	} else if c.isRun() {
		return c.runWriteTo(w)
	} else {
		return c.bitmapWriteTo(w)
	}
}

func (c *Container) arrayWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/arrayWriteTo")
	array := c.array()
	if len(array) == 0 {
		return 0, nil
	}

	// Verify all elements are valid.
	// TODO: instead of commenting this out, we need to make it a configuration option
	//	for _, v := range c.array {
	//	assert(lowbits(uint64(v)) == v, "cannot write array value out of range: %d", v)
	//}

	// Write sizeof(uint16) * cardinality bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&array[0]))[: 2*c.N() : 2*c.N()])
	return int64(nn), err
}

func (c *Container) bitmapWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/bitmapWriteTo")
	bitmap := c.bitmap()
	// Write sizeof(uint64) * bitmapN bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&bitmap[0]))[:(8 * bitmapN):(8 * bitmapN)])
	return int64(nn), err
}

func (c *Container) runWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/runWriteTo")
	runs := c.runs()
	if len(runs) == 0 {
		return 0, nil
	}
	var byte2 [2]byte
	binary.LittleEndian.PutUint16(byte2[:], uint16(len(runs)))
	_, err = w.Write(byte2[:])
	if err != nil {
		return 0, err
	}
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&runs[0]))[: interval16Size*len(runs) : interval16Size*len(runs)])
	return int64(runCountHeaderSize + nn), err
}

// size returns the encoded size of the container, in bytes.
func (c *Container) size() int {
	if c.isArray() {
		return len(c.array()) * 2 // sizeof(uint16)
	} else if c.isRun() {
		return len(c.runs())*interval16Size + runCountHeaderSize
	} else {
		return len(c.bitmap()) * 8 // sizeof(uint64)
	}
}

// info returns the current stats about the container.
func (c *Container) info() containerInfo {
	info := containerInfo{N: c.N()}
	if c == nil {
		info.Type = "nil"
		info.Alloc = 0
		return info
	}

	if c.isArray() {
		info.Type = "array"
		info.Alloc = len(c.array()) * 2 // sizeof(uint16)
	} else if c.isRun() {
		info.Type = "run"
		info.Alloc = len(c.runs())*interval16Size + runCountHeaderSize
	} else {
		info.Type = "bitmap"
		info.Alloc = len(c.bitmap()) * 8 // sizeof(uint64)
	}

	if c.Mapped() {
		if c.isArray() {
			info.Pointer = unsafe.Pointer(&c.array()[0])
		} else if c.isRun() {
			info.Pointer = unsafe.Pointer(&c.runs()[0])
		} else {
			info.Pointer = unsafe.Pointer(&c.bitmap()[0])
		}
	}

	return info
}

// check performs a consistency check on the container.
func (c *Container) check() error {
	var a ErrorList

	if c == nil {
		return nil
	}
	if c.isArray() {
		array := c.array()
		if int32(len(array)) != c.N() {
			a.Append(fmt.Errorf("array count mismatch: count=%d, n=%d", len(array), c.N()))
		}
	} else if c.isRun() {
		n := c.runCountRange(0, maxContainerVal+1)
		if n != c.N() {
			a.Append(fmt.Errorf("run count mismatch: count=%d, n=%d", n, c.N()))
		}
	} else if c.isBitmap() {
		if n := c.bitmapCountRange(0, maxContainerVal+1); n != c.N() {
			a.Append(fmt.Errorf("bitmap count mismatch: count=%d, n=%d", n, c.N()))
		}
	} else {
		a.Append(fmt.Errorf("empty container"))
		if c.N() != 0 {
			a.Append(fmt.Errorf("empty container with nonzero count: n=%d", c.N()))
		}
	}

	if a == nil {
		return nil
	}
	return a
}

// Repair repairs the cardinality of c if it has been corrupted by
// optimized operations.
func (c *Container) Repair() {
	// a frozen container can't have had n or contents changed, so we
	// don't need to recount it.
	if c.frozen() {
		return
	}
	if c.isBitmap() {
		c.bitmapRepair()
	}
}

func (c *Container) bitmapRepair() {
	n := int32(0)
	// Manually unroll loop to make it a little faster.
	// TODO(rartoul): Can probably make this a few x faster using
	// SIMD instructions.
	bitmap := c.bitmap()[:bitmapN]
	for i := 0; i <= bitmapN-4; i += 4 {
		n += int32(popcount(bitmap[i]))
		n += int32(popcount(bitmap[i+1]))
		n += int32(popcount(bitmap[i+2]))
		n += int32(popcount(bitmap[i+3]))
	}
	c.setN(n)
}

// containerInfo represents a point-in-time snapshot of container stats.
type containerInfo struct {
	Key     uint64         // container key
	Type    string         // container type (array, bitmap, or run)
	N       int32          // number of bits
	Alloc   int            // memory used
	Pointer unsafe.Pointer // offset within the mmap
}

// flip returns a new container containing the inverse of all
// bits in a.
func flip(a *Container) *Container { // nolint: deadcode
	if a.isArray() {
		return flipArray(a)
	} else if a.isRun() {
		return flipRun(a)
	} else {
		return flipBitmap(a)
	}
}

func flipArray(b *Container) *Container {
	statsHit("flipArray")
	// TODO: actually implement this
	x := b.Clone()
	x = x.arrayToBitmap()
	return flipBitmap(x)
}

func flipBitmap(b *Container) *Container {
	statsHit("flipBitmap")
	other := NewContainerBitmapN(nil, 0)
	bitmap := b.bitmap()
	otherBitmap := other.bitmap()
	for i, word := range bitmap {
		otherBitmap[i] = ^word
	}

	other.setN(other.count())
	return other
}

func flipRun(b *Container) *Container {
	statsHit("flipRun")
	// TODO: actually implement this
	x := b.Clone()
	x = x.runToBitmap()
	return flipBitmap(x)
}

func intersectionCount(a, b *Container) int32 {
	if a.N() == maxContainerVal+1 {
		return b.N()
	}
	if b.N() == maxContainerVal+1 {
		return a.N()
	}
	if a.N() == 0 || b.N() == 0 {
		return 0
	}
	if a.isArray() {
		if b.isArray() {
			return intersectionCountArrayArray(a, b)
		} else if b.isRun() {
			return intersectionCountArrayRun(a, b)
		} else {
			return intersectionCountArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectionCountArrayRun(b, a)
		} else if b.isRun() {
			return intersectionCountRunRun(a, b)
		} else {
			return intersectionCountBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectionCountArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectionCountBitmapRun(a, b)
		} else {
			return intersectionCountBitmapBitmap(a, b)
		}
	}
}

func intersectionCountArrayArray(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayArray")
	ca, cb := a.array(), b.array()
	na, nb := len(ca), len(cb)
	if na > nb {
		ca, cb = cb, ca
		na, nb = nb, na // nolint: staticcheck, ineffassign
	}
	j := 0
	for _, va := range ca {
		for cb[j] < va {
			j++
			if j >= nb {
				return n
			}
		}
		if cb[j] == va {
			n++
		}
	}
	return n
}

func intersectionCountArrayRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayRun")
	array, runs := a.array(), b.runs()
	na, nb := len(array), len(runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := array[i], runs[j]
		if va < vb.start {
			i++
		} else if va >= vb.start && va <= vb.last {
			i++
			n++
		} else if va > vb.last {
			j++
		}
	}
	return n
}

func intersectionCountRunRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if va.start > vb.last {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			n += 1 + int32(vb.last-va.start)
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			n += 1 + int32(vb.last-vb.start)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			n += 1 + int32(va.last-va.start)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			n += 1 + int32(va.last-vb.start)
			i++
		}
	}
	return n
}

func intersectionCountBitmapRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/BitmapRun")
	for _, iv := range b.runs() {
		n += a.bitmapCountRange(int32(iv.start), int32(iv.last)+1)
	}
	return n
}

func intersectionCountArrayBitmap(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayBitmap")
	bitmap := b.bitmap()
	ln := len(bitmap)
	for _, val := range a.array() {
		i := int(val >> 6)
		if i >= ln {
			break
		}
		off := val % 64
		n += int32(bitmap[i]>>off) & 1
	}
	return n
}

func intersectionCountBitmapBitmap(a, b *Container) (n int32) {
	statsHit("intersectionCount/BitmapBitmap")
	return int32(popcountAndSlice(a.bitmap(), b.bitmap()))
}

func intersect(a, b *Container) *Container {
	if a.N() == maxContainerVal+1 {
		return b.Freeze()
	}
	if b.N() == maxContainerVal+1 {
		return a.Freeze()
	}
	if a.N() == 0 || b.N() == 0 {
		return nil
	}
	if a.isArray() {
		if b.isArray() {
			return intersectArrayArray(a, b)
		} else if b.isRun() {
			return intersectArrayRun(a, b)
		} else {
			return intersectArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectArrayRun(b, a)
		} else if b.isRun() {
			return intersectRunRun(a, b)
		} else {
			return intersectBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectBitmapRun(a, b)
		} else {
			return intersectBitmapBitmap(a, b)
		}
	}
}

func intersectArrayArray(a, b *Container) *Container {
	statsHit("intersect/ArrayArray")
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	output := make([]uint16, 0, na)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := aa[i], ab[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			output = append(output, va)
			i, j = i+1, j+1
		}
	}
	return NewContainerArray(output)
}

// intersectArrayRun computes the intersect of an array container and a run
// container. The return is always an array container (since it's guaranteed to
// be low-cardinality)
func intersectArrayRun(a, b *Container) *Container {
	statsHit("intersect/ArrayRun")
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var output []uint16
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := aa[i], rb[j]
		if va < vb.start {
			i++
		} else if va > vb.last {
			j++
		} else {
			output = append(output, va)
			i++
		}
	}
	return NewContainerArray(output)
}

// intersectRunRun computes the intersect of two run containers.
func intersectRunRun(a, b *Container) *Container {
	statsHit("intersect/RunRun")
	output := NewContainerRun(nil)
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	n := int32(0)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if vb.last < va.start {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			n += output.runAppendInterval(interval16{start: va.start, last: vb.last})
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			n += output.runAppendInterval(vb)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			n += output.runAppendInterval(va)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			n += output.runAppendInterval(interval16{start: vb.start, last: va.last})
			i++
		}
	}
	output.setN(n)
	runs := output.runs()
	if n < ArrayMaxSize && int32(len(runs)) > n/2 {
		output = output.runToArray()
	} else if len(runs) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// intersectBitmapRun returns an array container if either container's
// cardinality is <= ArrayMaxSize. Otherwise it returns a bitmap container.
func intersectBitmapRun(a, b *Container) *Container {
	statsHit("intersect/BitmapRun")
	var output *Container
	runs := b.runs()
	if b.N() <= ArrayMaxSize || a.N() <= ArrayMaxSize {
		// output is array container
		array := make([]uint16, 0, b.N())
		for _, iv := range runs {
			for i := iv.start; i <= iv.last; i++ {
				if a.bitmapContains(i) {
					array = append(array, i)
				}
				// If the run ends the container, break to avoid an infinite loop.
				if i == 65535 {
					break
				}
			}
		}

		output = NewContainerArray(array)
	} else {
		// right now this iterates through the runs and sets integers in the
		// bitmap that are in the runs. alternately, we could zero out ranges in
		// the bitmap which are between runs.
		output = NewContainerBitmap(0, nil)
		bitmap := output.bitmap()
		aBitmap := a.bitmap()
		n := int32(0)
		for j := 0; j < len(runs); j++ {
			vb := runs[j]
			i := vb.start >> 6 // index into a
			vastart := i << 6
			valast := vastart + 63
			for valast >= vb.start && vastart <= vb.last && i < bitmapN {
				if vastart >= vb.start && valast <= vb.last { // a within b
					bitmap[i] = aBitmap[i]
					n += int32(popcount(aBitmap[i]))
				} else if vb.start >= vastart && vb.last <= valast { // b within a
					var mask uint64 = ((1 << (vb.last - vb.start + 1)) - 1) << (vb.start - vastart)
					bits := aBitmap[i] & mask
					bitmap[i] |= bits
					n += int32(popcount(bits))
				} else if vastart < vb.start { // a overlaps front of b
					offset := 64 - (1 + valast - vb.start)
					bits := (aBitmap[i] >> offset) << offset
					bitmap[i] |= bits
					n += int32(popcount(bits))
				} else if vb.start < vastart { // b overlaps front of a
					offset := 64 - (1 + vb.last - vastart)
					bits := (aBitmap[i] << offset) >> offset
					bitmap[i] |= bits
					n += int32(popcount(bits))
				}
				// update loop vars
				i++
				vastart = i << 6
				valast = vastart + 63
			}
		}
		output.setN(n)
	}
	return output
}

func intersectArrayBitmap(a, b *Container) *Container {
	statsHit("intersect/ArrayBitmap")
	array := make([]uint16, 0)
	bBitmap := b.bitmap()
	for _, va := range a.array() {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bBitmap[bmidx]
		if b&mask > 0 {
			array = append(array, va)
		}
	}
	return NewContainerArray(array)
}

func intersectBitmapBitmap(a, b *Container) *Container {
	statsHit("intersect/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)
		n  int32
	)
	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] & bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	return output
}

func union(a, b *Container) *Container {
	if a.N() == maxContainerVal+1 || b.N() == maxContainerVal+1 {
		return fullContainer
	}
	if a.isArray() {
		if b.isArray() {
			return unionArrayArray(a, b)
		} else if b.isRun() {
			return unionArrayRun(a, b)
		} else {
			return unionArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return unionArrayRun(b, a)
		} else if b.isRun() {
			return unionRunRun(a, b)
		} else {
			return unionBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return unionArrayBitmap(b, a)
		} else if b.isRun() {
			return unionBitmapRun(a, b)
		} else {
			return unionBitmapBitmap(a, b)
		}
	}
}

func unionArrayArray(a, b *Container) *Container {
	statsHit("union/ArrayArray")
	if a.N() == 0 {
		return b
	}
	if b.N() == 0 {
		return a
	}
	s1, s2 := a.array(), b.array()
	n1, n2 := len(s1), len(s2)
	output := make([]uint16, 0, n1+n2)
	i, j := 0, 0
	for {
		va, vb := s1[i], s2[j]
		if va < vb {
			output = append(output, va)
			i++
		} else if va > vb {
			output = append(output, vb)
			j++
		} else {
			output = append(output, va)
			i++
			j++
		}
		// It's possible we hit the ends at the same time,
		// in which case the append will copy 0 items. This
		// is cheaper than performing a separate conditional
		// check every time...
		if j >= n2 {
			output = append(output, s1[i:]...)
			break
		}
		if i >= n1 {
			output = append(output, s2[j:]...)
			break
		}
	}
	return NewContainerArray(output)
}

// unionArrayArrayInPlace does what it sounds like -- tries to combine
// the two arrays in-place. It does not try to ensure that the result is
// of a good array size, so it could be up to twice that size, temporarily.
func unionArrayArrayInPlace(a, b *Container) *Container {
	statsHit("union/ArrayArrayInPlace")
	if a.N() == 0 {
		if b.N() != 0 {
			// for InPlace, we actually want to ensure that
			// we update a, as long as it's not frozen.
			a = a.Thaw()
			// ... but we also want to be sure we don't end up
			// copying in a mapped object into our not-mapped
			// object.
			a.setArrayMaybeCopy(b.array(), b.Mapped())
			return a.optimize()
		}
		return a
	}
	if b.N() == 0 {
		return a
	}
	s1, s2 := a.array(), b.array()
	n1, n2 := len(s1), len(s2)
	output := make([]uint16, 0, n1+n2)
	i, j := 0, 0
	for {
		va, vb := s1[i], s2[j]
		if va < vb {
			output = append(output, va)
			i++
		} else if va > vb {
			output = append(output, vb)
			j++
		} else {
			output = append(output, va)
			i++
			j++
		}
		// It's possible we hit the ends at the same time,
		// in which case the append will copy 0 items. This
		// is cheaper than performing a separate conditional
		// check every time...
		if j >= n2 {
			output = append(output, s1[i:]...)
			break
		}
		if i >= n1 {
			output = append(output, s2[j:]...)
			break
		}
	}
	// a union can't omit anything that was previously in a, so if
	// the output is the same length, nothing changed.
	if len(output) != int(a.N()) {
		a = a.Thaw()
		a.setArray(output)
	}
	return a.optimize()
}

// unionArrayRun optimistically assumes that the result will be a run container,
// and converts to a bitmap or array container afterwards if necessary.
func unionArrayRun(a, b *Container) *Container {
	statsHit("union/ArrayRun")
	output := NewContainerRun(nil)
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var vb interval16
	var va uint16
	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = aa[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va < vb.start) {
			n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else {
			n += output.runAppendInterval(vb)
			j++
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// runAppendInterval adds the given interval to the run container. It assumes
// that the interval comes at the end of the list of runs, and does not check
// that this is the case. It will not behave correctly if the start of the given
// interval is earlier than the start of the last interval in the list of runs.
// Its return value is the amount by which the cardinality of the container was
// increased.
func (c *Container) runAppendInterval(v interval16) int32 {
	runs := c.runs()
	if len(runs) == 0 {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.last-v.start) + 1
	}

	last := runs[len(runs)-1]
	if last.last == maxContainerVal { //protect against overflow
		return 0
	}
	if last.last+1 >= v.start && v.last > last.last {
		runs[len(runs)-1].last = v.last
		c.setRuns(runs)
		return int32(v.last - last.last)
	} else if last.last+1 < v.start {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.last-v.start) + 1
	}
	return 0
}

func unionRunRun(a, b *Container) *Container {
	statsHit("union/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	output := NewContainerRun(make([]interval16, 0, na+nb))
	var va, vb interval16
	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = ra[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va.start < vb.start) {
			n += output.runAppendInterval(va)
			i++
		} else {
			n += output.runAppendInterval(vb)
			j++
		}
	}
	output.setN(n)
	if len(output.runs()) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

func unionBitmapRun(a, b *Container) *Container {
	statsHit("union/BitmapRun")
	output := a.Clone()
	for _, run := range b.runs() {
		output.bitmapSetRange(uint64(run.start), uint64(run.last)+1)
	}
	return output
}

// unions the run b into the bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapRunInPlace(a, b *Container) *Container {
	a = a.Thaw()
	bitmap := a.bitmap()
	statsHit("union/BitmapRun")
	for _, run := range b.runs() {
		bitmapSetRangeIgnoreN(bitmap, uint64(run.start), uint64(run.last)+1)
	}
	return a
}

const maxBitmap = 0xFFFFFFFFFFFFFFFF

// sets all bits in [i, j) (c must be a bitmap container, and bitmap must
// be its bitmap).
func (c *Container) bitmapSetRange(i, j uint64) {
	bitmap := c.bitmap()
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	xcnt := popcount(X)
	ycnt := popcount(Y)
	n := int32(c.N())
	if x == y {
		n += int32((j - i) - popcount(bitmap[x]&(X&Y)))
		bitmap[x] |= (X & Y)
	} else {
		n += int32(xcnt - popcount(bitmap[x]&X))
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			n += int32(64 - popcount(bitmap[i]))
			bitmap[i] = maxBitmap
		}
		n += int32(ycnt - popcount(bitmap[y]&Y))
		bitmap[y] |= Y
	}
	c.setN(n)
}

// sets all bits in [i, j) without updating any corresponding n value.
func bitmapSetRangeIgnoreN(bitmap []uint64, i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))

	if x == y {
		bitmap[x] |= (X & Y)
	} else {
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			bitmap[i] = maxBitmap
		}
		bitmap[y] |= Y
	}
}

// xor's all bits in [i, j) with all true (c must be a bitmap container).
func (c *Container) bitmapXorRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	bitmap := c.bitmap()
	n := c.N()
	if x == y {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= (X & Y) //// flip
		n += int32(popcount(bitmap[x]) - cnt)
	} else {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= X
		n += int32(popcount(bitmap[x]) - cnt)
		for i := x + 1; i < y; i++ {
			cnt = popcount(bitmap[i])
			bitmap[i] ^= maxBitmap
			n += int32(popcount(bitmap[i]) - cnt)
		}
		cnt = popcount(bitmap[y])
		bitmap[y] ^= Y
		n += int32(popcount(bitmap[y]) - cnt)
	}
	c.setN(n)
}

// zeroes all bits in [i, j) (c must be a bitmap container)
func (c *Container) bitmapZeroRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	bitmap := c.bitmap()
	n := c.N()
	if x == y {
		n -= int32(popcount(bitmap[x] & (X & Y)))
		bitmap[x] &= ^(X & Y)
	} else {
		n -= int32(popcount(bitmap[x] & X))
		bitmap[x] &= ^X
		for i := x + 1; i < y; i++ {
			n -= int32(popcount(bitmap[i]))
			bitmap[i] = 0
		}
		n -= int32(popcount(bitmap[y] & Y))
		bitmap[y] &= ^Y
	}
	c.setN(n)
}

func typePair(ct1, ct2 byte) int {
	return int((ct1 << 4) | ct2)
}

// compareArrayBitmap actually only verifies that everything in the array
// is in the bitmap. It's used only after comparing the N for the containers,
// so if there's anything in the bitmap that's not in the array, either there's
// something in the array that's not in the bitmap, or we didn't get here.
func compareArrayBitmap(a []uint16, b []uint64) error {
	for _, v := range a {
		w, bit := b[v>>6], v&63
		if w>>bit&1 == 0 {
			return fmt.Errorf("value %d missing", v)
		}
	}
	return nil
}

// compareArrayRuns determines whether an array matches a provided
// set of runs. As with compareArrayBitmap, it only verifies presence
// of the array's values in the run collection. the run collection
// can't be empty; if it were, N would have been 0, and we wouldn't
// have gotten here.
func compareArrayRuns(a []uint16, r []interval16) error {
	ri := 0
	ru := r[ri]
	ri++
	for _, v := range a {
		if v < ru.start {
			return fmt.Errorf("value %d missing", v)
		}
		if v > ru.last {
			if ri >= len(r) {
				return fmt.Errorf("value %d missing", v)
			}
			ru = r[ri]
			ri++
			// if they're identical, the array value must be
			// the start of the next run.
			if v != ru.start {
				return fmt.Errorf("value %d missing", v)
			}
		}
	}
	return nil
}

// compareArrayArray reports whether everything in a1 is equal to everything
// in a2.
func compareArrayArray(a1, a2 []uint16) error {
	if len(a1) != len(a2) {
		return fmt.Errorf("unexpected length mismatch, %d vs %d", len(a1), len(a2))
	}
	for i := range a1 {
		if a1[i] != a2[i] {
			return fmt.Errorf("item %d: %d vs %d", i, a1[i], a2[i])
		}
	}
	return nil
}

// BitwiseCompare reports whether two containers are equal. It returns
// an error describing any difference it finds. This is mostly intended
// for use in tests that expect equality.
func (c *Container) BitwiseCompare(c2 *Container) error {
	if c.N() != c2.N() {
		return errors.New("containers are different lengths")
	}
	if c.N() == 0 {
		return nil
	}
	switch typePair(c.typ(), c2.typ()) {
	case typePair(containerArray, containerArray):
		return compareArrayArray(c.array(), c2.array())
	case typePair(containerArray, containerBitmap):
		return compareArrayBitmap(c.array(), c2.bitmap())
	case typePair(containerBitmap, containerArray):
		return compareArrayBitmap(c2.array(), c.bitmap())
	case typePair(containerArray, containerRun):
		return compareArrayRuns(c.array(), c2.runs())
	case typePair(containerRun, containerArray):
		return compareArrayRuns(c2.array(), c.runs())
	default:
		c3 := xor(c, c2)
		if c3.N() != 0 {
			return fmt.Errorf("%d bits differenct between containers", c3.N())
		}
	}
	return nil
}

func unionArrayBitmap(a, b *Container) *Container {
	output := b.Clone()
	bitmap := output.bitmap()
	n := output.N()
	for _, v := range a.array() {
		if !output.bitmapContains(v) {
			bitmap[v/64] |= (1 << uint64(v%64))
			n++
		}
	}
	output.setN(n)
	return output
}

// unions array b into bitmap a, mutating a in place. The n value
// of a will need to be repaired after the fact.
func unionBitmapArrayInPlace(a, b *Container) *Container {
	a = a.Thaw()
	bitmap := a.bitmap()
	for _, v := range b.array() {
		bitmap[v>>6] |= (uint64(1) << (v % 64))
	}
	return a
}

func unionBitmapBitmap(a, b *Container) *Container {
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] | bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	return output
}

// unions bitmap b into bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapBitmapInPlace(a, b *Container) *Container {
	a = a.Thaw()

	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
	)
	// Manually unroll loop to make it a little faster.
	// TODO(rartoul): Can probably make this a few x faster using
	// SIMD instructions.
	for i := 0; i < bitmapN; i += 4 {
		ab[i] |= bb[i]
		ab[i+1] |= bb[i+1]
		ab[i+2] |= bb[i+2]
		ab[i+3] |= bb[i+3]
	}
	return a
}

func difference(a, b *Container) *Container {
	if a.N() == 0 || b.N() == maxContainerVal+1 {
		return nil
	}
	if b.N() == 0 {
		return a.Freeze()
	}
	if a.isArray() {
		if b.isArray() {
			return differenceArrayArray(a, b)
		} else if b.isRun() {
			return differenceArrayRun(a, b)
		} else {
			return differenceArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return differenceRunArray(a, b)
		} else if b.isRun() {
			return differenceRunRun(a, b)
		} else {
			return differenceRunBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return differenceBitmapArray(a, b)
		} else if b.isRun() {
			return differenceBitmapRun(a, b)
		} else {
			return differenceBitmapBitmap(a, b)
		}
	}
}

// differenceArrayArray computes the difference bween two arrays.
func differenceArrayArray(a, b *Container) *Container {
	statsHit("difference/ArrayArray")
	output := NewContainerArray(nil)
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	for i, j := 0, 0; i < na; {
		va := aa[i]
		if j >= nb {
			output.add(va)
			i++
			continue
		}

		vb := ab[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			j++
		} else {
			i, j = i+1, j+1
		}
	}
	return output
}

// differenceArrayRun computes the difference of an array from a run.
func differenceArrayRun(a, b *Container) *Container {
	statsHit("difference/ArrayRun")
	// func (ac *arrayContainer) iandNotRun16(rc *runContainer16) container {
	output := make([]uint16, 0, a.N())
	// cardinality upper bound: card(A)

	i := 0 // array index
	j := 0 // run index
	aa, rb := a.array(), b.runs()

	// handle overlap
	for i < len(aa) {

		// keep all array elements before beginning of runs
		if aa[i] < rb[j].start {
			output = append(output, aa[i])
			i++
			continue
		}

		// if array element in run, skip it
		if aa[i] >= rb[j].start && aa[i] <= rb[j].last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if aa[i] > rb[j].last {
			j++
			if j == len(rb) {
				break
			}
		}
	}

	if i < len(aa) {
		// keep all array elements after end of runs
		// It's possible that output was converted from array to bitmap in output.add()
		// so check container type before proceeding.
		output = append(output, aa[i:]...)
	}
	return NewContainerArray(output)
}

// differenceBitmapRun computes the difference of an bitmap from a run.
func differenceBitmapRun(a, b *Container) *Container {
	statsHit("difference/BitmapRun")
	output := a.Clone()
	for _, run := range b.runs() {
		output.bitmapZeroRange(uint64(run.start), uint64(run.last)+1)
	}
	return output
}

// differenceRunArray subtracts the bits in an array container from a run
// container.
func differenceRunArray(a, b *Container) *Container {
	statsHit("difference/RunArray")
	ra, ab := a.runs(), b.array()
	runs := make([]interval16, 0, len(ra))

	bidx := 0
	vb := ab[bidx]

RUNLOOP:
	for _, run := range ra {
		start := run.start
		for vb < run.start {
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}
		for vb >= run.start && vb <= run.last {
			if vb == start {
				if vb == 65535 { // overflow
					break RUNLOOP
				}
				start++
				bidx++
				if bidx >= len(ab) {
					break
				}
				vb = ab[bidx]
				continue
			}
			runs = append(runs, interval16{start: start, last: vb - 1})
			if vb == 65535 { // overflow
				break RUNLOOP
			}
			start = vb + 1
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}

		if start <= run.last {
			runs = append(runs, interval16{start: start, last: run.last})
		}
	}
	output := NewContainerRun(runs)
	output = output.optimize()
	return output
}

// differenceRunBitmap computes the difference of an run from a bitmap.
func differenceRunBitmap(a, b *Container) *Container {
	statsHit("difference/RunBitmap")
	ra := a.runs()
	// If a is full, difference is the flip of b.
	if len(ra) > 0 && ra[0].start == 0 && ra[0].last == 65535 {
		return flipBitmap(b)
	}
	bb := b.bitmap()[:1024]
	runs := make([]interval16, 0, len(ra))
	for _, inputRun := range ra {
		run := inputRun
		add := true
		for bit := inputRun.start; bit <= inputRun.last; bit++ {
			idx, exp := int(bit>>6), bit&63
			if (bb[idx]>>exp)&1 != 0 {
				if run.start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.start++
				} else if bit == run.last {
					run.last--
				} else {
					run.last = bit - 1
					if run.last >= run.start {
						if len(runs) >= runMaxSize {
							asBitmap := a.runToBitmap()
							return differenceBitmapBitmap(asBitmap, b)
						}
						runs = append(runs, run)
					}
					run.start = bit + 1
					run.last = inputRun.last
				}
				if run.start > run.last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.start <= run.last {
			if add {
				if len(runs) >= runMaxSize {
					asBitmap := a.runToBitmap()
					return differenceBitmapBitmap(asBitmap, b)
				}
				runs = append(runs, run)
			}
		}
	}

	output := NewContainerRun(runs)
	if output.N() < ArrayMaxSize && int32(len(runs)) > output.N()/2 {
		output = output.runToArray()
	} else if len(runs) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// differenceRunRun computes the difference of two runs.
func differenceRunRun(a, b *Container) *Container {
	statsHit("difference/RunRun")

	ra, rb := a.runs(), b.runs()
	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := ra[apos].start
	alast := ra[apos].last
	bstart := rb[bpos].start
	blast := rb[bpos].last
	alen := len(ra)
	blen := len(rb)

	runs := make([]interval16, 0, alen+blen) // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			runs = append(runs, interval16{start: astart, last: alast})
			apos++
			if apos < alen {
				astart = ra[apos].start
				alast = ra[apos].last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = rb[bpos].start
				blast = rb[bpos].last
			}
		default:
			// overlap
			if astart < bstart {
				runs = append(runs, interval16{start: astart, last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = ra[apos].start
					alast = ra[apos].last
				}
			}
		}
	}
	if apos < alen {
		runs = append(runs, interval16{start: astart, last: alast})
		apos++
		if apos < alen {
			runs = append(runs, ra[apos:]...)
		}
	}
	return NewContainerRun(runs)
}

func differenceArrayBitmap(a, b *Container) *Container {
	statsHit("difference/ArrayBitmap")
	output := make([]uint16, 0, a.N())
	bitmap := b.bitmap()
	for _, va := range a.array() {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bitmap[bmidx]

		if mask&^b > 0 {
			output = append(output, va)
		}
	}
	return NewContainerArray(output)
}

func differenceBitmapArray(a, b *Container) *Container {
	statsHit("difference/BitmapArray")
	output := a.Clone()
	bitmap := output.bitmap()

	n := output.N()
	for _, v := range b.array() {
		if output.bitmapContains(v) {
			bitmap[v/64] &^= (uint64(1) << uint(v%64))
			n--
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

func differenceBitmapBitmap(a, b *Container) *Container {
	statsHit("difference/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] & (^bb[i])
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	if output.N() < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

func xor(a, b *Container) *Container {
	if a.N() == 0 {
		return b.Freeze()
	}
	if b.N() == 0 {
		return a.Freeze()
	}
	if a.isArray() {
		if b.isArray() {
			return xorArrayArray(a, b)
		} else if b.isRun() {
			return xorArrayRun(a, b)
		} else {
			return xorArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return xorArrayRun(b, a)
		} else if b.isRun() {
			return xorRunRun(a, b)
		} else {
			return xorBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return xorArrayBitmap(b, a)
		} else if b.isRun() {
			return xorBitmapRun(a, b)
		} else {
			return xorBitmapBitmap(a, b)
		}
	}
}

func xorArrayArray(a, b *Container) *Container {
	statsHit("xor/ArrayArray")
	output := make([]uint16, 0)
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && j >= nb {
			output = append(output, aa[i])
			i++
			continue
		} else if i >= na && j < nb {
			output = append(output, ab[j])
			j++
			continue
		}

		va, vb := aa[i], ab[j]
		if va < vb {
			output = append(output, va)
			i++
		} else if va > vb {
			output = append(output, vb)
			j++
		} else { //==
			i++
			j++
		}
	}
	return NewContainerArray(output)
}

func xorArrayBitmap(a, b *Container) *Container {
	statsHit("xor/ArrayBitmap")
	output := b.Clone()
	for _, v := range a.array() {
		if b.bitmapContains(v) {
			output, _ = output.remove(v)
		} else {
			output, _ = output.add(v)
		}
	}

	// It's possible that output was converted from bitmap to array in output.remove()
	// so we only do this conversion if output is still a bitmap container.
	if output.typ() == containerBitmap && output.count() < ArrayMaxSize {
		output = output.bitmapToArray()
	}

	return output
}

func xorBitmapBitmap(a, b *Container) *Container {
	statsHit("xor/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] ^ bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	if n < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

// shift() shifts the contents of c by one. It returns
// the new container and a bool indicating whether a
// carry bit was shifted out.
func shift(c *Container) (*Container, bool) {
	if c.N() == 0 {
		return nil, false
	}
	if c.isArray() {
		return shiftArray(c)
	} else if c.isRun() {
		return shiftRun(c)
	}
	return shiftBitmap(c)
}

// shiftArray is an array-specific implementation of shift().
func shiftArray(a *Container) (*Container, bool) {
	statsHit("shift/Array")
	carry := false
	aa := a.array()
	output := make([]uint16, 0, len(aa))
	for _, v := range aa {
		if v+1 == 0 { // overflow
			carry = true
		} else {
			output = append(output, v+1)
		}
	}
	return NewContainerArray(output), carry
}

// shiftBitmap is a bitmap-specific implementation of shift().
func shiftBitmap(a *Container) (*Container, bool) {
	statsHit("shift/Bitmap")
	carry := uint64(0)
	output := NewContainerBitmapN(nil, 0)
	ba, bo := a.bitmap(), output.bitmap()
	lastCarry := uint64(0)
	for i, v := range ba {
		carry = v >> 63
		v = v<<1 | lastCarry
		bo[i] = v
		lastCarry = carry
	}
	output.setN(a.N() - int32(carry))
	return output, carry != 0
}

// shiftRun is a run-specific implementation of shift().
func shiftRun(a *Container) (*Container, bool) {
	statsHit("shift/Run")
	carry := false
	ra := a.runs()
	ro := make([]interval16, 0, len(ra))

	for _, v := range ra {
		if v.start+1 == 0 { // final run was 1 bit on container edge
			carry = true
			break
		} else if v.last+1 == 0 { // final run ends on container edge
			v.start++
			carry = true
		} else {
			v.start++
			v.last++
			carry = false
		}
		ro = append(ro, v)
	}

	return NewContainerRun(ro), carry
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd           = opType(0)
	opTypeRemove        = opType(1)
	opTypeAddBatch      = opType(2)
	opTypeRemoveBatch   = opType(3)
	opTypeAddRoaring    = opType(4)
	opTypeRemoveRoaring = opType(5)
)

// op represents an operation on the bitmap.
type op struct {
	typ     opType
	opN     int
	value   uint64
	values  []uint64
	roaring []byte
}

// apply executes the operation against a bitmap.
func (op *op) apply(b *Bitmap) (changed bool) {
	switch op.typ {
	case opTypeAdd:
		return b.DirectAdd(op.value)
	case opTypeRemove:
		return b.remove(op.value)
	case opTypeAddBatch:
		changed = b.DirectAddN(op.values...) > 0
	case opTypeRemoveBatch:
		changed = b.DirectRemoveN(op.values...) > 0
	case opTypeAddRoaring:
		changedN, _, _ := b.ImportRoaringBits(op.roaring, false, false, 0)
		changed = changedN != 0
	case opTypeRemoveRoaring:
		changedN, _, _ := b.ImportRoaringBits(op.roaring, true, false, 0)
		changed = changedN != 0
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
	return changed
}

// WriteTo writes op to the w.
func (op *op) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, op.encodeSize())

	// Write type and value.
	buf[0] = byte(op.typ)
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		binary.LittleEndian.PutUint64(buf[1:9], op.value)
	case opTypeAddBatch, opTypeRemoveBatch:
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(op.values)))
		p := 13 // start of values (skip 4 for checksum)
		for _, v := range op.values {
			binary.LittleEndian.PutUint64(buf[p:p+8], v)
			p += 8
		}
	case opTypeAddRoaring, opTypeRemoveRoaring:
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(op.roaring)))
		binary.LittleEndian.PutUint32(buf[13:17], uint32(op.opN))
	default:
		return 0, fmt.Errorf("can't marshal unknown op type %d", op.typ)
	}

	// Add checksum at the end.
	h := fnv.New32a()
	_, _ = h.Write(buf[0:9])
	_, _ = h.Write(buf[13:])
	if op.typ == 4 || op.typ == 5 {
		_, _ = h.Write(op.roaring)
	}
	binary.LittleEndian.PutUint32(buf[9:13], h.Sum32())

	// Write to writer.
	nn, err := w.Write(buf)
	if err != nil {
		return int64(nn), err
	}
	if op.typ == 4 || op.typ == 5 {
		var nn2 int
		// separate write so we don't have to copy the whole thing
		nn2, err = w.Write(op.roaring)
		nn += nn2
	}
	return int64(nn), err
}

var minOpSize = 13
var maxBatchSize = uint64(1 << 59)

// UnmarshalBinary decodes data into an op.
func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < minOpSize {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}
	statsHit("op/UnmarshalBinary")

	op.typ = opType(data[0])
	// op.value will actually contain the length of values for batch ops, or
	// length of the roaring bitmap for roaring bitmap ops
	op.value = binary.LittleEndian.Uint64(data[1:9])

	// Verify checksum.
	h := fnv.New32a()
	_, _ = h.Write(data[0:9])

	switch op.typ {
	case opTypeAdd, opTypeRemove:
		// nothing to do, just being not-default
	case opTypeAddBatch, opTypeRemoveBatch:
		// This ensures that in doing 13+op.value*8, the max int won't be exceeded and a wrap around case
		// (resulting in a negative value) won't occur in the slice indexing while writing
		if op.value > maxBatchSize {
			return fmt.Errorf("maximum operation size exceeded")
		}
		if len(data) < int(13+op.value*8) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value*8, len(data))
		}
		_, _ = h.Write(data[13 : 13+op.value*8])
		op.values = make([]uint64, op.value)
		for i := uint64(0); i < op.value; i++ {
			start := 13 + i*8
			op.values[i] = binary.LittleEndian.Uint64(data[start : start+8])
		}
		op.value = 0
	case opTypeAddRoaring, opTypeRemoveRoaring:
		if len(data) < int(13+4+op.value) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value, len(data))
		}
		op.opN = int(binary.LittleEndian.Uint32(data[13:17]))
		// gratuitous hack: treat any roaring write as having at least 1/8 of
		// its length in bits, even if it didn't actually change things.
		if op.opN < int(op.value/8) {
			op.opN = int(op.value / 8)
		}
		op.roaring = data[17 : 17+op.value]
		_, _ = h.Write(data[13 : 17+op.value])
		// op.value = 0
	default:
		return fmt.Errorf("unknown op type: %d", op.typ)
	}
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: type %d, exp=%08x, got=%08x", op.typ, h.Sum32(), chk)
	}

	return nil
}

// size returns the encoded size of the op, in bytes.
func (op *op) size() int {
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		return 1 + 8 + 4
	case opTypeAddBatch, opTypeRemoveBatch:
		return 1 + 8 + 4 + len(op.values)*8

	case opTypeAddRoaring, opTypeRemoveRoaring:
		return 1 + 8 + 4 + 4 + len(op.roaring)
	}
	if roaringParanoia {
		panic(fmt.Sprintf("op size() called on unknown op type %d", op.typ))
	}
	return 0
}

// size returns the size needed to encode the op, in bytes. for
// roaring ops, this does not include the roaring data, which is
// already encoded.
func (op *op) encodeSize() int {
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		return 1 + 8 + 4
	case opTypeAddBatch, opTypeRemoveBatch:
		return 1 + 8 + 4 + len(op.values)*8

	case opTypeAddRoaring, opTypeRemoveRoaring:
		return 1 + 8 + 4 + 4
	}
	if roaringParanoia {
		panic(fmt.Sprintf("op encodeSize() called on unknown op type %d", op.typ))
	}
	return 0
}

// count returns the number of bits the operation mutates.
func (op *op) count() int {
	switch op.typ {
	case 0, 1:
		return 1
	case 2, 3:
		return len(op.values)
	case 4, 5:
		return op.opN
	default:
		panic(fmt.Sprintf("unknown operation type: %d", op.typ))
	}
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// search32 returns the index of value in a. If value is not found, it works the
// same way as search64.
func search32(a []uint16, value uint16) int32 {
	statsHit("search32")
	// Optimize for elements and the last element.
	n := int32(len(a))
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := int32(0), n-1
	for lo+16 <= hi {
		i := int32(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// search64 returns the index of value in a. If value is not found, -1 * (1 +
// the index where v would be if it were inserted) is returned. This is done in
// order to both signal that value was not found (negative number), and also
// return information about where v would go if it were inserted. The +1 offset
// is necessary due to the case where v is not found, but would go at index 0.
// since negative 0 is no different from positive 0, we offset the returned
// negative indices by 1. See the test for this function for examples.
func search64(a []uint64, value uint64) int {
	statsHit("search64")
	// Optimize for elements and the last element.
	n := len(a)
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := 0, n-1
	for lo+16 <= hi {
		i := int(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// trailingZeroN returns the number of trailing zeros in v.
// v must be greater than zero.
func trailingZeroN(v uint64) int {
	return bits.TrailingZeros64(v)
}

// ErrorList represents a list of errors.
type ErrorList []error

func (a ErrorList) Error() string {
	switch len(a) {
	case 0:
		return "no errors"
	case 1:
		return a[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", a[0], len(a)-1)
}

// Append appends an error to the list. If err is an ErrorList then all errors are appended.
func (a *ErrorList) Append(err error) {
	switch err := err.(type) {
	case ErrorList:
		*a = append(*a, err...)
	default:
		*a = append(*a, err)
	}
}

// AppendWithPrefix appends an error to the list and includes a prefix.
func (a *ErrorList) AppendWithPrefix(err error, prefix string) {
	switch err := err.(type) {
	case ErrorList:
		for i := range err {
			*a = append(*a, fmt.Errorf("%s%s", prefix, err[i]))
		}
	default:
		*a = append(*a, fmt.Errorf("%s%s", prefix, err))
	}
}

// xorArrayRun computes the exclusive or of an array and a run container.
func xorArrayRun(a, b *Container) *Container {
	statsHit("xor/ArrayRun")
	output := NewContainerRun(nil)
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var vb interval16
	var va uint16
	lastI, lastJ := -1, -1
	n := int32((0))
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && i != lastI {
			va = aa[i]
		}
		if j < nb && j != lastJ {
			vb = rb[j]
		}
		lastI = i
		lastJ = j

		if i < na && (j >= nb || va < vb.start) { //before
			n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else if j < nb && (i >= na || va > vb.last) { //after
			n += output.runAppendInterval(vb)
			j++
		} else if va > vb.start {
			if va < vb.last {
				n += output.runAppendInterval(interval16{start: vb.start, last: va - 1})
				i++
				vb.start = va + 1

				if vb.start > vb.last {
					j++
				}
			} else if va > vb.last {
				n += output.runAppendInterval(vb)
				j++
			} else { // va == vb.last
				vb.last--
				if vb.start <= vb.last {
					n += output.runAppendInterval(vb)
				}
				j++
				i++
			}

		} else { // we know va == vb.start
			if vb.start == maxContainerVal { // protect overflow
				j++
			} else {
				vb.start++
				if vb.start > vb.last {
					j++
				}
			}
			i++
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// xorCompare computes first exclusive run between two runs.
func xorCompare(x *xorstm) (r1 interval16, hasData bool) {
	hasData = false
	if !x.vaValid || !x.vbValid {
		if x.vbValid {
			x.vbValid = false
			return x.vb, true
		}
		if x.vaValid {
			x.vaValid = false
			return x.va, true
		}
		return r1, false
	}

	if x.va.last < x.vb.start { //va  before
		x.vaValid = false
		r1 = x.va
		hasData = true
	} else if x.vb.last < x.va.start { //vb before
		x.vbValid = false
		r1 = x.vb
		hasData = true
	} else if x.va.start == x.vb.start && x.va.last == x.vb.last { // Equal
		x.vaValid = false
		x.vbValid = false
	} else if x.va.start <= x.vb.start && x.va.last >= x.vb.last { //vb inside
		x.vbValid = false
		if x.va.start != x.vb.start {
			r1 = interval16{start: x.va.start, last: x.vb.start - 1}
			hasData = true
		}

		if x.vb.last == maxContainerVal { // Check for overflow
			x.vaValid = false

		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}

	} else if x.vb.start <= x.va.start && x.vb.last >= x.va.last { //va inside
		x.vaValid = false
		if x.vb.start != x.va.start {
			r1 = interval16{start: x.vb.start, last: x.va.start - 1}
			hasData = true
		}

		if x.va.last == maxContainerVal { //check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}

	} else if x.va.start < x.vb.start && x.va.last <= x.vb.last { //va first overlap
		x.vaValid = false
		r1 = interval16{start: x.va.start, last: x.vb.start - 1}
		hasData = true
		if x.va.last == maxContainerVal { // check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}
	} else if x.vb.start < x.va.start && x.vb.last <= x.va.last { //vb first overlap
		x.vbValid = false
		r1 = interval16{start: x.vb.start, last: x.va.start - 1}
		hasData = true

		if x.vb.last == maxContainerVal { // check for overflow
			x.vaValid = false
		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}
	}
	return r1, hasData
}

//stm  is state machine used to "xor" iterate over runs.
type xorstm struct {
	vaValid, vbValid bool
	va, vb           interval16
}

// xorRunRun computes the exclusive or of two run containers.
func xorRunRun(a, b *Container) *Container {
	statsHit("xor/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	output := NewContainerRun(nil)

	lastI, lastJ := -1, -1

	state := &xorstm{}

	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && lastI != i {
			state.va = ra[i]
			state.vaValid = true
		}

		if j < nb && lastJ != j {
			state.vb = rb[j]
			state.vbValid = true
		}
		lastI, lastJ = i, j

		r1, ok := xorCompare(state)
		if ok {
			n += output.runAppendInterval(r1)
		}
		if !state.vaValid {
			i++
		}
		if !state.vbValid {
			j++
		}

	}

	l := len(output.runs())
	output.setN(n)
	if n < ArrayMaxSize && int32(l) > n/2 {
		output = output.runToArray()
	} else if l > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// xorRunRun computes the exclusive or of a bitmap and a run container.
func xorBitmapRun(a, b *Container) *Container {
	statsHit("xor/BitmapRun")
	output := a.Clone()

	for _, run := range b.runs() {
		output.bitmapXorRange(uint64(run.start), uint64(run.last)+1)
	}

	return output
}

// CompareBitmapSlice checks whether a bitmap has the same values in it
// that a provided slice does.
func CompareBitmapSlice(b *Bitmap, vals []uint64) (bool, error) {
	count := b.Count()
	if count != uint64(len(vals)) {
		return false, fmt.Errorf("length mismatch: bitmap has %d bits, slice has %d", count, len(vals))
	}
	for _, v := range vals {
		if !b.Contains(v) {
			return false, fmt.Errorf("bitmap lacks expected value %d", v)
		}
	}
	return true, nil
}

// CompareBitmapMap checks whether a bitmap has the same values in it
// that a provided map[uint64]struct{} has as keys.
func CompareBitmapMap(b *Bitmap, vals map[uint64]struct{}) (bool, error) {
	count := b.Count()
	if count != uint64(len(vals)) {
		return false, fmt.Errorf("length mismatch: bitmap has %d bits, map has %d", count, len(vals))
	}
	for v := range vals {
		if !b.Contains(v) {
			return false, fmt.Errorf("bitmap lacks expected value %d", v)
		}
	}
	return true, nil
}

// BitwiseEqual is used mostly in test cases to confirm that two bitmaps came
// out the same. It does not expect corresponding opN, or OpWriter, but expects
// identical bit contents. It does not expect identical representations; a bitmap
// container can be identical to an array container. It returns a boolean value,
// and also an explanation for a false value.
func (b *Bitmap) BitwiseEqual(c *Bitmap) (bool, error) {
	biter, _ := b.Containers.Iterator(0)
	citer, _ := c.Containers.Iterator(0)
	bn, cn := biter.Next(), citer.Next()
	var bk, ck uint64
	var bc, cc *Container
	bct, cct := 0, 0
	for bn && cn {
		bk, bc = biter.Value()
		ck, cc = citer.Value()
		// zero containers are allowed to match no-container
		if bk < ck {
			if bc.N() == 0 {
				bn = biter.Next()
				continue
			}
		}
		if ck < bk {
			if cc.N() == 0 {
				cn = citer.Next()
				continue
			}
		}
		bct++
		cct++
		if bk != ck {
			return false, fmt.Errorf("differing keys [%d vs %d]", bk, ck)
		}
		diff := xor(bc, cc)
		if diff.N() != 0 {
			return false, fmt.Errorf("differing containers for key %d: %v vs %v", bk, bc, cc)
		}
		bn, cn = biter.Next(), citer.Next()
	}
	// only one can have containers left. they should all be empty. so we
	// look at any remaining containers, break out of the loop if they're not
	// empty, and otherwise keep iterating.
	for bn {
		bn = biter.Next()
		bk, bc = biter.Value()
		if bc.N() != 0 {
			bct++
			break
		}
		bn = biter.Next()
	}
	for cn {
		cn = citer.Next()
		ck, cc = biter.Value()
		if cc.N() != 0 {
			cct++
			break
		}
		cn = biter.Next()
	}
	if bn {
		return false, fmt.Errorf("container mismatch: %d vs %d containers, first bitmap has extra container %d [%d bits]", bct, cct, bk, bc)
	}
	if cn {
		return false, fmt.Errorf("container mismatch: %d vs %d containers, second bitmap has extra container %d [%d bits]", bct, cct, ck, cc)
	}
	return true, nil
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func popcountAndSlice(s, m []uint64) uint64 {
	var (
		a = s[:bitmapN]
		b = m[:bitmapN]
	)

	cnt := uint64(0)
	for i := 0; i < bitmapN; i++ {
		cnt += popcount(a[i] & b[i])
	}
	return cnt
}

// constants from github.com/RoaringBitmap/roaring
// taken from  roaring/util.go
const (
	serialCookieNoRunContainer = 12346 // only arrays and bitmaps
	serialCookie               = 12347 // runs, arrays, and bitmaps
)

func readOfficialHeader(buf []byte) (size uint32, containerTyper func(index uint, card int) byte, header, pos int, haveRuns bool, err error) {
	statsHit("readOfficialHeader")
	if len(buf) < 8 {
		err = fmt.Errorf("buffer too small, expecting at least 8 bytes, was %d", len(buf))
		return size, containerTyper, header, pos, haveRuns, err
	}
	cf := func(index uint, card int) (newType byte) {
		newType = containerBitmap
		if card < ArrayMaxSize {
			newType = containerArray
		}
		return newType
	}
	containerTyper = cf
	cookie := binary.LittleEndian.Uint32(buf)
	pos += 4

	// cookie header
	if cookie == serialCookieNoRunContainer {
		size = binary.LittleEndian.Uint32(buf[pos:])
		pos += 4
	} else if cookie&0x0000FFFF == serialCookie {
		haveRuns = true
		size = uint32(uint16(cookie>>16) + 1) // number of containers

		// create is-run-container bitmap
		isRunBitmapSize := (int(size) + 7) / 8
		if pos+isRunBitmapSize > len(buf) {
			err = fmt.Errorf("malformed bitmap, is-run bitmap overruns buffer at %d", pos+isRunBitmapSize)
			return size, containerTyper, header, pos, haveRuns, err
		}

		isRunBitmap := buf[pos : pos+isRunBitmapSize]
		pos += isRunBitmapSize
		containerTyper = func(index uint, card int) byte {
			if isRunBitmap[index/8]&(1<<(index%8)) != 0 {
				return containerRun
			}
			return cf(index, card)
		}
	} else {
		err = fmt.Errorf("did not find expected serialCookie in header")
		return size, containerTyper, header, pos, haveRuns, err
	}

	header = pos
	if size > (1 << 16) {
		err = fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
		return size, containerTyper, header, pos, haveRuns, err
	}

	// descriptive header
	if pos+2*2*int(size) >= len(buf) {
		err = fmt.Errorf("malformed bitmap, key-cardinality slice overruns buffer at %d", pos+2*2*int(size))
		return size, containerTyper, header, pos, haveRuns, err
	}
	pos += 2 * 2 * int(size) // moving pos past keycount
	return size, containerTyper, header, pos, haveRuns, err
}

// handledIter and handledIters are wrappers around Bitmap Container iterators
// and assist with the unionIntoTarget algorithm by abstracting away some tedious
// operations.
type handledIter struct {
	iter    ContainerIterator
	hasNext bool
	handled bool
}

type handledIters []handledIter

func (w handledIters) next() bool {
	hasNext := false

	for i, wrapped := range w {
		next := wrapped.iter.Next()
		w[i].hasNext = next
		w[i].handled = false
		if next {
			hasNext = true
		}
	}

	return hasNext
}

// Check all the iters from startIdx and up to see whether their next
// key is the given key; if it is, mark them as handled.
func (w handledIters) markItersWithKeyAsHandled(startIdx int, key uint64) {
	for i := startIdx; i < len(w); i++ {
		wrapped := w[i]
		currKey, _ := wrapped.iter.Value()
		if currKey == key {
			w[i].handled = true
		}
	}
}

func (w handledIters) calculateSummaryStats(key uint64) containerUnionSummaryStats {
	summary := containerUnionSummaryStats{}

	for _, iter := range w {
		// Calculate key-level statistics here
		currKey, currContainer := iter.iter.Value()

		if key == currKey {
			summary.c++
			summary.n += int64(currContainer.N())

			if currContainer.N() == maxContainerVal+1 {
				summary.hasMaxRange = true
				summary.n = maxContainerVal + 1
				return summary
			}
		}
	}

	return summary
}

// Summary statistics about all the containers in the other bitmaps
// that share the same key so we can make smarter union strategy
// decisions.
type containerUnionSummaryStats struct {
	// Estimated cardinality of the union of all containers with the same
	// key across all bitmaps. This calculation is very rough as we just sum
	// the cardinality of the container across the different bitmaps which could
	// result in very inflated values, but it allows us to avoid allocating
	// expensive bitmaps when unioning many low density containers.
	n int64
	// Containers found with this key. May be inaccurate if hasMaxRange is true.
	c int
	// Whether any of the containers with the specified keys are storing every possible
	// value that they can. If so, we can short-circuit all the unioning logic and use
	// a RLE container with a single value in it. This is an optimization to
	// avoid using an expensive bitmap container for bitmaps that have some
	// extremely dense containers.
	hasMaxRange bool
}

// DifferenceInPlace returns the bitwise difference of b and others, modifying
// b in place.
func (b *Bitmap) DifferenceInPlace(others ...*Bitmap) {
	bSize := b.Size()

	// If b doesn't have any containers then return early.
	if bSize == 0 {
		return
	}

	const staticSize = 20
	var (
		requiredSliceSize = len(others)
		// To avoid having to allocate a slice every time, if the number of bitmaps
		// being differenced is small enough (i.e. smaller than staticSize), we can
		// just use this stack-allocated array.
		staticHandledIters  = [staticSize]handledIter{}
		bitmapIters         handledIters
		target              = b
		removeContainerKeys = make([]uint64, 0, bSize)
	)

	if requiredSliceSize <= staticSize {
		bitmapIters = staticHandledIters[:0]
	} else {
		bitmapIters = make(handledIters, 0, requiredSliceSize)
	}

	for _, other := range others {
		otherIter, _ := other.Containers.Iterator(0)
		if otherIter.Next() {
			bitmapIters = append(bitmapIters, handledIter{
				iter:    otherIter,
				hasNext: true,
			})
		}
	}

	targetItr, _ := target.Containers.Iterator(0)
	// Go through all the containers and remove the other bits
	for targetItr.Next() {
		targetKey, curContainer := targetItr.Value()
		// no point in subtracting things from an empty container.
		if curContainer.N() == 0 {
			removeContainerKeys = append(removeContainerKeys, targetKey)
		}
		// Loop until every iters current value has been handled.
		for _, iIter := range bitmapIters {
			if !iIter.hasNext {
				continue
			}
			iKey, iContainer := iIter.iter.Value()
			for iKey < targetKey {
				iIter.hasNext = iIter.iter.Next()
				if iIter.hasNext {
					iKey, iContainer = iIter.iter.Value()
				} else {
					break
				}
			}
			if targetKey == iKey {
				// note: a nil container is valid, and has N == 0.
				if iContainer.N() != 0 {
					if curContainer.frozen() {
						curContainer = curContainer.Clone()
						b.Containers.Put(targetKey, curContainer)
					}
					curContainer.differenceInPlace(iContainer)
					if curContainer.N() == 0 {
						removeContainerKeys = append(removeContainerKeys, targetKey)
						break
					}
				}
				iIter.hasNext = iIter.iter.Next()
			}
		}
	}

	for _, key := range removeContainerKeys {
		b.Containers.Remove(key)

	}
	target.Containers.Repair()
}

func (c *Container) differenceInPlace(other *Container) {
	if other == nil {
		return
	}
	if other.isArray() {
		if c.isArray() {
			differenceArrayArrayInPlace(c, other)
		} else if c.isBitmap() {
			differenceBitmapArrayInPlace(c, other)
		} else if c.isRun() {
			differenceRunArrayInPlace(c, other)
		}
	} else if other.isBitmap() {
		if c.isArray() {
			differenceArrayBitmapInPlace(c, other)
		} else if c.isBitmap() {
			differenceBitmapBitmapInPlace(c, other)
		} else if c.isRun() {
			differenceRunBitmapInPlace(c, other)
		}
	} else if other.isRun() {
		if c.isArray() {
			differenceArrayRunInPlace(c, other)
		} else if c.isBitmap() {
			differenceBitmapRunInPlace(c, other)
		} else if c.isRun() {
			differenceRunRunInPlace(c, other)
		}
	}
}

func differenceArrayArrayInPlace(c, other *Container) {
	statsHit("differenceInPlace/ArrayArray")
	aa, ab := c.array(), other.array()
	na, nb := len(aa), len(ab)
	if na == 0 || nb == 0 {
		return
	}
	n := 0
	for i, j := 0, 0; i < na; {
		va := aa[i]
		if j >= nb {
			aa[n] = va
			n++
			i++
			continue
		}

		vb := ab[j]
		if va < vb {
			aa[n] = va
			n++
			i++
		} else if va > vb {
			j++
		} else {
			i, j = i+1, j+1
		}
	}
	aa = aa[:n]
	c.setArray(aa)
}

func differenceArrayBitmapInPlace(c, other *Container) {
	statsHit("differenceInPlace/ArrayBitmap")
	aa := c.array()
	n := 0
	bitmap := other.bitmap()
	if len(aa) == 0 || len(bitmap) == 0 {
		return
	}
	for _, va := range aa {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bitmap[bmidx]

		if mask&^b > 0 {
			aa[n] = va
			n++
		}
	}
	aa = aa[:n]
	c.setArray(aa)
}

func differenceArrayRunInPlace(c, other *Container) {
	statsHit("differenceInPlace/ArrayRun")

	i := 0 // array index
	j := 0 // run index
	aa, rb := c.array(), other.runs()
	if len(aa) == 0 || len(rb) == 0 {
		return
	}
	n := 0

	// handle overlap
	for i < len(aa) {

		// keep all array elements before beginning of runs
		if aa[i] < rb[j].start {
			aa[n] = aa[i]
			n++
			i++
			continue
		}

		// if array element in run, skip it
		if aa[i] >= rb[j].start && aa[i] <= rb[j].last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if aa[i] > rb[j].last {
			j++
			if j == len(rb) {
				break
			}
		}
	}
	for ; i < len(aa); i++ {
		aa[n] = aa[i]
		n++
	}
	aa = aa[:n]
	c.setArray(aa)
}

func differenceBitmapArrayInPlace(c, other *Container) {
	statsHit("differenceInPlace/BitmapArray")
	bitmap := c.bitmap()
	ab := other.array()
	if len(bitmap) == 0 || len(ab) == 0 {
		return
	}

	n := c.N()
	for _, v := range ab {
		if c.bitmapContains(v) {
			bitmap[v/64] &^= (uint64(1) << uint(v%64))
			n--
		}
	}
	c.setN(n)
	if n < ArrayMaxSize {
		c.bitmapToArray() // With This Work
	}
}

func differenceBitmapBitmapInPlace(c, other *Container) {
	statsHit("differenceInPlace/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	a := c.bitmap()
	b := other.bitmap()
	if len(a) == 0 || len(b) == 0 {
		return
	}

	var (
		ab = a[:bitmapN]
		bb = b[:bitmapN]
		n  int32
	)

	for i := 0; i < bitmapN; i++ {
		ab[i] = ab[i] & (^bb[i])
		n += int32(popcount(ab[i]))
	}
	c.setN(n)
	if n < ArrayMaxSize {
		c.bitmapToArray() // Will this work?
	}
}

func differenceBitmapRunInPlace(c, other *Container) {
	statsHit("differenceInPlace/BitmapRun")
	if len(c.bitmap()) == 0 {
		return
	}
	for _, run := range other.runs() {
		c.bitmapZeroRange(uint64(run.start), uint64(run.last)+1)
	}
}

func differenceRunArrayInPlace(c, other *Container) {
	statsHit("differenceInPlace/RunArray")
	ra, ab := c.runs(), other.array()
	if len(ra) == 0 || len(ab) == 0 {
		return
	}
	runs := make([]interval16, 0, len(ra))
	bidx := 0
	vb := ab[bidx]

RUNLOOP:
	for _, run := range ra {
		start := run.start
		for vb < run.start {
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}
		for vb >= run.start && vb <= run.last {
			if vb == start {
				if vb == 65535 { // overflow
					break RUNLOOP
				}
				start++
				bidx++
				if bidx >= len(ab) {
					break
				}
				vb = ab[bidx]
				continue
			}
			runs = append(runs, interval16{start: start, last: vb - 1})
			if vb == 65535 { // overflow
				break RUNLOOP
			}
			start = vb + 1
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}

		if start <= run.last {
			runs = append(runs, interval16{start: start, last: run.last})
		}
	}
	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.last-run.start) + 1
	}
	c.optimize()
}

func differenceRunBitmapInPlace(c, other *Container) {
	statsHit("differenceInPlace/RunBitmap")
	ra := c.runs()
	if len(ra) == 0 || len(other.bitmap()) == 0 {
		return
	}
	// If a is full, difference is the flip of b.
	if len(ra) > 0 && ra[0].start == 0 && ra[0].last == 65535 {
		clone := other.Clone()
		bitmap := clone.bitmap()
		for i, word := range other.bitmap() {
			bitmap[i] = ^word
		}
		c.setTyp(containerBitmap)
		c.setMapped(false)
		c.setBitmap(bitmap)
		c.setN(c.count())
		return
	}
	runs := make([]interval16, 0, len(ra))
	for _, inputRun := range ra {
		run := inputRun
		add := true
		for bit := inputRun.start; bit <= inputRun.last; bit++ {
			if other.bitmapContains(bit) {
				if run.start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.start++
				} else if bit == run.last {
					run.last--
				} else {
					run.last = bit - 1
					if run.last >= run.start {
						runs = append(runs, run)
					}
					run.start = bit + 1
					run.last = inputRun.last
				}
				if run.start > run.last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.start <= run.last {
			if add {
				runs = append(runs, run)
			}
		}
	}

	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.last-run.start) + 1
	}
	if c.N() < ArrayMaxSize && int32(len(runs)) > c.N()/2 {
		c.runToArray()
	} else if len(runs) > runMaxSize {
		c.runToBitmap()
	}
}

func differenceRunRunInPlace(c, other *Container) {
	statsHit("differenceInPlace/RunRun")

	ra, rb := c.runs(), other.runs()
	if len(ra) == 0 || len(rb) == 0 {
		return
	}
	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := ra[apos].start
	alast := ra[apos].last
	bstart := rb[bpos].start
	blast := rb[bpos].last
	alen := len(ra)
	blen := len(rb)

	runs := make([]interval16, 0, alen+blen) // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			runs = append(runs, interval16{start: astart, last: alast})
			apos++
			if apos < alen {
				astart = ra[apos].start
				alast = ra[apos].last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = rb[bpos].start
				blast = rb[bpos].last
			}
		default:
			// overlap
			if astart < bstart {
				runs = append(runs, interval16{start: astart, last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = ra[apos].start
					alast = ra[apos].last
				}
			}
		}
	}
	if apos < alen {
		runs = append(runs, interval16{start: astart, last: alast})
		apos++
		if apos < alen {
			runs = append(runs, ra[apos:]...)
		}
	}
	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.last-run.start) + 1
	}
}
