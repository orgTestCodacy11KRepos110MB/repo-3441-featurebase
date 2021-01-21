// Copyright 2020 Pilosa Corp.
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
	"testing"

	"github.com/pilosa/pilosa/v2/testhook"
)

var _ = fmt.Printf

type testHolderOperator struct {
	indexSeen, indexProcessed       int
	fieldSeen, fieldProcessed       int
	viewSeen, viewProcessed         int
	fragmentSeen, fragmentProcessed int
	waitHere                        chan struct{}
}

func (t *testHolderOperator) CheckIndex(string) (bool, bool) {
	t.indexSeen++
	return true, true
}

func (t *testHolderOperator) CheckField(string, string) (bool, bool) {
	t.fieldSeen++
	return true, true
}

func (t *testHolderOperator) CheckView(string, string, string) (bool, bool) {
	t.viewSeen++
	return true, true
}

func (t *testHolderOperator) CheckFragment(string, string, string, uint64) bool {
	t.fragmentSeen++
	return true
}

func (t *testHolderOperator) ProcessIndex(*Index) error {
	t.indexProcessed++
	return nil
}

func (t *testHolderOperator) ProcessField(*Field) error {
	t.fieldProcessed++
	return nil
}

func (t *testHolderOperator) ProcessView(*view) error {
	t.viewProcessed++
	return nil
}

func (t *testHolderOperator) ProcessFragment(*fragment) error {
	if t.waitHere != nil {
		<-t.waitHere
	}
	t.fragmentProcessed++
	return nil
}

func makeHolder(tb testing.TB, backend string) (*Holder, string, error) {
	path, err := testhook.TempDir(tb, "pilosa-")
	if err != nil {
		return nil, "", err
	}
	cfg := mustHolderConfig()
	if backend != "" {
		cfg.StorageConfig.Backend = backend
	}
	h := NewHolder(path, cfg)
	return h, path, h.Open()
}

func testSetBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) {

	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	f, err := idx.CreateFieldIfNotExists(field, OptFieldTypeDefault())
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
	_, err = f.SetBit(nil, rowID, columnID, nil)
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
}

func testMustHaveBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) {

	//shard := columnID / ShardWidth

	// hmm... if its a new holder, meta data isn't there, so ask for it.
	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	panicOn(err)

	f := idx.Field(field)
	if f == nil {
		t.Fatalf("no such field '%v'", field)
	}

	row, err := f.Row(nil, rowID)
	if err != nil {
		t.Fatalf("error getting field.Row(rowID=%v): %v", rowID, err)
	}

	cols := row.Columns()
	if len(cols) == 0 {
		t.Fatalf("error getting field.Row().Columns(): empty columns, colID %v bit was not hot", columnID)
	}

	for _, c := range cols {
		if c == columnID {
			return // ok, found it.
		}
	}
	t.Fatalf("error getting field.Row().Columns(): colID %v bit was not hot", columnID)
}

func testMustNotHaveBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) {
	if testHasBit(t, h, index, field, rowID, columnID) {
		t.Fatalf("error, expected no bit but this bit was hot: index='%v', field='%v', rowID='%v', columnID='%v'", index, field, rowID, columnID)
	}
}

func testHasBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) bool {

	idx := h.Index(index)
	if idx == nil {
		return false // not even an index by this name. Obviously no hot bits either.
	}

	f := idx.Field(field)
	if f == nil {
		return false
	}

	row, err := f.Row(nil, rowID)
	if err != nil {
		return false
	}

	cols := row.Columns()
	if len(cols) == 0 {
		return false
	}

	for _, c := range cols {
		if c == columnID {
			return true // ok, found it.
		}
	}
	return false
}

func TestHolderOperatorProcess(t *testing.T) {
	h, path, err := makeHolder(t, "")
	if err != nil {
		t.Fatalf("creating holder: %v", err)
	}
	defer os.RemoveAll(path)
	defer h.Close()

	// Write bits to separate indexes.
	testSetBit(t, h, "i0", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 12345678)

	testOp := testHolderOperator{}
	ctx := context.Background()
	err = h.Process(ctx, &testOp)
	if err != nil {
		t.Fatalf("processing holder: %v", err)
	}
	expected := testHolderOperator{
		indexSeen: 2, indexProcessed: 2,
		fieldSeen: 2, fieldProcessed: 2,
		viewSeen: 2, viewProcessed: 2,
		fragmentSeen: 3, fragmentProcessed: 3,
	}
	if testOp != expected {
		t.Fatalf("holder processor did not process as expected. expected %#v, got %#v", expected, testOp)
	}
}

func TestHolderOperatorCancel(t *testing.T) {
	h, path, err := makeHolder(t, "")
	if err != nil {
		t.Fatalf("creating holder: %v", err)
	}
	defer os.RemoveAll(path)
	defer h.Close()

	// Write bits to separate indexes.
	testSetBit(t, h, "i0", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 200)
	testSetBit(t, h, "i1", "f", 100, 12345678)

	// Here, we want to ensure that the operation gets cancelled
	// successfully. In practice we expect it to process one fragment, then
	// end up blocked on the waitHere, then get cancelled... But the
	// waitHere blockage isn't really something holder.Process can do
	// anything about, so we close the channel, so two fragments are
	// processed. But in theory you could end up with only one fragment
	// processed if this goroutine managed to cancel before the processor
	// gets to the next fragment. Point is, it shouldn't hit all three,
	// because the checks against the cancellation should fire before it
	// gets there.
	testOp := testHolderOperator{waitHere: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		err = h.Process(ctx, &testOp)
		close(done)
	}()
	testOp.waitHere <- struct{}{}
	cancel()
	close(testOp.waitHere)
	<-done
	if err != context.Canceled {
		t.Fatalf("processing holder: expected context.Canceled, got %v", err)
	}
	testOp.waitHere = nil
	expected := testHolderOperator{
		indexSeen: 2, indexProcessed: 2,
		fieldSeen: 2, fieldProcessed: 2,
		viewSeen: 2, viewProcessed: 2,
		fragmentSeen: 3, fragmentProcessed: 3,
	}
	if testOp == expected {
		t.Fatalf("holder processor did not cancel. expected something other than %#v", expected)
	}
}

// mustHolderConfig is meant to help minimize the number of places in the code
// where we're reading the PILOSA_STORAGE_BACKEND environment variable for
// testing purposes. Ideally we would handle this differently, but this is a
// first attempt at improving things. Note: the actual os.Getenv() call was
// moved to the CurrentBackend() function.
func mustHolderConfig() *HolderConfig {
	cfg := DefaultHolderConfig()
	if backend := CurrentBackend(); backend != "" {
		_ = MustTxsrcToTxtype(backend)
		cfg.StorageConfig.Backend = backend
	}
	return cfg
}
