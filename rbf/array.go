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

package rbf

import (
	"unsafe"

	"github.com/pilosa/pilosa/v2/roaring"
)

// toArray16 converts a byte slice into a slice of uint16 values using unsafe.
func toArray16(a []byte) []uint16 {
	return (*[4096]uint16)(unsafe.Pointer(&a[0]))[: len(a)/2 : len(a)/2]
}

// fromArray16 converts a slice of uint16 values into a byte slice using unsafe.
func fromArray16(a []uint16) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*2 : len(a)*2]
}

// arrayIndex returns the insertion index of v in a. Returns true if exact match.
func arrayIndex(a []uint16, v uint16) (int, bool) {
	return search(len(a), func(i int) int {
		if a[i] == v {
			return 0
		} else if v < a[i] {
			return -1
		}
		return 1
	})
}

// toArray64 converts a byte slice into a slice of uint64 values using unsafe.
func toArray64(a []byte) []uint64 {
	return (*[1024]uint64)(unsafe.Pointer(&a[0]))[:1024:1024]
}

// fromArray64 converts a slice of uint64 values into a byte slice using unsafe.
func fromArray64(a []uint64) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[:8192:8192]
}

func cloneArray64(a []uint64) []uint64 {
	other := make([]uint64, len(a))
	copy(other, a)
	return other
}

// toArray16 converts a byte slice into a slice of uint16 values using unsafe.
func toInterval16(a []byte) []roaring.Interval16 {
	return (*[2048]roaring.Interval16)(unsafe.Pointer(&a[0]))[: len(a)/4 : len(a)/4]
}

// fromArray16 converts a slice of uint16 values into a byte slice using unsafe.
func fromInterval16(a []roaring.Interval16) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*4 : len(a)*4]
}