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

package ctl

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"context"

	"github.com/molecula/featurebase/v2/testhook"
)

func TestCheckCommand_RunCacheFile(t *testing.T) {
	fi, err := testhook.TempFile(t, "test*.cache")
	if err != nil {
		t.Fatalf("creating test file: %v", err)
	}
	cacheFile := fi.Name()

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{cacheFile}

	err = cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}

	if !strings.Contains(buf.String(), "ignoring cache file") {
		t.Fatalf("expect: ignoring cache file, actual: '%s'", err)
	}
}

func TestCheckCommand_RunSnapshot(t *testing.T) {
	fi, err := testhook.TempFile(t, "test*.snapshotting")
	if err != nil {
		t.Fatalf("creating test file: %v", err)
	}
	snapshotFile := fi.Name()

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{snapshotFile}

	err = cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}

	if !strings.Contains(buf.String(), "ignoring snapshot file") {
		t.Fatalf("expect: ignoring snapshot file, actual: '%s'", err)
	}
}

func TestCheckCommand_Run(t *testing.T) {
	file, err := testhook.TempFile(t, "run-command")
	if err != nil {
		t.Fatal(err)
	}
	fname := file.Name()
	if _, err := file.Write([]byte("1234,1223")); err != nil {
		t.Fatalf("writing to temp file: %v", err)
	}
	file.Close()

	rder := []byte{}
	stdin := bytes.NewReader(rder)
	r, w, _ := os.Pipe()
	cm := NewCheckCommand(stdin, w, w)
	cm.Paths = []string{fname}

	err = cm.Run(context.Background())
	w.Close()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		t.Fatalf("copy: %v", err)
	}

	expectedPrefix := "checking bitmap: unmarshalling: "
	if !strings.HasPrefix(err.Error(), expectedPrefix) {
		t.Fatalf("expect error: '%s...', actual: '%s'", expectedPrefix, err)
	}
	//	Todo: need correct roaring file for happy path
}
