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

package test_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2/disco"
	"github.com/pilosa/pilosa/v2/test"
)

func TestNewCluster(t *testing.T) {
	numNodes := 3
	cluster := test.MustRunCluster(t, numNodes)
	defer cluster.Close()

	primary := getPrimary(cluster.Nodes[0])
	for i := 1; i < numNodes; i++ {
		if coordi := getPrimary(cluster.Nodes[i]); coordi != primary {
			t.Fatalf("node %d does not have the same primary as node 0. '%v' and '%v' respectively", i, coordi, primary)
		}
	}
	req, err := http.NewRequest(
		"GET",
		cluster.Nodes[0].URL()+"/status",
		strings.NewReader(""),
	)
	if err != nil {
		t.Fatalf("creating http request: %v", err)
	}

	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("sending request: %v", err)
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	body := struct {
		State string
		Nodes []struct {
			Scheme string
			Host   string
			Port   int
		}
	}{}

	err = dec.Decode(&body)
	if err != nil {
		t.Fatalf("decoding status response: %v", err)
	}

	bytes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}

	if len(body.Nodes) != 3 {
		t.Fatalf("wrong number of nodes in status: %s", bytes)
	}

	if body.State != string(disco.ClusterStateNormal) {
		t.Fatalf("cluster state should be %s but is %s", disco.ClusterStateNormal, body.State)
	}
}

func getPrimary(m *test.Command) string {
	hosts := m.API.Hosts(context.Background())
	for _, host := range hosts {
		if host.IsPrimary {
			return host.ID
		}
	}
	panic("no primary in cluster")
}
