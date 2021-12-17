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
package authz_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/authz"
)

func TestAuth_ReadPermissionsFile(t *testing.T) {

	singleInput := `"dca35310-ecda-4f23-86cd-876aee55906b":
  "test": "read"`

	multiInput := `"dca35310-ecda-4f23-86cd-876aee55906b":
  "test": "read"
"dca35310-ecda-4f23-86cd-876aee559900":
  "test": "admin"`

	singleStruct := map[string]map[string]string{
		"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "read"},
	}

	multiStruct := map[string]map[string]string{
		"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "read"},
		"dca35310-ecda-4f23-86cd-876aee559900": {"test": "admin"},
	}

	tests := []struct {
		input  string
		output map[string]map[string]string
	}{
		{singleInput, singleStruct},
		{multiInput, multiStruct},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			permFile := strings.NewReader(test.input)

			var p authz.GroupPermissions
			err := p.ReadPermissionsFile(permFile)
			if err != nil {
				t.Fatalf("readPermissionsFile error: %s", err)
			}

			if !reflect.DeepEqual(p.Permissions, test.output) {
				t.Fatalf("expected output %s, but got %s", test.output, p.Permissions)
			}
		},
		)
	}
}

func TestAuth_GetPermissions(t *testing.T) {

	// initializes different example of permissions file in yaml
	permissions1 := `"dca35310-ecda-4f23-86cd-876aee55906b":
  "test": "read"`

	permissions2 := `"dca35310-ecda-4f23-86cd-876aee559900":
  "test": "write"`

	permissions3 := `"dca35310-ecda-4f23-86cd-876aee55906b":
  "test": "write"
  "test2": "read"
"dca35310-ecda-4f23-86cd-876aee559900":
  "test": "admin"`

	permissions4 := `"dca35310-ecda-4f23-86cd-876aee559900":
  "test": ""`

	// initializes groups that are returned from identity provider
	groupName := "name"
	userId := "user-id"
	groupsList1 := []authz.Group{}
	groupsList2 := []authz.Group{{userId, "fake-group", groupName}}
	groupsList3 := []authz.Group{
		{userId, "dca35310-ecda-4f23-86cd-876aee55906b", groupName},
		{userId, "dca35310-ecda-4f23-86cd-876aee559900", groupName},
	}

	tests := []struct {
		yamlData   string
		groups     []authz.Group
		index      string
		userAccess string
		err        string
	}{
		{
			permissions1,
			groupsList1,
			"test",
			"",
			"user is not part of any groups in identity provider",
		},
		{
			permissions1,
			groupsList3,
			"test1",
			"",
			"does not have permission to index",
		},
		{
			permissions2,
			groupsList2,
			"test",
			"",
			"does not have permission to FeatureBase",
		},
		{
			permissions1,
			groupsList3,
			"test",
			"read",
			"",
		},
		{
			permissions2,
			groupsList3,
			"test",
			"write",
			"",
		},
		{
			permissions3,
			groupsList3,
			"test",
			"admin",
			"",
		},
		{
			permissions4,
			groupsList3,
			"test",
			"",
			"no permissions found",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			permFile := strings.NewReader(test.yamlData)

			var p authz.GroupPermissions
			if err := p.ReadPermissionsFile(permFile); err != nil {
				t.Errorf("Error: %s", err)
			}

			p1, err := p.GetPermissions(test.groups, test.index)

			if p1 != test.userAccess {
				t.Errorf("expected permission to be %s, but got %s", test.userAccess, p1)
			}

			if err != nil {
				if !strings.Contains(err.Error(), test.err) {
					t.Errorf("expected error to contain %s, but got %s", test.err, err.Error())
				}
			}

		})
	}
}

func TestAuth_IsAdmin(t *testing.T) {

	group := []authz.Group{
		{"user-is", "dca35310-ecda-4f23-86cd-876aee55906b", "group-name"},
	}

	groupPermissions1 := map[string]map[string]string{
		"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "admin"},
	}

	groupPermissions2 := map[string]map[string]string{
		"dca35310-ecda-4f23-86cd-876aee55906b": {"test": "read"},
	}

	tests := []struct {
		groups           []authz.Group
		groupPermissions map[string]map[string]string
		output           bool
	}{
		{
			group, groupPermissions1, true,
		},
		{
			group, groupPermissions2, false,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			p := authz.GroupPermissions{test.groupPermissions}
			resp := p.IsAdmin(test.groups)
			if resp != test.output {
				t.Errorf("expected %t, but got %t", test.output, resp)
			}
		})
	}
}

func TestAuth_GetAuthorizedIndexList(t *testing.T) {

	group := []authz.Group{
		{"user-is", "dca35310-ecda-4f23-86cd-876aee55906b", "group-name"},
	}

	p := authz.GroupPermissions{map[string]map[string]string{
		"dca35310-ecda-4f23-86cd-876aee55906b": {
			"test1": "admin",
			"test2": "read",
			"test3": "write",
		},
	}}

	tests := []struct {
		groups     []authz.Group
		permission string
		output     []string
	}{
		{
			group,
			"read",
			[]string{"test1", "test2", "test3"},
		},
		{
			group,
			"admin",
			[]string{"test1"},
		},
		{
			group,
			"write",
			[]string{"test1", "test3"},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {

			indexList := p.GetAuthorizedIndexList(test.groups, test.permission)
			sort.Strings(indexList)

			if !reflect.DeepEqual(indexList, test.output) {
				t.Errorf("expected %s, but got %s", test.output, indexList)
			}
		})
	}

}
