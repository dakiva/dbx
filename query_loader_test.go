// Copyright 2014 Daniel Akiva

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dbx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	query1 QueryIdentifier = "Query1"
	query2                 = "Query2"
)

func TestBadNamedQueryFile(t *testing.T) {
	_, err := LoadNamedQueries("abc")

	assert.NotNil(t, err)
}

func TestLoadNamedQueries(t *testing.T) {
	// when
	queryMap, err := LoadNamedQueries("test_queries.json")

	// then
	assert.Nil(t, err)
	assert.Equal(t, 2, len(queryMap))

	value, ok := queryMap[query1]
	assert.True(t, ok)
	assert.Equal(t, "query1", value.Query)
	assert.Equal(t, "description1", value.Description)

	value, ok = queryMap[query2]
	assert.True(t, ok)
	assert.Equal(t, "duplicate", value.Query)
	assert.Equal(t, "duplicate description", value.Description)
}
