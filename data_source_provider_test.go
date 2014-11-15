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
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

const (
	postgresDsn = "POSTGRES_DSN"
)

func TestSchemaBasedProvider(t *testing.T) {
	// given
	pgdsn := os.Getenv(postgresDsn)
	schema := "public"

	// when
	provider, err := NewSchemaScopedProvider(pgdsn, schema, "test_queries.json")

	// then
	assert.Nil(t, err)

	// verify the search path
	tx, err := provider.Begin()
	assert.Nil(t, err)
	row := tx.QueryRow("SHOW search_path")
	val := ""
	row.Scan(&val)
	assert.Equal(t, schema, val)

	// verify the queries were loaded
	query, ok := provider.FindNamedQuery(query1)
	assert.True(t, ok)
	assert.Equal(t, "query1", query)
}
