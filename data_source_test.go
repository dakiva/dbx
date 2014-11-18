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
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

const (
	pgType      = "postgres"
	postgresDsn = "POSTGRES_DSN"
)

func TestConstructDataSource(t *testing.T) {
	// given
	pgdsn := os.Getenv(postgresDsn)
	schema := "public"

	// when
	source, err := NewDataSource(pgType, pgdsn, schema, "test_queries.json")

	// then
	assert.Nil(t, err)

	// verify the search path
	tx, err := source.Begin()
	assert.Nil(t, err)
	row := tx.QueryRow("SHOW search_path")
	val := ""
	row.Scan(&val)
	assert.Equal(t, schema, val)

	// verify the queries were loaded
	query, ok := source.FindQuery(query1)
	assert.True(t, ok)
	assert.Equal(t, "query1", query)
}

func TestDataSourceNamedExecutions(t *testing.T) {
	// given
	pgdsn := os.Getenv(postgresDsn)
	// create the schema
	schema := fmt.Sprintf("schema%v", time.Now().Unix())
	db, _ := sqlx.Connect(pgType, pgdsn)
	defer db.Close()
	defer db.Exec(fmt.Sprintf("DROP SCHEMA %v CASCADE", schema))
	db.Exec(fmt.Sprintf("CREATE SCHEMA %v", schema))
	expected := "value"

	// when
	source, _ := NewDataSource(pgType, pgdsn, schema)
	_, err := source.NamedExec("CREATE TABLE test (col text)", make(map[string]interface{}))
	assert.Nil(t, err)
	_, err = source.NamedExec("INSERT INTO test VALUES ('value')", make(map[string]interface{}))
	assert.Nil(t, err)

	// then
	rows, err := source.NamedQuery("SELECT col FROM test", make(map[string]interface{}))
	assert.Nil(t, err)
	assert.True(t, rows.Next())
	var v sql.NullString
	rows.Scan(&v)
	assert.True(t, v.Valid)
	assert.Equal(t, expected, v.String)
}

func TestNamedPreparedStatement(t *testing.T) {
	// given
	pgdsn := os.Getenv(postgresDsn)
	// create the schema
	schema := fmt.Sprintf("schema%v", time.Now().Unix())
	db, _ := sqlx.Connect(pgType, pgdsn)
	defer db.Close()
	defer db.Exec(fmt.Sprintf("DROP SCHEMA %v CASCADE", schema))
	db.Exec(fmt.Sprintf("CREATE SCHEMA %v", schema))
	expected := "value"

	// when
	source, _ := NewDataSource(pgType, pgdsn, schema)
	source.NamedExec("CREATE TABLE test (col text)", make(map[string]interface{}))
	params := make(map[string]interface{})
	params["val"] = expected
	stmt, err := source.PrepareNamed("INSERT INTO test VALUES (:val)")

	// then
	assert.Nil(t, err)
	_, err = stmt.Exec(params)
	assert.Nil(t, err)

	queryStmt, err := source.PrepareNamed("SELECT col FROM test")
	assert.Nil(t, err)
	rows, err := queryStmt.Query(make(map[string]interface{}))
	assert.Nil(t, err)
	assert.True(t, rows.Next())
	var v sql.NullString
	rows.Scan(&v)
	assert.True(t, v.Valid)
	assert.Equal(t, expected, v.String)
}
