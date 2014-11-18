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

func TestSchemaCreation(t *testing.T) {
	// given
	pgdsn := os.Getenv(postgresDsn)
	// create the schema
	schema := fmt.Sprintf("schema%v", time.Now().Unix())
	db, err := sqlx.Connect(pgType, pgdsn)
	assert.Nil(t, err)
	defer db.Close()
	defer DropSchema(schema, db)

	// when
	err = CreateSchema(schema, db)

	// then
	assert.Nil(t, err)

	// verify the search path using the new Role
	db2, err := sqlx.Connect(pgType, CreateDsnForRole(pgdsn, schema))
	assert.Nil(t, err)
	defer db2.Close()
	row := db2.QueryRow("SHOW search_path")
	val := ""
	row.Scan(&val)
	assert.Equal(t, schema, val)

}

func TestParseDsn(t *testing.T) {
	// given
	dsn := "user=abc password=secret dbname=database host=localhost port=5432 sslmode=disable"
	// when
	dsnMap := ParseDsn(dsn)

	// then
	assert.Equal(t, "abc", dsnMap["user"])
	assert.Equal(t, "secret", dsnMap["password"])
	assert.Equal(t, "localhost", dsnMap["host"])
	assert.Equal(t, "5432", dsnMap["port"])
	assert.Equal(t, "disable", dsnMap["sslmode"])
	assert.Equal(t, "database", dsnMap["dbname"])
}

func TestBuildDsn(t *testing.T) {
	// given
	dsnMap := make(map[string]string)
	dsnMap["user"] = "abc"
	dsnMap["password"] = "secret"
	dsnMap["host"] = "localhost"
	dsnMap["port"] = "5432"
	dsnMap["sslmode"] = "disable"
	dsnMap["dbname"] = "database"

	// when
	dsn := BuildDsn(dsnMap)
	parsedMap := ParseDsn(dsn)

	// then
	assert.Equal(t, dsnMap, parsedMap)
}

func TestCreateDsnForRole(t *testing.T) {
	// given
	dsn := "user=abc password=secret dbname=database host=localhost port=5432 sslmode=disable"
	role := "test"

	// when
	modifiedDsn := CreateDsnForRole(dsn, role)
	dsnMap := ParseDsn(modifiedDsn)

	// then
	assert.Equal(t, role, dsnMap["user"])
	assert.Equal(t, "secret", dsnMap["password"])
	assert.Equal(t, "localhost", dsnMap["host"])
	assert.Equal(t, "5432", dsnMap["port"])
	assert.Equal(t, "disable", dsnMap["sslmode"])
	assert.Equal(t, "database", dsnMap["dbname"])
}
