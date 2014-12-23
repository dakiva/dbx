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
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	postgresDsn  = "POSTGRES_DSN"
	rolePassword = "password"
)

var testSchema string

// Initializes and migrates a test schema, returning a DB object that has the proper search path
// set to the initialized schema.
// Accepts a dsn "user= password= dbname= host= port= sslmode=[disable|require|verify-ca|verify-full] connect-timeout=
func InitializeTestDB(migrationsDir string) (*sqlx.DB, error) {
	testSchema = fmt.Sprintf("schema%v", time.Now().Unix())
	pgdsn := os.Getenv(postgresDsn)
	return InitializeDB(pgdsn, testSchema, rolePassword, migrationsDir)
}

// Initializes and migrates a test schema, returning a DB object that has the proper search path
// set to the initialized schema. This function will panic on an error.
func MustInitializeTestDB(migrationsDir string) *sqlx.DB {
	db, err := InitializeTestDB(migrationsDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing test database: %v", err))
	}
	return db
}

// Drops the test schema, returning an error if dropping the schema fails.
func TearDownTestDB() error {
	pgdsn := os.Getenv(postgresDsn)
	db, err := sqlx.Connect(pgType, pgdsn)
	if err != nil {
		return err
	}
	return DropSchema(testSchema, db)
}
