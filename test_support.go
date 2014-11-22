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
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	postgresDsn  = "POSTGRES_DSN"
	pgType       = "postgres"
	rolePassword = "password"
)

// Initializes and migrates a test schema, returning a DB object that has the proper search path
// set to the initialized schema.
// Accepts a dsn "user= password= dbname= host= port= sslmode=[disable|require|verify-ca|verify-full] connect-timeout=
func InitializeTestDB(migrationsDir string) (*sqlx.DB, error) {
	schema := fmt.Sprintf("schema%v", time.Now().Unix())
	pgdsn := os.Getenv(postgresDsn)
	if pgdsn == "" {
		return nil, errors.New("Error retrieving Postgres dsn.")
	}
	db, err := sqlx.Connect(pgType, pgdsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	err = CreateSchema(schema, rolePassword, db)
	if err != nil {
		return nil, err
	}
	schemaDsn := CreateDsnForRole(pgdsn, schema, rolePassword)
	err = MigrateSchema(schemaDsn, schema, migrationsDir)
	if err != nil {
		return nil, err
	}
	return sqlx.Connect(pgType, schemaDsn)
}

func MustInitializeTestDB(migrationsDir string) *sqlx.DB {
	db, err := InitializeTestDB(migrationsDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing test database: %v", err))
	}
	return db
}
