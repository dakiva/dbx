// Copyright 2019 Daniel Akiva

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
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	rolePassword = "password"
)

// InitializeTestDB initializes and migrates a test schema, returning a DB object that has the proper search path set to the initialized schema. Requires a dsn in the form "user= password= dbname= host= port= sslmode=[disable|require|verify-ca|verify-full] connect-timeout=
func InitializeTestDB(dsn, schemaName, migrationsDir string) (*sqlx.DB, error) {
	return InitializeDB(dsn, schemaName, rolePassword, migrationsDir)
}

// MustInitializeTestDB calls InitializeTestDB panics on an error.
func MustInitializeTestDB(dsn, schemaName, migrationsDir string) *sqlx.DB {
	db, err := InitializeTestDB(dsn, schemaName, migrationsDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing test database: %v", err))
	}
	return db
}

// TearDownTestDB drops the test schema, returning an error if dropping the schema fails.
func TearDownTestDB(dsn, schemaName string) error {
	db, err := sqlx.Connect(PostgresType, dsn)
	if err != nil {
		return err
	}
	return DropSchema(schemaName, db)
}

// GenerateSchemaName generates a unique schema name suitable for use during testing.
func GenerateSchemaName(prefix string) string {
	return fmt.Sprintf("%v%v", prefix, time.Now().Unix())
}
