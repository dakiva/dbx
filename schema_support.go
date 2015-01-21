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
	"strings"

	"bitbucket.org/liamstask/goose/lib/goose"

	"github.com/jmoiron/sqlx"
)

const (
	pgType = "postgres"
)

// Initializes and migrates a schema, returning a DB object that has the proper search path
// set to the initialized schema.
// Accepts a dsn "user= password= dbname= host= port= sslmode=[disable|require|verify-ca|verify-full] connect-timeout=" The role must have privileges to create a new database schema.
// Schema must be set to a valid schema
// migrationsDir is the path to the migration scripts. This function uses goose to migrate the
// schema
func InitializeDB(pgdsn, schema, schemaPassword, migrationsDir string) (*sqlx.DB, error) {
	if pgdsn == "" {
		return nil, errors.New("Postgres dsn must not be empty.")
	}
	db, err := sqlx.Connect(pgType, pgdsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	err = EnsureSchema(schema, schemaPassword, db)
	if err != nil {
		return nil, err
	}
	schemaDsn := CreateDsnForRole(pgdsn, schema, schemaPassword)
	err = MigrateSchema(schemaDsn, schema, migrationsDir)
	if err != nil {
		return nil, err
	}
	return sqlx.Connect(pgType, schemaDsn)
}

// Initializes and migrates a schema, returning a DB object thathas the proper search path
// set to the initialized schema. This function will panic on an error.
func MustInitializeDB(pgdsn, schema, schemaPassword, migrationsDir string) *sqlx.DB {
	db, err := InitializeDB(pgdsn, schema, schemaPassword, migrationsDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing database: %v", err))
	}
	return db
}

// Migrates a Postgres schema. Returns an error if migration fails.
func MigrateSchema(pgdsn, schema, migrationsDir string) error {
	// only supports Postgres
	driver := goose.DBDriver{
		Name:    pgType,
		OpenStr: pgdsn,
		Import:  "github.com/lib/pq",
		Dialect: &goose.PostgresDialect{},
	}
	conf := &goose.DBConf{
		MigrationsDir: migrationsDir,
		Env:           "",
		Driver:        driver,
		PgSchema:      schema,
	}
	targetVersion, err := goose.GetMostRecentDBVersion(migrationsDir)
	if err != nil {
		return err
	}
	return goose.RunMigrations(conf, migrationsDir, targetVersion)
}

// Creates a new Postgres schema along with a specific role as the owner if neither exist.
// Returns an error if schema creation fails. If the schema and/or role already exists, this
// function ignores and continues without creation.
func EnsureSchema(schema, password string, db *sqlx.DB) error {
	if schema == "" {
		return errors.New("Empty schema name specified")
	}
	rows, err := db.Query(fmt.Sprintf("SELECT rolname FROM pg_roles WHERE rolname = '%v'", schema))
	if !rows.Next() {
		_, err = db.Exec(fmt.Sprintf("CREATE ROLE %v WITH LOGIN ENCRYPTED PASSWORD '%v'", schema, password))
		if err != nil {
			return err
		}
	} else {
		// schema already exists, update the password
		_, err = db.Exec(fmt.Sprintf("ALTER ROLE %v WITH ENCRYPTED PASSWORD '%v'", schema, password))
		if err != nil {
			return err
		}
	}
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v", schema))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("ALTER SCHEMA %v OWNER TO %v", schema, schema))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("ALTER ROLE %v SET search_path TO %v", schema, schema))
	if err != nil {
		return err
	}
	return nil
}

// Drops a Postgres schema along with the specific role owner. Exercise caution when using this method.
func DropSchema(schema string, db *sqlx.DB) error {
	if schema == "" {
		return errors.New("Empty schema name specified")
	}
	_, err := db.Exec(fmt.Sprintf("DROP SCHEMA IF EXISTS %v CASCADE", schema))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("DROP ROLE IF EXISTS %v", schema))
	if err != nil {
		return err
	}
	return nil
}

// Returns the current schema version, or an error if the version could not be determined.
// This function will create the migrations versions table if the migrations table does not
// exist.
func GetCurrentSchemaVersion(schema string, db *sqlx.DB) (int64, error) {
	// only supports Postgres
	driver := goose.DBDriver{
		Name: pgType,
		// open str is unused for checking the schema version
		OpenStr: "",
		Import:  "github.com/lib/pq",
		Dialect: &goose.PostgresDialect{},
	}
	conf := &goose.DBConf{
		// migrations dir is not needed for checking the schema version
		MigrationsDir: "",
		Env:           "",
		Driver:        driver,
		PgSchema:      schema,
	}
	// ensure the search path is set so that a versions table is not inadvertently created
	// for the wrong schema.
	_, err := db.Exec(fmt.Sprintf("SET search_path TO %v", schema))
	if err != nil {
		return -1, err
	}
	return goose.EnsureDBVersion(conf, db.DB)
}

// Takes an existing, valid dsn and replaces the user name with the specified role name.
// If the password is non-empty, sets the password.
func CreateDsnForRole(existingDsn, role, password string) string {
	dsnMap := ParseDsn(existingDsn)
	dsnMap["user"] = role
	if password != "" {
		dsnMap["password"] = password
	}
	return BuildDsn(dsnMap)
}

// Parses a dsn into a map
func ParseDsn(dsn string) map[string]string {
	dsnMap := make(map[string]string)
	params := strings.Split(dsn, " ")
	for _, param := range params {
		pair := strings.Split(param, "=")
		dsnMap[pair[0]] = pair[1]
	}
	return dsnMap
}

// Builds a dsn from a map
func BuildDsn(dsnMap map[string]string) string {
	dsn := ""
	for param, value := range dsnMap {
		if dsn != "" {
			dsn += " "
		}
		dsn += fmt.Sprintf("%v=%v", param, value)
	}
	return dsn
}
