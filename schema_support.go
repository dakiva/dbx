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

	"github.com/jmoiron/sqlx"
)

// Creates a new Postgres schema along with a specific role as the owner.
func CreateSchema(schema, password string, db *sqlx.DB) error {
	if schema == "" {
		return errors.New("Empty schema name specified")
	}
	_, err := db.Exec(fmt.Sprintf("CREATE ROLE %v WITH LOGIN ENCRYPTED PASSWORD '%v'", schema, password))
	if err != nil {
		return err
	}
	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA %v AUTHORIZATION %v", schema, schema))
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

// Takes an existing, valid dsn and replaces the user name with the specified role name.
func CreateDsnForRole(existingDsn, role, password string) string {
	dsnMap := ParseDsn(existingDsn)
	dsnMap["user"] = role
	dsnMap["password"] = password
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
