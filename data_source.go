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
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

const (
	searchPath = "SET search_path TO %v;"
)

// A protocol for providing data from an underlying database. The lack of exposure of transaction semantics here is deliberate as transactions can be adapted to this protocol. In other words, a DB object or Tx object can conform to this interface, provided that sqlx is used for named query support.
type DBContext interface {
	// Execute a query that contains named query parameters, returning result metadata or an error.
	NamedExec(query string, arg interface{}) (sql.Result, error)
	// Execute a query that contains named parameters. Retuns rows returned by the database or an error.
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	// Prepares a query with named parameters. Returns a prepared statement or an error.
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

// A source that is scoped to a schema. All transactions and connections are guranteed to provide the proper query namespace if one is defined.
type DataSource struct {
	namedQueryMap map[QueryIdentifier]QueryValue
	db            *sqlx.DB
	schema        string
}

// Finds and returns a query for the given identifier. Also returns a flag determining whether a value was found and returned.
func (this *DataSource) FindQuery(name QueryIdentifier) (string, bool) {
	if value, ok := this.namedQueryMap[name]; ok {
		return value.Query, ok
	}
	return "", false
}

// Starts a new transaction utilizing the guarantees prvoided by the underlying data source. The transaction and underlying connection are guaranteed to provide the proper query namespace if one is used. As such, callers are not required to, and are advised against, providing full table namespaces in queries.
func (this *DataSource) Begin() (*sqlx.Tx, error) {
	tx, err := this.db.Beginx()
	if err != nil {
		return nil, err
	}
	if this.schema != "" {
		_, err = tx.Exec(fmt.Sprintf(searchPath, this.schema))
		if err != nil {
			return nil, err
		}
	}
	return tx, nil
}

// Execute a query that contains named query parameters, returning result metadata or an error.
// The arg parameter should be passed as a map[string]interface{}.
func (this *DataSource) NamedExec(query string, arg interface{}) (sql.Result, error) {
	return this.db.NamedExec(this.namespaceQuery(query), arg)
}

// Execute a query that contains named parameters. Retuns rows returned by the database or an error.
// The arg parameter should be passed as a map[string]interface{}.
func (this *DataSource) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	return this.db.NamedQuery(this.namespaceQuery(query), arg)
}

// Prepares a query with named parameters. Returns a prepared statement or an error.
func (this *DataSource) PrepareNamed(query string) (*sqlx.NamedStmt, error) {
	return this.db.PrepareNamed(query)
}

func (this *DataSource) namespaceQuery(query string) string {
	namespacedQuery := ""
	if this.schema != "" {
		namespacedQuery = fmt.Sprintf(searchPath, this.schema)
	}
	return namespacedQuery + query
}

// Creates a new provider, optionally scoped to a specific schema.
func NewDataSource(dbType, dsn, schema string, queryFiles ...string) (*DataSource, error) {
	if dbType == "" || dsn == "" {
		return nil, errors.New("dbType and dsn must be specified.")
	}
	queryMap, err := LoadNamedQueries(queryFiles...)
	if err != nil {
		return nil, err
	}

	db, err := sqlx.Connect(dbType, dsn)
	if err != nil {
		return nil, err
	}
	return &DataSource{namedQueryMap: queryMap, db: db, schema: schema}, nil
}
