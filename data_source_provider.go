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

	"github.com/jmoiron/sqlx"
)

const (
	POSTGRES = "postgres"
)

// A protocol for providing data from an underlying source such as a database. The exposure of transaction semantics here is deliberate as this provider upholds and enforces transactional guarantees as defined by implementors. Providers are intended to abstract the loading of named queries. Look at query_loader.go for details.
type DataSourceProvider interface {
	// find a query by identifier.
	FindNamedQuery(name QueryIdentifier) (string, bool)
	Begin() (*sqlx.Tx, error)
}

// A provider that is scoped to a Postgres schema. All transactions and connections are guranteed to provide the proper query namespace.
type SchemaScopedProvider struct {
	namedQueryMap map[QueryIdentifier]QueryValue
	db            *sqlx.DB
	schema        string
}

// Returns a query for the specific name. Also returns true if a query was found for the name, false otherwise.
func (this *SchemaScopedProvider) FindNamedQuery(name QueryIdentifier) (string, bool) {
	value, ok := this.namedQueryMap[name]
	return value.Query, ok
}

// Starts a new transaction utilizing the guarantees prvoided by the underlying data source. The transaction and underlying connection is guaranteed to provide the proper query namespace. As such, queries are not required to, and are advised against, providing full table namespaces. If a table name poses an ambiguity in the underlying data model due to multiple references in schemas, the table in the explicit schema will be referenced.
func (this *SchemaScopedProvider) Begin() (*sqlx.Tx, error) {
	tx, err := this.db.Beginx()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(fmt.Sprintf("SET search_path TO %v", this.schema))
	if err != nil {
		return nil, err
	}
	return tx, nil
}

// Creates a new provider scoped to a specfic postgres schema.
func NewSchemaScopedProvider(dsn, schema string, queryFiles ...string) (*SchemaScopedProvider, error) {
	if schema == "" || dsn == "" {
		return nil, errors.New("Schema name and dsn must be specified.")
	}
	queryMap, err := LoadNamedQueries(queryFiles...)
	if err != nil {
		return nil, err
	}

	db, err := sqlx.Connect(POSTGRES, dsn)
	if err != nil {
		return nil, err
	}
	return &SchemaScopedProvider{namedQueryMap: queryMap, db: db, schema: schema}, nil
}
