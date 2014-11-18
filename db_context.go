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

	"github.com/jmoiron/sqlx"
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
