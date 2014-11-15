dbx
===

A library that provides database extensions for Go. These tools are intended to work with both sql and sqlx.

Overview
========
Nogo provides easy to use role-based access controls for servers as well as access control lists (ACLs) support for defining access to resources.

DISCLAIMER: This is a work in progress and has not yet been locked down. Expect the APIs to change until otherwise noted.

Installation
============
Make sure you have a working Go environment. The core library has an external dependency on sqlx. To run the unit tests, however, the [testify](https://github.com/stretchr/testify) library is required.

To install, run:
   ```
   go get github.com/dakiva/dbx
   ```

Tests
=====
To run the tests, you'll need to connect to a Postgres database:

POSTGRES_DSN="name= dbname= host= port= sslmode=" go test

Getting Started
===============
This library has support for externalizing queries into JSON files.

```
const (
      QueryA QueryIdentifier = "QueryA"
      ...
)

queryMap := loadNamedQueries("path/to/querfile.json", "path/to/queryfile2.json")
db *DB = ...
db.Exec(queryMap[QueryA], ...)
```

Additionally, an abstraction over DB is also provided allowing for use cases such as preparing connections - for casses such as setting the search_path when using Postgres schemas. The abstraction supports finding named queries using the mechanism above.

About
=====
This library is written by Daniel Akiva and is licensed under the apache-2.0 license.  Pull requests are welcome.

* TODO
  - Provide out of the box repositories for RDBMS.
  - Support for groups. This is useful when managing ACLs. Currently principals must be added directly to ACLs.
