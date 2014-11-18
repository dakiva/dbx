dbx
===

A library that provides database extensions for Go. These tools are intended to work with both sql and sqlx.

[![wercker status](https://app.wercker.com/status/b4812ae58dbd3745ade9bd97647e90c9/m "wercker status")](https://app.wercker.com/project/bykey/b4812ae58dbd3745ade9bd97647e90c9)

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

queryMap := LoadNamedQueries("path/to/querfile.json", "path/to/queryfile2.json")
db *DB = ...
db.Exec(queryMap.Q(QueryA), ...)
```

Additionally, the schema_support file contains useful Postgres specific functions for managing schemas.

About
=====
This library is written by Daniel Akiva and is licensed under the apache-2.0 license.  Pull requests are welcome.
