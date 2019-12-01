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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"bitbucket.org/liamstask/goose/lib/goose"

	"github.com/jmoiron/sqlx"
)

const (
	// PostgresType is the internal type used to initialize a sql or sqlx DB object for postgres databases.
	PostgresType = "postgres"
	// PostgresDsnEnv is the environment variable name holding the Postgres Dsn
	PostgresDsnEnv     = "POSTGRES_DSN"
	defaultUser        = "postgres"
	extensionsFileName = "_extensions"
)

// InitializeDB initializes a connection pool, establishing a connection to the database and migrates a schema, returning a DB object that has the proper search path  set to the schema.
// Accepts a dsn "user= password= dbname= host= port= sslmode=[disable|require|verify-ca|verify-full] connect-timeout=" The role must have privileges to create a new database schema and install extensions.
// Schema must be set to a valid schema
// migrationsDir is the path to the migration scripts. This function uses goose to migrate the
// schema
func InitializeDB(pgdsn, schema, schemaPassword, migrationsDir string) (*sqlx.DB, error) {
	if pgdsn == "" {
		return nil, errors.New("Postgres dsn must not be empty")
	}
	db, err := sqlx.Connect(PostgresType, pgdsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	err = EnsureSchema(schema, schemaPassword, db)
	if err != nil {
		return nil, err
	}
	schemaDsn := CreateDsnForRole(pgdsn, schema, schemaPassword)
	err = fixPrivileges(pgdsn, schema, schemaDsn)
	if err != nil {
		return nil, err
	}
	err = InstallExtensions(schema, migrationsDir, db)
	if err != nil {
		return nil, err
	}
	err = MigrateSchema(schemaDsn, schema, migrationsDir)
	if err != nil {
		return nil, err
	}
	return sqlx.Connect(PostgresType, schemaDsn)
}

// MustInitializeDB calls InitializeDB  returning a DB object that has the proper search path
// set to the initialized schema. This function will panic on an error.
func MustInitializeDB(pgdsn, schema, schemaPassword, migrationsDir string) *sqlx.DB {
	db, err := InitializeDB(pgdsn, schema, schemaPassword, migrationsDir)
	if err != nil {
		panic(fmt.Sprintf("Error initializing database: %v", err))
	}
	return db
}

// GetDsn returns a datasource name suitable for use during testing by first looking
// for a dsn in an environment variable POSTGRES_DSN. If the environment variable is not
// set, generates a DSN using suitable local values.
func GetDsn() string {
	if pgdsn, exists := os.LookupEnv(PostgresDsnEnv); exists {
		return pgdsn
	}
	return GenerateDefaultDsn()
}

// GenerateDefaultDsn generates a DSN using suitable local values: localhost, port 5432 and using the system username as the role and database name.
func GenerateDefaultDsn() string {
	user := getDefaultDBName()
	if user == "" {
		user = defaultUser
	}
	m := map[string]string{
		"host":    "localhost",
		"port":    "5432",
		"user":    user,
		"dbname":  user,
		"sslmode": "disable",
	}
	return BuildDsn(m)
}

func getDefaultDBName() string {
	if user, err := user.Current(); err == nil {
		return user.Username
	}
	return ""
}

// MigrateSchema migrates a Postgres schema to the latest versioned schema script. Returns an error if migration fails.
func MigrateSchema(pgdsn, schema, migrationsDir string) error {
	// only supports Postgres
	driver := goose.DBDriver{
		Name:    PostgresType,
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

// RemoveExtensions attempts to remove all extensions. This function will not halt on an error and will iterate through all extensions. Removal does not remove the extension if it is in use. The first error found is returned.
func RemoveExtensions(migrationsDir string, db *sqlx.DB) error {
	extensions, err := getExtensions(migrationsDir)
	if err != nil {
		return err
	}
	var retErr error
	for _, extension := range extensions {
		_, err := db.Exec(fmt.Sprintf("DROP EXTENSION IF EXISTS %v", extension))
		if err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

// InstallExtensions looks for an _extensions file in the migrations dir, loads and attempts to create the extensions with the objects of the extension stored within the newly created schema. This function is a noop if the file does not exist.
func InstallExtensions(schema, migrationsDir string, db *sqlx.DB) error {
	extensions, err := getExtensions(migrationsDir)
	if err != nil {
		return err
	}
	for _, extension := range extensions {
		_, err := db.Exec(fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %v WITH SCHEMA %v", extension, schema))
		if err != nil {
			return err
		}
	}
	return nil
}

// EnsureSchema creates a new Postgres schema along with a specific role as the owner if neither exist. Returns an error if schema creation fails. This call is idempotent. If the schema and/or role already exists, this function ignores creation and continues configuring the schema and role.
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

// DropSchema drops a Postgres schema along with the specific role owner. Exercise caution when using this method, as its impact is irreversible.
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

// GetCurrentSchemaVersion returns the current schema version, or an error if the version could not be determined. This function will create the migrations versions table if the migrations table does not exist.
func GetCurrentSchemaVersion(schema string, db *sqlx.DB) (int64, error) {
	// only supports Postgres
	driver := goose.DBDriver{
		Name: PostgresType,
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

// CreateDsnForRole takes an existing, valid dsn and replaces the user name with the specified role name. If the password is non-empty, sets the password.
func CreateDsnForRole(existingDsn, role, password string) string {
	dsnMap := ParseDsn(existingDsn)
	dsnMap["user"] = role
	if password != "" {
		dsnMap["password"] = password
	}
	return BuildDsn(dsnMap)
}

// On some managed databases, such as RDS, the admin user is not really the actual super user and thus does not have privileges to modify the schema once the ownership is altered. This is required in order to properly install extensions.
func fixPrivileges(pgdsn, schema, schemaDsn string) error {
	parsed := ParseDsn(pgdsn)
	if adminUser, ok := parsed["user"]; ok {
		db, err := sqlx.Connect(PostgresType, schemaDsn)
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON SCHEMA %v TO %v", schema, adminUser))
		if err != nil {
			return err
		}
	}
	return nil
}

// ParseDsn parses a dsn into a map.
func ParseDsn(dsn string) map[string]string {
	dsnMap := make(map[string]string)
	params := strings.Split(dsn, " ")
	for _, param := range params {
		pair := strings.Split(param, "=")
		dsnMap[pair[0]] = pair[1]
	}
	return dsnMap
}

// BuildDsn builds a dsn from a map.
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

// getExtensions returns all extensions found in the _extensions file in the migrations directory, or an error. If the _extensions file does not exist, no error is returned.
func getExtensions(migrationsDir string) ([]string, error) {
	contents, err := ioutil.ReadFile(filepath.Join(migrationsDir, extensionsFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	extensions := strings.Split(string(contents), "\n")
	sanitizedExtensions := make([]string, 0)
	for _, extension := range extensions {
		sanitized := strings.TrimSpace(extension)
		if sanitized != "" {
			sanitizedExtensions = append(sanitizedExtensions, extension)
		}
	}
	return sanitizedExtensions, nil
}
