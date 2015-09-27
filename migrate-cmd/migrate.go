package main

import (
	"flag"
	"log"
	"os"

	"github.com/dakiva/dbx"
	"github.com/jmoiron/sqlx"
)

const (
	postgresDsn = "POSTGRES_DSN"
)

func main() {
	pgdsn := os.Getenv(postgresDsn)
	migrationsDir := flag.String("migrations", "", "Path to migration scripts directory.")
	schema := flag.String("schema", "", "Schema name to migrate.")
	password := flag.String("password", "", "Schema role password to set when creating the schema role.")
	dsn := flag.String("dsn", "", "Postgres data source name parameters. Can also be specified using the POSTGRES_DSN environment variable.")
	dropSchema := flag.Bool("drop", false, "Drops the specified schema.")

	flag.Parse()

	if *dsn != "" {
		// use the dsn parameter value as an override if supplied
		pgdsn = *dsn
	}
	if pgdsn == "" {
		// if both the env and dsn parameter are not present, return an error.
		log.Fatalln("A valid postgres dsn is required.")
	}
	if *schema == "" {
		log.Fatalln("A valid schema name is required.")
	}

	if !*dropSchema {
		if *migrationsDir == "" {
			log.Fatalln("A valid migrations directory is required.")
		}
		db, err := dbx.InitializeDB(pgdsn, *schema, *password, *migrationsDir)
		if err != nil {
			log.Fatalln(err)
		}
		db.Close()
	} else {
		db, err := sqlx.Connect("postgres", *dsn)
		if err != nil {
			log.Fatalln(err)
		}
		defer db.Close()
		err = dbx.DropSchema(*schema, db)
		if err != nil {
			log.Fatalln(err)
		}
	}
}
