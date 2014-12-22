package main

import (
	"flag"
	"log"

	"github.com/dakiva/dbx"
	"github.com/jmoiron/sqlx"
)

func main() {
	migrationsDir := flag.String("migrations", "", "Path to migration scripts directory.")
	schema := flag.String("schema", "", "Schema name to migrate.")
	password := flag.String("password", "", "Schema role password to set when creating the schema role.")
	dsn := flag.String("dsn", "", "Postgres data source name parameters.")
	dropSchema := flag.Bool("drop", false, "Drops the specified schema.")

	flag.Parse()
	if *dsn == "" {
		log.Fatalln("A valid postgres dsn is required.")
	}
	if *schema == "" {
		log.Fatalln("A valid schema name is required.")
	}

	if !*dropSchema {
		if *migrationsDir == "" {
			log.Fatalln("A valid migrations directory is required.")
		}
		if *password == "" {
			log.Fatalln("A non-empty password is required.")
		}
		db, err := dbx.InitializeDB(*dsn, *schema, *password, *migrationsDir)
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
