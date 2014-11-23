package main

import (
	"flag"
	"log"

	"github.com/dakiva/dbx"
)

func main() {
	migrationsDir := flag.String("migrations", "", "Path to migration scripts directory.")
	schema := flag.String("schema", "", "Schema name to migrate.")
	password := flag.String("password", "", "Schema role password to set when creating the schema role.")
	dsn := flag.String("dsn", "", "Postgres data source name parameters.")

	flag.Parse()

	log.Println(*dsn)
	if *migrationsDir == "" {
		log.Fatalln("A valid migrations directory is required.")
	}
	if *schema == "" {
		log.Fatalln("A valid schema name is required.")
	}
	if *password == "" {
		log.Fatalln("A non-empty password is required.")
	}
	if *dsn == "" {
		log.Fatalln("A valid postgres dsn is required.")
	}
	db, err := dbx.InitializeDB(*dsn, *schema, *password, *migrationsDir)
	if err != nil {
		log.Fatalln(err)
	}
	db.Close()
}
