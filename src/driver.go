package main

import (
	"database-project/config"
	"database-project/pgsql"
	"database/sql"
)

/*
	communication with dbms modules
*/

//
func DriverRunExecCtx(e *config.ExecCtx) error {
	var db *sql.DB
	var err error

	if db, err = pgsql.CSCreateDBfromConfig(e.Name, e.Auth); err != nil {
		return err
	}

	defer db.Close()

	for _, ectx := range e.Exec {
		ect
	}

	return nil
}
