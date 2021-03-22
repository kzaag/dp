package pgsql

import "database/sql"

func DbClose(db interface{}) {
	db.(*sql.DB).Close()
}

func DbPing(db interface{}) error {
	return db.(*sql.DB).Ping()
}

func DbExec(db interface{}, stmt string, args ...interface{}) error {
	var err error
	if args == nil {
		_, err = db.(*sql.DB).Exec(stmt)
	} else {
		_, err = db.(*sql.DB).Exec(stmt, args...)
	}
	return err
}
