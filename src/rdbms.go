package main

import (
	"database/sql"
)

type ColAltLvl uint

const (
	CL_NONE ColAltLvl = 0
	CL_TYPE ColAltLvl = 1
	CL_NULL ColAltLvl = 2
)

func (c ColAltLvl) IsType() bool {
	if (c & CL_TYPE) != 0 {
		return true
	}
	return false
}

func (c ColAltLvl) IsNull() bool {
	if (c & CL_NULL) != 0 {
		return true
	}
	return false
}

type Rdbms struct {
	GetAllTableName func(*sql.DB) ([]string, error)
	GetTableDef     func(*sql.DB, string) (*Table, error)
	TableDef        func(Table) string

	AddIx     func(string, *Index) string
	AddUnique func(string, *Unique) string
	AddPk     func(string, *PrimaryKey) string
	AddFk     func(string, *ForeignKey) string
	AddCheck  func(string, *Check) string

	DropConstraint func(string, *Constraint) string
	DropIx         func(string, *Index) string

	AddColumn   func(string, *Column) string
	AlterColumn func(string, *Column, ColAltLvl) string
	DropColumn  func(string, *Column) string
	ColumnType  func(*Column) string
}

func RdbmsMssql() Rdbms {
	res := Rdbms{}
	res.GetAllTableName = MssqlGetAllTableName
	res.GetTableDef = MssqlGetTableDef
	res.TableDef = MssqlTableDef
	res.AddIx = SqlAddIx
	res.AddUnique = SqlAddUnique
	res.AddPk = SqlAddPk
	res.AddFk = SqlAddFk
	res.AddCheck = SqlAddCheck
	res.DropConstraint = SqlDropConstraint
	res.DropIx = SqlDropIx
	res.AddColumn = MssqlAddColumn
	res.AlterColumn = MssqlAlterColumn
	res.DropColumn = SqlDropColumn
	res.ColumnType = MssqlColumnType
	return res
}

func RdbmsPgsql() Rdbms {
	res := Rdbms{}
	res.GetAllTableName = PgsqlGetAllTableName
	res.GetTableDef = PgsqlGetTableDef
	res.TableDef = PgsqlTableDef
	res.AddIx = SqlAddIx
	res.AddUnique = SqlAddUnique
	res.AddPk = SqlAddPk
	res.AddFk = SqlAddFk
	res.AddCheck = SqlAddCheck
	res.DropConstraint = SqlDropConstraint
	res.DropIx = SqlDropIx
	res.AddColumn = PgsqlAddColumn
	res.AlterColumn = PgsqlAlterColumn
	res.DropColumn = SqlDropColumn
	res.ColumnType = PgsqlColumnType
	return res
}

func RdbmsGetAllTables(dbms Rdbms, db *sql.DB) ([]Table, error) {

	names, err := dbms.GetAllTableName(db)
	if err != nil {
		return nil, err
	}

	tbls := make([]Table, len(names))

	for i := 0; i < len(names); i++ {
		tbl, err := dbms.GetTableDef(db, names[i])
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			continue
		}
		tbls[i] = *tbl
	}

	return tbls, nil
}

func RdbmsGetMatchTables(dbms Rdbms, db *sql.DB, userTables []Table) ([]Table, error) {

	tbls := make([]Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := dbms.GetTableDef(db, userTables[i].Name)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			continue
		}
		tbls[i] = *tbl
	}

	return tbls, nil
}
