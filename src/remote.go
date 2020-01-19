package main

import (
	"database/sql"
	"fmt"
	"strings"
)

// mssql, mysql, postgresql
type Remote interface {

	// downloading schema

	GetAllTableName(db *sql.DB) ([]string, error)
	GetAllUnique(db *sql.DB, tableName string) ([]Unique, error)
	GetAllCheck(db *sql.DB, tableName string) ([]Check, error)
	GetAllColumn(db *sql.DB, tableName string) ([]Column, error)
	GetAllFK(db *sql.DB, tableName string) ([]ForeignKey, error)
	GetAllPK(db *sql.DB, tableName string) (*PrimaryKey, error)
	GetAllIx(db *sql.DB, tableName string) ([]Index, error)

	// table definition to string

	ColumnToString(*Column) (string, error)

	// add index on table

	AddIxToString(tableName string, ix *Index) (string, error)

	// alter table add constraints

	AddUniqueToString(tableName string, c *Unique) (string, error)
	AddPkToString(tableName string, c *PrimaryKey) (string, error)
	AddFkToString(tableName string, fk *ForeignKey) (string, error)
	AddCheckToString(tableName string, c *Check) (string, error)

	// drop generic constraint

	DropCsToString(tableName string, c *Constraint) (string, error)
	DropIxToString(tableName string, c *Index) (string, error)

	// add , alter , drop columns

	AddColumnToString(tableName string, c *Column) (string, error)
	AlterColumnToString(tableName string, c *Column) (string, error)
	DropColumnToString(tableName string, c *Column) (string, error)
}

func RemoteGetTableDefinition(remote Remote, db *sql.DB, name string) (*Table, error) {
	cols, err := remote.GetAllColumn(db, name)
	if err != nil {
		return nil, err
	}
	if cols == nil || len(cols) == 0 {
		return nil, nil
	}

	pk, err := remote.GetAllPK(db, name)
	if err != nil {
		return nil, err
	}

	fks, err := remote.GetAllFK(db, name)
	if err != nil {
		return nil, err
	}

	uq, err := remote.GetAllUnique(db, name)
	if err != nil {
		return nil, err
	}

	c, err := remote.GetAllCheck(db, name)
	if err != nil {
		return nil, err
	}

	ix, err := remote.GetAllIx(db, name)
	if err != nil {
		return nil, err
	}

	var tbl Table
	tbl.Check = c
	tbl.Unique = uq
	tbl.Name = name
	tbl.Columns = cols
	tbl.Foreign = fks
	tbl.Primary = pk
	tbl.Indexes = ix

	return &tbl, nil
}

func RemoteGetAllTables(remote Remote, db *sql.DB) ([]Table, error) {
	names, err := remote.GetAllTableName(db)
	if err != nil {
		return nil, err
	}

	tbls := make([]Table, len(names))

	for i := 0; i < len(names); i++ {
		tbl, err := RemoteGetTableDefinition(remote, db, names[i])
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

func RemoteGetMatchTables(remote Remote, db *sql.DB, userTables []Table) ([]Table, error) {

	tbls := make([]Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := RemoteGetTableDefinition(remote, db, userTables[i].Name)
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

func RemoteTableToString(remote Remote, tf *Table) (string, error) {
	ret := ""

	if tf.Name == "" {
		return "", fmt.Errorf("table name was empty")
	}

	ret += "CREATE TABLE " + tf.Name + " ( \n"

	if tf.Columns != nil {
		for i := 0; i < len(tf.Columns); i++ {
			column := tf.Columns[i]
			columnStr, err := remote.ColumnToString(&column)
			if err != nil {
				return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
			}
			ret += "\t" + columnStr + ",\n"
		}
	}

	// if tf.Foreign != nil {
	// 	for i := 0; i < len(tf.Foreign); i++ {
	// 		fk := tf.Foreign[i]
	// 		fkdef, err := remote.FkToString(&fk)
	// 		if err != nil {
	// 			return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
	// 		}
	// 		ret += "\t" + fkdef + ",\n"
	// 	}
	// }

	// if tf.Primary != nil {
	// 	pkdef, err := remote.PkToString(tf.Primary)
	// 	if err != nil {
	// 		return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
	// 	}
	// 	ret += "\t" + pkdef + ",\n"
	// }

	// if tf.Check != nil {
	// 	for i := 0; i < len(tf.Check); i++ {
	// 		ckdef, err := remote.CheckToString(&tf.Check[i])
	// 		if err != nil {
	// 			return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
	// 		}
	// 		ret += "\t" + ckdef + ",\n"
	// 	}
	// }

	// if tf.Unique != nil {
	// 	for i := 0; i < len(tf.Unique); i++ {
	// 		udef, err := remote.UniqueToString(&tf.Unique[i])
	// 		if err != nil {
	// 			return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
	// 		}
	// 		ret += "\t" + udef + ",\n"
	// 	}
	// }

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	// if tf.Indexes != nil {
	// 	for i := 0; i < len(tf.Indexes); i++ {
	// 		udef, err := remote.AddIxToString(tf.Name, &tf.Indexes[i])
	// 		if err != nil {
	// 			return "", fmt.Errorf("table " + tf.Name + " : " + err.Error())
	// 		}
	// 		ret += udef
	// 	}
	// }

	return ret, nil
}
