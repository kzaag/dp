package main

import (
	"fmt"
	"strconv"
	"strings"
)

// split {schema}.{name} sql name into 2 parts
func MssqlSchemaName(name string) (string, string, error) {
	if name == "" || !strings.Contains(name, ".") {
		return "", "", fmt.Errorf("invalid sql name. expected: {schema}.{name} got: \"" + name + "\"")
	}
	sn := strings.Split(name, ".")
	if sn == nil || len(sn) != 2 {
		return "", "", fmt.Errorf("couldnt split sql name into schema, name array")
	}
	return sn[0], sn[1], nil
}

func MssqlColumnType(column *Column) string {
	cs := ""
	switch strings.ToLower(column.Type) {
	case "nvarchar":
		if column.Length == -1 {
			cs += column.Type + "(max)"
		} else {
			cs += column.Type + "(" + strconv.Itoa(column.Length/2) + ")"
		}
	case "varbinary":
		fallthrough
	case "varchar":
		if column.Length == -1 {
			cs += column.Type + "(max)"
		} else {
			cs += column.Type + "(" + strconv.Itoa(column.Length) + ")"
		}
	case "int":
		fallthrough
	case "smallint":
		fallthrough
	case "tinyint":
		fallthrough
	case "bigint":
		fallthrough
	case "bit":
		fallthrough
	case "uniqueidentifier":
		cs += column.Type
	case "binary":
		cs += column.Type + "(" + strconv.Itoa(int(column.Length)) + ")"
	case "datetime2":
		cs += column.Type + "(" + strconv.Itoa(int(column.Scale)) + ")"
	case "decimal":
		cs += column.Type + "(" + strconv.Itoa(int(column.Precision)) + "," + strconv.Itoa(int(column.Scale)) + ")"
	default:
		panic("no mssql mapper for type : " + strings.ToLower(column.Type))
	}
	return strings.ToUpper(cs)
}

func MssqlAlterTableColumn(r *Remote, tableName string, c *Column) string {
	s := c.Name + " " + c.FullType

	if !c.Nullable {
		s += " NOT NULL"
	}

	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n"
}
