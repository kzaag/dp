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

func MssqlColumnType(column *RawColumn) string {
	cs := ""
	switch strings.ToLower(column.Type) {
	case "nvarchar":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length/2) + ")"
		}
	case "varbinary":
		fallthrough
	case "varchar":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length) + ")"
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
		cs += strings.ToUpper(column.Type)
	case "binary":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Max_length)) + ")"
	case "datetime2":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Scale)) + ")"
	case "decimal":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + "," + strconv.Itoa(int(column.Scale)) + ")"
	default:
		panic("no mssql mapper for type : " + strings.ToLower(column.Type))
	}
	return cs
}

func MssqlAlterColumn(r *Remote, tableName string, c *Column) string {
	s := c.Name + " " + c.Type

	if !c.Is_nullable {
		s += " NOT NULL"
	}

	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n"
}
