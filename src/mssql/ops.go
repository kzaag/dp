package mssql

import (
	"strconv"
	"strings"

	"github.com/kzaag/database-project/src/sql"
)

func ColumnTypeStr(column *sql.Column) string {
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

func AddIxStr(tableName string, ix *sql.Index) string {
	unique := ""
	if ix.Is_unique {
		unique = "UNIQUE "
	}

	var t string = "NONCLUSTERED "

	if ix.Type != "" {
		t = strings.ToUpper(ix.Type) + " "
	}

	ret :=
		"CREATE " + unique +
			t + "INDEX " +
			ix.Name + " ON " + tableName + " ("

	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if c.Is_Included_column {
			continue
		}
		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	ret += ")"

	var includes bool = false
	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if !c.Is_Included_column {
			continue
		}

		if !includes {
			ret += " INCLUDE ("
		}
		includes = true

		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	if includes {
		ret += ")"
	}

	ret += ";\n"

	return ret
}

func DropIxStr(tableName string, i *sql.Index) string {
	return "DROP INDEX " + i.Name + " ON " + tableName + ";\n"
}

func AlterTableColumnStr(tableName string, c *sql.Column) string {
	s := c.Name + " " + c.FullType

	if !c.Nullable {
		s += " NOT NULL"
	}

	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n"
}

func ColumnStr(column *sql.Column) string {

	var cs string
	cs += column.Name + " " + column.FullType

	if !column.Nullable {
		cs += " NOT NULL"
	} else {
		cs += " NULL"
	}

	if column.Identity {
		cs += " IDENTITY"
	}

	return cs
}
