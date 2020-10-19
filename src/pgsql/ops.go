package pgsql

import (
	"strconv"
	"strings"

	"github.com/kzaag/database-project/src/sql"
)

func ColumnTypeStr(column *sql.Column) string {
	cs := ""
	switch strings.ToLower(column.Type) {
	case "bit":
		fallthrough
	case "varbit":
		fallthrough
	case "bit varying":
		fallthrough
	case "char":
		if column.Type == "char" {
			column.Type = "bpchar"
		}
		fallthrough
	case "character":
		if column.Type == "character" {
			column.Type = "bpchar"
		}
		fallthrough
	case "bpchar":
		fallthrough
	case "varchar":
		if column.Type == "varchar" {
			column.Type = "character varying"
		}
		fallthrough
	case "character varying":
		if column.Length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Length) + ")"
		}
	case "bigint":
		if column.Type == "bigint" {
			column.Type = "int8"
		}
		fallthrough
	case "int8":
		fallthrough
	case "serial8":
		fallthrough
	case "bigserial":
		fallthrough
	case "bool":
		if column.Type == "bool" {
			column.Type = "boolean"
		}
		fallthrough
	case "boolean":
		fallthrough
	case "box":
		fallthrough
	case "bytea":
		fallthrough
	case "cidr":
		fallthrough
	case "circle":
		fallthrough
	case "date":
		fallthrough
	case "double precision":
		fallthrough
	case "float8":
		if column.Type == "float8" {
			column.Type = "double precision"
		}
		fallthrough
	case "inet":
		fallthrough
	case "int4":
		if column.Type == "int4" {
			column.Type = "integer"
		}
		fallthrough
	case "int":
		if column.Type == "int" {
			column.Type = "integer"
		}
		fallthrough
	case "integer":
		fallthrough
	case "json":
		fallthrough
	case "jsonb":
		fallthrough
	case "line":
		fallthrough
	case "lseg":
		fallthrough
	case "macaddr":
		fallthrough
	case "money":
		fallthrough
	case "path":
		fallthrough
	case "pg_lsn":
		fallthrough
	case "point":
		fallthrough
	case "polygon":
		fallthrough
	case "float4":
		if column.Type == "float4" {
			column.Type = "real"
		}
		fallthrough
	case "real":
		fallthrough
	case "smallint":
		fallthrough
	case "int2":
		if column.Type == "int2" {
			column.Type = "smallint"
		}
		fallthrough
	case "smallserial":
		fallthrough
	case "serial 2":
		fallthrough
	case "serial":
		fallthrough
	case "serial 4":
		fallthrough
	case "text":
		fallthrough
	case "tsquery":
		fallthrough
	case "tsvector":
		fallthrough
	case "txid_snapshot":
		fallthrough
	case "uuid":
		fallthrough
	case "xml":
		cs += strings.ToUpper(column.Type)
	case "numeric":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + "," + strconv.Itoa(int(column.Scale)) + ")"
	case "time":
		fallthrough
	case "timetz":
		fallthrough
	case "interval":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + ")"
	case "timestamp":
		if column.Type == "timestamp" {
			column.Type = "timestamp without time zone"
		}
		fallthrough
	case "timestamp without time zone":
		cs += "TIMESTAMP(" + strconv.Itoa(int(column.Precision)) + ") WITHOUT TIME ZONE"
	case "timestamptz":
		if column.Type == "timestamptz" {
			column.Type = "timestamp with time zone"
		}
		fallthrough
	case "timestamp with time zone":
		cs += "TIMESTAMP(" + strconv.Itoa(int(column.Precision)) + ") WITH TIME ZONE"
	default:
		//panic("no pgsql mapper for type : " + strings.ToLower(column.Type))
		return column.Type
	}
	return strings.ToUpper(cs)
}

func AddIxStr(tableName string, ix *sql.Index) string {
	unique := ""
	if ix.Is_unique {
		unique = "UNIQUE "
	}

	var t string

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
	return "DROP INDEX " + i.Name + ";\n"
}

func AlterTableColumnStr(tableName string, sc, c *sql.Column) string {

	ret := ""

	if sc.FullType != c.FullType {

		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name + " SET DATA TYPE " + c.FullType

		// here theoretically could be introduced USING ( ... ) to the alter
		// but it seems too complex to properly introduce trimming for any pg type.
		// thus leaving it empty - user must handle this alone

		s += ";\n"
		ret += s
	}

	if sc.Nullable != c.Nullable {
		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name
		if c.Nullable {
			s += " DROP NOT NULL"
		} else {
			s += " SET NOT NULL"
		}
		ret += s + ";\n"
	}

	return ret
}

func ColumnStr(column *sql.Column) string {

	var cs string
	cs += column.Name + " " + column.FullType

	if (column.Meta & CM_CompType) == CM_CompType {
		return cs
	}

	if !column.Nullable {
		cs += " NOT NULL"
	} else {
		cs += " NULL"
	}

	if column.Identity {
		cs += " GENERATED ALWAYS AS IDENTITY"
	}

	return cs
}

func AddColumn(tableName string, c *sql.Column) string {

	s := ColumnStr(r, c)

	if (c.Meta & CM_CompType) == CM_CompType {
		return "ALTER TYPE " + tableName + " ADD ATTRIBUTE " + s + " CASCADE;\n"
	}

	return "ALTER TABLE " + tableName + " ADD " + s + ";\n"
}
