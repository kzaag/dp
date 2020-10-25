package mssql

import (
	"database-project/rdbms"
	"strconv"
	"strings"
)

func StmtNew() *rdbms.StmtCtx {
	ctx := rdbms.StmtCtx{}
	ctx.AddCheck = rdbms.StmtAddCheck
	ctx.AddColumn = StmtAddColumn
	ctx.AddFK = rdbms.StmtAddFk
	ctx.AddIndex = StmtCreateIx
	ctx.AddPK = rdbms.StmtAddPK
	ctx.AddUnique = rdbms.StmtAddUnique
	ctx.AlterColumn = StmtAlterColumn
	ctx.ColumnType = StmtColumnType
	ctx.CreateTable = StmtCreateTable
	ctx.DropColumn = StmtDropColumn
	ctx.DropConstraint = rdbms.StmtDropConstraint
	ctx.DropIndex = StmtDropIx
	ctx.DropTable = rdbms.StmtDropTable
	return &ctx
}

func StmtColumnType(column *rdbms.Column) string {
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

func StmtCreateIx(tableName string, ix *rdbms.Index) string {
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

func StmtDropIx(tableName string, i *rdbms.Index) string {
	return "DROP INDEX " + i.Name + " ON " + tableName + ";\n"
}

func StmtAlterColumn(tableName string, old, new *rdbms.Column) string {
	s := new.Name + " " + new.FullType

	if !new.Nullable {
		s += " NOT NULL"
	}

	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n"
}

func StmtDefColumn(column *rdbms.Column) string {

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

func StmtAddColumn(tableName string, c *rdbms.Column) string {
	return "ALTER TABLE " + tableName + " ADD " + StmtDefColumn(c) + ";\n"
}

func StmtDropColumn(tablename string, c *rdbms.Column) string {
	return "ALTER TABLE " + tablename + " DROP COLUMN " + c.Name + ";\n"
}

func StmtCreateTable(t *rdbms.Table) string {
	ret := ""

	ret += "CREATE TABLE " + t.Name + " ( \n"

	for i := 0; i < len(t.Columns); i++ {
		column := &t.Columns[i]
		columnStr := StmtDefColumn(column)
		ret += "\t" + columnStr + ",\n"
	}

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	return ret
}
