package pgsql

import (
	"log"
	"strconv"
	"strings"
)

type StmtCtx struct {
	// types dont really depend on the tables in pg
	DropType func(*Type) string
	AddType  func(*Type) string
	//
	AddColumn      func(string, *Column) string
	AlterColumn    func(string, *Column, *Column) string
	DropColumn     func(string, *Column) string
	ColumnType     func(*Column) string
	AddIndex       func(string, *Index) string
	DropIndex      func(string, *Index) string
	DropConstraint func(string, *Constraint) string
	AddUnique      func(string, *Unique) string
	AddCheck       func(string, *Check) string
	AddFK          func(string, *ForeignKey) string
	AddPK          func(string, *PrimaryKey) string
	CreateTable    func(*Table) string
	DropTable      func(*Table) string
}

func StmtNew() *StmtCtx {
	ctx := StmtCtx{}
	ctx.AddCheck = StmtAddCheck
	ctx.AddColumn = StmtAddColumn
	ctx.AddFK = StmtAddFk
	ctx.AddIndex = StmtAddIndex
	ctx.AddPK = StmtAddPK
	ctx.AddType = StmtAddType
	ctx.AddUnique = StmtAddUnique
	ctx.AlterColumn = StmtAlterColumn
	ctx.ColumnType = StmtColumnType
	ctx.CreateTable = StmtCreateTable
	ctx.DropColumn = StmtDropColumn
	ctx.DropConstraint = StmtDropConstraint
	ctx.DropIndex = StmtDropIndex
	ctx.DropTable = StmtDropTable
	ctx.DropType = StmtDropType
	return &ctx
}

func StmtConstraintColumn(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func StmtAddConstraint(tableName string, c *Constraint, cType string) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += StmtConstraintColumn(c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret
}

func StmtAddUnique(tableName string, u *Unique) string {
	return StmtAddConstraint(tableName, &u.Constraint, "unique")
}

func StmtAddPK(tableName string, pk *PrimaryKey) string {
	return StmtAddConstraint(tableName, &pk.Constraint, "primary key")
}

func StmtAddFk(tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += StmtConstraintColumn(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += StmtConstraintColumn(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",") + ")"

	if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
		ret += " ON DELETE " + strings.ToUpper(fk.OnDelete)
	}

	if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
		ret += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
	}

	ret += ";\n"
	return ret
}

func StmtAddCheck(tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func StmtDropConstraint(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}

func StmtDropTable(table *Table) string {
	return "DROP TABLE " + table.Name + ";\n"
}

func __StmtDefColumn(column *Column) string {

	var cs string
	cs += column.Name + " " + column.FullType

	if column.HasTag(TypeComposite) {
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

	if column.Default != "" {
		d := column.Default
		if !strings.Contains(d, "::") {
			d += "::" + strings.ToLower(column.FullType)
		}
		cs += " DEFAULT " + d
	}

	return cs
}

// func OP_AlterColumn(parentName string, old *rdbms.Column, new *rdbms.Column) string {
// 	if (new.Meta & CM_CompType) == CM_CompType {
// 		return "ALTER TYPE " + typename + " DROP ATTRIBUTE " + c.Name + " CASCADE;\n"
// 	}
// 	return "ALTER TABLE " + tableName + " DROP COLUMN " + c.Name + ";\n"
// }

// func AlterTypeColumnStr(r *Remote, typename string, sc *rdbms.Column, c *rdbms.Column) string {

// 	ret := ""

// 	if sc.FullType != c.FullType {
// 		ret += fmt.Sprintf("ALTER TYPE %s ALTER ATTRIBUTE %s SET DATA TYPE %s CASCADE", typename, c.Name, c.FullType)
// 	}

// 	ret += ";\n"

// 	return ret
// }

//

func StmtCreateTable(t *Table) string {

	ret := ""

	if t.Type != "" {
		return "CREATE TABLE " + t.Name + " OF " + t.Type + ";\n"
	}

	ret += "CREATE TABLE " + t.Name + " ( \n"

	for i := 0; i < len(t.Columns); i++ {
		column := &t.Columns[i]
		columnStr := __StmtDefColumn(column)
		ret += "\t" + columnStr + ",\n"
	}

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	return ret
}

func StmtAddIndex(tableName string, ix *Index) string {
	// unique := ""
	// if ix.Is_unique {
	// 	unique = "UNIQUE "
	// }

	using := ""

	if ix.Using != "" {
		using = " USING " + ix.Using
	}

	ret :=
		"CREATE INDEX " +
			ix.Name + " ON " + tableName + using + " ("

	isFirst := true

	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if c.Is_Included_column {
			continue
		}
		if isFirst {
			isFirst = false
		} else {
			ret += ","
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
			includes = true
		} else {
			ret += ","
		}

		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	if includes {
		ret += ")"
	}

	ret += ";\n"

	if ix.Is_clustered {
		ret += "CLUSTER " + tableName + " USING " + ix.Name + ";\n"
	}

	return ret
}

func StmtDropIndex(tableName string, i *Index) string {
	return "DROP INDEX " + i.Name + ";\n"
}

func StmtColumnType(column *Column) string {

	isArr := false
	if strings.HasPrefix(column.Type, "_") {
		isArr = true
		column.Type = strings.TrimPrefix(column.Type, "_")
	}
	if strings.HasSuffix(column.Type, "[]") {
		isArr = true
		column.Type = strings.TrimSuffix(column.Type, "[]")
	}
	cs := ""
	switch strings.ToLower(column.Type) {
	case "smallserial":
		fallthrough
	case "serial 2":
		fallthrough
	case "serial 4":
		fallthrough
	case "serial":
		fallthrough
	case "bigserial":
		log.Fatal(`SERIAL data types are not supported in this version.
		Use identity to flag column as auto generated, like so:
		columns:
		 - name: id
		   type: bigint
		   identity: true`)
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
	case "float":
		if column.Type == "float" {
			column.Type = "double precision"
		}
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
		column.Type = "time without time zone"
		cs += "TIME(" + strconv.Itoa(int(column.Precision)) + ") WITHOUT TIME ZONE"
	case "timetz":
		column.Type = "time with time zone"
		cs += "TIME(" + strconv.Itoa(int(column.Precision)) + ") WITH TIME ZONE"
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
	if isArr {
		cs += "[]"
	}
	return strings.ToUpper(cs)
}

func StmtAddType(t *Type) string {

	ret := ""

	ret += "CREATE TYPE " + t.Name + " AS"

	switch t.Type {
	case TypeEnum:

		ret += " ENUM ("

		for i := 0; i < len(t.Values); i++ {
			if i > 0 {
				ret += ","
			}
			ret += "'" + t.Values[i] + "'"
		}

		ret += ");\n"

	case TypeComposite:

		ret += " ("

		for i := 0; i < len(t.Columns); i++ {
			if i > 0 {
				ret += ","
			}
			ret += "\n\t"
			ret += StmtColumnNameAndType(&t.Columns[i])
		}

		ret += "\n);\n"
	}

	return ret
}

func StmtColumnNameAndType(column *Column) string {
	var cs string
	cs += column.Name + " " + column.FullType
	return cs
}

func StmtDropType(t *Type) string {
	return "DROP TYPE " + t.Name + ";\n"
}

func StmtDropColumn(tablename string, c *Column) string {
	if c.HasTag(TypeComposite) {
		return "ALTER TYPE " + tablename + " DROP ATTRIBUTE " + c.Name + " CASCADE;\n"
	}
	return "ALTER TABLE " + tablename + " DROP COLUMN " + c.Name + ";\n"
}

func StmtAlterColumn(tableName string, sc, c *Column) string {

	ret := ""

	if sc.FullType != c.FullType {
		isType := c.HasTag(TypeComposite)
		s := ""
		if isType {
			s = "ALTER TYPE " + tableName +
				" ALTER ATTRIBUTE " + c.Name +
				" SET DATA TYPE " + c.FullType +
				" CASCADE"
		} else {
			s = "ALTER TABLE " + tableName +
				" ALTER COLUMN " + c.Name +
				" SET DATA TYPE " + c.FullType
			if c.HintUsing != "" {
				s += " USING " + c.HintUsing
			}
		}

		// here theoretically could be introduced USING ( ... ) to the alter
		// but it seems too complex to properly introduce trimming for any pg type.
		// thus leaving it empty - user must handle this alone

		s += ";\n"
		ret += s

		// no point of checking nullable on types
		if isType {
			return ret
		}
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

	if sc.Default != "" && c.Default == "" {
		s := "ALTER TABLE " + tableName +
			" ALTER COLUMN " + c.Name +
			" DROP DEFAULT"
		ret += s + ";\n"
	} else if sc.Default != c.Default {
		s := "ALTER TABLE " + tableName +
			" ALTER COLUMN " + c.Name +
			" SET DEFAULT " + c.Default
		ret += s + ";\n"
	}

	return ret
}

func StmtAddColumn(tableName string, c *Column) string {

	s := __StmtDefColumn(c)

	if c.HasTag(TypeComposite) {
		return "ALTER TYPE " + tableName + " ADD ATTRIBUTE " + s + " CASCADE;\n"
	}

	r := "ALTER TABLE " + tableName + " ADD " + s

	return r + ";\n"
}
