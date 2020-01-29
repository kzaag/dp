package main

import (
	"container/list"
	"database/sql"
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

func MssqlGetAllTableName(db *sql.DB) ([]string, error) {

	r, err := db.Query(
		"select s.name + '.' + t.name from sys.tables t inner join sys.schemas s on s.schema_id = t.schema_id")
	if err != nil {
		return nil, err
	}

	ret, err := RdbmsMapTNames(r)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func MssqlGetAllUnique(db *sql.DB, tableName string) ([]Unique, error) {

	r, err := db.Query(
		`select kc.name
		from sys.key_constraints kc
		inner join sys.tables tt on tt.object_id = kc.parent_object_id
		inner join sys.schemas ss on ss.schema_id = tt.schema_id
		where ss.name + '.' + tt.name = @tname and kc.type = 'UQ'`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k Unique
		err := r.Scan(&k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]Unique, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Unique)
		r, err := db.Query(
			`select
				c.name, ic.is_descending_key
			from sys.indexes i
				inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
				inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
			where i.is_unique_constraint = 1 and i.name = @cname
			`, sql.Named("cname", c.Name))
		if err != nil {
			return nil, err
		}
		c.Columns, err = RdbmsMapCColumns(r)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func MssqlGetAllCheck(db *sql.DB, tableName string) ([]Check, error) {

	r, err := db.Query(
		`select
			cc.name,
			cc.definition
		from sys.check_constraints cc
			inner join sys.tables t on t.object_id = cc.parent_object_id
			inner join sys.schemas s on s.schema_id = t.schema_id
		where s.name + '.' + t.name = @tname`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}

	c, err := RdbmsMapCheck(r)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func MssqlGetAllColumn(db *sql.DB, tableName string) ([]Column, error) {
	r, err := db.Query(
		`select
			c.name,
			t.name as type,
			c.max_length,
			c.precision,
			c.scale,
			c.is_nullable,
			c.is_identity
		from sys.columns c
		inner join sys.tables tt on tt.object_id = c.object_id
		inner join sys.schemas s on s.schema_id = tt.schema_id
		inner join sys.types t on t.system_type_id = c.system_type_id and t.name != 'sysname'
		where s.name + '.' + tt.name =@table_name
		order by c.column_id`, sql.Named("table_name", tableName))
	if err != nil {
		return nil, err
	}

	c, err := RdbmsMapColumns(r)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func MssqlGetAllFK(db *sql.DB, tableName string) ([]ForeignKey, error) {
	r, err := db.Query(
		`select
			s.name + '.' + t.name as ref_table,
			fk.name as name
		from sys.foreign_keys fk
			inner join sys.tables t on t.object_id = fk.referenced_object_id
			inner join sys.schemas s on s.schema_id = t.schema_id
			inner join sys.tables tt on tt.object_id = fk.parent_object_id
			inner join sys.schemas ss on ss.schema_id = tt.schema_id
		where ss.name + '.' + tt.name = @tname`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k ForeignKey
		err := r.Scan(&k.Ref_table, &k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]ForeignKey, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		fk := x.Value.(ForeignKey)
		r, err := db.Query(
			`select fkcc.name , 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.parent_column_id and fkcc.object_id = fk.parent_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
		if err != nil {
			return nil, err
		}
		fk.Columns, err = RdbmsMapCColumns(r)
		if err != nil {
			return nil, err
		}
		r, err = db.Query(
			`select fkcc.name, 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.referenced_column_id and fkcc.object_id = fk.referenced_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
		if err != nil {
			return nil, err
		}
		fk.Ref_columns, err = RdbmsMapCColumns(r)
		if err != nil {
			return nil, err
		}
		ret[i] = fk
	}

	return ret, nil
}

func MssqlGetAllPK(db *sql.DB, tableName string) (*PrimaryKey, error) {
	r, err := db.Query(`
	SELECT
	c.name
	FROM sys.tables t
	INNER JOIN sys.schemas s on s.schema_id = t.schema_id
	INNER JOIN sys.key_constraints c ON t.object_id = c.parent_object_id
	WHERE s.name + '.' + t.name =@tname and c.type = 'PK'`, sql.Named("tname", tableName))
	if err != nil {
		return nil, err
	}

	if !r.Next() {
		return nil, nil
	}

	var ret PrimaryKey
	r.Scan(&ret.Name)

	r, err = db.Query(
		`select
			c.name, ic.is_descending_key
	from sys.indexes i
		inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
		inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
	where i.is_primary_key = 1 and i.name = @pkname`, sql.Named("pkname", ret.Name))

	cols, err := RdbmsMapCColumns(r)
	if err != nil {
		return nil, err
	}

	ret.Columns = cols

	return &ret, nil
}

func MssqlGetAllIx(db *sql.DB, tableName string) ([]Index, error) {

	schema, name, err := MssqlSchemaName(tableName)
	if err != nil {
		return nil, err
	}

	r, err := db.Query(
		`select 
		i.name,
		i.is_unique,
		i.type_desc
	 from
		sys.indexes i
		inner join sys.tables t on t.object_id = i.object_id
		inner join sys.schemas s on s.schema_id = t.schema_id
	where s.name = @s and 
		  t.name = @n and 
		  i.type <> 0 and
		  is_primary_key = 0 and 
		  is_unique_constraint = 0`, sql.Named("s", schema), sql.Named("n", name))
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k Index
		err := r.Scan(&k.Name, &k.Is_unique, &k.Type)
		if err != nil {
			panic(err)
		}
		tmp.PushBack(k)
	}

	ret := make([]Index, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Index)
		r, err := db.Query(
			`select
				c.name, 
				ic.is_descending_key, 
				ic.is_included_column
			from sys.indexes i
				inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
				inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
			where  i.name = @cname`, sql.Named("cname", c.Name))
		if err != nil {
			return nil, err
		}
		c.Columns, err = RdbmsMapIxColumns(r)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func MssqlColumnType(column *Column) string {
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

func MssqlColumn(column *Column) string {
	var cs string

	cs += column.Name + " "

	cs += MssqlColumnType(column)

	if !column.Is_nullable {
		cs += " NOT NULL"
	}

	if column.Is_Identity {
		cs += " IDENTITY"
	}

	return cs
}

func MssqlAddColumn(tableName string, c *Column) string {
	s := c.Name + " "

	s += MssqlColumnType(c)

	if !c.Is_nullable {
		s += " NOT NULL"
	}

	if c.Is_Identity {
		s += " IDENTITY"
	}

	return "ALTER TABLE " + tableName + " ADD " + s + ";\n"
}

func MssqlAlterColumn(tableName string, c *Column, l ColAltLvl) string {
	s := c.Name + " "

	s += MssqlColumnType(c)

	if !c.Is_nullable {
		s += " NOT NULL"
	}

	return "ALTER TABLE " + tableName + " ALTER COLUMN " + s + ";\n"
}

func MssqlTableDef(t Table) string {
	ret := ""

	ret += "CREATE TABLE " + t.Name + " ( \n"

	if t.Columns != nil {
		for i := 0; i < len(t.Columns); i++ {
			column := t.Columns[i]
			columnStr := MssqlColumnType(&column)
			ret += "\t" + columnStr + ",\n"
		}
	}

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	return ret
}

func MssqlGetTableDef(db *sql.DB, name string) (*Table, error) {
	cols, err := MssqlGetAllColumn(db, name)
	if err != nil {
		return nil, err
	}
	if cols == nil || len(cols) == 0 {
		return nil, nil
	}

	pk, err := MssqlGetAllPK(db, name)
	if err != nil {
		return nil, err
	}

	fks, err := MssqlGetAllFK(db, name)
	if err != nil {
		return nil, err
	}

	uq, err := MssqlGetAllUnique(db, name)
	if err != nil {
		return nil, err
	}

	c, err := MssqlGetAllCheck(db, name)
	if err != nil {
		return nil, err
	}

	ix, err := MssqlGetAllIx(db, name)
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

func MssqlDropIx(tableName string, c *Index) string {
	return "DROP INDEX " + c.Name + " ON " + tableName + ";\n"
}