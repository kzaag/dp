package main

import (
	"container/list"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

func PgsqlGetAllTableName(db *sql.DB) ([]string, error) {

	r, err := db.Query(
		`select table_name 
		from information_schema.tables 
		where table_type = 'BASE TABLE' and table_schema = 'public'`)
	if err != nil {
		return nil, err
	}

	ret, err := RdbmsMapTNames(r)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func PgsqlGetAllUnique(db *sql.DB, tableName string) ([]Unique, error) {

	r, err := db.Query(
		`select tc.constraint_name
		 from information_schema.table_constraints tc
		 where tc.table_name = $1 and tc.constraint_type='UNIQUE'`, tableName)
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
			`select column_name, false as Is_descending
			from information_schema.constraint_column_usage 
			where constraint_name = $1`, c.Name)
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

func PgsqlGetAllCheck(db *sql.DB, tableName string) ([]Check, error) {

	r, err := db.Query(
		`select tc.constraint_name, cc.check_clause
		from information_schema.table_constraints tc
		inner join information_schema.check_constraints cc 
			on cc.constraint_name = tc.constraint_name
		where tc.table_name = $1 and tc.constraint_type='CHECK' and exists (
			select 1 from information_schema.constraint_column_usage where constraint_name = cc.constraint_name
		)`, tableName)
	if err != nil {
		return nil, err
	}

	c, err := RdbmsMapCheck(r)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func PgsqlGetAllColumn(db *sql.DB, tableName string) ([]Column, error) {
	r, err := db.Query(
		`select 
			column_name, 
			data_type, 
			coalesce(character_maximum_length, -1), 
			coalesce(numeric_precision, datetime_precision,  -1), 
			coalesce(numeric_scale, -1), 
			coalesce(case when is_nullable = 'YES' then true else false end, false), 
			coalesce(case when is_identity = 'YES' then true else false end, false) 
		from information_schema.columns 
		where table_name = $1
		order by ordinal_position asc;`, tableName)
	if err != nil {
		return nil, err
	}

	c, err := RdbmsMapColumns(r)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func PgsqlGetAllFK(db *sql.DB, tableName string) ([]ForeignKey, error) {
	r, err := db.Query(
		`select tc.constraint_name, tf.table_name as ref_table 
		from information_schema.table_constraints AS tc 
		join lateral ( select * from information_schema.constraint_column_usage ccu where ccu.constraint_name = tc.constraint_name limit 1) tf on true
		WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=$1;`, tableName)
	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for r.Next() {
		var k ForeignKey
		err := r.Scan(&k.Name, &k.Ref_table)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]ForeignKey, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		fk := x.Value.(ForeignKey)
		r, err := db.Query(
			`select column_name, false as is_descending
			from information_schema.key_column_usage
			where constraint_name = $1`, fk.Name)
		if err != nil {
			return nil, err
		}
		fk.Columns, err = RdbmsMapCColumns(r)
		if err != nil {
			return nil, err
		}
		r, err = db.Query(
			`select column_name, false as is_descending
			from information_schema.constraint_column_usage
			where constraint_name = $1`, fk.Name)
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

func PgsqlGetAllPK(db *sql.DB, tableName string) (*PrimaryKey, error) {
	r, err := db.Query(
		`select constraint_name 
		from information_schema.table_constraints 
		where constraint_type ='PRIMARY KEY' and table_name = $1`, tableName)
	if err != nil {
		return nil, err
	}

	if !r.Next() {
		return nil, nil
	}

	var ret PrimaryKey
	r.Scan(&ret.Name)

	r, err = db.Query(
		`select column_name, false as is_descending
		from information_schema.key_column_usage 
		where constraint_name = $1`, ret.Name)

	cols, err := RdbmsMapCColumns(r)
	if err != nil {
		return nil, err
	}

	ret.Columns = cols

	return &ret, nil
}

func PgsqlGetAllIx(db *sql.DB, tableName string) ([]Index, error) {

	r, err := db.Query(
		`select
			i.relname as index_name,
			ix.indisunique as is_unique,
			ix.indisclustered as is_clustered,
			*
		from
			pg_class t,
			pg_class i,
			pg_index ix
		where
			t.oid = ix.indrelid
			and i.oid = ix.indexrelid
			and t.relkind = 'r'
			and t.relname = $1
			and indisprimary = false
			and indisunique = false`, tableName)
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
				column_name,
				case when indoption[unn-1] = 3 then true else false end as is_descending,
				false as is_included_column 
			from (
				select
					t.relname as table_name,
					i.relname as index_name,
					a.attname as column_name,
					unnest(ix.indkey) as unn,
					ix.indoption,
					a.attnum
				from
					pg_class t,
					pg_class i,
					pg_index ix,
					pg_attribute a
				where
					t.oid = ix.indrelid
					and i.oid = ix.indexrelid
					and a.attrelid = t.oid
					and a.attnum = ANY(ix.indkey)
					and t.relkind = 'r'
					and i.relname = $1
				order by
					t.relname,
					i.relname,
					generate_subscripts(ix.indkey,1)
			) sb
			where unn = attnum`, c.Name)
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

func PgsqlColumnType(column *Column) string {
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
			column.Type = "character"
		}
		fallthrough
	case "character":
		fallthrough
	case "varchar":
		if column.Type == "varchar" {
			column.Type = "character varying"
		}
		fallthrough
	case "character varying":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length) + ")"
		}
	case "int8":
		fallthrough
	case "bigint":
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
		fallthrough
	case "int4":
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
		panic("no pgsql mapper for type : " + strings.ToLower(column.Type))
	}
	return cs
}

func PgsqlColumn(column *Column) string {
	var cs string

	cs += column.Name + " "

	cs += PgsqlColumnType(column)

	if !column.Is_nullable {
		cs += " NOT NULL"
	} else {
		cs += " NULL"
	}

	if column.Is_Identity {
		cs += " GENERATED ALWAYS AS IDENTITY"
	}

	return cs
}

func PgsqlAddColumn(tableName string, c *Column) string {
	s := c.Name + " "

	s += PgsqlColumnType(c)

	if !c.Is_nullable {
		s += " NOT NULL"
	} else {
		s += " NULL"
	}

	if c.Is_Identity {
		s += " GENERATED ALWAYS AS IDENTITY"
	}

	return "ALTER TABLE " + tableName + " ADD " + s + ";\n"
}

func PgsqlAlterColumn(tableName string, c *Column, l ColAltLvl) string {

	ret := ""

	if l.IsType() {
		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name + " SET DATA TYPE " + PgsqlColumnType(c) + ";\n"
		ret += s
	}

	if l.IsNull() {
		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name
		if c.Is_nullable {
			s += " DROP NOT NULL"
		} else {
			s += " SET NOT NULL"
		}
		ret += s + ";\n"
	}

	return ret
}

func PgsqlTableDef(t Table) string {
	ret := ""

	ret += "CREATE TABLE " + t.Name + " ( \n"

	if t.Columns != nil {
		for i := 0; i < len(t.Columns); i++ {
			column := t.Columns[i]
			columnStr := column.Name + " "
			columnStr += PgsqlColumnType(&column)
			if column.Is_nullable {
				columnStr += " NULL"
			} else {
				columnStr += " NOT NULL"
			}
			if column.Is_Identity {
				columnStr += " GENERATED ALWAYS AS IDENTITY"
			}
			ret += "\t" + columnStr + ",\n"
		}
	}

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	return ret
}

func PgsqlGetTableDef(db *sql.DB, name string) (*Table, error) {
	cols, err := PgsqlGetAllColumn(db, name)
	if err != nil {
		return nil, fmt.Errorf("col: %s", err.Error())
	}
	if cols == nil || len(cols) == 0 {
		return nil, nil
	}

	pk, err := PgsqlGetAllPK(db, name)
	if err != nil {
		return nil, fmt.Errorf("pk: %s", err.Error())
	}

	fks, err := PgsqlGetAllFK(db, name)
	if err != nil {
		return nil, fmt.Errorf("fk: %s", err.Error())
	}

	uq, err := PgsqlGetAllUnique(db, name)
	if err != nil {
		return nil, fmt.Errorf("uq: %s", err.Error())
	}

	c, err := PgsqlGetAllCheck(db, name)
	if err != nil {
		return nil, fmt.Errorf("ch: %s", err.Error())
	}

	ix, err := PgsqlGetAllIx(db, name)
	if err != nil {
		return nil, fmt.Errorf("ix: %s", err.Error())
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
