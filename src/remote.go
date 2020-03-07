package main

import (
	"container/list"
	"database/sql"
	"strings"
)

type RemoteType uint

const (

	/*

		mssql server since version 2016
		table types are not yet supported

	*/
	Mssql RemoteType = iota

	/*
		supports postgresql version 12.1
		other versions are not tested.
	*/
	Pgsql RemoteType = iota
)

type Remote struct {
	conn *sql.DB
	tp   RemoteType
}

func RemoteMssql(db *sql.DB) *Remote {
	r := Remote{db, Mssql}
	return &r
}

func RemotePgsql(db *sql.DB) *Remote {
	r := Remote{db, Pgsql}
	return &r
}

func RemoteColumnType(r *Remote, c *Column) string {
	switch r.tp {
	case Mssql:
		return MssqlColumnType(c)
	case Pgsql:
		return PgsqlColumnType(c)
	default:
		panic("remote type not implemented")
	}
}

// func RemoteGetAllTableName(r *Remote) ([]string, error) {

// 	var query string
// 	switch r.tp {
// 	case Mssql:
// 		query = `select s.name + '.' + t.name
// 				from sys.tables t
// 				inner join sys.schemas s on s.schema_id = t.schema_id`
// 	case Pgsql:
// 		query = `select table_name
// 				from information_schema.tables
// 				where table_type = 'BASE TABLE' and table_schema = 'public'`
// 	default:
// 		panic("remote type not implemented")
// 	}

// 	rows, err := r.conn.Query(query)
// 	if err != nil {
// 		return nil, err
// 	}

// 	ret, err := MapTableNames(rows)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return ret, nil
// }

func RemoteAddIx(r *Remote, tableName string, ix *Index) string {
	unique := ""
	if ix.Is_unique {
		unique = "UNIQUE "
	}

	var t string

	if r.tp == Mssql {
		t = "NONCLUSTERED "
	}

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

func RemoteAddUnique(r *Remote, tableName string, u *Unique) string {
	return SqlAddConstraint(tableName, &u.Constraint, "unique")
}

func RemoteAddPk(r *Remote, tableName string, pk *PrimaryKey) string {
	return SqlAddConstraint(tableName, &pk.Constraint, "primary key")
}

func RemoteAddFk(r *Remote, tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += SqlCColumn(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += SqlCColumn(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += " );\n"
	return ret
}

func RemoteAddCheck(r *Remote, tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func RemoteDropConstraint(r *Remote, tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}

func RemoteDropIx(r *Remote, tableName string, i *Index) string {
	switch r.tp {
	case Mssql:
		return "DROP INDEX " + i.Name + " ON " + tableName + ";\n"
	case Pgsql:
		return "DROP INDEX " + i.Name + ";\n"
	default:
		panic("remote type not implemented")
	}
}

func RemoteAddColumn(r *Remote, tableName string, c *Column) string {

	s := RemoteColumn(r, c)

	if (c.Meta & CM_CompType) == CM_CompType {
		return "ALTER TYPE add attribute " + s + ";\n"
	}

	return "ALTER TABLE " + tableName + " ADD " + s + ";\n"
}

func RemoteAlterColumn(r *Remote, parentName string, old *Column, new *Column) string {
	switch r.tp {
	case Mssql:
		return MssqlAlterTableColumn(r, parentName, new)
	case Pgsql:
		if (new.Meta & CM_CompType) == CM_CompType {
			return PgsqlAlterTypeColumn(r, parentName, old, new)
		}
		return PgsqlAlterTableColumn(r, parentName, old, new)
	default:
		panic("remote type not implemented")
	}
}

func RemoteDropTableColumn(r *Remote, tableName string, c *Column) string {
	return "ALTER TABLE " + tableName + " DROP COLUMN " + c.Name + ";\n"
}

func RemoteDropTypeColumn(r *Remote, typename string, c *Column) string {
	return "ALTER TYPE " + typename + " DROP ATTRIBUTE " + c.Name + ";\n"
}

func RemoteDropColumn(r *Remote, typename string, c *Column) string {
	switch c.Meta {
	case CM_CompType:
		return RemoteDropTypeColumn(r, typename, c)
	default:
		return RemoteDropTableColumn(r, typename, c)
	}
}

// func RemoteGetAllTables(remote *Remote) ([]Table, error) {

// 	names, err := RemoteGetAllTableName(remote)
// 	if err != nil {
// 		return nil, err
// 	}

// 	tbls := make([]Table, len(names))

// 	for i := 0; i < len(names); i++ {
// 		tbl, err := RemoteGetTableDef(remote, names[i])
// 		if err != nil {
// 			return nil, err
// 		}
// 		if tbl == nil {
// 			continue
// 		}
// 		tbls[i] = *tbl
// 	}

// 	return tbls, nil
// }

func RemoteGetMatchTables(remote *Remote, userTables []Table) ([]Table, error) {

	tbls := make([]Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := RemoteGetTableDef(remote, userTables[i].Name)
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

func RemoteTypeColumn(remote *Remote, column *Column) string {
	var cs string
	cs += column.Name + " " + column.FullType
	return cs
}

func RemoteColumn(remote *Remote, column *Column) string {

	var cs string
	cs += column.Name + " " + column.FullType

	if remote.tp == Pgsql && (column.Meta&CM_CompType) == CM_CompType {
		return cs
	}

	if !column.Nullable {
		cs += " NOT NULL"
	} else {
		cs += " NULL"
	}

	if column.Identity {
		switch remote.tp {
		case Mssql:
			cs += " IDENTITY"
		case Pgsql:
			cs += " GENERATED ALWAYS AS IDENTITY"
		default:
			panic("remote type not implemented")
		}
	}

	return cs
}

func RemoteTypeDef(remote *Remote, t *Type) string {

	if remote.tp != Pgsql {
		panic("not implemented")
	}

	ret := ""

	ret += "CREATE TYPE " + t.Name + " AS"

	switch t.Type {
	case TT_Enum:

		ret += " ENUM ("

		for i := 0; i < len(t.Values); i++ {
			if i > 0 {
				ret += ","
			}
			ret += "'" + t.Values[i] + "'"
		}

		ret += ");\n"

	case TT_Composite:

		ret += " ("

		for i := 0; i < len(t.Columns); i++ {
			if i > 0 {
				ret += ","
			}
			ret += RemoteTypeColumn(remote, &t.Columns[i])
		}

		ret += ");\n"
	}

	return ret
}

func RemoteTableDef(remote *Remote, t *Table) string {
	ret := ""

	if t.Type != "" && remote.tp == Pgsql {
		return "CREATE TABLE " + t.Name + " OF " + t.Type + ";\n"
	}

	ret += "CREATE TABLE " + t.Name + " ( \n"

	for i := 0; i < len(t.Columns); i++ {
		column := &t.Columns[i]
		columnStr := RemoteColumn(remote, column)
		ret += "\t" + columnStr + ",\n"
	}

	ret = strings.TrimSuffix(ret, ",\n")
	ret += "\n);\n"

	return ret
}

func RemoteGetAllUnique(r *Remote, tableName string) ([]Unique, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Mssql:
		rows, err = r.conn.Query(
			`select kc.name
			from sys.key_constraints kc
			inner join sys.tables tt on tt.object_id = kc.parent_object_id
			inner join sys.schemas ss on ss.schema_id = tt.schema_id
			where ss.name + '.' + tt.name = @tname and kc.type = 'UQ'`, sql.Named("tname", tableName))
	case Pgsql:
		rows, err = r.conn.Query(
			`select tc.constraint_name
			from information_schema.table_constraints tc
			where tc.table_name = $1 and tc.constraint_type='UNIQUE'`, tableName)
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}

	tmp := list.New()
	for rows.Next() {
		var k Unique
		err := rows.Scan(&k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]Unique, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Unique)
		switch r.tp {
		case Mssql:
			rows, err = r.conn.Query(
				`select
					c.name, ic.is_descending_key
				from sys.indexes i
					inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
					inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
				where i.is_unique_constraint = 1 and i.name = @cname
				`, sql.Named("cname", c.Name))
		case Pgsql:
			rows, err = r.conn.Query(
				`select column_name, false as Is_descending
				from information_schema.constraint_column_usage 
				where constraint_name = $1`, c.Name)
		default:
			panic("remote type not implemented")
		}
		if err != nil {
			return nil, err
		}
		c.Columns, err = MapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetAllCheck(r *Remote, tableName string) ([]Check, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Mssql:
		rows, err = r.conn.Query(
			`select
				cc.name,
				cc.definition
			from sys.check_constraints cc
				inner join sys.tables t on t.object_id = cc.parent_object_id
				inner join sys.schemas s on s.schema_id = t.schema_id
			where s.name + '.' + t.name = @tname`, sql.Named("tname", tableName))
	case Pgsql:
		rows, err = r.conn.Query(
			`select tc.constraint_name, cc.check_clause
			from information_schema.table_constraints tc
			inner join information_schema.check_constraints cc 
				on cc.constraint_name = tc.constraint_name
			where tc.table_name = $1 and tc.constraint_type='CHECK' and exists (
				select 1 from information_schema.constraint_column_usage where constraint_name = cc.constraint_name
			)`, tableName)
	default:
		panic("remote type not implemented")
	}
	if err != nil {
		return nil, err
	}

	c, err := MapCheck(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllColumn(r *Remote, tableName string) ([]Column, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Mssql:
		rows, err = r.conn.Query(
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
	case Pgsql:
		rows, err = r.conn.Query(
			`select 
				column_name, 
				udt_name,--UPPER(data_type), 
				coalesce(character_maximum_length, -1), 
				coalesce(numeric_precision, datetime_precision,  -1), 
				coalesce(numeric_scale, -1), 
				coalesce(case when is_nullable = 'YES' then true else false end, false), 
				coalesce(case when is_identity = 'YES' then true else false end, false) 
			from information_schema.columns 
			where table_name = $1
			order by ordinal_position asc;`, tableName)
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}

	c, err := MapColumns(r, rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllFK(r *Remote, tableName string) ([]ForeignKey, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Pgsql:
		rows, err = r.conn.Query(
			`select tc.constraint_name, tf.table_name as ref_table 
			from information_schema.table_constraints AS tc 
			join lateral ( select * from information_schema.constraint_column_usage ccu where ccu.constraint_name = tc.constraint_name limit 1) tf on true
			WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=$1;`, tableName)
	case Mssql:
		rows, err = r.conn.Query(
			`select
				fk.name as name,
				s.name + '.' + t.name as ref_table
			from sys.foreign_keys fk
				inner join sys.tables t on t.object_id = fk.referenced_object_id
				inner join sys.schemas s on s.schema_id = t.schema_id
				inner join sys.tables tt on tt.object_id = fk.parent_object_id
				inner join sys.schemas ss on ss.schema_id = tt.schema_id
			where ss.name + '.' + tt.name = @tname`, sql.Named("tname", tableName))
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}
	tmp := list.New()

	for rows.Next() {
		var k ForeignKey
		err := rows.Scan(&k.Name, &k.Ref_table)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]ForeignKey, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		fk := x.Value.(ForeignKey)

		switch r.tp {
		case Mssql:
			rows, err = r.conn.Query(
				`select fkcc.name , 0 as d
				from sys.foreign_keys fk
				inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
				inner join sys.columns fkcc on fkcc.column_id = fkc.parent_column_id and fkcc.object_id = fk.parent_object_id
				where fk.name = @fkname
				`, sql.Named("fkname", fk.Name))
		case Pgsql:
			rows, err = r.conn.Query(
				`select column_name, false as is_descending
				from information_schema.key_column_usage
				where constraint_name = $1`, fk.Name)
		default:
			panic("remote type not implemented")
		}

		if err != nil {
			return nil, err
		}

		fk.Columns, err = MapCColumns(rows)
		if err != nil {
			return nil, err
		}

		switch r.tp {
		case Mssql:
			rows, err = r.conn.Query(
				`select fkcc.name, 0 as d
				from sys.foreign_keys fk
				inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
				inner join sys.columns fkcc on fkcc.column_id = fkc.referenced_column_id and fkcc.object_id = fk.referenced_object_id
				where fk.name = @fkname
				`, sql.Named("fkname", fk.Name))
		case Pgsql:
			rows, err = r.conn.Query(
				`select column_name, false as is_descending
				from information_schema.constraint_column_usage
				where constraint_name = $1`, fk.Name)
		default:
			panic("remote type not implemented")
		}
		if err != nil {
			return nil, err
		}
		fk.Ref_columns, err = MapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = fk
	}

	return ret, nil
}

func RemoteGetAllPK(r *Remote, tableName string) (*PrimaryKey, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Mssql:
		rows, err = r.conn.Query(
			`SELECT c.name
			FROM sys.tables t
			INNER JOIN sys.schemas s on s.schema_id = t.schema_id
			INNER JOIN sys.key_constraints c ON t.object_id = c.parent_object_id
			WHERE s.name + '.' + t.name =@tname and c.type = 'PK'`, sql.Named("tname", tableName))
	case Pgsql:
		rows, err = r.conn.Query(
			`select constraint_name 
			from information_schema.table_constraints 
			where constraint_type ='PRIMARY KEY' and table_name = $1`, tableName)
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		return nil, nil
	}

	var ret PrimaryKey
	if err = rows.Scan(&ret.Name); err != nil {
		return nil, err
	}

	switch r.tp {
	case Mssql:
		rows, err = r.conn.Query(
			`select c.name, ic.is_descending_key
			from sys.indexes i
			inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
			inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
			where i.is_primary_key = 1 and i.name = @pkname`, sql.Named("pkname", ret.Name))
	case Pgsql:
		rows, err = r.conn.Query(
			`select column_name, false as is_descending
			from information_schema.key_column_usage 
			where constraint_name = $1`, ret.Name)
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}

	cols, err := MapCColumns(rows)
	if err != nil {
		return nil, err
	}

	ret.Columns = cols

	return &ret, nil
}

func RemoteGetAllIx(r *Remote, tableName string) ([]Index, error) {

	var rows *sql.Rows
	var err error

	switch r.tp {
	case Mssql:
		schema, name, err := MssqlSchemaName(tableName)
		if err != nil {
			return nil, err
		}

		if rows, err = r.conn.Query(
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
			is_unique_constraint = 0`, sql.Named("s", schema), sql.Named("n", name)); err != nil {
			return nil, err
		}

	case Pgsql:
		rows, err = r.conn.Query(
			`select
				i.relname as index_name,
				ix.indisunique as is_unique,
				ix.indisclustered as is_clustered
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
	default:
		panic("remote type not implemented")
	}

	if err != nil {
		return nil, err
	}

	tmp := list.New()

	for rows.Next() {
		var k Index
		err := rows.Scan(&k.Name, &k.Is_unique, &k.Type)
		if err != nil {
			panic(err)
		}
		tmp.PushBack(k)
	}

	ret := make([]Index, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Index)

		switch r.tp {
		case Pgsql:
			rows, err = r.conn.Query(
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
		case Mssql:
			rows, err = r.conn.Query(
				`select
					c.name, 
					ic.is_descending_key, 
					ic.is_included_column
				from sys.indexes i
					inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
					inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
				where  i.name = @cname`, sql.Named("cname", c.Name))
		default:
			panic("remote type not implemented")
		}

		if err != nil {
			return nil, err
		}
		c.Columns, err = MapIxColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetTableDef(remote *Remote, tableName string) (*Table, error) {
	cols, err := RemoteGetAllColumn(remote, tableName)
	if err != nil {
		return nil, err
	}

	if len(cols) == 0 {
		return nil, nil
	}

	pk, err := RemoteGetAllPK(remote, tableName)
	if err != nil {
		return nil, err
	}

	fks, err := RemoteGetAllFK(remote, tableName)
	if err != nil {
		return nil, err
	}

	uq, err := RemoteGetAllUnique(remote, tableName)
	if err != nil {
		return nil, err
	}

	c, err := RemoteGetAllCheck(remote, tableName)
	if err != nil {
		return nil, err
	}

	ix, err := RemoteGetAllIx(remote, tableName)
	if err != nil {
		return nil, err
	}

	var tbl Table
	tbl.Check = c
	tbl.Unique = uq
	tbl.Name = tableName
	tbl.Columns = cols
	tbl.Foreign = fks
	tbl.Primary = pk
	tbl.Indexes = ix

	return &tbl, nil
}

func RemoteGetTypes(r *Remote, localTypes []Type) ([]Type, error) {

	if r.tp != Pgsql {
		// not implemented to mssql yet
		return nil, nil
	}

	return PgsqlGetTypes(r, localTypes)
}

func RemoteDropType(r *Remote, t *Type) string {

	if r.tp != Pgsql {
		return EMPTY
	}

	return "DROP TYPE " + t.Name + ";\n"

}
