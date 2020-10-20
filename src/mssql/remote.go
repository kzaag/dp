package mssql

import (
	"container/list"
	"database/sql"
)

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

func RemoteGetAllUnique(r *Remote, tableName string) ([]Unique, error) {

	var rows *sql.Rows
	var err error
	rows, err = r.conn.Query(
		`select kc.name
		from sys.key_constraints kc
		inner join sys.tables tt on tt.object_id = kc.parent_object_id
		inner join sys.schemas ss on ss.schema_id = tt.schema_id
		where ss.name + '.' + tt.name = @tname and kc.type = 'UQ'`, sql.Named("tname", tableName))

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
		rows, err = r.conn.Query(
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

	rows, err = r.conn.Query(
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

	c, err := MapCheck(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllColumn(r *Remote, tableName string) ([]Column, error) {

	var rows *sql.Rows
	var err error

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
		rows, err = r.conn.Query(
			`select fkcc.name , 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.parent_column_id and fkcc.object_id = fk.parent_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))

		if err != nil {
			return nil, err
		}

		fk.Columns, err = MapCColumns(rows)
		if err != nil {
			return nil, err
		}

		rows, err = r.conn.Query(
			`select fkcc.name, 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.referenced_column_id and fkcc.object_id = fk.referenced_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
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
	rows, err = r.conn.Query(
		`SELECT c.name
		FROM sys.tables t
		INNER JOIN sys.schemas s on s.schema_id = t.schema_id
		INNER JOIN sys.key_constraints c ON t.object_id = c.parent_object_id
		WHERE s.name + '.' + t.name =@tname and c.type = 'PK'`, sql.Named("tname", tableName))

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

	rows, err = r.conn.Query(
		`select c.name, ic.is_descending_key
		from sys.indexes i
		inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
		inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
		where i.is_primary_key = 1 and i.name = @pkname`, sql.Named("pkname", ret.Name))

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
		rows, err = r.conn.Query(
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

	if remote.tp == Pgsql {
		if err = PgsqlGetTypedTableInfo(remote, &tbl); err != nil {
			return nil, err
		}
	}

	return &tbl, nil
}
