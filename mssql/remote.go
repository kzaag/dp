package mssql

import (
	"container/list"
	"database/sql"

	"github.com/kzaag/dp/rdbms"
)

func RemoteGetAllUnique(db *sql.DB, tableName string) ([]rdbms.Unique, error) {

	var rows *sql.Rows
	var err error
	rows, err = db.Query(
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
		var k rdbms.Unique
		err := rows.Scan(&k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]rdbms.Unique, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(rdbms.Unique)
		rows, err = db.Query(
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
		c.Columns, err = rdbms.HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetAllCheck(db *sql.DB, tableName string) ([]rdbms.Check, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
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

	c, err := rdbms.HelperMapChecks(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllColumn(db *sql.DB, tableName string) ([]rdbms.Column, error) {

	var rows *sql.Rows
	var err error
	var el rdbms.Column
	ret := make([]rdbms.Column, 0, 10)

	rows, err = db.Query(
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

	for rows.Next() {
		err = rows.Scan(
			&el.Name,
			&el.Type,
			&el.Length,
			&el.Precision,
			&el.Scale,
			&el.Nullable,
			&el.Identity)
		if err != nil {
			return nil, err
		}

		el.FullType = StmtColumnType(&el)
		ret = append(ret, el)
	}

	return ret, nil
}

func RemoteGetAllFK(db *sql.DB, tableName string) ([]rdbms.ForeignKey, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
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
		var k rdbms.ForeignKey
		err := rows.Scan(&k.Name, &k.Ref_table)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	ret := make([]rdbms.ForeignKey, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		fk := x.Value.(rdbms.ForeignKey)
		rows, err = db.Query(
			`select fkcc.name , 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.parent_column_id and fkcc.object_id = fk.parent_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))

		if err != nil {
			return nil, err
		}

		fk.Columns, err = rdbms.HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}

		rows, err = db.Query(
			`select fkcc.name, 0 as d
			from sys.foreign_keys fk
			inner join sys.foreign_key_columns fkc on fkc.constraint_object_id = fk.object_id
			inner join sys.columns fkcc on fkcc.column_id = fkc.referenced_column_id and fkcc.object_id = fk.referenced_object_id
			where fk.name = @fkname
			`, sql.Named("fkname", fk.Name))
		if err != nil {
			return nil, err
		}
		fk.Ref_columns, err = rdbms.HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = fk
	}

	return ret, nil
}

func RemoteGetAllPK(db *sql.DB, tableName string) (*rdbms.PrimaryKey, error) {

	var rows *sql.Rows
	var err error
	rows, err = db.Query(
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

	var ret rdbms.PrimaryKey
	if err = rows.Scan(&ret.Name); err != nil {
		return nil, err
	}

	rows, err = db.Query(
		`select c.name, ic.is_descending_key
		from sys.indexes i
		inner join sys.index_columns ic on ic.index_id = i.index_id and i.object_id = ic.object_id
		inner join sys.columns c on c.column_id = ic.column_id and ic.object_id = c.object_id
		where i.is_primary_key = 1 and i.name = @pkname`, sql.Named("pkname", ret.Name))

	if err != nil {
		return nil, err
	}

	cols, err := rdbms.HelperMapCColumns(rows)
	if err != nil {
		return nil, err
	}

	ret.Columns = cols

	return &ret, nil
}

func RemoteGetAllIx(db *sql.DB, tableName string) ([]rdbms.Index, error) {

	var rows *sql.Rows
	var err error

	schema, name, err := HelperSplitSchemaName(tableName)
	if err != nil {
		return nil, err
	}

	if rows, err = db.Query(
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
		var k rdbms.Index
		err := rows.Scan(&k.Name, &k.Is_unique, &k.Type)
		if err != nil {
			panic(err)
		}
		tmp.PushBack(k)
	}

	ret := make([]rdbms.Index, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(rdbms.Index)
		rows, err = db.Query(
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
		c.Columns, err = rdbms.HelperMapIxColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetTableDef(db *sql.DB, tableName string) (*rdbms.Table, error) {

	cols, err := RemoteGetAllColumn(db, tableName)
	if err != nil {
		return nil, err
	}

	if len(cols) == 0 {
		return nil, nil
	}

	pk, err := RemoteGetAllPK(db, tableName)
	if err != nil {
		return nil, err
	}

	fks, err := RemoteGetAllFK(db, tableName)
	if err != nil {
		return nil, err
	}

	uq, err := RemoteGetAllUnique(db, tableName)
	if err != nil {
		return nil, err
	}

	c, err := RemoteGetAllCheck(db, tableName)
	if err != nil {
		return nil, err
	}

	ix, err := RemoteGetAllIx(db, tableName)
	if err != nil {
		return nil, err
	}

	var tbl rdbms.Table
	tbl.Check = c
	tbl.Unique = uq
	tbl.Name = tableName
	tbl.Columns = cols
	tbl.Foreign = fks
	tbl.Primary = pk
	tbl.Indexes = ix

	return &tbl, nil
}

func RemoteGetMatchedTables(db *sql.DB, userTables []rdbms.Table) ([]rdbms.Table, error) {

	tbls := make([]rdbms.Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := RemoteGetTableDef(db, userTables[i].Name)
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
