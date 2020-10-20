package pgsql

import (
	"container/list"
	"database/sql"
)

func REM_GetTypes(r *Remote, localTypes []Type) ([]Type, error) {

	enums, err := PgsqlGetEnum(r)
	if err != nil {
		return nil, err
	}

	composite, err := PgsqlGetComposite(r)
	if err != nil {
		return nil, err
	}

	elen := len(enums)
	clen := len(composite)

	cb := make([]Type, elen+clen)

	for i := 0; i < elen; i++ {
		v := &enums[i]
		if TExists(v, localTypes) {
			cb[i] = *v
		}
	}

	for i := 0; i < clen; i++ {
		v := &composite[i]
		if TExists(v, localTypes) {
			cb[elen+i] = *v
		}
	}

	return cb, nil
}

func REM_GetEnum() ([]Type, error) {

	q := `select typname from pg_type where typcategory = 'E'`
	rows, err := r.conn.Query(q)
	if err != nil {
		return nil, err
	}

	var tps []Type

	for rows.Next() {
		var enumname string
		err := rows.Scan(&enumname)
		if err != nil {
			return nil, err
		}
		tps = append(tps, Type{enumname, TT_Enum, nil, nil})
	}

	query := `select e.enumlabel
			 from pg_enum e
			 join pg_type t ON e.enumtypid = t.oid
			 where typname = $1
			 order by enumsortorder;`

	for i := 0; i < len(tps); i++ {

		tp := &tps[i]
		rows, err := r.conn.Query(query, tp.Name)
		if err != nil {
			return nil, err
		}

		cols := list.New()
		for rows.Next() {
			var colname string
			err := rows.Scan(&colname)
			if err != nil {
				return nil, err
			}
			cols.PushBack(colname)
		}

		tp.Values = make([]string, cols.Len())

		for i, x := 0, cols.Front(); x != nil; i, x = i+1, x.Next() {
			tp.Values[i] = x.Value.(string)
		}

	}

	return tps, nil
}

func REM_GetComposite() ([]Type, error) {

	q := `select typname from pg_type where typcategory = 'C' and typarray <> 0;`
	rows, err := r.conn.Query(q)
	if err != nil {
		return nil, err
	}

	var tps []Type

	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tps = append(tps, Type{name, TT_Composite, nil, nil})
	}

	query := `SELECT attname, UPPER(format_type(atttypid, atttypmod)) AS type
			FROM   pg_attribute
			WHERE  attrelid = $1::regclass
			AND    attnum > 0
			AND    NOT attisdropped
			ORDER  BY attnum;`

	for i := 0; i < len(tps); i++ {
		tp := &tps[i]
		rows, err := r.conn.Query(query, tp.Name)
		if err != nil {
			return nil, err
		}

		cols := list.New()
		for rows.Next() {
			var colname string
			var coltype string
			err := rows.Scan(&colname, &coltype)
			c := Column{colname, "", coltype, -1, -1, -1, false, false, CM_CompType}
			if err != nil {
				return nil, err
			}
			cols.PushBack(c)
		}

		tp.Columns = make([]Column, cols.Len())

		for i, x := 0, cols.Front(); x != nil; i, x = i+1, x.Next() {
			tp.Columns[i] = x.Value.(Column)
		}
	}

	return tps, nil
}

func REM_GetTypedTableInfo(t *sql.Table) error {

	var err error
	var rows *sql.Rows

	q :=
		`select 
		user_defined_type_name 
	from information_schema.tables where table_name = $1`

	if rows, err = r.conn.Query(q, t.Name); err != nil {
		return err
	}

	for rows.Next() {
		var ptname *string
		if err = rows.Scan(&ptname); err != nil {
			return err
		}
		if ptname != nil {
			t.Type = *ptname
		}
	}

	return nil
}

func REM_GetMatchTables(userTables []sql.Table) ([]Table, error) {

	tbls := make([]Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := GetTableDef(remote, userTables[i].Name)
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

func RemoteGetAllUnique(tableName string) ([]Unique, error) {

	var rows *sql.Rows
	var err error

	rows, err = r.conn.Query(
		`select tc.constraint_name
		from information_schema.table_constraints tc
		where tc.table_name = $1 and tc.constraint_type='UNIQUE'`, tableName)
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
			`select column_name, false as Is_descending
			from information_schema.constraint_column_usage 
			where constraint_name = $1`, c.Name)
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

func GetAllCheck(tableName string) ([]Check, error) {

	var rows *sql.Rows
	var err error

	rows, err = r.conn.Query(
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
		`select tc.constraint_name, tf.table_name as ref_table 
		from information_schema.table_constraints AS tc 
		join lateral ( select * from information_schema.constraint_column_usage ccu where ccu.constraint_name = tc.constraint_name limit 1) tf on true
		WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=$1;`, tableName)

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
			`select column_name, false as is_descending
			from information_schema.key_column_usage
			where constraint_name = $1`, fk.Name)
		if err != nil {
			return nil, err
		}

		fk.Columns, err = MapCColumns(rows)
		if err != nil {
			return nil, err
		}

		rows, err = r.conn.Query(
			`select column_name, false as is_descending
			from information_schema.constraint_column_usage
			where constraint_name = $1`, fk.Name)
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
		`select constraint_name 
		from information_schema.table_constraints 
		where constraint_type ='PRIMARY KEY' and table_name = $1`, tableName)
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
		`select column_name, false as is_descending
		from information_schema.key_column_usage 
		where constraint_name = $1`, ret.Name)
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

func RemoteGetTypes(r *Remote, localTypes []Type) ([]Type, error) {

	if r.tp != Pgsql {
		// not implemented to mssql yet
		return nil, nil
	}

	return PgsqlGetTypes(r, localTypes)
}
