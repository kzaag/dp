package pgsql

import (
	"container/list"
	"database/sql"

	"github.com/kzaag/dp/rdbms"
)

func RemoteGetEnum(db *sql.DB) ([]Type, error) {

	q := `select typname from pg_type where typcategory = 'E'`
	rows, err := db.Query(q)
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
		tps = append(tps, Type{enumname, TypeEnum, nil, nil})
	}

	query := `select e.enumlabel
			 from pg_enum e
			 join pg_type t ON e.enumtypid = t.oid
			 where typname = $1
			 order by enumsortorder;`

	for i := 0; i < len(tps); i++ {

		tp := &tps[i]
		rows, err := db.Query(query, tp.Name)
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

func RemoteGetComposite(db *sql.DB) ([]Type, error) {

	q := `select typname from pg_type where typcategory = 'C' and typarray <> 0;`
	rows, err := db.Query(q)
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
		tps = append(tps, Type{name, TypeComposite, nil, nil})
	}

	query := `SELECT attname, UPPER(format_type(atttypid, atttypmod)) AS type
			FROM   pg_attribute
			WHERE  attrelid = $1::regclass
			AND    attnum > 0
			AND    NOT attisdropped
			ORDER  BY attnum;`

	for i := 0; i < len(tps); i++ {
		tp := &tps[i]
		rows, err := db.Query(query, tp.Name)
		if err != nil {
			return nil, err
		}

		cols := list.New()
		for rows.Next() {
			var colname string
			var coltype string
			err := rows.Scan(&colname, &coltype)
			c := rdbms.Column{
				Name:      colname,
				Type:      "",
				FullType:  coltype,
				Length:    -1,
				Precision: -1,
				Scale:     -1,
				Nullable:  false,
				Identity:  false,
				Tags: map[string]struct{}{
					TypeComposite: {},
				}}
			if err != nil {
				return nil, err
			}
			cols.PushBack(c)
		}

		tp.Columns = make([]rdbms.Column, cols.Len())

		for i, x := 0, cols.Front(); x != nil; i, x = i+1, x.Next() {
			tp.Columns[i] = x.Value.(rdbms.Column)
		}
	}

	return tps, nil
}

func RemoteGetTypes(db *sql.DB, localTypes []Type) ([]Type, error) {

	enums, err := RemoteGetEnum(db)
	if err != nil {
		return nil, err
	}

	composite, err := RemoteGetComposite(db)
	if err != nil {
		return nil, err
	}

	elen := len(enums)
	clen := len(composite)

	cb := make([]Type, elen+clen)

	for i := 0; i < elen; i++ {
		v := &enums[i]
		if HelperTypeExists(v, localTypes) {
			cb[i] = *v
		}
	}

	for i := 0; i < clen; i++ {
		v := &composite[i]
		if HelperTypeExists(v, localTypes) {
			cb[elen+i] = *v
		}
	}

	return cb, nil
}

func RemoteGetAllPK(db *sql.DB, tableName string) (*rdbms.PrimaryKey, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
		`select constraint_name 
		from information_schema.table_constraints 
		where constraint_type ='PRIMARY KEY' and table_name = $1`, tableName)
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
		`select column_name, false as is_descending
		from information_schema.key_column_usage 
		where constraint_name = $1`, ret.Name)
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

func RemoteGetAllColumn(db *sql.DB, tableName string) ([]rdbms.Column, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
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

	c, err := HelperMapColumns(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllUnique(db *sql.DB, tableName string) ([]rdbms.Unique, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
		`select tc.constraint_name
		from information_schema.table_constraints tc
		where tc.table_name = $1 and tc.constraint_type='UNIQUE'`, tableName)
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
			`select column_name, false as Is_descending
			from information_schema.constraint_column_usage 
			where constraint_name = $1`, c.Name)
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

	c, err := rdbms.HelperMapChecks(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllFK(db *sql.DB, tableName string) ([]rdbms.ForeignKey, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
		`select tc.constraint_name, tf.table_name as ref_table 
		from information_schema.table_constraints AS tc 
		join lateral ( select * from information_schema.constraint_column_usage ccu where ccu.constraint_name = tc.constraint_name limit 1) tf on true
		WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name=$1;`, tableName)

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
			`select column_name, false as is_descending
			from information_schema.key_column_usage
			where constraint_name = $1`, fk.Name)
		if err != nil {
			return nil, err
		}

		fk.Columns, err = rdbms.HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}

		rows, err = db.Query(
			`select column_name, false as is_descending
			from information_schema.constraint_column_usage
			where constraint_name = $1`, fk.Name)
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

func RemoteGetAllIx(db *sql.DB, tableName string) ([]rdbms.Index, error) {

	var rows *sql.Rows
	var err error
	rows, err = db.Query(
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
		c.Columns, err = rdbms.HelperMapIxColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetTypedTableInfo(db *sql.DB, t *rdbms.Table) error {

	var err error
	var rows *sql.Rows

	q :=
		`select 
		user_defined_type_name 
	from information_schema.tables where table_name = $1`

	if rows, err = db.Query(q, t.Name); err != nil {
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

func RemoteGetTable(db *sql.DB, tableName string) (*rdbms.Table, error) {

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

	if err = RemoteGetTypedTableInfo(db, &tbl); err != nil {
		return nil, err
	}

	return &tbl, nil
}

func RemoteGetMatchedTables(db *sql.DB, userTables []rdbms.Table) ([]rdbms.Table, error) {

	tbls := make([]rdbms.Table, len(userTables))

	for i := 0; i < len(userTables); i++ {
		tbl, err := RemoteGetTable(db, userTables[i].Name)
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
