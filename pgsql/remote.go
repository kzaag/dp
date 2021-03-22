package pgsql

import (
	"container/list"
	"database/sql"
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
			rows.Close()
			return nil, err
		}
		tps = append(tps, Type{enumname, TypeEnum, nil, nil})
	}
	rows.Close()
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
				rows.Close()
				return nil, err
			}
			cols.PushBack(colname)
		}
		rows.Close()
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
		defer rows.Close()
		cols := list.New()
		for rows.Next() {
			var colname string
			var coltype string
			err := rows.Scan(&colname, &coltype)
			c := Column{
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

		tp.Columns = make([]Column, cols.Len())

		for i, x := 0, cols.Front(); x != nil; i, x = i+1, x.Next() {
			tp.Columns[i] = x.Value.(Column)
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

func RemoteGetAllPK(db *sql.DB, tableName string) (*PrimaryKey, error) {

	var rows *sql.Rows
	var err error

	rows, err = db.Query(
		`select constraint_name 
		from information_schema.table_constraints 
		where constraint_type ='PRIMARY KEY' and table_name = $1`, tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}

	var ret PrimaryKey
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

	cols, err := HelperMapCColumns(rows)
	if err != nil {
		return nil, err
	}

	ret.Columns = cols

	return &ret, nil
}

func RemoteGetAllColumn(db *sql.DB, tableName string) ([]Column, error) {

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
			coalesce(case when is_identity = 'YES' then true else false end, false),
			coalesce(column_default, '')
		from information_schema.columns 
		where table_name = $1
		order by ordinal_position asc;`, tableName)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	c, err := HelperMapColumns(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllUnique(db *sql.DB, tableName string) ([]Unique, error) {

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
		var k Unique
		err := rows.Scan(&k.Name)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	defer rows.Close()
	ret := make([]Unique, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Unique)
		rows, err = db.Query(
			`select column_name, false as Is_descending
			from information_schema.constraint_column_usage 
			where constraint_name = $1`, c.Name)
		if err != nil {
			return nil, err
		}
		c.Columns, err = HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetAllCheck(db *sql.DB, tableName string) ([]Check, error) {

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

	defer rows.Close()
	c, err := HelperMapChecks(rows)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func RemoteGetAllFK(db *sql.DB, tableName string) ([]ForeignKey, error) {

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

		rows, err = db.Query(
			`select update_rule, delete_rule from information_schema.referential_constraints 
			where constraint_name = $1`, fk.Name,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		if rows.Next() {
			if err := rows.Scan(&fk.OnUpdate, &fk.OnDelete); err != nil {
				return nil, err
			}
		}

		rows, err = db.Query(
			`select column_name, false as is_descending
			from information_schema.key_column_usage
			where constraint_name = $1`, fk.Name)
		if err != nil {
			return nil, err
		}

		fk.Columns, err = HelperMapCColumns(rows)
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
		fk.Ref_columns, err = HelperMapCColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = fk
	}

	return ret, nil
}

func RemoteGetAllIx(db *sql.DB, tableName string) ([]Index, error) {

	var rows *sql.Rows
	var err error
	rows, err = db.Query(
		`select
			i.relname as index_name,
			--ix.indisunique as is_unique,
			ix.indisclustered as is_clustered,
			am.amname as type
		from
			pg_class t,
			pg_class i,
			pg_index ix,
			pg_am am
		where
			t.oid = ix.indrelid
			and i.oid = ix.indexrelid
			and t.relkind = 'r'
			and t.relname = $1
			and indisprimary = false
			and indisunique = false
			and am.oid = i.relam`, tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tmp := list.New()

	for rows.Next() {
		var k Index
		var c bool
		var t string
		err := rows.Scan(&k.Name, &c, &t)
		k.Is_clustered = c
		k.Using = t
		if err != nil {
			panic(err)
		}
		tmp.PushBack(k)
	}

	ret := make([]Index, tmp.Len())
	for i, x := 0, tmp.Front(); x != nil; i, x = i+1, x.Next() {
		c := x.Value.(Index)

		rows, err = db.Query(
			`SELECT 
			pg_catalog.pg_get_indexdef(ci.oid, (i.keys).n, false) AS column_name, 
		   CASE i.indoption[(i.keys).n - 1] & 1 
			   WHEN 1 THEN true 
			   ELSE false
			 END AS is_descending,
			CASE 
			   WHEN i.indoption[(i.keys).n - 1] is null THEN true 
			   ELSE false
			 END AS is_included
		-- debug fields, and for future reference and testing...
		  --,pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS filter_condition 
		  --,i.*
		  --,(i.keys).n
	FROM pg_catalog.pg_class ct 
	  --JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) 
	  JOIN (SELECT i.indexrelid, i.indrelid, i.indoption, 
			  i.indisunique, i.indisclustered, i.indpred, 
			  i.indexprs, 
			  information_schema._pg_expandarray(i.indkey) AS keys 
			FROM pg_catalog.pg_index i) i 
		ON (ct.oid = i.indrelid) 
	  JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) 
	  JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) 
	WHERE ci.relname = $1
	order by (i.keys).n;
	`, c.Name)

		if err != nil {
			return nil, err
		}
		defer rows.Close()
		c.Columns, err = HelperMapIxColumns(rows)
		if err != nil {
			return nil, err
		}
		ret[i] = c
	}

	return ret, nil
}

func RemoteGetTypedTableInfo(db *sql.DB, t *Table) error {

	var err error
	var rows *sql.Rows

	q :=
		`select 
		user_defined_type_name 
	from information_schema.tables where table_name = $1`

	if rows, err = db.Query(q, t.Name); err != nil {
		return err
	}
	defer rows.Close()

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

func RemoteGetTable(db *sql.DB, tableName string) (*Table, error) {

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

	var tbl Table
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

func RemoteGetMatchedTables(db *sql.DB, userTables []Table) ([]Table, error) {

	tbls := make([]Table, len(userTables))

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
