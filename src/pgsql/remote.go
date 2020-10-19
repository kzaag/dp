package pgsql

import (
	"container/list"
	"database/sql"
)

func GetTypes(r *Remote, localTypes []Type) ([]Type, error) {

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

func GetEnum(r *Remote) ([]Type, error) {

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

func GetComposite(r *Remote) ([]Type, error) {

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

func GetTypedTableInfo(r *Remote, t *Table) error {

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
