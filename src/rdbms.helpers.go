package main

import (
	"container/list"
	"database/sql"
)

func MapTableNames(r *sql.Rows) ([]string, error) {
	buff := list.New()
	for r.Next() {
		var t string
		err := r.Scan(&t)
		if err != nil {
			return nil, err
		}
		buff.PushBack(t)
	}

	var ret = make([]string, buff.Len())
	for i, x := 0, buff.Front(); i < buff.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(string)
	}

	return ret, nil
}

func MapCheck(r *sql.Rows) ([]Check, error) {
	tmp := list.New()
	for r.Next() {
		var k Check
		err := r.Scan(&k.Name, &k.Def)
		if err != nil {
			return nil, err
		}
		tmp.PushBack(k)
	}

	cs := make([]Check, tmp.Len())
	for i, x := 0, tmp.Front(); i < tmp.Len(); i, x = i+1, x.Next() {
		cs[i] = x.Value.(Check)
	}

	return cs, nil
}

func MapColumns(remote *Remote, r *sql.Rows) ([]Column, error) {

	buff := list.New()
	for r.Next() {
		var el RawColumn
		err := r.Scan(
			&el.Name,
			&el.Type,
			&el.Max_length,
			&el.Precision,
			&el.Scale,
			&el.Is_nullable,
			&el.Is_Identity)
		if err != nil {
			return nil, err
		}
		buff.PushBack(el.ToColumn(remote))
	}

	var ret = make([]Column, buff.Len())
	for i, x := 0, buff.Front(); i < buff.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(Column)
	}

	return ret, nil
}

func MapCColumns(r *sql.Rows) ([]ConstraintColumn, error) {
	tmp := list.New()

	for r.Next() {
		var column ConstraintColumn
		if err := r.Scan(&column.Name, &column.Is_descending); err != nil {
			return nil, err
		}
		tmp.PushBack(column)
	}

	cols := make([]ConstraintColumn, tmp.Len())
	for i, x := 0, tmp.Front(); i < tmp.Len(); i, x = i+1, x.Next() {
		cols[i] = x.Value.(ConstraintColumn)
	}
	return cols, nil
}

func MapIxColumns(rows *sql.Rows) ([]IndexColumn, error) {
	ccs := list.New()
	for rows.Next() {
		var cc IndexColumn
		err := rows.Scan(&cc.Name, &cc.Is_descending, &cc.Is_Included_column)

		if err != nil {
			panic(err)
		}
		ccs.PushBack(cc)
	}

	ret := make([]IndexColumn, ccs.Len())
	for i, x := 0, ccs.Front(); i < ccs.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(IndexColumn)
	}

	return ret, nil
}
