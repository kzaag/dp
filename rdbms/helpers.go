package rdbms

import (
	"container/list"
	"database/sql"
)

func HelperMapChecks(r *sql.Rows) ([]Check, error) {
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

func HelperMapCColumns(r *sql.Rows) ([]ConstraintColumn, error) {
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

func HelperMapIxColumns(rows *sql.Rows) ([]IndexColumn, error) {
	ccs := list.New()
	for rows.Next() {
		var cc IndexColumn
		err := rows.Scan(&cc.Name, &cc.Is_descending, &cc.Is_Included_column)

		if err != nil {
			return nil, err
		}
		ccs.PushBack(cc)
	}

	ret := make([]IndexColumn, ccs.Len())
	for i, x := 0, ccs.Front(); i < ccs.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(IndexColumn)
	}

	return ret, nil
}
