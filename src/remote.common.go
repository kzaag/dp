package main

import (
	"container/list"
	"database/sql"
)

func MapConstraintCols(rows *sql.Rows) ([]ConstraintColumn, error) {
	ccs := list.New()
	for rows.Next() {
		var cc ConstraintColumn
		err := rows.Scan(&cc.Name, &cc.Is_descending)
		if err != nil {
			return nil, err
		}
		ccs.PushBack(cc)
	}

	ret := make([]ConstraintColumn, ccs.Len())
	for i, x := 0, ccs.Front(); i < ccs.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(ConstraintColumn)
	}

	return ret, nil
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
