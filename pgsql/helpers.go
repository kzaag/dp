package pgsql

import (
	"container/list"
	"database/sql"
)

func HelperTypeExists(t *Type, localTypes []Type) bool {
	for i := 0; i < len(localTypes); i++ {
		if localTypes[i].Name == t.Name {
			return true
		}
	}
	return false
}

func HelperMapColumns(r *sql.Rows) ([]Column, error) {

	var el Column
	ret := make([]Column, 0, 10)

	for r.Next() {
		err := r.Scan(
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
