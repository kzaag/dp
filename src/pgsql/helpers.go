package pgsql

import (
	"container/list"
	"database/sql"
)

func TExists(t *Type, localTypes []Type) bool {
	for i := 0; i < len(localTypes); i++ {
		if localTypes[i].Name == t.Name {
			return true
		}
	}
	return false
}

func MapColumns(r *sql.Rows) ([]Column, error) {

	buff := list.New()
	for r.Next() {
		var el Column
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
		el.FullType = ColumnTypeStr(el)
		buff.PushBack(el)
	}

	var ret = make([]Column, buff.Len())
	for i, x := 0, buff.Front(); i < buff.Len(); i, x = i+1, x.Next() {
		ret[i] = x.Value.(Column)
	}

	return ret, nil
}
