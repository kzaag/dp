package pgsql

import (
	"database-project/rdbms"
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

func HelperMapColumns(r *sql.Rows) ([]rdbms.Column, error) {

	var el rdbms.Column
	ret := make([]rdbms.Column, 0, 10)

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
