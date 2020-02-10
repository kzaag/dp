package main

import (
	"container/list"
	"strconv"
	"strings"
)

func PgsqlColumnType(column *RawColumn) string {
	cs := ""
	switch strings.ToLower(column.Type) {
	case "bit":
		fallthrough
	case "varbit":
		fallthrough
	case "bit varying":
		fallthrough
	case "char":
		if column.Type == "char" {
			column.Type = "character"
		}
		fallthrough
	case "character":
		fallthrough
	case "varchar":
		if column.Type == "varchar" {
			column.Type = "character varying"
		}
		fallthrough
	case "character varying":
		if column.Max_length == -1 {
			cs += strings.ToUpper(column.Type) + "(MAX)"
		} else {
			cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(column.Max_length) + ")"
		}
	case "int8":
		fallthrough
	case "bigint":
		fallthrough
	case "serial8":
		fallthrough
	case "bigserial":
		fallthrough
	case "bool":
		if column.Type == "bool" {
			column.Type = "boolean"
		}
		fallthrough
	case "boolean":
		fallthrough
	case "box":
		fallthrough
	case "bytea":
		fallthrough
	case "cidr":
		fallthrough
	case "circle":
		fallthrough
	case "date":
		fallthrough
	case "double precision":
		fallthrough
	case "float8":
		if column.Type == "float8" {
			column.Type = "double precision"
		}
		fallthrough
	case "inet":
		fallthrough
	case "int":
		if column.Type == "int" {
			column.Type = "integer"
		}
		fallthrough
	case "integer":
		fallthrough
	case "json":
		fallthrough
	case "jsonb":
		fallthrough
	case "line":
		fallthrough
	case "lseg":
		fallthrough
	case "macaddr":
		fallthrough
	case "money":
		fallthrough
	case "path":
		fallthrough
	case "pg_lsn":
		fallthrough
	case "point":
		fallthrough
	case "polygon":
		fallthrough
	case "float4":
		if column.Type == "float4" {
			column.Type = "real"
		}
		fallthrough
	case "real":
		fallthrough
	case "smallint":
		fallthrough
	case "int2":
		if column.Type == "int2" {
			column.Type = "smallint"
		}
		fallthrough
	case "smallserial":
		fallthrough
	case "serial 2":
		fallthrough
	case "serial":
		fallthrough
	case "serial 4":
		fallthrough
	case "text":
		fallthrough
	case "tsquery":
		fallthrough
	case "tsvector":
		fallthrough
	case "txid_snapshot":
		fallthrough
	case "uuid":
		fallthrough
	case "xml":
		fallthrough
	case "int4":
		cs += strings.ToUpper(column.Type)
	case "numeric":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + "," + strconv.Itoa(int(column.Scale)) + ")"
	case "time":
		fallthrough
	case "timetz":
		fallthrough
	case "interval":
		cs += strings.ToUpper(column.Type) + "(" + strconv.Itoa(int(column.Precision)) + ")"
	case "timestamp":
		if column.Type == "timestamp" {
			column.Type = "timestamp without time zone"
		}
		fallthrough
	case "timestamp without time zone":
		cs += "TIMESTAMP(" + strconv.Itoa(int(column.Precision)) + ") WITHOUT TIME ZONE"
	case "timestamptz":
		if column.Type == "timestamptz" {
			column.Type = "timestamp with time zone"
		}
		fallthrough
	case "timestamp with time zone":
		cs += "TIMESTAMP(" + strconv.Itoa(int(column.Precision)) + ") WITH TIME ZONE"
	default:
		panic("no pgsql mapper for type : " + strings.ToLower(column.Type))
	}
	return cs
}

func PgsqlAlterColumn(r *Remote, tableName string, sc *Column, c *Column) string {

	ret := ""

	if sc.Type != c.Type {

		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name + " SET DATA TYPE " + c.Type

		// here theoretically could be introduced USING ( ... ) to the alter
		// but it seems too complex to properly introduce trimming for any pg type.
		// thus leaving it empty - user must handle this alone

		s += ";\n"
		ret += s
	}

	if sc.Is_nullable != c.Is_nullable {
		s := "ALTER TABLE " + tableName + " ALTER COLUMN " + c.Name
		if c.Is_nullable {
			s += " DROP NOT NULL"
		} else {
			s += " SET NOT NULL"
		}
		ret += s + ";\n"
	}

	return ret
}

func PgsqlGetEnum(r *Remote) (*list.List, error) {

	q := `select typname from pg_type where typcategory = 'E';`
	rows, err := r.conn.Query(q)
	if err != nil {
		return nil, err
	}

	tps := list.New()
	for rows.Next() {
		var enumname string
		err := rows.Scan(&enumname)
		if err != nil {
			return nil, err
		}
		tps.PushBack(Type{enumname, TT_Enum, nil, nil})
	}

	query := `select e.enumlabel
			 from pg_enum e
			 join pg_type t ON e.enumtypid = t.oid
			 where typname = $1
			 order by enumsortorder;`

	for x := tps.Front(); x != nil; x = x.Next() {
		tp := x.Value.(Type)
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

func PgsqlGetComposite(r *Remote) (*list.List, error) {

	q := `select typname from pg_type where typcategory = 'C';`
	rows, err := r.conn.Query(q)
	if err != nil {
		return nil, err
	}

	tps := list.New()
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}
		tps.PushBack(Type{name, TT_Composite, nil, nil})
	}

	query := `SELECT attname, format_type(atttypid, atttypmod) AS type
			FROM   pg_attribute
			WHERE  attrelid = $1::regclass
			AND    attnum > 0
			AND    NOT attisdropped
			ORDER  BY attnum;`

	for x := tps.Front(); x != nil; x = x.Next() {
		tp := x.Value.(Type)
		rows, err := r.conn.Query(query, tp.Name)
		if err != nil {
			return nil, err
		}

		cols := list.New()
		for rows.Next() {
			var colname string
			var coltype string
			err := rows.Scan(&colname, &coltype)
			c := Column{colname, coltype, false, false}
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
