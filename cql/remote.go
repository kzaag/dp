package cql

import (
	"sort"

	"github.com/gocql/gocql"
)

func RemoteGetColumns(
	sess *gocql.Session,
	db string,
	tablename string,
) (map[string]*Column, error) {
	const q = `select 
			column_name,
			type 
		from system_schema.columns 
		where keyspace_name = ? and table_name = ?`
	i := sess.Query(q, db, tablename).Iter()
	ret := make(map[string]*Column)
	var col *Column
	col = new(Column)
	for i.Scan(&col.Name, &col.Type) {
		ret[col.Name] = col
		col = new(Column)
	}
	return ret, i.Close()
}

func RemoteGetPK(
	sess *gocql.Session,
	db string,
	tablename string,
) (*PrimaryKey, error) {
	const q = `select 
			column_name, 
			clustering order, 
			kind, 
			position
		from system_schema.columns 
		where keyspace_name = ? and table_name = ?`
	type PKcanditate struct {
		Name     string
		Order    string
		Kind     string
		Position int
	}
	var tmp PKcanditate
	candidates := make([]PKcanditate, 0, 10)
	i := sess.Query(q, db, tablename).Iter()
	for i.Scan(&tmp.Name, &tmp.Order, &tmp.Kind, &tmp.Position) {
		candidates = append(candidates, tmp)
	}
	if err := i.Close(); err != nil {
		return nil, err
	}

	var pk PrimaryKey
	// pk.ClusteringColumns = make([]PKClusteringColumn)
	// pk.PartitionColumns = make([]PKPartitionColumn)

	for i := range candidates {
		switch candidates[i].Kind {
		case "partition_key":
			pk.PartitionColumns = append(
				pk.PartitionColumns,
				PKPartitionColumn{
					Name:     candidates[i].Name,
					Position: candidates[i].Position,
				})
		case "clustering":
			pk.ClusteringColumns = append(
				pk.ClusteringColumns,
				PKClusteringColumn{
					Name:     candidates[i].Name,
					Position: candidates[i].Position,
					Order:    candidates[i].Order,
				})
		}
	}

	sort.Slice(pk.PartitionColumns, func(i, j int) bool {
		return pk.PartitionColumns[i].Position > pk.PartitionColumns[j].Position
	})

	sort.Slice(pk.ClusteringColumns, func(i, j int) bool {
		return pk.ClusteringColumns[i].Position > pk.ClusteringColumns[j].Position
	})

	return &pk, nil
}

func RemoteGetTable(
	sess *gocql.Session,
	db, table string,
) (*Table, error) {
	pk, err := RemoteGetPK(sess, db, table)
	if err != nil {
		return nil, err
	}
	cols, err := RemoteGetColumns(sess, db, table)
	if err != nil {
		return nil, err
	}
	return &Table{
		Name:       table,
		Columns:    cols,
		PrimaryKey: pk,
	}, nil
}

func RemoteGetMatchingTables(
	sess *gocql.Session,
	db string,
	tables []Table,
) ([]Table, error) {
	const q = "select name from system_schema.tables where keyspace_name = ?"
	i := sess.Query(q, db).Iter()
	var tmp string
	tablenames := make([]string, 0, 10)
	for i.Scan(&tmp) {
		tablenames = append(tablenames, tmp)
	}
	if err := i.Close(); err != nil {
		return nil, err
	}
	ret := make([]Table, len(tablenames))
	mt := make(map[string]struct{})
	for i := range tables {
		mt[tables[i].Name] = struct{}{}
	}
	var err error
	var it = 0
	for _, tn := range tablenames {
		if _, ok := mt[tn]; !ok {
			continue
		}
		ret[it].Name = tn
		if ret[it].PrimaryKey, err = RemoteGetPK(sess, db, tn); err != nil {
			return nil, err
		}
		if ret[it].Columns, err = RemoteGetColumns(sess, db, tn); err != nil {
			return nil, err
		}
		it++
	}
	return ret[:it], err
}

func RemoteGetViews(
	sess *gocql.Session,
	db string,
) ([]MaterializedView, error) {
	q := `
		select view_name, base_table_name, where_clause 
		from system_schema.views 
		where keyspace_name = ?`
	i := sess.Query(q, db).Iter()
	var tmp MaterializedView
	var err error
	ret := make([]MaterializedView, 0, 10)
	for i.Scan(&tmp.Name, &tmp.Base, &tmp.WhereClause) {
		if tmp.Columns, err = RemoteGetColumns(sess, db, tmp.Name); err != nil {
			return nil, err
		}
		if tmp.PrimaryKey, err = RemoteGetPK(sess, db, tmp.Name); err != nil {
			return nil, err
		}
	}
	return ret, nil
}
