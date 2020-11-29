package cass

import (
	"fmt"
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
			clustering_order, 
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

func RemoteGetMatchingTables(
	sess *gocql.Session,
	db string,
	tables map[string]*Table,
) (map[string]*Table, error) {
	const q = "select table_name from system_schema.tables where keyspace_name = ?"
	i := sess.Query(q, db).Iter()
	var tmp string
	tablenames := make([]string, 0, 10)
	for i.Scan(&tmp) {
		tablenames = append(tablenames, tmp)
	}
	if err := i.Close(); err != nil {
		return nil, err
	}
	ret := make(map[string]*Table)
	mt := make(map[string]struct{})
	for i := range tables {
		mt[tables[i].Name] = struct{}{}
	}
	var err error
	for _, tn := range tablenames {
		if _, ok := mt[tn]; !ok {
			continue
		}
		t := &Table{}
		t.Name = tn
		if t.PrimaryKey, err = RemoteGetPK(sess, db, tn); err != nil {
			return nil, err
		}
		if t.Columns, err = RemoteGetColumns(sess, db, tn); err != nil {
			return nil, err
		}
		if t.SASIIndexes, err = RemoteGetSASIIndexes(sess, db, tn); err != nil {
			return nil, err
		}
		ret[t.Name] = t
	}
	return ret, err
}

func RemoteGetViews(
	sess *gocql.Session,
	db string,
) (map[string]*MaterializedView, error) {
	q := `select view_name, base_table_name, where_clause 
		from system_schema.views 
		where keyspace_name = ?`
	i := sess.Query(q, db).Iter()
	var tmp *MaterializedView
	var err error
	ret := make(map[string]*MaterializedView)
	tmp = new(MaterializedView)
	for i.Scan(&tmp.Name, &tmp.Base, &tmp.WhereClause) {
		if tmp.Columns, err = RemoteGetColumns(sess, db, tmp.Name); err != nil {
			return nil, err
		}
		if tmp.PrimaryKey, err = RemoteGetPK(sess, db, tmp.Name); err != nil {
			return nil, err
		}
		ret[tmp.Name] = tmp
		tmp = new(MaterializedView)
	}
	return ret, nil
}

func RemoteGetSASIIndexes(
	sess *gocql.Session, database, tablename string,
) (map[string]*SASIIndex, error) {
	q := `
		select index_name, options
		from system_schema.indexes where keyspace_name = ? and table_name = ?`
	i := sess.Query(q, database, tablename).Iter()
	tmp := new(SASIIndex)
	ret := make(map[string]*SASIIndex)
	var options map[string]string

	for i.Scan(&tmp.Name, &options) {
		if options["class_name"] != "org.apache.cassandra.index.sasi.SASIIndex" {
			continue
		}
		tmp.Column = options["target"]
		if tmp.Column == "" {
			_ = i.Close()
			return nil, fmt.Errorf("Couldnt find target for sasi index")
		}
		ret[tmp.Name] = tmp
		tmp = new(SASIIndex)
	}
	return ret, i.Close()
}
