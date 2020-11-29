package cql

type Column struct {
	Name string
	Type string
}

// type Index struct {
// }

type PKClusteringColumn struct {
	Name     string
	Order    string
	Position int
}

type PKPartitionColumn struct {
	Name     string
	Position int
}

type PrimaryKey struct {
	PartitionColumns  []PKPartitionColumn
	ClusteringColumns []PKClusteringColumn
}

type Table struct {
	Name       string
	Columns    map[string]*Column
	PrimaryKey *PrimaryKey
}

type MaterializedView struct {
	Name        string
	Base        string
	Columns     map[string]*Column
	WhereClause string
	PrimaryKey  *PrimaryKey
}
