package cass

type Column struct {
	Name string
	Type string
}

type SASIIndex struct {
	Name   string
	Column string
}

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
	PartitionColumns  []PKPartitionColumn  `yaml:"partition"`
	ClusteringColumns []PKClusteringColumn `yaml:"clustering"`
}

type Table struct {
	Name        string
	Columns     map[string]*Column
	PrimaryKey  *PrimaryKey           `yaml:"primary"`
	SASIIndexes map[string]*SASIIndex `yaml:"sasi_index"`
}

type MaterializedView struct {
	Name        string
	Base        string
	Columns     map[string]*Column
	WhereClause string      `yaml:"where"`
	PrimaryKey  *PrimaryKey `yaml:"primary"`
}
