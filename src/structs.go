package main

type IndexColumn struct {
	Name               string
	Is_descending      bool
	Is_Included_column bool
}

type Index struct {
	Name      string
	Type      string
	Is_unique bool
	Columns   []IndexColumn
}

type ConstraintColumn struct {
	Name          string
	Is_descending bool
}

type Constraint struct {
	Name    string
	Columns []ConstraintColumn
}

type Unique struct {
	Constraint
}

type Check struct {
	Name string
	Def  string
}

type PrimaryKey struct {
	Constraint
}

type ForeignKey struct {
	Ref_columns []ConstraintColumn
	Ref_table   string
	Constraint
}

type RawColumn struct {
	Name        string
	Type        string
	Max_length  int
	Precision   int
	Scale       int
	Is_nullable bool
	Is_Identity bool
}

func (rc *RawColumn) ToColumn(r *Remote) Column {
	return Column{rc.Name, RemoteColumnType(r, rc), rc.Is_nullable, rc.Is_Identity}
}

type Column struct {
	Name        string
	Type        string
	Is_nullable bool
	Is_Identity bool
}

type Table struct {
	Name    string
	Columns []Column
	Foreign []ForeignKey
	Unique  []Unique
	Primary *PrimaryKey
	Check   []Check
	Indexes []Index
}

const (
	TT_Enum      string = "Enum"
	TT_Composite string = "Composite"
)

type Type struct {
	Name    string
	Type    string
	Values  []string
	Columns []Column
}

type DbConstraint struct {
	Name string
	Type string
}

type DbConf struct {
	Server   string
	Database string
	User     string
	Password string
	Other    map[string]string
	Pre      []string
	Post     []string
}

func (conf *DbConf) ToCS() string {
	return "server=" + conf.Server +
		";user id=" + conf.User +
		";password=" + conf.Password +
		";database=" + conf.Database +
		";"
}
