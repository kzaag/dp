package pgsql

const (
	TypeEnum      string = "enum"
	TypeComposite string = "composite"
)

type Type struct {
	Name    string
	Type    string
	Values  []string
	Columns []Column
}

type IndexColumn struct {
	Name               string
	Is_descending      bool
	Is_Included_column bool
}

type Index struct {
	Name         string
	Is_clustered bool
	//Is_unique bool
	Columns []IndexColumn
	Using   string
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
	Constraint `yaml:",inline"`
}

type Check struct {
	Name string
	Def  string
}

type PrimaryKey struct {
	Constraint `yaml:",inline"`
}

type ForeignKey struct {
	Ref_columns []ConstraintColumn
	Ref_table   string
	Constraint  `yaml:",inline"`
	OnDelete    string `yaml:"on_delete"`
	OnUpdate    string `yaml:"on_update"`
}

// type ColumnMeta int

// const (
// 	// standard column
// 	//CM_None ColumnMeta = 0
// 	// composite type column
// 	CM_CompType ColumnMeta = 1
// )

type Column struct {
	Name string

	/*
		user should realize that specifying FullType instead of time does not require
		specifying Precision, Scale and type fields which may seem to be easier but
		becuase many rdbs use aliases for type ( user inputs `INT` but pgsql will output this as `INTEGER` )
		user must be very cautious whilst doing it.

		however user type is supported anyways because of situations in which we cant get full column architecture
		but only string representation
	*/
	Type      string
	FullType  string
	Length    int
	Precision int
	Scale     int
	Nullable  bool
	Identity  bool
	Default   string
	/*
		special value used during converting column types
		if specified then during type conversion this will be used
	*/
	HintDTUsing string `yaml:"hint_dt_using"`

	Tags map[string]struct{}
}

func (c *Column) HasTag(tag string) bool {
	if c.Tags == nil {
		return false
	}
	if _, ok := c.Tags[tag]; ok {
		return true
	}
	return false
}

type Table struct {
	Name    string
	Type    string
	Columns []Column
	Foreign []ForeignKey
	Unique  []Unique
	Primary *PrimaryKey
	Check   []Check
	Indexes []Index
}
