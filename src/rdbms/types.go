package rdbms

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

type ColumnMeta int

const (
	// standard column
	//CM_None ColumnMeta = 0
	// composite type column
	CM_CompType ColumnMeta = 1
)

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

	// extra metadata which specifies extra behaviour about column in specific dbms
	//( example: like composite type column which cannot be not null, or different add/drop/alter syntax applies)
	Meta ColumnMeta
}

// // will set FullType field based on rest of fields
// func (rc *Column) SetFullType(r *Remote) {
// 	rc.FullType = RemoteColumnType(r, rc)
// }

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

// type DbConstraint struct {
// 	Name string
// 	Type string
// }

// type DbConf struct {
// 	Server   string
// 	Database string
// 	User     string
// 	Password string
// 	Other    map[string]string
// 	Pre      []string
// 	Post     []string
// }

// func (conf *DbConf) ToCS() string {
// 	return "server=" + conf.Server +
// 		";user id=" + conf.User +
// 		";password=" + conf.Password +
// 		";database=" + conf.Database +
// 		";"
// }
