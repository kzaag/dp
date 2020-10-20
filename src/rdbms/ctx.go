package rdbms

type Rdbms struct {
	remTables   []Table
	localTables []Table
	create      *string
	drop        *string
	//
	AddColumn      func(string, *Column) string
	AlterColumn    func(string, *Column, *Column) string
	DropColumn     func(string, *Column) string
	ColumnType     func(*Column) string
	AddIndex       func(string, *Index) string
	DropIndex      func(string, *Index) string
	DropConstraint func(string, *Constraint) string
	AddUnique      func(string, *Unique) string
	AddCheck       func(string, *Check) string
	AddFK          func(string, *ForeignKey) string
	AddPK          func(string, *PrimaryKey) string
	CreateTable    func(*Table) string
}

func (m *Rdbms) SetBuffers(create *string, drop *string) {
	m.create = create
	m.drop = drop
}
