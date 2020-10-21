package rdbms

type IRdbmsDef interface {
	AddColumn(string, *Column) string
	AlterColumn(string, *Column, *Column) string
	DropColumn(string, *Column) string
	ColumnType(*Column) string
	AddIndex(string, *Index) string
	DropIndex(string, *Index) string
	DropConstraint(string, *Constraint) string
	AddUnique(string, *Unique) string
	AddCheck(string, *Check) string
	AddFK(string, *ForeignKey) string
	AddPK(string, *PrimaryKey) string
	CreateTable(*Table) string
}
