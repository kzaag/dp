package pgsql

import "github.com/kzaag/dp/rdbms"

const (
	TT_Enum      string = "enum"
	TT_Composite string = "composite"
)

type Type struct {
	Name    string
	Type    string
	Values  []string
	Columns []rdbms.Column
}
