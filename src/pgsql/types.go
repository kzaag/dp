package pgsql

import "github.com/kzaag/database-project/src/sql"

const (
	TT_Enum      string = "enum"
	TT_Composite string = "composite"
)

type Type struct {
	Name    string
	Type    string
	Values  []string
	Columns []sql.Column
}
