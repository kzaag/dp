package pgsql

import "github.com/kzaag/dp/rdbms"

const (
	TypeEnum      string = "enum"
	TypeComposite string = "composite"
)

type Type struct {
	Name    string
	Type    string
	Values  []string
	Columns []rdbms.Column
}
