package mssql

import (
	"fmt"
	"strings"
)

// split {schema}.{name} sql name into 2 parts
func SplitSchemaName(name string) (string, string, error) {
	if name == "" || !strings.Contains(name, ".") {
		return "", "", fmt.Errorf("invalid sql name. expected: {schema}.{name} got: \"" + name + "\"")
	}
	sn := strings.Split(name, ".")
	if sn == nil || len(sn) != 2 {
		return "", "", fmt.Errorf("couldnt split sql name into schema, name array")
	}
	return sn[0], sn[1], nil
}
