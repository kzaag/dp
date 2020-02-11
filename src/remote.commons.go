package main

import "strings"

func SqlCColumn(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func SqlAddConstraint(tableName string, c *Constraint, cType string) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += SqlCColumn(c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret
}
