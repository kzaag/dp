package sql

import "strings"

func ConstraintColumnStr(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func AddConstraintStr(tableName string, c *Constraint, cType string) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += ConstraintColumnStr(c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret
}

func AddUniqueStr(tableName string, u *Unique) string {
	return AddConstraintStr(tableName, &u.Constraint, "unique")
}

func AddPkStr(tableName string, pk *PrimaryKey) string {
	return AddConstraintStr(tableName, &pk.Constraint, "primary key")
}

func AddFk(tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += ConstraintColumnStr(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += ConstraintColumnStr(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += " );\n"
	return ret
}

func AddCheckStr(tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func DropConstraintStr(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}
