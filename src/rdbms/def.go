package rdbms

import "strings"

func DefConstraintColumn(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func DefAddConstraint(tableName string, c *Constraint, cType string) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += DefConstraintColumn(c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret
}

func DefAddUnique(tableName string, u *Unique) string {
	return DefAddConstraint(tableName, &u.Constraint, "unique")
}

func DefAddPK(tableName string, pk *PrimaryKey) string {
	return DefAddConstraint(tableName, &pk.Constraint, "primary key")
}

func DefAddFk(tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += DefConstraintColumn(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += DefConstraintColumn(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += " );\n"
	return ret
}

func DefAddCheck(tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func DefDropConstraint(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}

func DefDropTable(tableName string) string {
	return "DROP TABLE " + tableName + ";\n"
}

func DefColumnNameAndType(column *Column) string {
	var cs string
	cs += column.Name + " " + column.FullType
	return cs
}
