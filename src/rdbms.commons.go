package main

import "strings"

func SqlCColumn(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func SqlAddIx(tableName string, ix *Index) string {
	unique := ""
	if ix.Is_unique {
		unique = "UNIQUE "
	}

	t := "NONCLUSTERED "
	if ix.Type != "" {
		t = strings.ToUpper(ix.Type) + " "
	}

	ret :=
		"CREATE " + unique +
			t + "INDEX " +
			ix.Name + " ON " + tableName + " ("

	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if c.Is_Included_column {
			continue
		}
		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	ret += ")"

	var includes bool = false
	for i := 0; i < len(ix.Columns); i++ {
		c := ix.Columns[i]
		if !c.Is_Included_column {
			continue
		}

		if !includes {
			ret += " INCLUDE ("
		}
		includes = true

		ret += c.Name
		if c.Is_descending {
			ret += " DESC"
		}
	}

	if includes {
		ret += ")"
	}

	ret += ";\n"

	return ret
}

func SqlDropConstraint(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
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

func SqlAddUnique(tableName string, c *Unique) string {
	return SqlAddConstraint(tableName, &c.Constraint, "unique")
}

func SqlAddPk(tableName string, pk *PrimaryKey) string {
	return SqlAddConstraint(tableName, &pk.Constraint, "primary key")
}

func SqlAddFk(tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += SqlCColumn(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += SqlCColumn(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")

	ret += " );\n"
	return ret
}

func SqlAddCheck(tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func SqlDropCs(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}

func SqlDropColumn(tableName string, c *Column) string {
	return "ALTER TABLE " + tableName + " DROP COLUMN " + c.Name + ";\n"
}

func SqlDropIx(tableName string, c *Index) string {
	return "DROP INDEX " + c.Name + " ON " + tableName + ";\n"
}
