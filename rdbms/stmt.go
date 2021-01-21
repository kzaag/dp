package rdbms

import "strings"

/*
	either encapsulate it or inherit from it in your implementations.

	########
	To any of the OOP geeks reading this file:
		I know i could make it interface.
		Hovewer i went with struct for following reasons:
			1. In interface ALL of the methods must be implemented.
			   hovewer here its perfectly fine for user to omit parts of the functionality. Eg. Check constraints
			   and just assign null to function pointers.
			   i could just add param to functions which would decide whether its ok to run it, but why?
			   1. It would only complicate the code,
			   2. and make it slower.
					   (its much faster to check for null address than push variable to the execution stack )
			2. sometimes i want to use those functions without having to instantiate structure.
			   lets not waste CPU cycles and mallocs on memory which we dont even use.
			3. Those functions are meant to be stateless (at least for now) that means :this: pointer would be wasted space anyways.
				Again. Its more code to mantain, and more cpu cycles.
	#########
*/
type StmtCtx struct {
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
	DropTable      func(*Table) string
}

/*
	Common SQL statmement definitons, so other DBMSs dont copy-paste same functions
*/

func StmtConstraintColumn(c ConstraintColumn) string {
	ret := c.Name
	if c.Is_descending {
		ret += " DESC"
	}
	return ret
}

func StmtAddConstraint(tableName string, c *Constraint, cType string) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " " + strings.ToUpper(cType) + " ("
	for z := 0; z < len(c.Columns); z++ {
		ret += StmtConstraintColumn(c.Columns[z]) + ","
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += ");\n"
	return ret
}

func StmtAddUnique(tableName string, u *Unique) string {
	return StmtAddConstraint(tableName, &u.Constraint, "unique")
}

func StmtAddPK(tableName string, pk *PrimaryKey) string {
	return StmtAddConstraint(tableName, &pk.Constraint, "primary key")
}

func StmtAddFk(tableName string, fk *ForeignKey) string {
	var ret string
	ret += "ALTER TABLE " + tableName + " ADD CONSTRAINT " + fk.Name + " FOREIGN KEY ("
	for z := 0; z < len(fk.Columns); z++ {
		ret += StmtConstraintColumn(fk.Columns[z])
	}
	ret = strings.TrimSuffix(ret, ",")
	ret += " ) REFERENCES " + fk.Ref_table + " ( "

	for z := 0; z < len(fk.Ref_columns); z++ {
		ret += StmtConstraintColumn(fk.Ref_columns[z])
	}
	ret = strings.TrimSuffix(ret, ",") + ")"

	if fk.OnDelete != "" && fk.OnDelete != "NO ACTION" {
		ret += " ON DELETE " + strings.ToUpper(fk.OnDelete)
	}

	if fk.OnUpdate != "" && fk.OnUpdate != "NO ACTION" {
		ret += " ON UPDATE " + strings.ToUpper(fk.OnUpdate)
	}

	ret += ";\n"
	return ret
}

func StmtAddCheck(tableName string, c *Check) string {
	return "ALTER TABLE " + tableName + " ADD CONSTRAINT " + c.Name + " CHECK (" + c.Def + ");\n"
}

func StmtDropConstraint(tableName string, c *Constraint) string {
	return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + c.Name + ";\n"
}

func StmtDropTable(table *Table) string {
	return "DROP TABLE " + table.Name + ";\n"
}

func StmtColumnNameAndType(column *Column) string {
	var cs string
	cs += column.Name + " " + column.FullType
	return cs
}
