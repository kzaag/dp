package cql

import (
	"fmt"
	"strings"
)

type StmtCtx struct {
	AddColumn   func(string, *Column) string
	AlterColumn func(string, *Column, *Column) string
	DropColumn  func(string, *Column) string
	//AddIndex               func(string, *Index) string
	//DropIndex              func(string, *Index) string
	CreateTable            func(*Table) string
	DropTable              func(*Table) string
	CreateMaterializedView func(*MaterializedView) string
	DropMaterializedView   func(*MaterializedView) string
}

func StmtAddColumn(
	tableName string, column *Column,
) string {
	return fmt.Sprintf(
		"alter %s add %s %s;\n",
		tableName,
		column.Name,
		column.Type)
}

func StmtAlterColumn(
	tablename string, c1, c2 *Column,
) string {
	ret := ""
	if c1.Type != c2.Type {
		ret += fmt.Sprintf(
			"alter table %s alter %s type %s",
			tablename,
			c1.Name,
			c2.Type,
		)
		ret += ";\n"
	}
	return ret
}

func StmtDropColumn(tablename string, c1 *Column) string {
	return fmt.Sprintf(
		"alter table %s drop column %s",
		tablename,
		c1.Name,
	)
}

func StmtPKDef(pk *PrimaryKey) string {
	s := "primary key(("
	for i := 0; i < len(pk.PartitionColumns); i++ {
		s += pk.PartitionColumns[i].Name + ","
	}
	s = strings.TrimSuffix(s, ",")
	s += ")"
	if len(pk.ClusteringColumns) > 0 {
		s += ","
		for i := 0; i < len(pk.ClusteringColumns); i++ {
			s += pk.ClusteringColumns[i].Name + ","
		}
		s = strings.TrimSuffix(s, ",")
	}
	s += ")"
	return s
}

func StmtCreateTable(t *Table) string {
	s := "create table " + t.Name + " ( \n)"
	for k := range t.Columns {
		s += fmt.Sprintf("\t%s %s,\n", k, t.Columns[k].Type)
	}
	s += StmtPKDef(t.PrimaryKey)
	s += "\n)"
	addedTag := false
	for i := 0; i < len(t.PrimaryKey.ClusteringColumns); i++ {
		cc := &t.PrimaryKey.ClusteringColumns[i]
		if cc.Order != "none" {
			if !addedTag {
				addedTag = true
				s += " with clustering order by ("
			}
			s += fmt.Sprintf("%s %s, ",
				cc.Name, cc.Order)
		}
	}
	if addedTag {
		s = strings.TrimSuffix(s, ",")
		s += ")"
	}
	s += ";\n"
	return s
}

func StmtDropTable(t *Table) string {
	return "drop table " + t.Name + ";\n"
}

func StmtCreateMaterializedView(m *MaterializedView) string {
	s := "create materialized view " + m.Name + " as\n"
	if len(m.Columns) == 0 {
		s += "select * from " + m.Base + "\n"
	} else {
		s += "select "
		for k := range m.Columns {
			s += fmt.Sprintf("\t%s,\n", k)
		}
		s = strings.TrimSuffix(s, ",\n")
		s += "from " + m.Base + "\n"
	}
	s += " where " + m.WhereClause
	s += StmtPKDef(m.PrimaryKey)
	s += ";\n"
	return s
}

func StmtDropMaterializedView(m *MaterializedView) string {
	return "drop materialized view " + m.Name + ";\n"
}
