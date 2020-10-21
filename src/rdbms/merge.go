package rdbms

import (
	"container/list"
	"strings"
)

/*
	generating differential script based on local vs rem schema
*/

// func (m *SqlMergeCtx) SetLocalSchema(t []Table, tp []Type) {
// 	m.localTables = t
// 	m.localTypes = tp
// }

// func (m *SqlMergeCtx) SetRemoteSchema(t []Table, tp []Type) {
// 	m.remTables = t
// 	m.remTypes = tp
// }

const (
	DC_TYPE_PK = iota
	DC_TYPE_FK = iota
	DC_TYPE_UQ = iota
	DC_TYPE_CH = iota
	DC_TYPE_C  = iota
)

/*
	Golang, why dont you implement unions, Whyyyyyy...!
*/
type SqlMergeDropBuff struct {
	Table   string
	Type    uint
	Foreign *ForeignKey
	Primary *PrimaryKey
	Unique  *Unique
	Check   *Check
	Column  *Column
}

type RdbmsScriptStore struct {
	create *string
	drop   *string
}

type RdbmsTableStore struct {
	remoteTables []Table
	localTables  []Table
}

func SqlMergeNewDropFk(tableName string, fk *ForeignKey) SqlMergeDropBuff {
	return SqlMergeDropBuff{tableName, DC_TYPE_FK, fk, nil, nil, nil, nil}
}

func SqlMergeNewDropPk(tableName string, pk *PrimaryKey) SqlMergeDropBuff {
	return SqlMergeDropBuff{tableName, DC_TYPE_PK, nil, pk, nil, nil, nil}
}

func SqlMergeNewDropUq(tablename string, u *Unique) SqlMergeDropBuff {
	return SqlMergeDropBuff{tablename, DC_TYPE_UQ, nil, nil, u, nil, nil}
}

// func MergeNewDropCh(tablename string, c *Check) MergeDropBuff {
// 	return MergeDropBuff{tablename, DC_TYPE_CH, nil, nil, nil, c, nil}
// }

func SqlMergeNewDropC(tablename string, c *Column) SqlMergeDropBuff {
	return SqlMergeDropBuff{tablename, DC_TYPE_C, nil, nil, nil, nil, c}
}

func (d SqlMergeDropBuff) IsPk() bool {
	return d.Type == DC_TYPE_PK
}

func (d SqlMergeDropBuff) IsFk() bool {
	return d.Type == DC_TYPE_FK
}

func (d SqlMergeDropBuff) IsUq() bool {
	return d.Type == DC_TYPE_UQ
}

func (d SqlMergeDropBuff) IsCh() bool {
	return d.Type == DC_TYPE_CH
}

func (d SqlMergeDropBuff) IsC() bool {
	return d.Type == DC_TYPE_C
}

func SqlMergeAddOperation(dest *string, elem string) {
	if !strings.Contains(*dest, elem) {
		*dest += elem
	}
}

func SqlMergeCompareIColumn(c1 []IndexColumn, c2 []IndexColumn) bool {

	if c1 == nil || c2 == nil {
		return false
	}

	if len(c1) != len(c2) {
		return true
	}

	for i := 0; i < len(c1); i++ {
		var found bool = false
		for j := 0; j < len(c2); j++ {
			x := c1[i]
			y := c2[j]
			if x.Name == y.Name && x.Is_descending == y.Is_descending && x.Is_Included_column == y.Is_Included_column {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func SqlMergeCompareCColumn(c1 []ConstraintColumn, c2 []ConstraintColumn) bool {

	if c1 == nil || c2 == nil {
		return false
	}

	if len(c1) != len(c2) {
		return false
	}

	for i := 0; i < len(c1); i++ {
		var found bool = false
		for j := 0; j < len(c2); j++ {
			x := c1[i]
			y := c2[j]
			if x.Name == y.Name && x.Is_descending == y.Is_descending {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func SqlMergeCompareColumn(m IRdbmsDef, c1 *Column, c2 *Column) bool {
	sc1 := m.AddColumn("", c1)
	sc2 := m.AddColumn("", c2)
	return sc1 == sc2
}

func SqlMergeColumns(
	rem IRdbmsDef,
	localTable *Table,
	remTable *Table,
	ts *RdbmsTableStore,
	ss *RdbmsScriptStore) {

	if remTable == nil {
		return
	}

	if localTable.Type != "" || localTable.Type != remTable.Type {
		return
	}

	matchedIxs := list.New()
	for i := 0; i < len(localTable.Columns); i++ {

		uc := &localTable.Columns[i]
		var index int = -1
		if remTable.Columns != nil {
			for j := 0; j < len(remTable.Columns); j++ {
				if remTable.Columns[j].Name == uc.Name {
					index = j
				}
			}
		}

		if index < 0 {

			SqlMergeAddOperation(ss.create, rem.AddColumn(localTable.Name, uc))

		} else {
			dc := &remTable.Columns[index]
			matchedIxs.PushBack(index)

			if SqlMergeCompareColumn(rem, uc, dc) {
				continue
			}

			drefs := SqlMergeDropColRefs(rem, dc, remTable, ts.remoteTables, ss.drop)

			if dc.Identity != uc.Identity {

				SqlMergeAddOperation(ss.drop, rem.DropColumn(localTable.Name, dc))
				SqlMergeAddOperation(ss.create, rem.AddColumn(localTable.Name, uc))

			} else {

				SqlMergeAddOperation(ss.create, rem.AlterColumn(localTable.Name, dc, uc))

			}

			SqlMergeRecreateColRefs(rem, ts.localTables, ss.create, drefs)
		}
	}

	for i := 0; i < len(remTable.Columns); i++ {
		exists := false
		for v := matchedIxs.Front(); v != nil; v = v.Next() {
			if v.Value.(int) == i {
				exists = true
				break
			}
		}
		if exists {
			continue
		}

		dc := &remTable.Columns[i]

		drefs := SqlMergeDropColRefs(rem, dc, remTable, ts.remoteTables, ss.drop)

		SqlMergeAddOperation(ss.drop, rem.DropColumn(localTable.Name, dc))

		SqlMergeRecreateColRefs(rem, ts.localTables, ss.create, drefs)
	}
}

func SqlMergeIx(
	mrg IRdbmsDef,
	localTable *Table, remTable *Table,
	ss *RdbmsScriptStore) {

	matchedIxs := list.New()
	if localTable.Indexes != nil && len(localTable.Indexes) > 0 {
		for i := 0; i < len(localTable.Indexes); i++ {
			userUq := &localTable.Indexes[i]
			var index int = -1
			if remTable != nil && remTable.Indexes != nil {
				for j := 0; j < len(remTable.Indexes); j++ {
					if remTable.Indexes[j].Name == userUq.Name {
						index = j
					}
				}
			}

			if index < 0 {

				SqlMergeAddOperation(ss.create, mrg.AddIndex(localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if !SqlMergeCompareIColumn(userUq.Columns, remTable.Indexes[index].Columns) {

					SqlMergeAddOperation(ss.drop, mrg.DropIndex(localTable.Name, &remTable.Indexes[index]))
					SqlMergeAddOperation(ss.create, mrg.AddIndex(localTable.Name, userUq))

				}
			}
		}
	}

	if remTable == nil {
		return
	}

	for i := 0; i < len(remTable.Indexes); i++ {
		exists := false
		for v := matchedIxs.Front(); v != nil; v = v.Next() {
			if v.Value.(int) == i {
				exists = true
				break
			}
		}

		if exists {
			continue
		}

		SqlMergeAddOperation(ss.drop, mrg.DropIndex(localTable.Name, &remTable.Indexes[i]))
	}
}

func SqlMergeUnique(
	mrg IRdbmsDef,
	localTable *Table, remTable *Table,
	ss *RdbmsScriptStore) {

	matchedIxs := list.New()
	if localTable.Unique != nil && len(localTable.Unique) > 0 {
		for i := 0; i < len(localTable.Unique); i++ {
			userUq := &localTable.Unique[i]
			var index int = -1
			if remTable != nil && remTable.Unique != nil {
				for j := 0; j < len(remTable.Unique); j++ {
					if remTable.Unique[j].Name == userUq.Name {
						index = j
					}
				}
			}

			if index < 0 {

				SqlMergeAddOperation(ss.create, mrg.AddUnique(localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if !SqlMergeCompareCColumn(userUq.Columns, remTable.Unique[index].Columns) {

					SqlMergeAddOperation(
						ss.drop,
						mrg.DropConstraint(localTable.Name, &remTable.Unique[index].Constraint))
					SqlMergeAddOperation(
						ss.create, mrg.AddUnique(localTable.Name, userUq))
				}
			}
		}
	}

	if remTable == nil {
		return
	}

	for i := 0; i < len(remTable.Unique); i++ {
		exists := false
		for v := matchedIxs.Front(); v != nil; v = v.Next() {
			if v.Value.(int) == i {
				exists = true
				break
			}
		}
		if exists {
			continue
		}

		SqlMergeAddOperation(ss.drop, mrg.DropConstraint(localTable.Name, &remTable.Unique[i].Constraint))
	}
}

func SqlMergeCheck(
	mrg IRdbmsDef,
	localTable *Table, remTable *Table,
	ss *RdbmsScriptStore) {

	matchedIxs := list.New()
	if localTable.Check != nil && len(localTable.Check) > 0 {
		for i := 0; i < len(localTable.Check); i++ {
			userC := &localTable.Check[i]
			var index int = -1
			if remTable != nil && remTable.Check != nil {
				for j := 0; j < len(remTable.Check); j++ {
					if remTable.Check[j].Name == userC.Name {
						index = j
					}
				}
			}

			if index < 0 {

				SqlMergeAddOperation(ss.create, mrg.AddCheck(localTable.Name, userC))

			} else {
				matchedIxs.PushBack(index)
				if userC.Def != remTable.Check[i].Def {

					SqlMergeAddOperation(
						ss.drop,
						mrg.DropConstraint(localTable.Name, &Constraint{remTable.Check[index].Name, nil}))
					SqlMergeAddOperation(
						ss.create, mrg.AddCheck(localTable.Name, userC))
				}
			}
		}
	}

	if remTable == nil {
		return
	}

	for i := 0; i < len(remTable.Check); i++ {
		exists := false
		for v := matchedIxs.Front(); v != nil; v = v.Next() {
			if v.Value.(int) == i {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		c := &Constraint{remTable.Check[i].Name, nil}
		SqlMergeAddOperation(ss.drop, mrg.DropConstraint(localTable.Name, c))
	}
}

func MergeFK(
	mrg IRdbmsDef,
	localTable *Table, remTable *Table,
	ss *RdbmsScriptStore) {

	userFKs := localTable.Foreign
	matchedIxs := list.New()

	if len(userFKs) > 0 {
		for i := 0; i < len(userFKs); i++ {
			userFK := &userFKs[i]
			var index int = -1
			if remTable != nil && remTable.Foreign != nil {
				for j := 0; j < len(remTable.Foreign); j++ {
					if remTable.Foreign[j].Name == userFK.Name {
						index = j
					}
				}
			}

			if index < 0 {

				SqlMergeAddOperation(ss.create, mrg.AddFK(localTable.Name, userFK))

			} else {
				matchedIxs.PushBack(index)

				ceq := SqlMergeCompareCColumn(userFK.Columns, remTable.Foreign[index].Columns)
				rceq := SqlMergeCompareCColumn(userFK.Ref_columns, remTable.Foreign[index].Ref_columns)

				if !ceq || !rceq {
					SqlMergeAddOperation(
						ss.drop,
						mrg.DropConstraint(localTable.Name, &remTable.Foreign[index].Constraint))
					SqlMergeAddOperation(ss.create, mrg.AddFK(localTable.Name, userFK))
				}
			}
		}
	}

	if remTable == nil {
		return
	}

	for i := 0; i < len(remTable.Foreign); i++ {
		exists := false
		for v := matchedIxs.Front(); v != nil; v = v.Next() {
			if v.Value.(int) == i {
				exists = true
				break
			}
		}

		if exists {
			continue
		}

		SqlMergeAddOperation(
			ss.drop,
			mrg.DropConstraint(localTable.Name, &remTable.Foreign[i].Constraint))
	}
}

func SqlMergeDropColRefs(
	m IRdbmsDef,
	col *Column,
	table *Table,
	tables []Table,
	drop *string) []SqlMergeDropBuff {

	list := list.New()
	if table.Primary != nil && table.Primary.Columns != nil {
		for i := 0; i < len(table.Primary.Columns); i++ {
			c := table.Primary.Columns[i]
			if c.Name == col.Name {
				bf := SqlMergeDropPkRefs(m, table.Name, tables, drop)
				for i := 0; i < len(bf); i++ {
					list.PushBack(bf[i])
				}
				SqlMergeAddOperation(
					drop, m.DropConstraint(table.Name, &table.Primary.Constraint))
				list.PushBack(SqlMergeNewDropPk(table.Name, table.Primary))
			}
		}
	}

	if table.Unique != nil {
		for i := 0; i < len(table.Unique); i++ {
			c := table.Unique[i]
			if c.Columns == nil {
				continue
			}
			for j := 0; j < len(c.Columns); j++ {
				cc := c.Columns[j]
				if cc.Name == col.Name {
					SqlMergeAddOperation(drop, m.DropConstraint(table.Name, &c.Constraint))
					list.PushBack(SqlMergeNewDropUq(table.Name, &c))
				}
			}
		}
	}

	ret := make([]SqlMergeDropBuff, list.Len())
	for i, x := 0, list.Front(); x != nil; i, x = i+1, x.Next() {
		ret[i] = x.Value.(SqlMergeDropBuff)
	}

	return ret
}

func SqlMergeRecreateDropBuff(
	m IRdbmsDef,
	dc []SqlMergeDropBuff,
	ss *RdbmsScriptStore,
	ts *RdbmsTableStore) {

	if dc == nil {
		return
	}

	for i := len(dc) - 1; i >= 0; i-- {
		dropBuff := dc[i]
		for j := 0; j < len(ts.localTables); j++ {

			table := &ts.localTables[j]

			if table.Name != dropBuff.Table {
				continue
			}

			switch dropBuff.Type {
			case DC_TYPE_FK:
				if table.Foreign == nil {
					continue
				}
				for z := 0; z < len(table.Foreign); z++ {
					fk := &table.Foreign[z]
					if fk.Name == dropBuff.Foreign.Name {
						SqlMergeAddOperation(ss.create, m.AddFK(table.Name, fk))
					}
				}
			case DC_TYPE_PK:
				if table.Primary == nil {
					continue
				}
				pk := table.Primary
				if pk.Name == dropBuff.Primary.Name {
					SqlMergeAddOperation(ss.create, m.AddPK(table.Name, pk))
				}
			case DC_TYPE_UQ:
				if table.Unique == nil {
					continue
				}
				u := table.Unique
				for z := 0; z < len(u); z++ {
					uu := &u[z]
					if uu.Name == dropBuff.Unique.Name {
						SqlMergeAddOperation(ss.create, m.AddUnique(table.Name, uu))
					}
				}
			case DC_TYPE_C:
				for z := 0; z < len(table.Columns); z++ {
					c := &table.Columns[z]
					if c.Name == dropBuff.Column.Name {
						SqlMergeAddOperation(ss.create, m.AddColumn(table.Name, c))
					}
				}
			}
		}
	}
}

// Obsolete : this method will be removed
func SqlMergeRecreateColRefs(
	m IRdbmsDef,
	tables []Table,
	create *string,
	dc []SqlMergeDropBuff) {

	if dc == nil {
		return
	}

	for i := len(dc) - 1; i >= 0; i-- {
		dropBuff := dc[i]
		for j := 0; j < len(tables); j++ {
			table := tables[j]
			if table.Name != dropBuff.Table {
				continue
			}
			if dropBuff.IsFk() {
				if table.Foreign != nil {
					for z := 0; z < len(table.Foreign); z++ {
						fk := table.Foreign[z]
						if fk.Name == dropBuff.Foreign.Name {
							SqlMergeAddOperation(create, m.AddFK(table.Name, &fk))
						}
					}
				}
				continue
			}

			if dropBuff.IsPk() {
				if table.Primary != nil {
					pk := table.Primary
					if pk.Name == dropBuff.Primary.Name {
						SqlMergeAddOperation(create, m.AddPK(table.Name, pk))
					}
				}
				continue
			}

			if dropBuff.IsUq() {
				if table.Unique != nil {
					u := table.Unique
					for z := 0; z < len(u); z++ {
						uu := u[z]
						if uu.Name == dropBuff.Unique.Name {
							SqlMergeAddOperation(create, m.AddUnique(table.Name, &uu))
						}
					}
				}
			}
		}
	}
}

func SqlMergeDropPkRefs(
	m IRdbmsDef,
	tableName string,
	tables []Table,
	drop *string) []SqlMergeDropBuff {

	var c int = 0
	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				c++
			}
		}
	}
	ret := make([]SqlMergeDropBuff, c)
	lastix := 0

	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				SqlMergeAddOperation(drop, m.DropConstraint(tables[i].Name, &fks[j].Constraint))
				ret[lastix] = SqlMergeNewDropFk(tables[i].Name, &fks[j])
				lastix++
			}
		}
	}

	return ret
}

func SqlMergeRecreatePkRefs(
	m IRdbmsDef,
	tables []Table,
	create *string,
	dc []SqlMergeDropBuff) {

	if dc == nil {
		return
	}

	for i := 0; i < len(dc); i++ {
		droppedC := dc[i]
		for j := 0; j < len(tables); j++ {
			if tables[j].Name == droppedC.Table {
				for z := 0; z < len(tables[j].Foreign); z++ {
					fk := tables[j].Foreign[z]
					if fk.Name == droppedC.Foreign.Name {
						SqlMergeAddOperation(create, m.AddFK(tables[j].Name, &fk))
					}
				}
			}
		}
	}
}

func SqlMergePrimary(
	mrg IRdbmsDef,
	localTable *Table, remTable *Table,
	ss *RdbmsScriptStore,
	ts *RdbmsTableStore) {

	userPK := localTable.Primary

	if userPK == nil && (remTable == nil || remTable.Primary == nil) {
		return
	}

	tname := localTable.Name

	if userPK != nil && (remTable == nil || remTable.Primary == nil) {
		SqlMergeAddOperation(ss.create, mrg.AddPK(tname, userPK))
		return
	}

	var droppedCs []SqlMergeDropBuff

	if userPK == nil && (remTable != nil && remTable.Primary != nil) {
		droppedCs = SqlMergeDropPkRefs(mrg, tname, ts.remoteTables, ss.drop)

		SqlMergeAddOperation(
			ss.drop, mrg.DropConstraint(tname, &remTable.Primary.Constraint))
		SqlMergeRecreatePkRefs(mrg, ts.localTables, ss.create, droppedCs)

		return
	}

	eq := SqlMergeCompareCColumn(userPK.Columns, remTable.Primary.Columns)

	if userPK.Name != remTable.Primary.Name || !eq {
		droppedCs = SqlMergeDropPkRefs(mrg, tname, ts.remoteTables, ss.drop)

		SqlMergeAddOperation(ss.drop, mrg.DropConstraint(tname, &remTable.Primary.Constraint))
		SqlMergeAddOperation(ss.create, mrg.AddPK(tname, userPK))

		SqlMergeRecreatePkRefs(mrg, ts.localTables, ss.create, droppedCs)
	}
}

func SqlMergeFindTable(name string, tables []Table) *Table {
	var index int = -1
	for j := 0; j < len(tables); j++ {
		if tables[j].Name == name {
			index = j
		}
	}
	var t *Table = nil
	if index >= 0 {
		t = &tables[index]
	}
	return t
}
