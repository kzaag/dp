package rdbms

import (
	"container/list"
	"fmt"
	"strings"
)

/*
	generating differential script based on local vs rem schema
*/

const (
	DC_TYPE_PK = iota
	DC_TYPE_FK = iota
	DC_TYPE_UQ = iota
	DC_TYPE_CH = iota
	DC_TYPE_C  = iota
)

/*
	Golang, why dont you implement unions
*/
type MergeDropCtx struct {
	Table   string
	Type    uint
	Foreign *ForeignKey
	Primary *PrimaryKey
	Unique  *Unique
	Check   *Check
	Column  *Column
}

type MergeScriptCtx struct {
	Create *string
	Drop   *string
}

type MergeTableCtx struct {
	RemoteTables []Table
	LocalTables  []Table
}

func MergeDropNewFK(tableName string, fk *ForeignKey) MergeDropCtx {
	return MergeDropCtx{tableName, DC_TYPE_FK, fk, nil, nil, nil, nil}
}

func MergeDropNewPK(tableName string, pk *PrimaryKey) MergeDropCtx {
	return MergeDropCtx{tableName, DC_TYPE_PK, nil, pk, nil, nil, nil}
}

func MergeDropNewUnique(tablename string, u *Unique) MergeDropCtx {
	return MergeDropCtx{tablename, DC_TYPE_UQ, nil, nil, u, nil, nil}
}

// func MergeNewDropCh(tablename string, c *Check) MergeDropBuff {
// 	return MergeDropBuff{tablename, DC_TYPE_CH, nil, nil, nil, c, nil}
// }

func MergeDropNewC(tablename string, c *Column) MergeDropCtx {
	return MergeDropCtx{tablename, DC_TYPE_C, nil, nil, nil, nil, c}
}

func (d MergeDropCtx) IsPk() bool {
	return d.Type == DC_TYPE_PK
}

func (d MergeDropCtx) IsFk() bool {
	return d.Type == DC_TYPE_FK
}

func (d MergeDropCtx) IsUq() bool {
	return d.Type == DC_TYPE_UQ
}

func (d MergeDropCtx) IsCh() bool {
	return d.Type == DC_TYPE_CH
}

func (d MergeDropCtx) IsC() bool {
	return d.Type == DC_TYPE_C
}

func MergeAddOperation(dest *string, elem string) {
	if !strings.Contains(*dest, elem) {
		*dest += elem
	}
}

func MergeCompareCColumn(c1 []ConstraintColumn, c2 []ConstraintColumn) bool {

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

func MergeCompareColumn(m *StmtCtx, c1 *Column, c2 *Column) bool {
	sc1 := m.AddColumn("", c1)
	sc2 := m.AddColumn("", c2)
	return sc1 == sc2
}

func MergeColumns(
	rem *StmtCtx,
	localTable *Table,
	remTable *Table,
	ts *MergeTableCtx,
	ss *MergeScriptCtx) {

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

			MergeAddOperation(ss.Create, rem.AddColumn(localTable.Name, uc))

		} else {
			dc := &remTable.Columns[index]
			matchedIxs.PushBack(index)

			if MergeCompareColumn(rem, uc, dc) {
				continue
			}

			drefs := MergeDropColRefs(rem, dc, remTable, ts.RemoteTables, ss.Drop)

			if dc.Identity != uc.Identity {

				MergeAddOperation(ss.Drop, rem.DropColumn(localTable.Name, dc))
				MergeAddOperation(ss.Create, rem.AddColumn(localTable.Name, uc))

			} else {

				MergeAddOperation(ss.Create, rem.AlterColumn(localTable.Name, dc, uc))

			}

			MergeDropRecreateColRefs(rem, ts.LocalTables, ss.Create, drefs)
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

		drefs := MergeDropColRefs(rem, dc, remTable, ts.RemoteTables, ss.Drop)

		MergeAddOperation(ss.Drop, rem.DropColumn(localTable.Name, dc))

		MergeDropRecreateColRefs(rem, ts.LocalTables, ss.Create, drefs)
	}
}

func MergeIx(
	mrg *StmtCtx,
	localTable *Table, remTable *Table,
	ss *MergeScriptCtx) {

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

				MergeAddOperation(ss.Create, mrg.AddIndex(localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if mrg.AddIndex("", userUq) != mrg.AddIndex("", &remTable.Indexes[index]) {
					fmt.Println(mrg.AddIndex("", userUq), mrg.AddIndex("", &remTable.Indexes[index]))

					MergeAddOperation(ss.Drop, mrg.DropIndex(localTable.Name, &remTable.Indexes[index]))
					MergeAddOperation(ss.Create, mrg.AddIndex(localTable.Name, userUq))

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

		MergeAddOperation(ss.Drop, mrg.DropIndex(localTable.Name, &remTable.Indexes[i]))
	}
}

func MergeUnique(
	mrg *StmtCtx,
	localTable *Table, remTable *Table,
	ss *MergeScriptCtx) {

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

				MergeAddOperation(ss.Create, mrg.AddUnique(localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if !MergeCompareCColumn(userUq.Columns, remTable.Unique[index].Columns) {

					MergeAddOperation(
						ss.Drop,
						mrg.DropConstraint(localTable.Name, &remTable.Unique[index].Constraint))
					MergeAddOperation(
						ss.Create, mrg.AddUnique(localTable.Name, userUq))
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

		MergeAddOperation(ss.Drop, mrg.DropConstraint(localTable.Name, &remTable.Unique[i].Constraint))
	}
}

func MergeCheck(
	mrg *StmtCtx,
	localTable *Table, remTable *Table,
	ss *MergeScriptCtx) {

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

				MergeAddOperation(ss.Create, mrg.AddCheck(localTable.Name, userC))

			} else {
				matchedIxs.PushBack(index)
				if userC.Def != remTable.Check[i].Def {

					MergeAddOperation(
						ss.Drop,
						mrg.DropConstraint(localTable.Name, &Constraint{remTable.Check[index].Name, nil}))
					MergeAddOperation(
						ss.Create, mrg.AddCheck(localTable.Name, userC))
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
		MergeAddOperation(ss.Drop, mrg.DropConstraint(localTable.Name, c))
	}
}

func MergeFK(
	mrg *StmtCtx,
	localTable *Table, remTable *Table,
	ss *MergeScriptCtx) {

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

				MergeAddOperation(ss.Create, mrg.AddFK(localTable.Name, userFK))

			} else {
				matchedIxs.PushBack(index)

				// ceq := MergeCompareCColumn(userFK.Columns, remTable.Foreign[index].Columns)
				// rceq := MergeCompareCColumn(userFK.Ref_columns, remTable.Foreign[index].Ref_columns)

				if mrg.AddFK("", userFK) != mrg.AddFK("", &remTable.Foreign[index]) {
					MergeAddOperation(
						ss.Drop,
						mrg.DropConstraint(localTable.Name, &remTable.Foreign[index].Constraint))
					MergeAddOperation(ss.Create, mrg.AddFK(localTable.Name, userFK))
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

		MergeAddOperation(
			ss.Drop,
			mrg.DropConstraint(localTable.Name, &remTable.Foreign[i].Constraint))
	}
}

func MergeDropColRefs(
	m *StmtCtx,
	col *Column,
	table *Table,
	tables []Table,
	drop *string) []MergeDropCtx {

	list := list.New()
	if table.Primary != nil && table.Primary.Columns != nil {
		for i := 0; i < len(table.Primary.Columns); i++ {
			c := table.Primary.Columns[i]
			if c.Name == col.Name {
				bf := MergeDropPkRefs(m, table.Name, tables, drop)
				for i := 0; i < len(bf); i++ {
					list.PushBack(bf[i])
				}
				MergeAddOperation(
					drop, m.DropConstraint(table.Name, &table.Primary.Constraint))
				list.PushBack(MergeDropNewPK(table.Name, table.Primary))
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
					MergeAddOperation(drop, m.DropConstraint(table.Name, &c.Constraint))
					list.PushBack(MergeDropNewUnique(table.Name, &c))
				}
			}
		}
	}

	ret := make([]MergeDropCtx, list.Len())
	for i, x := 0, list.Front(); x != nil; i, x = i+1, x.Next() {
		ret[i] = x.Value.(MergeDropCtx)
	}

	return ret
}

func MergeDropRecreate(
	m *StmtCtx,
	dc []MergeDropCtx,
	ss *MergeScriptCtx,
	ts *MergeTableCtx) {

	if dc == nil {
		return
	}

	for i := len(dc) - 1; i >= 0; i-- {
		dropBuff := dc[i]
		for j := 0; j < len(ts.LocalTables); j++ {

			table := &ts.LocalTables[j]

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
						MergeAddOperation(ss.Create, m.AddFK(table.Name, fk))
					}
				}
			case DC_TYPE_PK:
				if table.Primary == nil {
					continue
				}
				pk := table.Primary
				if pk.Name == dropBuff.Primary.Name {
					MergeAddOperation(ss.Create, m.AddPK(table.Name, pk))
				}
			case DC_TYPE_UQ:
				if table.Unique == nil {
					continue
				}
				u := table.Unique
				for z := 0; z < len(u); z++ {
					uu := &u[z]
					if uu.Name == dropBuff.Unique.Name {
						MergeAddOperation(ss.Create, m.AddUnique(table.Name, uu))
					}
				}
			case DC_TYPE_C:
				for z := 0; z < len(table.Columns); z++ {
					c := &table.Columns[z]
					if c.Name == dropBuff.Column.Name {
						MergeAddOperation(ss.Create, m.AddColumn(table.Name, c))
					}
				}
			}
		}
	}
}

// Obsolete : this method will be removed
func MergeDropRecreateColRefs(
	m *StmtCtx,
	tables []Table,
	create *string,
	dc []MergeDropCtx) {

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
							MergeAddOperation(create, m.AddFK(table.Name, &fk))
						}
					}
				}
				continue
			}

			if dropBuff.IsPk() {
				if table.Primary != nil {
					pk := table.Primary
					if pk.Name == dropBuff.Primary.Name {
						MergeAddOperation(create, m.AddPK(table.Name, pk))
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
							MergeAddOperation(create, m.AddUnique(table.Name, &uu))
						}
					}
				}
			}
		}
	}
}

func MergeDropPkRefs(
	m *StmtCtx,
	tableName string,
	tables []Table,
	drop *string) []MergeDropCtx {

	var c int = 0
	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				c++
			}
		}
	}
	ret := make([]MergeDropCtx, c)
	lastix := 0

	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				MergeAddOperation(drop, m.DropConstraint(tables[i].Name, &fks[j].Constraint))
				ret[lastix] = MergeDropNewFK(tables[i].Name, &fks[j])
				lastix++
			}
		}
	}

	return ret
}

func MergeDropRecreatePkRefs(
	m *StmtCtx,
	tables []Table,
	create *string,
	dc []MergeDropCtx) {

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
						MergeAddOperation(create, m.AddFK(tables[j].Name, &fk))
					}
				}
			}
		}
	}
}

func MergePrimary(
	mrg *StmtCtx,
	localTable *Table, remTable *Table,
	ss *MergeScriptCtx,
	ts *MergeTableCtx) {

	userPK := localTable.Primary

	if userPK == nil && (remTable == nil || remTable.Primary == nil) {
		return
	}

	tname := localTable.Name

	if userPK != nil && (remTable == nil || remTable.Primary == nil) {
		MergeAddOperation(ss.Create, mrg.AddPK(tname, userPK))
		return
	}

	var droppedCs []MergeDropCtx

	if userPK == nil && (remTable != nil && remTable.Primary != nil) {
		droppedCs = MergeDropPkRefs(mrg, tname, ts.RemoteTables, ss.Drop)

		MergeAddOperation(
			ss.Drop, mrg.DropConstraint(tname, &remTable.Primary.Constraint))
		MergeDropRecreatePkRefs(mrg, ts.LocalTables, ss.Create, droppedCs)

		return
	}

	eq := MergeCompareCColumn(userPK.Columns, remTable.Primary.Columns)

	if userPK.Name != remTable.Primary.Name || !eq {
		droppedCs = MergeDropPkRefs(mrg, tname, ts.RemoteTables, ss.Drop)

		MergeAddOperation(ss.Drop, mrg.DropConstraint(tname, &remTable.Primary.Constraint))
		MergeAddOperation(ss.Create, mrg.AddPK(tname, userPK))

		MergeDropRecreatePkRefs(mrg, ts.LocalTables, ss.Create, droppedCs)
	}
}

func MergeFindTable(name string, tables []Table) *Table {
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
