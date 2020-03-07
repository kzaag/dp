package main

import (
	"container/list"
	"strings"
)

/*
generating differential script based on local vs rem schema
*/

type Merger struct {
	remTables   []Table
	localTables []Table

	remTypes   []Type
	localTypes []Type

	create *string
	drop   *string
}

func MergeNew() *Merger {
	return &Merger{}
}

func (m *Merger) SetBuffers(create *string, drop *string) {
	m.create = create
	m.drop = drop
}

func (m *Merger) SetLocalSchema(t []Table, tp []Type) {
	m.localTables = t
	m.localTypes = tp
}

func (m *Merger) SetRemoteSchema(t []Table, tp []Type) {
	m.remTables = t
	m.remTypes = tp
}

const (
	DC_TYPE_PK = iota
	DC_TYPE_FK = iota
	DC_TYPE_UQ = iota
	DC_TYPE_CH = iota
	DC_TYPE_C  = iota
)

// probably could have implemented this as one int -> pointer to void
// and then based on type unbox value pointed by it.
// but that would be potentially unsafe + i'd like to keep code as simple as possible
// problem is already pretty complex.
type MergeDropBuff struct {
	Table   string
	Type    uint
	Foreign *ForeignKey
	Primary *PrimaryKey
	Unique  *Unique
	Check   *Check
	Column  *Column
}

func MergeNewDropFk(tableName string, fk *ForeignKey) MergeDropBuff {
	return MergeDropBuff{tableName, DC_TYPE_FK, fk, nil, nil, nil, nil}
}

func MergeNewDropPk(tableName string, pk *PrimaryKey) MergeDropBuff {
	return MergeDropBuff{tableName, DC_TYPE_PK, nil, pk, nil, nil, nil}
}

func MergeNewDropUq(tablename string, u *Unique) MergeDropBuff {
	return MergeDropBuff{tablename, DC_TYPE_UQ, nil, nil, u, nil, nil}
}

// func MergeNewDropCh(tablename string, c *Check) MergeDropBuff {
// 	return MergeDropBuff{tablename, DC_TYPE_CH, nil, nil, nil, c, nil}
// }

func MergeNewDropC(tablename string, c *Column) MergeDropBuff {
	return MergeDropBuff{tablename, DC_TYPE_C, nil, nil, nil, nil, c}
}

func (d MergeDropBuff) IsPk() bool {
	return d.Type == DC_TYPE_PK
}

func (d MergeDropBuff) IsFk() bool {
	return d.Type == DC_TYPE_FK
}

func (d MergeDropBuff) IsUq() bool {
	return d.Type == DC_TYPE_UQ
}

func (d MergeDropBuff) IsCh() bool {
	return d.Type == DC_TYPE_CH
}

func (d MergeDropBuff) IsC() bool {
	return d.Type == DC_TYPE_C
}

func MergeAddOperation(dest *string, elem string) {
	if !strings.Contains(*dest, elem) {
		*dest += elem
	}
}

func MergeCompareIColumn(c1 []IndexColumn, c2 []IndexColumn) bool {

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

func MergeCompareColumn(rem *Remote, c1 *Column, c2 *Column) bool {
	sc1 := RemoteAddColumn(rem, "", c1)
	sc2 := RemoteAddColumn(rem, "", c2)

	if sc1 == sc2 && c1.Identity == c2.Identity && c1.Nullable == c2.Nullable {
		return true
	}

	//fmt.Printf("%s != %s\n", sc1, sc2)

	return false
}

func MergeColumns(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {

	if remTable == nil {
		return
	}

	matchedIxs := list.New()
	if localTable.Columns != nil {
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

				MergeAddOperation(mrg.create, RemoteAddColumn(rem, localTable.Name, uc))

			} else {
				dc := &remTable.Columns[index]
				matchedIxs.PushBack(index)

				if MergeCompareColumn(rem, uc, dc) {
					continue
				}

				drefs := MergeDropColRefs(rem, dc, remTable, mrg.remTables, mrg.drop)

				if dc.Identity != uc.Identity {

					MergeAddOperation(mrg.drop, RemoteDropColumn(rem, localTable.Name, dc))
					MergeAddOperation(mrg.create, RemoteAddColumn(rem, localTable.Name, uc))

				} else {

					MergeAddOperation(mrg.create, RemoteAlterColumn(rem, localTable.Name, dc, uc))

				}

				MergeRecreateColRefs(rem, mrg.localTables, mrg.create, drefs)
			}
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

		drefs := MergeDropColRefs(rem, dc, remTable, mrg.remTables, mrg.drop)

		MergeAddOperation(mrg.drop, RemoteDropColumn(rem, localTable.Name, dc))

		MergeRecreateColRefs(rem, mrg.localTables, mrg.create, drefs)
	}
}

func MergeIx(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {

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

				MergeAddOperation(mrg.create, RemoteAddIx(rem, localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if !MergeCompareIColumn(userUq.Columns, remTable.Indexes[index].Columns) {

					MergeAddOperation(mrg.drop, RemoteDropIx(rem, localTable.Name, &remTable.Indexes[index]))
					MergeAddOperation(mrg.create, RemoteAddIx(rem, localTable.Name, userUq))

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

		MergeAddOperation(mrg.drop, RemoteDropIx(rem, localTable.Name, &remTable.Indexes[i]))
	}
}

func MergeUnique(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {
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

				MergeAddOperation(mrg.create, RemoteAddUnique(rem, localTable.Name, userUq))

			} else {
				matchedIxs.PushBack(index)

				if !MergeCompareCColumn(userUq.Columns, remTable.Unique[index].Columns) {

					MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, &remTable.Unique[index].Constraint))
					MergeAddOperation(mrg.create, RemoteAddUnique(rem, localTable.Name, userUq))
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

		MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, &remTable.Unique[i].Constraint))
	}
}

func MergeCheck(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {

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

				MergeAddOperation(mrg.create, RemoteAddCheck(rem, localTable.Name, userC))

			} else {
				matchedIxs.PushBack(index)
				if userC.Def != remTable.Check[i].Def {

					MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, &Constraint{remTable.Check[index].Name, nil}))
					MergeAddOperation(mrg.create, RemoteAddCheck(rem, localTable.Name, userC))
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
		MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, c))
	}
}

func MergeFK(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {

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

				MergeAddOperation(mrg.create, RemoteAddFk(rem, localTable.Name, userFK))

			} else {
				matchedIxs.PushBack(index)

				ceq := MergeCompareCColumn(userFK.Columns, remTable.Foreign[index].Columns)
				rceq := MergeCompareCColumn(userFK.Ref_columns, remTable.Foreign[index].Ref_columns)

				if !ceq || !rceq {
					MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, &remTable.Foreign[index].Constraint))
					MergeAddOperation(mrg.create, RemoteAddFk(rem, localTable.Name, userFK))
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

		MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, localTable.Name, &remTable.Foreign[i].Constraint))
	}
}

func MergeDropColRefs(rem *Remote, col *Column, table *Table, tables []Table, drop *string) []MergeDropBuff {

	list := list.New()
	if table.Primary != nil && table.Primary.Columns != nil {
		for i := 0; i < len(table.Primary.Columns); i++ {
			c := table.Primary.Columns[i]
			if c.Name == col.Name {
				bf := MergeDropPkRefs(rem, table.Name, tables, drop)
				for i := 0; i < len(bf); i++ {
					list.PushBack(bf[i])
				}
				MergeAddOperation(drop, RemoteDropConstraint(rem, table.Name, &table.Primary.Constraint))
				list.PushBack(MergeNewDropPk(table.Name, table.Primary))
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
					MergeAddOperation(drop, RemoteDropConstraint(rem, table.Name, &c.Constraint))
					list.PushBack(MergeNewDropUq(table.Name, &c))
				}
			}
		}
	}

	ret := make([]MergeDropBuff, list.Len())
	for i, x := 0, list.Front(); x != nil; i, x = i+1, x.Next() {
		ret[i] = x.Value.(MergeDropBuff)
	}

	return ret
}

func MergeRecreateDropBuff(rem *Remote, m *Merger, dc []MergeDropBuff) {
	if dc == nil {
		return
	}

	for i := len(dc) - 1; i >= 0; i-- {
		dropBuff := dc[i]
		for j := 0; j < len(m.localTables); j++ {

			table := &m.localTables[j]

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
						MergeAddOperation(m.create, RemoteAddFk(rem, table.Name, fk))
					}
				}
			case DC_TYPE_PK:
				if table.Primary == nil {
					continue
				}
				pk := table.Primary
				if pk.Name == dropBuff.Primary.Name {
					MergeAddOperation(m.create, RemoteAddPk(rem, table.Name, pk))
				}
			case DC_TYPE_UQ:
				if table.Unique == nil {
					continue
				}
				u := table.Unique
				for z := 0; z < len(u); z++ {
					uu := &u[z]
					if uu.Name == dropBuff.Unique.Name {
						MergeAddOperation(m.create, RemoteAddUnique(rem, table.Name, uu))
					}
				}
			case DC_TYPE_C:
				for z := 0; z < len(table.Columns); z++ {
					c := &table.Columns[z]
					if c.Name == dropBuff.Column.Name {
						MergeAddOperation(m.create, RemoteAddColumn(rem, table.Name, c))
					}
				}
			}
		}
	}
}

// Obsolete : this method will be removed
func MergeRecreateColRefs(rem *Remote, tables []Table, create *string, dc []MergeDropBuff) {
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
							MergeAddOperation(create, RemoteAddFk(rem, table.Name, &fk))
						}
					}
				}
				continue
			}

			if dropBuff.IsPk() {
				if table.Primary != nil {
					pk := table.Primary
					if pk.Name == dropBuff.Primary.Name {
						MergeAddOperation(create, RemoteAddPk(rem, table.Name, pk))
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
							MergeAddOperation(create, RemoteAddUnique(rem, table.Name, &uu))
						}
					}
				}
			}
		}
	}
}

func MergeDropPkRefs(rem *Remote, tableName string, tables []Table, drop *string) []MergeDropBuff {

	var c int = 0
	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				c++
			}
		}
	}
	ret := make([]MergeDropBuff, c)
	lastix := 0

	for i := 0; i < len(tables); i++ {
		fks := tables[i].Foreign
		for j := 0; j < len(fks); j++ {
			if fks[j].Ref_table == tableName {
				MergeAddOperation(drop, RemoteDropConstraint(rem, tables[i].Name, &fks[j].Constraint))
				ret[lastix] = MergeNewDropFk(tables[i].Name, &fks[j])
				lastix++
			}
		}
	}

	return ret
}

// Obsolete : this method will be removed
func MergeRecreatePkRefs(rem *Remote, tables []Table, create *string, dc []MergeDropBuff) {
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
						MergeAddOperation(create, RemoteAddFk(rem, tables[j].Name, &fk))
					}
				}
			}
		}
	}
}

func MergePrimary(rem *Remote, mrg *Merger, localTable *Table, remTable *Table) {

	userPK := localTable.Primary

	if userPK == nil && (remTable == nil || remTable.Primary == nil) {
		return
	}

	tname := localTable.Name

	if userPK != nil && (remTable == nil || remTable.Primary == nil) {
		MergeAddOperation(mrg.create, RemoteAddPk(rem, tname, userPK))
		return
	}

	var droppedCs []MergeDropBuff

	if userPK == nil && (remTable != nil && remTable.Primary != nil) {
		droppedCs = MergeDropPkRefs(rem, tname, mrg.remTables, mrg.drop)

		MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, tname, &remTable.Primary.Constraint))

		MergeRecreatePkRefs(rem, mrg.localTables, mrg.create, droppedCs)

		return
	}

	eq := MergeCompareCColumn(userPK.Columns, remTable.Primary.Columns)

	if userPK.Name != remTable.Primary.Name || !eq {
		droppedCs = MergeDropPkRefs(rem, tname, mrg.remTables, mrg.drop)

		MergeAddOperation(mrg.drop, RemoteDropConstraint(rem, tname, &remTable.Primary.Constraint))
		MergeAddOperation(mrg.create, RemoteAddPk(rem, tname, userPK))

		MergeRecreatePkRefs(rem, mrg.localTables, mrg.create, droppedCs)
	}
}

func MergeDropTypeRefs(rem *Remote, m *Merger, lt *Type) []MergeDropBuff {

	var ret []MergeDropBuff

	for i := 0; i < len(m.remTables); i++ {

		t := &m.remTables[i]

		for j := 0; j < len(t.Columns); j++ {

			c := &t.Columns[j]

			if c.FullType == lt.Name {

				MergeAddOperation(m.drop, RemoteDropColumn(rem, t.Name, c))
				ret = append(ret, MergeNewDropC(t.Name, c))

			}

		}

	}

	return ret
}

todo finish merging columns
func MergeCompositeColumns(r *Remote, m *Merger, lt *Type, rt *Type) {

	// if typed table exists
	// MergeColumns(rem, m, ...)
	// for typed table
	// else
	//MergeAddOperation(m.drop, RemoteDropType(rem, rt))
	//MergeAddOperation(m.create, RemoteTypeDef(rem, lt))

	// ???
	// db := MergeDropTypeRefs(rem, m, rt)
	// MergeRecreateDropBuff(rem, m, db)
}

todo finish merging columns
func MergeComposite(rem *Remote, m *Merger, lt *Type, rt *Type) {

	var eq bool = true

	if len(lt.Columns) != len(rt.Columns) {

		eq = false

	} else {

		for i := 0; i < len(lt.Columns); i++ {

			if !MergeCompareColumn(rem, &lt.Columns[i], &rt.Columns[i]) {
				eq = false
				break
			}

		}

	}

	if eq {
		return
	}

	MergeCompositeColumns(rem, m, lt, rt)

}

func MergeEnum(rem *Remote, m *Merger, lt *Type, rt *Type) {

	var eq bool = true

	if len(lt.Values) != len(rt.Values) {

		eq = false

	} else {

		for i := 0; i < len(lt.Values); i++ {

			if lt.Values[i] != rt.Values[i] {
				eq = false
				break
			}

		}

	}

	if eq {
		return
	}

	db := MergeDropTypeRefs(rem, m, rt)

	MergeAddOperation(m.drop, RemoteDropType(rem, rt))

	MergeAddOperation(m.create, RemoteTypeDef(rem, lt))

	MergeRecreateDropBuff(rem, m, db)
}

func MergeType(rem *Remote, m *Merger, lt *Type, rt *Type) {

	if rt.Type == lt.Type {

		switch lt.Type {
		case TT_Composite:

			MergeComposite(rem, m, lt, rt)

		case TT_Enum:

			MergeEnum(rem, m, lt, rt)

		}

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

func MergeFindType(t *Type, ts []Type) *Type {

	for i := 0; i < len(ts); i++ {

		if t.Name == ts[i].Name {
			return &ts[i]
		}

	}

	return nil
}

func MergeTables(mrg *Merger, rem *Remote) {

	var devnull string

	for i := 0; i < len(mrg.localTables); i++ {

		if MergeFindTable(mrg.localTables[i].Name, mrg.remTables) == nil {

			*mrg.create += RemoteTableDef(rem, &mrg.localTables[i])
		}
	}

	drop := mrg.drop

	mrg.drop = &devnull

	for i := 0; i < len(mrg.localTables); i++ {

		t := MergeFindTable(mrg.localTables[i].Name, mrg.remTables)
		MergeColumns(rem, mrg, &mrg.localTables[i], t)

	}

	mrg.drop = drop

	for i := 0; i < len(mrg.localTables); i++ {

		t := MergeFindTable(mrg.localTables[i].Name, mrg.remTables)
		MergePrimary(rem, mrg, &mrg.localTables[i], t)

	}

	for i := 0; i < len(mrg.localTables); i++ {

		t := MergeFindTable(mrg.localTables[i].Name, mrg.remTables)
		MergeUnique(rem, mrg, &mrg.localTables[i], t)

	}

	for i := 0; i < len(mrg.localTables); i++ {

		t := MergeFindTable(mrg.localTables[i].Name, mrg.remTables)
		MergeFK(rem, mrg, &mrg.localTables[i], t)
		MergeCheck(rem, mrg, &mrg.localTables[i], t)
		MergeIx(rem, mrg, &mrg.localTables[i], t)

	}

	create := mrg.create
	mrg.create = &devnull

	for i := 0; i < len(mrg.localTables); i++ {

		t := MergeFindTable(mrg.localTables[i].Name, mrg.remTables)
		MergeColumns(rem, mrg, &mrg.localTables[i], t)

	}

	mrg.create = create

}

func MergeTypes(m *Merger, rem *Remote) {

	for i := 0; i < len(m.localTypes); i++ {

		lt := &m.localTypes[i]

		if rt := MergeFindType(lt, m.remTypes); rt != nil {

			MergeType(rem, m, lt, rt)

		} else {

			MergeAddOperation(m.create, RemoteTypeDef(rem, lt))

		}

	}

}

// merge remote schema with local
func MergeSchema(mrg *Merger, rem *Remote) (string, error) {

	drop := ""
	create := ""

	mrg.SetBuffers(&create, &drop)

	if mrg.localTypes != nil {
		MergeTypes(mrg, rem)
	}

	MergeTables(mrg, rem)

	cmd := ""
	cmd += drop
	cmd += create

	return cmd, nil
}
