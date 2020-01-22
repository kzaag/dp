package main

import (
	"container/list"
	"database/sql"
	"strings"
)

/*
generating differential script based on local vs remote schema
*/

const (
	DC_TYPE_PK = iota
	DC_TYPE_FK = iota
	DC_TYPE_UQ = iota
	DC_TYPE_CH = iota
)

type MergeDropBuff struct {
	Table   string
	Type    uint
	Foreign *ForeignKey
	Primary *PrimaryKey
	Unique  *Unique
	Check   *Check
}

func MergeNewDropFk(tableName string, fk *ForeignKey) MergeDropBuff {
	return MergeDropBuff{tableName, DC_TYPE_FK, fk, nil, nil, nil}
}

func MergeNewDropPk(tableName string, pk *PrimaryKey) MergeDropBuff {
	return MergeDropBuff{tableName, DC_TYPE_PK, nil, pk, nil, nil}
}

func MergeNewDropUq(tablename string, u *Unique) MergeDropBuff {
	return MergeDropBuff{tablename, DC_TYPE_UQ, nil, nil, u, nil}
}

func MergeNewDropCh(tablename string, c *Check) MergeDropBuff {
	return MergeDropBuff{tablename, DC_TYPE_CH, nil, nil, nil, c}
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

func MergeCompareColumn(remote Remote, c1 Column, c2 Column) bool {
	dcStr := remote.ColumnToString(&c1)
	userColStr := remote.ColumnToString(&c2)
	if dcStr == userColStr {
		return true
	}
	return false
}

func MergeColumns(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) {

	if dbT == nil {
		return
	}

	matchedIxs := list.New()
	if userT.Columns != nil {
		for i := 0; i < len(userT.Columns); i++ {
			uc := userT.Columns[i]
			var index int = -1
			if dbT.Columns != nil {
				for j := 0; j < len(dbT.Columns); j++ {
					if dbT.Columns[j].Name == uc.Name {
						index = j
					}
				}
			}

			if index < 0 {
				MergeAddOperation(create, remote.AddColumnToString(userT.Name, &uc))
			} else {
				dc := dbT.Columns[index]
				matchedIxs.PushBack(index)

				if MergeCompareColumn(remote, uc, dc) {
					continue
				}

				drefs := MergeDropColRefs(remote, dc, *dbT, oldTables, drop)

				if dc.Is_Identity != uc.Is_Identity {
					MergeAddOperation(drop, remote.DropColumnToString(userT.Name, &dc))
					MergeAddOperation(create, remote.AddColumnToString(userT.Name, &uc))
				} else {
					MergeAddOperation(create, remote.AlterColumnToString(userT.Name, &uc))
				}
				MergeRecreateColRefs(remote, newTables, create, drefs)
			}
		}
	}

	for i := 0; i < len(dbT.Columns); i++ {
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

		drefs := MergeDropColRefs(remote, dbT.Columns[i], *dbT, oldTables, drop)

		MergeAddOperation(drop, remote.DropColumnToString(userT.Name, &dbT.Columns[i]))

		MergeRecreateColRefs(remote, newTables, create, drefs)
	}
}

func MergeIx(remote Remote, userT *Table, dbT *Table, create *string, drop *string) {
	matchedIxs := list.New()
	if userT.Indexes != nil && len(userT.Indexes) > 0 {
		for i := 0; i < len(userT.Indexes); i++ {
			userUq := userT.Indexes[i]
			var index int = -1
			if dbT != nil && dbT.Indexes != nil {
				for j := 0; j < len(dbT.Indexes); j++ {
					if dbT.Indexes[j].Name == userUq.Name {
						index = j
					}
				}
			}

			if index < 0 {
				MergeAddOperation(create, remote.AddIxToString(userT.Name, &userUq))
			} else {
				matchedIxs.PushBack(index)

				if !MergeCompareIColumn(userUq.Columns, dbT.Indexes[index].Columns) {
					MergeAddOperation(drop, remote.DropIxToString(userT.Name, &dbT.Indexes[index]))
					MergeAddOperation(create, remote.AddIxToString(userT.Name, &userUq))
				}
			}
		}
	}

	if dbT == nil {
		return
	}

	for i := 0; i < len(dbT.Indexes); i++ {
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
		MergeAddOperation(drop, remote.DropIxToString(userT.Name, &dbT.Indexes[i]))
	}
}

func MergeUnique(remote Remote, userT *Table, dbT *Table, create *string, drop *string) {
	matchedIxs := list.New()
	if userT.Unique != nil && len(userT.Unique) > 0 {
		for i := 0; i < len(userT.Unique); i++ {
			userUq := userT.Unique[i]
			var index int = -1
			if dbT != nil && dbT.Unique != nil {
				for j := 0; j < len(dbT.Unique); j++ {
					if dbT.Unique[j].Name == userUq.Name {
						index = j
					}
				}
			}

			if index < 0 {
				MergeAddOperation(create, remote.AddUniqueToString(userT.Name, &userUq))
			} else {
				matchedIxs.PushBack(index)

				if !MergeCompareCColumn(userUq.Columns, dbT.Unique[index].Columns) {
					MergeAddOperation(drop, remote.DropCsToString(userT.Name, &dbT.Unique[index].Constraint))
					MergeAddOperation(create, remote.AddUniqueToString(userT.Name, &userUq))
				}
			}
		}
	}

	if dbT == nil {
		return
	}

	for i := 0; i < len(dbT.Unique); i++ {
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
		MergeAddOperation(drop, remote.DropCsToString(userT.Name, &dbT.Unique[i].Constraint))
	}
}

func MergeCheck(remote Remote, userT *Table, dbT *Table, create *string, drop *string) {
	matchedIxs := list.New()
	if userT.Check != nil && len(userT.Check) > 0 {
		for i := 0; i < len(userT.Check); i++ {
			userC := userT.Check[i]
			var index int = -1
			if dbT != nil && dbT.Check != nil {
				for j := 0; j < len(dbT.Check); j++ {
					if dbT.Check[j].Name == userC.Name {
						index = j
					}
				}
			}

			if index < 0 {
				MergeAddOperation(create, remote.AddCheckToString(userT.Name, &userC))
			} else {
				matchedIxs.PushBack(index)
				if userC.Def != dbT.Check[i].Def {
					MergeAddOperation(drop, remote.DropCsToString(userT.Name, &Constraint{dbT.Check[index].Name, nil}))
					MergeAddOperation(create, remote.AddCheckToString(userT.Name, &userC))
				}
			}
		}
	}

	if dbT == nil {
		return
	}

	for i := 0; i < len(dbT.Check); i++ {
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
		MergeAddOperation(drop, remote.DropCsToString(userT.Name, &Constraint{dbT.Check[i].Name, nil}))
	}
}

func MergeFK(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) {

	userFKs := userT.Foreign
	matchedIxs := list.New()

	if userFKs != nil && len(userFKs) > 0 {
		for i := 0; i < len(userFKs); i++ {
			userFK := userFKs[i]
			var index int = -1
			if dbT != nil && dbT.Foreign != nil {
				for j := 0; j < len(dbT.Foreign); j++ {
					if dbT.Foreign[j].Name == userFK.Name {
						index = j
					}
				}
			}

			if index < 0 {
				MergeAddOperation(create, remote.AddFkToString(userT.Name, &userFK))
			} else {
				matchedIxs.PushBack(index)

				ceq := MergeCompareCColumn(userFK.Columns, dbT.Foreign[index].Columns)
				rceq := MergeCompareCColumn(userFK.Ref_columns, dbT.Foreign[index].Ref_columns)
				if !ceq || !rceq {
					MergeAddOperation(drop, remote.DropCsToString(userT.Name, &dbT.Foreign[index].Constraint))
					MergeAddOperation(create, remote.AddFkToString(userT.Name, &userFK))
				}
			}
		}
	}

	if dbT == nil {
		return
	}

	for i := 0; i < len(dbT.Foreign); i++ {
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
		MergeAddOperation(drop, remote.DropCsToString(userT.Name, &dbT.Foreign[i].Constraint))
	}
}

func MergeDropColRefs(remote Remote, col Column, table Table, tables []Table, drop *string) []MergeDropBuff {
	list := list.New()
	if table.Primary != nil && table.Primary.Columns != nil {
		for i := 0; i < len(table.Primary.Columns); i++ {
			c := table.Primary.Columns[i]
			if c.Name == col.Name {
				bf := MergeDropPkRefs(remote, table.Name, tables, drop)
				for i := 0; i < len(bf); i++ {
					list.PushBack(bf[i])
				}
				MergeAddOperation(drop, remote.DropCsToString(table.Name, &table.Primary.Constraint))
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
					MergeAddOperation(drop, remote.DropCsToString(table.Name, &c.Constraint))
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

func MergeRecreateColRefs(remote Remote, tables []Table, create *string, dc []MergeDropBuff) {
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
							MergeAddOperation(create, remote.AddFkToString(table.Name, &fk))
						}
					}
				}
				continue
			}

			if dropBuff.IsPk() {
				if table.Primary != nil {
					pk := table.Primary
					if pk.Name == dropBuff.Primary.Name {
						MergeAddOperation(create, remote.AddPkToString(table.Name, pk))
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
							MergeAddOperation(create, remote.AddUniqueToString(table.Name, &uu))
						}
					}
				}
			}
		}
	}
}

func MergeDropPkRefs(remote Remote, tableName string, tables []Table, drop *string) []MergeDropBuff {

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
				MergeAddOperation(drop, remote.DropCsToString(tables[i].Name, &fks[j].Constraint))
				ret[lastix] = MergeNewDropFk(tables[i].Name, &fks[j])
				lastix++
			}
		}
	}

	return ret
}

func MergeRecreatePkRefs(remote Remote, tables []Table, create *string, dc []MergeDropBuff) {
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
						MergeAddOperation(create, remote.AddFkToString(tables[j].Name, &fk))
					}
				}
			}
		}
	}
}

func MergePrimary(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) {

	userPK := userT.Primary

	if userPK == nil && (dbT == nil || dbT.Primary == nil) {
		return
	}

	tname := userT.Name

	if userPK != nil && (dbT == nil || dbT.Primary == nil) {
		MergeAddOperation(create, remote.AddPkToString(tname, userPK))
		return
	}

	var droppedCs []MergeDropBuff = nil

	if userPK == nil && (dbT != nil && dbT.Primary != nil) {
		droppedCs = MergeDropPkRefs(remote, tname, oldTables, drop)

		MergeAddOperation(drop, remote.DropCsToString(tname, &dbT.Primary.Constraint))

		MergeRecreatePkRefs(remote, newTables, create, droppedCs)
	}

	eq := MergeCompareCColumn(userPK.Columns, dbT.Primary.Columns)

	if userPK.Name != dbT.Primary.Name || !eq {
		droppedCs = MergeDropPkRefs(remote, tname, oldTables, drop)

		MergeAddOperation(drop, remote.DropCsToString(tname, &dbT.Primary.Constraint))
		MergeAddOperation(create, remote.AddPkToString(tname, userPK))

		MergeRecreatePkRefs(remote, newTables, create, droppedCs)
	}
}

func MergeConstraints(
	remote Remote,
	userT *Table,
	dbT *Table,
	create *string,
	drop *string,
	oldTables []Table,
	newTables []Table) error {

	MergeFK(remote, userT, dbT, oldTables, newTables, create, drop)

	MergeCheck(remote, userT, dbT, create, drop)

	MergeIx(remote, userT, dbT, create, drop)

	return nil
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

func MergeTablesStr(remote Remote, db *sql.DB, tables []Table) (string, error) {

	remTables, err := RemoteGetMatchTables(remote, db, tables)

	if err != nil {
		return "", err
	}

	drop := ""
	create := ""
	var devnull string
	for i := 0; i < len(tables); i++ {
		if MergeFindTable(tables[i].Name, remTables) == nil {
			create += RemoteTableToString(remote, &tables[i])
		}

	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		MergeColumns(remote, &tables[i], t, remTables, tables, &create, &devnull)
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		MergePrimary(remote, &tables[i], t, remTables, tables, &create, &drop)
	}
	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		MergeUnique(remote, &tables[i], t, &create, &drop)
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		MergeConstraints(
			remote,
			&tables[i],
			t,
			&create,
			&drop,
			remTables,
			tables)
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		MergeColumns(remote, &tables[i], t, remTables, tables, &devnull, &drop)
	}

	cmd := ""
	cmd += drop
	cmd += create
	return cmd, nil
}
