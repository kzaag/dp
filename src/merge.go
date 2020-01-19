package main

import (
	"container/list"
	"database/sql"
	"strings"
)

/*
generating differential script based on local vs remote schema
*/

func MergeAddOperation(dest *string, elem string) {
	if !strings.Contains(*dest, elem) {
		*dest += elem
	}
}

func MergeCompareIColumn(c1 []IndexColumn, c2 []IndexColumn) (bool, error) {

	if c1 == nil || c2 == nil {
		return false, nil
	}

	if len(c1) != len(c2) {
		return true, nil
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
			return false, nil
		}
	}

	return true, nil
}

func MergeCompareCColumn(c1 []ConstraintColumn, c2 []ConstraintColumn) (bool, error) {

	if c1 == nil || c2 == nil {
		return false, nil
	}

	if len(c1) != len(c2) {
		return false, nil
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
			return false, nil
		}
	}

	return true, nil
}

func MergeCompareColumn(remote Remote, c1 Column, c2 Column) (bool, error) {
	var userColStr, dcStr string
	var err error
	if dcStr, err = remote.ColumnToString(&c1); err != nil {
		return false, err
	}
	if userColStr, err = remote.ColumnToString(&c2); err != nil {
		return false, err
	}
	if dcStr == userColStr {
		return true, nil
	}
	return false, nil
}

func MergeColumns(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) error {

	if dbT == nil {
		return nil
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
				c, err := remote.AddColumnToString(userT.Name, &uc)
				if err != nil {
					return err
				}
				MergeAddOperation(create, c)
			} else {
				dc := dbT.Columns[index]
				matchedIxs.PushBack(index)

				if eq, err := MergeCompareColumn(remote, uc, dc); err != nil {
					return err
				} else if eq {
					continue
				}

				drefs, err := MergeDropColRefs(remote, dc, *dbT, oldTables, drop)
				if err != nil {
					return err
				}

				if dc.Is_Identity != uc.Is_Identity {
					var d, c string
					var err error
					if d, err = remote.DropColumnToString(userT.Name, &dc); err != nil {
						return err
					}
					if c, err = remote.AddColumnToString(userT.Name, &uc); err != nil {
						return err
					}
					MergeAddOperation(drop, d)
					MergeAddOperation(create, c)

					if err = MergeRecreateColRefs(remote, newTables, create, drefs); err != nil {
						return err
					}
					continue
				}

				if s, err := remote.AlterColumnToString(userT.Name, &uc); err != nil {
					return err
				} else {
					MergeAddOperation(create, s)
				}

				if err = MergeRecreateColRefs(remote, newTables, create, drefs); err != nil {
					return err
				}
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
		s, err := remote.DropColumnToString(userT.Name, &dbT.Columns[i])
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
	}

	return nil
}

func MergeIx(remote Remote, userT *Table, dbT *Table, create *string, drop *string) error {
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
				s, err := remote.AddIxToString(userT.Name, &userUq)
				if err != nil {
					return err
				}
				MergeAddOperation(create, s)
			} else {
				matchedIxs.PushBack(index)
				eq, err := MergeCompareIColumn(userUq.Columns, dbT.Indexes[index].Columns)
				if err != nil {
					return err
				}
				if eq {
					// nothing to do
				} else {
					s, err := remote.DropIxToString(userT.Name, &dbT.Indexes[index])
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
					s, err = remote.AddIxToString(userT.Name, &userUq)
					if err != nil {
						return err
					}
					MergeAddOperation(create, s)
				}
			}
		}
	}

	if dbT == nil {
		return nil
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
		s, err := remote.DropIxToString(userT.Name, &dbT.Indexes[i])
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
	}

	return nil
}

func MergeUnique(remote Remote, userT *Table, dbT *Table, create *string, drop *string) error {
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
				s, err := remote.AddUniqueToString(userT.Name, &userUq)
				if err != nil {
					return err
				}
				MergeAddOperation(create, s)
			} else {
				matchedIxs.PushBack(index)
				eq, err := MergeCompareCColumn(userUq.Columns, dbT.Unique[index].Columns)
				if err != nil {
					return err
				}
				if eq {
					// nothing to do
				} else {
					s, err := remote.DropCsToString(userT.Name, &dbT.Unique[index].Constraint)
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
					s, err = remote.AddUniqueToString(userT.Name, &userUq)
					if err != nil {
						return err
					}
					MergeAddOperation(create, s)
				}
			}
		}
	}

	if dbT == nil {
		return nil
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
		s, err := remote.DropCsToString(userT.Name, &dbT.Unique[i].Constraint)
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
	}

	return nil
}

func MergeCheck(remote Remote, userT *Table, dbT *Table, create *string, drop *string) error {
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
				s, err := remote.AddCheckToString(userT.Name, &userC)
				if err != nil {
					return err
				}
				MergeAddOperation(create, s)
			} else {
				matchedIxs.PushBack(index)
				if userC.Def == dbT.Check[i].Def {
					// nothing to do
				} else {
					s, err := remote.DropCsToString(userT.Name, &Constraint{dbT.Check[index].Name, nil})
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
					s, err = remote.AddCheckToString(userT.Name, &userC)
					if err != nil {
						return err
					}
					MergeAddOperation(create, s)
				}
			}
		}
	}

	if dbT == nil {
		return nil
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
		s, err := remote.DropCsToString(userT.Name, &Constraint{dbT.Check[i].Name, nil})
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
	}

	return nil
}

func MergeFK(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) error {
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
				s, err := remote.AddFkToString(userT.Name, &userFK)
				if err != nil {
					return err
				}
				//MergeWarnFK(userT.Name, &userFK, newTables, create)
				MergeAddOperation(create, s)
			} else {
				matchedIxs.PushBack(index)
				ceq, err := MergeCompareCColumn(userFK.Columns, dbT.Foreign[index].Columns)
				if err != nil {
					return err
				}
				rceq, err := MergeCompareCColumn(userFK.Ref_columns, dbT.Foreign[index].Ref_columns)
				if err != nil {
					return err
				}
				if ceq && rceq {
					// nothing to do
				} else {
					s, err := remote.DropCsToString(userT.Name, &dbT.Foreign[index].Constraint)
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
					s, err = remote.AddFkToString(userT.Name, &userFK)
					if err != nil {
						return err
					}
					//MergeWarnFK(userT.Name, &userFK, newTables, create)
					MergeAddOperation(create, s)
				}
			}
		}
	}

	if dbT == nil {
		return nil
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
		s, err := remote.DropCsToString(userT.Name, &dbT.Foreign[i].Constraint)
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
	}

	return nil
}

const (
	DC_TYPE_PK = iota
	DC_TYPE_FK = iota
)

type MergeDropBuff struct {
	Table   string
	Foreign *ForeignKey
	Primary *PrimaryKey
	Type    uint
}

func MergeNewDropBuffFk(tableName string, fk *ForeignKey) MergeDropBuff {
	return MergeDropBuff{tableName, fk, nil, DC_TYPE_FK}
}

func MergeNewDropBuffPk(tableName string, pk *PrimaryKey) MergeDropBuff {
	return MergeDropBuff{tableName, nil, pk, DC_TYPE_PK}
}

func (d MergeDropBuff) IsPk() bool {
	return d.Type == DC_TYPE_PK
}

func (d MergeDropBuff) IsFk() bool {
	return d.Type == DC_TYPE_FK
}

func (d MergeDropBuff) Recreate(remote Remote, create *string) error {
	var op string
	var err error
	switch d.Type {
	case DC_TYPE_FK:
		op, err = remote.AddFkToString(d.Table, d.Foreign)
	case DC_TYPE_PK:
		op, err = remote.AddPkToString(d.Table, d.Primary)
	}
	if err != nil {
		return err
	}
	MergeAddOperation(create, op)
	return nil
}

func MergeDropColRefs(remote Remote, col Column, table Table, tables []Table, drop *string) ([]MergeDropBuff, error) {
	list := list.New()
	if table.Primary != nil && table.Primary.Columns != nil {
		for i := 0; i < len(table.Primary.Columns); i++ {
			c := table.Primary.Columns[i]
			if c.Name == col.Name {
				bf, err := MergeDropPkRefs(remote, table.Name, tables, drop)
				if err != nil {
					return nil, err
				}
				for i := 0; i < len(bf); i++ {
					list.PushBack(bf[i])
				}
				d, err := remote.DropCsToString(table.Name, &table.Primary.Constraint)
				if err != nil {
					return nil, err
				}
				MergeAddOperation(drop, d)
				list.PushBack(MergeNewDropBuffPk(table.Name, table.Primary))
			}
		}
	}

	ret := make([]MergeDropBuff, list.Len())
	for i, x := 0, list.Front(); x != nil; i, x = i+1, x.Next() {
		ret[i] = x.Value.(MergeDropBuff)
	}

	return ret, nil
}

func MergeRecreateColRefs(remote Remote, tables []Table, create *string, dc []MergeDropBuff) error {
	if dc == nil {
		return nil
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
							if s, err := remote.AddFkToString(table.Name, &fk); err != nil {
								return err
							} else {
								MergeAddOperation(create, s)
							}
						}
					}
				}
				continue
			}

			if dropBuff.IsPk() {
				if table.Primary != nil {
					pk := table.Primary
					if pk != nil && pk.Name == dropBuff.Primary.Name {
						if s, err := remote.AddPkToString(table.Name, pk); err != nil {
							return err
						} else {
							MergeAddOperation(create, s)
						}
					}
				}
				continue
			}
		}
	}

	return nil
}

func MergeDropPkRefs(remote Remote, tableName string, tables []Table, drop *string) ([]MergeDropBuff, error) {

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
				s, err := remote.DropCsToString(tables[i].Name, &fks[j].Constraint)
				if err != nil {
					return nil, err
				}
				MergeAddOperation(drop, s)
				ret[lastix] = MergeNewDropBuffFk(tables[i].Name, &fks[j])
				lastix++
			}
		}
	}

	return ret, nil
}

func MergeRecreatePkRefs(remote Remote, tables []Table, create *string, dc []MergeDropBuff) error {
	if dc == nil {
		return nil
	}

	for i := 0; i < len(dc); i++ {
		droppedC := dc[i]
		for j := 0; j < len(tables); j++ {
			if tables[j].Name == droppedC.Table {
				for z := 0; z < len(tables[j].Foreign); z++ {
					fk := tables[j].Foreign[z]
					if fk.Name == droppedC.Table {
						if s, err := remote.AddFkToString(tables[j].Name, &fk); err != nil {
							return err
						} else {
							MergeAddOperation(create, s)
						}
					}
				}
			}
		}
	}

	return nil
}

func MergePrimary(
	remote Remote,
	userT *Table, dbT *Table,
	oldTables []Table, newTables []Table,
	create *string, drop *string) error {

	userPK := userT.Primary

	if userPK == nil && (dbT == nil || dbT.Primary == nil) {
		return nil
	}

	tname := userT.Name

	if userPK != nil && (dbT == nil || dbT.Primary == nil) {
		if s, err := remote.AddPkToString(tname, userPK); err != nil {
			return err
		} else {
			MergeAddOperation(create, s)
			return nil
		}
	}

	var droppedCs []MergeDropBuff = nil

	if userPK == nil && (dbT != nil && dbT.Primary != nil) {
		var err error
		droppedCs, err = MergeDropPkRefs(remote, tname, oldTables, drop)
		if err != nil {
			return err
		}
		if s, err := remote.DropCsToString(tname, &dbT.Primary.Constraint); err != nil {
			return err
		} else {
			MergeAddOperation(drop, s)
		}
		if err := MergeRecreatePkRefs(remote, newTables, create, droppedCs); err != nil {
			return err
		}
		return nil
	}

	eq, err := MergeCompareCColumn(userPK.Columns, dbT.Primary.Columns)
	if err != nil {
		return err
	}

	if userPK.Name != dbT.Primary.Name || !eq {
		if droppedCs, err = MergeDropPkRefs(remote, tname, oldTables, drop); err != nil {
			return err
		}
		s, err := remote.DropCsToString(tname, &dbT.Primary.Constraint)
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
		s, err = remote.AddPkToString(tname, userPK)
		if err != nil {
			return err
		}
		MergeAddOperation(create, s)
		if err := MergeRecreatePkRefs(remote, newTables, create, droppedCs); err != nil {
			return err
		}
	}

	return nil
}

func MergeConstraints(
	remote Remote,
	userT *Table,
	dbT *Table,
	create *string,
	drop *string,
	oldTables []Table,
	newTables []Table) error {

	if err := MergeFK(remote, userT, dbT, oldTables, newTables, create, drop); err != nil {
		return err
	}

	if err := MergeCheck(remote, userT, dbT, create, drop); err != nil {
		return err
	}

	if err := MergeIx(remote, userT, dbT, create, drop); err != nil {
		return err
	}

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

	//MergeSortTables(tables)

	remTables, err := RemoteGetMatchTables(remote, db, tables)

	if err != nil {
		return "", err
	}

	drop := ""
	create := ""
	var devnull string
	for i := 0; i < len(tables); i++ {
		if MergeFindTable(tables[i].Name, remTables) == nil {
			ct, err := RemoteTableToString(remote, &tables[i])
			if err != nil {
				return "", err
			}
			create += ct
		}

	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		if err := MergeColumns(remote, &tables[i], t, remTables, tables, &create, &devnull); err != nil {
			return "", err
		}
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		if err := MergePrimary(remote, &tables[i], t, remTables, tables, &create, &drop); err != nil {
			return "", err
		}
	}
	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		if err := MergeUnique(remote, &tables[i], t, &create, &drop); err != nil {
			return "", err
		}
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		err = MergeConstraints(
			remote,
			&tables[i],
			t,
			&create,
			&drop,
			remTables,
			tables)
		if err != nil {
			return "", err
		}
	}

	for i := 0; i < len(tables); i++ {
		t := MergeFindTable(tables[i].Name, remTables)
		if err := MergeColumns(remote, &tables[i], t, remTables, tables, &devnull, &drop); err != nil {
			return "", err
		}
	}

	cmd := ""
	cmd += drop
	cmd += create
	return cmd, nil
}
