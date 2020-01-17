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

// func MergeShiftTables(t []Table, si int, di int) {
// 	var ptmp Table = t[di]
// 	var tmp Table
// 	for i := si; i <= di; i++ {
// 		tmp = t[i]
// 		t[i] = ptmp
// 		ptmp = tmp
// 	}
// }

// func MergeSortTables(ds []Table) {
// 	for i := 0; i < len(ds); {
// 		fks := ds[i].Foreign
// 		shifted := false
// 		ic := i
// 		for j := 0; j < len(fks); j++ {
// 			ix := -1
// 			for z := 0; z < len(ds); z++ {
// 				if ds[z].Name == fks[j].Ref_table {
// 					ix = z
// 				}
// 			}
// 			if ix == -1 {
// 				break
// 			}
// 			if ix > i {
// 				MergeShiftTables(ds, ic, ix)
// 				ic++
// 				shifted = true
// 			}
// 		}
// 		if !shifted {
// 			i++
// 		}
// 	}
// }

func MergeCompareIxCs(c1 []IndexColumn, c2 []IndexColumn) (bool, error) {

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

func MergeCompareCCs(c1 []ConstraintColumn, c2 []ConstraintColumn) (bool, error) {

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

func MergeColumns(remote Remote, userT *Table, dbT *Table, create *string, drop *string) error {
	if dbT == nil {
		return nil
	}

	matchedIxs := list.New()
	if userT.Columns != nil && len(userT.Columns) > 0 {
		for i := 0; i < len(userT.Columns); i++ {
			userCol := userT.Columns[i]
			var index int = -1
			if dbT.Columns != nil {
				for j := 0; j < len(dbT.Columns); j++ {
					if dbT.Columns[j].Name == userCol.Name {
						index = j
					}
				}
			}

			if index < 0 {
				c, err := remote.AddColumnToString(userT.Name, &userCol)
				if err != nil {
					return err
				}
				MergeAddOperation(create, c)
			} else {
				dc := dbT.Columns[index]
				matchedIxs.PushBack(index)
				dcStr, err := remote.ColumnToString(&dc)
				if err != nil {
					return err
				}
				userColStr, err := remote.ColumnToString(&userCol)
				if err != nil {
					return err
				}
				if dc.Is_nullable == userCol.Is_nullable && dcStr == userColStr {
					continue
				}
				s, err := remote.AlterColumnToString(userT.Name, &userCol)
				if err != nil {
					return err
				}
				MergeAddOperation(create, s)
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
				eq, err := MergeCompareIxCs(userUq.Columns, dbT.Indexes[index].Columns)
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
				eq, err := MergeCompareCCs(userUq.Columns, dbT.Unique[index].Columns)
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
				ceq, err := MergeCompareCCs(userFK.Columns, dbT.Foreign[index].Columns)
				if err != nil {
					return err
				}
				rceq, err := MergeCompareCCs(userFK.Ref_columns, dbT.Foreign[index].Ref_columns)
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
		s, err := remote.AddPkToString(tname, userPK)
		if err != nil {
			return err
		}
		MergeAddOperation(create, s)
		return nil
	}

	if userPK == nil && dbT != nil && dbT.Primary != nil {
		for i := 0; i < len(oldTables); i++ {
			fks := oldTables[i].Foreign
			for j := 0; j < len(fks); j++ {
				if fks[j].Ref_table == tname {
					s, err := remote.DropCsToString(oldTables[i].Name, &fks[j].Constraint)
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
				}
			}
		}
		s, err := remote.DropCsToString(tname, &dbT.Primary.Constraint)
		if err != nil {
			return err
		}
		MergeAddOperation(drop, s)
		return nil
	}

	eq, err := MergeCompareCCs(userPK.Columns, dbT.Primary.Columns)
	if err != nil {
		return err
	}

	type DroppedConstraint struct {
		Table string
		Name  string
	}
	droppedCs := list.New()

	if userPK.Name != dbT.Primary.Name || !eq {

		for i := 0; i < len(oldTables); i++ {
			fks := oldTables[i].Foreign
			for j := 0; j < len(fks); j++ {
				if fks[j].Ref_table == tname {
					s, err := remote.DropCsToString(oldTables[i].Name, &fks[j].Constraint)
					if err != nil {
						return err
					}
					MergeAddOperation(drop, s)
					droppedCs.PushBack(DroppedConstraint{oldTables[i].Name, fks[j].Constraint.Name})
				}
			}
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

		for x := droppedCs.Front(); x != nil; x = x.Next() {
			droppedC := x.Value.(DroppedConstraint)
			for i := 0; i < len(newTables); i++ {
				if newTables[i].Name == droppedC.Table {
					for z := 0; z < len(newTables[i].Foreign); z++ {
						fk := newTables[i].Foreign[z]
						if fk.Name == droppedC.Name {
							s, err := remote.AddFkToString(oldTables[i].Name, &fk)
							if err != nil {
								return err
							}
							//MergeWarnFK(oldTables[i].Name, &fk, newTables, create)
							MergeAddOperation(create, s)
						}
					}
				}
			}
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
		if err := MergeColumns(remote, &tables[i], t, &create, &devnull); err != nil {
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
		if err := MergeColumns(remote, &tables[i], t, &devnull, &drop); err != nil {
			return "", err
		}
	}

	cmd := ""
	cmd += drop
	cmd += create
	return cmd, nil
}
