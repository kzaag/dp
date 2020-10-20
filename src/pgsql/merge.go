package pgsql

import (
	"container/list"

	"github.com/kzaag/database-project/src/sql"
)

type PgSqlMergeCtx struct {
	sql.SqlMergeCtx
	remTypes   []Type
	localTypes []Type
}

func PgSqlMergeDropTypeRefs(m *PgSqlMergeCtx, lt *Type) []SqlMergeDropBuff {

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

func MergeDropCompositeTypeColumnRefs(r *Remote, m *Merger, lt *Type, c *Column) []MergeDropBuff {

	var ret []MergeDropBuff

	for i := 0; i < len(m.remTables); i++ {

		var t *Table = &m.remTables[i]

		if t.Type != lt.Name {
			continue
		}
		ret = append(ret, MergeDropColRefs(r, c, t, m.remTables, m.drop)...)

	}

	return ret
}

func MergeComposite(r *Remote, m *Merger, lt *Type, rt *Type) {

	matchedIxs := list.New()
	for i := 0; i < len(lt.Columns); i++ {

		lc := &lt.Columns[i]
		var index int = -1
		if rt.Columns != nil {
			for j := 0; j < len(rt.Columns); j++ {
				if rt.Columns[j].Name == lc.Name {
					index = j
				}
			}
		}

		if index < 0 {
			MergeAddOperation(m.create, RemoteAddColumn(r, lt.Name, lc))
		} else {
			rc := &rt.Columns[index]
			matchedIxs.PushBack(index)

			if MergeCompareColumn(r, lc, rc) {
				continue
			}

			MergeAddOperation(m.create, RemoteAlterColumn(r, lt.Name, rc, lc))

		}
	}

	for i := 0; i < len(rt.Columns); i++ {
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

		dc := &rt.Columns[i]

		db := MergeDropCompositeTypeColumnRefs(r, m, rt, dc)
		//drefs := MergeDropColRefs(rem, dc, remTable, mrg.remTables, mrg.drop)

		MergeAddOperation(m.drop, RemoteDropColumn(r, lt.Name, dc))

		//MergeRecreateColRefs(rem, mrg.localTables, mrg.create, drefs)
		MergeRecreateDropBuff(r, m, db)
	}

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

func SqlMergeFindType(t *Type, ts []Type) *Type {

	for i := 0; i < len(ts); i++ {

		if t.Name == ts[i].Name {
			return &ts[i]
		}

	}

	return nil
}

func SqlMergeTypes(m *Merger, rem *Remote) {

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
func PgSqlMerge(mrg *PgSqlMergeCtx) (string, error) {

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

func SqlMergeTables(mrg *SqlMergeCtx) {

	var devnull string

	for i := 0; i < len(mrg.localTables); i++ {

		if t := SqlMergeFindTable(mrg.localTables[i].Name, mrg.remTables); t == nil {

			*mrg.create += mrg.CreateTable(&mrg.localTables[i])

		} else {

			if t.Type != mrg.localTables[i].Type {

				MergeAddOperation(mrg.drop, RemoteDropTable(rem, t.Name))
				*mrg.create += RemoteTableDef(rem, &mrg.localTables[i])

				// modifying table buffers is not really the way of this algorithm
				// so this will need to be removed eventually
				t.Check = nil
				t.Foreign = nil
				t.Indexes = nil
				t.Primary = nil
				t.Unique = nil

			}

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
