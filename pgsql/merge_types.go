package pgsql

import (
	"container/list"
)

type MergeTypeCtx struct {
	remoteTypes []Type
	localTypes  []Type
}

func MergeDropTypeRefs(
	m *StmtCtx,
	ts *MergeTableCtx,
	ss *MergeScriptCtx,
	lt *Type) []MergeDropCtx {

	var ret []MergeDropCtx

	for i := 0; i < len(ts.RemoteTables); i++ {

		t := &ts.RemoteTables[i]

		for j := 0; j < len(t.Columns); j++ {

			c := &t.Columns[j]

			if c.FullType == lt.Name {

				MergeAddOperation(ss.Drop, m.DropColumn(t.Name, c))
				ret = append(ret, MergeDropNewC(t.Name, c))

			}

		}

	}

	return ret
}

func MergeDropCompositeTypeColumnRefs(
	r *StmtCtx,
	lt *Type,
	ts *MergeTableCtx,
	ss *MergeScriptCtx,
	c *Column) []MergeDropCtx {

	var ret []MergeDropCtx

	for i := 0; i < len(ts.RemoteTables); i++ {

		var t *Table = &ts.RemoteTables[i]

		if t.Type != lt.Name {
			continue
		}
		ret = append(ret, MergeDropColRefs(r, c, t, ts.RemoteTables, ss.Drop)...)

	}

	return ret
}

func MergeComposite(
	r *StmtCtx,
	ts *MergeTableCtx,
	ss *MergeScriptCtx,
	lt *Type,
	rt *Type) {

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
			MergeAddOperation(ss.Create, r.AddColumn(lt.Name, lc))
		} else {
			rc := &rt.Columns[index]
			matchedIxs.PushBack(index)

			if MergeCompareColumn(r, lc, rc) {
				continue
			}

			MergeAddOperation(ss.Create, r.AlterColumn(lt.Name, rc, lc))

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

		db := MergeDropCompositeTypeColumnRefs(r, rt, ts, ss, dc)
		//drefs := MergeDropColRefs(rem, dc, remTable, mrg.remTables, mrg.drop)

		MergeAddOperation(ss.Drop, r.DropColumn(lt.Name, dc))

		//MergeRecreateColRefs(rem, mrg.localTables, mrg.create, drefs)
		MergeDropRecreate(r, db, ss, ts)
	}

}

func MergeEnum(
	r *StmtCtx,
	ts *MergeTableCtx,
	ss *MergeScriptCtx,
	lt *Type, rt *Type) {

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

	db := MergeDropTypeRefs(r, ts, ss, rt)

	MergeAddOperation(ss.Drop, r.DropType(rt))

	MergeAddOperation(ss.Create, r.AddType(lt))

	MergeDropRecreate(r, db, ss, ts)
}

func MergeType(
	r *StmtCtx,
	ts *MergeTableCtx,
	ss *MergeScriptCtx,
	lt *Type, rt *Type) {

	if rt.Type == lt.Type {

		switch lt.Type {
		case TypeComposite:

			MergeComposite(r, ts, ss, lt, rt)

		case TypeEnum:

			MergeEnum(r, ts, ss, lt, rt)

		}

	}

}

func MergeFindType(t *Type, ts []Type) *Type {

	for i := 0; i < len(ts); i++ {

		if t.Name == ts[i].Name {
			return &ts[i]
		}

	}

	return nil
}

func MergeTypes(
	m *StmtCtx,
	tt *MergeTypeCtx,
	ts *MergeTableCtx,
	ss *MergeScriptCtx) {

	for i := 0; i < len(tt.localTypes); i++ {

		lt := &tt.localTypes[i]

		if rt := MergeFindType(lt, tt.remoteTypes); rt != nil {

			MergeType(m, ts, ss, lt, rt)

		} else {

			MergeAddOperation(ss.Create, m.AddType(lt))

		}

	}

}
