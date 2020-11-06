package pgsql

import (
	"container/list"

	"github.com/kzaag/dp/rdbms"
)

type MergeTypeCtx struct {
	remoteTypes []Type
	localTypes  []Type
}

func MergeDropTypeRefs(
	m *rdbms.StmtCtx,
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx,
	lt *Type) []rdbms.MergeDropCtx {

	var ret []rdbms.MergeDropCtx

	for i := 0; i < len(ts.RemoteTables); i++ {

		t := &ts.RemoteTables[i]

		for j := 0; j < len(t.Columns); j++ {

			c := &t.Columns[j]

			if c.FullType == lt.Name {

				rdbms.MergeAddOperation(ss.Drop, m.DropColumn(t.Name, c))
				ret = append(ret, rdbms.MergeDropNewC(t.Name, c))

			}

		}

	}

	return ret
}

func MergeDropCompositeTypeColumnRefs(
	r *rdbms.StmtCtx,
	lt *Type,
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx,
	c *rdbms.Column) []rdbms.MergeDropCtx {

	var ret []rdbms.MergeDropCtx

	for i := 0; i < len(ts.RemoteTables); i++ {

		var t *rdbms.Table = &ts.RemoteTables[i]

		if t.Type != lt.Name {
			continue
		}
		ret = append(ret, rdbms.MergeDropColRefs(r, c, t, ts.RemoteTables, ss.Drop)...)

	}

	return ret
}

func MergeComposite(
	r *rdbms.StmtCtx,
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx,
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
			rdbms.MergeAddOperation(ss.Create, r.AddColumn(lt.Name, lc))
		} else {
			rc := &rt.Columns[index]
			matchedIxs.PushBack(index)

			if rdbms.MergeCompareColumn(r, lc, rc) {
				continue
			}

			rdbms.MergeAddOperation(ss.Create, r.AlterColumn(lt.Name, rc, lc))

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

		rdbms.MergeAddOperation(ss.Drop, r.DropColumn(lt.Name, dc))

		//MergeRecreateColRefs(rem, mrg.localTables, mrg.create, drefs)
		rdbms.MergeDropRecreate(r, db, ss, ts)
	}

}

func MergeEnum(
	r *StmtCtx,
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx,
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

	db := MergeDropTypeRefs(&r.StmtCtx, ts, ss, rt)

	rdbms.MergeAddOperation(ss.Drop, r.DropType(rt))

	rdbms.MergeAddOperation(ss.Create, r.AddType(lt))

	rdbms.MergeDropRecreate(&r.StmtCtx, db, ss, ts)
}

func MergeType(
	r *StmtCtx,
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx,
	lt *Type, rt *Type) {

	if rt.Type == lt.Type {

		switch lt.Type {
		case TT_Composite:

			MergeComposite(&r.StmtCtx, ts, ss, lt, rt)

		case TT_Enum:

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
	ts *rdbms.MergeTableCtx,
	ss *rdbms.MergeScriptCtx) {

	for i := 0; i < len(tt.localTypes); i++ {

		lt := &tt.localTypes[i]

		if rt := MergeFindType(lt, tt.remoteTypes); rt != nil {

			MergeType(m, ts, ss, lt, rt)

		} else {

			rdbms.MergeAddOperation(ss.Create, m.AddType(lt))

		}

	}

}

func MergeTables(ctx *StmtCtx, ts *rdbms.MergeTableCtx, ss *rdbms.MergeScriptCtx) {

	var devnull string

	for i := 0; i < len(ts.LocalTables); i++ {

		if t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables); t == nil {

			*ss.Create += ctx.CreateTable(&ts.LocalTables[i])

		} else {

			if t.Type != ts.LocalTables[i].Type {

				rdbms.MergeAddOperation(ss.Drop, ctx.DropTable(t))
				*ss.Create += ctx.CreateTable(&ts.LocalTables[i])

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

	drop := ss.Drop
	ss.Drop = &devnull

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeColumns(&ctx.StmtCtx, &ts.LocalTables[i], t, ts, ss)

	}

	ss.Drop = drop

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergePrimary(&ctx.StmtCtx, &ts.LocalTables[i], t, ss, ts)

	}

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeUnique(&ctx.StmtCtx, &ts.LocalTables[i], t, ss)

	}

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeFK(&ctx.StmtCtx, &ts.LocalTables[i], t, ss)
		rdbms.MergeCheck(&ctx.StmtCtx, &ts.LocalTables[i], t, ss)
		rdbms.MergeIx(&ctx.StmtCtx, &ts.LocalTables[i], t, ss)

	}

	create := ss.Create
	ss.Create = &devnull

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeColumns(&ctx.StmtCtx, &ts.LocalTables[i], t, ts, ss)

	}

	ss.Create = create

}

// merge remote schema with local
func Merge(
	mrg *StmtCtx,
	tableCtx *rdbms.MergeTableCtx,
	typeCtx *MergeTypeCtx) (string, error) {

	ss := rdbms.MergeScriptCtx{}

	drop := ""
	create := ""

	ss.Create = &create
	ss.Drop = &drop

	if typeCtx != nil && typeCtx.localTypes != nil {
		MergeTypes(mrg, typeCtx, tableCtx, &ss)
	}

	MergeTables(mrg, tableCtx, &ss)

	cmd := ""
	cmd += drop
	cmd += create

	return cmd, nil
}
