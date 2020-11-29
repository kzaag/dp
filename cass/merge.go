package cass

type MergeTableCtx struct {
	RemoteTables map[string]*Table
	LocalTables  map[string]*Table
}

func MergeTableCtxNew() *MergeTableCtx {
	ret := new(MergeTableCtx)
	ret.LocalTables = make(map[string]*Table)
	ret.RemoteTables = make(map[string]*Table)
	return ret
}

type MergeViewCtx struct {
	RemoteViews map[string]*MaterializedView
	LocalViews  map[string]*MaterializedView
}

func MergeViewCtxNew() *MergeViewCtx {
	ret := new(MergeViewCtx)
	ret.LocalViews = make(map[string]*MaterializedView)
	ret.RemoteViews = make(map[string]*MaterializedView)
	return ret
}

type MergeScriptCtx struct {
	Drop   *string
	Create *string
}

// true if is the same
func MergeCmdPK(p1, p2 *PrimaryKey) bool {
	if len(p1.ClusteringColumns) != len(p2.ClusteringColumns) {
		return false
	}
	if len(p1.PartitionColumns) != len(p2.PartitionColumns) {
		return false
	}
	for i := range p1.ClusteringColumns {
		if p2.ClusteringColumns[i].Name != p1.ClusteringColumns[i].Name {
			return false
		}
		if p2.ClusteringColumns[i].Order != p1.ClusteringColumns[i].Order {
			return false
		}
	}
	for i := range p1.PartitionColumns {
		if p2.PartitionColumns[i].Name != p1.PartitionColumns[i].Name {
			return false
		}
	}
	return true
}

func MergeColumns(
	stmt *StmtCtx,
	sctx *MergeScriptCtx,
	lt, rt *Table,
) {
	// iterate on local columns
	for lc := range lt.Columns {
		if rc, ok := rt.Columns[lc]; ok {
			// if found matching column then alter
			// (returns empty string if columns are the same)
			*sctx.Create += stmt.AlterColumn(lt.Name, lt.Columns[lc], rc)
		} else {
			// if you didnt find matching column on remote then create it
			*sctx.Create += stmt.AddColumn(lt.Name, lt.Columns[lc])
		}
	}

	// iterate on remote columns
	for rc := range rt.Columns {
		if _, ok := lt.Columns[rc]; !ok {
			// if local schema doesnt contain remote column
			// then drop it from database
			*sctx.Drop += stmt.DropColumn(lt.Name, rt.Columns[rc])
		}
	}
}

func MergeTables(
	stmt *StmtCtx,
	tctx *MergeTableCtx,
	sctx *MergeScriptCtx,
) {
	for k := range tctx.LocalTables {
		lt := tctx.LocalTables[k]
		if rt, ok := tctx.RemoteTables[k]; ok {
			// remote table exists

			// if primary key differs
			// then we dont have other choice than to recreate the table
			if MergeCmdPK(lt.PrimaryKey, rt.PrimaryKey) {

				MergeColumns(stmt, sctx, lt, rt)

			} else {
				*sctx.Drop += stmt.DropTable(lt)
				*sctx.Create += stmt.CreateTable(rt)
			}
		} else {
			// remote table doesnt exist
			*sctx.Create += stmt.CreateTable(tctx.LocalTables[k])
		}
	}
}

func MergeViews(
	stmt *StmtCtx,
	vctx *MergeViewCtx,
	sctx *MergeScriptCtx,
) {
	for k := range vctx.LocalViews {
		lv := vctx.LocalViews[k]
		if rv, ok := vctx.RemoteViews[k]; ok {
			if !MergeCmdPK(lv.PrimaryKey, rv.PrimaryKey) {
				*sctx.Drop += stmt.DropMaterializedView(lv)
				*sctx.Create += stmt.CreateMaterializedView(rv)
			}
		} else {
			// remote table doesnt exist
			*sctx.Create += stmt.CreateMaterializedView(lv)
		}
	}
	for k := range vctx.RemoteViews {
		rv := vctx.RemoteViews[k]
		if _, ok := vctx.LocalViews[k]; !ok {
			*sctx.Drop += stmt.DropMaterializedView(rv)
		}
	}
}

func Merge(
	stmt *StmtCtx,
	tctx *MergeTableCtx,
	vctx *MergeViewCtx,
) string {
	sctx := MergeScriptCtx{}
	drop := ""
	create := ""
	sctx.Create = &create
	sctx.Drop = &drop

	MergeTables(stmt, tctx, &sctx)

	MergeViews(stmt, vctx, &sctx)

	cmd := ""
	cmd += drop
	cmd += create

	return cmd
}
