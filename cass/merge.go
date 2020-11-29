package cass

import (
	"strings"
)

type MergeTableCtx struct {
	RemoteTables map[string]*Table
	LocalTables  map[string]*Table
}

func MergeAddOperation(dest *string, elem string) {
	if !strings.Contains(*dest, elem) {
		*dest += elem
	}
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
func MergeCmpPK(p1, p2 *PrimaryKey) bool {
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

// func MergeDropColumnRefs(
// 	stmt *StmtCtx,
// 	sctx *MergeScriptCtx,

// ) {

// }

func MergeColumns(
	stmt *StmtCtx,
	sctx *MergeScriptCtx,
	lt, rt *Table,
	tctx *MergeTableCtx,
	vctx *MergeViewCtx,
) {
	// iterate on local columns
	for lc := range lt.Columns {
		if rc, ok := rt.Columns[lc]; ok {
			// if found matching column then alter
			// (returns empty string if columns are the same)
			MergeAddOperation(sctx.Create, stmt.AlterColumn(lt.Name, lt.Columns[lc], rc))
		} else {
			// if you didnt find matching column on remote then create it
			MergeAddOperation(sctx.Create, stmt.AddColumn(lt.Name, lt.Columns[lc]))
		}
	}

	// iterate on remote columns
	for rc := range rt.Columns {
		if _, ok := lt.Columns[rc]; !ok {
			// if local schema doesnt contain remote column
			// then drop it from database
			droppedViews := make([]string, 0, 10)
			for rvk := range vctx.RemoteViews {
				if vctx.RemoteViews[rvk].Base != lt.Name {
					continue
				}
				MergeAddOperation(sctx.Drop, stmt.DropMaterializedView(vctx.RemoteViews[rvk]))
				droppedViews = append(droppedViews, rvk)
			}
			MergeAddOperation(sctx.Drop, stmt.DropColumn(lt.Name, rt.Columns[rc]))
			for _, dvk := range droppedViews {
				if dv, ok := vctx.LocalViews[dvk]; ok {
					MergeAddOperation(sctx.Create, stmt.CreateMaterializedView(dv))
				}
			}
		}
	}
}

// true if ix is the same
func MergeCmpSASIIndex(l, r *SASIIndex) bool {
	return l.Column == r.Column
}

func MergeSASIIndexes(
	stmt *StmtCtx,
	sctx *MergeScriptCtx,
	lt, rt *Table,
) {
	for lixName := range lt.SASIIndexes {
		lix := lt.SASIIndexes[lixName]
		if rix, ok := rt.SASIIndexes[lixName]; ok {
			if !MergeCmpSASIIndex(lix, rix) {
				// if indexes differ we will recreate them
				MergeAddOperation(sctx.Drop, stmt.DropIndex(lix.Name))
				MergeAddOperation(sctx.Create, stmt.CreateSASIIndex(rt.Name, rix))
			}
		} else {
			MergeAddOperation(sctx.Create, stmt.CreateSASIIndex(lt.Name, lix))
		}
	}
	for rixName := range rt.SASIIndexes {
		if _, ok := lt.SASIIndexes[rixName]; !ok {
			MergeAddOperation(sctx.Drop, stmt.DropIndex(rixName))
		}
	}
}

func MergeTables(
	stmt *StmtCtx,
	tctx *MergeTableCtx,
	sctx *MergeScriptCtx,
	vctx *MergeViewCtx,
) {
	for k := range tctx.LocalTables {
		lt := tctx.LocalTables[k]
		if rt, ok := tctx.RemoteTables[k]; ok {
			// remote table exists

			// if primary key differs
			// then we dont have other choice than to recreate the table
			if MergeCmpPK(lt.PrimaryKey, rt.PrimaryKey) {
				MergeColumns(stmt, sctx, lt, rt, tctx, vctx)
				MergeSASIIndexes(stmt, sctx, lt, rt)
			} else {
				for ixk := range lt.SASIIndexes {
					MergeAddOperation(sctx.Drop, stmt.DropIndex(lt.SASIIndexes[ixk].Name))
				}
				MergeAddOperation(sctx.Drop, stmt.DropTable(lt))
				MergeAddOperation(sctx.Create, stmt.CreateTable(rt))
				for ixk := range rt.SASIIndexes {
					ix := rt.SASIIndexes[ixk]
					MergeAddOperation(sctx.Create, stmt.CreateSASIIndex(k, ix))
				}
			}
		} else {
			// remote table doesnt exist
			MergeAddOperation(sctx.Create, stmt.CreateTable(lt))
			for ixk := range lt.SASIIndexes {
				ix := lt.SASIIndexes[ixk]
				MergeAddOperation(sctx.Create, stmt.CreateSASIIndex(k, ix))
			}
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
			if !MergeCmpPK(lv.PrimaryKey, rv.PrimaryKey) {
				MergeAddOperation(sctx.Drop, stmt.DropMaterializedView(lv))
				MergeAddOperation(sctx.Create, stmt.CreateMaterializedView(rv))
			}
		} else {
			// remote table doesnt exist
			MergeAddOperation(sctx.Create, stmt.CreateMaterializedView(lv))
		}
	}
	for k := range vctx.RemoteViews {
		rv := vctx.RemoteViews[k]
		if _, ok := vctx.LocalViews[k]; !ok {
			MergeAddOperation(sctx.Drop, stmt.DropMaterializedView(rv))
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

	MergeTables(stmt, tctx, &sctx, vctx)

	MergeViews(stmt, vctx, &sctx)

	cmd := ""
	cmd += drop
	cmd += create

	return cmd
}
