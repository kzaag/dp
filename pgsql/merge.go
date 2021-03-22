package pgsql

// merge remote schema with local
func Merge(
	mrg *StmtCtx,
	tableCtx *MergeTableCtx,
	typeCtx *MergeTypeCtx) (string, error) {

	ss := MergeScriptCtx{}

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
