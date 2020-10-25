package mssql

import "database-project/rdbms"

func Merge(ctx *rdbms.StmtCtx, ts *rdbms.MergeTableCtx) (string, error) {

	ss := &rdbms.MergeScriptCtx{}

	var devnull string

	for i := 0; i < len(ts.LocalTables); i++ {
		if t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables); t == nil {
			*ss.Create += ctx.CreateTable(&ts.LocalTables[i])
		}
	}

	drop := ss.Drop
	ss.Drop = &devnull

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeColumns(ctx, &ts.LocalTables[i], t, ts, ss)

	}

	ss.Drop = drop

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergePrimary(ctx, &ts.LocalTables[i], t, ss, ts)

	}

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeUnique(ctx, &ts.LocalTables[i], t, ss)

	}

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeFK(ctx, &ts.LocalTables[i], t, ss)
		rdbms.MergeCheck(ctx, &ts.LocalTables[i], t, ss)
		rdbms.MergeIx(ctx, &ts.LocalTables[i], t, ss)

	}

	create := ss.Create
	ss.Create = &devnull

	for i := 0; i < len(ts.LocalTables); i++ {

		t := rdbms.MergeFindTable(ts.LocalTables[i].Name, ts.RemoteTables)
		rdbms.MergeColumns(ctx, &ts.LocalTables[i], t, ts, ss)

	}

	ss.Create = create

	cmd := ""
	cmd += *ss.Drop
	cmd += *ss.Create

	return cmd, nil
}
