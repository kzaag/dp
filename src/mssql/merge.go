package mssql

func SqlMergeTables(mrg *SqlMergeCtx) {

	var devnull string

	for i := 0; i < len(mrg.localTables); i++ {
		if t := SqlMergeFindTable(mrg.localTables[i].Name, mrg.remTables); t == nil {
			*mrg.create += mrg.CreateTable(&mrg.localTables[i])
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
