package pgsql

import (
	"database-project/config"
	"database-project/rdbms"
	"database/sql"
	"fmt"
	"path"
	"strings"
)

func __DriverGetMergeScript(db *sql.DB, ectx *config.Target, args []string) (string, error) {
	ts := rdbms.MergeTableCtx{}
	tt := MergeTypeCtx{}
	ctx := StmtNew()
	var mergeScript string
	var err error
	var dd []DDObject

	for _, arg := range args {
		if dd, err = ParserGetObjectsInDir(ctx, arg); err != nil {
			return "", err
		}
		for _, obj := range dd {
			if obj.Table != nil {
				ts.LocalTables = append(ts.LocalTables, *obj.Table)
			} else {
				tt.localTypes = append(tt.localTypes, *obj.Type)
			}
		}
	}

	if ts.RemoteTables, err = RemoteGetMatchedTables(db, ts.LocalTables); err != nil {
		return "", err
	}

	if tt.remoteTypes, err = RemoteGetTypes(db, tt.localTypes); err != nil {
		return "", err
	}

	if mergeScript, err = Merge(ctx, &ts, &tt); err != nil {
		return "", err
	}

	return mergeScript, nil
}

func __DriverFixAbsPaths(basepath string, paths []string) {
	var i int
	var p *string
	for i = 0; i < len(paths); i++ {
		p = &paths[i]
		if !path.IsAbs(*p) {
			*p = path.Join(basepath, *p)
		}
	}
}

func DriverRunTarget(basepath string, ectx *config.Target) error {
	var err error
	var db *sql.DB

	if db, err = CSCreateDBfromConfig(ectx); err != nil {
		return err
	}

	defer db.Close()

	execParams := rdbms.ExecParams{}
	execParams.Exec = true
	execParams.Verb = true

	for i, e := range ectx.Exec {
		if execParams.Verb {
			fmt.Printf("\033[33m\n%s %s@%s %d (%s)\033[0m\n\n",
				ectx.Name,
				ectx.Database,
				ectx.Server,
				i, e.Type)
		}
		switch e.Type {
		case "merge":
			__DriverFixAbsPaths(basepath, e.Args)
			script := ""
			script, err = __DriverGetMergeScript(db, ectx, e.Args)
			if err != nil {
				break
			}
			cc := strings.Split(script, ";\n")
			_, err = rdbms.ExecLines(db, cc, execParams)
		case "stmt":
			_, err = rdbms.ExecLines(db, e.Args, execParams)
		case "script":
			__DriverFixAbsPaths(basepath, e.Args)
			err = rdbms.ExecPaths(db, e.Args, execParams)
		}
		if err != nil {
			switch e.Err {
			case "ignore":
				break
			case "warn":
				fmt.Printf("\033[4;31mQuery completed with errors: %s.\033[0m\n", err.Error())
			case "raise":
			case "":
				return err
			}
		}
	}
	return nil
}
