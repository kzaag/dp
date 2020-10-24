package pgsql

import (
	"database-project/cmn"
	"database-project/rdbms"
	"database/sql"
	"fmt"
	"path"
	"strings"
)

func __TargetGetMergeScript(db *sql.DB, ectx *cmn.Target, args []string) (string, error) {
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

func __TargetFixAbsPaths(basepath string, paths []string) {
	var i int
	var p *string
	for i = 0; i < len(paths); i++ {
		p = &paths[i]
		if !path.IsAbs(*p) {
			*p = path.Join(basepath, *p)
		}
	}
}

func TargetRunSingle(basepath string, target *cmn.Target, uargv *cmn.Args) error {
	var err error
	var db *sql.DB
	var userExecute bool

	if db, err = CSCreateDBfromConfig(target); err != nil {
		return err
	}

	defer db.Close()

	for i, e := range target.Exec {

		if e.Execute {
			/* override user execute */
			userExecute = uargv.Execute
			uargv.Execute = e.Execute
		}

		if uargv.Verbose {
			cmn.CndPrintfln(
				uargv.Raw,
				cmn.PrintflnNotify,
				"TARGET %s: %s:%s@%s exec: %d/%s",
				target.Name, target.User, target.Database, target.Server, i, e.Type)
		}

		switch e.Type {
		case "merge":
			__TargetFixAbsPaths(basepath, e.Args)
			script := ""
			script, err = __TargetGetMergeScript(db, target, e.Args)
			if err != nil {
				break
			}
			cc := strings.Split(script, ";\n")
			_, err = rdbms.ExecLines(db, cc, uargv)
		case "stmt":
			_, err = rdbms.ExecLines(db, e.Args, uargv)
		case "script":
			__TargetFixAbsPaths(basepath, e.Args)
			err = rdbms.ExecPaths(db, e.Args, uargv)
		}

		if e.Execute {
			/* restore user execute */
			uargv.Execute = userExecute
		}

		if err != nil {
			switch e.Err {
			case "ignore":
				break
			case "warn":
				cmn.CndPrintfln(uargv.Raw, cmn.PrintflnWarn, "%v.", err)
			default:
				cmn.CndPrintError(uargv.Raw, err)
				return fmt.Errorf("Terminated execution due to errors")
			}
		}
	}
	return nil
}

func TargetRunFromConfig(config *cmn.Config, uargs *cmn.Args) error {
	var err error
	for _, target := range config.Targets {
		if err = TargetRunSingle(config.Base, target, uargs); err != nil {
			return err
		}
	}
	return err
}
