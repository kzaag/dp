package rdbms

import (
	"database/sql"
	"fmt"
	"path"
	"strings"

	"github.com/kzaag/dp/cmn"
)

type TargetCtx struct {
	GetMergeScript func(*sql.DB, *cmn.Target, []string) (string, error)
	GetDB          func(*cmn.Target) (*sql.DB, error)
}

func targetFixAbsPaths(basepath string, paths []string) {
	var i int
	var p *string
	for i = 0; i < len(paths); i++ {
		p = &paths[i]
		if !path.IsAbs(*p) {
			*p = path.Join(basepath, *p)
		}
	}
}

func TargetRunSingle(ctx *TargetCtx, basepath string, target *cmn.Target, uargv *cmn.Args) error {
	var err error
	var db *sql.DB
	var userExecute bool

	if db, err = ctx.GetDB(target); err != nil {
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
			targetFixAbsPaths(basepath, e.Args)
			script := ""
			script, err = ctx.GetMergeScript(db, target, e.Args)
			if err != nil {
				break
			}
			cc := strings.Split(script, ";\n")
			_, err = ExecLines(db, cc, uargv)
		case "stmt":
			_, err = ExecLines(db, e.Args, uargv)
		case "script":
			targetFixAbsPaths(basepath, e.Args)
			err = ExecPaths(db, e.Args, uargv)
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

func TargetRunFromConfig(ctx *TargetCtx, config *cmn.Config, uargs *cmn.Args) error {
	var err error
	for _, target := range config.Targets {
		if err = TargetRunSingle(ctx, config.Base, target, uargs); err != nil {
			return err
		}
	}
	return err
}
