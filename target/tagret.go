package target

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/kzaag/dp/cmn"
)

type Target struct {
	ConnectionString string
	Server           []string
	Database         string
	User             string
	Password         string
	Args             map[string]string
	Exec             []Exec
	Name             string
	OnDemand         bool `yaml:"on_demand"`
}

func (t *Target) GetInt(name string, val *int) error {
	var ok bool
	var v string
	var err error
	if v, ok = t.Args[name]; !ok {
		return nil
	}
	if *val, err = strconv.Atoi(v); err != nil {
		return fmt.Errorf("target.GetInt(%s) couldnt parse: %v", name, err)
	}
	return err
}

type Exec struct {
	Type    string
	Args    []string
	Err     string
	Execute bool
}

type Ctx struct {
	GetMergeScript func(dbCtx interface{}, target *Target, args []string) (string, error)
	DbNew          func(*Target) (interface{}, error)
	DbClose        func(dbCtx interface{})
	DbPing         func(dbCtx interface{}) error
	DbExec         func(dbCtx interface{}, stmt string, args ...interface{}) error
	DbSuffix       string
}

func fixAbsPaths(basepath string, paths []string) {
	var i int
	var p *string
	for i = 0; i < len(paths); i++ {
		p = &paths[i]
		if !path.IsAbs(*p) {
			*p = path.Join(basepath, *p)
		}
	}
}

func (ctx *Ctx) ExecTarget(
	basepath string,
	target *Target,
	uargv *Args,
) error {
	var err error
	var db interface{}
	var userExecute bool

	if target.OnDemand {
		if _, ok := uargv.Demand[target.Name]; !ok {
			return nil
		}
	}

	if db, err = ctx.DbNew(target); err != nil {
		return err
	}

	defer ctx.DbClose(db)

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
				"",
				"%v%s %s@%s %d/%s%v",
				cmn.ForeBlue,
				target.Name,
				target.Database,
				target.Server[0],
				i, e.Type,
				cmn.AttrOff)
		}

		switch e.Type {
		case "merge":
			fixAbsPaths(basepath, e.Args)
			script := ""
			script, err = ctx.GetMergeScript(db, target, e.Args)
			if err != nil {
				break
			}
			cc := strings.Split(script, ";\n")
			_, err = ctx.ExecLines(db, cc, uargv)
		case "stmt":
			_, err = ctx.ExecLines(db, e.Args, uargv)
		case "script":
			fixAbsPaths(basepath, e.Args)
			err = ctx.ExecPaths(db, e.Args, uargv)
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
				cmn.CndPrintfln(uargv.Raw, cmn.PrintflnWarn, "    ", "%v.", err)
			case "warn abort":
				cmn.CndPrintfln(uargv.Raw, cmn.PrintflnWarn, "    ", "%v.", err)
				return nil
			default:
				cmn.CndPrintError(uargv.Raw, err)
				return fmt.Errorf("Terminated execution due to errors")
			}
		}
	}
	return nil
}

func (ctx *Ctx) ExecConfig(config *Config, uargs *Args) error {
	var err error
	for _, target := range config.Targets {
		if err = ctx.ExecTarget(config.Base, target, uargs); err != nil {
			return err
		}
	}
	return err
}
