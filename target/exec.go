package target

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/kzaag/dp/cmn"
)

func (ctx *Ctx) ExecLines(
	db interface{},
	cc []string,
	uargv *Args,
) (int, error) {
	done := 0
	var start time.Time
	var err error

	for i := 0; i < len(cc); i++ {
		if strings.TrimSpace(cc[i]) == "" {
			continue
		}
		if uargv.Verbose {
			fmt.Println(cc[i])
			start = time.Now()
		}
		if uargv.Execute {
			err = ctx.DbExec(db, cc[i])
		} else if ctx.DbPing != nil {
			err = ctx.DbPing(db)
		}
		if err != nil {
			return done, err
		}
		if uargv.Verbose && uargv.Execute {
			elapsed := time.Since(start)
			cmn.CndPrintfln(
				uargv.Raw,
				cmn.PrintflnSuccess,
				"Query completed in %v.", elapsed)
		}
		done++
	}

	if done == 0 {
		cmn.CndPrintln(
			uargv.Raw,
			cmn.PrintflnSuccess,
			"Already up to date.")
	}

	return done, nil
}

func (ctx *Ctx) ExecFile(
	db interface{}, filePath string, uargv *Args,
) error {
	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	script := string(b)
	var t time.Time

	if uargv.Verbose {
		t = time.Now()
		fmt.Println(filePath)
	}

	if uargv.Execute {
		err = ctx.DbExec(db, script)
		if err != nil {
			return err
		}
	}

	if uargv.Verbose && uargv.Execute {
		el := time.Since(t)
		cmn.CndPrintfln(
			uargv.Raw,
			cmn.PrintflnSuccess,
			"Query completed in %v.", el)
	}

	return nil
}

func (ctx *Ctx) ExecDir(
	db interface{}, dirPath string, uargv *Args,
) error {

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for i := 0; i < len(files); i++ {
		newpath := path.Join(dirPath, files[i].Name())
		fi, err := os.Lstat(newpath)
		if err != nil {
			return err
		}
		if fi.IsDir() {

			if err = ctx.ExecDir(db, newpath, uargv); err != nil {
				return err
			}

		} else {

			if !strings.HasSuffix(files[i].Name(), ctx.DbSuffix) {
				continue
			}

			if err = ctx.ExecFile(db, newpath, uargv); err != nil {
				return err
			}

		}
	}
	return nil
}

func (ctx *Ctx) ExecPath(
	db interface{}, path string, uargv *Args,
) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return ctx.ExecDir(db, path, uargv)
	} else {
		return ctx.ExecFile(db, path, uargv)
	}
}

func (ctx *Ctx) ExecPaths(
	db interface{}, paths []string, uargv *Args,
) error {
	var err error
	for i := 0; i < len(paths); i++ {
		if err = ctx.ExecPath(db, paths[i], uargv); err != nil {
			return err
		}
	}
	return err
}
