package rdbms

import (
	"database-project/cmn"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

func ExecLines(db *sql.DB, cc []string, uargv *cmn.Args) (int, error) {
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
			_, err = db.Exec(cc[i])
		} else {
			err = db.Ping()
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

func ExecFile(db *sql.DB, filePath string, uargv *cmn.Args) error {

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
		_, err = db.Exec(script)
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

func ExecDir(db *sql.DB, dirPath string, uargv *cmn.Args) error {
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

			if err = ExecDir(db, newpath, uargv); err != nil {
				return err
			}

		} else {

			if !strings.HasSuffix(files[i].Name(), ".sql") {
				continue
			}

			if err = ExecFile(db, newpath, uargv); err != nil {
				return err
			}

		}
	}
	return nil
}

func ExecPath(db *sql.DB, path string, uargv *cmn.Args) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return ExecDir(db, path, uargv)
	} else {
		return ExecFile(db, path, uargv)
	}
}

func ExecPaths(db *sql.DB, paths []string, uargv *cmn.Args) error {
	var err error
	for i := 0; i < len(paths); i++ {
		if err = ExecPath(db, paths[i], uargv); err != nil {
			return err
		}
	}
	return err
}
