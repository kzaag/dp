package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"
)

type ExecParams struct {
	exec bool
	verb bool
}

func ExecCmdsVerbose(db *sql.DB, cmds string) (int, int, error) {
	cc := strings.Split(cmds, ";\n")
	fmt.Println()
	all := 0
	for i := 0; i < len(cc); i++ {
		if cc[i] == "" {
			continue
		}
		all++
		fmt.Println(cc[i])
		start := time.Now()
		_, err := db.Exec(cc[i])
		if err != nil {
			fmt.Println("\033[4;31mError " + err.Error() + "\033[0m")
			return i, len(cc), fmt.Errorf("")
		}
		elapsed := time.Since(start)
		fmt.Println("\033[4;32mQuery completed in " + elapsed.String() + "\033[0m\n")
	}
	return all, all, nil
}

func ExecCmds(db *sql.DB, cmds string) error {
	cc := strings.Split(cmds, ";\n")
	for i := 0; i < len(cc); i++ {
		_, err := db.Exec(cc[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func ExecFile(db *sql.DB, filePath string, params ExecParams) error {

	b, err := ioutil.ReadFile(spec.Path)
	if err != nil {
		return err
	}

	script := string(b)
	var t time.Time

	if params.exec {
		t = time.Now()
		_, err = db.Exec(script)
		if err != nil {
			return fmt.Errorf("in %s: %s", filePath, err.Error())
		}
	}

	if params.verb {
		fmt.Print(filePath)
		if uc.exec {
			el := time.Since(t)
			fmt.Printf(" in %s", el.String())
		}
		fmt.Println()
	}

	return nil
}

func ExecDir(db *sql.DB, dirPath string, params ExecParams) error {
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

			if err = ExecDir(db, newpath, params); err != nil {
				return err
			}

		} else {

			if !strings.HasSuffix(files[i].Name(), ".sql") {
				continue
			}

			if err = ExecFile(db, newpath, params); err != nil {
				return err
			}

		}
	}
	return nil
}

func ExecPath(db *sql.DB, path string, params ExecParams) error {
	fi, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return ExecDir(db, path, params)
	} else {
		return ExecFile(db, path, params)
	}
}
