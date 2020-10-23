package rdbms

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
	Exec bool
	Verb bool
}

// func ExecCmdsVerbose(db *sql.DB, cmds string) (int, int, error) {
// 	cc := strings.Split(cmds, ";\n")
// 	fmt.Println()
// 	all := 0
// 	for i := 0; i < len(cc); i++ {
// 		if cc[i] == "" {
// 			continue
// 		}
// 		all++
// 		fmt.Println(cc[i])
// 		start := time.Now()
// 		_, err := db.Exec(cc[i])
// 		if err != nil {
// 			fmt.Println("\033[4;31mError " + err.Error() + "\033[0m")
// 			return i, len(cc), fmt.Errorf("")
// 		}
// 		elapsed := time.Since(start)
// 		fmt.Println("\033[4;32mQuery completed in " + elapsed.String() + "\033[0m\n")
// 	}
// 	return all, all, nil
// }

// func ExecCmds(db *sql.DB, cmds string) error {
// 	cc := strings.Split(cmds, ";\n")
// 	for i := 0; i < len(cc); i++ {
// 		_, err := db.Exec(cc[i])
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

func ExecLines(db *sql.DB, cc []string, params ExecParams) (int, error) {
	done := 0
	var start time.Time
	var err error

	for i := 0; i < len(cc); i++ {
		if strings.TrimSpace(cc[i]) == "" {
			continue
		}
		if params.Verb {
			fmt.Printf("%s\n", cc[i])
			start = time.Now()
		}
		if params.Exec {
			_, err = db.Exec(cc[i])
		} else {
			err = db.Ping()
		}
		if err != nil {
			return done, err
		}
		if params.Verb {
			elapsed := time.Since(start)
			if params.Exec {
				fmt.Printf(
					"\033[4;32mQuery completed in %s.\033[0m\n\n",
					elapsed.String())
			}
		}
		done++
	}

	return done, nil
}

func ExecFile(db *sql.DB, filePath string, params ExecParams) error {

	b, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	script := string(b)
	var t time.Time

	if params.Exec {
		t = time.Now()
		_, err = db.Exec(script)
		if err != nil {
			return fmt.Errorf("in %s: %s", filePath, err.Error())
		}
	}

	if params.Verb {
		fmt.Print(filePath)
		if params.Exec {
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

func ExecPaths(db *sql.DB, paths []string, params ExecParams) error {
	var err error
	for i := 0; i < len(paths); i++ {
		if err = ExecPath(db, paths[i], params); err != nil {
			return err
		}
	}
	return err
}
