package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

const (
	F_JSON = iota
	F_SQL  = iota
)

func DpExt(f uint) string {
	switch f {
	case F_JSON:
		fallthrough
	default:
		return ".json"
	case F_SQL:
		return ".sql"
	}
}

func DpTableString(dbms Rdbms, t Table, f uint) ([]byte, error) {
	switch f {
	case F_JSON:
		fallthrough
	default:
		return json.MarshalIndent(t, "", "\t")
	case F_SQL:
		b := dbms.TableDef(t)
		return []byte(b), nil
	}
}

func DpStdoutWriteTables(dbms Rdbms, tables []Table, dir string, format uint) error {
	if dir != "" {
		fs, err := os.Stat(dir)
		if err != nil && os.IsNotExist(err) {
			return err
		}
		if !fs.IsDir() {
			return fmt.Errorf(dir + " is not directory")
		}
		for i := 0; i < len(tables); i++ {
			if tables[i].Name == "" {
				return fmt.Errorf("empty table name")
			}
			ext := DpExt(format)
			f, err := os.Create(path.Join(dir, tables[i].Name+ext))
			if err != nil {
				return err
			}
			def, err := DpTableString(dbms, tables[i], format)
			if err != nil {
				return err
			}
			_, err = f.Write(def)
			if err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < len(tables); i++ {
			var def []byte
			def, err := DpTableString(dbms, tables[i], format)
			if err != nil {
				return err
			}
			fmt.Println(string(def))
		}
	}
	return nil
}

func DpExecuteCmdsVerbose(db *sql.DB, cmds string) error {
	cc := strings.Split(cmds, ";\n")
	for i := 0; i < len(cc); i++ {
		if cc[i] == "" {
			continue
		}
		fmt.Println(cc[i])
		start := time.Now()
		_, err := db.Exec(cc[i])
		if err != nil {
			fmt.Println("\033[4;31mError " + err.Error() + "\033[0m")
			return fmt.Errorf("")
		}
		elapsed := time.Since(start)
		fmt.Println("\033[4;32mQuery completed in " + elapsed.String() + "\033[0m")
	}
	return nil
}

func DpExecuteCmds(db *sql.DB, cmds string) error {
	cc := strings.Split(cmds, ";\n")
	for i := 0; i < len(cc); i++ {
		_, err := db.Exec(cc[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func DpExecuteFile(db *sql.DB, spec *ScriptSpec, e bool) error {

	b, err := ioutil.ReadFile(spec.Path)
	if err != nil {
		return err
	}

	script := string(b)

	for key, val := range spec.Values {
		script = strings.Replace(script, "@"+key+"@", val, -1)
	}

	if e {
		_, err = db.Exec(script)
		if err != nil {
			return err
		}
	}

	return nil
}

func DpExecuteSpec(db *sql.DB, spec *ScriptSpec, e bool) error {
	fi, err := os.Lstat(spec.Path)
	if err != nil {
		return err
	}

	if fi.IsDir() {
		files, err := ioutil.ReadDir(spec.Path)
		if err != nil {
			return err
		}
		for i := 0; i < len(files); i++ {
			newpath := path.Join(spec.Path, files[i].Name())
			newspec := ScriptSpec{newpath, spec.Values}
			if err = DpExecuteFile(db, &newspec, e); err != nil {
				return err
			}
		}
	} else {
		return DpExecuteFile(db, spec, e)
	}

	return nil
}

func Program() error {

	p := flag.String("p", "merge", "profile. to be executed: merge, import")
	f := flag.Uint("f", F_SQL, "available import formats: json="+strconv.Itoa(F_JSON)+", sql="+strconv.Itoa(F_SQL))
	c := flag.String("c", "main.conf", "config path")
	e := flag.Bool("e", false, "execute, if not specified then this is dry run")
	v := flag.Bool("v", false, "verbose - report progress as program runs")

	flag.Parse()

	var dbms Rdbms = RdbmsMssql()

	conf := ConfNew()
	if err := ConfInit(conf, *c); err != nil {
		return err
	}

	path, err := conf.Get("tables")
	if err != nil {
		return err
	}

	t, err := ReadTables(path)
	if err != nil {
		return err
	}

	cs, err := conf.Cs()
	if err != nil {
		return err
	}

	db, err := sql.Open(
		"sqlserver",
		cs)
	if err != nil {
		return err
	}

	defer db.Close()

	if conf.Before != nil {
		for i := 0; i < len(conf.Before); i++ {
			beforeSpec, err := ConfScriptSpec(conf.Before[i])
			if err != nil {
				return err
			}
			if err := DpExecuteSpec(db, beforeSpec, *e); err != nil {
				return err
			}
		}
	}

	switch *p {
	case "merge":
		s, err := MergeRdbmsTables(dbms, db, t)
		if err != nil {
			return err
		}

		if *e {
			if *v {
				start := time.Now()
				if err = DpExecuteCmdsVerbose(db, s); err != nil {
					fmt.Println("\033[0;31mcouldnt complete deploy. statements remaining:\033[0m")
					s, err := MergeRdbmsTables(dbms, db, t)
					if err != nil {
						return err
					}
					fmt.Print(s)
					return err
				}
				elapsed := time.Since(start)
				fmt.Printf("Completed in %s. Database is up to date\n", elapsed.String())
			} else {
				if err = DpExecuteCmds(db, s); err != nil {
					return err
				}
			}
		} else {
			fmt.Print(s)
		}
	case "import":
		tbls, err := RdbmsGetAllTables(dbms, db)
		if err != nil {
			return err
		}
		if *e {
			err := DpStdoutWriteTables(dbms, tbls, path, *f)
			if err != nil {
				return err
			}
		} else {
			for i := 0; i < len(tbls); i++ {
				bf, err := DpTableString(dbms, tbls[i], *f)
				if err != nil {
					return err
				}
				fmt.Println(string(bf))
			}
			if *v {
				fmt.Println("will be written into " + path)
			}
		}
	default:
		flag.Usage()
		return fmt.Errorf("invalid profile specified: " + *p)
	}

	if conf.After != nil {
		for i := 0; i < len(conf.After); i++ {
			s, err := ConfScriptSpec(conf.After[i])
			if err != nil {
				return err
			}
			if err := DpExecuteSpec(db, s, *e); err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	err := Program()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
