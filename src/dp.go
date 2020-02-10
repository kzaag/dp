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
	_ "github.com/lib/pq"
)

// formats
const (
	F_JSON = iota
	F_SQL  = iota
)

type DpUserConf struct {
	profile string
	format  uint
	config  string
	exec    bool
	verb    bool
}

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

func DpTableString(remote *Remote, t *Table, f uint) ([]byte, error) {
	switch f {
	case F_JSON:
		fallthrough
	default:
		return json.MarshalIndent(t, "", "\t")
	case F_SQL:
		b := RemoteTableDef(remote, t)
		return []byte(b), nil
	}
}

func DpStdoutWriteTables(remote *Remote, tables []Table, dir string, format uint) error {
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
			def, err := DpTableString(remote, &tables[i], format)
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
			def, err := DpTableString(remote, &tables[i], format)
			if err != nil {
				return err
			}
			fmt.Println(string(def))
		}
	}
	return nil
}

func DpExecuteCmdsVerbose(db *sql.DB, cmds string) (int, int, error) {
	cc := strings.Split(cmds, ";\n")
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
		fmt.Println("\033[4;32mQuery completed in " + elapsed.String() + "\033[0m")
	}
	return all, all, nil
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

func DpGetUserConf() *DpUserConf {
	var c DpUserConf

	flag.StringVar(&c.profile, "p", "merge", "profile. to be executed: merge, import")
	flag.UintVar(&c.format, "f", F_SQL, "available import formats: json="+strconv.Itoa(F_JSON)+", sql="+strconv.Itoa(F_SQL))
	flag.StringVar(&c.config, "c", "", "config path. defaults to first config beginning with main.* ex. main.mssql.conf")
	flag.BoolVar(&c.exec, "e", false, "execute, if not specified then this is dry run")
	flag.BoolVar(&c.verb, "v", false, "verbose - report progress as program runs")

	flag.Parse()

	return &c
}

func DpRemoteInit(conf *Config) (*Remote, error) {

	driver, err := conf.Get("driver")
	if err != nil {
		return nil, err
	}

	var cs string
	switch driver {
	case "postgres":
		cs, err = conf.PgCs()
		if err != nil {
			return nil, err
		}
	case "sqlserver":
		cs, err = conf.SqlCs()
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("driver is not supported")
	}

	db, err := sql.Open(driver, cs)

	if err != nil {
		return nil, err
	}

	var remote *Remote
	switch driver {
	case "postgres":
		remote = RemotePgsql(db)
	case "sqlserver":
		remote = RemoteMssql(db)
	}

	return remote, nil
}

func DpExecuteImport(c *Config, uc *DpUserConf, remote *Remote) error {
	panic("not implemented")
	// tbls, err := RemoteGetAllTables(remote)
	// if err != nil {
	// 	return err
	// }
	// if *e {
	// 	err := DpStdoutWriteTables(remote, tbls, tablepath, *f)
	// 	if err != nil {
	// 		return err
	// 	}
	// } else {
	// 	for i := 0; i < len(tbls); i++ {
	// 		bf, err := DpTableString(remote, &tbls[i], *f)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		fmt.Println(string(bf))
	// 	}
	// 	if *v {
	// 		fmt.Println("will be written into " + tablepath)
	// 	}
	// }
}

func DpGenerateMergeScr(m *Merger, r *Remote, verbose bool) (string, error) {

	var start time.Time
	if verbose {
		start = time.Now()
	}

	script, err := MergeSchema(m, r)

	if verbose {
		elapsed := time.Since(start)
		fmt.Printf("Generated merge script in %s. \n", elapsed.String())
	}

	return script, err
}

func DpExecuteMerge(c *Config, uc *DpUserConf, remote *Remote) error {

	var merge *Merger
	merge = MergeNew()

	var err error

	if err = DpInitLocalSchema(merge, c, uc.verb); err != nil {
		return err
	}

	if err = DpInitRemoteSchema(remote, merge, uc.verb); err != nil {
		return err
	}

	var script string
	if script, err = DpGenerateMergeScr(merge, remote, uc.verb); err != nil {
		return err
	}

	if !uc.exec {
		fmt.Println(script)
		return nil
	}

	if uc.verb {

		start := time.Now()
		c, _, err := DpExecuteCmdsVerbose(remote.conn, script)
		if err != nil {
			fmt.Println("\033[0;31mcouldnt complete deploy.\033[0m")
			return err
		}
		elapsed := time.Since(start)
		fmt.Printf("Merge completed in %s. Executed %d operations.\n", elapsed.String(), c)

	} else {

		if err = DpExecuteCmds(remote.conn, script); err != nil {
			return err
		}

	}

	return nil
}

func DpExecuteProfiles(c *Config, uc *DpUserConf, remote *Remote) error {

	var err error

	switch uc.profile {
	case "merge":

		err = DpExecuteMerge(c, uc, remote)

	case "import":

		err = DpExecuteImport(c, uc, remote)

	default:

		flag.Usage()
		return fmt.Errorf("invalid profile specified: " + uc.profile)

	}

	return err
}

func DpInitLocalSchema(m *Merger, conf *Config, verbose bool) error {

	var start time.Time
	if verbose {
		start = time.Now()
	}

	var tpath, tppath string
	var err error

	if tpath, err = conf.Get("tables"); err != nil {
		return err
	}

	// optional field and can fail
	if tppath, err = conf.Get("types"); err != nil {
		tppath = ""
	}

	var t []Table
	var tp []Type

	if t, err = ReadTables(tpath); err != nil {
		return err
	}

	if len(tppath) != 0 {
		if tp, err = ReadTypes(tppath); err != nil {
			return err
		}
	}

	m.SetLocalSchema(t, tp)

	if verbose {
		elapsed := time.Since(start)
		fmt.Printf("Parsed local schema in %s. \n", elapsed.String())
	}

	return nil
}

func DpInitRemoteSchema(r *Remote, m *Merger, verbose bool) error {

	var start time.Time
	if verbose {
		start = time.Now()
	}

	if m.localTables == nil {
		return fmt.Errorf("merger local schema wasnt inizialized")
	}

	var err error

	if m.remTables, err = RemoteGetMatchTables(r, m.localTables); err != nil {
		return err
	}

	if m.localTypes != nil {
		if m.remTypes, err = RemoteGetTypes(r, m.localTypes); err != nil {
			return err
		}
	}

	if verbose {
		elapsed := time.Since(start)
		fmt.Printf("Downloaded remote schema in %s. \n", elapsed.String())
	}

	return nil

}

func DpProgram() error {

	uconfig := DpGetUserConf()
	var err error

	conf := ConfNew()
	if err := ConfInit(conf, uconfig.config); err != nil {
		return err
	}

	var remote *Remote

	if remote, err = DpRemoteInit(conf); err != nil {
		return err
	}

	defer remote.conn.Close()

	if conf.Before != nil {
		for i := 0; i < len(conf.Before); i++ {
			beforeSpec, err := ConfScriptSpec(conf.Before[i])
			if err != nil {
				return err
			}
			if err := DpExecuteSpec(remote.conn, beforeSpec, uconfig.exec); err != nil {
				return err
			}
		}
	}

	if err = DpExecuteProfiles(conf, uconfig, remote); err != nil {
		return err
	}

	if conf.After != nil {
		for i := 0; i < len(conf.After); i++ {
			s, err := ConfScriptSpec(conf.After[i])
			if err != nil {
				return err
			}
			if err := DpExecuteSpec(remote.conn, s, uconfig.exec); err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	err := DpProgram()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
