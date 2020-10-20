package main

import (
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/kzaag/database-project/rdbms"
	_ "github.com/lib/pq"
)

// formats
// const (
// 	F_JSON = iota
// 	F_SQL  = iota
// )

// func DpExt(f uint) string {
// 	switch f {
// 	case F_JSON:
// 		fallthrough
// 	default:
// 		return ".json"
// 	case F_SQL:
// 		return ".sql"
// 	}
// }

// func DpTableString(remote *Remote, t *Table, f uint) ([]byte, error) {
// 	switch f {
// 	case F_JSON:
// 		fallthrough
// 	default:
// 		return json.MarshalIndent(t, "", "\t")
// 	case F_SQL:
// 		b := RemoteTableDef(remote, t)
// 		return []byte(b), nil
// 	}
// }

// func DpStdoutWriteTables(remote *Remote, tables []Table, dir string, format uint) error {
// 	if dir != "" {
// 		fs, err := os.Stat(dir)
// 		if err != nil && os.IsNotExist(err) {
// 			return err
// 		}
// 		if !fs.IsDir() {
// 			return fmt.Errorf(dir + " is not directory")
// 		}
// 		for i := 0; i < len(tables); i++ {
// 			if tables[i].Name == "" {
// 				return fmt.Errorf("empty table name")
// 			}
// 			ext := DpExt(format)
// 			f, err := os.Create(path.Join(dir, tables[i].Name+ext))
// 			if err != nil {
// 				return err
// 			}
// 			def, err := DpTableString(remote, &tables[i], format)
// 			if err != nil {
// 				return err
// 			}
// 			_, err = f.Write(def)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 	} else {
// 		for i := 0; i < len(tables); i++ {
// 			var def []byte
// 			def, err := DpTableString(remote, &tables[i], format)
// 			if err != nil {
// 				return err
// 			}
// 			fmt.Println(string(def))
// 		}
// 	}
// 	return nil
// }

//func DpExecuteImport(c *Config, uc *DpUserConf, remote *Remote) error {
//	panic("not implemented")
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
//}

// func DpGenerateMergeScr(m *Merger, r *Remote, verbose bool) (string, error) {

// 	var start time.Time
// 	if verbose {
// 		start = time.Now()
// 	}

// 	script, err := MergeSchema(m, r)

// 	if verbose {
// 		elapsed := time.Since(start)
// 		fmt.Printf("Generated merge script in %s. \n", elapsed.String())
// 	}

// 	return script, err
// }

// func DpDisplayScript(script string, uc *DpUserConf) {

// 	if script == EMPTY {

// 		if uc.verb {

// 			fmt.Print("\n\033[0;32mNo merge needed\033[0m\n\n")
// 		}

// 	} else {

// 		if uc.verb {
// 			fmt.Print("\n\033[0;36m-----BEGIN SCRIPT-----\033[0m\n\n")
// 		}

// 		fmt.Print(script)

// 		if uc.verb {
// 			fmt.Print("\n\033[0;36m-----END SCRIPT-----\033[0m\n\n")
// 		}

// 	}

// }

// func DpExecuteScript(script string, uc *DpUserConf, remote *Remote) error {

// 	var err error

// 	if uc.verb {

// 		if uc.verb {
// 			fmt.Print("\n\033[0;36m-----BEGIN SCRIPT-----\033[0m\n\n")
// 		}

// 		start := time.Now()
// 		c, _, err := DpExecuteCmdsVerbose(remote.conn, script)
// 		if err != nil {
// 			fmt.Println("\033[0;31mcouldnt complete deploy.\033[0m")
// 			return err
// 		}
// 		elapsed := time.Since(start)

// 		if uc.verb {
// 			fmt.Print("\n\033[0;36m-----END SCRIPT-----\033[0m\n\n")
// 		}

// 		fmt.Printf("\033[4;32mMerge completed in %s. Executed %d operations.\033[0m\n\n", elapsed.String(), c)

// 	} else {

// 		if err = DpExecuteCmds(remote.conn, script); err != nil {
// 			return err
// 		}

// 	}

// 	return nil

// }

// func DpExecuteMerge(c *Config, uc *DpUserConf, remote *Remote) error {

// 	if uc.verb {
// 		fmt.Print("\nBeginning merge...\n\n")
// 	}

// 	var merge *Merger = MergeNew()

// 	var err error

// 	if err = DpInitLocalSchema(merge, c, remote, uc.verb); err != nil {
// 		return err
// 	}

// 	if err = DpInitRemoteSchema(remote, merge, uc.verb); err != nil {
// 		return err
// 	}

// 	var script string
// 	if script, err = DpGenerateMergeScr(merge, remote, uc.verb); err != nil {
// 		return err
// 	}

// 	if uc.exec {
// 		err = DpExecuteScript(script, uc, remote)
// 	} else {
// 		DpDisplayScript(script, uc)
// 	}

// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func DpExecuteProfiles(c *Config, uc *DpUserConf, remote *Remote) error {

// 	var err error
// 	var profile string

// 	if profile, err = c.Get("profile"); err != nil {
// 		profile = uc.profile
// 	}

// 	switch profile {
// 	case "merge":

// 		err = DpExecuteMerge(c, uc, remote)

// 	case "import":

// 		err = DpExecuteImport(c, uc, remote)

// 	default:

// 		return nil

// 	}

// 	return err
// }

// func DpInitLocalSchema(m *Merger, conf *Config, r *Remote, verbose bool) error {

// 	var start time.Time
// 	if verbose {
// 		start = time.Now()
// 	}

// 	var tpath, tppath string
// 	var err error

// 	if tpath, err = conf.Get("tables"); err != nil {
// 		return err
// 	}

// 	// optional field and can fail
// 	if tppath, err = conf.Get("types"); err != nil {
// 		tppath = ""
// 	}

// 	var t []Table
// 	var tp []Type

// 	if t, err = ReadTables(tpath, r); err != nil {
// 		return err
// 	}

// 	if len(tppath) != 0 {
// 		if tp, err = ReadTypes(tppath, r); err != nil {
// 			return err
// 		}
// 	}

// 	m.SetLocalSchema(t, tp)

// 	if verbose {
// 		elapsed := time.Since(start)
// 		fmt.Printf("Parsed local schema in %s. \n", elapsed.String())
// 	}

// 	return nil
// }

// func DpInitRemoteSchema(r *Remote, m *Merger, verbose bool) error {

// 	var start time.Time
// 	if verbose {
// 		start = time.Now()
// 	}

// 	if m.localTables == nil {
// 		return fmt.Errorf("merger local schema wasnt inizialized")
// 	}

// 	var err error

// 	if m.remTables, err = RemoteGetMatchTables(r, m.localTables); err != nil {
// 		return err
// 	}

// 	if m.localTypes != nil {
// 		if m.remTypes, err = RemoteGetTypes(r, m.localTypes); err != nil {
// 			return err
// 		}
// 	}

// 	if verbose {
// 		elapsed := time.Since(start)
// 		fmt.Printf("Downloaded remote schema in %s. \n", elapsed.String())
// 	}

// 	return nil

// }

// func DpExecPaths(paths []string, uconfig *DpUserConf, remote *Remote) error {

// 	if paths == nil {
// 		return nil
// 	}

// 	var spec *ScriptSpec
// 	var err error

// 	for i := 0; i < len(paths); i++ {

// 		if spec, err = ConfScriptSpec(paths[i]); err != nil {
// 			return err
// 		}

// 		if err := DpExecuteSpec(remote.conn, spec, uconfig); err != nil {
// 			return err
// 		}

// 	}

// 	return nil
// }

// func DpProgram() error {

// 	uconfig := DpGetUserConf()
// 	var err error

// 	conf := config.New()
// 	if err := config.CreateFromPath(conf, uconfig.config); err != nil {
// 		return err
// 	}

// 	var remote *Remote

// 	if remote, err = DpRemoteInit(conf); err != nil {
// 		return err
// 	}

// 	defer remote.conn.Close()

// 	if uconfig.verb && conf.Before != nil && len(conf.Check) != 0 {
// 		fmt.Print("\033[0;36m\nCheck:\033[0m\n")
// 	}

// 	/* check is always executed */
// 	uconfig.exec = true
// 	if err = DpExecPaths(conf.Check, uconfig, remote); err != nil {
// 		return err
// 	}
// 	uconfig.exec = false

// 	if uconfig.verb && conf.Before != nil && len(conf.Before) != 0 {
// 		fmt.Print("\033[0;36m\nBefore:\033[0m\n")
// 	}

// 	if err = DpExecPaths(conf.Before, uconfig, remote); err != nil {
// 		return err
// 	}

// 	if err = DpExecuteProfiles(conf, uconfig, remote); err != nil {
// 		return err
// 	}

// 	if uconfig.verb && conf.After != nil && len(conf.After) != 0 {
// 		fmt.Print("\033[0;36mAfter:\033[0m\n")
// 	}

// 	if err = DpExecPaths(conf.After, uconfig, remote); err != nil {
// 		return err
// 	}

// 	if uconfig.verb && conf.After != nil && len(conf.After) != 0 {
// 		fmt.Println()
// 	}

// 	return nil
// }

func main() {
	x := rdbms.Rdbms{}
	_ = x
	// err := DpProgram()
	// if err != nil {
	// 	fmt.Println(err)
	// 	os.Exit(1)
	// }
}
