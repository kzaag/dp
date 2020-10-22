package rdbms

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

/*

parsing local schema to structs

*/

func __ParserElevateErrorIndex(name string, err error) error {
	return fmt.Errorf("in index %s: %s", name, err.Error())
}

func ParserValidateIndex(ixs []Index) error {
	if ixs == nil {
		return nil
	}
	for i := 0; i < len(ixs); i++ {
		ix := ixs[i]
		if ix.Name == "" {
			return fmt.Errorf("index at index %d doesnt have name specified", i)
		}
		if ix.Columns == nil || len(ix.Columns) == 0 {
			return __ParserElevateErrorIndex(ix.Name, fmt.Errorf("no index columns specified"))
		}
		for i := 0; i < len(ix.Columns); i++ {
			c := ix.Columns[i]
			if c.Name == "" {
				return __ParserElevateErrorIndex(ix.Name, fmt.Errorf("column at index %d doesnt have name specified", i))
			}
		}
	}
	return nil
}

func ParserIsDirectory(path string) (bool, error) {
	fs, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fs.IsDir(), nil
}

func ParserValidateCheck(checks []Check) error {
	if checks == nil {
		return nil
	}
	for i := 0; i < len(checks); i++ {
		c := checks[i]
		if c.Name == "" {
			return fmt.Errorf("check at index %d doesnt have name specified", i)
		}
		if c.Def == "" {
			return fmt.Errorf("check at index %d doesnt have definition specified", i)
		}
	}
	return nil
}

func __ParserElevateErrorColumn(tname string, err error) error {
	return fmt.Errorf("in column %s: %s", tname, err.Error())
}

func ParserValidateColumn(ctx *StmtCtx, c []Column) error {
	if len(c) == 0 {
		return nil
	}
	for i := 0; i < len(c); i++ {
		col := &c[i]
		if col.Name == "" {
			return fmt.Errorf("column at index %d doesnt have name specified", i)
		}
		if col.Type == "" && col.FullType == "" {
			return __ParserElevateErrorColumn(col.Name, fmt.Errorf("type was not specified"))
		}

		if col.FullType == "" {
			col.FullType = ctx.ColumnType(col)
		} else {
			col.FullType = strings.ToUpper(col.FullType)
		}
	}
	return nil
}

func ParserValidateCColumn(c []ConstraintColumn) error {
	if c == nil {
		return nil
	}
	for i := 0; i < len(c); i++ {
		col := c[i]
		if col.Name == "" {
			return fmt.Errorf("column at index %d doesnt have name specified", i)
		}
	}
	return nil
}

func ParserValidateConstraint(c Constraint) error {
	if c.Name == "" {
		return fmt.Errorf("constaint name not specified")
	}
	err := ParserValidateCColumn(c.Columns)
	if err != nil {
		return fmt.Errorf("in constraint %s: %s", c.Name, err.Error())
	}
	return nil
}

func ParserValidateUnique(us []Unique) error {
	if us == nil {
		return nil
	}
	for i := 0; i < len(us); i++ {
		u := us[i]
		if err := ParserValidateConstraint(u.Constraint); err != nil {
			return err
		}
	}
	return nil
}

// func ElevateErrorPk(name string, err error) error {
// 	return fmt.Errorf("in primary key %s: %s", name, err.Error())
// }

func ParserValidatePK(pk *PrimaryKey) error {
	if pk == nil {
		return nil
	}
	if err := ParserValidateConstraint(pk.Constraint); err != nil {
		return err
	}
	return nil
}

func __ParserErrorFk(name string, err error) error {
	return fmt.Errorf("in foregign key %s: %s", name, err.Error())
}

func ParserValidateFK(fks []ForeignKey) error {
	if fks == nil {
		return nil
	}
	for i := 0; i < len(fks); i++ {
		fk := fks[i]
		if err := ParserValidateConstraint(fk.Constraint); err != nil {
			return __ParserErrorFk(fk.Name, err)
		}
		if err := ParserValidateCColumn(fk.Ref_columns); err != nil {
			return __ParserErrorFk(fk.Name, err)
		}
		if fk.Ref_table == "" {
			return __ParserErrorFk(fk.Name, fmt.Errorf("reference table name was empty"))
		}
	}
	return nil
}

// func __ParserElevateErrorType(tname string, err error) error {
// 	return fmt.Errorf("in type %s: %s", tname, err.Error())
// }

func __ParserErrorTable(tname string, err error) error {
	return fmt.Errorf("in table %s: %s", tname, err.Error())
}

func ParserValidateTables(ctx *StmtCtx, tables []Table) error {
	if tables == nil {
		return nil
	}
	for i := 0; i < len(tables); i++ {
		t := tables[i]
		if t.Name == "" {
			return fmt.Errorf("table at index %d doesnt have name specified", i)
		}
		name := t.Name
		if err := ParserValidateCheck(t.Check); err != nil {
			return __ParserErrorTable(name, err)
		}
		if err := ParserValidateUnique(t.Unique); err != nil {
			return __ParserErrorTable(name, err)
		}
		if err := ParserValidateFK(t.Foreign); err != nil {
			return __ParserErrorTable(name, err)
		}
		if err := ParserValidatePK(t.Primary); err != nil {
			return __ParserErrorTable(name, err)
		}
		if err := ParserValidateColumn(ctx, t.Columns); err != nil {
			return __ParserErrorTable(name, err)
		}
		if err := ParserValidateIndex(t.Indexes); err != nil {
			return __ParserErrorTable(name, err)
		}
	}
	return nil
}

func ParserGetTablesInDir(ctx *StmtCtx, dir string) ([]Table, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	length := len(files)
	defs := make([][]byte, length)
	allowedLen := 0
	for i := 0; i < length; i++ {
		name := path.Join(dir, files[i].Name())
		isDir, err := ParserIsDirectory(name)
		if err != nil {
			return nil, err
		}
		if !isDir && strings.HasSuffix(name, ".json") {
			content, err := ioutil.ReadFile(name)
			defs[i] = content
			if len(content) == 0 {
				return nil, fmt.Errorf("%s - empty file content", name)
			}
			if err != nil {
				return nil, err
			}
			allowedLen++
		} else {
			defs[i] = nil
		}
	}

	ret := make([]Table, allowedLen)
	ci := 0
	for i := 0; i < length; i++ {
		if defs[i] != nil {
			err = json.Unmarshal([]byte(defs[i]), &ret[ci])
			if err != nil {
				name := path.Join(dir, files[i].Name())
				return nil, fmt.Errorf("couldnt unmarshall %s %s", name, err.Error())
			}
			ci++
		}
	}

	if err := ParserValidateTables(ctx, ret); err != nil {
		return nil, err
	}

	return ret, nil
}
