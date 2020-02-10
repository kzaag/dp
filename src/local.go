package main

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

func ElevateErrorIndex(name string, err error) error {
	return fmt.Errorf("in index %s: %s", name, err.Error())
}

func ValidateIndex(ixs []Index) error {
	if ixs == nil {
		return nil
	}
	for i := 0; i < len(ixs); i++ {
		ix := ixs[i]
		if ix.Name == "" {
			return fmt.Errorf("index at index %d doesnt have name specified", i)
		}
		if ix.Columns == nil || len(ix.Columns) == 0 {
			return ElevateErrorIndex(ix.Name, fmt.Errorf("no index columns specified"))
		}
		for i := 0; i < len(ix.Columns); i++ {
			c := ix.Columns[i]
			if c.Name == "" {
				return ElevateErrorIndex(ix.Name, fmt.Errorf("column at index %d doesnt have name specified", i))
			}
		}
	}
	return nil
}

func IsDirectory(path string) (bool, error) {
	fs, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fs.IsDir(), nil
}

func ValidateCheck(checks []Check) error {
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

func ElevateErrorColumn(tname string, err error) error {
	return fmt.Errorf("in column %s: %s", tname, err.Error())
}

func ValidateColumn(c []Column) error {
	if c == nil || len(c) == 0 {
		return fmt.Errorf("no columns specified")
	}
	for i := 0; i < len(c); i++ {
		col := c[i]
		if col.Name == "" {
			return fmt.Errorf("column at index %d doesnt have name specified", i)
		}
		if col.Type == "" {
			return ElevateErrorColumn(col.Name, fmt.Errorf("type was not specified"))
		}
	}
	return nil
}

func ValidateStrArray(c []string) error {
	if c == nil || len(c) == 0 {
		return fmt.Errorf("no values specified")
	}
	for i := 0; i < len(c); i++ {
		col := c[i]
		if col == "" {
			return fmt.Errorf("invalid value at index %d", i)
		}
	}
	return nil
}

func ValidateCColumn(c []ConstraintColumn) error {
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

func ValidateConstraint(c Constraint) error {
	if c.Name == "" {
		return fmt.Errorf("constaint name not specified")
	}
	err := ValidateCColumn(c.Columns)
	if err != nil {
		return fmt.Errorf("in constraint %s: %s", c.Name, err.Error())
	}
	return nil
}

func ValidateUnique(us []Unique) error {
	if us == nil {
		return nil
	}
	for i := 0; i < len(us); i++ {
		u := us[i]
		if err := ValidateConstraint(u.Constraint); err != nil {
			return err
		}
	}
	return nil
}

func ElevateErrorPk(name string, err error) error {
	return fmt.Errorf("in primary key %s: %s", name, err.Error())
}

func ValidatePk(pk *PrimaryKey) error {
	if pk == nil {
		return nil
	}
	if err := ValidateConstraint(pk.Constraint); err != nil {
		return err
	}
	return nil
}

func ElevateErrorFk(name string, err error) error {
	return fmt.Errorf("in foregign key %s: %s", name, err.Error())
}

func ValidateFk(fks []ForeignKey) error {
	if fks == nil {
		return nil
	}
	for i := 0; i < len(fks); i++ {
		fk := fks[i]
		if err := ValidateConstraint(fk.Constraint); err != nil {
			return ElevateErrorFk(fk.Name, err)
		}
		if err := ValidateCColumn(fk.Ref_columns); err != nil {
			return ElevateErrorFk(fk.Name, err)
		}
		if fk.Ref_table == "" {
			return ElevateErrorFk(fk.Name, fmt.Errorf("reference table name was empty"))
		}
	}
	return nil
}

func ElevateErrorType(tname string, err error) error {
	return fmt.Errorf("in type %s: %s", tname, err.Error())
}

func ValidateTypes(types []Type) error {

	if types == nil {
		return nil
	}

	for i := 0; i < len(types); i++ {
		t := types[i]
		if t.Name == "" {
			return fmt.Errorf("type at index %d doesnt have name specified", i)
		}

		switch t.Type {
		case TT_Composite:
			if err := ValidateColumn(t.Columns); err != nil {
				return ElevateErrorType(t.Name, err)
			}
		case TT_Enum:
			if err := ValidateStrArray(t.Values); err != nil {
				return ElevateErrorType(t.Name, err)
			}
		default:
			return ElevateErrorType(t.Name, fmt.Errorf("uknown type of type: %s", t.Type))
		}
	}

	return nil
}

func ElevateErrorTable(tname string, err error) error {
	return fmt.Errorf("in table %s: %s", tname, err.Error())
}

func ValidateTables(tables []Table) error {
	if tables == nil {
		return nil
	}
	for i := 0; i < len(tables); i++ {
		t := tables[i]
		if t.Name == "" {
			return fmt.Errorf("table at index %d doesnt have name specified", i)
		}
		name := t.Name
		if err := ValidateCheck(t.Check); err != nil {
			return ElevateErrorTable(name, err)
		}
		if err := ValidateUnique(t.Unique); err != nil {
			return ElevateErrorTable(name, err)
		}
		if err := ValidateFk(t.Foreign); err != nil {
			return ElevateErrorTable(name, err)
		}
		if err := ValidatePk(t.Primary); err != nil {
			return ElevateErrorTable(name, err)
		}
		if err := ValidateColumn(t.Columns); err != nil {
			return ElevateErrorTable(name, err)
		}
		if err := ValidateIndex(t.Indexes); err != nil {
			return ElevateErrorTable(name, err)
		}
	}
	return nil
}

func ReadTables(dir string) ([]Table, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	length := len(files)
	defs := make([][]byte, length)
	allowedLen := 0
	for i := 0; i < length; i++ {
		name := path.Join(dir, files[i].Name())
		isDir, err := IsDirectory(name)
		if err != nil {
			return nil, err
		}
		if !isDir && strings.HasSuffix(name, ".json") {
			content, err := ioutil.ReadFile(name)
			defs[i] = content
			if len(content) == 0 {
				return nil, fmt.Errorf("empty file content")
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
				return nil, err
			}
			ci++
		}
	}

	if err := ValidateTables(ret); err != nil {
		return nil, err
	}

	return ret, nil
}

func ReadTypes(dir string) ([]Type, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	length := len(files)
	defs := make([][]byte, length)
	allowedLen := 0
	for i := 0; i < length; i++ {
		name := path.Join(dir, files[i].Name())
		isDir, err := IsDirectory(name)
		if err != nil {
			return nil, err
		}
		if !isDir && strings.HasSuffix(name, ".json") {
			content, err := ioutil.ReadFile(name)
			defs[i] = content
			if len(content) == 0 {
				return nil, fmt.Errorf("empty file content")
			}
			if err != nil {
				return nil, err
			}
			allowedLen++
		} else {
			defs[i] = nil
		}
	}

	ret := make([]Type, allowedLen)
	ci := 0
	for i := 0; i < length; i++ {
		if defs[i] != nil {
			err = json.Unmarshal([]byte(defs[i]), &ret[ci])
			if err != nil {
				return nil, err
			}
			ci++
		}
	}

	if err := ValidateTypes(ret); err != nil {
		return nil, err
	}

	return ret, nil
}
