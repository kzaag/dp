package pgsql

import (
	"database-project/rdbms"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
)

func ParserElevateErrorType(tname string, err error) error {
	return fmt.Errorf("in type %s: %s", tname, err.Error())
}

func __ParserValidateStrArray(c []string) error {
	if len(c) == 0 {
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

func ParserValidateTypes(ctx *rdbms.StmtCtx, types []Type) error {

	if types == nil {
		return nil
	}

	for i := 0; i < len(types); i++ {
		t := &types[i]
		if t.Name == "" {
			return fmt.Errorf("type at index %d doesnt have name specified", i)
		}

		t.Type = strings.ToLower(t.Type)

		switch t.Type {
		case TT_Composite:
			if err := rdbms.ParserValidateColumn(ctx, t.Columns); err != nil {
				return ParserElevateErrorType(t.Name, err)
			}
			for i := 0; i < len(t.Columns); i++ {
				t.Columns[i].Meta = rdbms.CM_CompType
			}
		case TT_Enum:
			if err := __ParserValidateStrArray(t.Values); err != nil {
				return ParserElevateErrorType(t.Name, err)
			}
		default:
			return ParserElevateErrorType(t.Name, fmt.Errorf("unknown type: \"%s\"", t.Type))
		}
	}

	return nil
}

func ParserGetTypes(ctx *rdbms.StmtCtx, dir string) ([]Type, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	length := len(files)
	defs := make([][]byte, length)
	allowedLen := 0
	for i := 0; i < length; i++ {
		name := path.Join(dir, files[i].Name())
		isDir, err := rdbms.ParserIsDirectory(name)
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

	ret := make([]Type, allowedLen)
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

	if err := ParserValidateTypes(ctx, ret); err != nil {
		return nil, err
	}

	return ret, nil
}
