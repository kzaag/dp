package pgsql

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"strings"
)

func ParserValidateTypes(types []Type) error {

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
			if err := PrepColumn(t.Columns, r); err != nil {
				return ElevateErrorType(t.Name, err)
			}
			for i := 0; i < len(t.Columns); i++ {
				t.Columns[i].Meta = CM_CompType
			}
		case TT_Enum:
			if err := PrepStrArray(t.Values); err != nil {
				return ElevateErrorType(t.Name, err)
			}
		default:
			return ElevateErrorType(t.Name, fmt.Errorf("unknown type: \"%s\"", t.Type))
		}
	}

	return nil
}

func ParserGetTypes(ctx *PgSqlMergeCtx, dir string) ([]Type, error) {
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

	if err := PrepTypes(ret, r); err != nil {
		return nil, err
	}

	return ret, nil
}
