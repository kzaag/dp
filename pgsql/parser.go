package pgsql

import (
	"fmt"
	"strings"

	"github.com/kzaag/dp/cmn"
	"github.com/kzaag/dp/rdbms"

	"gopkg.in/yaml.v2"
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

func ParserValidateType(ctx *rdbms.StmtCtx, t *Type, path string) error {
	if t == nil {
		return nil
	}
	if t.Name == "" {
		return fmt.Errorf("type defined in %s doesnt have name specified", path)
	}

	t.Type = strings.ToLower(t.Type)

	switch t.Type {
	case TypeComposite:
		if err := rdbms.ParserValidateColumn(ctx, t.Columns); err != nil {
			return ParserElevateErrorType(t.Name, err)
		}
		for i := 0; i < len(t.Columns); i++ {
			t.Columns[i].Tags = map[string]struct{}{
				TypeComposite: {},
			}
		}
	case TypeEnum:
		if err := __ParserValidateStrArray(t.Values); err != nil {
			return ParserElevateErrorType(t.Name, err)
		}
	default:
		return ParserElevateErrorType(t.Name, fmt.Errorf("unknown type: \"%s\"", t.Type))
	}
	return nil
}

type DDObject struct {
	Table *rdbms.Table
	Type  *Type
}

type ParseCtx struct {
	Stmt *StmtCtx
	ret  []DDObject
}

func ParserGetValidateObject(path string, fc []byte, args interface{}) error {
	var err error
	var obj DDObject
	ctx := args.(*ParseCtx)
	err = yaml.Unmarshal(fc, &obj)
	if err != nil {
		return fmt.Errorf("couldnt unmarshal %s %s", path, err.Error())
	}
	/* XOR table and type. only 1 can be present in file */
	if (obj.Table != nil) == (obj.Type != nil) {
		return fmt.Errorf("couldnt validate %s, assert table XOR type failed.", path)
	}
	if obj.Table != nil {
		err = rdbms.ParserValidateTable(&ctx.Stmt.StmtCtx, obj.Table, path)
	} else {
		err = ParserValidateType(&ctx.Stmt.StmtCtx, obj.Type, path)
	}
	ctx.ret = append(ctx.ret, obj)
	return err
}

func ParserGetObjectsInDir(ctx *StmtCtx, dir string) ([]DDObject, error) {
	parser := ParseCtx{}
	parser.Stmt = ctx
	err := cmn.ParserIterateOverSource(dir, ParserGetValidateObject, &parser)
	if err != nil {
		return nil, err
	}
	return parser.ret, err
}
