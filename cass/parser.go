package cass

import (
	"fmt"

	"github.com/kzaag/dp/cmn"
	"gopkg.in/yaml.v2"
)

/*
	TODO
	this still needs more comprehensive validation
*/

type ParseCtx struct {
	Stmt *StmtCtx
	ret  []interface{}
}

func ParserValidateView(
	stmt *StmtCtx, v *MaterializedView, path string,
) error {
	if v == nil {
		return nil
	}
	if v.Name == "" {
		return fmt.Errorf("Validate %s: view doesnt have name specified", path)
	}
	if v.PrimaryKey == nil {
		return fmt.Errorf("Validate %s: view primary key doesnt exist", path)
	}
	return nil
}

func ParserValidateTable(
	stmt *StmtCtx, t *Table, path string,
) error {
	if t == nil {
		return nil
	}
	if t.Name == "" {
		return fmt.Errorf("Validate %s: table doesnt have name specified", path)
	}
	if t.PrimaryKey == nil {
		return fmt.Errorf("Validate %s: table primary key doesnt exist", path)
	}
	for k := range t.Columns {
		t.Columns[k].Name = k
		if t.Columns[k].Type == "" {
			return fmt.Errorf("Validate %s: column %s doesnt specify type", path, k)
		}
	}
	return nil
}

func ParserGetValidateObject(
	path string, fc []byte, args interface{},
) error {
	var err error
	ctx := args.(*ParseCtx)
	var obj struct {
		Table *Table
		View  *MaterializedView
	}
	if err = yaml.Unmarshal(fc, &obj); err != nil {
		return fmt.Errorf("couldnt unmarshal %s %s", path, err.Error())
	}
	if (obj.Table != nil) == (obj.View != nil) {
		return fmt.Errorf("couldnt validate %s, assert table xor view failed.", path)
	}
	if obj.Table != nil {
		err = ParserValidateTable(ctx.Stmt, obj.Table, path)
		ctx.ret = append(ctx.ret, obj.Table)
	} else {
		err = ParserValidateView(ctx.Stmt, obj.View, path)
		ctx.ret = append(ctx.ret, obj.View)
	}
	return err
}

func ParserGetObjectsInDir(
	ctx *StmtCtx, dir string,
) ([]interface{}, error) {
	parsectx := &ParseCtx{
		Stmt: ctx,
	}
	err := cmn.ParserIterateOverSource(
		dir,
		ParserGetValidateObject,
		parsectx)
	if err != nil {
		return nil, err
	}
	return parsectx.ret, err
}
