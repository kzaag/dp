package mssql

import (
	"database/sql"
	"fmt"
	"syscall"

	"github.com/kzaag/dp/rdbms"
	"github.com/kzaag/dp/target"

	"golang.org/x/crypto/ssh/terminal"
)

func TargetGetMergeScript(
	dbctx interface{}, ectx *target.Target, argv []string,
) (string, error) {
	db := dbctx.(*sql.DB)
	ts := rdbms.MergeTableCtx{}
	ctx := StmtNew()
	var mergeScript string
	var err error
	var dd []rdbms.DDObject

	for _, arg := range argv {
		if dd, err = rdbms.ParserGetTablesInDir(ctx, arg); err != nil {
			return "", err
		}
		for _, obj := range dd {
			if obj.Table != nil {
				ts.LocalTables = append(ts.LocalTables, *obj.Table)
			}
		}
	}

	if ts.RemoteTables, err = RemoteGetMatchedTables(db, ts.LocalTables); err != nil {
		return "", err
	}

	if mergeScript, err = Merge(ctx, &ts); err != nil {
		return "", err
	}

	return mergeScript, nil
}

func TargetGetCS(target *target.Target) (string, error) {
	if target.ConnectionString != "" {
		return target.ConnectionString, nil
	}

	if target.Password != "" {
		fmt.Printf("password for %s: ", target.Name)
		bytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		target.Password = string(bytes)
	}

	host := ""
	if len(target.Server) == 1 && target.Server[0] != "" {
		host = fmt.Sprintf("server=%s;", target.Server)
	}
	user := ""
	if target.User != "" {
		user = fmt.Sprintf("user id=%s;", target.User)
	}
	password := ""
	if target.Password != "" {
		password = fmt.Sprintf("password=%s;", target.Password)
	}
	database := ""
	if target.Database != "" {
		database = fmt.Sprintf("database=%s;", target.Database)
	}

	return fmt.Sprintf(
		"%s%s%s%s",
		host,
		user,
		password,
		database,
	), nil
}

func TargetGetDB(target *target.Target) (interface{}, error) {
	var cs string
	var err error

	if cs, err = TargetGetCS(target); err != nil {
		return nil, err
	}

	return sql.Open("postgres", cs)
}

func TargetCtxNew() *target.Ctx {
	return &target.Ctx{
		DbClose:        rdbms.DbClose,
		DbExec:         rdbms.DbExec,
		DbNew:          TargetGetDB,
		DbPing:         rdbms.DbPing,
		DbSuffix:       ".sql",
		GetMergeScript: TargetGetMergeScript,
	}
}
