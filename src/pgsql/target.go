package pgsql

import (
	"database-project/cmn"
	"database-project/rdbms"
	"database/sql"
	"fmt"
	"syscall"

	"golang.org/x/crypto/ssh/terminal"
)

func __TargetGetCS(target *cmn.Target) (string, error) {

	if target.ConnectionString != "" {
		return target.ConnectionString, nil
	}

	if target.Password == "" {
		fmt.Printf("password for %s: ", target.Name)
		bytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		fmt.Println()
		target.Password = string(bytes)
	}

	host := ""
	if target.Server != "" {
		host = fmt.Sprintf("host=%s", target.Server)
	}
	user := ""
	if target.User != "" {
		user = fmt.Sprintf("user=%s", target.User)
	}
	password := ""
	if target.Password != "" {
		password = fmt.Sprintf("password=%s", target.Password)
	}
	database := ""
	if target.Database != "" {
		database = fmt.Sprintf("dbname=%s", target.Database)
	}

	cs := fmt.Sprintf(
		"%s %s %s %s",
		host,
		user,
		password,
		database)

	if target.Args != nil {
		for k := range target.Args {
			cs += fmt.Sprintf(" %s=%s", k, target.Args[k])
		}
	}

	return cs, nil
}

func __TargetGetDB(target *cmn.Target) (*sql.DB, error) {
	var cs string
	var err error

	if cs, err = __TargetGetCS(target); err != nil {
		return nil, err
	}

	return sql.Open("postgres", cs)
}

func __TargetGetMergeScript(db *sql.DB, ectx *cmn.Target, args []string) (string, error) {
	ts := rdbms.MergeTableCtx{}
	tt := MergeTypeCtx{}
	ctx := StmtNew()
	var mergeScript string
	var err error
	var dd []DDObject

	for _, arg := range args {
		if dd, err = ParserGetObjectsInDir(ctx, arg); err != nil {
			return "", err
		}
		for _, obj := range dd {
			if obj.Table != nil {
				ts.LocalTables = append(ts.LocalTables, *obj.Table)
			} else {
				tt.localTypes = append(tt.localTypes, *obj.Type)
			}
		}
	}

	if ts.RemoteTables, err = RemoteGetMatchedTables(db, ts.LocalTables); err != nil {
		return "", err
	}

	if tt.remoteTypes, err = RemoteGetTypes(db, tt.localTypes); err != nil {
		return "", err
	}

	if mergeScript, err = Merge(ctx, &ts, &tt); err != nil {
		return "", err
	}

	return mergeScript, nil
}

func TargetNew() *rdbms.TargetCtx {
	ctx := rdbms.TargetCtx{}
	ctx.GetDB = __TargetGetDB
	ctx.GetMergeScript = __TargetGetMergeScript
	return &ctx
}
