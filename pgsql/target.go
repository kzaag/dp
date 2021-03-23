package pgsql

import (
	"database/sql"
	"fmt"
	"syscall"

	"github.com/kzaag/dp/target"

	"golang.org/x/crypto/ssh/terminal"
)

func TargetGetCS(target *target.Target) (string, error) {

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
	if len(target.Server) == 1 && target.Server[0] != "" {
		host = fmt.Sprintf("host=%s", target.Server[0])
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

func TargetGetDB(target *target.Target) (interface{}, error) {
	var cs string
	var err error

	if cs, err = TargetGetCS(target); err != nil {
		return nil, err
	}

	return sql.Open("postgres", cs)
}

func TargetGetMergeScript(
	c *target.Config,
	dbCtx interface{},
	ectx *target.Target,
	args []string,
) (string, error) {
	db := dbCtx.(*sql.DB)
	ts := MergeTableCtx{}
	tt := MergeTypeCtx{}
	ctx := StmtNew()
	var mergeScript string
	var err error
	var dd []DDObject

	for _, arg := range args {
		if dd, err = ParserGetObjectsInDir(c, ctx, arg); err != nil {
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

func TargetCtxNew() *target.Ctx {
	return &target.Ctx{
		DbClose:        DbClose,
		DbExec:         DbExec,
		DbNew:          TargetGetDB,
		DbPing:         DbPing,
		DbSuffix:       ".sql",
		GetMergeScript: TargetGetMergeScript,
	}
}
