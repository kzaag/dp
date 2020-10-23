package pgsql

import (
	"database-project/config"
	"database-project/rdbms"
	"database/sql"
	"fmt"
)

func DriverCreateMergeScript(db *sql.DB, ectx []*config.Exec) (string, error) {

	ctx := StmtCtx{}
	ctx.AddCheck = rdbms.StmtAddCheck
	ctx.AddColumn = StmtAddColumn
	ctx.AddFK = rdbms.StmtAddFk
	ctx.AddIndex = StmtAddIndex
	ctx.AddPK = rdbms.StmtAddPK
	ctx.AddType = StmtAddType
	ctx.AddUnique = rdbms.StmtAddUnique
	ctx.AlterColumn = StmtAlterColumn
	ctx.ColumnType = StmtColumnType
	ctx.CreateTable = StmtCreateTable
	ctx.DropColumn = StmtDropColumn
	ctx.DropConstraint = rdbms.StmtDropConstraint
	ctx.DropIndex = StmtDropIndex
	ctx.DropTable = rdbms.StmtDropTable
	ctx.DropType = StmtDropType

	var err error
	ts := rdbms.MergeTableCtx{}
	tt := MergeTypeCtx{}
	var exec *config.Exec

	var tmptypes []Type
	var tmptables []rdbms.Table

	for _, exec = range ectx {
		switch exec.Type {
		case rdbms.CONFIG_TABLE_KEY:
			if tmptables, err = rdbms.ParserGetTablesInDir(&ctx.StmtCtx, exec.Path); err != nil {
				return "", err
			}
			ts.LocalTables = append(ts.LocalTables, tmptables...)
		case "types":
			if tmptypes, err = ParserGetTypes(&ctx.StmtCtx, exec.Path); err != nil {
				return "", err
			}
			tt.localTypes = append(tt.localTypes, tmptypes...)
		default:
			return "", fmt.Errorf("Unkown exec type: %s", exec.Type)
		}
	}

	if ts.RemoteTables, err = RemoteGetMatchedTables(db, ts.LocalTables); err != nil {
		return "", err
	}
	if tt.remoteTypes, err = RemoteGetTypes(db, tt.localTypes); err != nil {
		return "", err
	}

	var mergeScript string

	if mergeScript, err = Merge(&ctx, &ts, &tt); err != nil {
		return "", nil
	}

	return mergeScript, nil
}
