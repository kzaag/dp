package cass

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/kzaag/dp/target"
)

func getMergeScript(
	dbCtx interface{}, ectx *target.Target, argv []string,
) (string, error) {
	db := dbCtx.(*gocql.Session)
	tctx := MergeTableCtxNew()
	vctx := MergeViewCtxNew()
	sctx := StmtNew()
	var err error
	var dd []interface{}

	for _, arg := range argv {
		if dd, err = ParserGetObjectsInDir(sctx, arg); err != nil {
			return "", err
		}
		for _, obj := range dd {
			if v, ok := obj.(*Table); ok {
				tctx.LocalTables[v.Name] = v
			}
			if v, ok := obj.(*MaterializedView); ok {
				vctx.LocalViews[v.Name] = v
			}
		}
	}
	if tctx.RemoteTables, err = RemoteGetMatchingTables(
		db, ectx.Database, tctx.LocalTables,
	); err != nil {
		return "", err
	}

	if vctx.RemoteViews, err = RemoteGetViews(
		db, ectx.Database,
	); err != nil {
		return "", err
	}

	return Merge(sctx, tctx, vctx), nil
}

// panics if err != nil
func p(err error) {
	if err != nil {
		panic(err)
	}
}

func dbNew(target *target.Target) (interface{}, error) {
	var sess *gocql.Session
	var err error
	var timeout, retries, interval int = 10, 0, 2
	p(target.GetInt("timeout", &timeout))
	p(target.GetInt("retries", &retries))
	p(target.GetInt("interval", &interval))
	cluster := gocql.NewCluster(target.Server...)
	cluster.Timeout = time.Second * time.Duration(timeout)
	cluster.Keyspace = target.Database
	for retries > 0 {
		if sess, err = cluster.CreateSession(); err != nil {
			fmt.Println(err)
			time.Sleep(time.Second * time.Duration(interval))
		} else {
			break
		}
		retries--
	}
	return sess, err
}

func dbClose(db interface{}) {
	db.(*gocql.Session).Close()
}

func dbExec(db interface{}, stmt string, args ...interface{}) error {
	var err error
	if args == nil {
		err = db.(*gocql.Session).Query(stmt).Exec()
	} else {
		err = db.(*gocql.Session).Query(stmt, args...).Exec()
	}
	return err
}

func TargetCtxNew() *target.Ctx {
	return &target.Ctx{
		DbClose:        dbClose,
		DbExec:         dbExec,
		DbNew:          dbNew,
		DbPing:         nil,
		DbSuffix:       ".cql",
		GetMergeScript: getMergeScript,
	}
}
