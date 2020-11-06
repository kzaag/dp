package main

import (
	"fmt"
	"os"

	"github.com/kzaag/dp/cmn"
	"github.com/kzaag/dp/mssql"
	"github.com/kzaag/dp/pgsql"
	"github.com/kzaag/dp/rdbms"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/lib/pq"
)

func main() {

	var c *cmn.Config
	var err error

	/* read user parameters */
	args := cmn.UserArgsNew()

	/*
		parse configuration file
	*/
	if c, err = cmn.ConfigNewFromPath(args.ConfigPath); err != nil {
		cmn.CndPrintError(args.Raw, err)
		os.Exit(1)
	}

	/*
		it would be more elegant to load module as a dynamic plugin,
		but i would introduce more code and make project less portable
			(go dynamic libs only seem to be working on linux).
		Thus i use static module import
	*/
	switch c.Driver {
	case "postgres":
		ctx := pgsql.TargetCtxNew()
		err = rdbms.TargetRunFromConfig(ctx, c, args)
	case "mssql":
		ctx := mssql.TargetCtxNew()
		err = rdbms.TargetRunFromConfig(ctx, c, args)
	default:
		err = fmt.Errorf("Unkown driver: %s", c.Driver)
	}

	if err != nil {
		cmn.CndPrintError(args.Raw, err)
		os.Exit(1)
	}
}
