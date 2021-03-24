package main

import (
	"fmt"
	"os"

	"github.com/kzaag/dp/cmn"
	"github.com/kzaag/dp/pgsql"
	"github.com/kzaag/dp/target"

	_ "github.com/lib/pq"
)

func main() {

	var c *target.Config
	var err error

	/* read user parameters */
	args := target.NewArgsFromCli()

	/*
		parse configuration file
	*/
	if c, err = target.NewConfigFromPath(args.ConfigPath); err != nil {
		cmn.CndPrintError(args.Raw, err)
		os.Exit(2)
	}

	/*
		it would be more elegant to load module as a dynamic plugin,
		but i would introduce more code and make project less portable
			(go dynamic libs only seem to be working on linux).
		Thus i use static module import
	*/
	var ctx *target.Ctx
	switch c.Driver {
	case "postgres":
		ctx = pgsql.TargetCtxNew()
	default:
		cmn.CndPrintError(args.Raw, fmt.Errorf("Unkown driver: %s", c.Driver))
		os.Exit(2)
	}

	err = ctx.ExecConfig(c, args)

	if err != nil {
		cmn.CndPrintError(args.Raw, err)
		os.Exit(2)
	}
}
