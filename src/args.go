package main

import (
	"flag"
	"strconv"
)

type Args struct {
	profile string
	format  uint
	config  string
	exec    bool
	verb    bool
}

func ArgsParse() *Args {
	var c Args

	flag.StringVar(&c.profile, "p", "merge", "profile. to be executed: merge, import")
	flag.UintVar(&c.format, "f", F_SQL, "available import formats: json="+strconv.Itoa(F_JSON)+", sql="+strconv.Itoa(F_SQL))
	flag.StringVar(&c.config, "c", "", "config path. defaults to first config beginning with main.* ex. main.mssql.conf")
	flag.BoolVar(&c.exec, "e", false, "execute, if not specified then this is dry run")
	flag.BoolVar(&c.verb, "v", false, "verbose - report progress as program runs")

	flag.Parse()

	return &c
}
