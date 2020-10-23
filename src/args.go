package main

import (
	"flag"
)

type Args struct {
	config string
	exec   bool
	verb   bool
}

func ArgsCreate() *Args {
	var c Args

	flag.StringVar(&c.config, "c", "dp.yml", "config path.")
	flag.BoolVar(&c.exec, "e", false, "execute, if not specified then expect dry run, (nothing gets executed on database)")
	flag.BoolVar(&c.verb, "v", false, "verbosity - report progress as program runs")

	flag.Parse()

	return &c
}
