package target

import (
	"flag"
)

type ArgsFlag uint32

const (
	Verbose ArgsFlag = iota
	Execute
	Raw
)

/*
	it would be nice for golang to support bit fields.
	Bit fields are very elegant solution, hovewer since golang doesnt support them,
	We must introduce a lot of code, which makes program slower and more complex.
	Thats why i decided to waste some memory and introduce separate boolean fields.
	In future if number of those fields will increase we could make it bit fields.
*/
type Args struct {
	ConfigPath string
	Verbose    bool
	Execute    bool
	Raw        bool
}

func NewArgsFromCli() *Args {

	var c Args

	flag.StringVar(&c.ConfigPath, "c", "", "config path.")
	flag.BoolVar(&c.Execute, "e", false, "execute, if not specified then expect dry run, (nothing gets executed on database)")
	flag.BoolVar(&c.Verbose, "v", false, "verbosity - report progress as program runs")
	flag.BoolVar(&c.Raw, "r", false, "raw output - disable text formatting")

	flag.Parse()

	return &c
}
