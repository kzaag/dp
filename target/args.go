package target

import (
	"flag"
	"fmt"
	"strings"
)

type ArgsFlag uint32

const (
	Verbose ArgsFlag = iota
	Execute
	Raw
)

type MapFlags map[string]struct{}

func (i *MapFlags) String() string {
	repr := "["
	for k := range *i {
		repr += k + ","
	}
	repr += "]"
	return repr
}

func (i *MapFlags) Set(value string) error {
	(*i)[value] = struct{}{}
	return nil
}

type MapValues map[string]string

func (i *MapValues) String() string {
	repr := "["
	for k := range *i {
		repr += "{" + k + ":" + (*i)[k] + "},"
	}
	repr += "]"
	return repr
}

func (i *MapValues) Set(value string) error {
	kv := strings.SplitN(value, ":", 2)
	if len(kv) != 2 || kv[0] == "" {
		return fmt.Errorf("Invalid value, expected k:v got: \"" + value + "\"")
	}
	(*i)[kv[0]] = kv[1]
	return nil
}

/*
	it would be nice for golang to support bit fields.
	Bit fields are very elegant solution, hovewer since golang doesnt support them,
	We must introduce a lot of code, which makes program slower and more complex.
	Thats why i decided to waste some memory and introduce separate boolean fields.
	In future if number of those fields will increase we could make it bit fields.
*/
type Args struct {
	ConfigPath   string
	Verbose      bool
	ExtraVerbose bool
	Execute      bool
	Raw          bool
	Demand       MapFlags
	Set          MapValues
}

func NewArgsFromCli() *Args {

	var c Args

	c.Demand = make(MapFlags)
	c.Set = make(MapValues)

	flag.StringVar(&c.ConfigPath, "c", "", "config path.")
	flag.BoolVar(&c.Execute, "e", false, "execute, if not specified then expect dry run, (nothing gets executed on database)")
	flag.BoolVar(&c.Verbose, "v", false, "verbosity - report progress as program runs")
	flag.BoolVar(&c.Raw, "r", false, "raw output - disable text formatting")
	flag.Var(&c.Demand, "demand", "specifies on-demand targets")
	flag.Var(&c.Demand, "d", "short for '-demand'")
	flag.Var(&c.Set, "set", "allows to set preprocessor variables from arguments.\n"+
		"Note that this will override existing preporcessor variable with same the key")
	flag.Var(&c.Set, "s", "short for '-set'")
	flag.BoolVar(&c.ExtraVerbose, "vv", false, "extra verbose")

	flag.Parse()

	if c.ExtraVerbose && !c.Verbose {
		c.Verbose = true
	}

	return &c
}
