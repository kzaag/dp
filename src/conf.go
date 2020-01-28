package main

import (
	"container/list"
	"fmt"
	"io/ioutil"
	"strings"
)

var ConfReserved = []string{"before", "after"}

type Config struct {
	Before []string
	After  []string
	Values map[string]string
}

type ScriptSpec struct {
	Path   string
	Values map[string]string
}

func ConfScriptSpec(spec string) (*ScriptSpec, error) {
	var ret ScriptSpec
	ret.Values = make(map[string]string)

	parts := strings.Split(spec, " ")

	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid script specification.")
	}

	ret.Path = parts[0]
	for i := 1; i < len(parts); i++ {
		if parts[i] == "" {
			continue
		}
		args := strings.Split(parts[i], "=")
		if len(args) != 2 {
			return nil, fmt.Errorf("invalid argument in spec. expected: {key}={value} got " + parts[i])
		}
		ret.Values[args[0]] = args[1]
	}

	return &ret, nil
}

func (c Config) Get(key string) (string, error) {
	ret := c.Values[key]
	if ret == "" {
		return "", fmt.Errorf("key '" + key + "' not found in config")
	}
	return ret, nil
}

func (c Config) SqlCs() (string, error) {
	server := c.Values["server"]
	db := c.Values["database"]
	password := c.Values["password"]
	user := c.Values["user"]

	if server == "" {
		return "", fmt.Errorf("field: 'server' was not present in config")
	}

	if db == "" {
		return "", fmt.Errorf("field: 'database' was not present in config")
	}

	if password == "" {
		return "", fmt.Errorf("field: 'password' was not present in config")
	}

	if user == "" {
		return "", fmt.Errorf("field: 'user' was not present in config")
	}

	return "server=" + server +
		";user id=" + user +
		";password=" + password +
		";database=" + db +
		";", nil
}

func (c Config) PgCs() (string, error) {
	server := c.Values["server"]
	db := c.Values["database"]
	password := c.Values["password"]
	user := c.Values["user"]

	if server == "" {
		return "", fmt.Errorf("field: 'server' was not present in config")
	}

	if db == "" {
		return "", fmt.Errorf("field: 'database' was not present in config")
	}

	if password == "" {
		return "", fmt.Errorf("field: 'password' was not present in config")
	}

	if user == "" {
		return "", fmt.Errorf("field: 'user' was not present in config")
	}

	return "host=" + server +
		" user=" + user +
		" password=" + password +
		" dbname=" + db, nil
}

func ConfNew() *Config {
	return &Config{nil, nil, nil}
}

func ConfListToArr(list list.List) []string {
	ret := make([]string, list.Len())
	for i, x := 0, list.Front(); x != nil; i, x = i+1, x.Next() {
		ret[i] = x.Value.(string)
	}
	return ret
}

func ConfGetKeyCount(keys []string, key string) int {
	c := 0
	for i := 0; i < len(keys); i++ {
		if keys[i] == key {
			c++
		}
	}
	return c
}

func ConfCleanKey(key *string) {
	*key = strings.Replace(*key, " ", "", -1)
	*key = strings.ToLower(*key)
}

func ConfParseKeyVal(conf string) ([]string, []string, error) {
	lines := strings.Split(conf, "\n")

	clen := len(lines)
	keys := make([]string, clen)
	vals := make([]string, clen)
	for i := 0; i < clen; i++ {

		if lines[i] == "" {
			continue
		}

		split := strings.SplitN(lines[i], "=", 2)
		if len(split) != 2 || split[0] == "" {
			return nil, nil, fmt.Errorf("error: malformed config entry - " + lines[i])
		}

		ConfCleanKey(&split[0])

		if strings.HasPrefix(split[0], "#") {
			continue
		}

		keys[i] = split[0]
		vals[i] = split[1]
	}

	return keys, vals, nil
}

func ConfGetArray(key string, keys []string, vals []string) []string {
	kcount := ConfGetKeyCount(keys, key)
	count := len(keys)
	ret := make([]string, kcount)
	kix := 0
	for i := 0; i < count; i++ {
		if keys[i] == key {
			ret[kix] = vals[i]
			kix++
		}
	}
	return ret
}

func ConfReservedContains(key string) bool {
	for i := 0; i < len(ConfReserved); i++ {
		if ConfReserved[i] == key {
			return true
		}
	}
	return false
}

func ConfGetCustom(keys []string, vals []string) map[string]string {
	ret := make(map[string]string)
	count := len(keys)
	for i := 0; i < count; i++ {
		if ConfReservedContains(keys[i]) {
			continue
		}
		ret[keys[i]] = vals[i]
	}
	return ret
}

func ConfInit(c *Config, path string) error {
	var strc string = ""
	if path == "" {
		fs, err := ioutil.ReadDir(".")
		if err != nil {
			return err
		}
		for i := 0; i < len(fs); i++ {
			n := fs[i].Name()
			if strings.HasPrefix(n, "main.") && strings.HasSuffix(n, ".conf") {
				if bf, err := ioutil.ReadFile(n); err != nil {
					return err
				} else {
					strc = string(bf)
				}
				break
			}

			if i == len(fs)-1 {
				return fmt.Errorf("couldnt find config: main.*.conf")
			}
		}
	} else {
		if bf, err := ioutil.ReadFile(path); err != nil {
			return err
		} else {
			strc = string(bf)
		}
	}

	keys, values, err := ConfParseKeyVal(strc)
	if err != nil {
		return err
	}

	c.Before = ConfGetArray("before", keys, values)
	c.After = ConfGetArray("after", keys, values)
	c.Values = ConfGetCustom(keys, values)

	return nil
}
