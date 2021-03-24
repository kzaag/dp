package target

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

/*
	preconfig is getting parsed before actual config.
	preconfig must be contained in any config structure no matter version, it contains version meta data and preprocessor directives
*/
type PreConfig struct {
	Version string
	/*
		stupid-simple version of C preprocessor ( but in yaml )

		for example if you place:

		defines:
			- db : my_database
			- foo: bar

		will replace any occurrence of
		${db} with my_database and
		${foo} with bar
		ANYWHERE in dp files. It is like preprocessor ->
			dp-file gets loaded (it may be script, table, type and so on...)
			all defines are being 'evaluated' from top to bottom
			file is parsed into yaml structure / executed if sql and so on...

		it also allows for more advanced usage like so:

		defines:
			- db : test_db
			- drop_db : drop database ${db}
			- create_db: create database ${db}

		...

		targets:
			- name: foo
			  ....
			  exec:
			    - type: stmt
				  args: ["${drop_db}", "${create_db}"]

	*/
	Defines []map[string]string
}

/*
	these arrays really should be hash tables.
	deal with it.
*/
type Config struct {
	Driver    string
	Base      string
	Targets   []*Target
	PreConfig `yaml:",inline"`
}

const notFoundMsg = "stat *.yml: no such file or directory"

func EvaluateDefines(c *PreConfig, f []byte) ([]byte, error) {

	for i := range c.Defines {
		for k := range c.Defines[i] {
			f = bytes.Replace(f, []byte("${"+k+"}"), []byte(c.Defines[i][k]), -1)
		}
	}

	return f, nil
}

func MergeUserDefines(pc *PreConfig, uargv *Args) {
	if uargv != nil && uargv.Set != nil {
		for i := range pc.Defines {
			for c := range pc.Defines[i] {
				if v, ok := uargv.Set[c]; ok {
					pc.Defines[i][c] = v
				}
			}
		}
	}
}

func ExpandDefines(pc *PreConfig) {
	for i := range pc.Defines {
		for k, v := range pc.Defines[i] {
			for j := range pc.Defines {
				for k2, v2 := range pc.Defines[j] {
					if k2 != k {
						strings.Replace(v2, "${"+k+"}", v, -1)
					}
				}
			}
		}
	}
}

func prepareConfig(j []byte, uargv *Args) (*PreConfig, []byte, error) {
	var pc PreConfig
	if err := yaml.Unmarshal(j, &pc); err != nil {
		return nil, nil, err
	}

	if pc.Version != Version {
		return nil, nil, fmt.Errorf(
			"dp: config requested version %s which is incompatible with current module version %s.\nUpgrade your dp version",
			pc.Version, Version)
	}

	for i := range pc.Defines {
		if len(pc.Defines[i]) != 1 {
			return nil, nil, fmt.Errorf("invalid define at index %d\n\tDefines must be array of maps with only 1 record (they must be tupples)", i)
		}
	}

	// append defines from uargv into config defines.
	// if exists then replace
	MergeUserDefines(&pc, uargv)

	// evalute defines within pre config
	ExpandDefines(&pc)

	jc, err := EvaluateDefines(&pc, j)

	return &pc, jc, err
}

func createFromText(c *Config, j []byte, uargv *Args) error {

	pc, j, err := prepareConfig(j, uargv)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(j, c)

	if err != nil {
		return err
	}

	c.PreConfig = *pc

	return nil
}

func getBufFromDir(dir string) (string, []byte, error) {

	var bf []byte

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return "", nil, err
	}

	for i := 0; i < len(fs); i++ {
		n := path.Join(dir, fs[i].Name())
		if strings.HasSuffix(n, ".yml") {
			if bf, err = ioutil.ReadFile(n); err != nil {
				return "", nil, err
			}
			return n, bf, nil
		}
	}

	return "", nil, fmt.Errorf(notFoundMsg)
}

func NewConfigFromPath(configPath string, uargv *Args) (*Config, error) {
	var bf []byte
	var err error
	var fpath string
	var c Config
	var fi os.FileInfo

	if configPath == "" {
		if fpath, bf, err = getBufFromDir("."); err != nil {
			return nil, err
		}
	} else {
		if fi, err = os.Stat(configPath); err != nil {
			return nil, err
		}
		if fi.IsDir() {
			if fpath, bf, err = getBufFromDir(configPath); err != nil {
				return nil, err
			}
		} else {
			if bf, err = ioutil.ReadFile(configPath); err != nil {
				return nil, err
			}
			fpath = configPath
		}
	}

	if err = createFromText(&c, bf, uargv); err != nil {
		return nil, err
	}

	if c.Base == "" {
		if fpath, err = filepath.Abs(fpath); err != nil {
			return nil, err
		}
		dir := filepath.Dir(fpath)
		c.Base = dir
	}

	return &c, err
}

// func FindExecWithAuth(data *Data, execType string) *Exec {
// 	var ret *Exec = nil
// 	var i int
// 	for i = 0; i < len(data.Exec); i++ {
// 		if data.Exec[i].Type == execType {
// 			ret = &data.Exec[i]
// 			break
// 		}
// 	}
// 	if ret == nil {
// 		return ret
// 	}
// 	for i = 0; i < len(data.Auth); i++ {
// 		if data.Auth[i].Name == ret.Auth {
// 			ret.AuthPtr = &data.Auth[i]
// 		}
// 	}
// 	if ret.AuthPtr == nil {
// 		/* assign default auth if none is specified by user */
// 		if ret.Auth == "" && len(data.Auth) > 0 {
// 			ret.AuthPtr = &data.Auth[0]
// 		} else {
// 			return nil
// 		}
// 	}
// 	return ret
// }
