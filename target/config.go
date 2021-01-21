package target

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

/*
	these arrays really should be hash tables.
	deal with it.
*/
type Config struct {
	Version string
	Driver  string
	Base    string
	Targets []*Target
}

const notFoundMsg = "stat *.yml: no such file or directory"

func createFromText(c *Config, j []byte) error {
	return yaml.Unmarshal(j, c)
	/*
		i dont think that dp should support more than 1 config file format
		return json.Unmarshal(j, c)
	*/

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

func NewConfigFromPath(configPath string) (*Config, error) {
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

	if err = createFromText(&c, bf); err != nil {
		return nil, err
	}

	if c.Base == "" {
		if fpath, err = filepath.Abs(fpath); err != nil {
			return nil, err
		}
		dir := filepath.Dir(fpath)
		c.Base = dir
	}

	if c.Version != Version {
		return nil, fmt.Errorf(
			"Config requested version %s which is incompatible with current module version %s",
			c.Version, Version)
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
