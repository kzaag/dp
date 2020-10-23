package config

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

type Auth struct {
	ConnectionString string
	Server           string
	Database         string
	User             string
	Password         string
	Args             map[string]string
}

type Exec struct {
	Type string
	Path string
	Auth string
}

/*
	these arrays really should be hash tables.
	deal with it.
*/
type Data struct {
	Driver string
	Auth   map[string]*Auth
	Exec   []Exec
}

func New() *Data {
	return &Data{}
}

func CreateFromText(c *Data, j []byte) error {
	return yaml.Unmarshal(j, c)
	/*
		i dont really think that dp should support more than 1 config file format
		return json.Unmarshal(j, c)
	*/

}

func CreateFromPath(c *Data, configPath string) error {
	var bf []byte
	var err error
	var fpath string

	if configPath == "" {
		fs, err := ioutil.ReadDir(".")
		if err != nil {
			return err
		}
		for i := 0; i < len(fs); i++ {
			n := fs[i].Name()
			if strings.HasSuffix(n, ".json") {
				if bf, err = ioutil.ReadFile(n); err != nil {
					return err
				}
				fpath = n
				break
			}

			if i == len(fs)-1 {
				return fmt.Errorf("couldnt find config: *.json")
			}
		}
	} else {
		if bf, err = ioutil.ReadFile(configPath); err != nil {
			return err
		}
		fpath = configPath
	}

	if err = CreateFromText(c, bf); err != nil {
		return err
	}

	/* replace all relative paths to the absolute (relatively to config location) */
	if fpath, err = filepath.Abs(fpath); err != nil {
		return err
	}
	dir := filepath.Dir(fpath)
	for i := 0; i < len(c.Exec); i++ {
		if !path.IsAbs(c.Exec[i].Path) {
			c.Exec[i].Path = path.Join(dir, c.Exec[i].Path)
		}
	}

	return err
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
