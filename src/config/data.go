package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
)

type Auth struct {
	Name             string
	ConnectionString string
	Server           string
	Database         string
	User             string
	Password         string
}

type Data struct {
	Driver string
	Auth   []Auth
	Exec   []struct {
		Type string
		Path string
		Auth string
	}
}

func New() *Data {
	return &Data{}
}

func CreateFromText(c *Data, j []byte) error {
	return json.Unmarshal(j, c)
}

func CreateFromPath(c *Data, path string) error {
	var bf []byte
	var err error

	if path == "" {
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
				break
			}

			if i == len(fs)-1 {
				return fmt.Errorf("couldnt find config: *.json")
			}
		}
	} else {
		if bf, err = ioutil.ReadFile(path); err != nil {
			return err
		}
	}
	return CreateFromText(c, bf)
}
