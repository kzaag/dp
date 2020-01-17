package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

/*

parsing local schema to structs

*/

func IsDirectory(path string) (bool, error) {
	fs, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fs.IsDir(), nil
}

func ReadTables(dir string) ([]Table, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	length := len(files)
	defs := make([][]byte, length)
	allowedLen := 0
	for i := 0; i < length; i++ {
		name := path.Join(dir, files[i].Name())
		isDir, err := IsDirectory(name)
		if err != nil {
			return nil, err
		}
		if !isDir && strings.HasSuffix(name, ".json") {
			content, err := ioutil.ReadFile(name)
			defs[i] = content
			if len(content) == 0 {
				return nil, fmt.Errorf("empty file content")
			}
			if err != nil {
				return nil, err
			}
			allowedLen++
		} else {
			defs[i] = nil
		}
	}

	ret := make([]Table, allowedLen)
	ci := 0
	for i := 0; i < length; i++ {
		if defs[i] != nil {
			err = json.Unmarshal([]byte(defs[i]), &ret[ci])
			if err != nil {
				return nil, err
			}
			ci++
		}
	}

	return ret, nil
}
