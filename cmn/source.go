package cmn

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

func ParserIterateOverSource(
	sourcePath string,
	cb func(path string, fc []byte, args interface{}) error,
	args interface{}) error {

	var err error
	var fi os.FileInfo
	var di []os.FileInfo
	var fc []byte

	if fi, err = os.Stat(sourcePath); err != nil {
		return err
	}

	if fi.IsDir() {
		if di, err = ioutil.ReadDir(sourcePath); err != nil {
			return err
		}
		for _, fi = range di {
			if err = ParserIterateOverSource(
				path.Join(sourcePath, fi.Name()),
				cb, args); err != nil {
				return err
			}
		}
		return err
	}

	if fc, err = ioutil.ReadFile(sourcePath); err != nil {
		return err
	}
	if len(fc) == 0 {
		return fmt.Errorf("%s - empty file content", sourcePath)
	}
	return cb(sourcePath, fc, args)
}
