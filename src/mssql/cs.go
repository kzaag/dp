package mssql

import (
	"fmt"
	"syscall"

	"github.com/kzaag/database-project/src/config"
	"golang.org/x/crypto/ssh/terminal"
)

func CreateCSFromConfig(auth *config.Auth) (string, error) {
	if auth.Name == "" {
		return "", fmt.Errorf("Anonymous auth record")
	}

	if auth.ConnectionString != "" {
		return auth.ConnectionString, nil
	}

	if auth.Password != "" {
		fmt.Printf("password for %s: ", auth.Name)
		bytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		auth.Password = string(bytes)
	}

	host := ""
	if auth.Server != "" {
		host = fmt.Sprintf("server=%s;", auth.Server)
	}
	user := ""
	if auth.User != "" {
		user = fmt.Sprintf("user id=%s;", auth.User)
	}
	password := ""
	if auth.Password != "" {
		password = fmt.Sprintf("password=%s;", auth.Password)
	}
	database := ""
	if auth.Database != "" {
		database = fmt.Sprintf("database=%s;", auth.Database)
	}

	return fmt.Sprintf(
		"%s%s%s%s%s",
		host,
		user,
		password,
		database,
	), nil
}
