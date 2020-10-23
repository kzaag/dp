package pgsql

import (
	"database/sql"
	"fmt"
	"syscall"

	"database-project/config"

	"golang.org/x/crypto/ssh/terminal"
)

func CSCreateDBfromConfig(name string, auth *config.Auth) (*sql.DB, error) {
	var cs string
	var err error

	if cs, err = CSCreateFromConfig(name, auth); err != nil {
		return nil, err
	}

	return sql.Open("postgres", cs)
}

func CSCreateFromConfig(name string, auth *config.Auth) (string, error) {
	if name == "" {
		return "", fmt.Errorf("Encountered anonymous auth record")
	}

	if auth.ConnectionString != "" {
		return auth.ConnectionString, nil
	}

	if auth.Password == "" {
		fmt.Printf("password for %s: ", name)
		bytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		auth.Password = string(bytes)
	}

	host := ""
	if auth.Server != "" {
		host = fmt.Sprintf("host=%s", auth.Server)
	}
	user := ""
	if auth.User != "" {
		user = fmt.Sprintf("user=%s", auth.User)
	}
	password := ""
	if auth.Password != "" {
		password = fmt.Sprintf("password=%s", auth.Password)
	}
	database := ""
	if auth.Database != "" {
		database = fmt.Sprintf("dbname=%s", auth.Database)
	}

	cs := fmt.Sprintf(
		"%s %s %s %s",
		host,
		user,
		password,
		database)

	if auth.Args != nil {
		for k := range auth.Args {
			cs += fmt.Sprintf(" %s=%s", k, auth.Args[k])
		}
	}

	return cs, nil
}
