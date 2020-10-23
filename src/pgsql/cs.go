package pgsql

import (
	"database/sql"
	"fmt"
	"syscall"

	"database-project/config"

	"golang.org/x/crypto/ssh/terminal"
)

func CSCreateFromConfig(target *config.Target) (string, error) {
	if target.Name == "" {
		return "", fmt.Errorf("Encountered anonymous auth record")
	}

	if target.ConnectionString != "" {
		return target.ConnectionString, nil
	}

	if target.Password == "" {
		fmt.Printf("password for %s: ", target.Name)
		bytes, err := terminal.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return "", err
		}
		fmt.Println()
		target.Password = string(bytes)
	}

	host := ""
	if target.Server != "" {
		host = fmt.Sprintf("host=%s", target.Server)
	}
	user := ""
	if target.User != "" {
		user = fmt.Sprintf("user=%s", target.User)
	}
	password := ""
	if target.Password != "" {
		password = fmt.Sprintf("password=%s", target.Password)
	}
	database := ""
	if target.Database != "" {
		database = fmt.Sprintf("dbname=%s", target.Database)
	}

	cs := fmt.Sprintf(
		"%s %s %s %s",
		host,
		user,
		password,
		database)

	if target.Args != nil {
		for k := range target.Args {
			cs += fmt.Sprintf(" %s=%s", k, target.Args[k])
		}
	}

	return cs, nil
}

func CSCreateDBfromConfig(target *config.Target) (*sql.DB, error) {
	var cs string
	var err error

	if cs, err = CSCreateFromConfig(target); err != nil {
		return nil, err
	}

	return sql.Open("postgres", cs)
}
