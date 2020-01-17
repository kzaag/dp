#database-project 

intended to be database versioning tool.
similar to ssdt but opensource, xplatform, and xdbms

1. install https://golang.org/doc/install

2. prep golang environment:
    go get github.com/denisenkom/go-mssqldb

3. prepare config file
    mv conf.example main.conf
    fill up credentials in main.conf

5. build project: bash build.sh

6. cd bin && ./dp

7. verify changes and execute on target database connection using ./dp -e
