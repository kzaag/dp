database-project 

schema versioning and deployement tool

1. install https://golang.org/doc/install

2. prep golang environment:
    go get github.com/denisenkom/go-mssqldb
    go get github.com/lib/pq

3. prepare config file - consider that tool is not tested beyond mssql 2017 14.0 & pqsql 12.1
    mv conf.pgsql.example main.conf
    or
    mv conf.mssql.example main.conf
    
    fill up credentials in main.conf

5. build project: bash build.sh

6. cd bin && ./dp

7. verify changes and execute on target database connection using ./dp -e
