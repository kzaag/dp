
# database-project 

schema versioning and deployement

## Get started

### Postgresql
    
You can use following command to run pg server in docker container in terminal.  
```sudo docker run -it --rm --net host -e POSTGRES_PASSWORD=postgres --name dppg postgres;```  
go to the path with cloned project, build it and enter output directory:   
```make && cd bin```  
now to execute dp type in following   
```./dp -c pg/conf.yml -v ```  
- -c    the configuration file path. You can provide directory (then first found .yml file will be used  
- -v    Some extra-verbose logging.  
      
> Note that database dp will be created during this process,  
> because in configuration file im overriding user -e (execute) flag, for 'create database' statement  
> other queries will not be executed in dry run.  
  
that will generate queries which are to be executed.  
after verification you can execute those queries on target server  
```./dp -c pg/conf.yml -v -e```  
- -e : execute queries instead of generating output  
  

### MsSql
work in progress (unstable and is lacking features)  

### Cassandra
Work in progress (unstable and is lacking features)