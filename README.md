# Raizen Fuel Data

## Requirements:
- Docker Compose v1.29.1 and newer.

#### Checking docker-compose version:
```
$ docker-compose version
```

### Upgrade docker-compose version:  
   
Uninstall you current docker-compose:  

https://docs.docker.com/compose/install/uninstall/

- Uninstall docker-compose on Ubuntu:
```
// If installed via apt-get
$ sudo apt-get remove docker-compose
// If installed via curl
$ sudo rm /usr/local/bin/docker-compose
//If installed via pip
$ pip uninstall docker-compose
```

https://docs.docker.com/desktop/linux/install/

- Installing docker-compose on Ubuntu:
```
$ apt-get install docker-compose
```


## 1st RUN:

On Linux, the quick-start needs to know your host user id:

```
$ echo -e "AIRFLOW_UID=$(id -u)" > .env
```

## Executing docker-compose

Execute in git-clone repositore folder:

```
$ docker compose up -d
```

## App

The process used in this pipeline was ELT. As we were extrating the data from URL, and that could be inaccessible at any time, I collect the data and transform in a JSON file(Parquet would be a better option) to store the raw data in a cheap storage as https://www.backblaze.com/. After that I load in PostgreSQL to transform and use the data.

## Airflow credentials

Acess airflow on link: http://www.localhost:8080

Login: airflow  
Password: airflow

## Improvements

### Cloud

In order to have more secure an a more complete ecosytem, the use of Cloud would help to increase the possibilites of the project 

### BigQuery

Instead of using PostgreSQL to build the DW, BigQuery would be the most fitable option, one of the most complete plataform for the purpose. With BQ we can hit a better performance, be more scalable and not worry from the hardware problems.

### Soda

Soda.io is an open-source and user-friendly Data Observability plataform whos woul increase the quality and trustability of our data. 