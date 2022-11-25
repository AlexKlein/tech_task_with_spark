# Solution of some tech test

In this project I showed my coding skills.

## Description

This repository consists of:

```
- An ETL process for:
    - generating parquet files;
    - ETL data from files to a PostgreSQL DB using PySpark;
    - transforming the data and loading it from core to datamart layers using SQL;
- SQL script which creates:
    - database schemas;
    - tables;
- SQL queries for filling data in a datamart layer;
- Airflow DAG for scheduling this application;
- Docker files for wrapping this tool.
```

## Classes

Also, the project contains several classes, there are:
1. `logger` - for logging actions in the log file;
2. `singleton` - for making only one log file in the application;
3. `postgres_wrapper` - for making work with the database easier.

## Log file

You should make several steps for getting log file.
1. `docker ps -a` - you find container ID;
2. `docker exec -it {container_id} bash` - you get into Airflow docker container;
3. `tail -n 100 /tmp/etl_project/log/output.log` - you read last 100 lines of the log file (you may use `cat` command and read whole log file).

## Airflow
As the scheduler I choose Airflow. Also, I used "host" network that's why the link for the Airflow UI is http://192.168.99.100:8080/admin/ (if your "host" network has another IP address then you should use yours).

## Build

When you need to start the app with all infrastructure, you have to make this steps:
1. Change environment variables in [YML-file](./project/docker-compose.yml) (now the default values are) 
2. Execute the `docker-compose up -d --build` command - in this step the app will be built, tables will be created in the DB and ETL process will be started.

### Note

You should wait a couple of minutes for the database and Airflow webserer start. After that you may run the application and check logs as the next step.

## Concept of the ETL process

The app generates parquet files into file storage system of a container (it can be changed to AWS S3/Hadoop/etc.). After that data store in PostgreSQL DB aggregated by product and feature or release using PySpark. 
I don't aggregate data by month or year in this (core) layer because raw data can be updated up to five days back. And in the end data transform with aggregations for answering buisness questions.

## Layers

### Raw layer

- [5 parquet files](./project/utils/generator.py) are generated in a file storage system.

### Core layer

- [feature_retention_rate](./project/etl_core/feature_week_retention_rate_diff.py) table stores filtered and flatted data. It shows users who used feature for the first time and after 7 days;
- [number_of_new_users_use_feature](./project/etl_core/fraction_of_new_users_use_feature_14_days.py) table stores filtered and flatted data. It shows a number of new users and a number of unique users who used a feature during 2 weeks. This dataset aggregated by feature and product;
- [number_of_users_upgraded_in_one_month](./project/etl_core/fraction_of_users_upgraded_in_one_month.py) table stores filtered and flatted data. It shows a number of users and a number of unique users who upgraded products. This dataset aggregated by release and product.

### Datamart layer

- [feature_retention_rate](./project/etl_datamart/sql/feature_week_retention_rate_diff.sql) table shows retention rate for a product and a feature in a month. Also, it shows difference between two sequential features;
- [fraction_of_new_users_use_feature](./project/etl_datamart/sql/fraction_of_new_users_use_feature.sql) table shows fraction of new users who use a feature last 14 days and it's aggregated by month;
- [fraction_of_users_upgraded_in_one_month](./project/etl_datamart/sql/fraction_of_users_upgraded_in_one_month.sql) table shows fraction of users who upgraded a product in a month.

## Q&A

- Q: How the project build time depends on the technologies used in the project?
- A: In general, if we increae a number of technologies and complexity of integrations between them then the build time increases as well. But if we take a look in details then it can be not like this. The initial build can be the long one but 
if we use e.g. Docker image then it takes cached layers and rebuild only changed layers. Or if we use programming language as Python then we can use `volumes` and not rebuild the project with minor changes.

## Potential improvements

- Add a system for working with log files, e.g. Splunk or ELK.
- Add a visualization system, e.g. Tableau or Qlik Sense.
- Add a separate Spark master and Spark workers for processing data.
- Add Hadoop namenode and datanodes for storring data.
- Add a tool for deploying SQL scripts such as Flyway.
- Cover this application by tests.
