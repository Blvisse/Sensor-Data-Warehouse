# Sensor-Data-Warehouse
## Background
With increasing size,compleity and demand for data there is need for proper data piplines to ease data retrival. Traditional ETL models are falling behind to modern data problems as it increases in size and need for real-time data.

The project aims to create a proper data-pipeline that uses data from sensors collected on I80 highway, and through this carry out processing and delivering a report on the data

## Technologies used
1. Apache-Airflow
2. MYSQL
3. DBT
4. Spark
5. Redash

## Folder structure
1. airflow: This folder contains airflow setup and dag files used to periodically insert data into the datawarehouse
2. log: system logs
3. scripts: This contains scripts that insert , create and update tables in the warehouse
4. sensor_warehouse: contains dbt istallation and db schemas

## Installation requirements

### dbt
This database build tool is a powerful library that we shall use to carry out data trabsformation. To use it :
1. Navigate to the sensor_warehouse folder 
2. run the following command
3. ``` docker-compose up ``` 
4. To check if dbt is succefully installed run ``dbt version``
7. check to see that the installation didn;t bring any errors 
8. run `` dbt debug `` 
9. To run specified tests run `` dbt tests ``

### view lineage graph
To view the lineage graph and dependancies between data run ```dbt generate docs```

### airflow

Airflow is used to schedule jobs. It creates an updated stream of data into our warehouse
1. Navigate to the airflow folder
2. run `` docker-compose up ``

### Redash
Redash is our peorting techlogy. It creates a front-end whereby a ds can run queries on the data 
1. Navigate to redash folder
2. run `` docker-compose up ``

