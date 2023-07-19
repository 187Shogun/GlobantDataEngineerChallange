## Globant Data Engineer Challange

This project contains assets to build a set of ETL pipelines 
and Data Warehouse. The ETL pipelines are implemented in Python
using Google Cloud services to stage, move and transform data. 
BigQuery is the data warehouse service where DBT is also implemented
to build transformation pipelines. 

A set of `cronjobs` are implemented to schedule ETL functions 
using a Kubernetes cluster. The CronJobs run on set schedule but
also allow for manual execution from the console page. 

https://console.cloud.google.com/kubernetes/workload/overview

The `python` folder contains both the DBT and ETL objects. 
The folder is used to build a docker image in the cloud where
it is later used by Kubernetes to execute the jobs. Any pull
request to the master branch will trigger a CI/CD pipeline
to update the docker image. 

