# de-zoomcamp
> *this repo was created based on [DE-Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) by DataTalksClub as a reference*


## Syllabus
   > Note: NYC TLC changed the format of the data we use to parquet [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). But you can still access the csv files [here](https://github.com/DataTalksClub/nyc-tlc-data).

### [Week 1: Introduction & Prerequisites](week_1_basics_n_setup/)

* Course overview
* Introduction to GCP
* Docker and docker-compose
* Running Postgres locally with Docker
* Setting up infrastructure on GCP with Terraform
* Preparing the environment for the course
* [Homework](homework/week_1/)

[More details](week_1_basics_n_setup/)

### [Week 2: Workflow Orchestration](week_2_workflow_orchestration/)

* Data Lake
* Workflow orchestration
* Introduction to Prefect
* ETL with GCP & Prefect
* Parametrizing workflows
* Prefect Cloud and additional resources
* [Homework](homework/week_2/homework.md)

[More details](week_2_workflow_orchestration/)

## Overview
### Architecture diagram
<img src="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/images/architecture/arch_2.png"/>

### Technologies
* *Google Cloud Platform (GCP)*: Cloud-based auto-scaling platform by Google
  * *Google Cloud Storage (GCS)*: Data Lake
  * *BigQuery*: Data Warehouse
* *Terraform*: Infrastructure-as-Code (IaC)
* *Docker*: Containerization
* *SQL*: Data Analysis & Exploration
* *Prefect*: Workflow Orchestration
* *dbt*: Data Transformation
* *Spark*: Distributed Processing
* *Kafka*: Streaming
