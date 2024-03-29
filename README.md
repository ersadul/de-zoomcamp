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

### [Week 3: Data Warehouse](week_3_data_warehouse/)


* Data Warehouse
* BigQuery
* Partitioning and clustering
* BigQuery best practices
* Internals of BigQuery
* BigQuery Machine Learning
* [Homework](homework/week_3/homework.md)

[More details](week_3_data_warehouse/)

### [Week 4: Analytics engineering](week_4_analytics_engineering/)

* Basics of analytics engineering
* dbt (data build tool)
* BigQuery and dbt
* Postgres and dbt
* dbt models
* Testing and documenting
* Deployment to the cloud and locally
* Visualizing the data with google data studio and metabase
* [Homework](homework/week_4/homework.md)

[More details](week_4_analytics_engineering/)

### [Week 5: Batch processing](week_5_batch_processing/)

* Batch processing
* What is Spark
* Spark Dataframes
* Spark SQL
* Internals: GroupBy and joins
* [Homework](homework/week_5/homework.md)

[More details](week_5_batch_processing/)

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
