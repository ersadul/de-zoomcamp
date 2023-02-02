## Week 2: Workflow Orchestration

<details open>
<summary>How to</summary>

### Virtual env
#### Create virtual env
```
python -m venv .venv
```
#### Activate venv
```
source .venv/Scripts/activate
```
#### Install dependencies from file
```
pip install -r requirements.txt
```

### Deploy
you can explore more about flow deployment in Prefect [docs](https://docs.prefect.io/concepts/deployments/)

#### Create deployment
```
prefect deployment build flows/03_deployments/parameterized_flow.py:etl_parent_flow -n "Parameterized ETL"
```
deploy a flow with 'parameterized_flow.py' file to run 'etl_parent_flow' function as flow and named it with "Parameterized ETL"  
#### Create deployment with cron
```
$ prefect deployment build flows/03_deployments/parameterized_flow.py:etl_parent_flow --cron "*/5 * * * *" -n "parameterized_with_cron"
```  
run flow every 5 minute  
#### Apply deployment to server (local)
```
prefect deployment apply etl_parent_flow-deployment.yaml
```  
#### Start agent to execute the flow 
in case, our deployment in the "default" queue
```
prefect agent start  --work-queue "default"
```
#### Build an image
```
docker image build -t ersadul/prefect:zoom .
```
#### Run deployed flow with custom parameter
```
prefect deployment run etl-parent-flow/docker-flow -p "months=[1, 2]"
```

### Services
#### Start server
```
prefect orion start
```
#### Start agent
```
prefect agent start -q "default"
```
> agent listen every flow in "default" queue

#### Setting profile config

* use a local Orion API server
```
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```
* use Prefect Cloud
```
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"
```

</details>

### Data Lake (GCS)

* What is a Data Lake
* ELT vs. ETL
* Alternatives to components (S3/HDFS, Redshift, Snowflake etc.)

### 1. Introduction to Workflow orchestration

* What is orchestration?
* Workflow orchestrators vs. other types of orchestrators
* Core features of a workflow orchestration tool
* Different types of workflow orchestration tools that currently exist 


### 2. Introduction to Prefect concepts

* What is Prefect?
* Installing Prefect
* Prefect flow
* Creating an ETL
* Prefect task
* Blocks and collections
* Orion UI

[Source code](flows/01_start/)

### 3. ETL with GCP & Prefect

* Flow 1: Putting data to Google Cloud Storage 

[Source code](flows/02_gcp/etl_web_to_gcs.py)


### 4. From Google Cloud Storage to Big Query

* Flow 2: From GCS to BigQuery

[Source code](flows/02_gcp/etl_gcs_to_bq.py)

### 5. Parametrizing Flow & Deployments 

* Parametrizing the script from your flow
* Parameter validation with Pydantic
* Creating a deployment locally
* Setting up Prefect Agent
* Running the flow
* Notifications

[Source code](flows/03_deployments/parameterized_flow.py)

### 6. Schedules & Docker Storage with Infrastructure

* Scheduling a deployment
* Flow code storage
* Running tasks in Docker

[Source code](flows/03_deployments/docker_deploy.py)

### 7. Prefect Cloud and Additional Resources 


* Using Prefect Cloud instead of local Prefect
* Workspaces
* Running flows on GCP


### Code source
[course repo](https://github.com/discdiver/prefect-zoomcamp) by Jeff Hale (Prefect instructor)

### Homework
[link](../homework/week_2/homework.md) to my homework
