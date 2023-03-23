# Week 5: Batch Processing

## Tips/trick
* For understanding about spark architecture and core concept you can go through Alexey videos. you can also watch youtube video about [pyspark by freeCodeCamp](https://youtu.be/_C8kWso4ne4) for technical things and efficient work in doing things (example: you dont need to create your own schema because its by default will be StringType for every dataframe you read). 
* if you have problems using gsutil to move your data from vm to bucket you can go [here](https://gist.github.com/ryderdamen/926518ddddd46dd4c8c2e4ef5167243d)
* Spark Documentation for guiding to know spark well [here](https://spark.apache.org/docs/latest/)

## Setup
here some guide to [install spark on vm](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_5_batch_processing/setup)
there is two markdown to follow, first installing the spark. and the following markdown will install pyspark

## My alternative setup pyspark (easy for me)
install anaconda first, if you want to create virtual env you can create it and do the rest of step on it
```
curl -O https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh
```
install java 
```
conda install openjdk
```
install pyspark
```
conda install pyspark
```
install findspark
```
conda install -c conda-forge findspark
```
if you have problem with installing pyspark using conda you can using pip to install pyspark. first make sure you have pip installed on your machine
```
sudo apt install pip
```
install pyspark
```
pip install pyspark
```
install findspark 
```
pip install findspark
```

## Spark on Cloud
### Connecting to google cloud storage
you can download connector here https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage
or download it using gsutil to be a connector between your spark and cloud storage.
to see the detail steps you can go through the [notebook file](code/09_spark_gcs.ipynb)

### Creating local spark cluster
at first you should have spark and java installed in machine. if you dont go here to [install spark on vm](#setup).
and you can go to the folder 
```
cd $SPARK_HOME
```
start spark standalone mode
```
./sbin/start-master.sh
```
to open the UI you can do port forwarding first if you using ssh remote with `8080` and access it in browser `address:8080`  
  
start the worker 
```
./sbin/start-worker.sh <master-spark-URL>

# if you use older version of spark use this
./sbin/start-slave.sh <master-spark-URL>
```
here, I do the next step in [notebook](code/06_spark_sql.ipynb) in a very first cell

after we done using spark master and workers, we can shut it down by:
```
cd $SPARK_HOME
./sbin/stop-master.sh 
./sbin/stop-worker.sh 
```

### Setting up a Dataproc Cluster
1. via console
first thing we need to enable Dataproc API
then you can upload your script to bucket and run a job via Console there in Dataproc

2. via gcloud cli
you can go here as [reference](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud)
template:
```
gcloud dataproc jobs submit job-command \
    --cluster=cluster-name \
    --region=region \
    other dataproc-flags \
    -- job-args
```
you can see the modified version in [script.md](code/script.md)

### Connecting Spark to Big Query
you can use docs as [reference](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example)
