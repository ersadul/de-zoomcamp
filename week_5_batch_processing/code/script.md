### VM spark
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/* \
    --input_yellow=data/pq/yellow/2020/* \
    --output=data/report-2020

URL = "spark://de-z.asia-southeast1-b.c.dte-de-375705.internal:7077"

spark-submit \
    --master="spark://de-z.asia-southeast1-b.c.dte-de-375705.internal:7077" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/* \
        --input_yellow=data/pq/yellow/2021/* \
        --output=data/report-2021

--input_green=gs://nytaxi-2019-2020/pq/green/2021/* \
--input_yellow=gs://nytaxi-2019-2020/pq/yellow/2021/* \
--output=gs://nytaxi-2019-2020/report-2021

### Cloud Spark
#### run a jobs with gcloud 

gcloud dataproc jobs submit pyspark \
    --cluster=de-cluster \
    --region=us-central1 \
    gs://nytaxi-2019-2020/code/06_spark_sql.py \
    -- \
        --input_green gs://nytaxi-2019-2020/pq/green/2021/* \
        --input_yellow gs://nytaxi-2019-2020/pq/yellow/2021/* \
        --output gs://nytaxi-2019-2020/report-2021

#### spark to bq

gcloud dataproc jobs submit pyspark \
    --cluster=de-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://nytaxi-2019-2020/code/06_spark_sql_big_query.py \
    -- \
        --input_green gs://nytaxi-2019-2020/pq/green/2020/* \
        --input_yellow gs://nytaxi-2019-2020/pq/yellow/2020/* \
        --output trips_data_all.reports-2020
