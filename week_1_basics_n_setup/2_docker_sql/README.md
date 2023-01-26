**Link data and schema**:

https://github.com/DataTalksClub/nyc-tlc-data
https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

**Run a postgres in docker container with volumes**
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v d:/"learning and code"/de-zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

**Connect to postgres using pgcli**
```
pgcli -h host.docker.internal -p 5432 -u root -d ny_taxi
pgcli -h localhost -p 5432 -u root -d ny_taxi
```
**Inspect data**
```
less yellow_tripdata_2021-01.csv
head -n 100 yellow_tripdata_2021-01.csv > yellow_head.csv
wc -l yellow_tripdata_2021-01.csv
```
> *less*: show the file content at the screen
> 
> *head*: show -n number of line of the file 
> 
> *wc* (wordcount) (-l: line):  to show how many line the file have

**Run pgadmin in container**
```
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

**Create network**
```
docker network create pg-network
```

**Integrate containers: postgres and pgadmin in network**
```
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4
```
```
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v d:/"learning and code"/de-zoomcamp/week_1_basics_n_setup/2_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```
Add ```--network=pg-network``` in those script. And add ```--name pg-database``` in postgres for addressing purpose(for pgAdmin)

**Convert nb to py script**
```
jupyter nbconvert --to=script upload-data.ipynb
```
**Ingest data**
```
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url="yellow_tripdata_2021-01.csv"
```
**Build image from Dockerfile**
```
docker build -t taxi_ingest:v001 .
```
```
docker run -it --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --db=ny_taxi \
        --table_name=yellow_taxi_trips \
        --url="yellow_tripdata_2021-01.csv"
```
**Docker compose syntax**
```
docker-compose up
docker-compose up -d
docker-compose down
```
> docker-compose up: run all the configuration in docker-compose file
> 
> docker-compose down: stop the running container
> 
> -d(detach): running the container in "the background"

**Ingest data into database**
```
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url="green_tripdata_2019-01.csv"
```