***links***

how to setup dbt using [venv](https://docs.getdbt.com/faqs/Core/install-pip-best-practices.md#using-virtual-environments)  
how to [install dbt with pip](https://docs.getdbt.com/docs/get-started/pip-install)  
how to [install dbt from source](https://docs.getdbt.com/docs/get-started/source-install)  
tutorial video: [intro to dbt(create first project!) by Kahan Data Solutions](https://youtu.be/5rNquRnNb4E)  
article about [ref() and source in dbt](https://stackoverflow.com/questions/73784913/dbt-ref-vs-source#:~:text=Using%20ref%20creates%20the%20lineage,tables%20ingested%20into%20your%20DWH)

# Analytics Engineering

## Setup environtment
### Create venv
python -m venv dbt-env

### Activate venv 
source dbt-env/Scripts/activate

### Install dependencies
	> dbt-core is mandate and choose adapter plugins you need
    > example: pip install dbt-core dbt-bigquery

```
pip install \
  dbt-core \
  dbt-postgres \
  dbt-redshift \
  dbt-snowflake \
  dbt-bigquery \
  dbt-trino
```

### Alternative way 
you can see the details [here](https://docs.getdbt.com/docs/get-started/source-install), to install dbt from github
#### install dbt-core
```
git clone https://github.com/dbt-labs/dbt-core.git
cd dbt-core
pip install -r requirements.txt
```

#### install adapter plugins
```
git clone https://github.com/dbt-labs/dbt-redshift.git
cd dbt-redshift
pip install .
```

## dbt
First thing you must to do is create a repo or remote repo to github. And you can build your project there. I dont know why we need to create github repo first, dbt docs doesn't explain that.
### create dbt project
```
dbt init {project name}
```
### config dbt profile
change configuration in `profile.yml`to match the project you have created
```
code ~/.dbt/profiles.yml
```
### Check config and connection to DW
you need to concern about the granularity folder, because to run a debug and models, dbt want you in a same folder with `dbt_project.yaml` file.
```
dbt debug
```
### Run all models
running all models that available in the module folder
```
dbt run
```
### Run specific models
runing example models
```
dbt run --select my_first_dbt_model.sql
```
or
```
dbt run -m my_first_dbt_model.sql
```
### Build
build command will: run models, test, snapshot, seeds
```
dbt build -t prod --var 'is_test_run: false'
```
  > -t: target in profile
  > --var: variable we have defined in models

### Show lineage graph
at first generate docs and serve them
#### Generate dbt docs 
```
dbt docs generate
```
#### Serve docs
```
dbt docs serve
```
### Install packages
at first create file named `packages.yml` in same hierarchy level as `dbt_project.yml`, then define the packages you want to use. After that run command below
```
dbt deps
```
### Variable
We can create a variable in model files.  
In this case we use variable as limiter for test model.
```
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
```
you can see the code above. if we dont specify --var `is_test_run` with value `false`, by default when we run the model, it will be limited to 100 (rows).
### Seeds
dbt can load data(dataset) into DW using command below. You must place csv file(Im not sure that csv is only format that dbt can execute) in seeds folder in your dbt project
```
dbt seed
```
and if you change the content of csv or changing the columns format inside `dbt_project.yml` and then trying to renew the table, you can execute this command
```
dbt seed --full-refresh
```
### Test
by run this command, we can run pre-defined test that we have created in `models > staging > schema.yml`
```
dbt test
```

## Metabase
pull image from container registry
```
docker pull metabase/metabase:latest
```
start container
```
docker run -d -p 3000:3000 --name metabase metabase/metabase
```
you can access the dashboard using `http://localhost:3000`
