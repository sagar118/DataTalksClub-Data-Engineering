In week 2:
- We learnt to orchestrate workflows with Prefect.
- Download data from web and uploading them to Google Cloud Storage.
- Load data from Google Cloud Storage to Google BigQuery.
- Parameterizing our Flow's.
- Build Docker Images for Workflow Deployment.
- Work with Prefect Blocks for GCP.

Note: You can interact with your Prefect runs using GUI with `prefect orion start`. Prefect orion is second-generation workflow engine. To look at our Prefect runs go to `http://localhost:4200`

Week 2 Course [link](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/week_2_workflow_orchestration)

## Create Environment
Create a new environments and install requirements for this week: 
- `conda create -p venv python=3.9 -y`
- `conda activate venv/`
- `pip install -r requirements.txt`

## Incorporate Prefect to load data into Postgres
We start by incorporating Prefect in our workflow to our previous week data pipeline. We start our docker containers for Postgres and PgAdmin by `docker-compose start pgdatabase pgadmin`.

Move to `01_start` directory to run the `ingest_data.py` script which downloads the data from the web and loads it to the Postgres Database.

Run the following script using:

```python
python ingest_data.py \
    --table_name=taxi_data \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-07.csv.gz
```

## Prefect to load data to GCS and BigQuery
Next, we move to download data from web and load to Google Cloud Storage. Find the scripts for this step in `02_gcp` directory. We have created two scripts:

1. `etl_web_to_gcs.py`: Download data from web and load to GCS.
2. `etl_gcs_to_bq.py`: Download data from GCS and load it to BigQuery.

Run `etl_web_to_gcs.py` script using `python 2_Workflow_Orchestration/02_gcp/etl_web_to_gcs.py --color yellow --year 2021 --month 1 2 3`

Run `etl_gcs_to_bq.py` script using `python 2_Workflow_Orchestration/02_gcp/etl_gcs_to_bq.py --color=yellow --year=2021 --month=1`

## Deployment
To deploy our workflow we utilize Docker and Prefect Deployment. We create a script `docker_deploy.py` to get the Docker Container and build a deployment pipeline.

`Dockerfile` contains code to create our Docker Image for deployment.

**Build** the deployment using `prefect deployment build ./2_Workflow_Orchestration/02_gcp/etl_web_to_gcs.py:etl_parent_flow -n "Parameterized ETL"` Where we indicate the `etl_web_to_gcs.py` file path: Main flow starting point `etl_parent_flow`. Provide it a name `Parameterized ETL`.

Build step will create a yaml file with all the necessary information where you can change parameter values and more before applying the deployment.

Next we **apply** the changes using `prefect deployment apply etl_parent_flow-deployment.yaml`. However you can use the `-a` param in the build step to apply the changes.

To **execute** flow runs from this deployment, start an agent that pulls work from the 'default' work queue (In a new terminal): `prefect agent start -q 'default'`

For Docker container able to communicate with orion server: `prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"`

**Run the deployment**: `prefect deployment run etl_parent_flow/docker-flow -p "months=[1,2]"`

More:
  - We can schedule our deployments: Eg. Everyday at 12:00 am using cron
    - `prefect deployment build ./2_Workflow_Orchestration/02_gcp/etl_web_to_gcs.py:etl_parent_flow -n "Parameterized ETL" --cron "0 0 * * *" -a`
  - To view work queue present (By default we have `default` work queue): `prefect profile ls`

Recommended links:
  - [Prefect docs](https://docs.prefect.io/)
  - [Prefect Discourse](https://discourse.prefect.io/)
  - [Prefect Cloud](https://app.prefect.cloud/)
  - [Prefect Slack](https://prefect-community.slack.com/)
  - [Anna Geller GitHub](https://github.com/anna-geller)
