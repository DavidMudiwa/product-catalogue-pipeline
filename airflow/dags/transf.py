"""
DAG: test_load_webdata
Step 1 (load_webdata): loads scraped CSVs into BigQuery RAW/STAGING tables via
    loader.py, then MERGEs into the target table.
Step 2 (dbt_build): runs dbt build (models + tests) on top of the freshly
    loaded data. dbt does not run automatically -- it only transforms data
    when explicitly invoked, so this task is required for downstream models
    to pick up what load_webdata just wrote.

Both steps run in isolated sibling containers via DockerOperator (same
scrapper-scrapper image, different command), so each needs its own explicit
credential/profile mounts -- neither inherits anything from the Airflow
container.

Required environment variables (should already be set for the Airflow
worker/scheduler, e.g. via your .env / docker-compose):
    GCLOUD_ADC_PATH          - host path to application_default_credentials.json
    BQ_PROJECT_ID            - target GCP project id for BigQuery
    DBT_PROFILES_HOST_PATH   - host path to dbt profiles.yml

Container-path conventions below (credentials path, DBT_PROFILES_DIR, dbt
project dir) are taken from the scrapper-scrapper service's own standalone
docker-compose.yml, so this DAG matches how the image already expects to be
run rather than inventing new paths.
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# --- Config pulled from environment (mirrors docker-compose.yml) -----------
# Fail fast and with a clear message at DAG-parse time if these are missing,
# rather than letting the task fail deep inside the container later.

def _require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise RuntimeError(
            f"Required environment variable '{var_name}' is not set. "
            f"Set it in your .env / docker-compose environment before "
            f"triggering this DAG."
        )
    return value


GCLOUD_ADC_HOST_PATH = _require_env("GCLOUD_ADC_PATH")
GCP_PROJECT_ID = _require_env("BQ_PROJECT_ID")
DBT_PROFILES_HOST_PATH = _require_env("DBT_PROFILES_HOST_PATH")

# Paths inside the *spawned* scrapper containers (not the Airflow container).
# Matches the conventions already established in scrapper-scrapper's own
# standalone docker-compose.yml (working_dir /app; GOOGLE_APPLICATION_CREDENTIALS
# at /gcp/...; DBT_PROFILES_DIR at /app/.dbt; dbt project at /app/product_catalog,
# inferred from the dev.duckdb mount path there).
GCP_CREDS_CONTAINER_PATH = "/gcp/application_default_credentials.json"
DBT_PROFILES_CONTAINER_DIR = "/app/.dbt"
DBT_PROJECT_DIR = "/app/product_catalog"  # <-- adjust if this isn't right


with DAG(
    dag_id="test_dbt",
    description="Test BigQuery loader independently",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["test", "loader", "bigquery"],
) as dag:

    
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image="scrapper-scrapper:latest",
        working_dir="/app",
        idcommand=[
            "sh", "-c",
            f"dbt deps --project-dir {DBT_PROJECT_DIR} "
            f"&& dbt build --project-dir {DBT_PROJECT_DIR} --target prod",
        ],
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="scrapper_default",
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDS_CONTAINER_PATH,
            "GOOGLE_CLOUD_PROJECT": GCP_PROJECT_ID,
            "DBT_PROFILES_DIR": DBT_PROFILES_CONTAINER_DIR,
        },
        mounts=[
            Mount(
                source=GCLOUD_ADC_HOST_PATH,
                target=GCP_CREDS_CONTAINER_PATH,
                type="bind",
                read_only=True,
            ),
            Mount(
                source=DBT_PROFILES_HOST_PATH,
                target=f"{DBT_PROFILES_CONTAINER_DIR}/profiles.yml",
                type="bind",
                read_only=True,
            ),
        ],
    )

