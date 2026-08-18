"""
DAG: test_load_webdata
Runs the scrapper-scrapper loader in an isolated Docker container via DockerOperator,
mounting the host's GCP Application Default Credentials so the container can
authenticate to BigQuery.

Required environment variables (should already be set for the Airflow
worker/scheduler, e.g. via your .env / docker-compose):
    GCLOUD_ADC_PATH   - host path to application_default_credentials.json
    BQ_PROJECT_ID     - target GCP project id for BigQuery
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

# Path inside the *spawned* scrapper container (not the Airflow container).
GCP_CREDS_CONTAINER_PATH = "/opt/airflow/.gcp/application_default_credentials.json"


with DAG(
    dag_id="loadtoBQ",
    description="Test BigQuery loader independently",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["test", "loader", "bigquery"],
) as dag:

    load_webdata = DockerOperator(
        task_id="load_webdata",
        image="scrapper-scrapper:latest",
        command=[
            "uv", "run", "load/loader.py",
            "--input", "./scraped_data/",
            "--config", "load/config.yaml",
        ],
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="scrapper_default",
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": GCP_CREDS_CONTAINER_PATH,
            "GOOGLE_CLOUD_PROJECT": GCP_PROJECT_ID,
        },
        mounts=[
            Mount(
                source=GCLOUD_ADC_HOST_PATH,
                target=GCP_CREDS_CONTAINER_PATH,
                type="bind",
                read_only=True,
            ),
        ],
    )