from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="get_webdata2",
    description="Scrape product data from ecom site",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["scraper", "takealot"],
) as dag:

    get_webdata = DockerOperator(
        task_id="get_webdata",
        image="scrapper-scrapper:latest",  # <-- name of your app container's image
        command=[
            "uv", "run", "extract/run_scraper.py",
            "--config", "extract/categories.yml",
            "--output-dir", "scraped_data",
            "--max-pages", "1",
            "--delay", "2.0",
            "--delay-between-categories", "5.0"
        ],
        docker_url="unix://var/run/docker.sock",  # Airflow talks to host Docker
        auto_remove="success",
        mount_tmp_dir=False,
        network_mode="scrapper_default",
    )