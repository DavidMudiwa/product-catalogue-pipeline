from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator    


with DAG(
    dag_id="get_webdata",
    description="Scrape product data from ecom site",
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["scraper", "takealot"],
) as dag:

    get_webdata = BashOperator(
        task_id="get_webdata",
        bash_command="""
        cd /opt/airflow/scrapper && \
        uv run extract/run_scraper.py   --config extract/categories.yml   --output-dir scraped_data   --max-pages 1   --delay 2.0   --delay-between-categories 5.0
        """,
    )