# Airflow 3.x image + BigQuery provider + FAB auth (for admin/admin login) + dbt venv.
FROM apache/airflow:3.3.0

# Providers: google (BigQuery hooks/operators -- there's no separate "bigquery"
# package, BigQuery lives inside the broader GCP provider), fab (classic
# username/password login).
RUN pip install --no-cache-dir \
      apache-airflow-providers-google \
      apache-airflow-providers-fab \
      apache-airflow-providers-docker

# dbt in its own venv so it never clashes with Airflow's own pinned deps.
# DAG calls /opt/airflow/dbt_venv/bin/dbt
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-bigquery>=1.12.0"

# uv, so DAG tasks can run the scraper/loader via your EXISTING pyproject.toml
# and uv.lock (mounted in below) instead of re-declaring pandas/requests/
# google-cloud-bigquery a second time here. This is what actually avoids the
# pandas conflict -- uv creates its own project-local venv, fully separate
# from both Airflow's env and the dbt_venv above.
#
# The base image ends as USER airflow (non-root), and /usr/local/bin is
# root-owned -- switch to root just for this COPY, then switch back so the
# final image still runs as the same user Airflow itself expects.
USER root
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
USER airflow
