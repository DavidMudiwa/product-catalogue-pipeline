"""
Loads Takealot scraper CSV output into BigQuery.

Design:
  - Raw CSVs land untouched into a RAW table (explicit schema, no autodetect).
  - Each input file is loaded chunk-by-chunk into a per-run STAGING table
    (load jobs, not streaming inserts -> free, fast, no per-row API cost).
  - Once every chunk of a file has loaded successfully, a single MERGE moves
    staging rows into the target table keyed on (product_id, scraped_at) --
    this is what makes reruns idempotent even if a file gets processed twice.
  - A JSON checkpoint file tracks per-file, per-chunk progress, so a crash
    mid-file resumes from the next un-loaded chunk instead of starting over.
  - The staging table is dropped only after a successful MERGE, so a crash
    between "chunks loaded" and "MERGE complete" leaves the target table
    untouched (never half-loaded) and is safely retried from the checkpoint.

Usage:
    python load_to_bigquery.py --input ./scraped_data/tvs_20260715_143000.csv
    python load_to_bigquery.py --input ./scraped_data/ --dry-run
    python load_to_bigquery.py --input ./scraped_data/ --config config.yaml
    gcloud auth application-default login
    uv run load/loader.py --input . --project-id kestra-sandbox-492709 --dataset products_raw --table products --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
from google.api_core.exceptions import GoogleAPIError, NotFound
from google.api_core.retry import Retry
from google.cloud import bigquery

# --------------------------------------------------------------------------
# Schema -- mirrors ProductRow from the scraper exactly. Explicit types only;
# never rely on autodetect (see note in the design discussion: autodetect
# silently drifts type inference across files, e.g. an all-null column gets
# inferred as STRING in one run and INTEGER in the next, causing
# intermittent load failures that are painful to debug).
# --------------------------------------------------------------------------

RAW_SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("product_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("tsin", "INTEGER"),
    bigquery.SchemaField("offer_id", "INTEGER"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("brand", "STRING"),
    bigquery.SchemaField("slug", "STRING"),
    bigquery.SchemaField("product_url", "STRING"),
    bigquery.SchemaField("department_slug", "STRING"),
    bigquery.SchemaField("category_slug", "STRING"),
    bigquery.SchemaField("department_name", "STRING"),
    bigquery.SchemaField("category_name", "STRING"),
    bigquery.SchemaField("price_min", "FLOAT"),
    bigquery.SchemaField("price_max", "FLOAT"),
    bigquery.SchemaField("listing_price", "FLOAT"),
    bigquery.SchemaField("pretty_price", "STRING"),
    bigquery.SchemaField("discount_pct", "FLOAT"),
    bigquery.SchemaField("is_multi_offer", "BOOLEAN"),
    bigquery.SchemaField("in_stock", "BOOLEAN"),
    bigquery.SchemaField("stock_status", "STRING"),
    bigquery.SchemaField("is_preorder", "BOOLEAN"),
    bigquery.SchemaField("rating", "FLOAT"),
    bigquery.SchemaField("reviews", "INTEGER"),
    bigquery.SchemaField("rating_1_star", "INTEGER"),
    bigquery.SchemaField("rating_2_star", "INTEGER"),
    bigquery.SchemaField("rating_3_star", "INTEGER"),
    bigquery.SchemaField("rating_4_star", "INTEGER"),
    bigquery.SchemaField("rating_5_star", "INTEGER"),
    bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED"),
    # loader-added audit columns (metadata about the load, not a business
    # transformation -- kept distinct from scraper-derived columns above)
    bigquery.SchemaField("_source_file", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_loaded_at", "TIMESTAMP", mode="REQUIRED"),
]

MERGE_KEY_COLUMNS = ["product_id", "scraped_at"]

PARTITION_FIELD = "scraped_at"
CLUSTER_FIELDS = ["category_slug", "product_id"]


# --------------------------------------------------------------------------
# Config -- env vars override a config file; nothing is ever hardcoded.
# --------------------------------------------------------------------------

@dataclass
class Config:
    project_id: str
    dataset: str
    table: str
    credentials_path: str | None = None
    impersonate_service_account: str | None = None  # e.g. "bq-loader@kestra-sandbox-492709.iam.gserviceaccount.com"
    chunk_size: int = 1_000
    checkpoint_dir: str = ".checkpoints"
    location: str = "US"
    max_retries: int = 5

    @classmethod
    def load(cls, config_path: str | None, cli_overrides: dict[str, Any]) -> "Config":
        raw: dict[str, Any] = {}

        if config_path:
            with open(config_path) as f:
                if config_path.endswith((".yaml", ".yml")):
                    import yaml
                    raw = yaml.safe_load(f) or {}
                else:
                    raw = json.load(f)

        # env vars take precedence over the config file
        env_map = {
            "project_id": "BQ_PROJECT_ID",
            "dataset": "BQ_DATASET",
            "table": "BQ_TABLE",
            "credentials_path": "GOOGLE_APPLICATION_CREDENTIALS",
            "impersonate_service_account": "BQ_IMPERSONATE_SERVICE_ACCOUNT",
            "chunk_size": "BQ_CHUNK_SIZE",
            "checkpoint_dir": "BQ_CHECKPOINT_DIR",
            "location": "BQ_LOCATION",
            "max_retries": "BQ_MAX_RETRIES",
        }
        for field_name, env_var in env_map.items():
            if os.environ.get(env_var):
                raw[field_name] = os.environ[env_var]

        # CLI args take precedence over everything
        raw.update({k: v for k, v in cli_overrides.items() if v is not None})

        for int_field in ("chunk_size", "max_retries"):
            if int_field in raw:
                raw[int_field] = int(raw[int_field])

        missing = [f for f in ("project_id", "dataset", "table") if not raw.get(f)]
        if missing:
            raise ValueError(
                f"Missing required config: {missing}. "
                f"Set via --{missing[0].replace('_', '-')}, env var, or config file."
            )

        return cls(**{k: v for k, v in raw.items() if k in cls.__dataclass_fields__})


# --------------------------------------------------------------------------
# Structured logging -- JSON lines, so this pipes cleanly into log
# aggregation / alerting (Cloud Logging, Datadog, etc.) without a custom parser.
# --------------------------------------------------------------------------

class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            payload.update(record.extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger() -> logging.Logger:
    logger = logging.getLogger("bq_loader")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
    return logger


def log_with_fields(logger: logging.Logger, level: int, msg: str, **fields: Any) -> None:
    logger.log(level, msg, extra={"extra_fields": fields})


log = get_logger()


@dataclass
class Metrics:
    file_path: str
    rows_processed: int = 0
    rows_inserted: int = 0
    rows_skipped: int = 0
    rows_failed: int = 0
    chunks_total: int = 0
    chunks_completed: int = 0
    job_ids: list[str] = field(default_factory=list)
    started_at: float = field(default_factory=time.monotonic)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["duration_seconds"] = round(time.monotonic() - self.started_at, 2)
        return d


# --------------------------------------------------------------------------
# Checkpointing -- one JSON file per input CSV, tracking which chunks have
# already been loaded into staging. This is what makes a crash mid-file
# resumable instead of restarting the whole file.
# --------------------------------------------------------------------------

class Checkpoint:
    STATUS_PENDING = "pending"
    STATUS_IN_PROGRESS = "in_progress"
    STATUS_MERGED = "merged"
    STATUS_FAILED = "failed"

    def __init__(self, checkpoint_dir: str, source_file: str):
        os.makedirs(checkpoint_dir, exist_ok=True)
        safe_name = Path(source_file).name.replace(".", "_")
        self.path = Path(checkpoint_dir) / f"{safe_name}.checkpoint.json"
        self.data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        if self.path.exists():
            with open(self.path) as f:
                return json.load(f)
        return {
            "status": self.STATUS_PENDING,
            "chunks_completed": [],
            "staging_table": None,
            "rows_inserted": 0,
            "rows_failed": 0,
            "updated_at": None,
        }

    def save(self) -> None:
        self.data["updated_at"] = datetime.now(timezone.utc).isoformat()
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self.data, f, indent=2)
        tmp.replace(self.path)  # atomic on POSIX and Windows

    def is_chunk_done(self, chunk_index: int) -> bool:
        return chunk_index in self.data["chunks_completed"]

    def mark_chunk_done(self, chunk_index: int, rows: int) -> None:
        if chunk_index not in self.data["chunks_completed"]:
            self.data["chunks_completed"].append(chunk_index)
            self.data["rows_inserted"] += rows
        self.data["status"] = self.STATUS_IN_PROGRESS
        self.save()

    def is_fully_merged(self) -> bool:
        return self.data["status"] == self.STATUS_MERGED

    def mark_merged(self) -> None:
        self.data["status"] = self.STATUS_MERGED
        self.save()

    def mark_failed(self, error: str) -> None:
        self.data["status"] = self.STATUS_FAILED
        self.data["last_error"] = error
        self.save()


# --------------------------------------------------------------------------
# BigQuery operations
# --------------------------------------------------------------------------

# Retry only on transient errors; NotFound etc. should surface immediately.
_bq_retry = Retry(predicate=lambda exc: isinstance(exc, GoogleAPIError) and getattr(exc, "code", 0) in (429, 500, 502, 503, 504))


class BigQueryLoader:
    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.project_id, credentials=self._build_credentials(config))

    @staticmethod
    def _build_credentials(config: Config):
        """Auth precedence:
        1. impersonate_service_account -- recommended for production. Uses your
           own ADC (user login or an attached identity, e.g. Cloud Run/GCE/GKE
           default service account) as the *source* identity, then impersonates
           a target service account for short-lived tokens. No key file ever
           exists on disk. Requires 'Service Account Token Creator' on the
           target SA for whichever identity is running this script.
        2. credentials_path -- legacy JSON key fallback. Most orgs created
           after May 2024 have key creation disabled by default, so this path
           will often simply be unavailable -- kept only for environments that
           still issue keys.
        3. Plain ADC -- default. Run `gcloud auth application-default login`
           locally, or rely on the attached service account when running on
           GCP infrastructure (Cloud Run, GCE, GKE, Cloud Composer, etc.).
        """
        if config.impersonate_service_account:
            import google.auth
            from google.auth import impersonated_credentials

            source_creds, _ = google.auth.default()
            return impersonated_credentials.Credentials(
                source_credentials=source_creds,
                target_principal=config.impersonate_service_account,
                target_scopes=["https://www.googleapis.com/auth/bigquery"],
                lifetime=3600,
            )

        if config.credentials_path:
            from google.oauth2 import service_account
            return service_account.Credentials.from_service_account_file(config.credentials_path)

        return None  # bigquery.Client resolves plain ADC itself when credentials=None

    @property
    def dataset_ref(self) -> str:
        return f"{self.config.project_id}.{self.config.dataset}"

    @property
    def target_table_ref(self) -> str:
        return f"{self.dataset_ref}.{self.config.table}"

    def ensure_dataset(self) -> None:
        try:
            self.client.get_dataset(self.dataset_ref)
        except NotFound:
            ds = bigquery.Dataset(self.dataset_ref)
            ds.location = self.config.location
            self.client.create_dataset(ds)
            log_with_fields(log, logging.INFO, "Created dataset", dataset=self.dataset_ref)

    def ensure_target_table(self) -> None:
        """Creates the target table with partitioning/clustering if it doesn't exist.
        Partitioning cannot be retrofitted onto an existing table -- if the table
        already exists without it, we only warn, since altering it would require
        a full table rebuild that this script won't do silently."""
        try:
            table = self.client.get_table(self.target_table_ref)
            if not table.time_partitioning:
                log_with_fields(
                    log, logging.WARNING,
                    "Target table exists but is NOT partitioned -- every downstream "
                    "query will scan the full table. Consider recreating it partitioned "
                    f"by {PARTITION_FIELD} and clustered by {CLUSTER_FIELDS}.",
                    table=self.target_table_ref,
                )
            return
        except NotFound:
            pass

        table = bigquery.Table(self.target_table_ref, schema=RAW_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=PARTITION_FIELD
        )
        table.clustering_fields = CLUSTER_FIELDS
        self.client.create_table(table)
        log_with_fields(
            log, logging.INFO, "Created target table",
            table=self.target_table_ref, partition_field=PARTITION_FIELD, cluster_fields=CLUSTER_FIELDS,
        )

    def create_staging_table(self, run_id: str) -> str:
        staging_ref = f"{self.dataset_ref}._staging_{run_id}"
        table = bigquery.Table(staging_ref, schema=RAW_SCHEMA)
        table.expires = datetime.now(timezone.utc) + timedelta(days=1)  # auto-cleanup safety net
        self.client.create_table(table, exists_ok=True)
        return staging_ref

    def load_chunk(self, chunk_path: str, staging_ref: str) -> str:
        """Loads one CSV chunk into the staging table via a load job (not streaming
        inserts -- load jobs are free and built for exactly this bulk-CSV case)."""
        job_config = bigquery.LoadJobConfig(
            schema=RAW_SCHEMA,
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            max_bad_records=100,  # tolerate some malformed rows rather than failing the whole chunk
        )
        with open(chunk_path, "rb") as f:
            job = self.client.load_table_from_file(
                f, staging_ref, job_config=job_config, num_retries=self.config.max_retries
            )
        job.result(retry=_bq_retry)  # blocks until the job finishes or raises

        if job.errors:
            log_with_fields(
                log, logging.WARNING, "Chunk load completed with bad records",
                job_id=job.job_id, bad_record_errors=len(job.errors),
            )
        return job.job_id

    def merge_staging_into_target(self, staging_ref: str, source_file: str) -> int:
        """Atomically upserts staging rows into the target table, keyed on
        (product_id, scraped_at). This is the idempotency guarantee: even if a
        file gets fully reprocessed from scratch, the MERGE means no duplicate
        rows land in the target table."""
        key_condition = " AND ".join(f"T.{c} = S.{c}" for c in MERGE_KEY_COLUMNS)
        update_cols = [f.name for f in RAW_SCHEMA if f.name not in MERGE_KEY_COLUMNS]
        update_clause = ", ".join(f"{c} = S.{c}" for c in update_cols)
        insert_cols = [f.name for f in RAW_SCHEMA]

        query = f"""
        MERGE `{self.target_table_ref}` T
        USING `{staging_ref}` S
        ON {key_condition}
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(insert_cols)})
          VALUES ({", ".join(f"S.{c}" for c in insert_cols)})
        """
        job = self.client.query(query, retry=_bq_retry)
        job.result()
        rows_affected = job.num_dml_affected_rows or 0
        log_with_fields(
            log, logging.INFO, "MERGE complete",
            source_file=source_file, staging_table=staging_ref,
            job_id=job.job_id, rows_affected=rows_affected,
        )
        return rows_affected

    def drop_staging_table(self, staging_ref: str) -> None:
        self.client.delete_table(staging_ref, not_found_ok=True)


# --------------------------------------------------------------------------
# Chunking -- read the CSV with pandas' chunksize so memory stays bounded
# regardless of file size, and write each chunk to a small temp CSV that
# gets loaded independently (and checkpointed independently).
# --------------------------------------------------------------------------

def iter_chunks(csv_path: str, chunk_size: int) -> Iterator[tuple[int, pd.DataFrame]]:
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
        yield i, chunk


def validate_columns(df: pd.DataFrame) -> list[str]:
    expected = {f.name for f in RAW_SCHEMA if not f.name.startswith("_")}
    actual = set(df.columns)
    missing = expected - actual
    return sorted(missing)


def write_chunk_csv(df: pd.DataFrame, source_file: str, loaded_at: str, tmp_dir: str) -> str:
    df = df.copy()
    df["_source_file"] = Path(source_file).name
    df["_loaded_at"] = loaded_at
    # column order must match RAW_SCHEMA for a schema-matched load
    ordered_cols = [f.name for f in RAW_SCHEMA]
    df = df[ordered_cols]
    fd, path = tempfile.mkstemp(suffix=".csv", dir=tmp_dir)
    os.close(fd)
    df.to_csv(path, index=False)
    return path


# --------------------------------------------------------------------------
# Per-file processing
# --------------------------------------------------------------------------

def process_file(
    csv_path: str,
    loader: BigQueryLoader | None,
    config: Config,
    dry_run: bool,
) -> Metrics:
    metrics = Metrics(file_path=csv_path)
    checkpoint = Checkpoint(config.checkpoint_dir, csv_path)

    if checkpoint.is_fully_merged():
        log_with_fields(log, logging.INFO, "Skipping already-loaded file", file=csv_path)
        metrics.rows_skipped = checkpoint.data.get("rows_inserted", 0)
        return metrics

    loaded_at = datetime.now(timezone.utc).isoformat()
    run_id = uuid.uuid4().hex[:12]
    staging_ref = checkpoint.data.get("staging_table")

    if dry_run:
        total_rows = 0
        bad_columns: list[str] = []
        for _, chunk in iter_chunks(csv_path, config.chunk_size):
            total_rows += len(chunk)
            bad_columns = validate_columns(chunk) or bad_columns
        metrics.rows_processed = total_rows
        log_with_fields(
            log, logging.INFO, "[DRY RUN] Validated file",
            file=csv_path, row_count=total_rows,
            schema_valid=not bad_columns, missing_columns=bad_columns,
        )
        return metrics

    assert loader is not None
    loader.ensure_dataset()
    loader.ensure_target_table()

    if not staging_ref:
        staging_ref = loader.create_staging_table(run_id)
        checkpoint.data["staging_table"] = staging_ref
        checkpoint.save()

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            for chunk_index, chunk in iter_chunks(csv_path, config.chunk_size):
                metrics.chunks_total += 1
                metrics.rows_processed += len(chunk)

                if checkpoint.is_chunk_done(chunk_index):
                    metrics.rows_skipped += len(chunk)
                    metrics.chunks_completed += 1
                    continue

                missing_cols = validate_columns(chunk)
                if missing_cols:
                    raise ValueError(f"CSV missing expected columns: {missing_cols}")

                chunk_csv = write_chunk_csv(chunk, csv_path, loaded_at, tmp_dir)
                job_id = loader.load_chunk(chunk_csv, staging_ref)

                metrics.job_ids.append(job_id)
                metrics.rows_inserted += len(chunk)
                metrics.chunks_completed += 1
                checkpoint.mark_chunk_done(chunk_index, len(chunk))
                log_with_fields(
                    log, logging.INFO, "Chunk loaded to staging",
                    file=csv_path, chunk_index=chunk_index, job_id=job_id, rows=len(chunk),
                )

            # all chunks in staging -- now do the one atomic MERGE into target
            loader.merge_staging_into_target(staging_ref, csv_path)
            checkpoint.mark_merged()
            loader.drop_staging_table(staging_ref)

        except Exception as exc:
            metrics.rows_failed = metrics.rows_processed - metrics.rows_inserted
            checkpoint.mark_failed(str(exc))
            log_with_fields(
                log, logging.ERROR, "File processing failed -- safe to rerun, will resume",
                file=csv_path, error=str(exc), **metrics.as_dict(),
            )
            raise

    log_with_fields(log, logging.INFO, "File processing complete", **metrics.as_dict())
    return metrics


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def resolve_input_files(input_path: str) -> list[str]:
    p = Path(input_path)
    if p.is_dir():
        return sorted(str(f) for f in p.glob("*.csv"))
    if p.is_file():
        return [str(p)]
    raise FileNotFoundError(f"No such file or directory: {input_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Takealot scraper CSVs into BigQuery.")
    parser.add_argument("--input", required=True, help="a CSV file or a directory of CSV files")
    parser.add_argument("--config", default=None, help="path to a JSON/YAML config file")
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--table", default=None)
    parser.add_argument("--credentials-path", default=None, help="legacy JSON key fallback (often unavailable)")
    parser.add_argument("--impersonate-service-account", default=None, help="recommended: SA email to impersonate via ADC, no key file needed")
    parser.add_argument("--chunk-size", type=int, default=None)
    parser.add_argument("--checkpoint-dir", default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    cli_overrides = {
        "project_id": args.project_id,
        "dataset": args.dataset,
        "table": args.table,
        "credentials_path": args.credentials_path,
        "impersonate_service_account": args.impersonate_service_account,
        "chunk_size": args.chunk_size,
        "checkpoint_dir": args.checkpoint_dir,
    }

    try:
        config = Config.load(args.config, cli_overrides)
    except ValueError as exc:
        log_with_fields(log, logging.ERROR, "Config error", error=str(exc))
        return 2

    try:
        files = resolve_input_files(args.input)
    except FileNotFoundError as exc:
        log_with_fields(log, logging.ERROR, "Input error", error=str(exc))
        return 2

    if not files:
        log_with_fields(log, logging.WARNING, "No CSV files found", input=args.input)
        return 0

    loader = None if args.dry_run else BigQueryLoader(config)

    run_started = time.monotonic()
    all_metrics: list[Metrics] = []
    exit_code = 0

    for csv_path in files:
        try:
            m = process_file(csv_path, loader, config, args.dry_run)
            all_metrics.append(m)
        except Exception:
            exit_code = 1  # keep processing remaining files; report failure at the end
            continue

    total = {
        "files_processed": len(files),
        "files_failed": sum(1 for m in all_metrics if m.rows_failed),
        "rows_processed": sum(m.rows_processed for m in all_metrics),
        "rows_inserted": sum(m.rows_inserted for m in all_metrics),
        "rows_skipped": sum(m.rows_skipped for m in all_metrics),
        "rows_failed": sum(m.rows_failed for m in all_metrics),
        "duration_seconds": round(time.monotonic() - run_started, 2),
        "dry_run": args.dry_run,
    }
    log_with_fields(log, logging.INFO, "Run complete", **total)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())