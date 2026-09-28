"""
Fetch pipeline metadata from Databricks and output to JSON.

Jobs are discovered automatically by filtering for databricks=job tag.

Optional additional tags on jobs (vocabularies follow the team knowledge base):
    type: dataset-ingest | monitoring | exposure | alert | publish | annotation | schema-owner
    hazard: Comma-separated, e.g. flood,tropical-cyclone
    kb: Knowledge-base pipeline page stem (e.g. storms-pipeline)
    status: Job status (e.g., development)
    output_schema: Comma-separated schema.table list (e.g., storms.nhc_tracks)
    output_blob: Comma-separated container/prefix paths in blob storage
    data_mode: dev | prod — which data plane the job writes; inferred from job parameters when absent

Tables and blob paths are looked up in the job's data plane first, then the other one.

Usage:
    uv run scripts/fetch_pipelines.py

Environment variables (via .env file or environment):
    DATABRICKS_HOST: Your Databricks workspace URL
    DATABRICKS_TOKEN: Personal access token or service principal token
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from azure.storage.blob import BlobServiceClient
from cron_descriptor import Options, get_description
from croniter import croniter
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, RunLifeCycleState, RunResultState
from dotenv import load_dotenv
from ocha_stratus import get_engine
from sqlalchemy import text

load_dotenv()

STAGES = ("prod", "dev")
BLOB_ACCOUNTS = {"prod": "imb0chd0prod", "dev": "imb0chd0dev"}
_unavailable_engines: dict[str, object] = {}
_missing_sas_warned: set[str] = set()


def get_data_mode(job: BaseJob) -> str:
    """Which data plane the job writes: the data_mode tag, else job parameters, else prod."""
    settings = job.settings
    tags = (settings.tags or {}) if settings else {}
    if tags.get("data_mode") in STAGES:
        return tags["data_mode"]
    params = {p.name.lower(): str(p.default).lower() for p in ((settings.parameters if settings else None) or []) if p.default}
    for key in ("data_stage", "stage", "mode"):
        if params.get(key) in STAGES:
            return params[key]
    for task in (settings.tasks if settings else None) or []:
        args = task.spark_python_task.parameters if task.spark_python_task else []
        for flag, value in zip(args or [], (args or [])[1:]):
            if flag == "--mode" and value.lower() in STAGES:
                return value.lower()
    return "prod"


def stages_to_try(expected: str) -> tuple[str, ...]:
    return (expected, *(s for s in STAGES if s != expected))


def get_stage_engine(stage: str):
    """Database engine for a stage, or None once that stage has failed to connect."""
    if stage not in _unavailable_engines:
        try:
            engine = get_engine(stage=stage)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            _unavailable_engines[stage] = engine
        except Exception as e:
            print(f"  {stage} database unavailable, skipping its table stats: {e}")
            _unavailable_engines[stage] = None
    return _unavailable_engines[stage]


def quartz_to_standard_cron(quartz_cron: str) -> str | None:
    """Convert Quartz cron (6-7 fields with seconds) to standard cron (5 fields)."""
    parts = quartz_cron.split()
    if len(parts) >= 6:
        return " ".join(parts[1:6])  # Skip seconds, take min hour dom month dow
    return None


def get_tag_list(job: BaseJob, key: str) -> list[str]:
    """Split a comma-separated tag into a list."""
    if not job.settings or not job.settings.tags:
        return []
    return [t.strip() for t in job.settings.tags.get(key, "").split(",") if t.strip()]


def get_output_schemas(job: BaseJob) -> list[str]:
    """Extract output_schema tag entries, keeping only those in schema.table form."""
    entries = get_tag_list(job, "output_schema")
    valid = [e for e in entries if e.count(".") == 1 and all(e.split("."))]
    for bad in set(entries) - set(valid):
        print(f"  Ignoring output_schema '{bad}' on {job.settings.name}: must be schema.table")
    return valid


def get_blob_locations(job: BaseJob) -> list[dict]:
    """Split the comma-separated output_blob tag into container/prefix entries."""
    locations = []
    for entry in get_tag_list(job, "output_blob"):
        container, _, prefix = entry.strip("/").partition("/")
        if container:
            locations.append({"container": container, "prefix": prefix})
    return locations


def calculate_blob_storage_size(stage: str, container_name: str, prefix: str) -> dict | None:
    """Total size and count of blobs under a prefix in the given stage's storage account."""
    sas_token = os.getenv(f"DSCI_AZ_BLOB_{stage.upper()}_SAS")
    if not sas_token:
        if stage not in _missing_sas_warned:
            print(f"  No DSCI_AZ_BLOB_{stage.upper()}_SAS set, skipping {stage} blob stats")
            _missing_sas_warned.add(stage)
        return None
    try:
        account_url = f"https://{BLOB_ACCOUNTS[stage]}.blob.core.windows.net"

        # Use SAS token for authentication
        blob_service_client = BlobServiceClient(account_url=account_url, credential=sas_token)
        container_client = blob_service_client.get_container_client(container_name)

        total_size_bytes = 0
        blob_count = 0

        # List blobs with minimal properties for speed
        blob_list = container_client.list_blobs(
            name_starts_with=prefix if prefix else None
        )

        for blob in blob_list:
            total_size_bytes += blob.size
            blob_count += 1

        if blob_count > 0:
            # Convert to GB or MB depending on size
            size_mb = total_size_bytes / (1024 * 1024)
            if size_mb >= 1024:
                return {
                    "blob_size_gb": round(size_mb / 1024, 2),
                    "blob_count": blob_count
                }
            else:
                return {
                    "blob_size_mb": round(size_mb, 2),
                    "blob_count": blob_count
                }
        return None
    except Exception as e:
        print(f"Error calculating blob storage size for {container_name}/{prefix}: {e}")
        return None


def fetch_table_stats(engine, schema_name: str, table_name: str, timestamp_columns: list[str]) -> dict:
    """Fetch row count (from pg stats), table size, and min/max for timestamp columns."""
    full_table = f'"{schema_name}"."{table_name}"'
    stats: dict = {}

    try:
        with engine.connect() as conn:
            # Combined query for row count and table size (both from pg_class)
            metadata_query = text("""
                SELECT reltuples::bigint, pg_total_relation_size(c.oid)
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = :schema AND c.relname = :table
            """)
            result = conn.execute(metadata_query, {"schema": schema_name, "table": table_name})
            row = result.fetchone()

            if row:
                row_count = row[0]
                size_bytes = row[1]

                if row_count is not None and row_count >= 0:
                    stats["row_count"] = row_count

                if size_bytes is not None:
                    # Convert to MB or GB depending on size
                    size_mb = size_bytes / (1024 * 1024)
                    if size_mb >= 1024:
                        stats["size_gb"] = round(size_mb / 1024, 2)
                    else:
                        stats["size_mb"] = round(size_mb, 2)

            # Get min/max for timestamp columns
            if timestamp_columns:
                select_parts = []
                for col in timestamp_columns:
                    select_parts.extend([f'MIN("{col}")', f'MAX("{col}")'])

                minmax_query = text(f"SELECT {', '.join(select_parts)} FROM {full_table}")
                result = conn.execute(minmax_query)
                row = result.fetchone()

                if row:
                    ts_stats = {}
                    idx = 0
                    for col in timestamp_columns:
                        min_val = row[idx]
                        max_val = row[idx + 1]
                        idx += 2

                        if min_val is not None or max_val is not None:
                            col_stats = {}
                            if min_val is not None:
                                col_stats["min"] = min_val.isoformat().replace("+00:00", "Z") if hasattr(min_val, 'isoformat') else str(min_val)
                            if max_val is not None:
                                col_stats["max"] = max_val.isoformat().replace("+00:00", "Z") if hasattr(max_val, 'isoformat') else str(max_val)
                            if col_stats:
                                ts_stats[col] = col_stats

                    if ts_stats:
                        stats["timestamp_ranges"] = ts_stats
    except Exception:
        pass

    return stats


def fetch_table_schema(engine, full_table_name: str) -> dict | None:
    """Fetch column definitions for a table from the database, or None if it is missing or unreachable."""
    try:
        return _fetch_table_schema(engine, full_table_name)
    except Exception as e:
        print(f"  Could not read {full_table_name}: {e}")
        return None


def _fetch_table_schema(engine, full_table_name: str) -> dict | None:
    schema_name, table_name = full_table_name.split(".")

    query = text("""
        SELECT
            c.column_name,
            c.data_type,
            c.is_nullable,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            pgd.description AS column_comment
        FROM information_schema.columns c
        LEFT JOIN pg_catalog.pg_statio_all_tables st
            ON st.schemaname = c.table_schema
            AND st.relname = c.table_name
        LEFT JOIN pg_catalog.pg_description pgd
            ON pgd.objoid = st.relid
            AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = :schema AND c.table_name = :table
        ORDER BY c.ordinal_position
    """)

    timestamp_types = {'timestamp without time zone', 'timestamp with time zone', 'date', 'time without time zone', 'time with time zone'}

    with engine.connect() as conn:
        result = conn.execute(query, {"schema": schema_name, "table": table_name})
        columns = []
        timestamp_columns = []
        for row in result:
            r = row._mapping
            col_name = r["column_name"]
            data_type = r["data_type"]
            col = {
                "name": col_name,
                "type": data_type,
                "nullable": r["is_nullable"] == "YES",
            }
            if r["character_maximum_length"]:
                col["max_length"] = r["character_maximum_length"]
            if r["numeric_precision"]:
                col["precision"] = r["numeric_precision"]
                if r["numeric_scale"]:
                    col["scale"] = r["numeric_scale"]
            if r.get("column_comment"):
                col["comment"] = r["column_comment"]
            columns.append(col)

            if data_type in timestamp_types:
                timestamp_columns.append(col_name)

        if columns:
            schema_data = {"table": full_table_name, "columns": columns}
            # Fetch table stats
            stats = fetch_table_stats(engine, schema_name, table_name, timestamp_columns)
            schema_data.update(stats)
            return schema_data
        return None


def fetch_all_schemas(output_schemas: list[str], expected_stage: str) -> list[dict]:
    """Schema definitions for each table, from the expected data plane or failing that the other."""
    schemas = []
    for table_name in output_schemas:
        for stage in stages_to_try(expected_stage):
            engine = get_stage_engine(stage)
            schema = fetch_table_schema(engine, table_name) if engine is not None else None
            if schema:
                schemas.append({**schema, "stage": stage})
                break
    return schemas


def fetch_all_blob_storage(locations: list[dict], expected_stage: str) -> list[dict]:
    """Blob size per location, from the expected data plane or failing that the other."""
    results = []
    for location in locations:
        entry = {**location, "stage": expected_stage}
        for stage in stages_to_try(expected_stage):
            size = calculate_blob_storage_size(stage, location["container"], location["prefix"])
            if size:
                entry = {**location, "stage": stage, **size}
                break
        results.append(entry)
    return results


def get_jobs(client: WorkspaceClient) -> list[BaseJob]:
    """Discover all jobs with databricks=job tag."""
    return [
        job for job in client.jobs.list()
        if job.settings and job.settings.tags and job.settings.tags.get("databricks") == "job"
    ]


def get_job_tasks(job: BaseJob, client: WorkspaceClient) -> list[dict]:
    """Extract tasks with their git URLs from a job."""
    if not job.settings or not job.settings.tasks:
        return []

    job_git_url = None
    if job.settings.git_source:
        job_git_url = job.settings.git_source.git_url

    tasks = []
    for task in job.settings.tasks:
        if not task.task_key:
            continue

        git_url = job_git_url

        # If this task runs another job, get that job's git source
        if task.run_job_task:
            try:
                nested_job = client.jobs.get(task.run_job_task.job_id)
                if nested_job.settings and nested_job.settings.git_source:
                    git_url = nested_job.settings.git_source.git_url
            except Exception:
                pass

        tasks.append({"name": task.task_key, "git_url": git_url})

    return tasks


def get_job_status(job: BaseJob) -> str | None:
    """Extract status tag from a job (e.g., 'development')."""
    if not job.settings or not job.settings.tags:
        return None
    return job.settings.tags.get("status")


def get_job_schedule(job: BaseJob) -> str | None:
    """Extract schedule from job settings and convert to plain English."""
    if not job.settings:
        return None

    # Check for cron-based schedule
    if job.settings.schedule and job.settings.schedule.quartz_cron_expression:
        cron_5 = quartz_to_standard_cron(job.settings.schedule.quartz_cron_expression)
        if cron_5:
            try:
                options = Options()
                options.use_24hour_time_format = True
                description = get_description(cron_5, options)
                return f"{description} UTC"
            except Exception:
                return job.settings.schedule.quartz_cron_expression

    # Check for trigger-based schedule (periodic)
    if job.settings.trigger and job.settings.trigger.periodic:
        periodic = job.settings.trigger.periodic
        if periodic.unit:
            return f"Every {periodic.interval} {periodic.unit.value.lower()}"

    return None


def get_next_run(job: BaseJob) -> str | None:
    """Calculate next scheduled run time from cron expression."""
    if not job.settings or not job.settings.schedule:
        return None

    cron = job.settings.schedule.quartz_cron_expression
    if not cron:
        return None

    cron_5 = quartz_to_standard_cron(cron)
    if not cron_5:
        return None

    try:
        now = datetime.now(timezone.utc)
        cron_iter = croniter(cron_5, now)
        next_dt = cron_iter.get_next(datetime)
        next_dt = next_dt.replace(tzinfo=timezone.utc)
        return next_dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def map_status(run) -> str:
    """Map Databricks run state to simple status."""
    if not run or not run.state:
        return "unknown"

    if run.state.life_cycle_state in (RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING):
        return "running"
    elif run.state.result_state == RunResultState.SUCCESS:
        return "success"
    return "failed"


def epoch_to_iso(epoch_ms: int) -> str:
    """Convert epoch milliseconds to ISO format."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_pipeline_data(client: WorkspaceClient) -> dict:
    """Fetch data for all discovered jobs."""
    jobs = get_jobs(client)
    print(f"Found {len(jobs)} jobs with databricks=job tag")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "pipelines": [],
    }

    for job in jobs:
        try:
            full_job = client.jobs.get(job.job_id)
        except Exception as e:
            print(f"Error fetching job {job.job_id}: {e}")
            full_job = job

        job_name = full_job.settings.name if full_job.settings else f"Job {job.job_id}"

        data_mode = get_data_mode(full_job)
        schema_definitions = fetch_all_schemas(get_output_schemas(full_job), data_mode)
        blob_storage = fetch_all_blob_storage(get_blob_locations(full_job), data_mode)

        # Get latest run
        latest_run = None
        try:
            runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
            if runs:
                latest_run = runs[0]
        except Exception:
            pass

        # Build last run info
        last_run_data = None
        if latest_run:
            start_time = latest_run.start_time
            end_time = latest_run.end_time
            duration_min = None
            if start_time and end_time:
                duration_min = int((end_time - start_time) / 1000 / 60)

            last_run_data = {
                "start": epoch_to_iso(start_time) if start_time else None,
                "end": epoch_to_iso(end_time) if end_time else None,
                "duration_min": duration_min,
                "status": map_status(latest_run),
            }

        output["pipelines"].append({
            "name": job_name,
            "description": full_job.settings.description if full_job.settings else None,
            "tasks": get_job_tasks(full_job, client),
            "schedule": get_job_schedule(full_job),
            "last_run": last_run_data,
            "next_run": get_next_run(full_job),
            "tags": get_tag_list(full_job, "type"),
            "hazard": get_tag_list(full_job, "hazard"),
            "kb": full_job.settings.tags.get("kb") if full_job.settings and full_job.settings.tags else None,
            "job_status": get_job_status(full_job),
            "data_mode": data_mode,
            "output_schemas": schema_definitions,
            "blob_storage": blob_storage,
        })

    return output


def main():
    client = WorkspaceClient()
    print(f"Fetching pipeline data from {client.config.host}...")

    output = fetch_pipeline_data(client)

    output_path = Path(__file__).parent.parent / "data" / "pipelines.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
