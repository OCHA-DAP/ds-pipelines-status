"""
Fetch pipeline metadata from Databricks and output to JSON.

Jobs are discovered automatically by filtering for databricks=job tag.

Optional additional tags on jobs:
    type: Comma-separated categories (e.g., storms, rainfall)
    status: Job status (e.g., development)
    output_schema: Comma-separated list of output tables (e.g., storms.nhc_tracks)

Usage:
    uv run scripts/fetch_pipelines.py

Environment variables (via .env file or environment):
    DATABRICKS_HOST: Your Databricks workspace URL
    DATABRICKS_TOKEN: Personal access token or service principal token
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from cron_descriptor import Options, get_description
from croniter import croniter
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, RunLifeCycleState, RunResultState
from dotenv import load_dotenv
from ocha_stratus import get_engine
from sqlalchemy import text

load_dotenv()


def quartz_to_standard_cron(quartz_cron: str) -> str | None:
    """Convert Quartz cron (6-7 fields with seconds) to standard cron (5 fields)."""
    parts = quartz_cron.split()
    if len(parts) >= 6:
        return " ".join(parts[1:6])  # Skip seconds, take min hour dom month dow
    return None


def get_output_schemas(job: BaseJob) -> list[str]:
    """Extract output_schema tag from a job (comma-separated)."""
    if not job.settings or not job.settings.tags:
        return []
    schema_tag = job.settings.tags.get("output_schema", "")
    return [s.strip() for s in schema_tag.split(",") if s.strip()]


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
    """Fetch column definitions for a table from the database."""
    parts = full_table_name.split(".")
    if len(parts) != 2:
        return None

    schema_name, table_name = parts

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


def fetch_all_schemas(output_schemas: list[str], engine) -> list[dict]:
    """Fetch schema definitions for all unique tables."""
    schemas = []
    for table_name in output_schemas:
        schema = fetch_table_schema(engine, table_name)
        if schema:
            schemas.append(schema)
    return schemas


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


def get_job_tags(job: BaseJob) -> list[str]:
    """Extract type tags from a job (comma-separated 'type' tag)."""
    if not job.settings or not job.settings.tags:
        return []
    type_tag = job.settings.tags.get("type", "")
    return [t.strip() for t in type_tag.split(",") if t.strip()]


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


def fetch_pipeline_data(client: WorkspaceClient, db_engine) -> dict:
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

        # Fetch schema definitions from database
        output_schemas = get_output_schemas(full_job)
        schema_definitions = fetch_all_schemas(output_schemas, db_engine) if output_schemas else []

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
            "tags": get_job_tags(full_job),
            "job_status": get_job_status(full_job),
            "output_schemas": schema_definitions,
        })

    return output


def main():
    client = WorkspaceClient()
    print(f"Fetching pipeline data from {client.config.host}...")

    print("Connecting to database...")
    db_engine = get_engine(stage="prod")

    output = fetch_pipeline_data(client, db_engine)

    output_path = Path(__file__).parent.parent / "data" / "pipelines.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
