"""
Fetch pipeline metadata from Databricks and output to JSON.

Jobs are discovered automatically by filtering for databricks=job tag.

Optional additional tags on jobs:
    schedule: Daily at 14:00 UTC
    category: precipitation, daily, climate

Usage:
    python fetch_pipelines.py

Environment variables (via .env file or environment):
    DATABRICKS_HOST: Your Databricks workspace URL
    DATABRICKS_TOKEN: Personal access token or service principal token
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob, RunLifeCycleState, RunResultState
from dotenv import load_dotenv

load_dotenv()

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

    # Git source at job level
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
            except Exception as e:
                print(f"Error fetching nested job {task.run_job_task.job_id}: {e}")

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


def get_job_schedule(job) -> str | None:
    """Extract schedule from job settings and convert to plain English."""
    from cron_descriptor import Options, get_description

    if not job.settings:
        return None

    # Check for cron-based schedule
    if job.settings.schedule and job.settings.schedule.quartz_cron_expression:
        cron = job.settings.schedule.quartz_cron_expression
        # Quartz has 6-7 fields (with seconds), cron-descriptor expects 5-6
        # Remove the seconds field for standard cron format
        parts = cron.split()
        if len(parts) >= 6:
            cron_5 = " ".join(parts[1:6])  # Skip seconds, take min hour dom month dow
            try:
                options = Options()
                options.use_24hour_time_format = True
                description = get_description(cron_5, options)
                return f"{description} UTC"
            except Exception:
                return cron

    # Check for trigger-based schedule (periodic)
    if job.settings.trigger and job.settings.trigger.periodic:
        periodic = job.settings.trigger.periodic
        interval = periodic.interval
        unit = periodic.unit
        if unit:
            return f"Every {interval} {unit.value.lower()}"

    return None


def map_status(run) -> str:
    """Map Databricks run state to simple status."""
    if not run or not run.state:
        return "unknown"

    if run.state.life_cycle_state in (RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING):
        return "running"
    elif run.state.result_state == RunResultState.SUCCESS:
        return "success"
    else:
        return "failed"


def epoch_to_iso(epoch_ms: int) -> str:
    """Convert epoch milliseconds to ISO format."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def build_run_url(host: str, run) -> str | None:
    """Build URL to the run in Databricks UI."""
    if not run:
        return None
    return f"{host}/jobs/{run.job_id}/runs/{run.run_id}"


def fetch_pipeline_data(client: WorkspaceClient) -> dict:
    """Fetch data for all discovered jobs."""
    host = client.config.host.rstrip("/")
    jobs = get_jobs(client)

    print(f"Found {len(jobs)} jobs with databricks=job tag")

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "pipelines": [],
    }

    for job in jobs:
        # Get full job details (list() doesn't include tasks)
        try:
            full_job = client.jobs.get(job.job_id)
        except Exception as e:
            print(f"Error fetching job details for {job.job_id}: {e}")
            full_job = job

        job_name = full_job.settings.name if full_job.settings else f"Job {job.job_id}"
        tasks = get_job_tasks(full_job, client)
        schedule = get_job_schedule(full_job)
        tags = get_job_tags(full_job)
        job_status = get_job_status(full_job)

        # Get latest run
        latest_run = None
        try:
            runs = list(client.jobs.list_runs(job_id=job.job_id, limit=1))
            if runs:
                latest_run = runs[0]
        except Exception as e:
            print(f"Error fetching runs for job {job.job_id}: {e}")

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

        # Get next scheduled run time
        next_run = None
        if full_job.settings and full_job.settings.schedule:
            cron = full_job.settings.schedule.quartz_cron_expression
            if cron:
                from croniter import croniter
                # Convert Quartz (6-7 fields) to standard cron (5 fields)
                parts = cron.split()
                if len(parts) >= 6:
                    cron_5 = " ".join(parts[1:6])
                    try:
                        now = datetime.now(timezone.utc)
                        cron_iter = croniter(cron_5, now)
                        next_dt = cron_iter.get_next(datetime)
                        # croniter returns naive datetime, add UTC timezone
                        next_dt = next_dt.replace(tzinfo=timezone.utc)
                        next_run = next_dt.isoformat().replace("+00:00", "Z")
                    except Exception as e:
                        print(f"Error calculating next run for {job_name}: {e}")
        # Also check for periodic triggers
        elif full_job.settings and full_job.settings.trigger and full_job.settings.trigger.periodic:
            # For periodic triggers, we can't easily calculate next run
            pass

        output["pipelines"].append({
            "name": job_name,
            "tasks": tasks,
            "schedule": schedule,
            "last_run": last_run_data,
            "next_run": next_run,
            "tags": tags,
            "job_status": job_status,
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
