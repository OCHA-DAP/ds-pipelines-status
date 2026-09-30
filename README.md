# DSCI Databricks Pipeline Status

A minimal dashboard displaying the status of DSCI Databricks pipelines.

## How it updates

1. A Databricks job (`databricks.yml`, every 6 h on the hour) runs `scripts/fetch_pipelines.py --to-blob`. It reads every tagged job from the Jobs API and the tables and blob paths they write on both data planes, then uploads `pipelines.json` to the dev blob at `projects/ds-pipelines-status/`. It runs on Databricks because both Postgres servers are private-endpoint only.
2. The GitHub Action (`update.yml`, 15 minutes later) downloads that file into `data/` and commits it. It needs only the `DSCI_AZ_BLOB_DEV_SAS` secret.
3. The commit triggers the Azure Static Web App deploy.

Code changes ship by pushing `main`. Redeploy the bundle only when the job config changes:

```bash
databricks bundle deploy -t prod -p DEFAULT
databricks bundle deploy -t dev -p DEFAULT --var git_branch=my-branch   # test a branch (paused)
databricks bundle run pipeline_status_refresh -t dev -p DEFAULT
```

## View the dashboard locally

```bash
python -m http.server 8000
```

Then open http://localhost:8000. The page reads the committed `data/pipelines.json`.

## Job configuration

Jobs are discovered automatically by filtering for the `databricks=job` tag. Additional tags, documented as the team convention in the knowledge base (`infrastructure/databricks.md` → Job tags):

| Tag | Description |
|-----|-------------|
| `type` | One value from the knowledge-base pipeline vocabulary: `dataset-ingest`, `monitoring`, `exposure`, `alert`, `publish`, `annotation`, `schema-owner` |
| `hazard` | Comma-separated, same words as knowledge-base framework pages: `drought`, `flood`, `tropical-cyclone`, `cholera`, `plague` |
| `kb` | Stem of the job's knowledge-base pipeline page, e.g. `storms-pipeline` (rendered as a link) |
| `output_schema` | Comma-separated output tables, each as `schema.table` (e.g., `storms.nhc_tracks,storms.nhc_forecasts`). Bare schema names are ignored. |
| `output_blob` | Comma-separated blob paths, each as `container/prefix` (e.g., `raster/imerg/daily/late/v7/processed`) |
| `data_mode` | `dev` or `prod`: which data plane the job writes. Inferred from job parameters (`data_stage`, `stage`, `mode`) when absent. Tables and blob paths are looked up in that plane first, then the other, and the dashboard marks dev outputs with a badge. |
