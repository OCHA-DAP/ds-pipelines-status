# DSCI Databricks Pipeline Status

A minimal dashboard displaying the status of DSCI Databricks pipelines.

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Create a `.env` file with your Databricks credentials and Azure blob storage SAS token:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token
   DSCI_AZ_BLOB_PROD_SAS=your-azure-sas-token
   ```

## Usage

### Fetch pipeline data

```bash
uv run scripts/fetch_pipelines.py
```

This queries Databricks for all jobs tagged with `databricks=job` and writes the results to `data/pipelines.json`.

### View the dashboard

Serve the files locally:

```bash
python -m http.server 8000
```

Then open http://localhost:8000 in your browser.

## Automated updates

The GitHub Action in `.github/workflows/update.yml` runs every 4 hours to fetch the latest pipeline status and commit any changes. It requires the following repository secrets:

- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `DSCI_AZ_DB_PROD_HOST`, `DSCI_AZ_DB_PROD_UID`, `DSCI_AZ_DB_PROD_PW`
- `DSCI_AZ_DB_DEV_HOST`, `DSCI_AZ_DB_DEV_UID`, `DSCI_AZ_DB_DEV_PW`
- `DSCI_AZ_BLOB_PROD_SAS`, `DSCI_AZ_BLOB_DEV_SAS`

## Job configuration

Jobs are discovered automatically by filtering for the `databricks=job` tag. Additional optional tags:

| Tag | Description |
|-----|-------------|
| `type` | One value from the knowledge-base pipeline vocabulary: `dataset-ingest`, `monitoring`, `exposure`, `alert`, `publish`, `annotation`, `schema-owner` |
| `hazard` | Comma-separated, same words as knowledge-base framework pages: `drought`, `flood`, `tropical-cyclone`, `cholera`, `plague` |
| `kb` | Stem of the job's knowledge-base pipeline page, e.g. `storms-pipeline` (rendered as a link) |
| `status` | Set to `development` to highlight the row as in-progress |
| `output_schema` | Comma-separated output tables, each as `schema.table` (e.g., `storms.nhc_tracks,storms.nhc_forecasts`). Bare schema names are ignored. |
| `output_blob` | Comma-separated blob paths, each as `container/prefix` (e.g., `raster/imerg/daily/late/v7/processed`) |
| `data_mode` | `dev` or `prod`: which data plane the job writes. Inferred from job parameters (`data_stage`, `stage`, `mode`) when absent. Tables and blob paths are looked up in that plane first, then the other, and the dashboard marks dev outputs with a badge. |
