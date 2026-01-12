# DSCI Databricks Pipeline Status

A minimal dashboard displaying the status of DSCI Databricks pipelines.

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Create a `.env` file with your Databricks credentials:
   ```
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your-token
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

- `DSCI_DATABRICKS_HOST`
- `DSCI_DATABRICKS_TOKEN`

## Job configuration

Jobs are discovered automatically by filtering for the `databricks=job` tag. Additional optional tags:

| Tag | Description |
|-----|-------------|
| `type` | Comma-separated categories (e.g., `precipitation,daily`) |
| `status` | Set to `development` to highlight the row as in-progress |
