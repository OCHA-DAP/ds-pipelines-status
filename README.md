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
- `DSCI_AZ_BLOB_PROD_SAS`

## Job configuration

Jobs are discovered automatically by filtering for the `databricks=job` tag. Additional optional tags:

| Tag | Description |
|-----|-------------|
| `type` | Comma-separated categories (e.g., `precipitation,daily`) |
| `status` | Set to `development` to highlight the row as in-progress |
| `output_schema` | Comma-separated list of output tables (e.g., `storms.nhc_tracks,storms.nhc_forecasts`) |
| `blob_container` | Azure blob storage container name for pipeline outputs |
| `blob_prefix` | Path prefix within the blob container (optional) |

### Database and Blob Storage Information

When `output_schema` is specified, the dashboard displays detailed table information including:
- Column names, types, and descriptions
- Row count and table size (MB/GB)
- Timestamp column ranges (min/max dates)

When `blob_container` is specified, the dashboard displays:
- Total size of all blobs under the prefix (MB/GB)
- Number of blobs stored
