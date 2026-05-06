# localbricks

Localbricks is a local training stack for Databricks-style data engineering work before a real Databricks workspace is available.

It gives each developer:

- JupyterLab notebooks
- JupyterLab LSP and the Python language server
- Optional Jupyter AI configured for OpenAI
- Apache Spark 3.5.3
- Delta Lake 3.2.1
- Unity Catalog OSS
- Python dependencies managed through `uv`
- LangChain, OpenAI, and PDF processing libraries
- Shared local folders for raw files and Delta tables

## Prerequisites

Install these on your workstation:

- Git, with access to the company Enterprise GitHub repository
- Docker Desktop or Docker Engine with Docker Compose
- Optional: `uv`, only needed if you also want to run checks outside Docker

## First Run

Clone the repository, then create your local environment file:

```bash
cp .env.example .env
```

By default the stack starts without Jupyter AI enabled and does not require OpenAI API access or an OpenAI API key. Leave the OpenAI settings empty if you only want Spark, Delta, Unity Catalog, LSP, and PDF processing.

To opt in to OpenAI and Jupyter AI, edit `.env` and set:

```bash
JUPYTER_AI_ENABLED=true
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4o-mini
JUPYTER_AI_MODEL=gpt-4o-mini
JUPYTER_AI_EMBEDDINGS_MODEL=text-embedding-3-small
```

Start the stack:

```bash
docker compose up --build
```

Open JupyterLab:

```text
http://localhost:8888/lab?token=localbricks
```

JupyterLab starts with:

- `jupyterlab-lsp` and `python-lsp-server` for Python code intelligence
- Jupyter AI disabled by default
- Jupyter AI's chat UI and magics configured for OpenAI only when `JUPYTER_AI_ENABLED=true`

The OpenAI API key is read from `.env` through `OPENAI_API_KEY`; it is not committed into Jupyter configuration.

## Proxy Configuration

If your environment needs an HTTP or HTTPS proxy to fetch dependencies, export the proxy endpoint variables in the terminal where you run Docker Compose. The override file reuses the existing proxy endpoint values from that shell for both image builds and running containers:

```bash
export HTTP_PROXY=...
export HTTPS_PROXY=...
export http_proxy=...
export https_proxy=...
```

Copy the override example:

```bash
cp docker-compose.override.example.yaml docker-compose.override.yaml
```

Docker Compose loads `docker-compose.override.yaml` automatically. The proxy endpoint variables are applied as notebook build args and as runtime environment variables inside the notebook and Unity Catalog containers. Host `NO_PROXY` values are not reused directly because hostnames differ inside the Compose network. Localbricks sets container no-proxy values for `localhost`, `127.0.0.1`, and `uc-server` so internal Compose traffic does not go through the proxy:

```bash
docker compose up --build
```

If you need additional no-proxy hosts that are reachable from inside the containers, set `LOCALBRICKS_NO_PROXY_EXTRA` before running Docker Compose:

```bash
export LOCALBRICKS_NO_PROXY_EXTRA=internal.mirror,metadata.local
```

The notebook startup also converts these proxy variables into Spark JVM proxy options, so Spark can resolve Maven dependencies such as Delta Lake and the Unity Catalog connector while still bypassing the proxy for Unity Catalog. Recreate the notebook container and start a fresh notebook kernel after changing proxy values so Spark and Python clients read the updated settings.

Unity Catalog runs inside the Compose network at:

```text
http://uc-server:8080
```

Notebook code uses this internal REST API base:

```text
http://uc-server:8080/api/2.1/unity-catalog
```

From your host machine, use `localhost` and include a resource path. For example, open the catalog list at:

```text
http://localhost:8080/api/2.1/unity-catalog/catalogs
```

## Folders

- `notebooks/`: training notebooks mounted into JupyterLab
- `data/raw/pdfs/`: put local PDF files here for parsing exercises
- `warehouse/`: local Delta table storage

The `data` and `warehouse` folders are mounted into the notebook container at the same paths under `/workspace`.

## Persistence Model

The stack is designed to survive normal container restarts.

These paths are bind-mounted from this repository into the notebook container, so files created there are written to your host machine:

- `./notebooks` -> `/workspace/notebooks`
- `./data` -> `/workspace/data`
- `./warehouse` -> `/workspace/warehouse`

That means notebooks, uploaded PDFs, generated files, and Delta table data remain available after:

```bash
docker compose down
docker compose up
```

Unity Catalog metadata is also persisted, but it lives in the Docker named volume `localbricks_uc_data`. This metadata includes catalog, schema, table, and function definitions. The Delta table data itself lives under `./warehouse`.

Only the containers are removed by `docker compose down`; bind-mounted files and named volumes are kept. Data is removed only if you explicitly delete local folders such as `warehouse/` or run `docker compose down --volumes`.

For Databricks-style mapping:

- Unity Catalog metadata: Docker volume `localbricks_uc_data`
- Delta table files: `./warehouse/...`
- Notebook files: `./notebooks/...`
- Source files and PDFs: `./data/...`

## Example Notebook

Open:

```text
notebooks/01_localbricks_databricks_basics.ipynb
```

The notebook demonstrates:

- loading environment variables
- optionally using Jupyter AI magics with OpenAI
- starting Spark with Delta Lake and Unity Catalog
- querying Unity Catalog schemas and tables
- creating and querying Delta tables
- parsing PDFs from `/workspace/data/raw/pdfs`
- chunking text with LangChain
- writing chunks to Delta
- registering a Python function as a Unity Catalog AI tool
- optionally calling OpenAI with that UC tool

The first Spark session may take a few minutes because Spark downloads the Delta Lake and Unity Catalog Spark connector JARs into the Docker volume `spark_ivy_cache`.

## Resetting

Stop the containers:

```bash
docker compose down
```

Reset Unity Catalog metadata and Spark dependency cache:

```bash
docker compose down --volumes
```

Delete generated Delta table files by removing contents under `warehouse/`.

## Local Python Checks

If you have `uv` installed on your host:

```bash
uv sync
uv run python -c "import jupyter_ai, jupyterlab_lsp, pylsp, pyspark, delta, langchain, openai"
```

The Docker image installs from the same `pyproject.toml`, so host-side `uv` checks are optional.

## Notes

This stack emulates the basics of Databricks notebook work locally. It is not a full Databricks Runtime replacement. Use it for training workflows around Spark, Delta tables, Unity Catalog naming, PDF ingestion, chunking, and LLM tool integration.
