# GCP Composer DAG Factory

A configuration-driven DAG generator for **Google Cloud Composer** that lets you define Airflow pipelines in YAML — with native support for **Airflow Variables**, **Composer environment variables**, and **GCP operator aliases**.

---

## Architecture

```
gs://<composer-bucket>/
├── dags/
│   ├── generate_dags.py              ← Only Python file Airflow parses
│   └── configs/
│       ├── defaults.yml              ← Global defaults (all DAGs inherit)
│       ├── bq_etl_pipeline.yml       ← BigQuery ETL DAG
│       ├── gcs_file_processor.yml    ← GCS file processing DAG
│       ├── dataflow_pipeline.yml     ← Dataflow Flex Template DAG
│       ├── marketing/
│       │   ├── defaults.yml          ← Marketing dept overrides
│       │   └── campaign_attribution.yml
│       └── finance/
│           ├── defaults.yml          ← Finance dept overrides
│           └── revenue_reconciliation.yml
├── plugins/
│   └── dag_factory.py               ← Core engine (auto-loaded by Composer)
└── include/
    ├── tasks/
    │   ├── gcs_tasks.py              ← Python callables for PythonOperator
    │   └── finance_tasks.py
    └── setup_variables.py            ← One-time variable seeding script
```

## How It Works

1. **`generate_dags.py`** is the only `.py` file in `dags/`. Airflow's scheduler parses it.
2. It calls `load_yaml_dags()` which scans `dags/configs/` recursively for `*.yml` files.
3. Each YAML file defines one or more DAGs with their tasks and dependencies.
4. Before building DAGs, the engine resolves all variable placeholders.
5. `defaults.yml` files provide hierarchical inheritance (global → department → DAG).

---

## Variable Resolution

Three types of dynamic values are supported inside any YAML string:

| Syntax | Source | Resolved At | Example |
|---|---|---|---|
| `${var.key}` | Airflow Variable | DAG parse time | `${var.bq_dataset}` → `analytics` |
| `${var.key:default}` | Airflow Variable w/ fallback | DAG parse time | `${var.bq_location:US}` |
| `${env.KEY}` | OS / Composer env var | DAG parse time | `${env.GCP_PROJECT}` |
| `${env.KEY:default}` | Env var w/ fallback | DAG parse time | `${env.COMPOSER_REGION:us-central1}` |
| `{{ ds }}` | Airflow Jinja template | Task runtime | `{{ ds_nodash }}` → `20250311` |

### Composer Environment Variables

Composer automatically exposes several useful env vars:

| Variable | Description |
|---|---|
| `GCP_PROJECT` | The GCP project ID |
| `COMPOSER_ENVIRONMENT` | Composer environment name |
| `COMPOSER_LOCATION` | Composer region |
| `DAGS_FOLDER` | Path to dags folder (`/home/airflow/gcs/dags`) |

Set custom env vars via:
```bash
gcloud composer environments update ENVIRONMENT_NAME \
    --location LOCATION \
    --update-env-variables KEY=VALUE
```

### Airflow Variables

Set via the UI, CLI, or the included `setup_variables.py`:
```bash
# Single variable
gcloud composer environments run ENVIRONMENT_NAME \
    --location LOCATION \
    variables set -- bq_dataset "analytics"

# Bulk import from JSON
gcloud composer environments run ENVIRONMENT_NAME \
    --location LOCATION \
    variables import -- /path/to/variables.json
```

---

## GCP Operator Aliases

Instead of writing full import paths, use short aliases in your YAML:

```yaml
tasks:
  load_data:
    operator: GCSToBigQueryOperator    # instead of the full import path
    bucket: my-bucket
    ...
```

### Supported Aliases

| Alias | Full Path |
|---|---|
| `BigQueryInsertJobOperator` | `airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator` |
| `BigQueryCheckOperator` | `airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator` |
| `GCSToBigQueryOperator` | `airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator` |
| `BigQueryToGCSOperator` | `airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator` |
| `GCSListObjectsOperator` | `airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator` |
| `GCSToGCSOperator` | `airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator` |
| `DataflowStartFlexTemplateOperator` | `airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator` |
| `DataprocSubmitJobOperator` | `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator` |
| `CloudRunExecuteJobOperator` | `airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator` |
| `PubSubPublishMessageOperator` | `airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator` |
| `BashOperator` | `airflow.operators.bash.BashOperator` |
| `PythonOperator` | `airflow.operators.python.PythonOperator` |

Full import paths still work — aliases are optional convenience.

---

## Defaults & Inheritance

`defaults.yml` files cascade hierarchically:

```
configs/defaults.yml              ← schedule: @daily, owner: data-engineering
├── marketing/defaults.yml        ← schedule: 0 2 * * *, owner: marketing-analytics
│   └── campaign_attribution.yml  ← inherits marketing defaults
└── finance/defaults.yml          ← owner: finance-analytics, retries: 3
    └── revenue_reconciliation.yml← inherits finance defaults
```

A `default:` block inside any YAML file applies to all DAGs in that file:
```yaml
default:
  start_date: "2025-01-01"

my_dag_1:
  tasks: ...

my_dag_2:
  tasks: ...
```

---

## Adding a New DAG

1. Create a YAML file in `dags/configs/` (or a subdirectory):

```yaml
my_new_pipeline:
  description: "My new pipeline"
  schedule: "${var.my_schedule:@daily}"
  tags: ["dag-factory", "my-team"]

  tasks:
    step_one:
      operator: BigQueryInsertJobOperator
      configuration:
        query:
          query: "SELECT 1"
          useLegacySql: false
      location: "${var.bq_location:US}"
      gcp_conn_id: google_cloud_default

    step_two:
      operator: BashOperator
      bash_command: "echo done for {{ ds }}"
      dependencies: [step_one]
```

2. That's it — the generator picks it up on the next DAG parse cycle.

---

## Deployment to Composer

```bash
# Upload DAGs + configs
gsutil -m rsync -r -d dags/ gs://COMPOSER_BUCKET/dags/

# Upload plugins (the factory engine)
gsutil -m rsync -r -d plugins/ gs://COMPOSER_BUCKET/plugins/

# Upload include (Python callables)
gsutil -m rsync -r -d include/ gs://COMPOSER_BUCKET/data/include/

# (Optional) Seed Airflow Variables
gcloud composer environments run ENVIRONMENT_NAME \
    --location LOCATION \
    variables import -- /home/airflow/gcs/data/include/variables.json
```

---

## Testing Locally

```bash
# Install dependencies
pip install apache-airflow apache-airflow-providers-google pyyaml

# Set env vars to simulate Composer
export GCP_PROJECT=my-test-project
export COMPOSER_REGION=us-central1
export DAGS_FOLDER=$(pwd)/dags
export AIRFLOW_HOME=$(pwd)

# Validate DAGs parse without errors
python -c "
import sys
sys.path.insert(0, 'plugins')
sys.path.insert(0, '.')
from dag_factory import load_yaml_dags
g = {}
load_yaml_dags(globals_dict=g, config_path='dags/configs')
for name, obj in g.items():
    if hasattr(obj, 'dag_id'):
        print(f'  ✓ {obj.dag_id} — {len(obj.tasks)} tasks')
"
```

---

## Comparison: Astronomer DAG Factory vs This Approach

| Feature | Astronomer dag-factory | This Composer Factory |
|---|---|---|
| YAML-driven DAGs | ✓ | ✓ |
| Airflow Variable injection | Via Jinja only | `${var.key:default}` at parse time |
| Composer env var injection | — | `${env.KEY:default}` at parse time |
| GCP operator aliases | — | 20+ built-in aliases |
| Hierarchical defaults | ✓ | ✓ |
| TaskFlow API support | ✓ | Via PythonOperator callables |
| External dependency (pip) | `dag-factory` package | Zero — single plugin file |
| Asset-aware scheduling | Partial | Not yet (roadmap) |
