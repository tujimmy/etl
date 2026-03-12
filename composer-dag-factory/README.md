# GCP Composer DAG Factory

A configuration-driven DAG generator for **Google Cloud Composer** that lets you define Airflow pipelines in YAML — with native support for **Airflow Variables**, **Composer environment variables**, **GCP operator aliases**, and **nested task groups**.

---

## Architecture

```
gs://<composer-bucket>/
├── dags/
│   ├── generate_dags.py              <- Only Python file Airflow parses
│   └── configs/
│       ├── defaults.yml              <- Global defaults (all DAGs inherit)
│       ├── bq_etl_pipeline.yml       <- BigQuery ETL DAG (with task groups)
│       ├── gcs_file_processor.yml    <- GCS file processing DAG
│       ├── dataflow_pipeline.yml     <- Dataflow Flex Template DAG
│       ├── marketing/
│       │   ├── defaults.yml          <- Marketing dept overrides
│       │   └── campaign_attribution.yml  (nested task groups)
│       └── finance/
│           ├── defaults.yml          <- Finance dept overrides
│           └── revenue_reconciliation.yml  (group default_args)
├── plugins/
│   └── dag_factory.py               <- Core engine (auto-loaded by Composer)
└── data/
    ├── include/
    │   └── tasks/
    │       ├── gcs_tasks.py          <- Python callables for PythonOperator
    │       └── finance_tasks.py
    └── setup_variables.py            <- One-time variable seeding script
```

## How It Works

1. **`generate_dags.py`** is the only `.py` file in `dags/`. Airflow's scheduler parses it.
2. It calls `load_yaml_dags()` which scans `dags/configs/` recursively for `*.yml` files.
3. Each YAML file defines one or more DAGs with their tasks, task groups, and dependencies.
4. Before building DAGs, the engine resolves all variable placeholders.
5. `defaults.yml` files provide hierarchical inheritance (global -> department -> DAG).

---

## Variable Resolution

Three types of dynamic values are supported inside any YAML string:

| Syntax | Source | Resolved At | Example |
|---|---|---|---|
| `${var.key}` | Airflow Variable | DAG parse time | `${var.bq_dataset}` -> `analytics` |
| `${var.key:default}` | Airflow Variable w/ fallback | DAG parse time | `${var.bq_location:US}` |
| `${env.KEY}` | OS / Composer env var | DAG parse time | `${env.GCP_PROJECT}` |
| `${env.KEY:default}` | Env var w/ fallback | DAG parse time | `${env.COMPOSER_REGION:us-central1}` |
| `{{ ds }}` | Airflow Jinja template | Task runtime | `{{ ds_nodash }}` -> `20250311` |

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

## Task Groups

Task groups organize related tasks into collapsible sections in the Airflow UI.

### Basic Groups

```yaml
task_groups:
  extraction:
    tooltip: "Extract source data"
    prefix_group_id: true          # task IDs become extraction.task_name

tasks:
  load_data:
    operator: GCSToBigQueryOperator
    task_group_name: extraction    # assigns task to the group
    ...
```

### Nested Groups (Dot Notation)

```yaml
task_groups:
  etl:
    tooltip: "Full ETL pipeline"
    prefix_group_id: true
    children:
      extraction:
        tooltip: "Extract phase"
        prefix_group_id: true
      transformation:
        tooltip: "Transform phase"
        prefix_group_id: true
        dependencies: [extraction]   # group-level dependency

tasks:
  extract_data:
    task_group_name: etl.extraction  # dot notation for nested groups
    ...
  transform_data:
    task_group_name: etl.transformation
    ...
```

### Group-Level Dependencies

Entire groups can depend on other groups:

```yaml
task_groups:
  ingestion:
    tooltip: "Load data"
  transformation:
    tooltip: "Transform data"
    dependencies: [ingestion]       # all tasks in transformation
                                    # wait for all tasks in ingestion
  validation:
    tooltip: "Quality checks"
    dependencies: [transformation]
```

### Group-Level default_args

Override DAG-level defaults for all tasks within a group:

```yaml
task_groups:
  extraction:
    tooltip: "Extract data"
    default_args:
      retries: 4                   # extraction tasks get 4 retries
                                   # (overrides DAG-level retries: 2)
```

---

## GCP Operator Aliases

Instead of writing full import paths, use short aliases in your YAML:

```yaml
tasks:
  load_data:
    operator: GCSToBigQueryOperator    # alias — resolved to full import path
    bucket: my-bucket
    ...
```

### Using Full Import Paths (No Alias Needed)

Any operator works without an alias — just provide the full Python import path.
The factory calls `import_string()` on whatever string you give it:

```yaml
tasks:
  # A provider operator not in the alias list
  run_cloud_function:
    operator: airflow.providers.google.cloud.operators.functions.CloudFunctionInvokeFunctionOperator
    project_id: "${env.GCP_PROJECT:my-gcp-project}"
    location: "us-central1"
    input_data: {}
    function_id: "my-cloud-function"
    gcp_conn_id: google_cloud_default

  # An HTTP operator — not GCP-specific, no alias exists
  call_api:
    operator: airflow.providers.http.operators.http.HttpOperator
    http_conn_id: my_api_conn
    endpoint: "/api/v1/trigger"
    method: "POST"
    data: '{"date": "{{ ds }}"}'
    dependencies: [run_cloud_function]

  # A Slack notification — fully qualified path
  notify_slack:
    operator: airflow.providers.slack.operators.slack_webhook.SlackWebhookOperator
    slack_webhook_conn_id: slack_default
    message: "Pipeline complete for {{ ds }}"
    dependencies: [call_api]
```

### Using Custom Operators

If your team has custom operators, place them in `plugins/` (auto-loaded by Composer)
and reference them by their full module path:

```python
# plugins/custom_operators/quality_check.py
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    def __init__(self, table, checks, **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.checks = checks

    def execute(self, context):
        # your quality check logic here
        ...
```

```yaml
tasks:
  quality_check:
    operator: custom_operators.quality_check.DataQualityOperator
    table: "${env.GCP_PROJECT:my-gcp-project}.analytics.daily_events_{{ ds_nodash }}"
    checks:
      - row_count_gt_zero
      - no_nulls_in_pk
    dependencies: [transform_events]
```

### Supported Aliases

| Alias | Full Path |
|---|---|
| `BigQueryInsertJobOperator` | `airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator` |
| `BigQueryCheckOperator` | `airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator` |
| `BigQueryValueCheckOperator` | `airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator` |
| `GCSToBigQueryOperator` | `airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator` |
| `BigQueryToGCSOperator` | `airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator` |
| `GCSListObjectsOperator` | `airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator` |
| `GCSToGCSOperator` | `airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator` |
| `GCSDeleteObjectsOperator` | `airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator` |
| `DataflowStartFlexTemplateOperator` | `airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator` |
| `DataflowTemplatedJobStartOperator` | `airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` |
| `DataprocSubmitJobOperator` | `airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator` |
| `DataprocCreateClusterOperator` | `airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator` |
| `DataprocDeleteClusterOperator` | `airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator` |
| `CloudRunExecuteJobOperator` | `airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator` |
| `GKEStartPodOperator` | `airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator` |
| `PubSubPublishMessageOperator` | `airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator` |
| `CloudSQLExecuteQueryOperator` | `airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator` |
| `BashOperator` | `airflow.operators.bash.BashOperator` |
| `PythonOperator` | `airflow.operators.python.PythonOperator` |
| `BranchPythonOperator` | `airflow.operators.python.BranchPythonOperator` |
| `ShortCircuitOperator` | `airflow.operators.python.ShortCircuitOperator` |
| `EmailOperator` | `airflow.operators.email.EmailOperator` |
| `EmptyOperator` | `airflow.operators.empty.EmptyOperator` |

Full import paths still work — aliases are optional convenience.

---

## Defaults & Inheritance

`defaults.yml` files cascade hierarchically:

```
configs/defaults.yml              <- schedule: @daily, owner: data-engineering
├── marketing/defaults.yml        <- schedule: 0 2 * * *, owner: marketing-analytics
│   └── campaign_attribution.yml  <- inherits marketing defaults
└── finance/defaults.yml          <- owner: finance-analytics, retries: 3
    └── revenue_reconciliation.yml<- inherits finance defaults
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

  task_groups:
    processing:
      tooltip: "Core processing"
      prefix_group_id: true

  tasks:
    step_one:
      operator: BigQueryInsertJobOperator
      task_group_name: processing
      configuration:
        query:
          query: "SELECT 1"
          useLegacySql: false
      location: "${var.bq_location:US}"
      gcp_conn_id: google_cloud_default

    step_two:
      operator: BashOperator
      task_group_name: processing
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
    variables import -- /home/airflow/gcs/data/variables.json
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
        tasks = list(obj.tasks)
        groups = [t.task_group.group_id for t in obj.tasks if t.task_group and t.task_group.group_id]
        print(f'  DAG: {obj.dag_id} -- {len(tasks)} tasks, groups: {set(groups) or \"none\"}')
"
```

---

## Comparison: Astronomer DAG Factory vs This Approach

| Feature | Astronomer dag-factory | This Composer Factory |
|---|---|---|
| YAML-driven DAGs | Yes | Yes |
| Airflow Variable injection | Via Jinja only | `${var.key:default}` at parse time |
| Composer env var injection | -- | `${env.KEY:default}` at parse time |
| GCP operator aliases | -- | 25+ built-in aliases |
| Hierarchical defaults | Yes | Yes |
| Task groups (basic) | Yes | Yes |
| Nested task groups | -- | Yes (dot notation + children) |
| Group-level dependencies | -- | Yes |
| Group-level default_args | -- | Yes |
| Prefix group ID | Yes | Yes (configurable) |
| TaskFlow API support | Yes | Via PythonOperator callables |
| External dependency (pip) | `dag-factory` package | Zero -- single plugin file |
| Asset-aware scheduling | Partial | Not yet (roadmap) |
