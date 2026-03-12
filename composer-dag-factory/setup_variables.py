"""
Bootstrap Airflow Variables for DAG Factory
============================================
Run once to seed Airflow Variables used across YAML configs.

Usage (from Composer worker or gcloud):
    python setup_variables.py

Or import individual variables via the Airflow CLI:
    gcloud composer environments run ENVIRONMENT_NAME \
        --location LOCATION \
        variables set -- dag_owner "data-engineering"
"""

from airflow.models import Variable

VARIABLES = {
    # -- Global --
    "dag_owner": "data-engineering",
    "alert_email": "data-alerts@example.com",
    "bq_location": "US",
    "bq_dataset": "analytics",

    # -- GCS Buckets --
    "raw_bucket": "my-project-raw",
    "export_bucket": "my-project-export",
    "incoming_bucket": "my-project-incoming",
    "archive_bucket": "my-project-archive",

    # -- BigQuery ETL --
    "bq_etl_schedule": "0 6 * * *",

    # -- Dataflow --
    "dataflow_templates_bucket": "my-project-templates",
    "dataflow_temp_bucket": "my-project-df-temp",
    "dataflow_sa": "dataflow-runner@my-gcp-project.iam.gserviceaccount.com",
    "pubsub_topic": "raw-events",
    "vpc_network": "default",
    "vpc_subnet": "regions/us-central1/subnetworks/default",

    # -- Marketing --
    "marketing_owner": "marketing-analytics",
    "marketing_alert_email": "marketing-data@example.com",

    # -- Finance --
    "finance_owner": "finance-analytics",
    "finance_alert_email": "finance-data@example.com",
    "reconciliation_threshold": "0.01",
}


def seed_variables():
    for key, value in VARIABLES.items():
        Variable.set(key, value)
        print(f"  Set: {key} = {value}")
    print(f"\nDone -- {len(VARIABLES)} variables configured.")


if __name__ == "__main__":
    seed_variables()
