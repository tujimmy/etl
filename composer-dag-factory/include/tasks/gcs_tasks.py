"""
GCS Tasks — Python callables for PythonOperator
================================================
Business logic for the GCS File Processor pipeline.
Place this file at: gs://<composer-bucket>/data/include/tasks/gcs_tasks.py
"""

import logging

log = logging.getLogger(__name__)


def process_uploaded_files(
    project_id: str,
    bucket: str,
    dest_dataset: str,
    **context,
):
    """
    Process files uploaded to GCS.

    Receives the file list from the upstream GCSListObjectsOperator via XCom
    and loads/transforms each file into BigQuery.

    Parameters
    ----------
    project_id : str
        GCP project ID (from ${env.GCP_PROJECT}).
    bucket : str
        Source GCS bucket name (from ${var.incoming_bucket}).
    dest_dataset : str
        Destination BigQuery dataset (from ${var.bq_dataset}).
    context : dict
        Airflow context (automatically injected).
    """
    ti = context.get("ti")
    files = ti.xcom_pull(task_ids="list_incoming_files") if ti else []

    if not files:
        log.info("No files found to process for %s", context.get("ds", "unknown"))
        return

    log.info(
        "Processing %d files from gs://%s into %s.%s",
        len(files),
        bucket,
        project_id,
        dest_dataset,
    )

    for file_path in files:
        log.info("Processing: gs://%s/%s", bucket, file_path)
        # TODO: Add your file processing logic here
        # e.g. read from GCS, transform, load to BigQuery

    log.info("Completed processing %d files", len(files))
