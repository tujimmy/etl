"""
DAG Factory — Generator Script
===============================
This is the ONLY Python file the Airflow scheduler needs to parse.
It scans dags/configs/ recursively for YAML files and generates DAGs.

Place this file at: gs://<composer-bucket>/dags/generate_dags.py

The dag_factory plugin (in plugins/) handles:
  - YAML loading with defaults.yml inheritance
  - ${var.*}  → Airflow Variable resolution
  - ${env.*}  → Composer environment variable resolution
  - {{ ... }} → Jinja templating (passed through to Airflow runtime)
  - GCP operator alias mapping (e.g. "BigQueryInsertJobOperator")
"""

import os
from pathlib import Path

# Ensure Airflow's DAG discovery picks up this file
from airflow import DAG  # noqa: F401

from dag_factory import load_yaml_dags

# On Composer, DAGS_FOLDER = /home/airflow/gcs/dags
DAGS_FOLDER = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")
CONFIG_DIR = str(Path(DAGS_FOLDER) / "configs")

load_yaml_dags(
    globals_dict=globals(),
    config_path=CONFIG_DIR,
)
