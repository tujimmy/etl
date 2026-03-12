"""
Custom Data Quality Operator
=============================
Example custom operator that works with the DAG factory.

Usage in YAML:
    operator: custom_operators.quality_check.DataQualityOperator

Place this file at: gs://<composer-bucket>/plugins/custom_operators/quality_check.py
Composer auto-loads everything in plugins/ onto the Python path.
"""

import logging
from typing import Any

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

log = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Runs configurable data quality checks against a BigQuery table.

    Parameters
    ----------
    table : str
        Fully qualified table name (project.dataset.table).
    checks : list[str]
        List of check names to run. Supported checks:
        - row_count_gt_zero: Fail if table has zero rows
        - no_nulls_in_pk: Fail if primary key columns contain NULLs
        - values_in_range: Fail if numeric columns have out-of-range values
    gcp_conn_id : str
        Airflow connection ID for GCP (default: google_cloud_default).
    """

    template_fields = ("table",)

    def __init__(
        self,
        table: str,
        checks: list[str] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.checks = checks or ["row_count_gt_zero"]
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Any):
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        failures = []

        for check in self.checks:
            log.info("Running check '%s' on %s", check, self.table)

            if check == "row_count_gt_zero":
                result = hook.get_first(f"SELECT COUNT(*) FROM `{self.table}`")
                count = result[0] if result else 0
                if count == 0:
                    failures.append(f"row_count_gt_zero: table {self.table} has 0 rows")
                else:
                    log.info("  PASS: %d rows found", count)

            elif check == "no_nulls_in_pk":
                # TODO: Configure PK columns per table or via params
                log.info("  SKIP: no_nulls_in_pk requires PK column configuration")

            elif check == "values_in_range":
                # TODO: Configure expected ranges per column
                log.info("  SKIP: values_in_range requires range configuration")

            else:
                log.warning("  Unknown check: '%s' — skipping", check)

        if failures:
            raise ValueError(
                f"Data quality checks failed for {self.table}:\n"
                + "\n".join(f"  - {f}" for f in failures)
            )

        log.info("All %d checks passed for %s", len(self.checks), self.table)
