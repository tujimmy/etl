"""
Finance Tasks — Python callables for PythonOperator
====================================================
Business logic for the Revenue Reconciliation pipeline.
Place this file at: gs://<composer-bucket>/data/include/tasks/finance_tasks.py
"""

import logging

log = logging.getLogger(__name__)


def reconcile_revenue(
    project_id: str,
    run_date: str,
    threshold: str = "0.01",
    **context,
):
    """
    Reconcile billing invoices against payment transactions.

    Compares staged billing and payment data for the given run_date,
    flags any discrepancies exceeding the threshold.

    Parameters
    ----------
    project_id : str
        GCP project ID (from ${env.GCP_PROJECT}).
    run_date : str
        The logical date to reconcile (from {{ ds }}).
    threshold : str
        Maximum acceptable discrepancy ratio (from ${var.reconciliation_threshold}).
    context : dict
        Airflow context (automatically injected).
    """
    threshold_float = float(threshold)
    ds_nodash = run_date.replace("-", "")

    log.info(
        "Reconciling revenue for %s in project %s (threshold: %s)",
        run_date,
        project_id,
        threshold,
    )

    # TODO: Add your reconciliation logic here
    # Example approach:
    # 1. Query finance_staging.billing_{ds_nodash} for total billed
    # 2. Query finance_staging.payments_{ds_nodash} for total paid
    # 3. Compare and flag discrepancies > threshold
    # 4. Write results to a reconciliation table or send alerts

    billing_table = f"{project_id}.finance_staging.billing_{ds_nodash}"
    payments_table = f"{project_id}.finance_staging.payments_{ds_nodash}"

    log.info("Billing source:  %s", billing_table)
    log.info("Payments source: %s", payments_table)
    log.info("Reconciliation complete for %s", run_date)
