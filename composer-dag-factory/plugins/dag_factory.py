"""
GCP Composer DAG Factory
========================
Dynamically generates Airflow DAGs from YAML configuration files,
with native support for Airflow Variables, Composer environment
variables, GCP-specific operators, and nested task groups.

Variable Resolution Syntax in YAML:
    ${var.my_variable}           -> Airflow Variable "my_variable"
    ${var.my_variable:default}   -> Airflow Variable with fallback default
    ${env.MY_ENV_VAR}            -> OS / Composer environment variable
    ${env.MY_ENV_VAR:default}    -> Env var with fallback default
    {{ ds }}                     -> Standard Airflow Jinja templating (passed through)

Task Group Syntax in YAML:
    task_groups:
      group_name:
        tooltip: "Description"
        prefix_group_id: true          # prepend group name to task IDs
        default_args:                  # override DAG defaults for this group
          retries: 3
        dependencies: [other_group]    # group-level dependencies
        children:                      # nested sub-groups
          sub_group:
            tooltip: "Sub-group"

    tasks:
      my_task:
        task_group_name: group_name           # flat group
        task_group_name: parent.child         # nested group (dot notation)

Usage:
    # In your DAG generation script (dags/generate_dags.py):
    from dag_factory import load_yaml_dags
    load_yaml_dags(globals_dict=globals(), config_path="/home/airflow/gcs/dags/configs")
"""

from __future__ import annotations

import copy
import glob
import logging
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.utils.module_loading import import_string
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Variable resolution
# ---------------------------------------------------------------------------

# Matches ${var.key}, ${var.key:default}, ${env.KEY}, ${env.KEY:default}
_VAR_PATTERN = re.compile(
    r"\$\{(?P<source>var|env)\.(?P<key>[A-Za-z0-9_.\-]+)(?::(?P<default>[^}]*))?\}"
)

# ---------------------------------------------------------------------------
# Composer environment variable aliases
# ---------------------------------------------------------------------------
# Composer 2 uses different env var names than you might expect. This mapping
# lets you write ${env.GCP_PROJECT} in YAML even though the actual OS env var
# is GOOGLE_CLOUD_PROJECT. The resolver tries keys in order:
#   1. The exact key as written           (e.g. GCP_PROJECT)
#   2. All aliases for that key           (e.g. GOOGLE_CLOUD_PROJECT, GCLOUD_PROJECT)
#   3. The AIRFLOW_VAR_ prefixed version  (e.g. AIRFLOW_VAR_GCP_PROJECT)
#   4. The fallback default               (e.g. :my-gcp-project)

_ENV_ALIASES: dict[str, list[str]] = {
    "GCP_PROJECT": ["GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT", "GCP_PROJECT_ID", "DEVSHELL_PROJECT_ID"],
    "COMPOSER_REGION": ["COMPOSER_LOCATION"],
    "COMPOSER_LOCATION": ["COMPOSER_REGION"],
}


def _resolve_env(key: str) -> str | None:
    """
    Look up an environment variable by key, trying multiple fallback names.

    Resolution order:
      1. os.environ[key]                    — exact match
      2. os.environ[alias]                  — Composer name aliases
      3. os.environ[AIRFLOW_VAR_{key}]      — Composer env var override prefix
    """
    # 1. Exact match
    val = os.environ.get(key)
    if val is not None:
        return val

    # 2. Try known aliases
    for alias in _ENV_ALIASES.get(key, []):
        val = os.environ.get(alias)
        if val is not None:
            return val

    # 3. Try AIRFLOW_VAR_ prefix (Composer sets custom env vars this way in some versions)
    val = os.environ.get(f"AIRFLOW_VAR_{key}")
    if val is not None:
        return val

    return None


def _resolve_variables(value: Any) -> Any:
    """Recursively resolve ${var.*} and ${env.*} placeholders in a config tree."""
    if isinstance(value, str):
        return _resolve_string(value)
    if isinstance(value, dict):
        return {k: _resolve_variables(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_variables(item) for item in value]
    return value


def _resolve_string(text: str) -> str:
    """Replace all ${...} tokens in a single string value."""

    def _replacer(match: re.Match) -> str:
        source = match.group("source")
        key = match.group("key")
        default = match.group("default")  # may be None

        if source == "var":
            try:
                return Variable.get(key, deserialize_json=False)
            except KeyError:
                if default is not None:
                    return default
                log.warning("Airflow Variable '%s' not found and no default set", key)
                return match.group(0)  # leave placeholder as-is

        if source == "env":
            env_val = _resolve_env(key)
            if env_val is not None:
                return env_val
            if default is not None:
                return default
            log.warning(
                "Environment variable '%s' not found (also tried aliases: %s, AIRFLOW_VAR_%s) "
                "and no default set",
                key,
                _ENV_ALIASES.get(key, []),
                key,
            )
            return match.group(0)

        return match.group(0)

    return _VAR_PATTERN.sub(_replacer, text)


# ---------------------------------------------------------------------------
# YAML loading helpers
# ---------------------------------------------------------------------------


def _load_yaml(filepath: str) -> dict:
    """Load a YAML file and return its contents as a dict."""
    with open(filepath, "r") as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge *override* into a copy of *base*."""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _collect_defaults(config_dir: str, yaml_dir: str) -> dict:
    """Walk from *config_dir* down to *yaml_dir* collecting defaults.yml files."""
    defaults: dict = {}
    config_path = Path(config_dir)
    yaml_path = Path(yaml_dir)

    # Build the chain of directories from root -> yaml_dir
    try:
        relative = yaml_path.relative_to(config_path)
    except ValueError:
        return defaults

    chain = [config_path]
    for part in relative.parts:
        chain.append(chain[-1] / part)

    for directory in chain:
        defaults_file = directory / "defaults.yml"
        if defaults_file.is_file():
            file_defaults = _load_yaml(str(defaults_file))
            defaults = _deep_merge(defaults, file_defaults)

    return defaults


# ---------------------------------------------------------------------------
# Operator / task builder
# ---------------------------------------------------------------------------

# Maps short aliases to full import paths for common GCP operators
GCP_OPERATOR_ALIASES: dict[str, str] = {
    "BigQueryInsertJobOperator": "airflow.providers.google.cloud.operators.bigquery.BigQueryInsertJobOperator",
    "BigQueryCheckOperator": "airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator",
    "BigQueryGetDataOperator": "airflow.providers.google.cloud.operators.bigquery.BigQueryGetDataOperator",
    "BigQueryValueCheckOperator": "airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator",
    "GCSToBigQueryOperator": "airflow.providers.google.cloud.transfers.gcs_to_bigquery.GCSToBigQueryOperator",
    "BigQueryToGCSOperator": "airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator",
    "GCSCreateBucketOperator": "airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator",
    "GCSDeleteObjectsOperator": "airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator",
    "GCSListObjectsOperator": "airflow.providers.google.cloud.operators.gcs.GCSListObjectsOperator",
    "GCSToGCSOperator": "airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator",
    "DataflowTemplatedJobStartOperator": "airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator",
    "DataflowStartFlexTemplateOperator": "airflow.providers.google.cloud.operators.dataflow.DataflowStartFlexTemplateOperator",
    "DataprocSubmitJobOperator": "airflow.providers.google.cloud.operators.dataproc.DataprocSubmitJobOperator",
    "DataprocCreateClusterOperator": "airflow.providers.google.cloud.operators.dataproc.DataprocCreateClusterOperator",
    "DataprocDeleteClusterOperator": "airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator",
    "CloudSQLExecuteQueryOperator": "airflow.providers.google.cloud.operators.cloud_sql.CloudSQLExecuteQueryOperator",
    "PubSubPublishMessageOperator": "airflow.providers.google.cloud.operators.pubsub.PubSubPublishMessageOperator",
    "CloudRunExecuteJobOperator": "airflow.providers.google.cloud.operators.cloud_run.CloudRunExecuteJobOperator",
    "GKEStartPodOperator": "airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartPodOperator",
    "KubernetesPodOperator": "airflow.providers.cncf.kubernetes.operators.pod.KubernetesPodOperator",
    "BashOperator": "airflow.operators.bash.BashOperator",
    "PythonOperator": "airflow.operators.python.PythonOperator",
    "EmailOperator": "airflow.operators.email.EmailOperator",
    "DummyOperator": "airflow.operators.dummy.DummyOperator",
    "EmptyOperator": "airflow.operators.empty.EmptyOperator",
    "BranchPythonOperator": "airflow.operators.python.BranchPythonOperator",
    "ShortCircuitOperator": "airflow.operators.python.ShortCircuitOperator",
}


def _resolve_operator_class(operator_str: str):
    """Resolve an operator string to the actual class, supporting aliases."""
    full_path = GCP_OPERATOR_ALIASES.get(operator_str, operator_str)
    return import_string(full_path)


def _parse_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
    raise ValueError(f"Cannot parse datetime from: {value!r}")


# ---------------------------------------------------------------------------
# Task Group builder
# ---------------------------------------------------------------------------


def _build_task_groups(
    dag: DAG,
    task_groups_conf: dict,
    parent_group: TaskGroup | None = None,
) -> dict[str, TaskGroup]:
    """
    Recursively build TaskGroup objects from the YAML task_groups config.

    Returns a flat dict mapping dotted group paths to TaskGroup objects.
    E.g. {"etl": <TaskGroup>, "etl.extraction": <TaskGroup>}
    """
    groups: dict[str, TaskGroup] = {}

    for group_name, group_conf in task_groups_conf.items():
        conf = copy.deepcopy(group_conf or {})

        # Pop meta-keys
        tooltip = conf.pop("tooltip", None)
        prefix_group_id = conf.pop("prefix_group_id", True)
        children_conf = conf.pop("children", {})
        # dependencies and default_args are handled later
        conf.pop("dependencies", None)
        conf.pop("default_args", None)

        tg = TaskGroup(
            group_id=group_name,
            tooltip=tooltip,
            prefix_group_id=prefix_group_id,
            parent_group=parent_group,
            dag=dag,
        )

        # Compute the full dotted path for this group
        if parent_group and parent_group.group_id:
            full_path = f"{parent_group.group_id}.{group_name}"
        else:
            full_path = group_name

        groups[full_path] = tg

        # Recurse into children
        if children_conf:
            child_groups = _build_task_groups(dag, children_conf, parent_group=tg)
            groups.update(child_groups)

    return groups


def _wire_group_dependencies(
    task_groups_conf: dict,
    group_objects: dict[str, TaskGroup],
    prefix: str = "",
):
    """Wire group-level dependencies (entire group depends on another group)."""
    for group_name, group_conf in task_groups_conf.items():
        if not group_conf:
            continue

        full_path = f"{prefix}{group_name}" if not prefix else f"{prefix}.{group_name}"
        # Normalize: handle root-level groups without prefix dot
        if prefix == "":
            full_path = group_name

        deps = group_conf.get("dependencies", [])
        if deps and full_path in group_objects:
            for dep_name in deps:
                # Resolve dep_name: try as-is, then with same prefix
                dep_group = group_objects.get(dep_name)
                if dep_group is None and prefix:
                    dep_group = group_objects.get(f"{prefix}.{dep_name}")
                if dep_group is None:
                    # Try parent prefix
                    dep_group = group_objects.get(dep_name)

                if dep_group is not None:
                    dep_group >> group_objects[full_path]
                else:
                    log.warning(
                        "Task group dependency '%s' not found for group '%s'",
                        dep_name,
                        full_path,
                    )

        # Recurse into children
        children_conf = group_conf.get("children", {})
        if children_conf:
            _wire_group_dependencies(children_conf, group_objects, prefix=full_path)


# ---------------------------------------------------------------------------
# Task builder
# ---------------------------------------------------------------------------


def _build_task(
    dag: DAG,
    task_id: str,
    task_conf: dict,
    task_group: TaskGroup | None = None,
    group_default_args: dict | None = None,
):
    """Instantiate a single Airflow task from its YAML configuration."""
    conf = copy.deepcopy(task_conf)

    # Pop meta-keys that are not operator kwargs
    operator_str = conf.pop("operator", "EmptyOperator")
    dependencies = conf.pop("dependencies", [])
    python_callable_str = conf.pop("python_callable", None)
    conf.pop("task_group_name", None)  # consumed by the caller

    # Apply group-level default_args overrides
    if group_default_args:
        for key, value in group_default_args.items():
            if key not in conf:
                conf[key] = value

    # Resolve the operator class
    operator_class = _resolve_operator_class(operator_str)

    # If python_callable is specified, import it
    if python_callable_str:
        conf["python_callable"] = import_string(python_callable_str)

    # Resolve any remaining ${var.*} / ${env.*} in task params
    conf = _resolve_variables(conf)

    # Build kwargs — include task_group if provided
    kwargs = {"task_id": task_id, "dag": dag, **conf}
    if task_group is not None:
        kwargs["task_group"] = task_group

    task = operator_class(**kwargs)
    return task, dependencies


# ---------------------------------------------------------------------------
# DAG builder
# ---------------------------------------------------------------------------

# Keys at the DAG-level YAML that are NOT passed to the DAG constructor
_DAG_META_KEYS = {"tasks", "task_groups", "default_args"}


def _build_dag(dag_id: str, dag_conf: dict, source_yaml: str | None = None) -> DAG:
    """Build a single DAG object from its resolved YAML configuration."""
    conf = copy.deepcopy(dag_conf)

    # ---- default_args ----
    default_args = conf.pop("default_args", {})
    default_args = _resolve_variables(default_args)

    # Parse special default_args fields
    for dt_field in ("start_date", "end_date"):
        if dt_field in default_args:
            default_args[dt_field] = _parse_datetime(default_args[dt_field])
    if "retry_delay" in default_args and isinstance(default_args["retry_delay"], (int, float)):
        default_args["retry_delay"] = timedelta(seconds=default_args["retry_delay"])

    # ---- Extract tasks and task_groups before building DAG ----
    tasks_conf = conf.pop("tasks", {})
    task_groups_conf = conf.pop("task_groups", {})

    # ---- Remaining top-level keys become DAG kwargs ----
    dag_kwargs = _resolve_variables(conf)

    # Parse DAG-level date fields
    for dt_field in ("start_date", "end_date"):
        if dt_field in dag_kwargs:
            dag_kwargs[dt_field] = _parse_datetime(dag_kwargs[dt_field])

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        catchup=dag_kwargs.pop("catchup", False),
        **dag_kwargs,
    )

    # ---- Embed YAML source as DAG Docs (visible in Airflow UI) ----
    if source_yaml:
        dag.doc_md = f"## Source YAML\n\n```yaml\n{source_yaml}\n```"

    # ---- Build task groups ----
    group_objects: dict[str, TaskGroup] = {}
    if task_groups_conf:
        group_objects = _build_task_groups(dag, task_groups_conf)

    # Collect group-level default_args for each group path
    group_default_args: dict[str, dict] = {}
    _collect_group_default_args(task_groups_conf, group_default_args)

    # ---- Build tasks ----
    task_objects: dict = {}
    task_deps: dict = {}

    for task_id, task_conf in tasks_conf.items():
        task_conf = _resolve_variables(task_conf)

        # Determine which task group this task belongs to
        tg_name = task_conf.get("task_group_name")
        task_group = None
        grp_defaults = None

        if tg_name and tg_name in group_objects:
            task_group = group_objects[tg_name]
            grp_defaults = group_default_args.get(tg_name)
        elif tg_name:
            log.warning(
                "DAG '%s': task_group_name '%s' for task '%s' not found. "
                "Available groups: %s",
                dag_id,
                tg_name,
                task_id,
                list(group_objects.keys()),
            )

        task_obj, deps = _build_task(
            dag, task_id, task_conf,
            task_group=task_group,
            group_default_args=grp_defaults,
        )
        task_objects[task_id] = task_obj
        task_deps[task_id] = deps

    # ---- Wire task dependencies ----
    for task_id, deps in task_deps.items():
        for dep in deps:
            if dep in task_objects:
                task_objects[dep] >> task_objects[task_id]
            else:
                log.warning(
                    "DAG '%s': dependency '%s' for task '%s' not found",
                    dag_id,
                    dep,
                    task_id,
                )

    # ---- Wire group-level dependencies ----
    if task_groups_conf:
        _wire_group_dependencies(task_groups_conf, group_objects)

    return dag


def _collect_group_default_args(
    task_groups_conf: dict,
    result: dict[str, dict],
    prefix: str = "",
):
    """Recursively collect group-level default_args into a flat dict keyed by dotted path."""
    for group_name, group_conf in task_groups_conf.items():
        if not group_conf:
            continue

        full_path = f"{prefix}.{group_name}" if prefix else group_name
        grp_defaults = group_conf.get("default_args", {})
        if grp_defaults:
            result[full_path] = _resolve_variables(copy.deepcopy(grp_defaults))

        children_conf = group_conf.get("children", {})
        if children_conf:
            # Child groups inherit parent group's default_args
            for child_name, child_conf in children_conf.items():
                if child_conf is None:
                    children_conf[child_name] = {}
                child_path = f"{full_path}.{child_name}"
                merged = copy.deepcopy(grp_defaults)
                child_defaults = (child_conf or {}).get("default_args", {})
                merged.update(child_defaults)
                if merged:
                    result[child_path] = _resolve_variables(merged)

            _collect_group_default_args(children_conf, result, prefix=full_path)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_yaml_dags(
    globals_dict: dict,
    config_path: str | None = None,
    config_file: str | None = None,
):
    """
    Generate DAGs from YAML and inject them into *globals_dict*.

    Parameters
    ----------
    globals_dict : dict
        The calling module's ``globals()`` -- DAGs are added here so Airflow
        can discover them.
    config_path : str, optional
        Directory to scan recursively for ``*.yml`` / ``*.yaml`` files.
        Defaults to ``<DAGS_FOLDER>/configs``.
    config_file : str, optional
        Path to a single YAML config file.  If provided, *config_path* is
        ignored.
    """
    dags_folder = os.environ.get("DAGS_FOLDER", "/home/airflow/gcs/dags")

    if config_file:
        yaml_files = [config_file]
        root_config_dir = str(Path(config_file).parent)
    else:
        root_config_dir = config_path or os.path.join(dags_folder, "configs")
        yaml_files = sorted(
            glob.glob(os.path.join(root_config_dir, "**", "*.y*ml"), recursive=True)
        )

    for filepath in yaml_files:
        filename = os.path.basename(filepath)

        # Skip defaults files -- they are only used for inheritance
        if filename in ("defaults.yml", "defaults.yaml"):
            continue

        try:
            raw_config = _load_yaml(filepath)
            with open(filepath, "r") as f:
                raw_yaml_text = f.read()
        except Exception:
            log.exception("Failed to load YAML file: %s", filepath)
            continue

        # Collect inherited defaults
        yaml_dir = str(Path(filepath).parent)
        defaults = _collect_defaults(root_config_dir, yaml_dir)

        # A "default" top-level key in the file itself acts as file-level defaults
        file_defaults = raw_config.pop("default", {})
        defaults = _deep_merge(defaults, file_defaults)

        for dag_id, dag_conf in raw_config.items():
            if not isinstance(dag_conf, dict):
                continue

            # Merge: global defaults -> file defaults -> dag-specific config
            merged = _deep_merge(defaults, dag_conf)

            try:
                dag = _build_dag(dag_id, merged, source_yaml=raw_yaml_text)
                globals_dict[dag_id] = dag
                log.info("DAG Factory: created DAG '%s' from %s", dag_id, filepath)
            except Exception:
                log.exception("Failed to build DAG '%s' from %s", dag_id, filepath)
