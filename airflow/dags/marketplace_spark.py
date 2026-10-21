from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

try:
    from airflow.providers.apache.spark.operators.spark_submit import (
        SparkSubmitOperator,
    )
except ImportError:
    try:
        from airflow.sdk import BaseOperator
    except ImportError:
        from airflow.models.baseoperator import BaseOperator

    PROJECT_ROOT = "/opt/airflow/marketplace_orders"
    DEFAULT_ENV = {
        "PYTHONPATH": PROJECT_ROOT,
        "SPARK_UI_ENABLED": "false",
    }

    class SparkSubmitOperator(BaseOperator):


        template_fields = ("application", "application_args", "env_vars")

        def __init__(
            self,
            *,
            application: str,
            application_args: Sequence[str] | None = None,
            name: str | None = None,
            conf: Mapping[str, str] | None = None,
            env_vars: Mapping[str, str] | None = None,
            verbose: bool = False,
            **kwargs,
        ) -> None:
            super().__init__(**kwargs)
            self.application = application
            self.application_args = list(application_args or [])
            self.name = name or kwargs.get("task_id", "spark-job")
            self.conf = dict(conf or {})
            self.env_vars = dict(env_vars or {})
            self.verbose = verbose
            self.conf.setdefault("spark.master", "local[*]")
            self.conf.setdefault("spark.ui.enabled", "false")
            self.conf.setdefault("spark.sql.session.timeZone", "UTC")

        def execute(self, context) -> None:
            env = os.environ.copy()
            env.update(DEFAULT_ENV)
            env.update(self.env_vars)
            env["PYTHONPATH"] = (
                f"{PROJECT_ROOT}:{env.get('PYTHONPATH', '')}".rstrip(":")
            )
            args = [str(a) for a in self.application_args]
            spark_submit = shutil.which("spark-submit")
            if spark_submit:
                command = [
                    spark_submit,
                    "--master",
                    self.conf.get("spark.master", "local[*]"),
                    "--name",
                    self.name,
                ]
                for key, value in self.conf.items():
                    if key == "spark.master":
                        continue
                    command.extend(["--conf", f"{key}={value}"])
                command.append(self.application)
                command.extend(args)
            else:
                command = [sys.executable, self.application, *args]
            self.log.info("Running %s", " ".join(command))
            completed = subprocess.run(
                command,
                check=False,
                env=env,
                cwd=PROJECT_ROOT if Path(PROJECT_ROOT).is_dir() else None,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            if completed.stdout:
                self.log.info(completed.stdout)
            if completed.returncode != 0:
                raise RuntimeError(
                    f"{self.name} exited with {completed.returncode}: {' '.join(command)}\n"
                    f"{completed.stdout}"
                )


PUBLISH_POOL = "marketplace_orders_publish"
JOBS_DIR = "/opt/airflow/marketplace_orders/marketplace_etl/jobs"

SPARK_CONF = {
    "spark.master": "local[*]",
    "spark.ui.enabled": "false",
    "spark.sql.session.timeZone": "UTC",
}


def ensure_publish_pool() -> None:

    try:
        from airflow.models.pool import Pool

        Pool.create_or_update_pool(
            name=PUBLISH_POOL,
            slots=1,
            description="Serialize marketplace extract and publish (one EXCHANGE at a time)",
        )
    except Exception:

        pass


def spark_job(
    task_id: str,
    script: str,
    application_args: Sequence[str],
    name: str,
) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"{JOBS_DIR}/{script}",
        application_args=list(application_args),
        name=name,
        conf=SPARK_CONF,
        env_vars={"PYTHONPATH": "/opt/airflow/marketplace_orders"},
        verbose=True,
        pool=PUBLISH_POOL,
    )
