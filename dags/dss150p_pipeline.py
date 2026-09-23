from datetime import datetime, timedelta
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule

PROJECT = "/opt/airflow/project"

def failure_callback(context):
    ti = context["task_instance"]
    print(
        "TASK FAILED:",
        f"dag_run={context['run_id']} task={ti.task_id} try={ti.try_number} "
        f"exception={context.get('exception')}"
    )

DEFAULT_ARGS = {
    "owner": "dss150p",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": failure_callback,
}

with DAG(
    dag_id="dss150p_sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_mode": Param("full", enum=["full", "partition"]),
        "year": Param(2026, type="integer"),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
    tags=["DSS150P"],
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" python -m src.cli extract",
    )
    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" python -m src.cli transform",
    )

    def _choose_load_branch(**context):
        return "load_partition" if context["params"]["run_mode"] == "partition" else "load_full"

    choose_load = BranchPythonOperator(
        task_id="choose_load_branch",
        python_callable=_choose_load_branch,
    )

    load_full = BashOperator(
        task_id="load_full",
        bash_command=f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" python -m src.cli load",
    )
    load_partition = BashOperator(
        task_id="load_partition",
        bash_command=(
            f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" python -m src.cli load-partition "
            "--year {{ params.year }} --month {{ params.month }}"
        ),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=f"cd {PROJECT} && PIPELINE_RUN_ID=\"{{{{ run_id }}}}\" python -m src.cli validate",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    extract >> transform >> choose_load >> [load_full, load_partition] >> validate
