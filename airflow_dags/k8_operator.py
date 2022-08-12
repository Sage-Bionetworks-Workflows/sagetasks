"""Example DAG demonstrating the usage of the KubeOperator."""
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator


with DAG(
    dag_id='k8_test',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
) as dag:
    run_this_last = EmptyOperator(
        task_id='run_this_last',
    )

    k = KubernetesPodOperator(
        name="hello-dry-run",
        image="debian",
        cmds=["bash", "-cx"],
        arguments=["echo", "10"],
        labels={"foo": "bar"},
        task_id="dry_run_demo",
        do_xcom_push=True,
    )

    k >> run_this_last


if __name__ == "__main__":
    dag.cli()
