"""Example DAG demonstrating the usage of the KubeOperator."""
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

SYNAPSE_AUTH_TOKEN = Secret(
   deploy_type="env", deploy_target="SYNAPSE_AUTH_TOKEN", secret="tyu-syn-pat", key="SYNAPSE_AUTH_TOKEN"
)

with DAG(
    dag_id='k8_test',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    run_this_last = EmptyOperator(
        task_id='run_this_last',
    )

    k = KubernetesPodOperator(
        namespace='airflow',
        name="hello-dry-run",
        image="sagebionetworks/genie",
        cmds=["synapse", "login"],
        # arguments=["echo", "$SYNAPSE_AUTH_TOKEN"],
        labels={"foo": "bar"},
        task_id="dry_run_demo",
        # in_cluster=True,
        is_delete_operator_pod=True,
        secrets=[SYNAPSE_AUTH_TOKEN]
        # do_xcom_push=True,
    )

    k >> run_this_last


if __name__ == "__main__":
    dag.cli()
