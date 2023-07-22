from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _send_stats(email, **context):
    stats=pd.read_csv(context["templates_dict"]["stats_path"])
    email_stats(stats, email)

send_stats = PythonOperator(
    python_callable=_send_stats,
    task_id="send_stats",
    op_kwargs={"email": "user@example.com"},
    templates_dict={"stats_path": "/data/stats/{{ds}}.csv"},
    dag=dag
)

calculate_stats >> send_stats