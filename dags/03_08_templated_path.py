fetch_events=BashOperator(
    task_id="fetch__events",
    bash_command=(
        "mkdir -p /data/events && "
        "curl -o /data/events/{ds}}.json"
        "http://localhost:5000/events?"
        "start_date={{ds}}&",
        "end_date={{next_ds}}",
    dag=dag,
    )
)