def _calculate_stats(input_path, output_path):
    
    Path(output_path).parent.mkdir(exist_ok=True)
    events=pd.read_json(input_path)
    stats=events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

calculate_stats=PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path": "/data/events.json",
        "output_path": "/data/stats.csv",
    },
    dag=dag,
)

