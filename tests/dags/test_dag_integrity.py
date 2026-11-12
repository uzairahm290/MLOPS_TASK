from airflow.models import DagBag


def test_currency_dag_loads():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert not dag_bag.import_errors
    dag = dag_bag.get_dag("currency_exchange_etl")
    assert dag is not None
    assert [t.task_id for t in dag.tasks] == [
        "extract_exchange_rates",
        "transform_exchange_rates",
        "load_to_mongodb",
    ]
