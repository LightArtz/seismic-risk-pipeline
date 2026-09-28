from airflow.models import DagBag

def test_dags_import_successfully():
    dag_bag = DagBag()
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"

def test_all_tasks_have_retries():
    dag_bag = DagBag()
    for dag_id, dag in dag_bag.dags.items():
        for task in dag.tasks:
            assert task.retries is not None and task.retries > 0, \
                f"{dag_id}.{task.task_id} has no retries set"