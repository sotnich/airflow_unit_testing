from airflow.utils.state import DagRunState
from dags.business_dag import dag, source_table, target_table
from dags.config import schema_name


def test_mytest(db_hook):
    db_hook.run(f'DROP TABLE IF EXISTS {schema_name}.{source_table}')
    db_hook.run(f'DROP TABLE IF EXISTS {schema_name}.{target_table}')

    db_hook.run(f'CREATE TABLE {schema_name}.{source_table} (id bigint, first_name text, last_name text)')
    db_hook.run(f"INSERT INTO {schema_name}.{source_table} VALUES (1, 'John', 'Bogle')")
    db_hook.run(f"INSERT INTO {schema_name}.{source_table} VALUES (2, 'Phil', 'Adams')")
    db_hook.run(f"INSERT INTO {schema_name}.{source_table} VALUES (3, 'Phil', 'Bogle')")
    db_hook.run(f"INSERT INTO {schema_name}.{source_table} VALUES (3, 'Phil', 'Schmidt')")

    dag_run = dag.test()
    assert dag_run.state == DagRunState.SUCCESS

    res = db_hook.get_records(f'SELECT * FROM {schema_name}.{target_table}')
    ref = [('John', 1), ('Phil', 2)]
    assert sorted(res) == sorted(ref)
