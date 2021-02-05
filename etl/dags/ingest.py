import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from pathlib import Path

PROJECT_ID = Variable.get('PROJECT_ID')
PATH_SCRIPTS = Path(Variable.get('PATH_SCRIPTS'))
DAGVARS = Variable.get('DAGVARS_INGEST', deserialize_json=True)
CONN_ID_POSTGRES = DAGVARS['sql_connection_id']

dag = DAG(
    dag_id='ingest',
    description='Ingest raw data.',
    schedule_interval=None,
    default_args={
        'project_id': PROJECT_ID,
        'postgres_conn_id': CONN_ID_POSTGRES,
        'start_date': airflow.utils.dates.days_ago(0),
        'retries': False
    },
)

with dag:
    build_performance = PostgresOperator(
        task_id='build.performance',
        sql=(PATH_SCRIPTS / DAGVARS['query_build_performance']).read_text()
    )

    insert_into_qr_code = PostgresOperator(
        task_id='insert_into.qr_code',
        sql=(PATH_SCRIPTS / DAGVARS['query_insert_into_qr_code']).read_text()
    )

    for table, conf in DAGVARS['ingest_config'].items():
        create_staging_table = PostgresOperator(
            task_id=f'create_staging_table.{table}',
            sql=(PATH_SCRIPTS / conf['query_create_staging']).read_text()
        )

        staging = CloudSQLImportInstanceOperator(
            task_id=f'staging.{table}',
            instance='postgres',
            body={"importContext": {
                "uri": conf['gcs_source'],
                "fileType": 'csv',
                "csvImportOptions": {"table": conf['table_staging']},
                "database": 'postgres',
            }},
            pool='lock.staging'
        )
        staging.set_upstream(create_staging_table)

        insert_into = PostgresOperator(
            task_id=f'insert_into.{table}',
            sql=(PATH_SCRIPTS / conf['query_insert_into']).read_text()
        )
        insert_into.set_upstream(staging)
        insert_into.set_downstream(build_performance)
        if table == 'pickup':
            insert_into.set_downstream(insert_into_qr_code)
