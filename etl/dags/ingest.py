import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.google.cloud.operators.cloud_sql import CloudSQLImportInstanceOperator
from pathlib import Path

DAGVARS = Variable.get('DAGVARS_INGEST', deserialize_json=True)
PATH_SCRIPTS = Path(Variable.get('PATH_SCRIPTS'))
PROJECT_ID = Variable.get('PROJECT_ID')
CONN_ID_POSTGRES = DAGVARS['pgconid']

dag = DAG(
    dag_id='ingest',
    description='Ingest raw data.',
    schedule_interval=None,
    default_args={
        'project_id': PROJECT_ID,
        'start_date': airflow.utils.dates.days_ago(0),
        'retries': False
    },
)

with dag:
    for table, conf in DAGVARS['ingest_config'].items():
        create_staging_table = PostgresOperator(
            task_id=f'create_staging_table.{table}',
            postgres_conn_id=CONN_ID_POSTGRES,
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
