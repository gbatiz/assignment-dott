import unittest
import os
from pathlib import Path
import psycopg2
from psycopg2 import sql
import pandas as pd
import json


DB = os.getenv('DB_TEST')
NAME_TESTDB = 'dott_testing'
NAME_OTHERDB = 'postgres'

PATH_EXPECTATIONS_SQL_CREATE = os.getenv(
    'PATH_EXPECTATIONS_SQL_CREATE',
    '/run/media/gbatiz/datavault/source/workspaces/dott_assignment/git/test/expectation_sql_create.json'
)
expectations = json.loads(Path(PATH_EXPECTATIONS_SQL_CREATE).read_text())

PATH_SCRIPTS_FOLDER = os.getenv(
    'PATH_SCRIPTS',
    '/run/media/gbatiz/datavault/source/workspaces/dott_assignment/git/etl/plugins/scripts'
)
scripts = {}

for script in Path(PATH_SCRIPTS_FOLDER).iterdir():
    scripts[script.name] = script.read_text()

QUERY_CHECK_COLS = """
SELECT column_name
      ,is_nullable
      ,data_type
  FROM information_schema.columns AS c
 WHERE table_name = '{}'
"""

QUERY_CHECK_CONSTRAINTS = """
 SELECT CASE WHEN constraint_name LIKE '%not_null' THEN RIGHT(constraint_name, 8) ELSE constraint_name END AS constraint_name
       ,constraint_type
   FROM information_schema.table_constraints AS c
  WHERE table_name = '{}'
"""


class TestTableCreation(unittest.TestCase):



    def setUp(self):
        # Create test db
        conn = psycopg2.connect(DB)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(NAME_TESTDB))
        )
        cur.close()
        conn.close()

        # Connect to test db
        self.conn = psycopg2.connect(DB, database=NAME_TESTDB)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.maxDiff = None


    def tearDown(self):
        # Close connections on test db
        self.cur.close()
        self.conn.close()

        # Connect to other db, drop test db, close connections
        conn = psycopg2.connect(DB)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("DROP DATABASE {}").format(sql.Identifier(NAME_TESTDB))
        )
        cur.close()
        conn.close()



    def test_create_deployment(self):
        script = scripts['create_deployment.sql']
        table = 'public.deployment'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create__duplicate(self):
        script = scripts['create__duplicate.sql']
        table = 'public.duplicate'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_pickup(self):
        script = scripts['create_pickup.sql']
        table = 'public.pickup'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_qr_code(self):
        script = scripts['create_qr_code.sql']
        table = 'public.qr_code'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_ride(self):
        script = scripts['create_ride.sql']
        table = 'public.ride'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_staging_deployment(self):
        script = scripts['create_staging_deployment.sql']
        table = 'staging.deployment'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_staging_pickup(self):
        script = scripts['create_staging_pickup.sql']
        table = 'staging.pickup'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)

    def test_create_staging_ride(self):
        script = scripts['create_staging_ride.sql']
        table = 'staging.ride'
        expectation = expectations[table]
        self.cur.execute(script)
        results = []
        for check in [QUERY_CHECK_COLS, QUERY_CHECK_CONSTRAINTS]:
            result = pd.read_sql(
                sql=check.format(table.split('.')[1]),
                con=self.conn
            ).to_dict('records')
            results.extend(result)
        self.assertListEqual(results, expectation)


if __name__ == '__main__':
    unittest.main()
