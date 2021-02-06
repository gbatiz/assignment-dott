from pathlib import Path
import json
import os
import unittest

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import pandas as pd


DB_TEST = os.environ['DB_TEST']
NAME_TESTDB = 'dott_testing'
NAME_OTHERDB = 'postgres'

PATH_EXPECTATIONS_SQL_INSERT = Path(os.environ['PATH_EXPECTATIONS_SQL_INSERT'])
expectations = json.loads(PATH_EXPECTATIONS_SQL_INSERT.read_text())

PATH_SCRIPTS_FOLDER = os.environ['PATH_SCRIPTS_FOLDER']
scripts = {}
for script in Path(PATH_SCRIPTS_FOLDER).iterdir():
    scripts[script.name] = script.read_text()

def jsonify_exception(e):
    return {
        'type': e.__class__.__name__,
        'args': e.args[0]
    }



class BaseTestInsertInto(unittest.TestCase):


    def setUp(self):
        # Create test db
        conn = psycopg2.connect(DB_TEST)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(NAME_TESTDB))
        )
        cur.close()
        conn.close()
        # Connect to test db
        self.conn = psycopg2.connect(DB_TEST, database=NAME_TESTDB)
        self.conn.autocommit = True
        self.cur = self.conn.cursor()
        self.maxDiff = None

    def tearDown(self):
        # Close connections on test db
        self.cur.close()
        self.conn.close()
        # Connect to other db, drop test db, close connections
        conn = psycopg2.connect(DB_TEST)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            sql.SQL("DROP DATABASE {}").format(sql.Identifier(NAME_TESTDB))
        )
        cur.close()
        conn.close()

    def create_tables(self, table_name):
        for create_script_name in [
                f'create_{table_name}.sql',
                f'create_staging_{table_name}.sql',
                 'create__duplicate.sql'
            ]:
            create_script = scripts[create_script_name]
            self.cur.execute(create_script)

    def add_test_data_to_staging(self, test_insert_clause, test_insert_values):
        execute_values(
            cur=self.cur,
            sql=test_insert_clause,
            argslist=test_insert_values
        )

    def try_insert(self, table_name):
        result = None
        try:
            insert_query = scripts[f'insert_into_{table_name}.sql']
            self.cur.execute(insert_query)
        except Exception as e:
            result = jsonify_exception(e)
        return result

    def select_star(self, table_name_w_schema):
        result = pd.read_sql(
            sql=f"SELECT * FROM {table_name_w_schema}",
            con=self.conn
        ).to_json(orient='records')  # Using `to_json` to serialize timestamps
        result = json.loads(result)  # Getting a dict back
        return result

    def _check_insert_on_main_table(
            self,
            table_name,
            scenario,
            test_insert_clause,
            test_insert_values,
        ):
        self.create_tables(table_name)
        self.add_test_data_to_staging(test_insert_clause, test_insert_values)
        result = self.try_insert(table_name)
        if not result:
            result = self.select_star(f'public.{table_name}')
        return result



class TestInsertIntoDeployment(BaseTestInsertInto):
    table_name = 'deployment'


    def test_valid_data(self, scenario='valid_data'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('6fy0fO6BHW2IvxunS54N', 'i1Fycyzyi1HeeEN8eVh1', '2019-05-19 02:34:31.999 UTC', '2019-05-21 04:52:29.113 UTC'),
            ('36AUD64tmGggMdmXLwY3', 'inxamrBymPp1kKuGoOXE', '2019-05-19 03:20:30.842 UTC', '2019-05-20 06:38:49.76 UTC'),
            ('TcPf6gwxm98aVufsLcCo', '1DfPwD1x6teNIhv8aS3p', '2019-05-19 00:39:49.472 UTC', '2019-05-19 05:13:56.514 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertListEqual(result, expectations[table_name][scenario])


    def test_duplicated(self, scenario='duplicated'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('6fy0fO6BHW2IvxunS54N', 'i1Fycyzyi1HeeEN8eVh1', '2019-05-19 02:34:31.999 UTC', '2019-05-21 04:52:29.113 UTC'),
            ('36AUD64tmGggMdmXLwY3', 'inxamrBymPp1kKuGoOXE', '2019-05-19 03:20:30.842 UTC', '2019-05-20 06:38:49.76 UTC'),
            ('TcPf6gwxm98aVufsLcCo', '1DfPwD1x6teNIhv8aS3p', '2019-05-19 00:39:49.472 UTC', '2019-05-19 05:13:56.514 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.try_insert(table_name)
        result_duplicate_table = self.select_star('public.duplicate')
        result = {'main_table': result, 'duplicate_table': result_duplicate_table}
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_task_length(self, scenario='task_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('asd', 'i1Fycyzyi1HeeEN8eVh1', '2019-05-19 02:34:31.999 UTC', '2019-05-21 04:52:29.113 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_vehicle_length(self, scenario='vehicle_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('6fy0fO6BHW2IvxunS54N', '1234', '2019-05-19 02:34:31.999 UTC', '2019-05-21 04:52:29.113 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_missing_value(self, scenario='missing_value'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('6fy0fO6BHW2IvxunS54N', '1234', '2019-05-19 02:34:31.999 UTC', None),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_invalid_time(self, scenario='invalid_time'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'time_task_created', 'time_task_resolved'),
            ('6fy0fO6BHW2IvxunS54N', '1234', '2019-05-19 02:34:31.999 UTC', '2019-05-21-04:52:29.113 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])



class TestInsertIntoPickup(BaseTestInsertInto):
    table_name = 'pickup'


    def test_valid_data(self, scenario='valid_data'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('yV4kXBtAx6GNRcJDLsF2', 'JkDkmUHoZ4ngg7hAx0F4', 'XXBGC5', '2019-05-04 21:30:07.089 UTC', '2019-05-04 21:30:07.883 UTC'),
            ('xGAnYaxqoyGUa8RFcaTr', 'NpbLgU80Rliumo1QkoeZ', 'NWM2QA', '2019-05-04 00:07:17.13 UTC', '2019-05-05 16:11:24.233 UTC'),
            ('MdkXjdQEW0BfFssY1w44', 'qHbfAktQ2MYfme75EhBM', 'M43B0N', '2019-05-04 18:36:10.529 UTC', '2019-05-04 18:36:10.998 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertListEqual(result, expectations[table_name][scenario])


    def test_duplicated(self, scenario='duplicated'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('yV4kXBtAx6GNRcJDLsF2', 'JkDkmUHoZ4ngg7hAx0F4', 'XXBGC5', '2019-05-04 21:30:07.089 UTC', '2019-05-04 21:30:07.883 UTC'),
            ('xGAnYaxqoyGUa8RFcaTr', 'NpbLgU80Rliumo1QkoeZ', 'NWM2QA', '2019-05-04 00:07:17.13 UTC', '2019-05-05 16:11:24.233 UTC'),
            ('MdkXjdQEW0BfFssY1w44', 'qHbfAktQ2MYfme75EhBM', 'M43B0N', '2019-05-04 18:36:10.529 UTC', '2019-05-04 18:36:10.998 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.try_insert(table_name)
        result_duplicate_table = self.select_star('public.duplicate')
        result = {'main_table': result, 'duplicate_table': result_duplicate_table}
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_task_length(self, scenario='task_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('yV4kXBtasasaaAx6GNRcJDLsF2', 'JkDkmUHoZ4ngg7hAx0F4', 'XXBGC5', '2019-05-04 21:30:07.089 UTC', '2019-05-04 21:30:07.883 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_vehicle_length(self, scenario='vehicle_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('yV4kXBtAx6GNRcJDLsF2', 'JkDkmUHoZasasa4ngg7hAx0F4', 'XXBGC5', '2019-05-04 21:30:07.089 UTC', '2019-05-04 21:30:07.883 UTC'),
            ('xGAnYaxqoyGUa8RFcaTr', 'NpbLgU80Rliumo1QkoeZ', 'NWM2QA', '2019-05-04 00:07:17.13 UTC', '2019-05-05 16:11:24.233 UTC'),
            ('MdkXjdQEW0BfFssY1w44', 'qHbfAktQ2MYfme75EhBM', 'M43B0N', '2019-05-04 18:36:10.529 UTC', '2019-05-04 18:36:10.998 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_qr_length(self, scenario='qr_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('xGAnYaxqoyGUa8RFcaTr', '12', 'NWM2QA', '2019-05-04 00:07:17.13 UTC', '2019-05-05 16:11:24.233 UTC'),
            ('MdkXjdQEW0BfFssY1w44', 'qHbfAktQ2MYfme75EhBM', 'M43B0N', '2019-05-04 18:36:10.529 UTC', '2019-05-04 18:36:10.998 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_missing_value(self, scenario='missing_value'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('asd', None, None, '2019-05-04 18:36:10.529 UTC', None,)
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_invalid_time(self, scenario='invalid_time'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (task_id, vehicle_id, qr_code, time_task_created, time_task_resolved) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('task_id', 'vehicle_id', 'qr_code', 'time_task_created', 'time_task_resolved'),
            ('MdkXjdQEW0BfFssY1w44', 'qHbfAktQ2MYfme75EhBM', 'M43B0N', '2019/05-04 18:36:10.529 UTC', '2019-05-04 18:36:10.998 UTC'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])



class TestInsertIntoRide(BaseTestInsertInto):
    table_name = 'ride'


    def test_valid_data(self, scenario='valid_data'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('ride_id', 'vehicle_id', 'time_ride_start', 'time_ride_end', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gross_amount'),
            ('dVPK7fEkGfd3HJiYRMkj', 'f42vVrTOJh7yVYJACDCv', '2019-05-02 19:24:01.692 UTC', '2019-05-02 19:30:43.433 UTC', '48.829338073730469', '2.3760232925415039', '48.83233642578125', '2.3605866432189941', '2'),
            ('y9xR90cTsRdWkC2JuO0r', 'ZWuRqR3eNjRVEQlHPsJy', '2019-05-02 12:32:39.928 UTC', '2019-05-02 12:52:42.333 UTC', '48.83697509765625', '2.3043417930603027', '48.826877593994141', '2.3263616561889648', '4'),
            ('GSX8MZkpWEttApiFsELL', 'j8JnGWoM5bohbxAOzm42', '2019-05-02 21:59:50.218 UTC', '2019-05-02 22:08:13.39 UTC', '48.888835906982422', '2.2892532348632812', '48.888271331787109', '2.3052382469177246', '2.25'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertListEqual(result, expectations[table_name][scenario])


    def test_duplicated(self, scenario='duplicated'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('ride_id', 'vehicle_id', 'time_ride_start', 'time_ride_end', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gross_amount'),
            ('dVPK7fEkGfd3HJiYRMkj', 'f42vVrTOJh7yVYJACDCv', '2019-05-02 19:24:01.692 UTC', '2019-05-02 19:30:43.433 UTC', '48.829338073730469', '2.3760232925415039', '48.83233642578125', '2.3605866432189941', '2'),
            ('y9xR90cTsRdWkC2JuO0r', 'ZWuRqR3eNjRVEQlHPsJy', '2019-05-02 12:32:39.928 UTC', '2019-05-02 12:52:42.333 UTC', '48.83697509765625', '2.3043417930603027', '48.826877593994141', '2.3263616561889648', '4'),
            ('GSX8MZkpWEttApiFsELL', 'j8JnGWoM5bohbxAOzm42', '2019-05-02 21:59:50.218 UTC', '2019-05-02 22:08:13.39 UTC', '48.888835906982422', '2.2892532348632812', '48.888271331787109', '2.3052382469177246', '2.25'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.try_insert(table_name)
        result_duplicate_table = self.select_star('public.duplicate')
        result = {'main_table': result, 'duplicate_table': result_duplicate_table}
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_ride_length(self, scenario='ride_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('ride_id', 'vehicle_id', 'time_ride_start', 'time_ride_end', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gross_amount'),
            ('dVPK7fEkGfd3HJi_____YRMkj', 'f42vVrTOJh7yVYJACDCv', '2019-05-02 19:24:01.692 UTC', '2019-05-02 19:30:43.433 UTC', '48.829338073730469', '2.3760232925415039', '48.83233642578125', '2.3605866432189941', '2'),
            ('y9xR90cTsRdWkC2JuO0r', 'ZWuRqR3eNjRVEQlHPsJy', '2019-05-02 12:32:39.928 UTC', '2019-05-02 12:52:42.333 UTC', '48.83697509765625', '2.3043417930603027', '48.826877593994141', '2.3263616561889648', '4'),
            ('GSX8MZkpWEttApiFsELL', 'j8JnGWoM5bohbxAOzm42', '2019-05-02 21:59:50.218 UTC', '2019-05-02 22:08:13.39 UTC', '48.888835906982422', '2.2892532348632812', '48.888271331787109', '2.3052382469177246', '2.25'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_vehicle_length(self, scenario='vehicle_length'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('ride_id', 'vehicle_id', 'time_ride_start', 'time_ride_end', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gross_amount'),
            ('dVPK7fEkGfd3HJiYRMkj', 'f42vh7yVYJACDCv', '2019-05-02 19:24:01.692 UTC', '2019-05-02 19:30:43.433 UTC', '48.829338073730469', '2.3760232925415039', '48.83233642578125', '2.3605866432189941', '2'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_missing_value(self, scenario='missing_value'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('dVPK7fEkGfd3HJiYRMkj', 'f42vVrTOJh7yVYJACDCv', '2019-05-02 19:24:01.692 UTC', None, '48.829338073730469', '2.3760232925415039', '48.83233642578125', '2.3605866432189941', '2'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


    def test_invalid_invalid_time(self, scenario='invalid_time'):
        table_name = self.table_name
        test_insert_clause = (sql
            .SQL('INSERT INTO staging.{} (ride_id, vehicle_id, time_ride_start, time_ride_end, start_lat, start_lng, end_lat, end_lng, gross_amount) VALUES %s')
            .format(sql.Identifier(table_name))
        )
        test_insert_values = [
            ('ride_id', 'vehicle_id', 'time_ride_start', 'time_ride_end', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'gross_amount'),
            ('GSX8MZkpWEttApiFsELL', 'j8JnGWoM5bohbxAOzm42', '2019-05-02 21:69:50.218 UTC', '2019-05-02 22:08:13.39 UTC', '48.888835906982422', '2.2892532348632812', '48.888271331787109', '2.3052382469177246', '2.25'),
        ]
        result = self._check_insert_on_main_table(
            table_name=table_name,
            scenario=scenario,
            test_insert_clause=test_insert_clause,
            test_insert_values=test_insert_values,
        )
        self.assertDictEqual(result, expectations[table_name][scenario])


if __name__ == '__main__':
    unittest.main()
