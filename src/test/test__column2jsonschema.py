import os.path
from pprint import pprint
import psycopg2
import psycopg2.extras

from psqljson._jsonschema import col2jsonschema
from res.local import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD

DSN = f'dbname={DB_NAME} host={DB_HOST} user={DB_USER} password={DB_PASSWORD}'
SCHEMA = 'public'
TABLE = os.path.basename(os.path.splitext(__file__)[0])


class TestColumn2Jsonschema:

    def test__varchar_n(self):
        """ Normal test.
        """
        length = 123
        col_name = 'col'
        tbl_query = f"CREATE TABLE {SCHEMA}.{TABLE}({col_name} varchar({length}))"
        col_query = f"SELECT * FROM information_schema.columns WHERE table_schema='{SCHEMA}' AND " \
                    f"table_name='{TABLE}' AND column_name='{col_name}'"

        with psycopg2.connect(DSN) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(tbl_query)
            cur.execute(col_query)

            jsonschema = col2jsonschema(cur.fetchone())
            conn.rollback()

        pprint(jsonschema)

        ######################################################################
        # Assertion
        ######################################################################
        assert jsonschema.keys() == {'type', 'maxLength'}

        # schema.type contains 'string'.
        # If the column is nullable, schema.type contains 'null'.
        assert isinstance(jsonschema['type'], list)
        assert set(jsonschema['type']) == {'string', 'null'}

        # schema.maxLength equals to length of varchar
        assert jsonschema['maxLength'] == length

    def test__varchar(self):
        """ Normal test.
        """
        col_name = 'col'
        tbl_query = f"CREATE TABLE {SCHEMA}.{TABLE}({col_name} varchar)"
        col_query = f"SELECT * FROM information_schema.columns WHERE table_schema='{SCHEMA}' AND " \
                    f"table_name='{TABLE}' AND column_name='{col_name}'"

        with psycopg2.connect(DSN) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(tbl_query)
            cur.execute(col_query)

            column = cur.fetchone()
            pprint(dict(column))

            jsonschema = col2jsonschema(column)
            conn.rollback()

        pprint(jsonschema)

        ######################################################################
        # Assertion
        ######################################################################
        assert jsonschema.keys() == {'type'}

        # schema.type contains 'string'.
        # If the column is nullable, schema.type contains 'null'.
        assert isinstance(jsonschema['type'], list)
        assert set(jsonschema['type']) == {'string', 'null'}
