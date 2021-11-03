import os.path
from pprint import pprint
import psycopg2
import psycopg2.extensions
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

        with TemporaryColumnsCursor('col', f'varchar({length})') as cur:
            column = cur.fetch_column()
        jsonschema = col2jsonschema(column)

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
        with TemporaryColumnsCursor('col', 'varchar') as cur:
            column = cur.fetch_column()
        jsonschema = col2jsonschema(column)

        pprint(jsonschema)

        ######################################################################
        # Assertion
        ######################################################################
        assert jsonschema.keys() == {'type'}

        # schema.type contains 'string'.
        # If the column is nullable, schema.type contains 'null'.
        assert isinstance(jsonschema['type'], list)
        assert set(jsonschema['type']) == {'string', 'null'}


class TemporaryColumnsCursor:
    """Context manager to create temporary table with exactly one column.

    When enter, it create temporary table with exactly one column in the database for use in test.
    When exit, it will do rollback to delete the temporary table.

    By use ``self.fetch_column``, you can get the data of the column of new table.
    """
    col_name: str
    col_type: str
    conn: psycopg2.extensions.connection
    cur: psycopg2.extensions.cursor

    def __init__(self, col_name: str, col_type: str):
        self.col_name = col_name
        self.col_type = col_type

    def __enter__(self) -> 'TemporaryColumnsCursor':
        self.conn = psycopg2.connect(DSN)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # create new table for the test
        query = self._build_tbl_query()
        self.cur.execute(query)

        # get data of the column
        query = self._build_col_query()
        self.cur.execute(query)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # remove the new table
        self.conn.rollback()

        self.cur.close()
        self.conn.close()

    def fetch_column(self) -> psycopg2.extras.DictRow:
        column = self.cur.fetchone()
        pprint(dict(column))
        return column

    def _build_tbl_query(self):
        return f"CREATE TABLE {SCHEMA}.{TABLE}({self.col_name} {self.col_type})"

    def _build_col_query(self):
        return f"SELECT * FROM information_schema.columns WHERE table_schema='{SCHEMA}' AND " \
               f"table_name='{TABLE}' AND column_name='{self.col_name}'"
