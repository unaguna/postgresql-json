import psycopg2.extras
import psycopg2.extensions


def jsonschema(conn: psycopg2.extensions.connection,
               schema: str,
               table: str) -> dict:
    """

    Parameters
    ----------
    conn
        open connection to database
    schema
        target schema of the database
    table
        target table in the schema


    Returns
    -------
    dict
        jsonschema
    """
    # TODO: インジェクション対策
    col_query = f"SELECT * FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{table}'"

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(col_query)
        result = list(map(col2jsonschema, cur))

    # TODO: 適切にjsonschemaを構成
    return {}


def col2jsonschema(col: psycopg2.extras.DictRow) -> dict:
    """Build jsonschema of a column

    Parameters
    ----------
    col
        target column

    Returns
    -------
    dict
        jsonschema of the column
    """
    result = {}

    # Limited string
    if col['character_maximum_length'] is not None:
        result['type'] = ['string']
        result['maxLength'] = col['character_maximum_length']
    # Unlimited string
    elif col['data_type'] == 'character varying':
        result['type'] = ['string']

    if col['is_nullable']:
        if 'type' in result:
            result['type'].append('null')

    return result
