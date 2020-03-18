import json
import snowflake.connector
import pymysql
import logging

logging.getLogger().setLevel(logging.INFO)


def gen_mysql_conn(dbuser, secret_file, dbname=None, verbose=1):
    """Create mysql connection
    """
    with open(secret_file, "r") as f:
        params = json.load(f)
        if dbname is None:
            dbname = params[dbuser]["dbname"]
        conn = pymysql.connect(
            host=params[dbuser]["host"],
            user=params[dbuser]["user"],
            passwd=params[dbuser]["password"],
            database=params[dbuser]["dbname"] if dbname is None else dbname,
            port=params[dbuser]["port"],
        )
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if verbose >= 1:
            logging.info(f"Successfully connected with mysql: {dbname}")
    return conn, cursor



def gen_snowflake_conn(
    dbuser, secret_file, role=None, warehouse=None, dbname=None, schema=None, decoder=None, verbose=1
):
    """Create Snowflake connection
    """
    # Parameters
    if decoder is None:
        params = json.load(open(secret_file, "r"))
    else:
        params = json.loads(decoder.decode(secret_file))

    if "dbname" not in params[dbuser]:
        params[dbuser]["dbname"] = params[dbuser].get("database", "")

    warehouse = params[dbuser]["warehouse"] if warehouse is None else warehouse
    dbname = params[dbuser]["dbname"] if dbname is None else dbname
    schema = params[dbuser]["schema"] if schema is None else schema

    assert warehouse is not None
    assert dbname is not None
    assert schema is not None

    # Get the connection and execute the query
    conn = snowflake.connector.connect(
        user=dbuser,
        password=params[dbuser]["password"],
        role=params[dbuser]["role"] if role is None else role,
        warehouse=warehouse,
        database=dbname,
        schema=schema,
        account=params[dbuser]["account"],
        region=params[dbuser]["region"],
    )
    cursor = conn.cursor()
    if verbose >= 1:
        logging.info(f"Successfully connected with Snowflake db: {dbname}, schema: {schema}")

    if verbose >= 2:
        cursor.execute("select current_version()")
        logging.info("Snowflake connector version: %s" % cursor.fetchone()[0])
    return conn, cursor
