
"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - DB - database connections and CRUD operations. 
"""


import sys
import os
sys.path.append("../../ingest") 

import credentials as cr
import math
import json
import pyodbc
import re
import numpy as np
import pandas as pd
from datetime import datetime, date
import sqlalchemy
import urllib
import pandas.io.sql as sql
import platform
import pandas as pd
import bcpandas
import pycmap
import common as cmn
import warnings
warnings.filterwarnings('ignore')


# pycmap.API(cr.api_key)
######## DB Specific ############


def DB_query(query):
    """runs a SQL query using the pycmap query functionality"""
    api = pycmap.API()
    query_result = api.query(query)
    return query_result


def DB_modify(cmnd, server):
    """SQL modification function, use with caution!

    Args:
        cmnd (str): SQL query. ex. UPDATE tblRegions SET Region = 'North Pole' WHERE ID = '54NTA'
        server (str): Valid CMAP server name. ex Rainier
    """
    try:
        conn, cursor = dbConnect(server)
        conn.autocommit = True
        cursor.execute(cmnd)
        conn.close()
    except Exception as e:
        print(f"DB_modify Error: {e}")
        conn.close()


def dbRead(query, server):
    """runs a SQL query using the pyodbc query functionality"""
    conn, cursor = dbConnect(server)
    df = sql.read_sql(query, conn)
    conn.close()
    return df


def server_select_credentials(server):
    """Select and returns server credentials based on input server name. Add more servers if needed.

    Args:
        server (str): Valid CMAP server name. ex Rainier

    Returns:
        str(s): usr,psw,ip,port,db_name,TDS_Version
    """
    db_name = "opedia"
    TDS_Version = "7.3"
    if server.lower() == "rainier":
        usr = cr.usr_rainier
        psw = cr.psw_rainier
        ip = cr.ip_rainier
        port = cr.port_rainier
    elif server.lower() == "mariana":
        usr = cr.usr_mariana
        psw = cr.psw_mariana
        ip = cr.ip_mariana
        port = cr.port_mariana
    elif server.lower() == "rossby":
        usr = cr.usr_rossby
        psw = cr.psw_rossby
        ip = cr.ip_rossby
        port = cr.port_rossby
    elif server.lower() == "beast":
        db_name = 'Opedia_Sandbox'
        usr = cr.usr_beast
        psw = cr.psw_beast
        ip = cr.ip_beast
        port = cr.port_beast
    else:
        print("invalid server selected. exiting...")
        sys.exit()
    return usr, psw, ip, port, db_name, TDS_Version


def pyodbc_connection_string(server):
    """Returns pyodbc connection string

    Args:
        server (str): Valid CMAP server name. ex Rainier

    Returns:
        str: connection string
    """
    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    server = ip + "," + port

    if platform.system().lower().find("windows") != -1:
        driver_str = "{SQL Server}"
    elif platform.system().lower().find("darwin") != -1:
        driver_str = "/usr/local/lib/libtdsodbc.so"
    elif platform.system().lower().find("linux") != -1:
        driver_str = "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so"

    conn_str = """DRIVER={driver_str};TDS_Version={TDS_Version};SERVER={ip};PORT={port};database={db_name};UID={usr};PWD={psw}""".format(
        driver_str=driver_str,
        TDS_Version=TDS_Version,
        ip=ip,
        port=port,
        db_name=db_name,
        usr=usr,
        psw=psw,
    )
    return conn_str


def dbConnect(server):
    """Creates connection with pyodbc

    Args:
        server (str): Valid CMAP server name. ex Rainier

    Returns:
        conn, cursor: Returns connection and cursor object for pyodbc connection
    """
    conn_str = pyodbc_connection_string(server)
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    return conn, cursor

def addSensor(server, sensor):
    """Adds new Sensor and associated ID from server to Sensor table
    Args:
        server (str): Valid CMAP server name. ex Rainier
        sensor (str): Name of sensor to be added
    """
    last_id = cmn.get_last_ID('tblSensors', server)
    insertQuery = f"INSERT INTO dbo.tblSensors VALUES ({last_id + 1}, '{sensor}')"
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()

def addSpatialResolution(server, res):
    """Adds new resolution and associated ID from server to spatial resolution table
    Args:
        server (str): Valid CMAP server name. ex Rainier
        res (str): Name of spatial resolution to be added
    """
    last_id = cmn.get_last_ID('tblSpatial_Resolutions', server)
    insertQuery = f"INSERT INTO dbo.tblSpatial_Resolutions VALUES ({last_id + 1}, '{res}')"
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()

def addStudyDomain(server, study):
    """Adds new resolution and associated ID from server to study domain table
    Args:
        server (str): Valid CMAP server name. ex Rainier
        study (str): Name of study domain to be added
    """
    last_id = cmn.get_last_ID('tblStudy_Domains', server)
    insertQuery = f"INSERT INTO dbo.tblStudy_Domains VALUES ({last_id + 1}, '{study}')"
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()    


# def lineInsert(server, tableName, columnList, query, ID_insert=False):
#     """Single line insert functionallity

#     Args:
#         server (str): Valid CMAP server name. ex Rainier
#         tableName (str): Valid CMAP table name
#         columnList (list): list of columns in table
#         query (str): sql query values
#         ID_insert (bool, optional): Identity_insert. Defaults to False.
#     """
#     insertQuery = """INSERT INTO {} {} VALUES {} """.format(
#         tableName, columnList, query
#     )
#     insertQuery = insertQuery.replace("'NULL'", "NULL")
#     insertQuery = insertQuery.replace("CHAR(39)", "''")
#     insertQuery = insertQuery.replace(r""""N'""", r"""N'""")
#     insertQuery = insertQuery.replace(r"""'",""", r"""',""")
#     if ID_insert == True:
#         insertQuery = (
#             f"""SET IDENTITY_INSERT {tableName} ON """
#             + insertQuery
#             + f""" SET IDENTITY_INSERT {tableName} OFF """
#         )
#     conn, cursor = dbConnect(server)
#     cursor.execute(insertQuery)
#     conn.commit()



_NULL_STRINGS = {"", "null", "none", "nan", "na"}
_BOOL_STRINGS = {"true": True, "false": False} 


def _to_sql_param(x):
    # pandas/NumPy missing -> None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    if isinstance(x, str):
        s = x.strip()
        ls = s.lower()
        if ls in _NULL_STRINGS:
            return None
        if ls in _BOOL_STRINGS:
            return _BOOL_STRINGS[ls]
        return s   # keep as NVARCHAR; driver will escape quotes/newlines

    if isinstance(x, np.generic):
        x = x.item()

    if isinstance(x, np.integer):
        return int(x)
    if isinstance(x, np.floating):
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(x, np.bool_):
        return bool(x)

    if isinstance(x, np.datetime64):
        return pd.Timestamp(x).to_pydatetime()
    if isinstance(x, pd.Timestamp):
        return None if x is pd.NaT else x.to_pydatetime()

    if isinstance(x, (list, dict, tuple)):
        return json.dumps(x)

    return x


def lineInsert(server, tableName, columnList, values, ID_insert=False):
    """
    Single-row INSERT using parameterized query.

    Args:
        server (str): CMAP server name (used by dbConnect)
        tableName (str): e.g., "[dbo].[tblDatasets]" or "Opedia.[dbo].[tblDatasets]"
        columnList (list|tuple|str): column names OR a "(col1,col2,...)" string
        values (sequence): Python values aligned with columns
        ID_insert (bool): toggle SET IDENTITY_INSERT table ON/OFF
    """
    if isinstance(columnList, (list, tuple)):
        columns_txt = "(" + ",".join(columnList) + ")"
    else:
        columns_txt = columnList  # assume "(col1, col2, ...)"

    placeholders = "(" + ",".join(["?"] * len(values)) + ")"
    sql = f"INSERT INTO {tableName} {columns_txt} VALUES {placeholders};"

    clean_values = tuple(_to_sql_param(v) for v in values)

    conn, cursor = dbConnect(server)
    try:
        if ID_insert:
            cursor.execute(f"SET IDENTITY_INSERT {tableName} ON;")
        cursor.execute(sql, clean_values)
        conn.commit()
    except pyodbc.Error:
        try:
            conn.rollback()
        except Exception:
            pass
        # print(sql)
        # print(clean_values)
        # print([type(v) for v in clean_values])
        raise
    finally:
        if ID_insert:
            try:
                cursor.execute(f"SET IDENTITY_INSERT {tableName} OFF;")
                conn.commit()
            except Exception:
                pass
        cursor.close()
        conn.close()


def queryExecute(server, query):
    """Execute query directly in SQL, not through pycmap query functionality: DB_query(query)

    Args:
        server (str): Valid CMAP server name. ex Rainier
        query (str): sql query values
        ID_insert (bool, optional): Identity_insert. Defaults to False.
    """
    conn, cursor = dbConnect(server)
    cursor.execute(query)
    conn.commit()


def urllib_pyodbc_format(conn_str):
    """formats pyodbc connection string with urllib"""
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    return quoted_conn_str

def sqlalchemy_engine_string(quoted_conn_str):
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(quoted_conn_str)
    )
    return engine

def toSQLpandas(df, tableName, server, exists='append', ch=5000):
    """SQL ingestion option, uses pandas to_sql functionality.

    Args:
        df (Pandas DataFrame): Input Pandas DataFrame
        tableName (str): Valid CMAP table name
        server (str): Valid CMAP server. ex. Rainier
    """
    conn_str = pyodbc_connection_string(server)
    quoted_conn_str = urllib_pyodbc_format(conn_str)
    # engine = sqlalchemy.create_engine(
    #     "mssql+pyodbc:///?odbc_connect={}".format(quoted_conn_str),fast_executemany=True
    # )
    engine = sqlalchemy_engine_string(quoted_conn_str)
    df.to_sql(tableName, con=engine, if_exists=exists, method="multi", index=False, chunksize=ch)


def toSQLbcpandas(df, tableName, server):
    """SQL ingestion option, exploritory, uses bcpandas"""
    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    creds = bcpandas.SqlCreds(ip, db_name, usr, psw)
    bcpandas.to_sql(df, tableName, creds, index=False, if_exists="append")


def toSQLbcp_wrapper(df, tableName, server):
    """wrapper func for toSQLbcp function. Simplifies arguments"""
    export_path = f"{tableName}_{server}_bcp.csv"

    print("-------&&&&&&&&&&&&---------")
    print(f"export_path: {export_path}")
    print(f"server: {server}")
    print()


    df.to_csv(export_path, index=False)
    toSQLbcp(export_path, tableName, server)
    os.remove(export_path)


def toSQLbcp(export_path, tableName, server):
    """SQL ingestion option, uses bcp (BULK COPY PROGRAM) from MSSQL tools. See: https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15"""

    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    bcp_str = (
            """bcp """
            + db_name
            + """.dbo."""
            + tableName
            + """ in """
            + """'"""
            + export_path
            + """'"""
            + """ -F 2 -c -t, -b 50000 -h'TABLOCK' -U """
            + usr
            + """ -P """
            + psw
            + """ -S """
            + ip
            + ""","""
            + port
        )
    os.system(bcp_str)
