"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - DB - database connections and CRUD operations. 
"""


import sys
import os

import credentials as cr
import pyodbc
import sqlalchemy
import urllib
import pyodbc
import pandas.io.sql as sql
import platform
import pandas as pd
import bcpandas
import pycmap

pycmap.API(cr.api_key)
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
        print(e)
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


def lineInsert(server, tableName, columnList, query, ID_insert=False):
    """Single line insert functionallity

    Args:
        server (str): Valid CMAP server name. ex Rainier
        tableName (str): Valid CMAP table name
        columnList (list): list of columns in table
        query (str): sql query values
        ID_insert (bool, optional): Identity_insert. Defaults to False.
    """
    insertQuery = """INSERT INTO {} {} VALUES {} """.format(
        tableName, columnList, query
    )
    if ID_insert == True:
        insertQuery = (
            f"""SET IDENTITY_INSERT {tableName} ON """
            + insertQuery
            + f""" SET IDENTITY_INSERT {tableName} OFF """
        )
    conn, cursor = dbConnect(server)
    cursor.execute(insertQuery)
    conn.commit()


def urllib_pyodbc_format(conn_str):
    """formats pyodbc connection string with urllib"""
    quoted_conn_str = urllib.parse.quote_plus(conn_str)
    return quoted_conn_str


def toSQLpandas(df, tableName, server):
    """SQL ingestion option, uses padnas to_sql functionallity.

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
    engine = sqlalchemy.create_engine(
        "mssql+pyodbc:///?odbc_connect={}".format(quoted_conn_str)
    )
    df.to_sql(tableName, con=engine, if_exists="append", method="multi", index=False)


def toSQLbcpandas(df, tableName, server):
    """SQL ingestion option, exploritory, uses bcpandas"""
    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    creds = bcpandas.SqlCreds(ip, db_name, usr, psw)
    bcpandas.to_sql(df, tableName, creds, index=False, if_exists="append")


def toSQLbcp_wrapper(df, tableName, server):
    """wrapper func for toSQLbcp function. Simplifies arguments"""
    export_path = "temp_bcp.csv"
    df.to_csv(export_path, index=False)
    toSQLbcp(export_path, tableName, server)
    os.remove(export_path)


def toSQLbcp(export_path, tableName, server):
    """SQL ingestion option, uses bcp (BULK COPY PROGRAM) from MSSQL tools. See: https://docs.microsoft.com/en-us/sql/tools/bcp-utility?view=sql-server-ver15"""

    usr, psw, ip, port, db_name, TDS_Version = server_select_credentials(server)
    bcp_str = (
        """bcp Opedia.dbo."""
        + tableName
        + """ in """
        + """'"""
        + export_path
        + """'"""
        + """ -e error -F 2 -c -t, -U  """
        + usr
        + """ -P """
        + psw
        + """ -S """
        + ip
        + ""","""
        + port
    )
    os.system(bcp_str)
