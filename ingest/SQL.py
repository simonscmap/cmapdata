"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - SQL - SQL table/index suggestions and formatting
"""


import pandas as pd
import numpy as np

import credentials as cr
import common as cmn

######## Python Specific ############


def write_SQL_file(sql_str, tableName, make="observation"):
    """Takes SQL sugested string and builds a .sql record file.

    Args:
        sql_str (str): SQL string for table creation
        tableName (str): Valid CMAP table name
        make (str, optional): observation, model, assimilation. Defaults to "observation".
    """
    with open(
        f"""../../DB/mssql/build/tables/{make}/{tableName}.sql""", "w"
    ) as sql_file:
        print(sql_str, file=sql_file)


def build_SQL_suggestion_df(df):
    """Builds a dataframe of colulmn name and datatype for an input data specific dataframe"""
    sug_df = pd.DataFrame(columns=["column_name", "dtype"])
    exclude_list = [1, "", " ", np.nan, "nan", "NaN", "NAN"]
    for cn in list(df):
        if cn == "time":
            col_dtype = "datetime"
        else:
            col_clean = cmn.exclude_val_from_col(df[cn], exclude_list)
            try:
                col_convert = pd.to_numeric(col_clean)
            except:
                col_convert = col_clean
            col_dtype = col_convert.dtype
        sug_list = [cn, col_dtype]
        sug_df.loc[len(sug_df)] = sug_list
    return sug_df


def SQL_index_suggestion_formatter(
    data_df, tableName, server, FG="Primary", DB="Opedia"
):
    """Creates SQL index suggestion from input data DF"""
    if "depth" in cmn.lowercase_List(list(data_df)):
        uniqueTF = data_df.duplicated(subset=["time", "lat", "lon", "depth"]).any()
        depth_str = "depth"
        depth_flag_comma = ","
        depth_flag_lp = "["
        depth_flag_rp = "]"
    else:
        uniqueTF = data_df.duplicated(subset=["time", "lat", "lon"]).any()
        depth_str = ""
        depth_flag_comma = ""
        depth_flag_lp = ""
        depth_flag_rp = ""

    if uniqueTF == True:
        UNIQUE_flag = ""
    else:
        UNIQUE_flag = "UNIQUE"
    # if any are True, there are duplicates in subset
    SQL_index_str = f"""
    USE [{DB}]


    CREATE {UNIQUE_flag} NONCLUSTERED INDEX [IX_{tableName}_time_lat_lon_{depth_str}] ON [dbo].[{tableName}]
    (
    	[time] ASC,
    	[lat] ASC,
    	[lon] ASC{depth_flag_comma}
    	{depth_flag_lp}{depth_str}{depth_flag_rp}
    )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [{FG}]"""

    SQL_index_dir = {"sql_index": SQL_index_str}
    return SQL_index_dir


def SQL_tbl_suggestion_formatter(sdf, tableName, server, FG="Primary", DB="Opedia"):
    """Creates SQL table suggestion from input dataframe"""

    sdf["null_status"] = "NULL,"
    sdf.loc[sdf["column_name"] == "time", "dtype"] = "[datetime]"
    sdf.loc[sdf["column_name"] == "time", "null_status"] = "NOT NULL,"
    sdf.loc[sdf["column_name"] == "lat", "null_status"] = "NOT NULL,"
    sdf.loc[sdf["column_name"] == "lon", "null_status"] = "NOT NULL,"
    if "depth" in list(sdf["column_name"]):
        sdf.loc[sdf["column_name"] == "depth", "null_status"] = "NOT NULL,"
    sdf["null_status"].iloc[-1] = sdf["null_status"].iloc[-1].replace(",", "")
    sdf["column_name"] = "[" + sdf["column_name"].astype(str) + "]"
    sdf["dtype"] = sdf["dtype"].replace("object", "[nvarchar](200)")
    sdf["dtype"] = sdf["dtype"].replace("float64", "[float]")
    sdf["dtype"] = sdf["dtype"].replace("int64", "[int]")

    var_string = sdf.to_string(header=False, index=False)

    """ Print table as SQL format """
    SQL_tbl = f"""
    USE [{DB}]

    SET ANSI_NULLS ON


    SET QUOTED_IDENTIFIER ON


    CREATE TABLE [dbo].[{tableName}](

    {var_string}


    ) ON [{FG}]"""

    sql_dict = {"sql_tbl": SQL_tbl}
    return sql_dict
