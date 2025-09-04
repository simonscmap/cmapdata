"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - stats - cmap summary stats functionallity. 
"""


import os
from tqdm import tqdm
# import dask.dataframe as dd
import pandas as pd
import numpy as np
import glob
from tqdm import tqdm

import credentials as cr
import common as cmn
import DB
import transfer
import data
import vault_structure as vs
from datetime import datetime


def updateStatsTable(ID, json_str, server):
    """Updates entry for tblDataset_Stats

    Args:
        ID (str): Dataset_ID
        json_str (str): Formatted JSON string for tblDataset_Stats
        Server (str): Valid CMAP server name
    """
    conn, cursor = DB.dbConnect(server)
    deleteQuery = f"""DELETE FROM tblDataset_Stats WHERE Dataset_ID = '{ID}'"""
    insertQuery = f"""INSERT INTO tblDataset_Stats (Dataset_ID, JSON_stats) VALUES('{ID}','{json_str}')"""
    try:
        DB.DB_modify(deleteQuery, server)
        DB.DB_modify(insertQuery, server)
    except Exception as e:
        print(e)


def updateStats_Small(tableName, db_name, server, data_df=None):
    """Updates entry for tblDataset_Stats, wraps around updateStatsTable, but with common inputs.

    Args:
        tableName (str): CMAP table name
        db_name (str): CMAP database name (Opedia)
        Server (str): Valid CMAP server name
        data_df (Pandas DataFrame, optional): datframe to build stats from, if not provided table is queried from database. Defaults to None.
    """

    if data_df is not None:
        data_df = data_df
    else:
        query = "SELECT * FROM {tableName}".format(tableName=tableName)
        data_df = DB.dbRead(query, server)
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    data_df.replace(r'^\s*$', np.nan, regex=True, inplace = True)
    stats_df = data_df.describe()
    if "_Climatology" in tableName:
        return
    ## Include empty rows for non-numeric columns
    for var in data_df.columns.to_list():
        if var not in stats_df and var not in ['time','year','month','week','dayofyear','hour']:
            stats_df[var] = np.nan
    min_max_df = pd.DataFrame(
        {"time": [data_df["time"].min(), data_df["time"].max()]}, index=["min", "max"]
    )
    df = pd.concat([min_max_df, stats_df], axis=1, sort=True)
    ## Stats has to include time for plot defaults
    if 'datetime' in df.time.dtype.name:
        df['time'] =  df['time']
    else:
        df['time'] =  df['time'].astype('datetime64[ns]')
    json_str = df.to_json(date_format="iso")
    updateStatsTable(Dataset_ID, json_str, server)
    print("Updated stats for " + tableName)

def updateStats_Manual(dt1, dt2, lat1, lat2, lon1, lon2, dpt1, dpt2, row_count, tableName, db_name, server):
    """Updates entry for tblDataset_Stats, wraps around updateStatsTable, but with common inputs.
    Args:
        dt1, dt1, lat1, lat2, lon1, lon2: Min/Max stats of dataset
        dpt1, dpt2: Mix/max depth of dataset. If no depth in dataset, set to None
        row_count: Total row count for dataset
        tableName (str): CMAP table name
        db_name (str): CMAP database name (Opedia)
        Server (str): Valid CMAP server name
    """   
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    var_df = DB.dbRead(f"SELECT Short_Name FROM tblVariables WHERE Dataset_ID = {Dataset_ID}", server)
    stats_df = pd.DataFrame(index=['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max'])
    for var in var_df['Short_Name'].to_list():
        stats_df[var] = np.nan
    if dpt1 is None:
        min_max_df = pd.DataFrame(
            {"time": [dt1, dt2, row_count], "lat":[lat1, lat2, row_count], "lon":[lon1, lon2, row_count]}, index=["min", "max", "count"]
        )
    else:
        min_max_df = pd.DataFrame(
            {"time": [dt1, dt2, row_count], "lat":[lat1, lat2, row_count], "lon":[lon1, lon2, row_count], "depth":[dpt1, dpt2, row_count]}, index=["min", "max", "count"]
        )        
    df = pd.concat([min_max_df, stats_df], axis=1, sort=True)
    # if 'datetime' in df.time.dtype.name:
    #     df['time'] =  df['time'].dt.date
    # else:
    #     df['time'] =  df['time'].astype('datetime64[ns]').dt.date
    json_str = df.to_json(date_format="iso")
    updateStatsTable(Dataset_ID, json_str, server)
    print("Updated stats for " + tableName)


def buildLarge_Stats(df, datetime_slice, tableName, branch, transfer_flag="dropbox"):
    """Calculates summary stats for a dataframe. Can be used when calculating stats for large dataset ingestion. Alternative is build_stats_df_from_db_calls().

    Args:
        df (Pandas DataFrame): Input cleaned dataframe
        datetime_slice (str): time slice of input data. Ex. daily sat would be daily.
        tableName (str): CMAP table name
        branch (str): vault branch path, ex float.
        transfer_flag (str, optional): Way to transfer data, either dropbox or sshfs. Defaults to "dropbox".
    """
    """Input is dataframe slice (daily, 8 day, monthly etc.) of a dataset that is split into multiple files"""
    
    df_stats = df.describe()
    if "_Climatology" not in tableName:
        df_stats.insert(loc=0, column="time", value="")
        df_stats.at["count", "time"] = len(df["time"])
        df_stats.at["min", "time"] = min(df["time"])
        df_stats.at["max", "time"] = max(df["time"])
    branch_path = cmn.vault_struct_retrieval(branch)

    if transfer_flag == "dropbox":
        df_stats.to_csv("stats_temp.csv")
        transfer.dropbox_file_transfer(
            "stats_temp.csv",
            branch_path.split("Vault")[1]
            + tableName
            + "/stats/"
            + datetime_slice
            + "_summary_stats.csv",
        )
        os.remove("stats_temp.csv")
    else:
        df_stats.to_csv(
            branch_path + tableName + "/stats/" + datetime_slice + "_summary_stats.csv"
        )
    print(
        "stats built for :"
        + tableName
        + "  in branch: "
        + branch
        + " for date: "
        + datetime_slice
    )


def aggregate_large_stats(branch, tableName, server):
    """Aggregates stats of stats files for large dataset ingestion. Alternative is db calls using build_stats_df_from_db_calls()

    Args:
        branch (str): vault branch path, ex float.
        tableName (str): CMAP table name
        Server (str): Valid CMAP server name

    Returns:
        Pandas DataFrame: stats dataframe
    """
    """aggregates summary stats files and computes stats of stats. Returns stats dataframe"""
    branch_path = cmn.vault_struct_retrieval(branch)
    df = dd.read_csv(branch_path + tableName + "/stats/" + "*.csv*")
    df = df.compute().set_index(df.columns[0])
    df.index.name = "Stats"
    st_cols = data.ST_columns(df)
    ## ST_columns hard codes column names and order. if climatology, replace 'time' with 'month'
    if "_Climatology" in tableName:
        st_cols[0] = "month"
    var_list = list(set(list(df)) - set(st_cols))
    var_max_list, var_min_list, var_mean_list, var_std_list = [], [], [], []
    max_var_count_list = [cmn.getLatCount(tableName, server)] * len(list(df))
    for var in list(df):  # var_list:
        var_max = max(df.loc["max", var])
        var_max_list.append(var_max)
        var_min = min(df.loc["min", var])
        var_min_list.append(var_min)
        var_mean = np.mean(df.loc["mean", var])
        var_mean_list.append(var_mean)
        var_std = np.std(df.loc["std", var])
        var_std_list.append(var_std)
    stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])

    for var, max_var_count, var_max, var_min, var_mean, var_std in zip(
        list(df),
        max_var_count_list,
        var_max_list,
        var_min_list,
        var_mean_list,
        var_std_list,
    ):
        stats_DF[var] = ""  # create empty column to fill
        stats_DF.at["count", var] = max_var_count
        stats_DF.at["max", var] = var_max
        stats_DF.at["mean", var] = var_mean
        stats_DF.at["min", var] = var_min
        stats_DF.at["std", var] = var_std
    return stats_DF


def build_stats_df_from_db_calls(tableName, server, data_server):
    """

    Builds basic (min,max,count) summary stats from existing table in DB

    Args:
        tableName (str): CMAP table name
        Server (str): Valid CMAP server name

    Returns:
        Pandas DataFrame: stats dataframe
    """
    if len(data_server) >0:
        stats_server = data_server
    else:
        stats_server = server

    if "_Climatology" in tableName:
        col_list = cmn.get_numeric_cols_in_table_excluding_climatology(
        tableName, stats_server
        )
    else:
        col_list =  ["time"] + cmn.get_numeric_cols_in_table_excluding_climatology(
        tableName, stats_server
        ) 
    stats_DF = pd.DataFrame(index=["count", "max", "mean", "min", "std"])
    for var in tqdm(col_list):
        print(var)
        stats_DF[var] = ""
        if var == 'time':
            # stats_qry = (
            #     f"""SELECT count_big({var}),MAX({var}),MIN({var}),'','' FROM {tableName}"""
            # )     
            stats_qry = (
                f"""SELECT count_big({var}),MAX(cast({var} as datetime)),MIN(cast({var} as datetime)),'','' FROM {tableName}"""
            )         
            var_df = DB.dbRead(stats_qry, stats_server)    
        else:
            try:
                stats_qry = (
                    f"""SELECT count_big([{var}]),MAX([{var}]),MIN([{var}]),AVG([{var}]),STDEV([{var}]) FROM {tableName}"""
                )
                var_df = DB.dbRead(stats_qry, stats_server)
            except:
                stats_qry = f"""SELECT count_big([{var}]),MAX(cast([{var}] as numeric(12, 0))),MIN(cast([{var}] as numeric(12, 0))),AVG(cast([{var}] as numeric(12, 0))),STDEV(cast([{var}] as numeric(12, 0))) FROM {tableName}"""
                var_df = DB.dbRead(stats_qry, stats_server)            
        if (var == 'lat' or var == 'lon'):
            stats_DF.at["mean", var] = ""
            stats_DF.at["std", var] = ""
        else:
            stats_DF.at["mean", var] = var_df.iloc[:, 3][0]
            stats_DF.at["std", var] = var_df.iloc[:, 4][0]
        stats_DF.at["count", var] = var_df.iloc[:, 0][0] 
        stats_DF.at["max", var] = var_df.iloc[:, 1][0]
        stats_DF.at["min", var] = var_df.iloc[:, 2][0]
    ## Include empty rows for non-numeric columns
    for var in cmn.get_var_list_dataset(tableName, stats_server):
        if var not in stats_DF and var not in ['time','year','month','week','dayofyear','hour']:
            stats_DF[var] = np.nan
    return stats_DF


def update_stats_large(tableName, stats_df, db_name, server):
    """Updates tblDataset_Stats entry with new stats

    Args:
        tableName (str): CMAP table name
        stats_df (Pandas DataFrame): Input stats dataframe
        Server (str): Valid CMAP server name
    """
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    json_str = stats_df.to_json(date_format="iso")
    sql_df = pd.DataFrame({"Dataset_ID": [Dataset_ID], "JSON": [json_str]})
    updateStatsTable(Dataset_ID, json_str, server)

def build_stats_from_parquet(tableName, make, nrt):
    base_path = getattr(vs,make)
    flist = glob.glob(base_path+f'{tableName}/{nrt}/*.parquet')
    df_stats = pd.DataFrame()
    for fil in tqdm(flist):
        df = pd.read_parquet(fil)
        # df = df.query('POSITION_QC!="4" & JULD_QC!="4"')
        min_time = df['time'].min()
        max_time = df['time'].max()
        df_sp = df[['lat','lon','depth']]
        df_sp_d = df_sp.describe()
        d = {'min_time':[min_time], 'max_time':[max_time], 'min_lat':[df_sp_d['lat']['min']], 'max_lat':[df_sp_d['lat']['max']], 'min_lon':[df_sp_d['lon']['min']], 'max_lon':[df_sp_d['lon']['max']], 'min_depth':[df_sp_d['depth']['min']], 'max_depth':[df_sp_d['depth']['max']]}    
        temp_df = pd.DataFrame(data=d)
        df_stats = df_stats.append(temp_df, ignore_index = True)
        del df, df_sp,df_sp_d
    min_time = df_stats['time'].min()
    max_time = df_stats['time'].max()
    min_lat = df_stats['lat'].min()
    max_lat = df_stats['lat'].max()
    min_lon = df_stats['lon'].min()
    max_lon = df_stats['lon'].max()
    min_depth = df_stats['depth'].min()
    max_depth = df_stats['depth'].max()
    return min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth

def pull_from_stats_folder(tableName, make):
    try:
        df_stats = pd.read_excel(f'{make}{tableName}/stats/{tableName}_stats.xlsx')
        df_stats = df_stats.query('max_depth < 20000 and min_lat >=-90.0')
        min_time = df_stats['min_time'].min()
        max_time = df_stats['max_time'].max()
        min_lat = df_stats['min_lat'].min()
        max_lat = df_stats['max_lat'].max()
        min_lon = df_stats['min_lon'].min()
        max_lon = df_stats['max_lon'].max()
        min_depth = df_stats['min_depth'].min()
        max_depth = df_stats['max_depth'].max()
        row_count = df_stats['row_count'].sum()

        
        # min_time, max_time = datetime(2022, 1, 1, 12, 0, 0, 0), datetime(2024, 3, 29, 12, 0, 0, 0)
        # min_lat, max_lat = -80, 90
        # min_lon, max_lon = -180, 179.75
        # min_depth, max_depth = 0.4940253794193268, 5727.91650390625
        # row_count = 40157208000

    except Exception as e:    
        print(f"Error in pull_from_stats_folder:\n{e}")
    return min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth, row_count