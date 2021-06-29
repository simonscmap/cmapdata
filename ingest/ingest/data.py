import sys
import os
import pandas as pd
import numpy as np
import glob
import xarray as xr

from cmapingest import vault_structure as vs
from cmapingest import DB
from cmapingest import common as cmn
from cmapingest import metadata


""" Data Cleaning"""


def removeMissings(df, cols):
    """Removes missing rows for all columns provided

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    cols :  list
        List of column names

    Returns
    -------
    df
        Pandas DataFrame with missing rows removed
    """
    for col in cols:
        df[col].replace(r"^\s*$", np.nan, regex=True, inplace=True)
        # df[col].replace("", np.nan, inplace=True)
        df.dropna(subset=[col], inplace=True)
    return df


def format_time_col(df, time_col, format="%Y-%m-%d %H:%M:%S"):
    """Formats dataframe timecolumn

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    time_col : str
        Name of the time column. ex: 'time'
    format : str, optional, default = %Y-%m-%d %H:%M:%S

    Returns
    -------
    df
        Pandas DataFrame with time col formatted
    """
    df[time_col] = pd.to_datetime(df[time_col].astype(str), errors="coerce")
    # df["time"].dt.strftime(format)

    df[time_col] = df[time_col].dt.strftime(format)
    return df


def mapTo180180(df):
    df["lon"] = pd.to_numeric(df["lon"])
    df.loc[df["lon"] > 180, "lon"] = df.loc[df["lon"] > 180, "lon"] - 360
    return df


def sort_values(df, cols):
    """Sorts dataframe cols

    Parameters
    ----------
    df : Pandas DataFrame
        The dataframe to be modified
    cols : list
        List of column name strings

    Returns
    -------
    df
        Pandas DataFrame with input cols sorts in ASC order.
    """
    df = df.sort_values(cols, ascending=[True] * len(cols))
    return df


def check_ST_ordering(ST_vars):
    """Ensures that ST column list is in correct order. ie ['time','lat','lon'] not ['time','lon','lat]

    Args:
        ST_vars ([type]): [description]
    Returns: ST_vars (sorted)
    """
    if len(ST_vars) == 4:
        st_bool = ST_vars == ["time", "lat", "lon", "depth"]
        if st_bool == False:
            ST_vars = ["time", "lat", "lon", "depth"]
    elif len(ST_vars) == 3:
        st_bool = ST_vars == ["time", "lat", "lon"]
        if st_bool == False:
            ST_vars = ["time", "lat", "lon"]
    return ST_vars


def ST_columns(df):
    """Returns SpaceTime related columns in a dataset as a list"""
    df_cols = cmn.lowercase_List(list(df))
    ST_vars = [i for i in df_cols if i in ["time", "lat", "lon", "depth"]]
    ST_vars_ordered = check_ST_ordering(ST_vars)
    return ST_vars_ordered


def clean_data_df(df):
    """Combines multiple data functions to apply a clean to a pandas df"""
    df = cmn.strip_whitespace_headers(df)
    # df = cmn.nanToNA(df)
    df = format_time_col(df, "time")
    df = removeMissings(df, ST_columns(df))
    df = ensureST_numeric(df)
    df = sort_values(df, ST_columns(df))
    return df


def ensureST_numeric(df):
    ST_cols = ST_columns(df)
    ST_cols.remove("time")
    for col in ST_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def decode_df_columns(df):
    """Decodes any bytestring columns in pandas

    Args:
        df (Pandas DataFrame): Input DataFrame

    Returns:
        df: Pandas DataFrame
    """
    df = df.applymap(lambda x: x.decode() if isinstance(x, bytes) else x)
    return df


def add_day_week_month_year_clim(df):
    """Takes input pandas DataFrame and adds year, month, week, dayofyear columns to end.
    IMPORTANT, time column must be in dataframe and be named [time]

    Args:
        df (Pandas DataFrame): Input Pandas DataFrame containing 'time' column.

    Returns:
        df Pandas DataFrame: Output Pandas DataFrame with appended climatology columns
    """
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    return df


##############   Data Import    ############


def read_csv(path_and_filename, delim=","):
    """Imports csv into pandas DataFrame"""
    df = pd.read_csv(
        path_and_filename, sep=delim, skipinitialspace=True, parse_dates=["time"]
    )
    return df


def fetch_single_datafile(branch, tableName, process_level="REP", file_ext=".csv"):
    """Finds first file in glob with input path to vault structure. Returns path_filename"""
    vault_path = cmn.vault_struct_retrieval(branch)
    print(vault_path)
    flist = glob.glob(
        vault_path + tableName + "/" + process_level.lower() + "/" + "*" + file_ext
    )[0]
    return flist


def importDataMemory(branch, tableName, process_level):
    data_file_name = fetch_single_datafile(branch, tableName, process_level)
    data_df = read_csv(data_file_name)
    data_df = clean_data_df(data_df)
    data_df.rename(columns={"latitude": "lat", "longitude": "lon"}, inplace=True)
    dataset_metadata_df, variable_metadata_df = metadata.import_metadata(
        branch, tableName
    )
    data_dict = {
        "data_df": data_df,
        "dataset_metadata_df": dataset_metadata_df,
        "variable_metadata_df": variable_metadata_df,
    }
    return data_dict


##############   Data Insert    ############


def data_df_to_db(df, tableName, server, clean_data_df_flag=True):
    """Inserts dataframe into SQL tbl"""
    if clean_data_df_flag == True:
        clean_data_df(df)
    temp_file_path = tableName + ".csv"
    df.to_csv(temp_file_path, index=False, header=False)
    DB.toSQLbcp(temp_file_path, tableName, server)
    os.remove(temp_file_path)


##############   Data Transform    ############
def netcdf4_to_vaexdf(netcdf_file, data_var=None):
    """Imports a netcdf file into a vaex dataframe using pandas"""
    xdf = xr.open_dataset(netcdf_file)
    if data_var != None:
        xdf = xdf[data_var]
    df = xdf.to_dataframe().reset_index()
    vdf = vaex.from_pandas(df=df, copy_index=False)
    return vdf


def netcdf4_to_pandas(netcdf_file, data_var=None):
    """Imports a netcdf file into a pandas dataframe"""
    xdf = xr.open_dataset(netcdf_file)
    if data_var != None:
        xdf = xdf[data_var]
    df = xdf.to_dataframe().reset_index()
    return df
