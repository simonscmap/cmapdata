"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - common - commonly used, generalized functions.
"""


import sys
import os
import pandas as pd
import numpy as np
import glob
import dropbox
import DB
import pyarrow.parquet as pq
import pyarrow as pa
import vault_structure as vs
import credentials as cr
import api_checks as api

def getMax_SQL_date(table, server):
    """Returns the max date in a SQL table

    Args:
        table (str): CMAP table name
        server (str): Valid CMAP server name

    Returns:
        sql_date (datetime): Datetime object of max date in table
    """
    query = 'select max(time) as max_time from [Opedia].[dbo].' + table
    server = 'Rainier'
    sql_read = DB.dbRead(query, server)
    sql_date = sql_read["max_time"].iloc[0]
    return sql_date

def getLast_file_download(table, vault_type, raw=True):
    """Returns full filepath of the latest file downloaded
    Args:
        table (str): CMAP table name
        vault_type (str): Vault folder type (ie cruise, station, satellite)
        raw (bool): Get last file from /raw folder in vault. Default is True
    Returns:
        last_download (str): Full filepath of the latest file downloaded
    """
    ## Pass string as attribute to get full vault path
    if raw:
        base_folder = f'{getattr(vs,vault_type)}{table}/raw/'
    else:
        base_folder = f'{getattr(vs,vault_type)}{table}/'
    ## getctime gives create time, getmtime gets modified time
    last_download =  max(glob.glob(base_folder+'*'), key=os.path.getctime)
    last_modified =  max(glob.glob(base_folder+'*'), key=os.path.getmtime)
    max_filename =  max(glob.glob(base_folder+'*'))
    if max_filename != last_download != last_modified:
        print(f"All submissions: {glob.glob(base_folder+'*')}")
        print(f'max create date: {last_download}')
        print(f'max modified date: {last_modified}')
        print(f'max filename: {max_filename}')
        contYN = input(f"Do you want to continue with {max([last_download,last_modified])}? " + " ?  [yes/no]: ")
        if contYN.lower() == "yes":
            last_download = max([last_download,last_modified])
        else:
            contYN = input(f"Do you want to specify ingest from: {glob.glob(base_folder+'*')}? " + " ?  [yes/no]: ")
            if contYN.lower() == "yes":
                excel_index = input("Enter the zero based index of file in list above")
                last_download = glob.glob(base_folder+'*')[excel_index]
            else:
                sys.exit()
        last_download = max([last_download,last_modified])

    return last_download

def check_file_download(file_name, table, vault_type, raw=True):
    """Checks if file exists in the vault
    Args:
        file_name (str): Name of file in vault
        table (str): CMAP table name
        vault_raw (str): Vault folder type (ie cruise, station, satellite)
        raw (boolean): Optional argument to check in raw folder of vault. Default is true
    Returns:
        file_exists (boolean): True if file exists in the vault
    """
    ## Pass string as attribute to get full vault path
    if raw:
        base_folder = f'{getattr(vs,vault_type)}{table}/raw/'
    else:
        base_folder = f'{getattr(vs,vault_type)}{table}/'
    ## getctime gives create time, getmtime gets modified time
    file_exists =  os.path.isfile(base_folder + file_name) 
    return file_exists



def decode_xarray_bytes(xdf):
    """Decode any byte strings in any columns of an xarray dataset

    Args:
        xdf {xarray dataset}: Input xarray dataset

    Returns:
        {xarray dataset}: Output xarray dataset with decoded byte strings
    """
    for col in list(xdf):
        if xdf[col].dtype == "O":
            try:
                xdf[col] = xdf[col].astype(str)
            except:
                xdf[col] = xdf[col].str.decode("cp1252").str.strip()
    return xdf


def read_parquet_schema(fil):
    """Reads schema from a parquet files
    Args:
        fil {str}: Full data path of parquet file
    Returns:
        sch {list}: List of column names and data types
    """    
    parquet_file = pq.ParquetFile(fil)
    sch = pa.schema(
        [f.remove_metadata() for f in parquet_file.schema_arrow])
    return sch

def strip_whitespace_data(df):
    """Strips leading and trailing whitespace from dataframe

    Args:
        df {Pandas DataFrame}: Input Pandas DataFrame

    Returns:
        df {Pandas DataFrame}: Out Pandas DataFrame
    """
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def strip_whitespace_headers(df):
    """Strips any whitespace from dataframe header

    Args:
        df {Pandas DataFrame}: Input Pandas DataFrame

    Returns:
        df {Pandas DataFrame}: Out Pandas DataFrame
    """
    df.columns = df.columns.str.strip()
    return df


def strip_leading_trailing_whitespace_column(df, col_name):
    """Using left and right string strip, remove trailing and leading whitespace from given column

    Args:
        df {Pandas DataFrame}: Input Pandas DataFrame
        col_name {string}: Valid column name

    Returns:
        df {Pandas DataFrame}: Out Pandas DataFrame
    """
    df[col_name] = df[col_name].str.lstrip()
    df[col_name] = df[col_name].str.rstrip()
    return df


def nanToNA(df):
    """Replace numpy nan with ' '

    Args:
        df {Pandas DataFrame}: Input Pandas DataFrame

    Returns:
        df {Pandas DataFrame}: Out Pandas DataFrame
    """
    df = df.replace(np.nan, "", regex=True)
    return df


def lowercase_List(list):
    """Convert every string in a list to a lowercase string

    Args:
        list {list}: Python list

    Returns:
        {list}: Python list with lowercase values
    """
    lower_list = [x.lower() for x in list]
    return lower_list


def getColBounds_from_DB(tableName, col, server, data_server, list_multiplier=0):
    """Gets the min and max bounds of a column from the database

    Args:
        tableName {string} : Valid CMAP table name
        col {string}: Name of column
        server {string}: Valid server name (ie Rainier, Rossby, Mariana)
        data_server {string}: Valid server name where data is present (ie Rainier, Rossby, Mariana)
        list_multiplier {int}: Optional, default = 0. Output is a a list of returned values with length of length list_multiplier integer

    Returns:
        min_col, max_col {list}: Returns two lists of column mins and maxes

    """    
    qry = f"""SELECT MIN({col}),MAX({col}) FROM {tableName}"""
    if len(data_server) > 0:
        df = DB.dbRead(qry, data_server)
    else:
        df = DB.dbRead(qry, server)        
    min_col = [df.iloc[0].astype(str).iloc[0]]
    max_col = [df.iloc[0].astype(str).iloc[1]]
    if list_multiplier != "0":
        min_col = min_col * int(list_multiplier)
        max_col = max_col * int(list_multiplier)
    return min_col, max_col


def getColBounds(df, col, list_multiplier=0):
    """Gets the min and max bounds of a dataframe column

    Parameters
    ----------
    df : Pandas DataFrame
        Input DataFrame
    col : str
        Name of column
    list_multiplier: int, optional, default = 0
        Output is a a list of returned values with length of length list_multiplier integer


    Returns
    -------

    min_col_list, max_col_list
        returns two lists of column mins and maxes

    """
    if col == "time":
        min_col = [
            pd.to_datetime(df["time"])
            .min()
            .strftime("%Y-%m-%d %H:%M:%S")
        ]
        max_col = [
            pd.to_datetime(df["time"])
            .max()
            .strftime("%Y-%m-%d %H:%M:%S")
        ]
    else:
        min_col = [int(pd.to_numeric(df[col]).min())]
        max_col = [int(pd.to_numeric(df[col]).max())]
    if list_multiplier != "0":
        min_col = min_col * int(list_multiplier)
        max_col = max_col * int(list_multiplier)

    return min_col, max_col


def vault_struct_retrieval(branch):
    """Returns vault structure path for input branch"""
    if branch.lower() == "cruise":
        vs_struct = vs.cruise
    elif branch.lower() == "float":
        vs_struct = vs.float_dir
    elif branch.lower() == "drifter":
        vs_struct = vs.drifter
    elif branch.lower() == "mixed":
        vs_struct = vs.mixed        
    elif branch.lower() == "station":
        vs_struct = vs.station        
    elif branch.lower() == "satellite":
        vs_struct = vs.satellite
    elif branch.lower() == "model":
        vs_struct = vs.model
    elif branch.lower() == "assimilation":
        vs_struct = vs.assimilation
    else:
        print(
            "Vault branch structure not found in vault_structure.py. Please modify that script."
        )
    return vs_struct


def get_name_pkey(tableName, server):
    """Returns the name of the primary key column for a given table

    Args:
        tableName (str): CMAP table name
        Server (str): Valid CMAP server name
    """
    query = f"""
    SELECT Col.Column_Name from 
        INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, 
        INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col 
    WHERE 
        Col.Constraint_Name = Tab.Constraint_Name
        AND Col.Table_Name = Tab.Table_Name
        AND Constraint_Type = 'PRIMARY KEY'
        AND Col.Table_Name = '{tableName}'"""
    pkey_name = DB.dbRead(query, server)["Column_Name"].iloc[0]
    return pkey_name


def get_last_ID(tableName, server):
    """last ID in table"""
    pkey_col_name = get_name_pkey(tableName, server)
    last_ID_qry = f"""SELECT TOP 1 [{pkey_col_name}] FROM {tableName} ORDER BY [{pkey_col_name}] DESC"""
    last_ID = DB.dbRead(last_ID_qry, server)[pkey_col_name].iloc[0]
    return last_ID


def get_program_ID(tableName, server, prog):
    pkey_col_name = get_name_pkey(tableName, server)
    q = f"select Program_ID from {tableName} where lower(Program_Name)=lower('{prog}')"
    res = DB.dbRead(q, server)["Program_ID"]
    if len(res) != 1:
        return None   
    return res.iloc[0] 


def getSensorID_SensorName(sensor, db_name, server):
    """Get SensorID from input sensor name"""
    cur_str = (
        """select [ID] FROM """
        + db_name
        + """.[dbo].[tblSensors] WHERE [Sensor] = '"""
        + sensor
        + """'"""
    )
    query_return = DB.dbRead(cur_str, server)
    sID = query_return["ID"].iloc[0]
    return sID   
        
def getDatasetID_DS_Name(datasetName, db_name, server):
    """Get DatasetID from input dataset name"""
    cur_str = (
        """select [ID] FROM """
        + db_name
        + """.[dbo].[tblDatasets] WHERE [Dataset_Name] = '"""
        + datasetName
        + """'"""
    )
    query_return = DB.dbRead(cur_str, server)
    dsID = query_return["ID"].iloc[0]
    return dsID


def getDatasetID_Tbl_Name(tableName, db_name, server):
    """Get DatasetID from input table name"""
    cur_str = (
        """select distinct [Dataset_ID] FROM """
        + db_name
        + """.[dbo].[tblVariables] WHERE [Table_Name] = '"""
        + tableName
        + """'"""
    )
    query_return = DB.dbRead(cur_str, server=server)
    dsID = query_return["Dataset_ID"].iloc[0]
    return dsID

def getTbl_Name_DatasetID(Dataset_ID, db_name, server):
    """Get DatasetID from input table name"""
    cur_str = (
        """select distinct [Table_Name] FROM """
        + db_name
        + """.[dbo].[tblVariables] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    query_return = DB.dbRead(cur_str, server=server)
    tableName = query_return["Table_Name"].iloc[0]
    return tableName    

def getRefID_Tbl_Name_Ref(ref, tableName, db_name, server):
    """Get RefID from input reference and table name"""
    dsID = getDatasetID_Tbl_Name(tableName, db_name, server)
    cur_str = (
        """select distinct [Reference_ID], [Dataset_ID] FROM """
        + db_name
        + """.[dbo].[tblDataset_References] WHERE [Dataset_ID] = """
        + str(dsID)
        + """ AND [Reference] = '"""
        + ref
        + """'"""        
    )
    query_return = DB.dbRead(cur_str, server=server)
    refID = query_return["Reference_ID"].iloc[0]
    return refID, dsID


def getKeywordIDsTableNameVarName(tableName, var_short_name_list, server):
    """Get list of keyword ID's from input tableName"""
    vsnp = tuple(var_short_name_list)
    cur_str = f"""select [ID] from tblVariables where Table_Name = '{tableName}' AND [Short_Name] in {vsnp}"""
    if len(var_short_name_list) == 1:
        cur_str = cur_str.replace(",)", ")")
    query_return = DB.dbRead(cur_str, server=server)["ID"].tolist()
    return query_return


def getKeywordsIDDataset(dataset_ID, server):
    """Get list of keyword ID's from input dataset ID"""
    dataset_ID = str(dataset_ID)
    cur_str = f"""select [ID] from tblVariables where Dataset_ID = '{dataset_ID}'"""
    query_return = DB.dbRead(cur_str, server=server)["ID"].tolist()
    return query_return


def getTableName_Dtypes(tableName, server, data_server):
    """Get data types from input table name"""
    query = (
        """ select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '"""
        + tableName
        + """'"""
    )
    if len(data_server) > 0:
        query_return = DB.dbRead(query, data_server)
    else:
        query_return = DB.dbRead(query, server)
    return query_return

def getTableName_Dtypes_Cluster(tableName, var_list):
    """Get data types from input table name for variables in var_list"""
    qry = f'DESCRIBE TABLE {tableName}'
    df = api.query(qry)
    dlist = []
    for v in var_list:
        dlist.append(df.loc[df['col_name']==v,'data_type'].item())
    return dlist

def getCruiseID_Cruise_Name(cruiseName, server):
    """Get cruise ID from cruise name"""
    query = f"""SELECT ID FROM tblCruise where name = '{cruiseName}'"""
    cruise_ID = DB.dbRead(query, server=server)
    return cruise_ID['ID'][0]
    

def getCruiseDetails(cruiseName, server):
    """Get cruise details from cruise name using uspCruiseByName"""
    query = """EXEC uspCruiseByName '""" + cruiseName + """'"""
    query_return = DB.dbRead(query, server)
    return query_return


def getListCruises(server):
    """Get list of available cruises using uspCruises"""
    query = """SELECT * FROM tblCruise"""
    query_return = DB.dbRead(query, server=server)
    return query_return


def findVarID(datasetID, Short_Name, db_name, server):
    """Get ID value from tblVariables for specific variable"""
    cur_str = (
        """select [ID] FROM """
        + db_name
        + """.[dbo].[tblVariables] WHERE [Dataset_ID] = '"""
        + str(datasetID)
        + """' AND [Short_Name] = '"""
        + Short_Name
        + """'"""
    )
    query = DB.dbRead(cur_str, server)        
    print(query)
    VarID = query["ID"].iloc[0]
    return VarID


def verify_cruise_lists(dataset_metadata_df, server):
    """Take dataset_metadata_df, match cruise names with those in tblCruise, return tuple of matched and unmatched lists.

    Args:
        dataset_metadata_df {Pandas DataFrame}: dataframe constucted from dataset_meta_data sheet of the CMAP data template.
        server {string}: Valid CMAP server name. ex Rainer

    Returns:
        tuple: Two lists, matched and unmatched cruises.
    """
    cruise_series = strip_leading_trailing_whitespace_column(
        dataset_metadata_df, "cruise_names"
    )["cruise_names"]
    """ check that every cruise_name in column exists in the database. map those that don't exist into return"""
    cruise_set = set(lowercase_List(cruise_series.to_list()))
    nickname_list = lowercase_List(getListCruises(server)["Nickname"].to_list())
    name_list = lowercase_List(getListCruises(server)["Name"].to_list())
    db_cruise_set = set(nickname_list + name_list)
    matched = list(cruise_set.intersection(db_cruise_set))
    unmatched = list(cruise_set.difference(db_cruise_set))
    return matched, unmatched


def get_cruise_IDS(cruise_name_list, server):
    """Returns IDs of input cruise names

    Args:
        cruise_name_list {list}: list of cruise names
        server {string}: Valid CMAP server name. ex Rainer

    Returns:
        {list}: list of cruise IDs
    """
    cruise_db_df = getListCruises(server)
    cruise_name_list = lowercase_List(cruise_name_list)
    cruise_ID_list_name = cruise_db_df["ID"][
        cruise_db_df["Name"].str.lower().isin(cruise_name_list)
    ].to_list()
    cruise_ID_list_nickname = cruise_db_df["ID"][
        cruise_db_df["Nickname"].str.lower().isin(cruise_name_list)
    ].to_list()
    combined_cruise_id = list(set(cruise_ID_list_name + cruise_ID_list_nickname))
    return combined_cruise_id


def get_region_IDS(region_name_list, server):
    """Returns IDs of input region names

    Args:
        region_name_list {list}: list of region names
        server {string}: Valid CMAP server name. ex Rainer
    Returns:
        {list}: list of region IDs
    """
    region_db_df = DB.dbRead("""SELECT * FROM tblRegions""", server)
    region_name_list = lowercase_List(region_name_list)
    region_ID_list = region_db_df["Region_ID"][
        region_db_df["Region_Name"].str.lower().isin(region_name_list)
    ].to_list()
    return region_ID_list


def exclude_val_from_col(series, exclude_list):
    """Exclude any characters in a list from Pandas Series

    Args:
        series {Pandas Series}: Input pandas series
        exclude_list {list}: list of characters to exclude

    Returns:
        {Pandas Series}: Pandas series with characters excluded
    """
    mod_series = pd.Series(list(series[~series.isin(exclude_list)]))
    return mod_series


def empty_list_2_empty_str(inlist):
    """If a input list is empty returns an empty string, if not, list is returned"""
    if not inlist:
        inlist = ""
    return inlist


def getLatCount(tableName, server):
    """Get count of number of latitude rows from a give table in the database
    Args:
        tableName {string}: Valid CMAP tablename
        server {string}: Valid CMAP server name. ex Rainer

    Returns:
        {int}: count of # of rows in a table
    """
    query = f"""SELECT SUM(p.rows) FROM sys.partitions AS p
    INNER JOIN sys.tables AS t
    ON p.[object_id] = t.[object_id]
    INNER JOIN sys.schemas AS s
    ON s.[schema_id] = t.[schema_id]
    WHERE t.name = N'{tableName}'
    AND s.name = N'dbo'
    AND p.index_id IN (0,1);"""

    df = DB.dbRead(query, server)
    lat_count = df.columns[0]
    return lat_count


def tableInDB(tableName, server):
    """Return boolean if table exists in database

    Args:
        tableName {string}: Valid CMAP tablename
        server {string}: Valid CMAP server name. ex Rainer
    Returns:
        {bool}: Boolean value if table exists
    """
    qry = f"""SELECT TOP(1) * FROM {tableName}"""
    qry_result = DB.dbRead(qry, server)
    if len(qry_result) > 0:
        tableBool = True
    else:
        tableBool = False
    return tableBool


def datasetINtblDatasets(dataset_name, server):
    """Return boolean if table exists in tblDatasets

    Args:
        dataset_name {string}: Valid CMAP dataset name
        server {string}: Valid CMAP server name. ex Rainer
    Returns:
        {bool}: Boolean value if table exists
    """
    dataset_qry = DB.DB_query(
        f"""SELECT * FROM tblDatasets WHERE Dataset_Name = '{dataset_name}'"""
    )
    if len(dataset_qry) >= 1:
        ds_bool = True
    else:
        ds_bool = False
    return ds_bool


def length_of_tbl(tableName):
    """Return length of CMAP database table.

    Args:
        tableName {string}: CMAP table name

    Returns:
        {int}: Length / Number of rows of CMAP table
    """
    qry = f"""  select sum (spart.rows)
    from sys.partitions spart
    where spart.object_id = object_id('{tableName}')
    and spart.index_id < 2"""
    tableCount = list(DB.DB_query(qry))[0]
    return tableCount


def get_var_list_dataset(tableName, server):
    """Returns list of column names for a given dataset

    Args:
        tableName {string}: CMAP table name

    Returns:
        {list}: List of column names
    """
    col_name_list = DB.dbRead(f"""EXEC uspColumns '{tableName}'""", server)[
        "Columns"
    ].to_list()
    return col_name_list


def double_chars_in_col(df, col, list_of_chars):
    """Replaces a single character with two of the same character. To be used in SQL Server escaping.

    Args:
        df {Pandas DataFrame}: Input Pandas DataFrame
        col {str}: column of DataFrame used in replace
        list_of_chars {list}: A list of strings to be doubled

    Returns:
        df (Pandas DataFrame): Modified Pandas DataFrame
    """
    for char in list_of_chars:
        df[col] = df[col].str.replace(char, char * 2)
    return df


def get_numeric_cols_in_table_excluding_climatology(tableName, server):
    """Return list of columns in a table that are numeric (float & int) and are not any climatology caluclated fields, eg. dayofyear

    Args:
        tableName (string): CMAP table name
        Server (string): Valid CMAP server name

    Returns:
        col_list (list): List of column names
    """
    qry = f"""select COLUMN_NAME from Information_schema.columns where Table_name = '{tableName}' and( DATA_TYPE = 'float' or DATA_TYPE like '%int') and COLUMN_NAME NOT IN ('year','month','week','dayofyear')"""
    df = DB.dbRead(qry, server)
    col_list = df["COLUMN_NAME"].to_list()
    return col_list

def getStats_TblName(tableName, server):
    """Return existing stats from tblDataset_Stats for a table

    Args:
        tableName (string): CMAP table name
        Server (string): Valid CMAP server name

    Returns:
        min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth
    """        
    ds_id = getDatasetID_Tbl_Name(tableName, 'Opedia', server)
    qry = f"""  SELECT 
    JSON_VALUE(JSON_stats,'$.time.min') AS [Time_Min],
    JSON_VALUE(JSON_stats,'$.time.max') AS [Time_Max],
    CAST(JSON_VALUE(JSON_stats,'$.lat.min') AS float) AS [Lat_Min],
    CAST(JSON_VALUE(JSON_stats,'$.lat.max') AS float) AS [Lat_Max],
    CAST(JSON_VALUE(JSON_stats,'$.lon.min') AS float) AS [Lon_Min],
    CAST(JSON_VALUE(JSON_stats,'$.lon.max') AS float) AS [Lon_Max],
    CAST(JSON_VALUE(JSON_stats,'$.depth.min') AS float) AS [Depth_Min],
    CAST(JSON_VALUE(JSON_stats,'$.depth.max') AS float) AS [Depth_Max]
    from tblDataset_Stats where Dataset_ID = {ds_id} """
    df_stat = DB.dbRead(qry, server)
    min_time = df_stat['Time_Min'][0]
    max_time = df_stat['Time_Max'][0]
    min_lat = df_stat['Lat_Min'][0]
    max_lat = df_stat['Lat_Max'][0]
    min_lon =  df_stat['Lon_Min'][0]
    max_lon = df_stat['Lon_Max'][0]    
    min_depth = df_stat['Depth_Min'][0]   
    max_depth = df_stat['Depth_Max'][0]   
    return min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth


def built_meta_DataFrame():
    """Returns two empty dataframes for metadata excel template """
    dataset_meta = pd.DataFrame(
        columns=[
            "dataset_short_name",
            "dataset_long_name",
            "dataset_version",
            "dataset_release_date",
            "dataset_make",
            "dataset_source",
            "dataset_distributor",
            "dataset_acknowledgement",
            "dataset_history",
            "dataset_description",
            "dataset_references",
            "climatology",
            "cruise_names",
        ]
    )
    vars_meta = pd.DataFrame(
        columns=[
            "var_short_name",
            "var_long_name",
            "var_sensor",
            "var_unit",
            "var_spatial_res",
            "var_temporal_res",
            "var_discipline",
            "visualize",
            "var_keywords",
            "var_comment",
        ]
    )
    return dataset_meta, vars_meta


def combine_df_to_excel(filename, df, dataset_metadata, vars_metadata, cruise=False):
    """ Combines data, dataset metadata and var metadata dfs and saves as excel """
    writer = pd.ExcelWriter(filename, engine="xlsxwriter")
    if cruise:
        dataset_metadata.to_excel(writer, sheet_name="cruise_trajectory", index=False)
        vars_metadata.to_excel(writer, sheet_name="cruise_metadata", index=False)
    else:
        df.to_excel(writer, sheet_name="data", index=False)
        dataset_metadata.to_excel(writer, sheet_name="dataset_meta_data", index=False)
        vars_metadata.to_excel(writer, sheet_name="vars_meta_data", index=False)
    writer.save()

def dropbox_public_link(folder_path):
    """Creates a public link to Dropbox vault """
    dbx = dropbox.Dropbox(
            app_key = cr.dropbox_vault_key,
            app_secret = cr.dropbox_vault_secret,
            oauth2_refresh_token = cr.dropbox_vault_refresh_token
        )
    existing_link = dbx.sharing_list_shared_links(folder_path)
    try:
        shared_url = existing_link.links[0].url
    except:        
        print('Create new dropbox link')
        dbx_link = dbx.sharing_create_shared_link_with_settings(folder_path)
        shared_url = dbx_link.url
    return shared_url


