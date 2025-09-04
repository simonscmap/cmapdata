"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - metadata - cmap metadata formatting for table insertion.
"""


import sys
import os
import glob
import geopandas
from geopandas.tools import sjoin
from matplotlib.pyplot import table
import pandas as pd
import numpy as np
# import xarray as xr
import datetime
from pytz import timezone


import credentials as cr
import common as cmn
import cruise
import data
import DB
import vault_structure as vs
import api_checks as api
import stats


def ID_Var_Map(series_to_map, res_col, tableName, server):
    query = f"""SELECT * FROM {tableName}"""
    call = DB.dbRead(query, server)
    series = series_to_map.astype(str).str.lower()
    sdict = dict(zip(call[res_col].str.lower(), call.ID))
    mapped_series = series.map(sdict)
    mapped_series = list(cmn.nanToNA(mapped_series).replace("", 0))
    return mapped_series


def import_metadata(branch, tableName):
    branch_path = cmn.vault_struct_retrieval(branch)   
    ds_meta_list = max(glob.glob(branch_path + tableName + "/metadata/" + "*dataset_metadata*"), key=os.path.getctime)
    vars_meta_list = max(glob.glob(branch_path + tableName + "/metadata/" + "*vars_metadata*"), key=os.path.getctime)
    # vars_meta_list = glob.glob(
    #     branch_path + tableName + "/metadata/" + "*vars_metadata*"
    # )
    dataset_metadata_df = pd.read_parquet(ds_meta_list)
    vars_metadata_df = pd.read_parquet(vars_meta_list)

    dataset_metadata_df = cmn.nanToNA(cmn.strip_whitespace_headers(dataset_metadata_df))
    vars_metadata_df = cmn.nanToNA(cmn.strip_whitespace_headers(vars_metadata_df))
    ## Parquet nans are string
    dataset_metadata_df.replace({'nan':''},inplace=True)
    vars_metadata_df.replace({'nan':''},inplace=True)
    return dataset_metadata_df, vars_metadata_df


def tblDatasets_Insert(dataset_metadata_df, tableName, icon_filename, server, db_name):
    last_dataset_ID = cmn.get_last_ID("tblDatasets", server) + 1
    dataset_metadata_df = cmn.nanToNA(dataset_metadata_df)
    # dataset_metadata_df.replace({"'": "''"}, regex=True, inplace=True)
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    Dataset_Long_Name = dataset_metadata_df["dataset_long_name"].iloc[0]
    Dataset_Version = str(dataset_metadata_df["dataset_version"].iloc[0])
    Dataset_Release_Date = dataset_metadata_df["dataset_release_date"].iloc[0]
    if type(Dataset_Release_Date) != str:
        Dataset_Release_Date = Dataset_Release_Date.date().strftime('%Y-%m-%d')
    Dataset_Make = dataset_metadata_df["dataset_make"].iloc[0]
    Data_Source = (
        dataset_metadata_df["dataset_source"].iloc[0]
        .replace("\ufeff", "")
    )
    Distributor = dataset_metadata_df["dataset_distributor"].iloc[0]
    Acknowledgement = (
        dataset_metadata_df["dataset_acknowledgement"].iloc[0].replace("\xa0", " ")
    )
    Contact_Email = ""  # dataset_metadata_df["contact_email"].iloc[0]
    Dataset_History = dataset_metadata_df["dataset_history"].iloc[0]
    Description = (
        dataset_metadata_df["dataset_description"]
        .iloc[0]
        .replace("'", "CHAR(39)")
        .replace("’", "")
        .replace("‘", "")
        # .replace("\n", "")
        .replace("\xa0", " ")
        .replace("\ufeff", "")
    )
    Climatology = dataset_metadata_df["climatology"].iloc[0]
    if Climatology != '1':
        Climatology = '0'
    Db = db_name
    # Temps
    Variables = ""
    Doc_URL = ""
    if len(icon_filename) > 3:
        Icon_URL = f"""https://raw.githubusercontent.com/simonscmap/static/master/mission_icons/{icon_filename}"""    
    else:
        Icon_URL = f"""https://raw.githubusercontent.com/simonscmap/static/master/mission_icons/{tableName}.png"""

    query = (
        last_dataset_ID,
        Db,
        Dataset_Name,
        Dataset_Long_Name,
        Variables,
        Data_Source,
        Distributor,
        f"N'{Description}'",
        Climatology,
        Acknowledgement,
        Doc_URL,
        Icon_URL,
        Contact_Email,
        Dataset_Version,
        Dataset_Release_Date,
        Dataset_History,
    )
    columnList = "(ID,DB,Dataset_Name,Dataset_Long_Name,Variables,Data_Source,Distributor,Description,Climatology,Acknowledgement,Doc_URL,Icon_URL,Contact_Email,Dataset_Version,Dataset_Release_Date,Dataset_History)"
    try:
        DB.lineInsert(
            server, db_name +".[dbo].[tblDatasets]", columnList, query, ID_insert=True
        )
    except:
        DB.lineInsert(
            server, db_name +".[dbo].[tblDatasets]", columnList, query, ID_insert=False
        )
    ## Fix line breaks in description
    for row in dataset_metadata_df.itertuples():
        if getattr(row, 'Index') ==0:
            desc = row.dataset_description
            if "CHAR(39)" in desc:
                desc = desc.replace("CHAR(39)", "''")
            qry = f"update tbldatasets set description = '{desc}' where id = {last_dataset_ID}"   
            DB.DB_modify(qry, server)   
    print("Metadata inserted into tblDatasets.")

def tblProcess_Queue_Download_Insert(Original_Name, Table_Name, db_name, server, error_flag=''):
    dl_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    if len(error_flag) ==0:
        columnList = "(Original_Name, Table_Name, Downloaded)"
        query = (Original_Name, Table_Name, dl_str)
    else:
        columnList = "(Original_Name, Table_Name, Downloaded, Error_Str)"
        query = (Original_Name, Table_Name, dl_str, error_flag)        
    DB.lineInsert(
                server, db_name +".[dbo].[tblProcess_Queue]", columnList, query
            )
def tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name, Table_Name, db_name, server):
    pr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")   
    qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Downloaded = '{pr_str}', Original_Name = '{Original_Name}', Error_Str = NULL WHERE Table_Name = '{Table_Name}' and Original_Name = '{Error_Date}' "     
    DB.DB_modify(qry,server)

def tblProcess_Queue_Process_Update(Original_Name, Path, Table_Name, db_name, server, error_flag=''):
    pr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    if len(error_flag) ==0:    
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Processed = '{pr_str}', Path='{Path}' WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "
    else:
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Processed = '{pr_str}', Error_Str='{error_flag}' WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "        
    DB.DB_modify(qry,server)

def tblProcess_Queue_Overwrite(Original_Name, Table_Name, db_name, server, error_flag=''):
    pr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    if len(error_flag) ==0:    
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Downloaded = '{pr_str}', Processed=NULL WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "
    else:
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Downloaded = '{pr_str}', Processed=NULL, Error_Str='{error_flag}' WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "        
    
    print(f"tblProcess_Queue_Overwrite: {qry}")
    DB.DB_modify(qry,server)


def tblIngestion_Queue_Staged_Update(Path, Table_Name, db_name, server):
    sr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    columnList = "(Path, Table_Name, Staged)"
    query = (Path, Table_Name,sr_str)
    DB.lineInsert(
                server, db_name +".[dbo].[tblIngestion_Queue]", columnList, query
            )

def tblIngestion_Queue_Overwrite(Path, Table_Name, db_name, server):
    sr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    qry = f"UPDATE {db_name}.[dbo].[tblIngestion_Queue] SET Staged = '{sr_str}', Started=NULL,Ingested=NULL WHERE Table_Name = '{Table_Name}' and Path = '{Path}' "
    DB.DB_modify(qry,server)



def tblDataset_Single_Reference_Insert(ref, table_name, server, db_name, data_doi=0):
    DatasetID = cmn.getDatasetID_Tbl_Name(table_name, db_name, server)
    Ref_ID = cmn.get_last_ID("tblDataset_References", server) + 1
    columnList = "(Reference_ID, Dataset_ID, Reference, Data_DOI)"
    query = (Ref_ID,DatasetID, ref,data_doi)
    DB.lineInsert(
                server, db_name +".[dbo].[tblDataset_References]", columnList, query
            )

def tblDataset_Vault_Insert(tableName, server, db_name, make):
    Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    # full_vault_path = getattr(vs,make)+tableName
    full_vault_path = cmn.vault_struct_retrieval(make)+tableName
    vault_path = full_vault_path.split('vault/')[1]+'/'
    # vault_url = cmn.dropbox_public_link(full_vault_path.split('Simons CMAP')[1])
    vault_url = cmn.dropbox_public_link(full_vault_path.split('cmap_dbx')[1])
    columnList = "(Dataset_ID, Vault_Path, Vault_URL)"
    qry = (Dataset_ID,vault_path,vault_url)
    DB.lineInsert(
                server, db_name +".[dbo].[tblDataset_Vault]", columnList, qry
            )
    print(f"Metadata inserted into tblDataset_Vault for {tableName} ")

def tblDataset_DOI_Download_Insert(Ref_ID, server, db_name, DOI_download_link, DOI_download_file, DOI_CMAP_template):
    columnList = "(Reference_ID, DOI_Download_Link, Entity_Name, CMAP_format)"
    query = (Ref_ID, DOI_download_link, DOI_download_file, DOI_CMAP_template)   
    DB.lineInsert(
            server, db_name +".[dbo].[tblDataset_DOI_Download]", columnList, query
        ) 
    print("Inserting data into tblDataset_DOI_Download.")    

def tblDataset_References_Insert(dataset_metadata_df, server, db_name, DOI_link_append=None, DOI_download_link=None, DOI_download_file=None, DOI_CMAP_template=None):
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    IDvar = cmn.getDatasetID_DS_Name(Dataset_Name, db_name, server)
    columnList = "(Reference_ID, Dataset_ID, Reference, Data_DOI)"
    reference_list = (
        dataset_metadata_df["dataset_references"]
        .str.replace("\xa0", " ")
        .str.replace("\ufeff", "")
        .replace("", np.nan)
        .dropna()
        .tolist()
    )

    for ref in reference_list:
        if ref != " ":
            Ref_ID = cmn.get_last_ID("tblDataset_References", server) + 1
            query = (Ref_ID, IDvar, ref, 0)
            
            DB.lineInsert(
                server, db_name +".[dbo].[tblDataset_References]", columnList, query
            )
    if DOI_link_append != None:
        Ref_ID = cmn.get_last_ID("tblDataset_References", server) + 1
        query = (Ref_ID, IDvar, DOI_link_append, 1)   
        DB.lineInsert(
                server, db_name +".[dbo].[tblDataset_References]", columnList, query
            )  
        tblDataset_DOI_Download_Insert(Ref_ID, server, db_name, DOI_download_link, DOI_download_file, DOI_CMAP_template)     
    print("Inserting data into tblDataset_References.")


def tblDataset_Programs_Insert(dataset_metadata_df, server, db_name, programs):
    if not programs: return
    Dataset_Name = dataset_metadata_df["dataset_short_name"].iloc[0]
    Dataset_ID = cmn.getDatasetID_DS_Name(Dataset_Name, db_name, server)
    columnList = "(Dataset_ID, Program_ID)"
    program_list = programs.split(",")
    for prog in program_list:
        try:
            if len(prog) == 0:
                continue
            Program_ID = cmn.get_program_ID("tblPrograms", server, prog)
            if Program_ID:
                query = (Dataset_ID, Program_ID)                
                DB.lineInsert(
                    server, db_name +".[dbo].[tblDataset_Programs]", columnList, query
                )
            else:
                print("################ Program ID Issue ################")
                print(f"No program ID found for {prog}")    
        except Exception as e:
            print(e) 


def tblVariables_Insert(
    data_df,
    dataset_metadata_df,
    variable_metadata_df,
    Table_Name,
    server,
    db_name,
    process_level,
    data_server,
    has_depth,
    CRS="CRS",
):
    if len(data_server) == 0:
        data_server = server
    Db_list = len(variable_metadata_df) * [db_name]
    IDvar_list = len(variable_metadata_df) * [
        cmn.getDatasetID_DS_Name(
            dataset_metadata_df["dataset_short_name"].iloc[0], db_name, server
        )
    ]
    Table_Name_list = len(variable_metadata_df) * [Table_Name]
    Short_Name_list = variable_metadata_df["var_short_name"].tolist()
    Long_Name_list = variable_metadata_df["var_long_name"].tolist()
    Unit_list = variable_metadata_df["var_unit"].tolist()
    Temporal_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_temporal_res"],
        "Temporal_Resolution",
        "tblTemporal_Resolutions",
        server,
    )
    Spatial_Res_ID_list = ID_Var_Map(
        variable_metadata_df["var_spatial_res"],
        "Spatial_Resolution",
        "tblSpatial_Resolutions",
        server,
    )

    if type(data_df) != bool:
        if data_df.empty:
            print('Empty data tab')
        Temporal_Coverage_Begin_list, Temporal_Coverage_End_list = cmn.getColBounds(
            data_df, "time", list_multiplier=len(variable_metadata_df)
        )
        Lat_Coverage_Begin_list, Lat_Coverage_End_list = cmn.getColBounds(
            data_df, "lat", list_multiplier=len(variable_metadata_df)
        )
        Lon_Coverage_Begin_list, Lon_Coverage_End_list = cmn.getColBounds(
            data_df, "lon", list_multiplier=len(variable_metadata_df)
        )
        if 'depth' in data_df.columns.tolist():
            has_depth = 1
            has_depth_list = [has_depth] * len(variable_metadata_df)
        else:
            has_depth = 0
            has_depth_list = [has_depth] * len(variable_metadata_df)
    
    elif data_server.lower() == 'cluster':
        try:
            # min_date, max_date, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth = api.statsCluster(Table_Name, has_depth)
            min_date, max_date, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth, row_count =stats.pull_from_stats_folder(Table_Name,vs.float_dir)
            min_date = min_date.strftime("%Y-%m-%dT%H:%M:%S")
            max_date = max_date.strftime("%Y-%m-%dT%H:%M:%S")
        except:
            Yn = input("Read min/max lat lon from parquet? y or n \n")
            if Yn:
                fil = input("Input *full* parquet path after vault (ex satellite/tblModis/rep/tblModis_2020.parquet) \n")
                df_fil = pd.read_parquet(vs.vault+fil)
                min_lat = df_fil.lat.min()
                max_lat = df_fil.lat.max()
                min_lon = df_fil.lon.min()
                max_lon = df_fil.lon.max()
            else:
                min_lat = input("Enter min latitude (ex -57.5)\n")      
                max_lat = input("Enter max latitude (ex -57.5)\n")    
                min_lon = input("Enter min longitude (ex -57.5)\n")      
                max_lon = input("Enter max longitude (ex -57.5)\n")  
            min_date = input("Enter min date (ex 2011-09-13 00:00:00.000)\n")        
            max_date = input("Enter max date (ex 2021-09-13 00:00:00.000)\n")      
        Temporal_Coverage_Begin_list = [min_date] * int(len(variable_metadata_df))
        Temporal_Coverage_End_list = [max_date] * int(len(variable_metadata_df))
        Lat_Coverage_Begin_list = [min_lat] * int(len(variable_metadata_df))
        Lat_Coverage_End_list = [max_lat] * int(len(variable_metadata_df))
        Lon_Coverage_Begin_list = [min_lon] * int(len(variable_metadata_df))
        Lon_Coverage_End_list = [max_lon] * int(len(variable_metadata_df))
        has_depth_list = [has_depth] * len(variable_metadata_df)
    
    else:
        if "_Climatology" in Table_Name:
            (
                Temporal_Coverage_Begin_list,
                Temporal_Coverage_End_list,
            ) = cmn.getColBounds_from_DB(
                Table_Name, "month", server, data_server, list_multiplier=len(variable_metadata_df)
            )
        else:
            (
            Temporal_Coverage_Begin_list,
            Temporal_Coverage_End_list,
            ) = cmn.getColBounds_from_DB(
            Table_Name, "time", server, data_server, list_multiplier=len(variable_metadata_df)
            )
        Lat_Coverage_Begin_list, Lat_Coverage_End_list = cmn.getColBounds_from_DB(
            Table_Name, "lat", server, data_server, list_multiplier=len(variable_metadata_df)
        )
        Lon_Coverage_Begin_list, Lon_Coverage_End_list = cmn.getColBounds_from_DB(
            Table_Name, "lon", server, data_server, list_multiplier=len(variable_metadata_df)
        )
        has_depth_list = [has_depth] * len(variable_metadata_df)

    Grid_Mapping_list = [CRS] * len(variable_metadata_df)
    Sensor_ID_list = ID_Var_Map(
        variable_metadata_df["var_sensor"], "Sensor", "tblSensors", server
    )
    Make_ID_list = len(variable_metadata_df) * list(
        ID_Var_Map(
            dataset_metadata_df["dataset_make"].head(1), "Make", "tblMakes", server
        )
    )
    Process_ID_list = ID_Var_Map(
        pd.Series(len(variable_metadata_df) * [process_level]),
        "Process_Stage",
        "tblProcess_Stages",
        server,
    )
    Study_Domain_ID_list = ID_Var_Map(
        variable_metadata_df["var_discipline"],
        "Study_Domain",
        "tblStudy_Domains",
        server,
    )
    Comment_list = cmn.nanToNA(variable_metadata_df["var_comment"]).tolist()
    Visualize_list = cmn.nanToNA(variable_metadata_df["visualize"]).tolist()
    if data_server.lower() == 'cluster':      
        Data_Type_list = cmn.getTableName_Dtypes_Cluster(Table_Name, Short_Name_list)
    else: 
        ## cmn function returns time, lat, lon, etc as well       
        Data_Type_qry = cmn.getTableName_Dtypes(Table_Name, server, data_server).set_index('COLUMN_NAME')
        ## with index on COLUMN_NAME, df can be sorted and filtered by short_name_list 
        ## this covers case when vars in meta sheet don't match the order of the data sheet
        Data_Type_qry_1 = Data_Type_qry.loc[Short_Name_list]
        Data_Type_list = Data_Type_qry_1["DATA_TYPE"].tolist()
    if 'org_id' in variable_metadata_df.columns.tolist():
        Org_ID_list = variable_metadata_df["org_id"].replace('', 'NULL', regex=True).tolist()
        ## Replace n'' with NULL due to FK on org table
        # Org_ID_list = [x if ' ' not in str(x) else 'NULL' for x in Org_list]
    else:
        Org_ID_list = ['NULL'] * len(variable_metadata_df)
        
    if 'conversion_coefficient' in variable_metadata_df.columns.tolist():
        # Conversion_Coefficient = cmn.nanToNA(variable_metadata_df["conversion_coefficient"]).tolist()
        Conversion_Coefficient_list = variable_metadata_df["conversion_coefficient"].replace('', 'NULL', regex=True).tolist()
        # Conversion_Coefficient_list = [x if '' not in str(x) else 'NULL' for x in Conversion_Coefficient]
    else:
        Conversion_Coefficient_list = ['NULL'] * len(variable_metadata_df)
    columnList = "(ID,DB, Dataset_ID, Table_Name, Short_Name, Long_Name, Unit, Temporal_Res_ID, Spatial_Res_ID, Temporal_Coverage_Begin, Temporal_Coverage_End, Lat_Coverage_Begin, Lat_Coverage_End, Lon_Coverage_Begin, Lon_Coverage_End, Grid_Mapping, Make_ID, Sensor_ID, Process_ID, Study_Domain_ID, Comment, Visualize, Data_Type, Org_ID, Conversion_Coefficient, Has_Depth)"
    for (
        Db,
        IDvar,
        Table_Name,
        Short_Name,
        Long_Name,
        Unit,
        Temporal_Res_ID,
        Spatial_Res_ID,
        Temporal_Coverage_Begin,
        Temporal_Coverage_End,
        Lat_Coverage_Begin,
        Lat_Coverage_End,
        Lon_Coverage_Begin,
        Lon_Coverage_End,
        Grid_Mapping,
        Make_ID,
        Sensor_ID,
        Process_ID,
        Study_Domain_ID,
        Comment,
        Visualize,
        Data_Type,
        Org_ID, 
        Conversion_Coefficient,
        has_depth
    ) in zip(
        Db_list,
        IDvar_list,
        Table_Name_list,
        Short_Name_list,
        Long_Name_list,
        Unit_list,
        Temporal_Res_ID_list,
        Spatial_Res_ID_list,
        Temporal_Coverage_Begin_list,
        Temporal_Coverage_End_list,
        Lat_Coverage_Begin_list,
        Lat_Coverage_End_list,
        Lon_Coverage_Begin_list,
        Lon_Coverage_End_list,
        Grid_Mapping_list,
        Make_ID_list,
        Sensor_ID_list,
        Process_ID_list,
        Study_Domain_ID_list,
        Comment_list,
        Visualize_list,
        Data_Type_list,
        Org_ID_list, 
        Conversion_Coefficient_list,
        has_depth_list
    ):
        last_var_ID = cmn.get_last_ID("tblVariables", server) + 1

        query = (
            last_var_ID,
            Db,
            IDvar,
            Table_Name,
            Short_Name,
            Long_Name,
            Unit,
            Temporal_Res_ID,
            Spatial_Res_ID,
            Temporal_Coverage_Begin,
            Temporal_Coverage_End,
            Lat_Coverage_Begin,
            Lat_Coverage_End,
            Lon_Coverage_Begin,
            Lon_Coverage_End,
            Grid_Mapping,
            Make_ID,
            Sensor_ID,
            Process_ID,
            Study_Domain_ID,
            Comment,
            Visualize,
            Data_Type,
            Org_ID, 
            Conversion_Coefficient,
            has_depth
        )
        
        try:
            DB.lineInsert(
                server,
                db_name +".[dbo].[tblVariables]",
                columnList,
                query,
                ID_insert=True,
            )
        except:
            DB.lineInsert(
                server,
                db_name +".[dbo].[tblVariables]",
                columnList,
                query,
                ID_insert=False,
            )
    print("Inserting data into tblVariables")


def tblKeywords_Insert(variable_metadata_df, dataset_metadata_df, Table_Name, db_name, server):
    IDvar = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0], db_name, server
    )
    for index, row in variable_metadata_df.iterrows():
        VarID = cmn.findVarID(
            IDvar, variable_metadata_df.loc[index, "var_short_name"], db_name, server
        )
        keyword_list = (variable_metadata_df.loc[index, "var_keywords"]).split(",")
        for keyword in keyword_list:
            keyword = keyword.lstrip().replace('\ufeff','')
            query = (VarID, keyword)
            print(query)
            if len(keyword) > 0:  # won't insert empty values
                try:  # Cannot insert duplicate entries, so skips if duplicate
                    DB.lineInsert(
                        server,
                        db_name +".[dbo].[tblKeywords]",
                        "(var_ID, keywords)",
                        query,
                    )
                except Exception as e:
                    print(e)

def tblKeywords_Insert_VarID(var_id, keyword, db_name, server):
    keyword = keyword.lstrip().replace('\ufeff','')
    query = (var_id, keyword)
    print(query)
    if len(keyword) > 0:  # won't insert empty values
        try:  # Cannot insert duplicate entries, so skips if duplicate
            DB.lineInsert(
                server,
                db_name +".[dbo].[tblKeywords]",
                "(var_ID, keywords)",
                query,
            )
        except Exception as e:
            print(e)                    

def tblDataset_Server_Insert(tableName, db_name, server):
    columnList = "(Dataset_ID, Server_Alias)"
    for svr in cr.server_alias_list:
        try:
            Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, svr)
            query = (Dataset_ID, server.lower())
            DB.lineInsert(
                    svr, db_name +".[dbo].[tblDataset_Servers]", columnList, query
                )            
        except:
            print(f'{tableName} not on {svr}')
            continue      

def addServerAlias(tbl,server_list=cr.server_alias_list):
    """Adds alias in server_list to all servers
    Args:
        tbl (str): CMAP table name
        server_list (list): Default is all servers
    """
    for server in server_list:
        tblDataset_Server_Insert(tbl,'Opedia',server)

def user_input_build_cruise(df, dataset_metadata_df, server):
    tblCruise_df, cruise_name = cruise.build_cruise_metadata_from_user_input(df)
    print("The cruise metadata dataframe you generated looks like: ")
    print(tblCruise_df)
    meta_cont = input(
        "Do you want to ingest this [y], cancel the process [n] or go through the metadata build again [r]? "
    )
    if meta_cont.lower() == "r":
        tblCruise_df, cruise_name = cruise.build_cruise_metadata_from_user_input(df)
    elif meta_cont.lower() == "y":
        print(tuple(tblCruise_df.iloc[0].astype(str).to_list()))

        DB.lineInsert(
            server,
            "tblCruise",
            "(Nickname,Name,Ship_Name,Start_Time,End_Time,Lat_Min,Lat_Max,Lon_Min,Lon_Max,Chief_Name)",
            tuple(tblCruise_df.iloc[0].astype(str).to_list()),
        )
    elif meta_cont.lower() == "n":
        sys.exit()

    Cruise_ID = cmn.get_cruise_IDS([cruise_name], server)
    traj_df = cruise.return_cruise_trajectory_from_df(df, Cruise_ID)
    data.data_df_to_db(
        traj_df, "tblCruise_Trajectory", server, clean_data_df_flag=False
    )

def tblDataset_Cruises_Line_Insert(dataset_ID, cruise_ID, db_name, server):
    query = (dataset_ID, cruise_ID)
    DB.lineInsert(
        server,
        db_name + ".[dbo].[tblDataset_Cruises]",
        "(Dataset_ID, Cruise_ID)",
        query,
    )

def tblDataset_Cruises_Insert(data_df, dataset_metadata_df, db_name, server):

    matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(
        dataset_metadata_df, server
    )
    print("matched: ")
    print(matched_cruises)
    print("\n")
    print("umatched: ")
    print(unmatched_cruises)
    if matched_cruises == []:
        build_traj_yn = input(
            "Do you want to build cruise trajectory and metadata from this dataset? [Y/n]: "
        )
        if build_traj_yn.lower() == "y":
            print(
                "Building cruise trajectory and metadata from this dataset and user input. "
            )
            user_input_build_cruise(data_df, dataset_metadata_df, server)
            matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(
                dataset_metadata_df, server
            )
    cruise_ID_list = cmn.get_cruise_IDS(matched_cruises, server)
    dataset_ID = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0], db_name, server
    )
    for cruise_ID in cruise_ID_list:
        tblDataset_Cruises_Line_Insert(dataset_ID, cruise_ID, db_name, server)
    print("Dataset matched to cruises")


def tblMetadata_Cruises_Insert(dataset_metadata_df, db_name, server):

    matched_cruises, unmatched_cruises = cmn.verify_cruise_lists(
        dataset_metadata_df, server
    )
    print("matched: ")
    print(matched_cruises)
    print("\n")
    print("umatched: ")
    print(unmatched_cruises)

    cruise_ID_list = cmn.get_cruise_IDS(matched_cruises, server)
    dataset_ID = cmn.getDatasetID_DS_Name(
        dataset_metadata_df["dataset_short_name"].iloc[0], db_name, server
    )
    for cruise_ID in cruise_ID_list:
        query = (dataset_ID, cruise_ID)
        DB.lineInsert(
            server,
            db_name + ".[dbo].[tblDataset_Cruises]",
            "(Dataset_ID, Cruise_ID)",
            query,
        )
    print("Dataset matched to cruises")

def deleteFromtblKeywords(Dataset_ID, db_name, server):
    Keyword_ID_list = cmn.getKeywordsIDDataset(Dataset_ID, server)
    Keyword_ID_str = "','".join(str(key) for key in Keyword_ID_list)
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblKeywords] WHERE [var_ID] IN ('"""
        + Keyword_ID_str
        + """')"""
    )
    DB.DB_modify(cur_str, server)
    print("tblKeyword entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblKeywords_VarID(var_id, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblKeywords] WHERE [var_ID] = """
        + str(var_id)
    )
    DB.DB_modify(cur_str, server)
    print("tblKeyword entries deleted for Variable_ID: ", var_id)

def deleteFromtblDataset_Stats(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name 
        + """.[dbo].[tblDataset_Stats] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Stats entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Cruises(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_Cruises] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Cruises entries deleted for Dataset_ID: ", Dataset_ID)


def deleteFromtblDataset_Regions(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_Regions] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Regions entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDataset_DOI_Download(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE d FROM """
        + db_name
        + """.[dbo].[tblDataset_References] r INNER JOIN """
        + db_name 
        + """ .[dbo].[tblDataset_DOI_Download] d ON r.Reference_ID = d.Reference_ID WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_DOI_Download entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDataset_References(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_References] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_References entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDataset_Vault(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_Vault] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDataset_Vault entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDatasets_JSON_Metadata(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDatasets_JSON_Metadata] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblDatasets_JSON_Metadata entries deleted for Dataset_ID: ", Dataset_ID)    

def deleteFromtblVariables_JSON_Metadata(Dataset_ID, db_name, server):
    qry = f"SELECT ID FROM tblVariables WHERE [Dataset_ID] = {Dataset_ID}"
    df_var = DB.dbRead(qry, server)
    var_list = df_var['ID'].to_list()
    var_ids = "','".join(str(key) for key in var_list)
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblVariables_JSON_Metadata] WHERE [Var_ID] IN ('"""
        + str(var_ids)
        + """')"""
    )
    DB.DB_modify(cur_str, server)
    print("tblVariables_JSON_Metadata entries deleted for Dataset_ID: ", Dataset_ID)        

def deleteFromtblVariables(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblVariables] WHERE [Dataset_ID] = """
        + str(Dataset_ID)
    )
    DB.DB_modify(cur_str, server)
    print("tblVariables entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblVariables_VarID(var_id, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblVariables] WHERE [ID] = """
        + str(var_id)
    )
    DB.DB_modify(cur_str, server)
    print("tblVariables entries deleted for Variable_ID: ", var_id)    


def deleteFromtblDatasets(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDatasets] WHERE [ID] = """ + str(
        Dataset_ID
    ))
    DB.DB_modify(cur_str, server)
    print("tblDataset entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDataset_Servers(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_Servers] WHERE [Dataset_ID] = """ + str(
        Dataset_ID
    ))
    DB.DB_modify(cur_str, server)
    print("tblDataset_Servers entries deleted for Dataset_ID: ", Dataset_ID)

def deleteFromtblDataset_Programs(Dataset_ID, db_name, server):
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblDataset_Programs] WHERE [Dataset_ID] = """ + str(
        Dataset_ID
    ))
    DB.DB_modify(cur_str, server)
    print("tblDataset_Programs entries deleted for Dataset_ID: ", Dataset_ID)


def dropTable(tableName, server):
    cur_str = """DROP TABLE """ + tableName
    DB.DB_modify(cur_str, server)
    print(tableName, " Removed from DB")


def deleteCatalogTables(tableName, db_name, servers):
    contYN = input(
        "Are you sure you want to delete all of the catalog tables and data for "
        + tableName
        + " ?  [yes/no]: "
    )

    for server in servers.split(","):
        Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
        if contYN == "yes":
            deleteFromtblKeywords(Dataset_ID, db_name, server)
            deleteFromtblDataset_Stats(Dataset_ID, db_name, server)
            deleteFromtblDataset_Cruises(Dataset_ID, db_name, server)
            deleteFromtblDataset_Regions(Dataset_ID, db_name, server)
            deleteFromtblDataset_DOI_Download(Dataset_ID, db_name, server)
            deleteFromtblDataset_References(Dataset_ID, db_name, server)
            deleteFromtblVariables_JSON_Metadata(Dataset_ID, db_name, server)         
            deleteFromtblVariables(Dataset_ID, db_name, server)
            deleteFromtblDataset_Servers(Dataset_ID, db_name, server)
            deleteFromtblDataset_Programs(Dataset_ID, db_name, server)
            deleteFromtblDataset_Vault(Dataset_ID, db_name, server)
            deleteFromtblDatasets_JSON_Metadata(Dataset_ID, db_name, server)
            deleteFromtblDatasets(Dataset_ID, db_name, server)
            dropTable(tableName, server)
        else:
            print(f"Catalog tables for {tableName}, ID" + str(Dataset_ID) + " not deleted")

def deleteTableMetadata(tableName, db_name, server):
    contYN = input(
        "Are you sure you want to delete all metadata for "
        + tableName
        + " ?  [yes/no]: "
    )
    if contYN == "yes":    
        ## If variables for dataset are in tblVariables
        try:
            Dataset_ID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
            deleteFromtblKeywords(Dataset_ID, db_name, server)
            deleteFromtblVariables_JSON_Metadata(Dataset_ID, db_name, server) 
            deleteFromtblVariables(Dataset_ID, db_name, server)   
        ## If removing partial ingestion
        except:
            datasetName = input("Enter the dataset short name you want deleted: ")
            Dataset_ID = cmn.getDatasetID_DS_Name(datasetName, db_name, server)        
        deleteFromtblDataset_Stats(Dataset_ID, db_name, server)
        deleteFromtblDataset_Cruises(Dataset_ID, db_name, server)
        deleteFromtblDataset_Regions(Dataset_ID, db_name, server)
        deleteFromtblDataset_DOI_Download(Dataset_ID, db_name, server)
        deleteFromtblDataset_References(Dataset_ID, db_name, server)    
        deleteFromtblDataset_Servers(Dataset_ID, db_name, server)
        deleteFromtblDataset_Vault(Dataset_ID, db_name, server)
        deleteFromtblDatasets_JSON_Metadata(Dataset_ID, db_name, server)        
        deleteFromtblDatasets(Dataset_ID, db_name, server)
    else:
        print(f"Catalog tables for {tableName}, ID" + str(Dataset_ID) + " not deleted")      


def removeKeywords(keywords_list, var_short_name_list, tableName, db_name, server):
    """Removes a list of keywords for list of variables in a table"""

    keywords_list = cmn.lowercase_List(keywords_list)
    """Removes keyword from specific variable in table"""
    keyword_IDs = str(
        tuple(cmn.getKeywordIDsTableNameVarName(tableName, var_short_name_list, server))
    )
    cur_str = (
        """DELETE FROM """
        + db_name
        + """.[dbo].[tblKeywords] WHERE [var_ID] IN """
        + keyword_IDs
        + """ AND LOWER([keywords]) IN """
        + str(tuple(keywords_list))
    )
    DB.DB_modify(cur_str, server)
    print(
        "tblKeyword entries deleted for: ",
        keywords_list,
        var_short_name_list,
        tableName,
    )


def addKeywords(keywords_list, tableName, db_name, server, var_short_name_list="*"):
    if var_short_name_list == "*":
        var_short_name_list = cmn.get_var_list_dataset(tableName, server)
    """Inserts list of keywords for list of variables in a table"""
    keywords_list = cmn.lowercase_List(keywords_list)
    """Removes keyword from specific variable in table"""
    keyword_IDs = cmn.getKeywordIDsTableNameVarName(tableName, var_short_name_list, server)
    columnList = "(var_ID, keywords)"
    for var_ID in keyword_IDs:
        for keyword in keywords_list:
            query = """('{var_ID}', '{keyword}')""".format(
                var_ID=var_ID, keyword=keyword
            )
            cur_str = """INSERT INTO {db_name}.[dbo].[tblKeywords] {columnList} VALUES {query}""".format(
                db_name=db_name, columnList=columnList, query=query
            )
            try:
                DB.lineInsert(server, db_name+".[dbo].[tblKeywords]", columnList, query)
                print("Added keyword: " + keyword)
            except Exception as e:
                print(e)

def export_script_to_vault(tableName, branch, script_path, txt_name):
    """Saves git path for collect and process scripts to vault
    Args:
        tableName (str): CMAP table name
        branch (str): Vault structure folder (cruise, float_dir, model, etc)
        script_path (str): Directory structure within dataingest (collect/cruise/script.py)
        txt_name (str): Name of text file to save github link to (collect.txt)
    """    
    ## Vault export path
    code_path = os.path.join(getattr(vs,branch),tableName,"code",txt_name)
    git_path = cr.git_blob + script_path
    try:
        with open(code_path, 'w+') as fh:
            fh.write(git_path)
    except:
        print(f"{txt_name} not written to {tableName}")    


def export_data_to_parquet(tableName, db_name, server, branch,rep='rep'):
    ## Vault export path
    directory = getattr(vs,branch) + tableName
    if os.path.isfile(directory+f'/{rep}/{tableName}_data.parquet'):
        os.remove(directory+f'/{rep}/{tableName}_data.parquet')
        print('Vars metadata replaced')
    qry = f"SELECT * FROM {db_name}.dbo.{tableName}"
    df = DB.dbRead(qry,server)
    df.to_parquet(directory+f'/{rep}/{tableName}_data.parquet')

def export_metadata_to_parquet(tableName, db_name, server, branch):
    ds_cols = ['dataset_short_name','dataset_long_name','dataset_version','dataset_release_date','dataset_make','dataset_source','dataset_distributor','dataset_acknowledgement','dataset_history','dataset_description','dataset_references','climatology','cruise_names']
    ## Vault export path
    directory = getattr(vs,branch) + tableName
    DatasetID = cmn.getDatasetID_Tbl_Name(tableName, db_name, server)
    ## Pull dataset metadata
    qry = f"""
        select distinct d.id as Dataset_ID, d.Dataset_Name as dataset_short_name, d.Dataset_Long_Name as dataset_long_name, d.Dataset_Version as dataset_version, d.Dataset_Release_Date as dataset_release_date, m.Make as dataset_make, d.Data_Source as dataset_source, d.Distributor as dataset_distributor, d.Acknowledgement as dataset_acknowledgement, d.Dataset_History as dataset_history, d.Description as dataset_description, d.Climatology as climatology
            from tblDatasets d 
            inner join tblVariables v on d.ID = v.Dataset_ID 
            inner join tblMakes m on v.Make_ID =m.ID 
            where d.ID = {DatasetID}
    """
    df_ds = DB.dbRead(qry, server)
    qry = f"Select Dataset_ID, Reference as dataset_references from  tblDataset_References where Dataset_ID = {DatasetID}"
    df_ds_ref = DB.dbRead(qry, server)
    qry = f"Select dc.cruise_id, c.Name as cruise_names from tblDataset_Cruises dc inner join tblCruise c on dc.cruise_id = c.id where Dataset_ID = {DatasetID}"
    df_ds_cruise = DB.dbRead(qry, server)
    if branch == 'cruise' and len(df_ds_cruise) == 0:
        print("### Check associated cruises")        
    if branch == 'cruise' and len(df_ds_cruise) > 0:
        df_ds_meta = pd.concat([df_ds, df_ds_ref['dataset_references'],df_ds_cruise['cruise_names']],axis=1)
    else:
        df_ds_meta = pd.concat([df_ds, df_ds_ref['dataset_references']],axis=1)
        df_ds_meta['cruise_names'] = ''
    df_ds_meta = df_ds_meta[ds_cols]
    if os.path.isfile(directory+f'/metadata/{tableName}_dataset_metadata.parquet'):
        os.remove(directory+f'/metadata/{tableName}_dataset_metadata.parquet')
        print('Dataset metadata replaced')
    df_ds_meta.to_parquet(directory+f'/metadata/{tableName}_dataset_metadata.parquet')
    ## Climatology reads as True / False object. SQL can handle text False for bit column
    # df_ds_meta.replace({'climatology' : {False: 0, True:1}}, inplace=True)
    # df_ds_meta['climatology'] = df_ds_meta['climatology'].astype('Int64')
    ## Pull variable metadata
    qry = f"""
        select  v.Short_Name as var_short_name, v.Long_Name as var_long_name, s.Sensor as var_sensor, v.Unit as var_unit, r.Spatial_Resolution as var_spatial_res, t.Temporal_Resolution as var_temporal_res, d.Study_Domain as var_discipline, v.Visualize as visualize, [keywords_agg].Keywords as var_keywords, v.Comment as var_comment
            from tblVariables v 
            inner join tblSensors s on v.Sensor_ID = s.ID
            inner join tblSpatial_Resolutions r on v.Spatial_Res_ID = r.ID
            inner join tblTemporal_Resolutions t on v.Temporal_Res_ID = t.ID
            inner join tblStudy_Domains d on v.Study_Domain_ID = d.id
            JOIN (SELECT var_ID, STRING_AGG (CAST(keywords as NVARCHAR(MAX)), ', ') AS Keywords FROM tblVariables var_table
            JOIN tblKeywords key_table ON [var_table].ID = [key_table].var_ID GROUP BY var_ID)
            AS keywords_agg ON [keywords_agg].var_ID = v.ID
            where v.Dataset_ID = {DatasetID}
    """
    df_var_meta = DB.dbRead(qry, server)
    if os.path.isfile(directory+f'/metadata/{tableName}_vars_metadata.parquet'):
        os.remove(directory+f'/metadata/{tableName}_vars_metadata.parquet')
        print('Vars metadata replaced')
    df_var_meta.to_parquet(directory+f'/metadata/{tableName}_vars_metadata.parquet')
    if len(glob.glob(directory+'/metadata/*parquet')) > 2:
        print(f'##### More than two metadata parquet files for {tableName}')    
    print(f'Metadata export for {tableName} complete')



# def pullNetCDFMetadata(ncdf_path,meta_csv):
#     """Pulls out metadata for all variables in a NetCDF file and saves as csv in staging/combined

#     Args:
#         ncdf_path (string): Folder structure for location of NetCDF (ex. model/ARGO_MLD_Climatology/ )
#         meta_csv (string): File name for csv (ex. Argo_MLD_meta)
#     Returns:
#         csv: Saved in staging/combined
#     """   
#     nc_dir = vs.collected_data + ncdf_path
#     flist_all = np.sort(glob.glob(os.path.join(nc_dir, '*.nc')))
#     nc = sorted(flist_all, reverse=True)[:1][0]
#     xdf = xr.open_dataset(nc)

#     cols = []
#     for varname, da in xdf.data_vars.items():
#         cols = cols + list(da.attrs.keys())
    
#     col_list = list(set(cols + ['var_name']))

#     df_meta = pd.DataFrame(columns=col_list)
#     for varname, da in xdf.data_vars.items():
#         s = pd.DataFrame(da.attrs, index=[0])
#         s['var_name'] = varname     
#         df_meta = df_meta.append(s)

#     df_meta.to_csv(
#         vs.staging + 'combined/' + meta_csv + '.csv',
#         sep=",",
#         index=False)

"""
###############################################
###############################################
     Ocean Region Classification Functions
###############################################
###############################################
"""


def geopandas_load_gpkg(input_df):
    """

    Args:
        input_df (Pandas DataFrame): CMAP formatted DataFrame
    Returns:
        gdf (Geopandas GeoDataFrame): Geopandas formatted DataFrame. ie. geometry column.
    """
    gdf = geopandas.GeoDataFrame(
        input_df, geometry=geopandas.points_from_xy(input_df.lon, input_df.lat)
    )
    return gdf


def load_gpkg_ocean_region(input_gpkg_fpath):
    """Uses Geopandas to load input geopackage (gpkg)

    Args:
        input_gpkg_fpath (Geopackage - .gpkg): Input Geopackage containing geometries used for ocean region classifcation.
    Returns:
        gpkg_region (GeoDataFrame): Outputs geodataframe of ocean region geometries.
    """
    gdf = geopandas.read_file(input_gpkg_fpath)
    return gdf


def classify_gdf_with_gpkg_regions(data_gdf, region_gdf):
    """Takes sparse data geodataframe and classifies it to an ocean region

    Args:
        data_gdf (geopandas geodataframe): A geodataframe created from the input CMAP dataframe.
        region_gdf (geopandas geodataframe): A geodataframe created from ocean region gpkg.
    """
    classified_gdf = sjoin(data_gdf, region_gdf, how="left", op="within")
    # This line removes any rows where null exists. This might be do to points to close to land.
    classified_gdf_nonull = classified_gdf[~classified_gdf["NAME"].isnull()]
    return classified_gdf_nonull


def classified_gdf_to_list(classified_gdf):
    """Takes a classified/joined geodataframe and returns a set of ocean regions

    Args:
        classified_gdf (geopandas geodataframe): The joined geodataframe that contains points as well as regions.
    Returns:
        region_set : A unique list of regions belonging to that dataset.
    """
    region_set = list(set(classified_gdf["NAME"]))
    return region_set


def ocean_region_insert(region_id_list, dataset_name, db_name, server):
    dataset_ID = cmn.getDatasetID_DS_Name(dataset_name, db_name, server)
    region_ID_list = cmn.get_region_IDS(region_id_list, server)

    for region_ID in region_ID_list:
        query = (dataset_ID, region_ID)
        DB.lineInsert(
            server,
            db_name+".[dbo].[tblDataset_Regions]",
            "(Dataset_ID, Region_ID)",
            query,
        )


def ocean_region_cruise_insert(region_id_list, cruise_name, db_name, server):
    cruise_ID = cmn.get_cruise_IDS([cruise_name], server)[0]
    region_ID_list = cmn.get_region_IDS(region_id_list, server)
    for region_ID in region_ID_list:
        query = (cruise_ID, region_ID)
        DB.lineInsert(
            server,
            db_name+".[dbo].[tblCruise_Regions]",
            "(Cruise_ID, Region_ID)",
            query,
        )
    print("cruises classified by ocean region.")


def ocean_region_classification_cruise(trajectory_df, cruise_name, db_name, server):
    """This function geographically classifies a cruise trajectory into a specific ocean region

    Args:
        df (Pandas DataFrame): Input CMAP formatted cruise trajectory (ST Index: time,lat,lon)
        cruise_name (string): UNOLS name
    """
    data_gdf = geopandas_load_gpkg(trajectory_df)
    region_gdf = load_gpkg_ocean_region(
        vs.spatial_data + "World_Seas_IHO_v1_simplified/World_Seas_Simplifed.gpkg"
    )
    classified_gdf = classify_gdf_with_gpkg_regions(data_gdf, region_gdf)
    region_set = classified_gdf_to_list(classified_gdf)
    ocean_region_cruise_insert(region_set, cruise_name, db_name, server)


def ocean_region_classification(data_df, dataset_name, db_name, server):
    """This function geographically classifies a sparse dataset into a specific ocean region

    Args:
        df (Pandas DataFrame): Input CMAP formatted DataFrame (ST Index: time,lat,lon,<depth>)
        dataset_name (string): name of dataset in CMAP
    """

    data_gdf = geopandas_load_gpkg(data_df)
    region_gdf = load_gpkg_ocean_region(
        vs.spatial_data + "World_Seas_IHO_v1_simplified/World_Seas_Simplifed.gpkg"
    )
    classified_gdf = classify_gdf_with_gpkg_regions(data_gdf, region_gdf)
    region_set = classified_gdf_to_list(classified_gdf)
    ocean_region_insert(region_set, dataset_name, db_name, server)

    print("Dataset matched to the following Regions: ", region_set)

def ocean_region_names(data_df):
    """This function geographically classifies a sparse dataset into a specific ocean region

    Args:
        df (Pandas DataFrame): Input CMAP formatted DataFrame (ST Index: time,lat,lon,<depth>)
    Returns:
        region_set (list): List of region names associated with dataset
    """

    data_gdf = geopandas_load_gpkg(data_df)
    region_gdf = load_gpkg_ocean_region(
        vs.spatial_data + "World_Seas_IHO_v1_simplified/World_Seas_Simplifed.gpkg"
    )
    classified_gdf = classify_gdf_with_gpkg_regions(data_gdf, region_gdf)
    region_set = classified_gdf_to_list(classified_gdf)
    return region_set

def if_exists_dataset_region(dataset_name, db_name, server):
    """Checks if dataset ID is already in tblDatasets_Regions

    Args:
        dataset_name (string): The short name of the dataset in CMAP tblDatasets.
    Returns: Boolean
    """
    ds_ID = cmn.getDatasetID_DS_Name(dataset_name, db_name, server)
    cur_str = """SELECT * FROM {db_name}.[dbo].[tblDataset_Regions] WHERE [Dataset_ID] = {Dataset_ID}""".format(
        db_name=db_name,Dataset_ID=ds_ID
    )
    query_return = DB.dbRead(cur_str, server)
    if query_return.empty:
        bool_return = False
    else:
        bool_return = True
    return bool_return
