import sys
import pandas as pd
import requests
from urllib.parse import quote

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)

sys.path.append("ingest")

import common as cmn
import metadata
import stats
import DB
import vault_structure as vs
import credentials as cr

def returnAPI(url):
    res = requests.get(url)
    data = res.json()['data']
    df = pd.DataFrame.from_dict(data)
    message = res.json()['message']
    er = res.json()['error']
    ver = res.json()['version']
    if ver != '0.1.39':
        print(f"### New API version: {ver}")
    return df, message, er, ver

def exportMetaParquet(ds_id, db_name, server):
    tableName = cmn.getTbl_Name_DatasetID(ds_id, db_name, server)
    branch = input(f"####### Enter cruise, satellite, float_dir, etc for {tableName}\n")
    metadata.export_metadata_to_parquet(tableName, db_name, server, branch)


def varsInTableNotCatalog(table_name):
    ## Checks all on prem servers
    url = f"https://cmapdatavalidation.com/db/varsInTableNotCatalog?table_name={table_name}"
    df, message, er, ver = returnAPI(url)

def strandedTables():
    url = 'https://cmapdatavalidation.com/db/strandedTables'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("strandedTables successfully run")
    else:
        print("### Stranded Tables found:")
        print(df)

def duplicateDatasetLongName():
    url = 'https://cmapdatavalidation.com/db/duplicateDatasetLongName'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("duplicateDatasetLongName successfully run")
    else:
        print("### Duplicate Dataset Long Names found:")
        print(df)

def strandedVariables(server_list, db_name):
    ## Checks all on prem servers
    url = 'https://cmapdatavalidation.com/db/strandedVariables'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("strandedVariables successfully run")
    else:
        for row in df.itertuples():
            var = row.Variable
            var_name = var.replace('[','').replace(']','')
            Yn = input(f"### Remove stranded variable {var_name} not in {row.Table_Name}? Y or N")
            if Yn.lower() == 'y':     
                for server in server_list:       
                    ds_id = cmn.getDatasetID_Tbl_Name(row.Table_Name, db_name, server)
                    var_id = cmn.findVarID(ds_id, var_name, db_name, server)
                    metadata.deleteFromtblKeywords_VarID(var_id, db_name, server)
                    metadata.deleteFromtblVariables_VarID(var_id, db_name, server)
                    stats.updateStats_Small(row.Table_Name, db_name, server)
                exportMetaParquet(ds_id, db_name, server)
            else:
                continue    

def numericLeadingVariables(server, db_name):
    url = 'https://cmapdatavalidation.com/db/numericLeadingVariables'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("numericLeadingVariables successfully run")
    else:
        for row in df.itertuples():
            ds_id = cmn.getDatasetID_Tbl_Name(row.Table_Name, db_name, server)
            var_id = cmn.findVarID(ds_id, row.Short_Name, db_name, server)
            qry = f"SELECT ID, short_name, long_name, comment from tblvariables where dataset_id = {ds_id}"
            df_var = DB.dbRead(qry,server)
            print(f"#### Var table for {row.Table_Name}. Variable short name: {row.Short_Name} ")
            print(df_var)


def datasetsWithBlankSpaces(server_list, db_name):
    url = 'https://cmapdatavalidation.com/db/datasetsWithBlankSpace'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("datasetsWithBlankSpace successfully run")
    else:
        for row in df.itertuples():
            ds_id = cmn.getDatasetID_DS_Name(row.Dataset_Short_Name, db_name, server)
            new_name = row.Dataset_Short_Name.strip().replace('- ','').replace(' ','_')
            Yn = input(f"### Replace {row.Dataset_Short_Name} with {new_name}? Y or N")
            if Yn.lower() == 'y':
                for server in server_list:
                    qry_df = f"UPDATE {db_name}.dbo.tblDatasets SET Dataset_Name = '{new_name}' WHERE ID = {ds_id}"
                    DB.DB_modify(qry_df,server)
                    ## add new dataset name as to keyword to all vars in dataset
                    tableName = cmn.getTbl_Name_DatasetID(ds_id, db_name, server)
                    metadata.addKeywords([new_name], tableName, db_name, server)
                exportMetaParquet(ds_id, db_name, server)


def duplicateVarLongName(server, db_name):
    url = 'https://cmapdatavalidation.com/db/duplicateVarLongName'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("duplicateVarLongName successfully run")
    else:
        for row in df.itertuples():
            ds_id = cmn.getDatasetID_Tbl_Name(row.table_name, db_name, server)
            qry = f"SELECT ID, Short_Name, Long_Name, Unit, Comment FROM tblVariables WHERE Dataset_ID = {ds_id} and Long_Name = '{row.Long_Name}'"
            df_var = DB.dbRead(qry,server)
            print(f"### Var table for {row.table_name}. Duplicate names: {row.Long_Name} ")
            print(df_var)

def varsWithBlankSpace(server_list, db_name):
    url = 'https://cmapdatavalidation.com/db/varsWithBlankSpace'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("varsWithBlankSpace successfully run")
    else:
        for row in df.itertuples():
            ds_id = cmn.getDatasetID_Tbl_Name(row.Table_Name, db_name, server)
            new_name = row.Short_Name.replace(' ','')
            Yn = input(f"### Replace {row.Short_Name} with {new_name} in table {row.Table_Name}? Y or N: ")
            if Yn.lower() == 'y':
                for server in server_list:
                    ## Check if column name also has a space before updating
                    ## Have to check if column name LIKE variable name because SQL ignores trailing spaces when checking if equal or if pulling from the table itself
                    ## varsInTableNotCatalog(table_name) ## Checks all on prem servers
                    qry_test = f"select c.name from sys.columns c inner join sys.tables t on c.object_id = t.object_id where t.name = '{row.Table_Name}' and c.name like '{row.Short_Name}'"
                    df_test = DB.dbRead(qry_test, server)
                    if len(df_test) !=0:
                        print(f"#### UPDATE {row.Table_Name} RENAME '{row.Short_Name}' ####")
                        sys.exit()
                    qry_v = f"SELECT ID from {db_name}.dbo.tblVariables WHERE Dataset_ID = {ds_id} and Short_Name = '{row.Short_Name}'"
                    var_id_return = DB.dbRead(qry_v, server)
                    if len(var_id_return) != 1:
                        print(f"### Multiple entries of '{row.Short_Name}' in tblVariables for {row.Table_Name}")
                        sys.exit()
                    var_id = var_id_return.iloc[0][0]
                    qry = f"UPDATE {db_name}.dbo.tblVariables SET Short_Name = '{new_name}' WHERE ID = {var_id}"
                    DB.DB_modify(qry, server)
                    metadata.tblKeywords_Insert_VarID(var_id, new_name, db_name, server)
                exportMetaParquet(ds_id, db_name, server)
            else:
                continue


# def langDescription(dataset_name):
#     url = f'https://cmapdatavalidation.com/db/langDescription?dataset_name={dataset_name}'
#     df, message, er, ver = returnAPI(url)
#     if message == 'success' and not er:
#         print("langDescription successfully run")
#     else:
#         print(f"### Dataset description issue found in {dataset_name} ")
#         print(df)        


def deadLinks(dataset_name):
    url = f'https://cmapdatavalidation.com/db/deadLinks?dataset_name={dataset_name}'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("deadLinks successfully run")
    else:
        print(f"### Dead links found in {dataset_name} ")
        print(df)        

def checkCatalog():
    url = 'https://cmapdatavalidation.com/db/catalogsMismatch'
    df, message, er, ver = returnAPI(url)
    if message == 'success' and not er:
        print("checkCatalog successfully run")
    return df


def temporalRange(tbl):
    url = f'https://cmapdatavalidation.com/cluster/temporalRange?table_name={tbl}'
    df, message, er, ver = returnAPI(url)
    min_time = df['min_time'][0]
    max_time = df['max_time'][0]
    return min_time, max_time


def statsCluster(tbl, depth_flag):
    if depth_flag ==1:
        qry = f"Select min(time) Time_Min, max(time) Time_Max, min(lat) Lat_Min, max(lat) Lat_Max, min(lon) Lon_Min, max(lon) Lon_Max, min(depth) Depth_Min, max(depth) Depth_Max from {tbl}"
    else:
        qry = f"Select min(time) Time_Min, max(time) Time_Max, min(lat) Lat_Min, max(lat) Lat_Max, min(lon) Lon_Min, max(lon) Lon_Max from {tbl}"
        min_depth = None
        max_depth = None
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(qry)}"
    try:
        outPath = vs.download_transfer+'data.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        totalbits = 0
        with open(outPath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
        df = pd.read_parquet(outPath)
        min_time = df['Time_Min'][0]
        max_time = df['Time_Max'][0]
        min_lat = df['Lat_Min'][0]
        max_lat = df['Lat_Max'][0]
        min_lon =  df['Lon_Min'][0]
        max_lon = df['Lon_Max'][0]   
        if depth_flag == 1: 
            min_depth = df['Depth_Min'][0]   
            max_depth = df['Depth_Max'][0]   
        else:
            min_depth = None
            max_depth = None
    except Exception as e:        
        print(str(e))
    return min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth

def maxDateCluster(tbl):
    qry = f"Select max(time) time from {tbl}"
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(qry)}"
    try:
        outPath = vs.download_transfer+f'{tbl}mxdata.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        totalbits = 0
        with open(outPath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
        df = pd.read_parquet(outPath)
        mx_time = df['time'][0]

        try:
            return mx_time.to_pydatetime().date()
        except:
            return mx_time
            
    except Exception as e:        
        print(str(e))
    return 

def minDateCluster(tbl):
    qry = f"Select min(time) time from {tbl}"
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(qry)}"
    try:
        outPath = vs.download_transfer+f'{tbl}mndata.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        totalbits = 0
        with open(outPath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
        df = pd.read_parquet(outPath)
        mn_time = df['time'][0]
    except Exception as e:        
        print(str(e))
    return mn_time

def clusterModify(sql):
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(sql)}"
    try:
        outPath = vs.download_transfer+'qry.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        # totalbits = 0
        # with open(outPath, 'wb') as f:
        #     for chunk in resp.iter_content(chunk_size=1024):
        #         if chunk:
        #             totalbits += 1024
        #             print("Downloaded",totalbits*1025,"KB...")
        #             f.write(chunk)
        # print(pd.read_parquet(outPath))
    except Exception as e:        
        print(str(e))
    return 


def query(sql):   
    url = f"https://cmapdatavalidation.com/cluster/query?sql={quote(sql)}"
    try:
        outPath = vs.download_transfer+'data.parquet'
        headers = {
            "accept": "application/json'",
            "Authorization": f"{cr.pycmap_api_key}"
        }
        resp = requests.get(url, headers=headers, timeout=1000) 
        totalbits = 0
        with open(outPath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    totalbits += 1024
                    print("Downloaded",totalbits*1025,"KB...")
                    f.write(chunk)
        df = pd.read_parquet(outPath)
    except Exception as e:        
        print(str(e))
    return df

def postIngestAPIChecks(server = 'Rossby'):
    
    #TODO: remove after once mariana is fixed
    import sys
    sys.exit()
    ###########################################
    

    ## Runs DB endpoint checks. Default server is Rossby
    db_name = 'Opedia'
    strandedTables()
    strandedVariables(server, db_name) ## Checks all on prem servers
    numericLeadingVariables(server, db_name)
    duplicateVarLongName(server, db_name)
    duplicateDatasetLongName()
    datasetsWithBlankSpaces(server, db_name)
    varsWithBlankSpace(server, db_name)


#deadLinks(dataset_name)







