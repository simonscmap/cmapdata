#~/anaconda3/envs/gdalenv/envs/hdf38/bin/python
"""
Author: Diana Haring <dharing@uw.edu>
Date: 05-25-2023

Script to run processing for continuous ingestion
"""
import sys
import os

import pandas as pd
import numpy as np
from pyhdf.SD import SD, SDC
from pathlib import Path
from multiprocessing import Pool
import datetime
from pytz import timezone

from tqdm import tqdm

sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import credentials as cr
import DB 


tbl = 'tblModis_AOD_REP'
base_folder = f'{vs.satellite}{tbl}/raw/MYD08_M3'
rep_folder = f'{vs.satellite}{tbl}/rep/'
os.chdir(rep_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'/2019/001/MYD08_M3.A2019001.061.2019035155007.hdf'
file = SD(test_fil, SDC.READ)
sds_obj = file.select('AOD_550_Dark_Target_Deep_Blue_Combined_Mean_Mean')
test_items = sds_obj.attributes().items()

## Compare data types with oldest parquet in vault 
test_fil = rep_folder+'tblModis_AOD_REP_2002_07_01.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

def tblProcess_Queue_Process_Update(Original_Name, Path, Table_Name, db_name, server, error_flag=''):
    pr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    if len(error_flag) ==0:    
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Processed = '{pr_str}', Path='{Path}' WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "
    else:
        qry = f"UPDATE {db_name}.[dbo].[tblProcess_Queue] SET Processed = '{pr_str}', Error_Str='{error_flag}' WHERE Table_Name = '{Table_Name}' and Original_Name = '{Original_Name}' "        
    DB.DB_modify(qry,server)

def tblIngestion_Queue_Staged_Update(Path, Table_Name, db_name, server):
    sr_str = datetime.datetime.now().astimezone(timezone('US/Pacific')).strftime("%Y-%m-%d %H:%M:%S")
    columnList = "(Path, Table_Name, Staged)"
    query = (Path, Table_Name,sr_str)
    DB.lineInsert(
                server, db_name +".[dbo].[tblIngestion_Queue]", columnList, query
            )

for f in tqdm(flist):
    file_date = f.rsplit('.',4)[1].replace('A','')
    check_date =pd.to_datetime(file_date, format = '%Y%j').date()
    file = SD(f"{base_folder}/{check_date.year}/{check_date.strftime('%j')}/{f}", SDC.READ)
    ## Get lat lon
    xdim = file.select('XDim')
    ydim = file.select('YDim')
    lon,lat = np.meshgrid(xdim[:].astype(np.double),ydim[:].astype(np.double))
    sds_obj = file.select('AOD_550_Dark_Target_Deep_Blue_Combined_Mean_Mean')
    check_items = sds_obj.attributes().items()
    if check_items != test_items:
        print(f"Raw format changed for {f}")
        sys.exit()
    data = sds_obj.get()
    for key, value in sds_obj.attributes().items():
        # print(key, value)
        if key == 'add_offset':
            add_offset = value  
        if key == 'scale_factor':
            scale_factor = value
        if key == '_FillValue':
            fill_value = value    
    data = data.astype('float')
    data[data == fill_value] = np.nan
    data_scaled = (data - add_offset) * scale_factor
    df = pd.DataFrame({'lat': lat.ravel(), 'lon': lon.ravel(), 'aod': data_scaled.ravel()})
    ## using .date() makes time an object instead of datetime
    df['time'] =pd.to_datetime(file_date, format = '%Y%j')
    df['time'] = df['time'].astype('datetime64[us]')
    df["year"] = pd.to_datetime(df["time"]).dt.year.astype(int)
    df["month"] = pd.to_datetime(df["time"]).dt.month.astype(int)
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week.astype(int)
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear.astype(int)
    df = df.sort_values(['time','lat','lon'],ascending=[True] * 3)
    df = df[['lat','lon','time','aod','year','month','week','dayofyear']]
    df.rename(columns={'aod':'AOD'}, inplace=True)
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {f}. New: {df.columns.to_list()}")        
        sys.exit()   
    df.to_parquet(f"{rep_folder}{tbl}_{check_date.strftime('%Y-%m-%d').replace('-','_')}.parquet",index=False)
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{check_date.strftime('%Y-%m-%d').replace('-','_')}.parquet"
    tblProcess_Queue_Process_Update(f, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {tbl}_{check_date.strftime('%Y-%m-%d').replace('-','_')}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/rep/"
    os.system(s3_str)
    tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier') 
    a = [df,df]
    b = [tbl,tbl]
    c = ['mariana','rossby'] 
    with Pool(processes=2) as pool:
        result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
        pool.close() 
        pool.join()

