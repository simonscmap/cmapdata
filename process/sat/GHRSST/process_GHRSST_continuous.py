"""
Author: Diana Haring <dharing@uw.edu>
Date: 02-10-2023

Script to run processing for continuous ingestion
"""

import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import xarray as xr
import numpy as np
from tqdm import tqdm 
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB
import data
import metadata

tbl = 'tblSST_AVHRR_OI_NRT'

sat_dir = f'{vs.satellite + tbl}/raw/'
nrt_dir = f'{vs.satellite + tbl}/nrt/'

os.chdir(nrt_dir)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = sat_dir+'20190428120000-NCEI-L4_GHRSST-SSTblend-AVHRR_OI-GLOB-v02.0-fv02.1.nc'
tx =  xr.open_dataset(test_fil)
test_df = tx.to_dataframe().reset_index()
test_cols = test_df.columns.tolist()
## Compare data types with oldest parquet in vault 
test_fil = nrt_dir+tbl+'_1981_09_01.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()


## Process and ingest SST NetCDF
for fil in tqdm(flist):
    xdf = xr.open_dataset(sat_dir+fil)
    df = xdf.to_dataframe().reset_index()
    if df.columns.to_list() != test_cols:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        sys.exit()
    ## nv variable 1 and 0, both hold same SST data
    df = df.query('nv == 1')
    df_import = df[['lat', 'lon','time','analysed_sst']]
    df_import.rename(columns={'analysed_sst':'sst'}, inplace = True)
    ## Original data in Kelvin
    df_import['sst'] = df_import['sst']- 273.15 
    df_import['time'] =  df_import['time'].dt.date
    # pd.to_datetime(df_import['time']).dt.date
    df_import = data.add_day_week_month_year_clim(df_import)
    df_import.sort_values(['time', 'lat', 'lon'], ascending=[True, True, True], inplace=True)
    df_import['week'] = df_import['week'].astype(int)
    ## Data type changed in 5/2023 from float(64) to float(32)
    for col in ['sst', 'lat', 'lon']:
        df_import[col] = df_import[col].astype('float64')
    for col in ['year', 'month', 'week', 'dayofyear']:
        df_import[col] = df_import[col].astype('int64')
    if df_import.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. \n\tNew: {df.columns.to_list()}, \n\tOld: {test_cols}")
        print(df_import.dtypes.to_dict())
        print(test_dtype)
        sys.exit()        
    fil_date = df_import['time'].max().strftime('%Y_%m_%d')
    path = f"{nrt_dir.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df_import.to_parquet(f'{nrt_dir}{tbl}_{fil_date}.parquet', index=False)


    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/tblSST_AVHRR_OI_NRT/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')
    ## Also add to on prem servers
    a = [df_import,df_import,df_import]
    b = [tbl,tbl,tbl]
    # c = ['mariana','rossby','rainier']
    c = ['rossby','rainier']
    with Pool(processes=3) as pool:
        result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
        pool.close() 
        pool.join()


