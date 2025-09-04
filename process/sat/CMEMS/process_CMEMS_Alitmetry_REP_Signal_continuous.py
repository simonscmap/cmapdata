"""
Author: Diana Haring <dharing@uw.edu>
Date: 04-19-2023

Script to run processing for continuous ingestion
"""

import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
from tqdm import tqdm
from multiprocessing import Pool

sys.path.append("cmapdata/ingest")

sys.path.append("ingest")
import vault_structure as vs
import DB 
import metadata
import data
import api_checks as api


tbl = 'tblAltimetry_REP_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
meta_folder = f'{vs.satellite}{tbl}/metadata/'
rep_folder = f'{vs.satellite}{tbl}/rep/'

os.chdir(rep_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()


## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'dt_global_allsat_phy_l4_19930101_20210726.nc'
tx =  xr.open_dataset(test_fil)
test_df = tx.to_dataframe().reset_index()
test_cols = test_df.columns.tolist()
## Compare data types with oldest parquet in vault 
test_fil = rep_folder+'dt_global_allsat_phy_l4_19930101_20210726.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

## Process and ingest Altimetry NetCDF
for fil in tqdm(flist):
    x = xr.open_dataset(base_folder+fil)
    df = x.to_dataframe().reset_index()
    x.close()
    ## old order ended in flag_ice, tpa_correction. new order ends in 'tpa_correction', 'flag_ice'
    if df.columns.to_list().sort() != test_cols.sort():
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        sys.exit()    
    df = df.query('nv == 1')
    df = df[['time','latitude', 'longitude', 'sla', 'err_sla', 'ugosa', 'err_ugosa', 'vgosa', 'err_vgosa', 'adt', 'ugos', 'vgos', 'flag_ice', 'tpa_correction']]
    df = df.sort_values(["time", "latitude","longitude"], ascending = (True, True,True))
    df.rename(columns={'latitude':'lat', 'longitude':'lon'}, inplace = True)
    df = data.add_day_week_month_year_clim(df)


    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')


    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}, Old: {test_cols}")
        print(df.dtypes.to_dict())
        print(test_dtype)
        sys.exit()   
    path = f"{rep_folder.split('vault/')[1]}{fil.split('.nc')[0]}.parquet"
    df.to_parquet(f"{rep_folder}{fil.split('.nc')[0]}.parquet", index=False)
    metadata.tblProcess_Queue_Process_Update(fil, path, tbl, 'Opedia', 'Rainier')    
    try:
        a = [df,df]
        b = [tbl,tbl]
        # c = ['mariana','rossby']     
        c = ['rossby']     
        with Pool(processes=2) as pool:
            result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
            pool.close() 
            pool.join()
        s3_str = f"aws s3 cp {fil.split('.nc')[0]}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/rep/"
        os.system(s3_str)
        metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')
        fil_date = df['time'].max().strftime('%Y_%m_%d')
        yr, mnth, day = fil_date.split('_')
        api.clusterModify(f"delete from tblAltimetry_NRT_Signal where time='{yr}-{mnth}-{day}'")
    except Exception as e:        
        print(str(e))            
    del df

# ### Fix old parquets to remove index
# flist = glob.glob(rep_folder+'*.parquet')
# for fl in tqdm(flist):
#     df = pd.read_parquet(fl)
#     df.to_parquet(fl,index=False)

# ## Backfill new data to Rossby and Mariana
# qry = f"SELECT path from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Processed IS NOT NULL AND Error_Str IS NULL "
# flist_imp = DB.dbRead(qry,'Rainier')
# flist = flist_imp['path'].str.strip().to_list()
# # old_flist = ['observation/remote/satellite/tblAltimetry_REP_Signal/rep/dt_global_allsat_phy_l4_20210101_20220120.parquet','observation/remote/satellite/tblAltimetry_REP_Signal/rep/dt_global_allsat_phy_l4_20210102_20220120.parquet','observation/remote/satellite/tblAltimetry_REP_Signal/rep/dt_global_allsat_phy_l4_20210103_20220120.parquet']
# old_flist = ['observation/remote/satellite/tblAltimetry_REP_Signal/rep/dt_global_allsat_phy_l4_20210102_20220120.parquet','observation/remote/satellite/tblAltimetry_REP_Signal/rep/dt_global_allsat_phy_l4_20210103_20220120.parquet']
# flist = old_flist+flist 

# # fil = flist[0] len(flist) server = 'mariana'
# server = 'rossby'
# for fil in tqdm(flist):
#     df = pd.read_parquet(vs.vault+fil)
#     DB.toSQLbcp_wrapper(df, tbl, server)
