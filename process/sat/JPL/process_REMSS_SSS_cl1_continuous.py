import sys
import os

import pandas as pd
import numpy as np
import xarray as xr
from tqdm import tqdm
from multiprocessing import Pool

sys.path.append("ingest")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB
import data
import metadata


tbl = 'tblSSS_NRT_cl1'
base_folder = f'{vs.satellite}{tbl}/raw/'
nrt_folder = f'{vs.satellite}{tbl}/nrt/'
os.chdir(nrt_folder)


## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'RSS_smap_SSS_L3_8day_running_2015_091_FNL_V05.0.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = nrt_folder+'tblSSS_NRT_cl1_2015_04_01.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()


for fil in tqdm(flist):
    x = xr.open_dataset(base_folder+fil)
    df_keys = list(x.keys())
    df_dims =  list(x.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(x.keys())}")
        sys.exit()       
    x_time = x.time.values[0]
    x = x['sss_smap']
    df_raw = x.to_dataframe().reset_index()
    df_raw['time'] = x_time
    x.close()
    df = data.add_day_week_month_year_clim(df_raw)
    df = df[['time','lat','lon','sss_smap','year','month','week','dayofyear']]
    df = df.sort_values(["time", "lat","lon"], ascending = (True, True,True))
    df = data.mapTo180180(df)


    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')

    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}. New: {df.columns.to_list()}")
        print(df.dtypes.to_dict())
        print(test_dtype)        
        sys.exit()       
    fil_name = os.path.basename(fil)
    fil_date = df['time'][0].strftime("%Y_%m_%d") 
    path = f"{nrt_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{nrt_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/nrt/"
    os.system(s3_str)
    metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier') 
    a = [df,df,df]
    b = [tbl,tbl,tbl]
    # c = ['mariana','rossby','rainier'] 
    c = ['rossby','rainier'] 
    with Pool(processes=3) as pool:
        result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
        pool.close() 
        pool.join()
