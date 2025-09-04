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
import metadata
import api_checks as api

tbl = 'tblModis_CHL_cl1'
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'

os.chdir(rep_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'AQUA_MODIS.20020704_20020711.L3m.8D.CHL.chlor_a.9km.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = rep_folder+'tblModis_CHL_cl1_2002_07_04.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()


for fil in tqdm(flist):
    fil_date = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")    
    x = xr.open_dataset(base_folder+fil)
    df_keys = list(x.keys())
    df_dims =  list(x.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(x.keys())}")
        sys.exit()     
    chl = x.drop_dims(['rgb','eightbitcolor'])
    df = chl.to_dataframe().reset_index()
    fdate = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df.insert(0,"time", fdate)
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear


    ####### This block of code needs to be checked!!
    df['time'] = df['time'].astype('<M8[us]')
    for col in ['lat', 'lon']:
        df[col] = df[col].astype('float64')
    for col in ['year', 'month', 'dayofyear']:
        df[col] = df[col].astype('int64')
    ################################################    


    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}.")
        print(df.dtypes.to_dict())
        print(test_dtype) 
        sys.exit()  
    fil_name = os.path.basename(fil)
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{fil_date}.parquet"
    df.to_parquet(f"{rep_folder}{tbl}_{fil_date}.parquet", index=False)      
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    x.close()
    try:
        a = [df,df]
        b = [tbl,tbl]
        # c = ['mariana','rossby']      
        c = ['rossby']      
        with Pool(processes=2) as pool:
            result = pool.starmap(DB.toSQLbcp_wrapper, zip(a,b,c))
            pool.close() 
            pool.join()
        s3_str = f"aws s3 cp {tbl}_{fil_date}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/rep/"
        os.system(s3_str)
        metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')
        yr, mnth, day = fil_date.split('_')
        api.clusterModify(f"delete from tblModis_CHL_NRT where time='{yr}-{mnth}-{day}'")
    except Exception as e:        
        print(str(e))        


# metadata.export_script_to_vault(tbl,'satellite','process/sat/MODIS/process_MODIS_CHL_8day_cl1.py','process.txt')

