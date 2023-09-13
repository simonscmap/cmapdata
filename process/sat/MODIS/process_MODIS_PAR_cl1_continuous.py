from tqdm import tqdm
import pandas as pd
import numpy as np
import xarray as xr
import os
import sys


sys.path.append("ingest")
sys.path.append("../../../ingest")
import vault_structure as vs
import DB
import metadata
import api_checks as api
import data_checks as dc


tbl = "tblModis_PAR_cl1"
base_folder = f'{vs.satellite}{tbl}/raw/'
rep_folder = f'{vs.satellite}{tbl}/rep/'
os.chdir(rep_folder)

## Pull list of newly downloaded files
qry = f"SELECT Original_Name from tblProcess_Queue WHERE Table_Name = '{tbl}' AND Path IS NULL AND Error_Str IS NULL"
flist_imp = DB.dbRead(qry,'Rainier')
flist = flist_imp['Original_Name'].str.strip().to_list()

## Compare original columns with oldest netCDF in vault
test_fil = base_folder+'AQUA_MODIS.20020704.L3m.DAY.PAR.par.9km.nc'
tx =  xr.open_dataset(test_fil)
test_keys = list(tx.keys())
test_dims = list(tx.dims)
## Compare data types with oldest parquet in vault 
test_fil = rep_folder+'tblModis_PAR_cl1_2002_07_04.parquet'
test_df = pd.read_parquet(test_fil)
test_dtype = test_df.dtypes.to_dict()

#https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/AQUA_MODIS.20020705.L3m.DAY.PAR.par.9km.nc

for fil in tqdm(flist):
    fil_name = os.path.basename(base_folder+fil)
    timecol = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        ).strftime("%Y_%m_%d")   
    xdf = xr.open_dataset(base_folder+fil)
    df_keys = list(xdf.keys())
    df_dims =  list(xdf.dims)
    if df_keys != test_keys or df_dims!= test_dims:
        print(f"Check columns in {fil}. New: {df.columns.to_list()}, Old: {list(xdf.keys())}")
        sys.exit()  
    ## Option 1
    par = xdf.drop_dims(['rgb','eightbitcolor'])
    df = par.to_dataframe().reset_index()
    ## Option 2. Either option works
    # df = data.netcdf4_to_pandas(base_folder + fil, "par")
    df["time"] = pd.to_datetime(
            fil.split(".",2)[1][0:8], format="%Y%m%d"
        )
    df["year"] = pd.to_datetime(df["time"]).dt.year
    df["month"] = pd.to_datetime(df["time"]).dt.month
    df["week"] = pd.to_datetime(df["time"]).dt.isocalendar().week
    df["dayofyear"] = pd.to_datetime(df["time"]).dt.dayofyear
    df = df[["time", "lat", "lon", "par", "year", "month", "week", "dayofyear"]]
    if df.dtypes.to_dict() != test_dtype:
        print(f"Check data types in {fil}.")
        sys.exit()      
    df.to_parquet(f"{rep_folder}{tbl}_{timecol}.parquet", index=False)      
    path = f"{rep_folder.split('vault/')[1]}{tbl}_{timecol.replace('-','_')}.parquet"
    metadata.tblProcess_Queue_Process_Update(fil_name, path, tbl, 'Opedia', 'Rainier')
    xdf.close()
    try:
        s3_str = f"aws s3 cp {tbl}_{timecol}.parquet s3://cmap-vault/observation/remote/satellite/{tbl}/rep/"
        os.system(s3_str)
        metadata.tblIngestion_Queue_Staged_Update(path, tbl, 'Opedia', 'Rainier')
        yr, mnth, day = timecol.split('_')
        api.clusterModify(f"delete from tblModis_PAR_NRT where time='{yr}-{mnth}-{day}'")
    except Exception as e:        
        print(str(e))     

