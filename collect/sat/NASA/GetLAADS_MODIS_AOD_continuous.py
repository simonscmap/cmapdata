
import sys
import os
import datetime as dt
from dateutil.relativedelta import relativedelta
import glob

sys.path.append("ingest")

import credentials as cr
import vault_structure as vs
import DB
import metadata


tbl = 'tblModis_AOD_REP'

base_folder = f'{vs.satellite}{tbl}/raw/'


qry = "select max(time) time  FROM [Opedia].[dbo].[tblModis_AOD_REP]"
df_max = DB.dbRead(qry,'Rossby')
max_date = df_max['time'][0]


def getAOD(date, retry=False):
    dayn = format(date, "%j")
    active_dir = f"{base_folder}MYD08_M3/{date.year}/"
    os.makedirs(active_dir, exist_ok=True)
    os.chdir(active_dir)
    wget_str = f'wget -e robots=off -m -np -R .html,.tmp -nH --cut-dirs=5 "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD08_M3/{date.year}/{dayn}/" --header "Authorization: Bearer {cr.nasa_earthdata_token}" -P .'
    Error_Date = date.strftime('%Y_%m_%d')     
    try:
        os.system(wget_str)
        save_path = glob.glob(f"{base_folder}MYD08_M3/{date.year}/{dayn}/*.hdf")[0]
            ## Remove empty downloads
        Original_Name = os.path.basename(save_path)        
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {Error_Date}')
            os.remove(save_path)
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','Download Error')
        else:
            if retry:
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        print("No file found for date: " + Error_Date)
        metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','No data')

next_aod_in_cmap = dt.date(max_date.year, max_date.month+1, 1)
now = dt.date(dt.date.today().year, dt.date.today().month, 1)


while next_aod_in_cmap <= now:
    print(f" Downloading AOD for {next_aod_in_cmap} ...")
    getAOD(next_aod_in_cmap)
    next_aod_in_cmap = next_aod_in_cmap + relativedelta(months=1)
