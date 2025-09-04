import sys
import os
import datetime as dt
from time import sleep

sys.path.append("ingest")
sys.path.append("../../../ingest")

import vault_structure as vs
import DB
import metadata
import api_checks as api

tbl = 'tblSSS_NRT_cl2'
base_folder = f'{vs.satellite}{tbl}/raw/'



def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')
    if len(df_ing) == 0:
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_path_date = dt.date(int(yr),int(mo),int(day)) 
        qry = f"SELECT max(original_name) mx from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_str IS NOT NULL"
        mx_name = DB.dbRead(qry,'Rainier')
        if mx_name['mx'][0] == None:
            max_name_date = dt.date(1900,1,1)
        else: 
            yr, mo, day = mx_name['mx'][0].strip().split('_')
            max_name_date = dt.date(int(yr),int(mo),int(day))  
        max_data_date = api.maxDateCluster(tbl)           
        max_date = max([max_path_date,max_name_date,max_data_date])
    else:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = dt.date(int(yr),int(mo),int(day))
    return max_date


def get_SSS(date, retry=False):
    yr = date.year
    dayn = format(date, "%j")
    dayn_str = dayn.zfill(3)
    file_url = f'https://data.remss.com/smap/SSS/V06.0/FINAL/L3/8day_running/{yr}/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V06.0.nc'
    save_path = f'{vs.satellite + tbl}/raw/RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V06.0.nc'
    wget_str = f'wget --no-check-certificate "{file_url}" -O "{save_path}"'  
    try:
        os.system(wget_str)
        Error_Date = date.strftime('%Y_%m_%d')         
        Original_Name = f'RSS_smap_SSS_L3_8day_running_{yr}_{dayn_str}_FNL_V06.0.nc'
            ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {Error_Date}')
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','Download Error')
                os.remove(save_path)
        else:
            if retry:
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        print("No file found for date: " + Error_Date )
        metadata.tblProcess_Queue_Download_Insert(Error_Date, tbl, 'Opedia', 'Rainier','No data')

        
def retryError(tbl):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [dt.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for date in dt_list:
            get_SSS(date, True)


retryError(tbl)
max_date = getMaxDate(tbl)
end_date = dt.date.today()
delta = dt.timedelta(days=1)
max_date += delta
## New data on ~ two week delay
end_date -= dt.timedelta(days=11)


while max_date <= end_date:
    get_SSS(max_date)
    sleep(1)    
    max_date += delta







