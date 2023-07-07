import sys
import os
import datetime
import glob
import time

directory = os.path.abspath('../../..')
sys.path.append(directory)

from ingest import vault_structure as vs
from ingest import DB
from ingest import credentials as cr
from ingest import metadata
from ingest import api_checks as api


tbl = 'tblAltimetry_REP_Signal'
base_folder = f'{vs.satellite}{tbl}/raw/'
# vs.leafStruc(vs.satellite+tbl)

output_dir = base_folder.replace(" ", "\\ ")

def getMaxDate(tbl):
    ## Check tblIngestion_Queue for downloaded but not ingested
    qry = f"SELECT Path from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NULL"
    df_ing = DB.dbRead(qry, 'Rainier')

    if len(df_ing) == 0:
        qry = f"SELECT max(path) mx from dbo.tblIngestion_Queue WHERE Table_Name = '{tbl}' AND Ingested IS NOT NULL"
        mx_path = DB.dbRead(qry,'Rainier')
        path_date = mx_path['mx'][0].split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_path_date = datetime.date(int(yr),int(mo),int(day)) 
        
        qry = f"SELECT max(original_name) mx from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_str IS NOT NULL"
        mx_name = DB.dbRead(qry,'Rainier')
        yr, mo, day = mx_name['mx'][0].strip().split('_')
        max_name_date = datetime.date(int(yr),int(mo),int(day))   

        min_time, max_time = api.temporalRange(tbl)   
        yr, mo, day = max_time.split('-')
        max_data_date = datetime.date(int(yr),int(mo),int(day)) 

        max_date = max([max_path_date,max_name_date,max_data_date])
    else:
        last_path = df_ing['Path'].max()
        path_date = last_path.split('.parquet')[0].rsplit(tbl+'_',1)[1]
        yr, mo, day = path_date.split('_')
        max_date = datetime.date(int(yr),int(mo),int(day))
    return max_date



#ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/2021/12/dt_global_allsat_phy_l4_20211201_20220422.nc

def wget_file(output_dir, date,retry=False):
    yr = f'{date:%Y}'
    mn = f'{date:%m}'
    dy = f'{date:%d}'
    start_index = date.strftime('%Y%m%d')
    fpath = f"ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/{yr}/{mn}/dt_global_allsat_phy_l4_{yr}{mn}{dy}_*"
    wget_str = f"""wget --no-parent -nd -r -m --ftp-user={cr.usr_cmem} --ftp-password={cr.psw_cmem} {fpath}  -P  {output_dir}"""
    try:
        os.system(wget_str)
        pname = glob.glob(f"{base_folder}dt_global_allsat_phy_l4_{yr}{mn}{dy}*")
        Original_Name = pname[0].split(f"{base_folder}")[1]
        save_path = base_folder+Original_Name
            ## Remove empty downloads
        if os.path.getsize(save_path) == 0:
            print(f'empty download for {start_index}')
            if not retry:
                metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','Download Error')
                os.remove(save_path)
        else:
            if retry:
                Error_Date = f"{start_index}"
                metadata.tblProcess_Queue_Download_Error_Update(Error_Date, Original_Name,  tbl, 'Opedia', 'Rainier')
                print(f"Successful retry for {Error_Date}")
            else:
                metadata.tblProcess_Queue_Download_Insert(Original_Name, tbl, 'Opedia', 'Rainier')
    except:
        print("No file found for date: " + start_index )
        metadata.tblProcess_Queue_Download_Insert(f"{start_index}", tbl, 'Opedia', 'Rainier','No data')

def retryError(tbl,output_dir):
    qry = f"SELECT Original_Name from dbo.tblProcess_Queue WHERE Table_Name = '{tbl}' AND Error_Str IS NOT NULL"
    df_err = DB.dbRead(qry, 'Rainier')
    dt_list = df_err['Original_Name'].to_list()
    if len(dt_list)>0:
        dt_list = [datetime.datetime.strptime(x.strip(), '%Y_%m_%d').date() for x in dt_list]
        for dt in dt_list:
            wget_file(output_dir, dt, True)

# retryError(tbl)

### Check if new data is available


max_date = getMaxDate(tbl)
print(f"Last {tbl} data downloaded: {max_date}")
end_date = datetime.date.today()

#2020-12-31
max_date = datetime.date(2021, 1, 16)
end_date = datetime.date(2022, 6, 23)



delta = datetime.timedelta(days=1)
max_date += delta

while max_date <= end_date:
    wget_file(output_dir, max_date)
    max_date += delta
    time.sleep(.1)
        



