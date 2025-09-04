"""
Author: Diana Haring <dharing@uw.edu>
Date: 05-04-2023

Script to run continuous ingestion
"""
import sys
import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import numpy as np
from tqdm import tqdm 
import requests
from urllib.parse import quote
import datetime
from multiprocessing import Pool
import time

sys.path.append("ingest")

import stats
import credentials as cr
import api_checks as api
import common as cmn
import DB



## Flag to run script
run_script = True

cur_dir = os.getcwd()
# get_pisces = '/collect/model/PISCES/GetCMEMS_NRT_MERCATOR_PISCES_continuous.py'
get_pisces_nut = '/collect/model/PISCES/GetCMEMS_PISCES_Nutrients_continuous.py'
get_pisces_bio = '/collect/model/PISCES/GetCMEMS_PISCES_Bio_continuous.py'
get_pisces_car = '/collect/model/PISCES/GetCMEMS_PISCES_Car_continuous.py'
get_pisces_optics = '/collect/model/PISCES/GetCMEMS_PISCES_Optics_continuous.py'
get_pisces_pft = '/collect/model/PISCES/GetCMEMS_PISCES_Pft_continuous.py'
get_pisces_co2 = '/collect/model/PISCES/GetCMEMS_PISCES_Co2_continuous.py'
get_wind = '/collect/sat/CMEMS/GetCMEMS_NRT_WIND_HOURLY_continuous.py'
get_sst = '/collect/sat/MURSST/GetGHRSST_SST_continuous.py'
get_sss = '/collect/sat/JPL/GetREMSS_SSS_cl1_continuous.py'
get_chl_nrt = '/collect/sat/MODIS/GetMODIS_CHL_8day_NRT_continuous.py'
get_chl =  '/collect/sat/MODIS/GetMODIS_CHL_8day_cl1_continuous.py'
get_poc_nrt = '/collect/sat/MODIS/GetMODIS_POC_8day_NRT_continuous.py'
get_poc =  '/collect/sat/MODIS/GetMODIS_POC_8day_cl1_continuous.py'
get_par_nrt = '/collect/sat/MODIS/GetMODIS_PAR_daily_NRT_continuous.py'
get_par =  '/collect/sat/MODIS/GetMODIS_PAR_daily_cl1_continuous.py'
get_alt_nrt = '/collect/sat/CMEMS/GetCMEMS_NRT_ALT_SIGNAL_continuous.py'
get_alt_rep = '/collect/sat/CMEMS/GetCMEMS_REP_ALT_SIGNAL_continuous.py'
get_aod = '/collect/sat/NASA/GetLAADS_MODIS_AOD_continuous.py'

# proc_pisces = '/process/model/process_Forecast_PISCES_001_028_continuous.py'
proc_pisces_nut = '/process/model/process_PISCES_Nutrients_continuous.py'
proc_pisces_bio = '/process/model/process_PISCES_Bio_continuous.py'
proc_pisces_car = '/process/model/process_PISCES_Car_continuous.py'
proc_pisces_optics = '/process/model/process_PISCES_Optics_continuous.py'
proc_pisces_pft = '/process/model/process_PISCES_Pft_continuous.py'
proc_pisces_co2 = '/process/model/process_PISCES_Co2_continuous.py'
proc_wind = '/process/sat/CMEMS/process_CMEMS_Wind_NRT_Hourly_continuous.py'
proc_sst = '/process/sat/GHRSST/process_GHRSST_continuous.py'
proc_sss = '/process/sat/JPL/process_REMSS_SSS_cl1_continuous.py'
proc_chl_nrt = '/process/sat/MODIS/process_MODIS_CHL_8day_NRT_continuous.py'
proc_chl = '/process/sat/MODIS/process_MODIS_CHL_8day_cl1_continuous.py'
proc_poc_nrt = '/process/sat/MODIS/process_MODIS_POC_8day_NRT_continuous.py'
proc_poc = '/process/sat/MODIS/process_MODIS_POC_8day_cl1_continuous.py'
proc_par_nrt = '/process/sat/MODIS/process_MODIS_PAR_NRT_continuous.py'
proc_par = '/process/sat/MODIS/process_MODIS_PAR_cl1_continuous.py'
proc_alt_nrt = '/process/sat/CMEMS/process_CMEMS_Alitmetry_NRT_Signal_continuous.py'
proc_alt_rep = '/process/sat/CMEMS/process_CMEMS_Alitmetry_REP_Signal_continuous.py'
process_aod = '/process/sat/NASA/process_LAADS_MODIS_AOD_continuous.py'

# import sys
# stats.updateStats_Manual("1928-07-24T17:00:00.000Z", "2016-08-22T04:50:00.000Z", -66.3667,	-7.967,	89.6289,	174.335,	0,	3300, 1988337, 'tblAustralian_Phytoplankton_Database', 'Opedia', 'mariana') 
# sys.exit()

def runScript(pth):
    """Run scripts in other directories."""
    os.system(f"python {cur_dir+pth}")

def updateCIStats(tbl):
    """Pull and format stats for manual update of stats table."""
    min_time, max_time, min_lat, max_lat, min_lon, max_lon, min_depth, max_depth = cmn.getStats_TblName(tbl,'rossby')
    min_time, max_time = api.temporalRange(tbl)
    if len(min_time)==10:
        min_time=min_time+'T00:00:00'
    if len(max_time)==10:
        max_time=max_time+'T00:00:00'        
    for server in cr.server_alias_list:
        stats.updateStats_Manual(min_time+'.000Z', max_time+'.000Z', min_lat, max_lat, min_lon, max_lon, min_depth, max_depth, None, tbl, 'Opedia', server)    

if run_script:
    if datetime.date.today().weekday() == 2:
        print("It's Wednesday!")
        # wlist = [get_pisces_nut, get_pisces_bio, get_pisces_car, get_pisces_optics, get_pisces_pft, get_pisces_co2, get_wind, get_sst, get_sss, get_par_nrt, get_par,get_alt_nrt,get_alt_rep]                         
        wlist = [get_pisces_nut, get_pisces_bio, get_pisces_car, get_pisces_optics, get_pisces_pft, get_pisces_co2, get_sst, get_par_nrt, get_par,get_alt_nrt,get_alt_rep, get_aod]  # removed get_sss, get_wind                         
        with Pool(processes=8) as pool:
            pool.map(runScript,  wlist)
            pool.close() 
            pool.join()
    elif datetime.date.today().weekday() == 1:
        print("It's Tuesday!")
        # dtlist = [get_chl_nrt, get_chl, get_poc_nrt, get_poc, get_wind, get_sst, get_sss, get_par_nrt, get_par, get_alt_nrt]
        dtlist = [get_chl_nrt, get_chl, get_poc_nrt, get_poc, get_sst, get_par_nrt, get_par, get_alt_nrt, get_aod]  # removed get_sss, get_wind
        with Pool(processes=14) as pool:
            pool.map(runScript,  dtlist)
            pool.close() 
            pool.join()
    else:
        # dlist = [get_wind, get_sst, get_sss, get_par_nrt, get_par, get_alt_nrt]
        dlist = [get_sst, get_par_nrt, get_par, get_alt_nrt, get_aod]  # removed get_sss, get_wind
        with Pool(processes=8) as pool:
            pool.map(runScript,  dlist)
            pool.close() 
            pool.join()
    dl_count = DB.dbRead("SELECT table_name, count(*) cnt from tblProcess_Queue where Processed is null and error_str is null group by table_name order by table_name","Rainier")
    print(dl_count)
    
    # Yn = input("Continue with data  processing? [y/n] ")
    Yn= 'y'
    
    if Yn == 'y':
        if datetime.date.today().weekday() == 2:
            print("It's Wednesday!")
            # plist = [proc_pisces_nut, proc_pisces_bio, proc_pisces_car, proc_pisces_optics, proc_pisces_pft, proc_pisces_co2, proc_wind, proc_sst, proc_sss, proc_par_nrt, proc_par, proc_alt_nrt, proc_alt_rep]                                                             
            plist = [proc_pisces_nut, proc_pisces_bio, proc_pisces_car, proc_pisces_optics, proc_pisces_pft, proc_pisces_co2, proc_sst, proc_par_nrt, proc_par, proc_alt_nrt, proc_alt_rep, process_aod]  # removed proc_sss, proc_wind
            with Pool(processes=20) as pool:
                pool.map(runScript,  plist)
                pool.close() 
                pool.join()
        elif datetime.date.today().weekday() == 1:  
            # snlist = [proc_wind, proc_sst, proc_sss, proc_par_nrt, proc_par, proc_alt_nrt, proc_chl_nrt, proc_chl, proc_poc_nrt, proc_poc]
            snlist = [proc_sst, proc_par_nrt, proc_par, proc_alt_nrt, proc_chl_nrt, proc_chl, proc_poc_nrt, proc_poc, process_aod]  # removed proc_sss, proc_wind
            with Pool(processes=8) as pool:
                pool.map(runScript,  snlist)
                pool.close() 
                pool.join()
        else:
            # plist = [proc_wind, proc_sst, proc_sss, proc_par_nrt, proc_par, proc_alt_nrt]
            plist = [proc_sst, proc_par_nrt, proc_par, proc_alt_nrt, process_aod]  # removed proc_sss, proc_wind         
            with Pool(processes=20) as pool:
                pool.map(runScript,  plist)
                pool.close() 
                pool.join()
    else:
        sys.exit()

    dl_count = DB.dbRead("SELECT table_name, count(*) cnt from tblProcess_Queue where Processed is null and error_str is null group by table_name order by table_name","Rainier")
    time_count = 0
    while len(dl_count) > 0 and time_count <5:
        time.sleep(300)
        time_count += 1
        print(f"## Rechecking tblProcess_Queue {time_count}")
        dl_count = DB.dbRead("SELECT table_name, count(*) cnt from tblProcess_Queue where Processed is null and error_str is null group by table_name order by table_name","Rainier")
    if len(dl_count)==0:
        print("#########   Starting the Spark Cluster for Ingestion   #########")
        requests.get(cr.S3_ingest)
    else:
        rerun = input("Enter shortname to rerun (ex proc_wind, proc_sst, proc_sss, proc_par_nrt, proc_par, proc_alt_nrt) ")
        runScript(rerun)
        ### Add recheck after rerun      
    pr_count = DB.dbRead("SELECT * from tblIngestion_Queue where Ingested is null","Rainier")
    time_count = 0
    while len(pr_count)>0 and time_count <30:
        time.sleep(300)
        time_count += 1
        print(f"## Rechecking tblIngestion_Queue {time_count}")        
        pr_count = DB.dbRead("SELECT * from tblIngestion_Queue where Ingested is null","Rainier")
    if len(pr_count)==0:
        tbl = 'tblWind_NRT_hourly'
        min_time, max_time = api.temporalRange(tbl)
        qry = f"Select max(hour) hr from {tbl} where time = '{max_time}'"
        max_hour = api.query(qry)
        mx_hr = max_hour['hr'][0]
        print(max_time+f'T{mx_hr}:00:00.000Z')
        
        # Yn = input("Update wind stats? [y/n] ")
        Yn = 'y'
        
        if Yn == 'y':  
            for server in cr.server_alias_list:
                stats.updateStats_Manual(min_time+'T00:00:00.000Z', max_time+f'T{mx_hr}:00:00.000Z', -89.9375, 89.9375, -179.9375, 179.9375, None, None,None, 'tblWind_NRT_hourly', 'Opedia', server)
        if datetime.date.today().weekday() == 2:
            tlist = ['tblsss_nrt_cl1', 'tblSST_AVHRR_OI_NRT', 'tblModis_par_cl1', 'tblModis_par_nrt', 'tblAltimetry_NRT_Signal', 'tblPisces_Forecast_Nutrients', 'tblPisces_Forecast_Bio', 'tblPisces_Forecast_Car', 'tblPisces_Forecast_Optics', 'tblPisces_Forecast_Pft', 'tblPisces_Forecast_Co2']
        elif datetime.date.today().weekday() == 1:  
            tlist = ['tblsss_nrt_cl1','tblSST_AVHRR_OI_NRT','tblModis_chl_cl1','tblModis_chl_nrt','tblModis_par_cl1','tblModis_par_nrt','tblModis_poc_cl1','tblModis_poc_nrt','tblAltimetry_NRT_Signal']
        else:
            tlist = ['tblsss_nrt_cl1','tblSST_AVHRR_OI_NRT','tblModis_par_cl1','tblModis_par_nrt','tblAltimetry_NRT_Signal', 'tblModis_AOD_REP'] 
        for tbl in tlist:
            updateCIStats(tbl)
    else:
        print("### Check tblIngestion_Queue ###")
    print(f"Ingestion finished at {datetime.datetime.now()}")




# qry = 'DESCRIBE tblaltimetry_NRT_signal flag_ice'
# qry = 'DESCRIBE TABLE tblargocore_rep_apr2023'
# ss = api.query(qry)

# ## Test overwrite
# qry = "SELECT chl FROM tblPisces_Forecast_cl1 where time ='2023-02-08 12:00:00' and depth <1"
# url_qry = quote(qry, safe='')
# url = f'https://cmapdatavalidation.com/cluster/query?query={url_qry}'
# res = requests.get(url)
# res
# df_cluster, message, er, ver = returnAPI(url)

# qry = "SELECT time FROM tblArgoBGC_REP_Jun2023 limit 1"
# df = api.query(qry)

