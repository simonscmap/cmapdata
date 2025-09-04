import sys
import os
import pandas as pd
import numpy as np
import xarray as xr
import seawater as sw
from multiprocessing import Pool

from tqdm import tqdm
import glob

### Check schema before bulk import to cluster
import pyarrow.parquet as pq
import pyarrow as pa

sys.path.append("../../../../ingest")

import vault_structure as vs
import credentials as cr
import common as cmn
import data
import metadata


#####################################
### File Structure and Unzipping  ###
#####################################
#####################################

date_string = 'Jun2024'
file_string = '202406'
tbl = f'tblArgoCore_REP_{date_string}'
base_folder = f'{vs.float_dir}{tbl}/raw/'
argo_base_path = vs.float_dir + f'tblArgoBGC_REP_{date_string}/raw/{file_string}-ArgoData/dac/'
argo_rep_path = f'{vs.float_dir}{tbl}/rep/'
meta_path =  f'{vs.float_dir}{tbl}/metadata/'

daac_list = [
    "aoml",
    "bodc",
    "coriolis",
    "csio",
    "csiro",
    "incois",
    "jma",
    "kma",
    "kordi",
    "meds",
    "nmdis",
]
def unzip_and_organize_Core():
    # 16189 files
    vs.makedir(argo_base_path + "Core_subset/")
    os.chdir(argo_base_path)
    for daac in tqdm(daac_list):
        os.system(
            f"""tar -xvf {daac}_core.tar.gz -C Core_subset/ --transform='s/.*\///' --wildcards --no-anchored '*_prof*'"""
        )
unzip_and_organize_Core()

#####################################
### Cleaning and Processing Block ###
#####################################
#####################################

def rename_cols(df):
    """Rename columns in a Core dataframe 
    Args:
        df (Pandas DataFrame): Input ARGO Core DataFrame
    Returns:
        df (Pandas DataFrame): DataFrame with renamed columns
    """        
    rename_df = df.rename(
        columns={
            "JULD": "time",
            "LATITUDE": "lat",
            "LONGITUDE": "lon",
            "CYCLE_NUMBER": "cycle",
            "PLATFORM_NUMBER": "float_id",
        }
    )
    return rename_df


def core_reorder_and_add_missing_cols(df):
    """Reordered a Core dataframe and fill in any missing columns
    Args:
        df (Pandas DataFrame): Input ARGO Core DataFrame
    Returns:
        df (Pandas DataFrame): DataFrame with added columns
    """       
    missing_cols = set(core_cols) ^ set(list(df))
    df = df.reindex(columns=[*df.columns.tolist(), *missing_cols]).reindex(
        columns=core_cols
    )
    return df

def set_core_dtypes(df):
    """Define data types for all columns in a Core dataframe"""     
    for c in df.columns.to_list():
        if c in ['time','JULD_LOCATION']:
            df[c] = pd.to_datetime(df[c].astype(str), format="%Y-%m-%d %H:%M:%S").astype("datetime64[s]")           
        elif c in ["DIRECTION","DATA_MODE", "DATA_CENTRE"] or "_QC" in c:
            df[c] = df[c].astype(str)
        elif c in ["float_id", "cycle"]:
            df[c] = df[c].astype(int) 
        else: 
            df[c] = df[c].astype(np.float64)   
    return df


def get_Core_flist():
    """Get a list of all Core NetCDFs to process"""     
    Core_flist = glob.glob(argo_base_path + f"Core_subset/*.nc")
    return Core_flist

Core_flist = get_Core_flist()
len(Core_flist)


core_cols = [
    "time",
    "lat",
    "lon",
    "depth",
    "year",
    "month",
    "week",
    "dayofyear",
    "float_id",
    "cycle",
    "POSITION_QC",
    "DIRECTION",
    "DATA_MODE",
    "DATA_CENTRE",
    "JULD_QC",
    "JULD_LOCATION",
    "PROFILE_PRES_QC",
    "PROFILE_TEMP_QC",
    "PROFILE_PSAL_QC",
    "PRES",
    "PRES_QC",
    "PRES_ADJUSTED",
    "PRES_ADJUSTED_QC",
    "PRES_ADJUSTED_ERROR",
    "TEMP",
    "TEMP_QC",
    "TEMP_ADJUSTED",
    "TEMP_ADJUSTED_QC",
    "TEMP_ADJUSTED_ERROR",
    "PSAL",
    "PSAL_QC",
    "PSAL_ADJUSTED",
    "PSAL_ADJUSTED_QC",
    "PSAL_ADJUSTED_ERROR",
]


um_cols = [
            "PLATFORM_NUMBER",
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            #"DC_REFERENCE", ##Unique identifier of the profile in the data centre. not unique across data centres
            "DATA_STATE_INDICATOR", 
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "POSITIONING_SYSTEM",
            "VERTICAL_SAMPLING_SCHEME",
            "CONFIG_MISSION_NUMBER",
            "PARAMETER",
            "HISTORY_INSTITUTION",
            "HISTORY_STEP",
            "HISTORY_SOFTWARE",
            "HISTORY_SOFTWARE_RELEASE",
            "HISTORY_REFERENCE",
            "HISTORY_DATE",
            "HISTORY_ACTION",
            "HISTORY_PARAMETER",
            "HISTORY_START_PRES",
            "HISTORY_STOP_PRES",
            "HISTORY_PREVIOUS_VALUE",
            "HISTORY_QCTEST",
        ]


sci_cols = [
            "PARAMETER",
            "SCIENTIFIC_CALIB_EQUATION",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
]



def process_core(fil):
    """Clean Core netCDF, save as parquet file"""      
    # open xarray
    xdf = xr.open_dataset(fil, mask_and_scale=True)
    # convert netcdf binary
    xdf = cmn.decode_xarray_bytes(xdf)
    xum = xdf.drop([x for x in list(xdf.keys()) if x not in um_cols])
    xum = cmn.decode_xarray_bytes(xum)
    scium = xdf.drop([x for x in list(xdf.keys()) if x not in sci_cols])  
    scium = cmn.decode_xarray_bytes(scium) 
    df_sci = scium.to_dataframe().reset_index()  
    df_sci = df_sci.drop(["N_CALIB", "N_PARAM","N_PROF"], axis=1)
    df_um = pd.DataFrame(columns=['name','attr','value'])
    for x in xum.data_vars:
        if xum.data_vars[x].size > 0:
            d = {'name':[x],'attr':[xum.data_vars[x].attrs],'value':[np.unique(xum.data_vars[x])]}
            temp= pd.DataFrame(data=d)
            df_um = df_um.append(temp)
    ## Exports for unstructured metadata
    with pd.ExcelWriter(f"{meta_path}unst_meta_{os.path.basename(fil).strip('.nc')}.xlsx") as writer:
        df_um.to_excel(writer, sheet_name="unst_meta",index=False)  
        df_sci.to_excel(writer, sheet_name="sci_calib",index=False)    
    # drop ex cols from xarray
    xdf = xdf.drop(
        [
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            "DC_REFERENCE",
            "DATA_STATE_INDICATOR", 
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "POSITIONING_SYSTEM",
            "VERTICAL_SAMPLING_SCHEME",
            "CONFIG_MISSION_NUMBER",
            "PARAMETER",
            "SCIENTIFIC_CALIB_EQUATION",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
            "HISTORY_INSTITUTION",
            "HISTORY_STEP",
            "HISTORY_SOFTWARE",
            "HISTORY_SOFTWARE_RELEASE",
            "HISTORY_REFERENCE",
            "HISTORY_DATE",
            "HISTORY_ACTION",
            "HISTORY_PARAMETER",
            "HISTORY_START_PRES",
            "HISTORY_STOP_PRES",
            "HISTORY_PREVIOUS_VALUE",
            "HISTORY_QCTEST",
        ],
        errors="ignore",
    )
    for c in list(xdf.keys()):
        if c not in core_cols and c not in ['PLATFORM_NUMBER','CYCLE_NUMBER','JULD','LATITUDE','LONGITUDE','MTIME','TILT']:
            print(f"New column needs to be added: {c}")
            print(f"{os.path.basename(fil).strip('.nc')}")
            sys.exit()
    # xdf to df, reset index
    df = xdf.to_dataframe().reset_index()
    # adds depth as column, calculated by seawater library
    df.reset_index(level=0, inplace=True)
    ##### CHANGE PRES TO FLOAT64
    df['PRES']=df['PRES'].astype(np.float64)
    depth_df = pd.DataFrame(sw.dpth(df['PRES'], df['LATITUDE']))
    depth_df.reset_index(level=0, inplace=True)
    depth_df.columns=['index','depth']
    df = pd.merge(df,depth_df, how='left', on='index')
    df = df.loc[df['depth'] >= 0 ]
    # drop ex metadata cols
    df = df.drop(["N_LEVELS", "N_PROF","index"], axis=1)
    # rename ST cols
    df = rename_cols(df)
    # adds any missing columns and reorders
    df = core_reorder_and_add_missing_cols(df)
    # drops any invalid ST rows"""
    df = df.dropna(subset=["time", "lat", "lon", "depth"])
    ## Mohammad requested to keep all data in lieu of dropping unique constraint
    # df = df.drop_duplicates(subset=["time", "lat", "lon", "depth"], keep="first")
    df.fillna(999999.9, inplace=True)
    df = df.drop_duplicates(keep="first")
    df = df.replace(999999.9, np.nan)
    # sort ST cols
    df = data.sort_values(df, ["time", "lat", "lon", "depth"])
    # adds climatology day,month,week,doy columns"""
    df = data.add_day_week_month_year_clim(df)
    # removes any inf vals
    df = df.replace([np.inf, -np.inf], np.nan)
    # removes any nan string. makes string floats
    df = df.replace("nan", np.nan)
    # strips any whitespace from col values"""
    df = cmn.strip_whitespace_data(df)
    # set schema
    df = set_core_dtypes(df)
    df['cycle'] = df['cycle'].astype("Int64")   
    # removes remaining nan strings
    df = df.replace("nan", "")
    # ### for overwrite
    # try:
    #     os.remove(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet")
    # except:
    #     print(f"Not a file: {os.path.basename(fil).strip('.nc')}")    
    #transfers data to vault/
    if len(df) >0:
        df.to_parquet(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet",
        index=False)

len(Core_flist)
with Pool(processes=10) as pool:
        pool.map(process_core,  Core_flist)
        pool.close() 
        pool.join()


flist = glob.glob(argo_rep_path +"*.parquet")
len(flist)
df_stats = pd.DataFrame()
for fil in tqdm(flist):
    df = pd.read_parquet(fil)
    df = df.query('POSITION_QC!="4" & JULD_QC!="4"')
    min_time = df['time'].min()
    max_time = df['time'].max()
    df_sp = df[['lat','lon','depth']]
    df_sp_d = df_sp.describe()
    d = {'min_time':[min_time], 'max_time':[max_time], 'min_lat':[df_sp_d['lat']['min']], 'max_lat':[df_sp_d['lat']['max']], 'min_lon':[df_sp_d['lon']['min']], 'max_lon':[df_sp_d['lon']['max']], 'min_depth':[df_sp_d['depth']['min']], 'max_depth':[df_sp_d['depth']['max']], 'row_count':[len(df)]}    
    temp_df = pd.DataFrame(data=d)
    df_stats = df_stats.append(temp_df, ignore_index = True)
    del df, df_sp,df_sp_d
# min_time = df_stats['min_time'].min()
# max_time = df_stats['max_time'].max()
# min_lat = df_stats['lat'].min()
# max_lat = df_stats['lat'].max()
# min_lon = df_stats['lon'].min()
# max_lon = df_stats['lon'].max()
# min_depth = df_stats['depth'].min()
# max_depth = df_stats['depth'].max()
# row_count =  df_stats['row_count'].sum()
df_stats.to_excel(f'{vs.float_dir}{tbl}/stats/{tbl}_stats.xlsx', index=False)


### Post processing testing
def read_parquet_schema(fil):
    parquet_file = pq.ParquetFile(fil)
    sch = pa.schema(
        [f.remove_metadata() for f in parquet_file.schema_arrow])
    return sch

sch = read_parquet_schema(flist[0])
sch_list = []
for fil in tqdm(flist):
    sch2 = read_parquet_schema(fil)
    if sch2 != sch:
        sch_list.append(fil)
    df = pd.read_parquet(fil)
    if len(df)<1:
        print(fil)
    for c in df.columns.to_list():
        if '_QC' in c:
            qc_list = df[c].unique().tolist()
            if 'NaN' in qc_list or 'nan' in qc_list:
                print(fil)
                print(c)

metadata.export_script_to_vault(tbl,'float_dir',f'process/insitu/float/ARGO/process_Argo_Core_{date_string}.py','process.txt')