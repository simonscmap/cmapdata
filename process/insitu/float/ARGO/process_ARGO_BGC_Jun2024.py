import sys
import os
import pandas as pd
import numpy as np
import xarray as xr
import seawater as sw
from multiprocessing import Pool

from tqdm import tqdm
import glob
import shutil

### Check schema before bulk import to cluster
import pyarrow.parquet as pq
import pyarrow as pa

sys.path.append("../../../../ingest")

import vault_structure as vs
import metadata
import common as cmn
import data



#####################################
### File Structure and Unzipping  ###
#####################################
#####################################
date_string = 'Jun2024'
file_string = '202406'
tbl = f'tblArgoBGC_REP_{date_string}'
base_folder = f'{vs.float_dir}{tbl}/raw/'

argo_core_path = vs.float_dir + f'tblArgoBGC_REP_{date_string}/raw/'
argo_rep_path = f'{vs.float_dir}{tbl}/rep/'
meta_path = f'{vs.float_dir}{tbl}/metadata/'


flist = glob.glob(base_folder+'*.tar.gz')
shutil.unpack_archive(flist[0], base_folder)

argo_base_path = vs.float_dir + f'tblArgoBGC_REP_{date_string}/raw/{file_string}-ArgoData/dac/'


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

def unzip_and_organize_BGC():
    vs.makedir(argo_base_path + "BGC_subset/")
    os.chdir(argo_base_path)
    for daac in tqdm(daac_list):
        os.system(
            f"""tar -xvf {daac}_bgc.tar.gz -C BGC_subset/ --transform='s/.*\///' --wildcards --no-anchored '*_Sprof*'"""
        )
unzip_and_organize_BGC()

#####################################
### Cleaning and Processing Block ###
#####################################
#####################################


def reorder_bgc_data(df):
    """Reordered a BGC dataframe to move ST coodinates first
    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame
    Returns:
        df (Pandas DataFrame): Reordered DataFrame
    """
    st_col_list = [
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
    ]
    st_cols = df[st_col_list]
    non_st_cols = df.drop(st_col_list, axis=1)
    reorder_df = pd.concat([st_cols, non_st_cols], axis=1, sort=False)
    return reorder_df

def rename_cols(df):
    """Rename columns in a BGC dataframe 
    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame
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


def bgc_reorder_and_add_missing_cols(df):
    """Reordered a BGC dataframe and fill in any missing columns
    Args:
        df (Pandas DataFrame): Input ARGO BGC DataFrame
    Returns:
        df (Pandas DataFrame): DataFrame with added columns
    """        
    missing_cols = set(bgc_cols) ^ set(list(df))
    df = df.reindex(columns=[*df.columns.tolist(), *missing_cols]).reindex(
        columns=bgc_cols
    )
    return df

def set_bgc_dtypes(df):
    """Define data types for all columns in a BGC dataframe"""      
    for c in df.columns.to_list():
        if c in ['time','JULD_LOCATION']:
            df[c] = pd.to_datetime(df[c].astype(str), format="%Y-%m-%d %H:%M:%S").astype("datetime64[s]")         
        elif c in ["DIRECTION", "DATA_CENTRE"] or "_QC" in c:
            df[c] = df[c].astype(str)
        elif c in ["float_id", "cycle"]:##
            df[c] = df[c].astype('int') 
        else: 
            df[c] = df[c].astype(np.float64)   
    return df

def get_BGC_flist():
    """Get a list of all BGC NetCDFs to process""" 
    BGC_flist = glob.glob(argo_base_path + "BGC_subset/*.nc")
    return BGC_flist


BGC_flist = get_BGC_flist()
len(BGC_flist)


bgc_cols = [
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
    "JULD_LOCATION",
    "DATA_CENTRE",
    "DIRECTION",
    "BBP470",
    "BBP470_ADJUSTED",
    "BBP470_ADJUSTED_ERROR",
    "BBP470_ADJUSTED_QC",
    "BBP470_QC",
    "BBP470_dPRES",
    "BBP532",
    "BBP532_ADJUSTED",
    "BBP532_ADJUSTED_ERROR",
    "BBP532_ADJUSTED_QC",
    "BBP532_QC",
    "BBP532_dPRES",
    "BBP700",
    "BBP700_2",
    "BBP700_2_ADJUSTED",
    "BBP700_2_ADJUSTED_ERROR",
    "BBP700_2_ADJUSTED_QC",
    "BBP700_2_QC",
    "BBP700_2_dPRES",
    "BBP700_ADJUSTED",
    "BBP700_ADJUSTED_ERROR",
    "BBP700_ADJUSTED_QC",
    "BBP700_QC",
    "BBP700_dPRES",
    "BISULFIDE",
    "BISULFIDE_ADJUSTED",
    "BISULFIDE_ADJUSTED_ERROR",
    "BISULFIDE_ADJUSTED_QC",
    "BISULFIDE_QC",
    "BISULFIDE_dPRES",
    "CNDC",
    "CNDC_ADJUSTED",
    "CNDC_ADJUSTED_ERROR",
    "CNDC_ADJUSTED_QC",
    "CNDC_QC",
    "CNDC_dPRES",
    "CDOM",
    "CDOM_ADJUSTED",
    "CDOM_ADJUSTED_ERROR",
    "CDOM_ADJUSTED_QC",
    "CDOM_QC",
    "CDOM_dPRES",
    "CHLA",
    "CHLA_ADJUSTED",
    "CHLA_ADJUSTED_ERROR",
    "CHLA_ADJUSTED_QC",
    "CHLA_QC",
    "CHLA_dPRES",
        "CHLA_FLUORESCENCE",
        "CHLA_FLUORESCENCE_ADJUSTED",
        "CHLA_FLUORESCENCE_ADJUSTED_ERROR",
        "CHLA_FLUORESCENCE_ADJUSTED_QC",
        "CHLA_FLUORESCENCE_QC",
        "CHLA_FLUORESCENCE_dPRES",
    "CP660",
    "CP660_ADJUSTED",
    "CP660_ADJUSTED_ERROR",
    "CP660_ADJUSTED_QC",
    "CP660_QC",
    "CP660_dPRES",
    "DOWNWELLING_PAR",
    "DOWNWELLING_PAR_ADJUSTED",
    "DOWNWELLING_PAR_ADJUSTED_ERROR",
    "DOWNWELLING_PAR_ADJUSTED_QC",
    "DOWNWELLING_PAR_QC",
    "DOWNWELLING_PAR_dPRES",
    "DOWN_IRRADIANCE380",
    "DOWN_IRRADIANCE380_ADJUSTED",
    "DOWN_IRRADIANCE380_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE380_ADJUSTED_QC",
    "DOWN_IRRADIANCE380_QC",
    "DOWN_IRRADIANCE380_dPRES",
    "DOWN_IRRADIANCE412",
    "DOWN_IRRADIANCE412_ADJUSTED",
    "DOWN_IRRADIANCE412_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE412_ADJUSTED_QC",
    "DOWN_IRRADIANCE412_QC",
    "DOWN_IRRADIANCE412_dPRES",
    "DOWN_IRRADIANCE443",
    "DOWN_IRRADIANCE443_ADJUSTED",
    "DOWN_IRRADIANCE443_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE443_ADJUSTED_QC",
    "DOWN_IRRADIANCE443_QC",
    "DOWN_IRRADIANCE443_dPRES",
    "DOWN_IRRADIANCE490",
    "DOWN_IRRADIANCE490_ADJUSTED",
    "DOWN_IRRADIANCE490_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE490_ADJUSTED_QC",
    "DOWN_IRRADIANCE490_QC",
    "DOWN_IRRADIANCE490_dPRES",
    "DOWN_IRRADIANCE555",
    "DOWN_IRRADIANCE555_ADJUSTED",
    "DOWN_IRRADIANCE555_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE555_ADJUSTED_QC",
    "DOWN_IRRADIANCE555_QC",
    "DOWN_IRRADIANCE555_dPRES",
    "DOWN_IRRADIANCE670",
    "DOWN_IRRADIANCE670_ADJUSTED",
    "DOWN_IRRADIANCE670_ADJUSTED_ERROR",
    "DOWN_IRRADIANCE670_ADJUSTED_QC",
    "DOWN_IRRADIANCE670_QC",
    "DOWN_IRRADIANCE670_dPRES",
    "DOWN_IRRADIANCE665",
        "DOWN_IRRADIANCE665_ADJUSTED",
        "DOWN_IRRADIANCE665_ADJUSTED_ERROR",
        "DOWN_IRRADIANCE665_ADJUSTED_QC",
        "DOWN_IRRADIANCE665_QC",
        "DOWN_IRRADIANCE665_dPRES",
    "DOXY",
    "DOXY_ADJUSTED",
    "DOXY_ADJUSTED_ERROR",
    "DOXY_ADJUSTED_QC",
    "DOXY_QC",
    "DOXY_dPRES",
    "DOXY2",
    "DOXY2_ADJUSTED",
    "DOXY2_ADJUSTED_ERROR",
    "DOXY2_ADJUSTED_QC",
    "DOXY2_QC",
    "DOXY2_dPRES",
    "DOXY3",
    "DOXY3_ADJUSTED",
    "DOXY3_ADJUSTED_ERROR",
    "DOXY3_ADJUSTED_QC",
    "DOXY3_QC",
    "DOXY3_dPRES",
    "JULD_QC",
    "NITRATE",
    "NITRATE_ADJUSTED",
    "NITRATE_ADJUSTED_ERROR",
    "NITRATE_ADJUSTED_QC",
    "NITRATE_QC",
    "NITRATE_dPRES",
    "PH_IN_SITU_TOTAL",
    "PH_IN_SITU_TOTAL_ADJUSTED",
    "PH_IN_SITU_TOTAL_ADJUSTED_ERROR",
    "PH_IN_SITU_TOTAL_ADJUSTED_QC",
    "PH_IN_SITU_TOTAL_QC",
    "PH_IN_SITU_TOTAL_dPRES",
    "POSITION_QC",
    "PRES",
    "PRES_ADJUSTED",
    "PRES_ADJUSTED_ERROR",
    "PRES_ADJUSTED_QC",
    "PRES_QC",
    "PROFILE_BBP470_QC",
    "PROFILE_BBP532_QC",
    "PROFILE_BBP700_2_QC",
    "PROFILE_BBP700_QC",
    "PROFILE_BISULFIDE_QC",
    "PROFILE_CNDC_QC",
    "PROFILE_CDOM_QC",
    "PROFILE_CHLA_QC",
        "PROFILE_CHLA_FLUORESCENCE_QC",
    "PROFILE_CP660_QC",
    "PROFILE_DOWNWELLING_PAR_QC",
    "PROFILE_DOWN_IRRADIANCE380_QC",
    "PROFILE_DOWN_IRRADIANCE412_QC",
    "PROFILE_DOWN_IRRADIANCE443_QC",
    "PROFILE_DOWN_IRRADIANCE490_QC",
    "PROFILE_DOWN_IRRADIANCE555_QC",
    "PROFILE_DOWN_IRRADIANCE665_QC",
    "PROFILE_DOWN_IRRADIANCE670_QC",
    "PROFILE_DOXY_QC",
    "PROFILE_DOXY2_QC",
    "PROFILE_DOXY3_QC",
    "PROFILE_NITRATE_QC",
    "PROFILE_PH_IN_SITU_TOTAL_QC",
    "PROFILE_PRES_QC",
    "PROFILE_PSAL_QC",
    "PROFILE_TEMP_QC",
    "PROFILE_TURBIDITY_QC",
    "PROFILE_UP_RADIANCE412_QC",
    "PROFILE_UP_RADIANCE443_QC",
    "PROFILE_UP_RADIANCE490_QC",
    "PROFILE_UP_RADIANCE555_QC",
    "PSAL",
    "PSAL_ADJUSTED",
    "PSAL_ADJUSTED_ERROR",
    "PSAL_ADJUSTED_QC",
    "PSAL_QC",
    "PSAL_dPRES",
    "TEMP",
    "TEMP_ADJUSTED",
    "TEMP_ADJUSTED_ERROR",
    "TEMP_ADJUSTED_QC",
    "TEMP_QC",
    "TEMP_dPRES",
        "TEMP_CNDC",
        "TEMP_CNDC_ADJUSTED",
        "TEMP_CNDC_ADJUSTED_ERROR",
        "TEMP_CNDC_ADJUSTED_QC",
        "TEMP_CNDC_QC",
        "TEMP_CNDC_dPRES",   
    "TURBIDITY",
    "TURBIDITY_ADJUSTED",
    "TURBIDITY_ADJUSTED_ERROR",
    "TURBIDITY_ADJUSTED_QC",
    "TURBIDITY_QC",
    "TURBIDITY_dPRES",
    "UP_RADIANCE412",
    "UP_RADIANCE412_ADJUSTED",
    "UP_RADIANCE412_ADJUSTED_ERROR",
    "UP_RADIANCE412_ADJUSTED_QC",
    "UP_RADIANCE412_QC",
    "UP_RADIANCE412_dPRES",
    "UP_RADIANCE443",
    "UP_RADIANCE443_ADJUSTED",
    "UP_RADIANCE443_ADJUSTED_ERROR",
    "UP_RADIANCE443_ADJUSTED_QC",
    "UP_RADIANCE443_QC",
    "UP_RADIANCE443_dPRES",
    "UP_RADIANCE490",
    "UP_RADIANCE490_ADJUSTED",
    "UP_RADIANCE490_ADJUSTED_ERROR",
    "UP_RADIANCE490_ADJUSTED_QC",
    "UP_RADIANCE490_QC",
    "UP_RADIANCE490_dPRES",
    "UP_RADIANCE555",
    "UP_RADIANCE555_ADJUSTED",
    "UP_RADIANCE555_ADJUSTED_ERROR",
    "UP_RADIANCE555_ADJUSTED_QC",
    "UP_RADIANCE555_QC",
    "UP_RADIANCE555_dPRES",
]

um_cols = [
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            "PARAMETER_DATA_MODE",
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "POSITIONING_SYSTEM",
            "CONFIG_MISSION_NUMBER",
]

sci_cols = [
            "PARAMETER",
            "SCIENTIFIC_CALIB_EQUATION",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
]


def clean_bgc(fil):
    """Clean BGC netCDF, save as parquet file"""  
    # open xarray
    # fil_done = os.path.isfile(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet") 
    # if fil_done:
    #     print(f'{fil} already done')
    #     return
    # else:
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
    xdf = xdf.drop(
        [
            "N_CALIB",
            "N_LEVELS",
            "N_PARAM",
            "N_PROF",
            "DATA_TYPE",
            "FORMAT_VERSION",
            "HANDBOOK_VERSION",
            "REFERENCE_DATE_TIME",
            "DATE_CREATION",
            "DATE_UPDATE",
            "PROJECT_NAME",
            "PI_NAME",
            "STATION_PARAMETERS",
            "PARAMETER_DATA_MODE",
            "PLATFORM_TYPE",
            "FLOAT_SERIAL_NO",
            "FIRMWARE_VERSION",
            "WMO_INST_TYPE",
            "POSITIONING_SYSTEM",
            "CONFIG_MISSION_NUMBER",
            "PARAMETER",
            "SCIENTIFIC_CALIB_COEFFICIENT",
            "SCIENTIFIC_CALIB_COMMENT",
            "SCIENTIFIC_CALIB_DATE",
            "SCIENTIFIC_CALIB_EQUATION",
        ],
        errors="ignore",
    )
    for c in list(xdf.keys()):
        if c not in bgc_cols and c not in ['PLATFORM_NUMBER','CYCLE_NUMBER','JULD','LATITUDE','LONGITUDE','MTIME','TILT']:
            print(f"New column needs to be added: {c}")
            print(f"{os.path.basename(fil).strip('.nc')}")
            sys.exit()
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
    # df["depth"] = df["PRES"]
    # drop ex metadata cols
    df = df.drop(["N_LEVELS", "N_PROF","index"], axis=1)
    # rename ST cols
    df = rename_cols(df)
    # drops any invalid ST rows"""
    df = df.dropna(subset=["time", "lat", "lon", "depth"])
    # sort ST cols
    df = data.sort_values(df, ["time", "lat", "lon", "depth"])
    # adds climatology day,month,week,doy columns"""
    df = data.add_day_week_month_year_clim(df)
    # adds any missing columns and reorders
    df = bgc_reorder_and_add_missing_cols(df)
    # removes any inf vals
    df = df.replace([np.inf, -np.inf], np.nan)
    # strips any whitespace from col values"""
    df = cmn.strip_whitespace_data(df)
    # removes any nan string
    df = df.replace("nan", np.nan)      
    # set schema
    df = set_bgc_dtypes(df)    
    # removes any nan string remaining in _QC columns
    df = df.replace("nan", "")  
    # ### for overwrite
    # try:
    #     os.remove(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet")
    # except:
    #     print(f"Not a file: {os.path.basename(fil).strip('.nc')}")          
    #transfers data to vault
    if len(df) >0:
        df.to_parquet(f"{argo_rep_path}{tbl}_{os.path.basename(fil).strip('.nc')}.parquet",
            index=False)

with Pool(processes=20) as pool:
        pool.map(clean_bgc,  BGC_flist)
        pool.close() 
        pool.join()

## Gather stats for ingestion
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
df_stats.to_excel(f'{vs.float_dir}{tbl}/stats/{tbl}_stats.xlsx', index=False)


### Post processing testing
def read_parquet_schema(fil):
    """Read schema from parquet file"""  
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

metadata.export_script_to_vault(tbl,'float_dir',f'process/insitu/float/ARGO/process_Argo_BGC_{date_string}.py','process.txt')

