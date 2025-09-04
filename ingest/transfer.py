"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - transfer - dataset template moveing and splitting.
"""


import os
import sys
import glob
import shutil
import pandas as pd
import requests
from tqdm import tqdm
import dropbox

import credentials as cr
import vault_structure as vs
import common as cm


def requests_Download(download_str, filename, path):
    """downloads url with requests module"""
    r = requests.get(download_str, stream=True)
    with open(path + filename, "wb") as f:
        f.write(r.content)


def clear_directory(directory):
    """Removes any files in directory"""
    try:
        flist = glob.glob(directory + "*")
        [os.remove(fil) for fil in flist]
    except:
        pass


def Zenodo_DOI_Formatter(DOI, filename):
    """Formats DOI into zenodod format"""
    doi_record = DOI.split("zenodo.")[1]
    doi_download_str = (
        "https://zenodo.org/record/{doi_record}}/files/{filename}}?download=1".format(
            doi_record=doi_record, filename=filename
        )
    )
    return doi_download_str


def cruise_staging_to_vault(cruise_name, remove_file_flag):
    """Transfer cruise files from staging to vault

    Args:
        filename : string
            Filename and extension to be transfered.
        cruise_name : string
            UNOLS cruise_name
        remove_file_flag : bool, default True, optional
            Flag option for removing input file from staging
    """
    meta_tree, traj_tree = vs.cruise_leaf_structure(vs.r2r_cruise + cruise_name)
    clear_directory(meta_tree)
    clear_directory(traj_tree)

    meta_fname = vs.staging + "metadata/" + cruise_name + "_cruise_metadata.parquet"
    traj_fname = vs.staging + "metadata/" + cruise_name + "_cruise_trajectory.parquet"

    shutil.copyfile(meta_fname, meta_tree + cruise_name + "_cruise_metadata.parquet")
    shutil.copyfile(traj_fname, traj_tree + cruise_name + "_cruise_trajectory.parquet")

    if remove_file_flag == True:
        os.remove(meta_fname)
        os.remove(traj_fname)

    print("cruise trajectory and metadata transferred from staging to vault.")


def validator_to_vault(filename, branch, tableName, data_missing_flag):
    """
    Transfers a file from validator submission to vault.

    Parameters
    ----------
    filename : string
        Filename and extension to be transfered.
    branch : string
        Vault organization path: ex: vs.cruise
    tableName : string
        SQL tableName
    data_missing_flag: boolean
        True if dataless ingestion
    """
    vs.leafStruc(branch + tableName)
    base_filename = os.path.splitext(os.path.basename(filename))[0]
    ## If edits are made in validator, additional copies of excel file are saved in Apps directory
    # validator_file = cm.getLast_file_download(base_filename, 'app_data', False)
    final_file = glob.glob(f'{vs.app_data}{base_filename}/final/*')
    if len(final_file) ==1:
        validator_file = final_file[0]
    ## This won't continue if there's nothing in the final_file list
    else:
        print(f"Looking in the final folde: {vs.app_data}{base_filename}/final/")
        print(f"Found these files in the final folders:\n{final_file}")
        contYN = input(f"Multiple files in final folder. Do you want to ingest: {final_file[0]}? " + " ?  [yes/no]: ")
        if contYN.lower() == "yes":
            validator_file = final_file[0]
        else:
            sys.exit()
    if data_missing_flag == False:
        vault_path = branch+tableName+'/raw/'
        shutil.copyfile(validator_file, vault_path + base_filename +'.xlsx')
    vault_path = branch+tableName+'/metadata/'
    shutil.copyfile(validator_file, vault_path + base_filename +'.xlsx')




def staging_to_vault(
    filename,
    branch,
    tableName,
    remove_file_flag=True,
    skip_data_flag=False,
    process_level="REP",
):

    """
    Transfers a file from staging to vault rep or nrt.

    Parameters
    ----------
    filename : string
        Filename and extension to be transfered.
    branch : string
        Vault organization path: ex: vs.cruise
    tableName : string
        SQL tableName
    remove_file_flag : bool, default True, optional
        Flag option for removing input file from staging
    process_level : str, default REP, optional
        Place the data in the REP or the NRT


    """
    nrt_tree, rep_tree, metadata_tree, stats_tree, doc_tree, code_tree, raw_tree = vs.leafStruc(
        branch + tableName
    )
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    clear_directory(rep_tree)
    clear_directory(nrt_tree)
    #clear_directory(metadata_tree)

    data_fname = vs.staging + "data/" + base_filename + "_data.parquet"
    dataset_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_dataset_metadata.parquet"
    )
    vars_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_vars_metadata.parquet"
    )
    if skip_data_flag == False:
        if process_level.lower() == "nrt":
            shutil.copyfile(data_fname, nrt_tree + base_filename + "_data.parquet")
        else:
            shutil.copyfile(data_fname, rep_tree + base_filename + "_data.parquet")

    shutil.copyfile(
        dataset_metadata_fname, metadata_tree + base_filename + "_dataset_metadata.parquet"
    )
    shutil.copyfile(
        vars_metadata_fname, metadata_tree + base_filename + "_vars_metadata.parquet"
    )

    if remove_file_flag == True:
        os.remove(dataset_metadata_fname)
        os.remove(vars_metadata_fname)
        if skip_data_flag == False:
            os.remove(data_fname)


def cruise_file_split(filename, cruise_name, in_vault):
    """Splits combined cruise template file into cruise metadata and cruise trajectory

    Args:
        filename (string): path name of file.
    """
        #meta, traj, code, raw
    meta_tree, traj_tree, code_tree, raw_tree = vs.cruise_leaf_structure(vs.r2r_cruise + cruise_name)

    if not in_vault:    
        cruise_metadata = pd.read_excel(
            vs.combined + filename, sheet_name="cruise_metadata"
        )
        cruise_trajectory = pd.read_excel(
            vs.combined + filename, sheet_name="cruise_trajectory"
        )

        shutil.copyfile(vs.combined + filename, raw_tree + filename)
    else:
        cruise_metadata = pd.read_excel(
            raw_tree + filename, sheet_name="cruise_metadata"
        )
        cruise_trajectory = pd.read_excel(
            raw_tree + filename, sheet_name="cruise_trajectory"
        )

    cruise_metadata.to_parquet(
        meta_tree + cruise_name + "_cruise_metadata.parquet"
    )
    cruise_trajectory.to_parquet(
        traj_tree + cruise_name + "_cruise_trajectory.parquet"
    )


def single_file_split(filename, branch, tableName, data_missing_flag):
    """

    Splits an excel file containing data, dataset_metadata and vars_metadata sheets
    into three separate files in the staging file strucutre.
    If additional metadata filename is provided, data is split.

    Parameters
    ----------
    filename : string
        Filename and extension to be split.
    """
    base_filename = os.path.splitext(os.path.basename(filename))[0]
    base_path = cm.vault_struct_retrieval(branch)+tableName

    dataset_metadata_df = pd.read_excel(
        base_path +'/metadata/'+ filename, sheet_name="dataset_meta_data"
    )
    dataset_metadata_df.columns = dataset_metadata_df.columns.str.lower()
    # dataset_metadata_df.replace({'\'': '\'\''}, regex=True, inplace = True)
    if (len(dataset_metadata_df[dataset_metadata_df.apply(lambda r: r.str.contains('"', case=False).any(), axis=1)] ) >0) and (len(dataset_metadata_df[dataset_metadata_df.apply(lambda r: r.str.contains("'", case=False).any(), axis=1)] ) >0) :
            dataset_metadata_df.replace({'"': '\'\''}, regex=True, inplace = True)
    dataset_metadata_df.replace({"'": "CHAR(39)"}, regex=True, inplace = True)
    vars_metadata_df = pd.read_excel(
        base_path +'/metadata/'+ filename, sheet_name="vars_meta_data"
    )
    vars_metadata_df.columns = vars_metadata_df.columns.str.lower()
    # vars_metadata_df.replace({'\'': '\'\''}, regex=True, inplace = True)
    if (len(vars_metadata_df[vars_metadata_df.apply(lambda r: r.str.contains('"', case=False).any(), axis=1)] ) >0) and (len(vars_metadata_df[vars_metadata_df.apply(lambda r: r.str.contains("'", case=False).any(), axis=1)] ) >0) :
            vars_metadata_df.replace({'"': '\'\''}, regex=True, inplace = True)
    vars_metadata_df.replace({"'": "CHAR(39)"}, regex=True, inplace = True)
    dataset_metadata_df.to_parquet(
         base_path +'/metadata/' + tableName + "_dataset_metadata.parquet", index=False
    )
    vars_metadata_df.astype({'var_unit': str}).to_parquet(
         base_path +'/metadata/' + tableName + "_vars_metadata.parquet", index=False
    )    
    if data_missing_flag == False:
        data_df = pd.read_excel(base_path +'/raw/' + filename, sheet_name="data")
        data_df.replace({'\'': '\'\''}, regex=True, inplace = True)
        if (len(data_df[data_df.apply(lambda r: r.str.contains('"', case=False).any(), axis=1)] ) >0) and (len(data_df[data_df.apply(lambda r: r.str.contains("'", case=False).any(), axis=1)] ) >0) :
            data_df.replace({'"': '\'\''}, regex=True, inplace = True)
        # data_df.to_csv(base_path+'/raw/' + base_filename + "_data.csv", sep=",", index=False)
        ## Handling mixed column types when exporting to parquet
        for col in data_df.select_dtypes([object]):
            data_df[col] = data_df[col].astype(str)
        data_df.to_parquet(base_path+'/rep/' + tableName + "_data.parquet", index=False)


def remove_data_metadata_fnames_staging(staging_sep_flag="combined"):
    if staging_sep_flag == "combined":
        for base_filename in os.listdir(vs.combined):
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("data", ""),
            )
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("metadata", ""),
            )
            os.rename(
                vs.combined + base_filename,
                vs.combined + base_filename.replace("meta_data", ""),
            )
    else:
        for base_filename in os.listdir(vs.data):
            os.rename(
                vs.data + base_filename, vs.data + base_filename.replace("data", "")
            )
        for base_filename in os.listdir(vs.metadata):
            os.rename(
                vs.metadata + base_filename,
                vs.metadata + base_filename.replace("metadata", ""),
            )


# def df_to_parquet(df, filename, branch, tableName, vault_level):
#     """
#     Transfers dataframe as parquet file to vault backup
    
#     Parameters
#     ----------
#     df (DataFrame
#     filename : string
#         Filename and extension to be split.
#     """
#      ## Export copy of data ingested to DB as parquet file into vault
#     vault_path = getattr(vs,branch)+tableName
#     df.to_parquet(
#         vault_path
#         + "/" + vault_level + "/"
#         + filename
#         + ".parquet"
#     )
#     print('Parquet saved')

def dropbox_file_transfer(input_file_path, output_file_path):

    """
    Transfers a file to dropbox using the dropbox v2 python api

    Parameters
    ----------
    input_file_path : string
        Input filepath, filename and extension to be transfered.
    output_file_path : string
        Output filepath, filename and extension to be transfered.
    """
    # dbx = dropbox.Dropbox(cr.dropbox_api_key_web, timeout=900)
    dbx = dropbox.Dropbox(
            app_key = cr.dropbox_vault_key,
            app_secret = cr.dropbox_vault_secret,
            oauth2_refresh_token = cr.dropbox_vault_refresh_token
        )
    chunk_size = 1024 * 1024
    with open(input_file_path, "rb") as f:
        file_size = os.path.getsize(input_file_path)
        if file_size <= chunk_size:
            dbx.files_upload(
                f.read(), output_file_path, mode=dropbox.files.WriteMode.overwrite
            )
        else:
            with tqdm(total=file_size, desc="%transfer") as pbar:
                upload_session_start_result = dbx.files_upload_session_start(
                    f.read(chunk_size)
                )
                pbar.update(chunk_size)
                cursor = dropbox.files.UploadSessionCursor(
                    session_id=upload_session_start_result.session_id,
                    offset=f.tell(),
                )
                commit = dropbox.files.CommitInfo(path=output_file_path)

                while f.tell() < file_size:
                    if (file_size - f.tell()) <= chunk_size:
                        dbx.files_upload_session_finish(
                            f.read(chunk_size), cursor, commit
                        )

                    else:
                        dbx.files_upload_session_append(
                            f.read(chunk_size),
                            cursor.session_id,
                            cursor.offset,
                        )
                        cursor.offset = f.tell()
                    pbar.update(chunk_size)


def dropbox_file_sync(input_file_path, output_file_path):

    """
    Transfers a file from dropbox using the dropbox v2 python api

    Parameters
    ----------
    input_file_path : string
        Input filepath, filename and extension to be transfered.
    output_file_path : string
        Output filepath, filename and extension to be transfered.
    """
    dbx = dropbox.Dropbox(
            app_key = cr.dropbox_vault_key,
            app_secret = cr.dropbox_vault_secret,
            oauth2_refresh_token = cr.dropbox_vault_refresh_token
        )
    # dbx = dropbox.Dropbox(cr.dropbox_api_key, timeout=900)
    chunk_size = 1024 * 1024
    with open(input_file_path, "rb") as f:
        file_size = os.path.getsize(input_file_path)
        if file_size <= chunk_size:
            dbx.files_download(
                f.read(), output_file_path, mode=dropbox.files.WriteMode.overwrite
            )

def dropbox_validator_sync(ingest_excel):

    """
    Transfers all files from dropbox validator for a dataset using the dropbox v2 python api

    Parameters
    ----------
    folder_name : string
        Input filepath, filename and extension to be transfered.
    """
    
    folder_name = ingest_excel.rsplit('.',1)[0]
    input_folder_path = f'/{folder_name}/final'
    output_folder_path = vs.app_data + folder_name +'/final'
    dbx = dropbox.Dropbox(
            app_key = cr.dropbox_vault_key,
            app_secret = cr.dropbox_vault_secret,
            oauth2_refresh_token = cr.dropbox_vault_refresh_token
        )
    # dbx = dropbox.Dropbox(cr.dropbox_api_key_web, timeout=900)
    vs.makedir(vs.app_data+folder_name)
    vs.makedir(vs.app_data+folder_name+'/final')
    try:
        dlist = dbx.files_list_folder(path=input_folder_path)
        for f in dlist.entries:
            print(f.name)
            dbx.files_download_to_file(output_folder_path+'/'+f.name, f.path_lower)
    except:
        print(f'No validator folder for {folder_name}')            
 


def convert_csv_meta_to_parquet(tableName,branch,remove_file_flag):
    """
    Converts existing csv metadata files in vault to parquet files
    Returns flag = 0 if no csv present, flag = 2 if both dataset and vars metadata converted
    Parameters
    ----------
    tableName : string
        SQL tableName
    branch : string
        Vault organization path: ex: cruise, satellite
    remove_file_flag: bit
        Set true to delete csv file after converting to parquet
    """
    tbl = tableName
    mk = branch
    directory = getattr(vs,mk) + tbl
    meta = os.path.join(directory, 'metadata')
    flist = glob.glob(meta+'/*metadata.csv')
    flag = 0
    if len(flist) < 2:
        print(f'### No existing csv files for {tableName} metadata')
    else:
        for fil in flist:
            if '_dataset_metadata.csv' in fil:
                df_ds = pd.read_csv(fil)
                df_ds.to_parquet(os.path.join(meta,f'{tbl}_dataset_metadata.parquet'))
                print(f'Dataset metadata exported for {tbl}')
                if remove_file_flag == True:
                    os.remove(fil)
                flag += 1
            if '_vars_metadata.csv' in fil:
                df_vs = pd.read_csv(fil)
                df_vs.to_parquet(os.path.join(meta,f'{tbl}_vars_metadata.parquet'))
                print(f'Variable metadata exported for {tbl}')
                if remove_file_flag == True:
                    os.remove(fil)
                flag += 1
    return flag
            

