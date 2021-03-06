"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - transfer - dataset template moveing and splitting.
"""


import os
import glob
import shutil
import pandas as pd
import requests
from tqdm import tqdm
import dropbox
import credentials as cr
import vault_structure as vs


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

    meta_fname = vs.staging + "metadata/" + cruise_name + "_cruise_metadata.csv"
    traj_fname = vs.staging + "metadata/" + cruise_name + "_cruise_trajectory.csv"

    shutil.copyfile(meta_fname, meta_tree + cruise_name + "_cruise_metadata.csv")
    shutil.copyfile(traj_fname, traj_tree + cruise_name + "_cruise_trajectory.csv")

    if remove_file_flag == True:
        os.remove(meta_fname)
        os.remove(traj_fname)

    print("cruise trajectory and metadata transferred from staging to vault.")


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
    nrt_tree, rep_tree, metadata_tree, stats_tree, doc_tree, code_tree = vs.leafStruc(
        branch + tableName
    )
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    clear_directory(rep_tree)
    clear_directory(nrt_tree)
    clear_directory(metadata_tree)

    data_fname = vs.staging + "data/" + base_filename + "_data.csv"
    dataset_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_dataset_metadata.csv"
    )
    vars_metadata_fname = (
        vs.staging + "metadata/" + base_filename + "_vars_metadata.csv"
    )
    if skip_data_flag == False:
        if process_level.lower() == "nrt":
            shutil.copyfile(data_fname, nrt_tree + base_filename + "_data.csv")
        else:
            shutil.copyfile(data_fname, rep_tree + base_filename + "_data.csv")

    shutil.copyfile(
        dataset_metadata_fname, metadata_tree + base_filename + "_dataset_metadata.csv"
    )
    shutil.copyfile(
        vars_metadata_fname, metadata_tree + base_filename + "_vars_metadata.csv"
    )

    if remove_file_flag == True:
        os.remove(dataset_metadata_fname)
        os.remove(vars_metadata_fname)
        if skip_data_flag == False:
            os.remove(data_fname)


def cruise_file_split(filename, cruise_name):
    """Splits combined cruise template file into cruise metadata and cruise trajectory

    Args:
        filename (string): path name of file.
    """
    base_filename = os.path.splitext(os.path.basename(filename))[0]

    cruise_metadata = pd.read_excel(
        vs.combined + filename, sheet_name="cruise_metadata"
    )
    cruise_trajectory = pd.read_excel(
        vs.combined + filename, sheet_name="cruise_trajectory"
    )
    cruise_metadata.to_csv(
        vs.metadata + cruise_name + "_cruise_metadata.csv", sep=",", index=False
    )
    cruise_trajectory.to_csv(
        vs.metadata + cruise_name + "_cruise_trajectory.csv", sep=",", index=False
    )


def single_file_split(filename, data_missing_flag):
    """

    Splits an excel file containing data, dataset_metadata and vars_metadata sheets
    into three seperate files in the staging file strucutre.
    If additional metadata filename is provided, data is split.

    Parameters
    ----------
    filename : string
        Filename and extension to be split.
    """

    base_filename = os.path.splitext(os.path.basename(filename))[0]

    dataset_metadata_df = pd.read_excel(
        vs.combined + filename, sheet_name="dataset_meta_data"
    )
    vars_metadata_df = pd.read_excel(
        vs.combined + filename, sheet_name="vars_meta_data"
    )

    dataset_metadata_df.to_csv(
        vs.metadata + base_filename + "_dataset_metadata.csv", sep=",", index=False
    )
    vars_metadata_df.to_csv(
        vs.metadata + base_filename + "_vars_metadata.csv", sep=",", index=False
    )
    if data_missing_flag == False:
        data_df = pd.read_excel(vs.combined + filename, sheet_name="data")
        data_df.to_csv(vs.data + base_filename + "_data.csv", sep=",", index=False)


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
    dbx = dropbox.Dropbox(cr.dropbox_api_key, timeout=900)
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
