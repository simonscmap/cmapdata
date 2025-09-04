"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-23-2021

cmapdata - vault_structure - generalized vault structure. Adapted from opedia.
"""


import os
from pathlib import Path


def makedir(directory):
    """Creates directory if does not exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    return

def leafStruc(base):
    """Creates leaf level structure for datasets"""
    makedir(base)
    if base[-1] != "/":
        base = base + "/"
    nrt = base + "nrt/"
    makedir(nrt)
    rep = base + "rep/"
    makedir(rep)
    metadata = base + "metadata/"
    makedir(metadata)
    stats = base + "stats/"
    makedir(stats)
    doc = base + "doc/"
    makedir(doc)
    code = base + "code/"
    makedir(code)
    raw = base + "raw/"
    makedir(raw)
    return nrt, rep, metadata, stats, doc, code, raw

def cruise_leaf_structure(base):
    """Creates leaf level structure for cruises"""
    makedir(base)
    if base[-1] != "/":
        base = base + "/"
    traj = base + "trajectory/"
    makedir(traj)
    meta = base + "metadata/"
    makedir(meta)
    code = base + "code/"
    makedir(code)
    raw = base + "raw/"
    makedir(raw)
    return meta, traj, code, raw



"""
vault = r"/data/CMAP Data Submission Dropbox/Simons CMAP/vault/"
staging = r"/data/CMAP Data Submission Dropbox/Simons CMAP/staging/"
collected_data = r"/data/CMAP Data Submission Dropbox/Simons CMAP/collected_data/"
spatial_data = r"/data/CMAP Data Submission Dropbox/Simons CMAP/spatial_data/"
dataset_audit = r"/data/CMAP Data Submission Dropbox/Simons CMAP/dataset_audit/"
download_transfer = r"/data/CMAP Data Submission Dropbox/Simons CMAP/download_transfer/"
app_data = r"/data/CMAP Data Submission Dropbox/Simons CMAP/Apps/Simons CMAP Web Data Submission/"



################# Static Mission Icon Directory ##################
static = r"/data/CMAP Data Submission Dropbox/Simons CMAP/static/"
mission_icons = static + r"mission_icons/"
"""



cmap_dbx = "/data/cmap_dbx"
vault = f"{cmap_dbx}/vault/"
staging = f"{cmap_dbx}/staging/"
collected_data = f"{cmap_dbx}/collected_data/"
spatial_data = f"{cmap_dbx}/spatial_data/"
dataset_audit = f"{cmap_dbx}/dataset_audit/"
download_transfer = f"{cmap_dbx}/download_transfer/"
app_data = f"{cmap_dbx}/Apps/Simons CMAP Web Data Submission/"



################# Static Mission Icon Directory ##################
static = f"{cmap_dbx}/static/"
mission_icons = static + r"mission_icons/"


################# Download Transfer Structure   ##################
makedir(download_transfer)

################# Collected Data Structure  ##################
makedir(collected_data)


################# Staging Area Structure  ##################
makedir(staging)

#########  data  #########
data = staging + "data/"
makedir(data)

########  metadata  ########
metadata = staging + "metadata/"
makedir(metadata)

########  combined (temporary)  ########
combined = staging + "combined/"
makedir(combined)


################## Simons CMAP vault structure ##################
makedir(vault)

#########  BCP  #########
BCP = vault + "BCP/"
makedir(BCP)

#########  assimilation  #########
assimilation = vault + "assimilation/"
makedir(assimilation)


###########  models  ##########
model = vault + "model/"
makedir(model)

#########  r2r2 cruise #########
r2r_cruise = vault + "r2r_cruise/"
makedir(r2r_cruise)

#########  observations  #########
obs = vault + "observation/"
makedir(obs)
in_situ = obs + "in-situ/"
makedir(in_situ)
remote = obs + "remote/"
makedir(remote)


#########  obs/in-situ/cruise  #########
cruise = in_situ + "cruise/"
makedir(cruise)

#########  obs/in-situ/station  #########
station = in_situ + "station/"
makedir(station)

#########  obs/in-situ/float  #########
float_dir = in_situ + "float/"
makedir(float_dir)

#########  obs/in-situ/drifter  #########
drifter = in_situ + "drifter/"
makedir(drifter)

#########  obs/in-situ/drifter  #########
mixed = in_situ + "mixed/"
makedir(mixed)

########  obs/remote/satellite  ########
satellite = remote + "satellite/"
makedir(satellite)
