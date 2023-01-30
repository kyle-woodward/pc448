#%%
import os
import subprocess
from fnmatch import fnmatch
import zipfile
from pathlib import Path
import datetime
import logging

repo_dir =  os.path.abspath(os.path.join(__file__ ,"../../..")) # three parents up
date_id = datetime.datetime.utcnow().strftime("%Y-%m-%d").replace('-','') # like 20221216
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.WARNING,
    filename = os.path.join(repo_dir,"log",f"{date_id}.log")
)
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

def zip_shp(shpfile:str):
    """
    zip .shp and associated files at current directory
    args: 
        shpfile (str): file path to shapefile
    return:
        path to zipped file 
    """
    # path = Path(shpfile).resolve()
    parent = os.path.dirname(shpfile)
    basename = os.path.basename(shpfile).split('.shp')[0]
    list_files  = [os.path.join(parent,i) for i in os.listdir(parent) if fnmatch(i,f'{basename}*')]
    logger.info(f"Adding files to .zip archive: {list_files}")
    zipfile_path = shpfile.replace('.shp','.zip')
    with zipfile.ZipFile(zipfile_path, 'w') as zipF:
        for file in list_files:
            zipF.write(file,os.path.basename(file),compress_type=zipfile.ZIP_DEFLATED)
    return zipfile_path

def upload_gcs(local_file:str,bucket:str,project_folder:str):
    """
    Upload files to new or existing location on a GCP Bucket
    args:
        local_file (str): local path to treatments shapefile
        bucket (str): GCP bucket name for destination
        project_folder (str): GEE project folder to create at projects/pyregence-ee/assets/{bucket}/{project}) for project-level file storage
        type (str): one of: ['image','table']
    returns:
        destination file path on Google Cloud Storage bucket (str)
    """
    # next upload file to google cloud storage
    # file_exists = os.path.basename(file) in os.popen(f"gsutil ls gs://op-tx/{project_folder}/treatments/")
    gcs_dest = f"gs://{bucket}/{project_folder}/treatments/{os.path.basename(local_file)}"
    gsutil_upload_cmd = f"gsutil cp {local_file} {gcs_dest}"
    logger.info(gsutil_upload_cmd)
    proc = subprocess.Popen(
            gsutil_upload_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    out, err = proc.communicate()
    return gcs_dest
    # finally upload GCS storage file to GEE in newly created asset folder


def upload_gee(gcs_file:str,bucket:str,project_folder:str,type:str):
    """
    Upload files from Google Cloud Storage Bucket to GEE assets
    args:
        gcs_file (str): local path to treatments shapefile
        bucket (str): GCP bucket name for destination
        project_folder (str): GEE project folder to create at projects/pyregence-ee/assets/{bucket}/{project}) for project-level file storage
        type (str): one of: ['image','table']
    returns:
        the file's asset path in GEE (str)
    """
    if (not type!='image') or (type!='table'):
        raise ValueError(f"type must be 'image' or 'table': {type}")
    logger.info(f"gcs file: {gcs_file}")
    
    # must first make directory tree in GEE assets
    # ee cli docs say you can use -p flag to make necessary parents but error indicates that is a lie
    # could make a function to handle this but for now will just assume one parent and one child will provided for the project
    gee_dir_tree = f"projects/pyregence-ee/assets/{bucket}/{project_folder}" # we could make the folder more organized if we wanted...                      
    create_parent_cmd =f"earthengine create folder {os.path.dirname(gee_dir_tree)}"
    create_child_cmd = f"earthengine create folder {gee_dir_tree}"
    logger.info(create_parent_cmd)
    proc = subprocess.Popen(
            create_parent_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    out, err = proc.communicate()
    logger.info(create_child_cmd)
    proc = subprocess.Popen(
            create_child_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    out, err = proc.communicate()
    
    # then upload gcs file to EE asset folder
    gee_asset_path = f"{gee_dir_tree}/{os.path.basename(gcs_file).split('.')[0]}"
    gee_upload_cmd = f"earthengine upload {type.lower()} --asset_id={gee_asset_path} {gcs_file}"
    logger.info(gee_upload_cmd)
    proc = subprocess.Popen(
            gee_upload_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
    out, err = proc.communicate()
    return gee_asset_path

#%%
# %%
