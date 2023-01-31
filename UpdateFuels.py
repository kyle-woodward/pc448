#%%
import ee
import os
import logging
import datetime

repo_dir =  os.path.dirname(__file__)
date_id = datetime.datetime.utcnow().strftime("%Y-%m-%d").replace('-','') # like 20221216
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.WARNING,
    filename = os.path.join(repo_dir,"log",f"{date_id}.log")
)
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)
try:
    credentials = ee.ServiceAccountCredentials(email=None,key_file='/home/private-key.json')
    ee.Initialize(credentials)
except:
    ee.Initialize()

# get config.yml file path, needed for every script
config_path = os.path.join(os.getcwd(),'config.yml')
print(config_path)
#%%
fuels_source = "pyrologix" # "firefactor"
#%%
# for pc448 and others like it, provide paths to the DIST imgs representing the treatment scenarios you're updating Fuels for
scenario_paths = ["projects/pyregence-ee/assets/op-tx/test_project/Treatments_Plumas_Protect_Alt1_DIST"]

#create fuelscape folders for each DIST scenario
fuels_folders= [(path + "_fuelscape") for path in scenario_paths]

for fuels_folder in fuels_folders:
    fuels_folder_list = os.popen(f"earthengine ls {os.path.dirname(fuels_folder)}").read().split('\n')[0:-1]
    if not fuels_folder in fuels_folder_list:
        os.popen(f"earthengine create folder {fuels_folder}").read()
        print(f'Created Folder: {fuels_folder}')
    else:
        print(f"{fuels_folder} already exists")
#%%
print(scenario_paths)
print(fuels_folders)
#%%
# Canopy Guide
for scn_img_path,scn_sub_folder in zip(scenario_paths,fuels_folders):
    cmd = f"python src/CreateEEFuels/create_canopy_guide.py -c {config_path} -d {scn_img_path} -o {scn_sub_folder}" # pass the config file path, the given scenarios DIST img path, and the given scenarios fuelscapes folder path
    print(cmd)
    os.popen(cmd).read()
    #break

# FM40 
for scn_img_path,scn_sub_folder in zip(scenario_paths,fuels_folders):
    cmd = f"python src/CreateEEFuels/calc_FM40.py -c {config_path} -d {scn_img_path} -o {scn_sub_folder} -f {fuels_source}" # pass the config file path, the given scenarios DIST img path, and the given scenarios fuelscapes folder path
    print(cmd)
    os.popen(cmd).read()
    #break        
#%%
# CC and CH
for scn_img_path,scn_sub_folder in zip(scenario_paths,fuels_folders):
    cmd = f"python src/CreateEEFuels/calc_CC_CH.py -c {config_path} -d {scn_img_path} -o {scn_sub_folder} -f {fuels_source}" # pass the config file path, the given scenarios DIST img path, and the given scenarios fuelscapes folder path
    print(cmd)
    os.popen(cmd).read()
    print('\n')
    #break
#%%
# # CBD and CBH
for scn_img_path,scn_sub_folder in zip(scenario_paths,fuels_folders):
    cmd = f"python src/CreateEEFuels/calc_CBD_CBH.py -c {config_path} -d {scn_img_path} -o {scn_sub_folder} -f {fuels_source}" # pass the config file path, the given scenarios DIST img path, and the given scenarios fuelscapes folder path
    print(cmd)
    os.popen(cmd).read()
    print('\n')
    #break
#%%
from src.CreateEEFuels.utils.yml_params import get_export_params
AOI = ee.Image("projects/pyregence-ee/assets/pc448/templateImg").geometry()
for scn_img_path,scn_sub_folder in zip(scenario_paths,fuels_folders):
    # print(scn_img_path)
    # print(scn_sub_folder)
    fm40 = ee.ImageCollection(scn_sub_folder+'/fm40_collection').select('new_fbfm40').mosaic()
    cc = ee.Image(scn_sub_folder+'/CC')
    ch = ee.Image(scn_sub_folder+'/CH')
    cbh = ee.Image(scn_sub_folder+'/CBH')
    cbd = ee.Image(scn_sub_folder+'/CBD')
    fuel_stack = fm40.addBands(cc).addBands(ch).addBands(cbh).addBands(cbd).rename('FM40','CC','CH', 'CBH','CBD').toInt16()
    # print(fuel_stack.bandNames().getInfo())

    # export
    scn_id = scn_sub_folder.split('/')[-1]
    # print(scn_id)
    
    desc = f"export_{scn_id}"
   
    crs,scale = get_export_params(config_path)
    # print(desc)
    # print(crs)
    # print(scale)
  
    fileNamePrefix=scn_id
    bucket='op-tx'
    task = ee.batch.Export.image.toCloudStorage(image=fuel_stack,description=desc,bucket=bucket,fileNamePrefix=fileNamePrefix,region=AOI,scale=scale,crs=crs,formatOptions={'cloudOptimized':True})
    # folder=f'PC448_Fuelscapes'
    # task = ee.batch.Export.image.toDrive(image=fuel_stack,description=desc,folder=folder,fileNamePrefix=fileNamePrefix,region=AOI,scale=scale,crs=crs)
    task.start()
    # print(f'Export started: {folder}/{fileNamePrefix}')
    print(f'Export started: {bucket}/{fileNamePrefix}')

    #break