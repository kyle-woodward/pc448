import pandas as pd
import os
import gspread
import time
import argparse
import logging 
import yaml
from pathlib import Path
import subprocess
import geopandas as gpd
import zipfile
from fnmatch import fnmatch

'''
Create CONUS-wide treatments shapefile containing 3-digit DIST code and ranks value for all records
Collects individual treatment shapefiles on storage containing the provided version tag in its name

Usage: python create_treatments.py -d path/to/repo/ -f path/to/one/treatment/file


'''

logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.WARNING,
    filename=os.path.join(os.path.dirname(__file__), 'create_treatments_custom.log')
)
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

#%%

def make_full_dist_shp(files:list, local_treatments_dir:str, local_zones_file:str, year_range:list, eff_yr:int) -> gpd.GeoDataFrame:
    
    def dist_rank_calculate(gdf:gpd.GeoDataFrame, TSD_remap:dict) -> gpd.GeoDataFrame:
        #make 3rd digit padded Type Severity code
        gdf.loc[:,'TYPE_SEV_0'] = (gdf['TYPE_SEV'] * 10).astype(int)
        gdf.loc[:,'diff_yr'] = (eff_yr - gdf['YEAR']).astype(int)
        
        #remap diff_yr value to TSD code based on zones rules
        gdf.loc[:,'TSD'] = gdf['diff_yr'].map(TSD_remap)
        gdf.loc[:,'DIST'] = (gdf['TYPE_SEV_0'] + gdf['TSD']).astype(int)
        
        # remap DIST code values to ranked values for export
        gdf.loc[:,'ranks'] = gdf['DIST'].map(code_ranks_dict)
        
        return gdf

    # retrieve lookup table google sheet
    gc = gspread.service_account()
    # going back and forth whether to use PNF LUT that we mde for pc448 or the FireFactor "super sheet" 
    trt_xwalk = gc.open("PNF treatment dist assignments")  #pc448 sheet
    # trt_xwalk = gc.open("Fuels-Treatments Crosswalk") # The FF super sheet
    lookup_table = pd.DataFrame(trt_xwalk.worksheet("DIST code").get_all_records()) # pc448 sheet
    # lookup_table = pd.DataFrame(trt_xwalk.worksheet("Lookup").get_all_records()) # The FF super sheet

    # create empty gdf to place processed gdfs on each for-loop iteration
    gdf_collection = gpd.GeoDataFrame()
    
    for file in files:
        file_pth = file #os.path.join(local_treatments_dir, os.path.basename(file))
        logger.info(f'reading {file_pth} to geopandas, joining to treatments lookup')

        start = time.time()
        
        # do the necessary wrangling and year filtering after joining to treatments lookup
        shp = gpd.read_file(file_pth)
        logger.info(f'total records in input shp: {shp.shape[0]}')
        shp = shp.merge(lookup_table, how='inner', left_on='TREATMENT', right_on='TREATMENT')
        
        shp.loc[:,'TYPE_SEV'] = shp['TYPE_SEV'].astype(int) 
        # filter to remove invalid TYPE_SEV values that would be over 100 
        # (TYPE_SEV =  TYPE*100 + Severity so invalid values are 847, 968, and 1089)
        shp = shp[shp['TYPE_SEV'] < 100]
        # filter for year range
        shp = shp[(shp['YEAR'] >= year_range[0]) & (shp['YEAR'] <= year_range[1])]

        # Bring in LANDFIRE Zones and create the SE and non SE zones 
        zones = gpd.read_file(local_zones_file)
        se_zone_nums = [46, 55, 56, 58, 99]
        se_zones = zones[zones['ZONE_NUM'].isin(se_zone_nums)]
        non_se_zones = zones[~zones['ZONE_NUM'].isin(se_zone_nums)]

        # convert zones and shp to same crs
        se_zones_4326 = se_zones.to_crs(epsg=4326)
        non_se_zones_4326 = non_se_zones.to_crs(epsg=4326)
        shp_4326 = shp.to_crs(epsg=4326)
        
        logger.info(f'total records in processed shp: {shp.shape[0]}')
        logger.info('performing TSD, DIST, ranks calculations for SE and Non SE subsets')
        
        # create tmp index column so that you can remove duplicate records produced by sjoin()
        shp_4326.loc[:,'tmp_idx'] = shp_4326.index
        
        #spatial join, drop all but ZONE_NUM column from the zones gdf
        shp_se = gpd.sjoin(shp_4326,se_zones_4326, how='inner')
        shp_se = shp_se.drop_duplicates(subset=['tmp_idx'])
        # only keep ZONE_NUM column from the Zones gdf, may be useful for QAing this step
        # shp_se = shp_se[['DATE', 'TREATMENT', 'YEAR', 'source_id', 'geometry', 'TYPE_SEV', 'ZONE_NUM']] 
        shp_se = shp_se[['TREATMENT', 'YEAR', 'geometry', 'TYPE_SEV', 'ZONE_NUM']] # reduced field schema for custom applications 
        logger.info(f'records in shp_se: {shp_se.shape[0]}')
        
        #spatial join, drop all but ZONE_NUM column from the zones gdf
        shp_non_se = gpd.sjoin(shp_4326,non_se_zones_4326, how='inner')
        shp_non_se = shp_non_se.drop_duplicates(subset=['tmp_idx'])
        # only keep ZONE_NUM column from the Zones gdf, may be useful for QAing this step
        # shp_non_se = shp_non_se[['DATE', 'TREATMENT', 'YEAR', 'source_id', 'geometry', 'TYPE_SEV', 'ZONE_NUM']]
        shp_non_se = shp_non_se[['TREATMENT', 'YEAR', 'geometry', 'TYPE_SEV', 'ZONE_NUM']] # reduced field schema for custom applications 
        logger.info(f'records in shp_non_se: {shp_non_se.shape[0]}')
        
        # TSD remap dictionaries
        non_se_TSD_remap_dict = {0:1, 1:1, 2:2, 3:2, 4:2, 5:2, 6:2, 7:3, 8:3, 9:3, 10:3}
        se_TSD_remap_dict = {0:1, 1:1, 2:2, 3:2, 4:2, 5:3, 6:3, 7:3, 8:3, 9:3, 10:3}

        # DIST code -> ranks remap dictionary 
        code_ranks_dict = {131:36, 132:35, 133:34, 
                            331:33, 332:32, 333:31, 
                            121:30, 122:29, 123:28,
                            111:27, 112:26, 113:25,
                            321:24, 231:23, 831:22,
                            311:21, 221:20, 821:19,
                            322:18, 211:17, 811:16,
                            312:15, 232:14, 832:13,
                            323:12, 222:11, 822:10,
                            313:9, 212:8, 812:7,
                            233:6, 833:5, 223:4,
                            823:3, 213:2, 813:1}

        

        # do the TSD -> DIST -> ranks calculations for each zone subset, using the two diff TSD remap dictionaries
        se_gdf_converted = dist_rank_calculate(shp_se, se_TSD_remap_dict)
        non_se_gdf_converted = dist_rank_calculate(shp_non_se, non_se_TSD_remap_dict)
        
        # then combine both subset gdfs into one gdf
        both_converted = gpd.GeoDataFrame( pd.concat( [se_gdf_converted, non_se_gdf_converted], ignore_index=True) ) # could ignore_index be affecting the bug ?
        
        # add finished gdf to the full gdf_collection
        logger.info('adding processed gdf to full gdf collection')
        gdf_collection = gpd.GeoDataFrame(pd.concat([both_converted, gdf_collection]))
        final_prj_collection = gdf_collection.set_crs(epsg=4326) #set crs to WGS84 
        end = time.time()
        logger.info(f'Time Elapsed: {(end-start)/60} minutes')
    return final_prj_collection

def main():

    # initalize new cli parser
    parser = argparse.ArgumentParser(
        description="Generate CONUS treatments dataset from versioned treatment datasets on cloud storage."
    )

    parser.add_argument(
        "-d",
        "--repo_dir",
        type=str,
        help="file path to repo's top-level directory"
    )
    
    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="file path to treatment (shp|zip)"
    )
    args = parser.parse_args()
    
    # parse config file
    config_path = os.path.join(args.repo_dir, 'config.yml')
    
    # make local data folders
    local_treatments_dir = Path(os.path.join(args.repo_dir, 'data','treatments')).resolve()
    if not local_treatments_dir.exists():
        local_treatments_dir.mkdir(parents=True)
    
    output_dir = Path(os.path.join(args.repo_dir, 'data','dist_outputs')).resolve()
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # holds zones shapefile which is needed for TSD value assignment b/w SE and non SE zones
    zones_dir = Path(os.path.join(args.repo_dir,'data','zones')).resolve()
    if not zones_dir.exists():
        zones_dir.mkdir(parents=True)
    
    # copy over Landfire zones shapefile from cloud
    local_zones_file = os.path.join(zones_dir,'zones.zip')
    # logger.info(f'{local_zones_file}')
    if not os.path.exists(local_zones_file):
        gsutil_cp_zones = f"gsutil cp gs://landfire/conus/landfire/zones/zones.zip {local_zones_file}"
        proc = subprocess.run(gsutil_cp_zones, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            logger.info(proc.stdout)
            raise Exception('gsutil cp failed')

    with open(config_path) as file:
        config = yaml.full_load(file)
    
    year_info = config["year_info"]
    year_range = year_info.get("range")
    eff_yr = year_info.get("effective")

    bucket_dir = "gs://pc448-20221128" # new bucket for pc task
    # version = "Treatments_Plumas_Protect" # treatment subfolder containing zipped trt shps (e.g. gs://bucket/treatments/Treatments_Plumas_Protect)
    
    gcs_output_dir = "gs://pc448-20221128/dist_outputs"
    ee_subdir = "pc448"
    
    # logger.info(f"Defined Version: {version}")
    logger.info(f"effective year: {eff_yr}")
    logger.info(f"year range: {year_range}")
    
    if args.file == None: # user does not provide -f file path arg to treatments shp, case is currently not handled well
        files = os.listdir(local_treatments_dir)
    else:
        files = [args.file] # directly provide treatment shapefile path (.shp|.zip)
    

    input_filename = os.path.basename(files[0]).split('.')[0]
    file_prefix = 'dist_w_ranks_'
    out_shp = os.path.join(output_dir,file_prefix+input_filename+'.shp')
    zipfile_path = out_shp.replace('.shp','.zip')
    gcs_outfile = f"{gcs_output_dir}/{file_prefix}{input_filename}.zip"
    ee_asset = f"projects/pyregence-ee/assets/{ee_subdir}/{file_prefix}{input_filename}"
    logger.info(f"input filename: {input_filename}")
    logger.info(f"output shpfile: {out_shp}")
    logger.info(f"zipfile_path: {zipfile_path}")
    if not os.path.exists(zipfile_path):
        logger.info(f"Found {len(files)} files, starting DIST shp generation")
        shp=make_full_dist_shp(files=files, local_treatments_dir=local_treatments_dir, local_zones_file=local_zones_file, year_range=year_range, eff_yr=eff_yr)
        print(shp.head())
        # output final shp to local storage
        logger.info(f'Exporting {out_shp}')
        shp.to_file(os.path.join(output_dir, out_shp))

        # zip .shp and associated files 
        list_files  = [os.path.join(output_dir,i) for i in os.listdir(output_dir) if fnmatch(i,f'*{input_filename}*')]
        logger.info(f"Adding files to .zip archive: {list_files}")
        with zipfile.ZipFile(zipfile_path, 'w') as zipF:
            for file in list_files:
                zipF.write(file,os.path.basename(file),compress_type=zipfile.ZIP_DEFLATED) # this is kind of funky, can't figure out how to get rel file path not whole directory to be added into archive file
        
        # upload .zip to cloud storage
        gsutil_upload_cmd = f"gsutil cp {zipfile_path} {gcs_outfile}"
        logger.info(gsutil_upload_cmd)
        proc = subprocess.run(gsutil_upload_cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            logger.info(proc.stdout)
            raise Exception('gsutil upload failed, ensure folder exsists on GS bucket')
            
        # upload .zip to Earth Engine
        ee_upload_cmd = f"earthengine upload table --asset_id={ee_asset} {gcs_outfile}"
        logger.info(f"{ee_upload_cmd}\n")
        proc = subprocess.run(ee_upload_cmd, shell=True,stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            logger.info(proc.stdout)
            raise Exception('earthengine upload cmd failed')
    else:
        raise Exception(f'{zipfile_path} already exists')

if __name__ == '__main__':
    main()