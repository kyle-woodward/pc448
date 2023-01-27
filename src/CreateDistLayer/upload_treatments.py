import os
import geopandas as gpd
import pandas as pd
import time
import argparse
import logging 
import yaml
from pathlib import Path
import datetime
from shapely.geometry import Point
from upload_utils import zip_shp, upload_gcs, upload_gee
'''
Upload treatments shapefile containing 3-digit DIST codes and their mapped ranks code

Usage: python upload_treatments.py -d path/to/repo/ -f path/to/one/treatment/shapefile.shp -a {optional} path/to/aoi/shapefile.shp
'''
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

def ranks_remap(gdf:gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    
    # create ranks field for DIST<->ranks remapping
    gdf.loc[:,'ranks'] = gdf['DIST'].map(code_ranks_dict)
    return gdf

def make_aoi(gdf:gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    local method to make minimum 15km bbox from shapefile's geometries
    
    args:
        gdf: geopandas.GeoDataFrame
    
    returns a GeoDataFrame
    """
    geom = gpd.GeoSeries(gdf['geometry'], crs='EPSG:5070') # CONUS Albers - units: meters
    env = geom.unary_union.envelope.buffer(7.5e3,join_style=2) # uses inherited geometries' crs unit for distance unit
    return gpd.GeoDataFrame({'FID':[1],'geometry':env},crs='EPSG:5070') # just doubling down since i've wrestled with this so much


def main():

    # initalize new cli parser
    parser = argparse.ArgumentParser(
        description="Upload processed treatments and AOI shapefiles to Earth Engine."
    )

    parser.add_argument(
        "-f",
        "--file",
        type=str,
        help="file path to treatment (shp)"
    )

    parser.add_argument(
        "-a",
        "--aoi",
        type=str,
        help="[optional] file path to AOI (shp)",
        required=False
    )
    args = parser.parse_args()
    
    shpfile_path = args.file
    aoi_path = args.aoi
    
    # parse config file
    config_path = os.path.join(repo_dir, 'config.yml')
    
    # inputs folder - holds input treatment and AOI(if any) shapefiles
    # should already be made
    inputs_dir = Path(os.path.join(repo_dir, 'data','inputs')).resolve()
    if not inputs_dir.exists():
        inputs_dir.mkdir(parents=True)
    
    # processed folder - holds processed treatment shapefile
    processed_dir = Path(os.path.join(repo_dir, 'data','processed')).resolve()
    if not processed_dir.exists():
        processed_dir.mkdir(parents=True)
    
    # proceess input shapefile
    shp = gpd.read_file(shpfile_path)
    logger.info(f"input shp CRS:{shp.crs}")
    shp.to_crs(epsg=5070,inplace=True)
    
    # enforce schema
    schema = gpd.GeoDataFrame(
        {'TREATMENT':['blah'],
        'CATEGORY':['blah'],
        'YEAR':[2019],
        'ACRES':[123456],
        'DIST':[321],
        'geometry':[Point(1, 2)]
        }
        )
    try:
        shp.dtypes == schema.dtypes
    except:
        raise ValueError(f"input shapefile does not have required schema:\n{schema.dtypes}\nCurrent schema:\n{shp.dtypes}")
    
    # process with ranks field
    w_ranks = ranks_remap(shp)
    
    # export processed shp
    processed_shp_path = os.path.join(processed_dir,os.path.basename(shpfile_path))
    logger.info(processed_shp_path)
    w_ranks.to_file(processed_shp_path)
    # zip up processed shp
    zipped_treatments = zip_shp(processed_shp_path)
    # upload processed treatments to GCS
    treatment_gcs_file = upload_gcs(local_file=zipped_treatments,bucket='op-tx',project_folder='test-project')
    # upload GCS file to GEE
    upload_gee(gcs_file=treatment_gcs_file,bucket='op-tx',project_folder='test_project',type='table')

    # make AOI if user chose not to provide
    if aoi_path is None:
        logger.info("Generating AOI")
        aoi = make_aoi(shp)
        #export to .shp
        aoi_path = os.path.join(processed_dir,os.path.basename(shpfile_path).replace('.shp','_AOI.shp'))
        aoi.to_file(aoi_path)
        # zip it
        zipped_aoi = zip_shp(aoi_path)
        # upload AOI to GCS       
        zipped_aoi_gcs = upload_gcs(local_file=zipped_aoi,bucket = 'op-tx',project_folder='test-project')
    else:
        zipped_aoi = zip_shp(aoi_path)
        zipped_aoi_gcs = upload_gcs(local_file=zipped_aoi,bucket = 'op-tx',project_folder='test-project')
    # upload AOI from GCS to GEE
    ## EOB Friday - wrestling with creating necessary GEE folders
    upload_gee(gcs_file=zipped_aoi_gcs,bucket='op-tx',project_folder='test_project',type='table')

if __name__ == '__main__':
    main()