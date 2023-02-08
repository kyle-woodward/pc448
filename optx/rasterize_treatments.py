import os 
import ee
import yaml
import argparse
import logging
import datetime
from utils.cloud_utils import poll_submitted_task

repo_dir =  os.path.abspath(os.path.join(__file__ ,"../..")) # three parents up
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
    ee.Initialize(project='pyregence-ee')

def code_rank_remap(img:ee.Image,to='DIST'):
    codes_list = ee.List([131, 132, 133,    # fire high sev | most recent to least                      
                        331, 332, 333,    # mech remove high sev | most recent to least               
                        121, 122, 123,    # all other                                          
                        111, 112, 113,    #   fires                            
                        321, 231, 831,    #                                                       
                        311, 221, 821,    #                                                          
                        322, 211, 811,    #        all other dists ranked by                               
                        312, 232, 832,    #             TSD, type, sev                                                         
                        323, 222, 822,    #  type rank goes mech remove, mech add, other                                               
                        313, 212, 812,                                                                
                            233, 833,    
                            223, 823,    
                            213, 813])             

    ranks_list = ee.List([36, 35, 34,
                    33, 32, 31,
                    30, 29, 28,
                    27, 26, 25,
                    24, 23, 22, 
                    21, 20, 19, 
                    18, 17, 16,
                    15, 14, 13,
                    12, 11, 10, 
                    9, 8, 7, 
                    6, 5, 4, 
                    3, 2, 1])

    # treatments DIST ranks asset
    img = ee.Image(img)
    
    # remap back to DIST codes
    if to == 'rank':
        remap_img = img.remap(codes_list, ranks_list, 0).rename('ranks')
    else:
        remap_img = img.remap(ranks_list, codes_list, 0).rename('DIST')
    return remap_img

# Potential different avenue to the curernt rasterize_treatments.py - rasterize each treatments_ranks FC on ranks property into one ee.Image
# testing if it works and is faster than local processing route
# Load in treatment FCs with ranks property
def rasterize(input:str,rasterize_on:str,aoi:str,poll=False):
    """
    Rasterize pre-existing treatments ee.FeatureCollection on 'ranks' or 'DIST' property
    args:
        input (str): treatments GEE asset path

        rasterize_on (str): one of: 'ranks', 'DIST'

        aoi (str): AOI GEE asset path 
    returns:
        GEE asset path of resulting ee.Image (str)
    """

    # parse config file
    with open(os.path.join(repo_dir,'config.yml')) as file:
        config = yaml.full_load(file)

    geo_info = config["geo"]
    version = config["version"].get('latest')

    # extract out geo information from config
    geo_t = geo_info["crsTransform"]
    scale = geo_info["scale"]
    x_size, y_size = geo_info["dimensions"]
    crs = geo_info["crs"]
    
    # default export geometry
    cc_ic = ee.ImageCollection("projects/pyregence-ee/assets/conus/landfire/cc")
    cc_img = ee.Image(
        cc_ic.filter(ee.Filter.eq("version", 200)).limit(1, "system:time_start").first()
    )
    
    input_fc = ee.FeatureCollection(input) # plug EE asset path straight in insted of version-based file naming
    ranksImg = input_fc.reduceToImage(['ranks'], ee.Reducer.max()).rename('ranks')
    
    # rasterizes on ranks or DIST field given user input to --rasterize_on
    if rasterize_on == "ranks":
        final = ranksImg
    elif rasterize_on == "DIST":
        final = code_rank_remap(ranksImg)
    else:
        raise ValueError(f"{rasterize_on} is not a valid property to rasterize on. Valid properties: ranks, DIST ")
    
    # logger.info(final.bandNames().getInfo())
   
    # Use aoi path provided to parameterize the region for export
    
    if aoi == None:
        AOI = cc_img.geometry() # no aoi path provided will result in exported img with whole CONUS boundary
    else: #user provides asset path to a image or featurecollection to use as the AOI
        asset_type = ee.data.getAsset(aoi).get('type') # determine what it is
    if asset_type == "IMAGE":
        AOI = ee.Image(aoi) # cast it
    elif asset_type == "TABLE":
        AOI = ee.FeatureCollection(aoi) # cast it
    else:
        raise RuntimeError(f"{asset_type} is not one of IMAGE or TABLE")
    
    desc = os.path.basename(input)+f'_{rasterize_on}'
    asset_id = '/'.join([os.path.dirname(input).replace('input','output'), desc]) # DIST saved in outputs/
    # set workload tag context with with 
    with ee.data.workloadTagContext('optx-export'):
        task = ee.batch.Export.image.toAsset(
            image=final,
            description=desc,
            assetId=asset_id,
            region=AOI.geometry(),
            crsTransform=geo_t,
            crs=crs,
            maxPixels=1e12,
        )
        task.start()  # kick off export
    logger.info(f'export task started: {asset_id}')
    if poll:
        poll_submitted_task(task,1)
    
    return asset_id