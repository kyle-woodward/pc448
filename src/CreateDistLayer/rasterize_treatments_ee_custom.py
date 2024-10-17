import os 
import ee
import yaml
import argparse
import logging

logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.WARNING,
    filename=os.path.join(os.path.dirname(__file__),'rasterize_treatments_ee_custom.log')
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

try:
    credentials = ee.ServiceAccountCredentials(email=None,key_file='/home/private-key.json')
    ee.Initialize(credentials)
except:
    ee.Initialize()

def ranks_to_dist(img:ee.Image):
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
    treatments_ranks = ee.Image(img)
    
    # remap back to DIST codes
    dist_img = treatments_ranks.remap(ranks_list, codes_list, 0).rename('DIST')
    return dist_img

# Potential different avenue to the curernt rasterize_treatments.py - rasterize each treatments_ranks FC on ranks property into one ee.Image
# testing if it works and is faster than local processing route
# Load in treatment FCs with ranks property
def main():
    """rasterize dist FC on ranks or DIST property"""
    
    # initalize new cli parser
    parser = argparse.ArgumentParser(
        description="rasterize dist FC on ranks or DIST property"
    )

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="path to config file",
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Earth Engine Asset path to dist w ranks file"
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="Earth Engine Asset output asset path"
    )

    parser.add_argument(
        "-r",
        "--rasterize_on",
        type=str,
        help="property to rasterize on, one of DIST or ranks"
    )

    parser.add_argument(
        "-a",
        "--aoi",
        type=str,
        help="Earth Engine asset path to an aoi feature collection or template image with desired boundaries"
    )
    args = parser.parse_args()

    # parse config file
    with open(args.config) as file:
        config = yaml.full_load(file)

    geo_info = config["geo"]
    version = config["version"].get('latest')

    # extract out geo information from config
    geo_t = geo_info["crsTransform"]
    scale = geo_info["scale"]
    x_size, y_size = geo_info["dimensions"]
    crs = geo_info["crs"]
    
    # for export geometry consistency
    cc_ic = ee.ImageCollection("projects/pyregence-ee/assets/conus/landfire/cc")
    cc_img = ee.Image(
        cc_ic.filter(ee.Filter.eq("version", 200)).limit(1, "system:time_start").first()
    )
    
    dist_w_ranks = ee.FeatureCollection(args.input) # plug EE asset path straight in insted of version-based file naming
    # we rasterize on ranks as intermediary step so that we can take the most impactful treatment (ranked in order 1 -> 36) in case of feature overlap
    ranksImg = dist_w_ranks.reduceToImage(['ranks'], ee.Reducer.max()).rename('ranks')
    
    # rasterizes on ranks or DIST field given user input to --rasterize_on
    if args.rasterize_on == "ranks":
        final = ranksImg
    elif args.rasterize_on == "DIST":
        final = ranks_to_dist(ranksImg)
    else:
        raise ValueError(f"{args.rasterize_on} is not a valid property to rasterize on. Valid properties: ranks, DIST ")
    
    logger.info(final.bandNames().getInfo())
   
    # Use aoi path provided to parameterize the region for export
    aoi_path = args.aoi
    if aoi_path == None:
        AOI = cc_img.geometry() # no aoi path provided will result in exported img with whole CONUS boundary
    else: #user provides asset path to a image or featurecollection to use as the AOI
        asset_type = ee.data.getAsset(aoi_path).get('type') # determine what it is
    if asset_type == "IMAGE":
        AOI = ee.Image(aoi_path) # cast it
    elif asset_type == "TABLE":
        AOI = ee.FeatureCollection(aoi_path) # cast it
    else:
        raise RuntimeError(f"{asset_type} is not one of IMAGE or TABLE")
    
    desc = os.path.basename(args.output).replace('/','_') 
    task = ee.batch.Export.image.toAsset(
        image=final,
        description=desc,
        assetId=args.output,
        region=AOI.geometry(),
        crsTransform=geo_t,
        crs=crs,
        maxPixels=1e12,
    )
    task.start()  # kick off export
    logger.info(f'export task started: {desc}')

if __name__ =="__main__":
    main()