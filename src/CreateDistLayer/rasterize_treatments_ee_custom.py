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

# Potential different avenue to the curernt rasterize_treatments.py - rasterize each treatments_ranks FC on ranks property into one ee.Image
# testing if it works and is faster than local processing route
# Load in treatment FCs with ranks property
def main():
    """Main level function for generating new CBH and CBD"""
    
    # initalize new cli parser
    parser = argparse.ArgumentParser(
        description="CLI process for generating new CBH and CBD."
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
    ranksImg = dist_w_ranks.reduceToImage(['ranks'], ee.Reducer.max()).rename('ranks')

    # logger.info(ranksImg.bandNames().getInfo())
   
    desc = os.path.basename(args.output).replace('/','_')
        
    task = ee.batch.Export.image.toAsset(
        image=ranksImg,
        description=desc,
        assetId=args.output,
        region=cc_img.geometry(),
        crsTransform=geo_t,
        crs=crs,
        maxPixels=1e12,
    )
    task.start()  # kick off export
    print(f'export task started: {desc}')

if __name__ =="__main__":
    main()