import geopandas as gpd
import os
import time
import logging
import rasterio
from rasterio import features
import subprocess
from fnmatch import fnmatch
import argparse
from pathlib import Path
import yaml
'''
Rasterize a shapefile to CONUS grid using user-entered burn in field, requires that you can access a folder containing template conus tile rasters (any of the fuels tiles will do)

Usage: python rasterize_conus.py -d /path/to/toplevel/repo/directory -i /path/to/input/shapefile  -b burn_in_field

NOTE: burn-in order is top-down prioritized in cases of spatial overlap, 
        so ensure that your burn-in field's values use a top-down numbering priority
'''
logging.basicConfig(
    format="%(asctime)s %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S %p",
    level=logging.WARNING,
    filename=os.path.join(os.path.dirname(__file__),'rasterize_upload_treatments.log')
)
logger = logging.getLogger(__name__)

logger.setLevel(logging.INFO)

def rasterize(input_file, output_dir, burn_field, mosaic_fn, version):
    if not os.path.exists(input_file):
        raise RuntimeError(f'{input_file} does not exist')
    else:
        
        # pull the 50 raster tiles from a fuels tiles folder on the bucket
        local_tiles_dir = Path(os.path.join(output_dir, 'template_tiles'))
        if not local_tiles_dir.exists():
            local_tiles_dir.mkdir(parents=True)
        if len(os.listdir(local_tiles_dir)) == 0:
            gsutil_cp_cmd = f"gsutil -m cp gs://landfire/conus/fuels/cc_tiles/Fuels_CC_00*.tif {local_tiles_dir}"
            logger.info(gsutil_cp_cmd)
            proc = subprocess.run(gsutil_cp_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        # make output folder for tiled treatmentsRanks .tifs
        output_tiles_dir = Path(os.path.join(output_dir, 'ranks_tiles')).resolve()
        if not output_tiles_dir.exists():
            output_tiles_dir.mkdir(parents=True)
        
        # get list of template raster tile .tifs to loop through
        tiles = [os.path.join(local_tiles_dir, i ) for i in os.listdir(local_tiles_dir)]
        logger.info(f'Reading {input_file}')
        shp = gpd.read_file(input_file)
        shp = shp.to_crs(epsg=5070)
        max_burn_value = shp[burn_field].max() # get value range of burn-in field 
        min_burn_value = shp[burn_field].min()
        
        for tile in tiles:
            tile_str = os.path.basename(tile).split('.tif')[0].split('_')[-1]

            # tiled output names
            out_fn = os.path.join(output_tiles_dir, f'dist_treatmentsRanks_{version}_{tile_str}.tif') 
            #out_fn = os.path.join(output_tiles_dir, f'{os.path.basename(input_file).split(".")[0]}_{burn_field}_{tile_str}.tif') 
            
            if not os.path.exists(out_fn):
                
                # benchmarking timing
                start = time.time()
                logger.info(f'Processing tile {tile_str}...')
                # template raster for rasterization
                rst = rasterio.open(tile)

                #copy and update the metadata from the input raster for the output
                meta = rst.meta.copy()
                meta.update(dtype = rasterio.uint8, compress='lzw', nodata = 0)
                rst.close()
                # logger.info(meta) #comment out after testing
                
                with rasterio.open(out_fn, 'w+', **meta) as out:
                    out_arr = out.read(1)
                    for val in range(min_burn_value, (max_burn_value+1)): # burn in geometries one set of burn-in values at a time, least to greatest
                        filtered_shp = shp[shp[burn_field]==val]
                        if len(filtered_shp) == 0:
                            continue
                        # logger.info(f"burning in {val} ranks")
                    
                        # create a generator of geom, value pairs to burn into raster
                        shapes = ((geom,value) for geom, value in zip(filtered_shp["geometry"], filtered_shp[burn_field]))

                        burned = features.rasterize(shapes=shapes, fill=0, out=out_arr, transform=out.transform)
                        out.write_band(1, burned)

                    out.close()

                end = time.time()
                logger.info(f'Time Elapsed: {(end-start)/60} minutes')
            else:
                logger.info(f"{out_fn} already exists, skipping..")
                continue

        scale = 30

        tile_paths = [os.path.join(output_tiles_dir,file) for file in os.listdir(output_tiles_dir) if fnmatch(file,f'dist_treatmentsRanks_{version}*.tif')]
        logger.info(f"tiles to pull for version {version}: {len(tile_paths)}")
        cmd = (
                f'gdal_merge.py -o {mosaic_fn} -ps {scale} {scale} -init "0" '
                + f"-a_nodata 0 -n 0 -of GTiff -co COPY_SRC_OVERVIEWS=YES "
                + f"-co NUM_THREADS=ALL_CPUS -co COMPRESS=LZW -co PREDICTOR=2 "
                + f"-co BIGTIFF=YES {' '.join(tile_paths)}"
            )
        logger.info(cmd)  # delete later

        # run merge
        logger.info("Running gdal merge...")
        procenv = os.environ.copy()
        
        proc = subprocess.Popen(
            cmd, env=procenv, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        out, err = proc.communicate()

def main():
    # initalize new cli parser
    parser = argparse.ArgumentParser(
        description="Rasterize treatments shapefile to CONUS grid on user-entered burn-in field."
    )
    
    parser.add_argument(
        "-d",
        "--repo_dir",
        type=str,
        help="file path to repo's top-level directory"
    )

    parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        help="input treatments shapefile",
    )


    parser.add_argument(
        "-b", 
        "--burn_field", 
        type=str, 
        help="field used for raster burn-in values"
    )
    
    
    args = parser.parse_args()

    input_file = args.input_file
    burn_field = args.burn_field
    
    # parse config file
    config_path = os.path.join(args.repo_dir, 'config.yml')
    output_dir = os.path.join(args.repo_dir, 'data/dist_outputs')
    with open(config_path) as file:
        config = yaml.full_load(file)
    
    version = config["version"].get('latest')
    gaia_dir = config["DIST"].get('gaia_subdir')
    
    logger.info(f"Defined Version: {version}")

    mosaic_fn = os.path.join(output_dir, f'dist_treatmentsRanks_{version}.tif')
    #mosaic_fn = os.path.join(output_dir, f'{os.path.basename(input_file).split(".")[0]}_{burn_field}_mosaic.tif')

    ee_asset = f"projects/pyregence-ee/assets/workflow_assets/{os.path.basename(mosaic_fn).split('.')[0]}"
    gs_fn = f"gs://landfire/dist_outputs/{os.path.basename(mosaic_fn)}"
    logger.info(f"local output file: {mosaic_fn}")
    logger.info(f"google storage output file: {gs_fn}")
    logger.info(f"output earth engine asset: {ee_asset}")


    if not os.path.exists(mosaic_fn):
        rasterize(input_file=input_file, output_dir=output_dir, burn_field=burn_field, mosaic_fn=mosaic_fn, version=version)
        
        gaia_cp_cmd = f'cp {mosaic_fn} {os.path.join(gaia_dir, os.path.basename(mosaic_fn))}'
        gs_upload_cmd = f'gsutil cp {mosaic_fn} {gs_fn}'
        ee_upload_cmd = f"earthengine upload image --asset_id={ee_asset} --pyramiding_policy=mean {gs_fn}"
        
        # Send 3 commands, cp mosaic tif to GAIA, then GS bucket, then upload it from GS bucket to EE asset
        procenv = os.environ.copy()
        
        # not necessary and won't work unless on hyperion VM
        # logger.info(f'copying mosaic to GAIA')
        # logger.info(gaia_cp_cmd)
        # proc = subprocess.Popen(
        # gaia_cp_cmd, env=procenv, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        # )
        # out, err = proc.communicate()

        logger.info(f'uploading to GS bucket')
        logger.info(gs_upload_cmd)
        proc = subprocess.Popen(
        gs_upload_cmd, env=procenv, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        out, err = proc.communicate()

        logger.info(f'uploading from GS bucket to EE')
        logger.info(ee_upload_cmd)
        proc = subprocess.Popen(
        ee_upload_cmd, env=procenv, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
        )
        out, err = proc.communicate()
    
    else:
        raise Exception(f"{mosaic_fn} already exists, exiting.")

if __name__ == '__main__':
    main()