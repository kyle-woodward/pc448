# op-tx (Operational Treatment Simulator with Earth Engine)

## `op-tx` is a slimmed-down version of the FireFactor Fuels fuels toolkit, intended for simulating updated fuelscapes for 1 to MANY treatment scenarios at a time over smaller geographic extents in CONUS

Key differences between `op-tx` and `firefactor-fuels`
- use the master Treatment to DIST crosswalk google sheet or a custom google sheet to define your own crosswalks
- choose between FireFactor or Pyrologix fuels baselines
- automatically runs in the minimum extent to cover your Area of Interest (re: CG/FM40 and LF Zone-wise exports)
- uses a top-level UpdateFuels notebook to streamline data management and script execution, especially handy for wanting to run multiple scenarios at once (multiple DIST rasters)

### Set-Up (should look familiar from firefactor-fuels)

#### Install python dependencies with conda/pip
```
conda create -n optx python=3.11
conda install -c conda-forge earthengine-api geopandas pyyaml
pip install gspread
```

#### Install google-cloud-cli from source

- This is more OS specific and is meant to be installed on your machine outside of a venv. 
- If you have ran `firefactor-fuels` before this should already be installed. 
- Please follow best-practice for your OS [here](https://cloud.google.com/sdk/docs/install)

#### Setup credentials for `gspread`
- Go to Google Cloud Console, and select the `pyregence-ee` project
- Follow these [instructions](https://docs.gspread.org/en/v6.1.3/oauth2.html#for-bots-using-service-account) to create a new public-private JSON key for your machine and move it into the correct location (different for Unix and Windows, follow the instructions)

what is gspread?

- gspread is a wrapper around the Google Sheets API that makes reading and manipulating online Google Sheets documents feel the same as from a tabular file on disk with `pandas`. 
- makes it a lot easier for non-technical collaborators to work with you and agree on the Treatment -> DIST crosswalk rules, no sending .csv files back and forth

## Run Instructions

We will split this into pre-checks and running code

## Pre-Checks

1) Agree on the Treatments DIST crosswalk table

As the one who will be running this codebase, make sure that your collaborator(s) have established the crosswalk logic via a specific Google sheets doc.  The key decision points are what the TYPE and SEVERITY codes are for each `TREATMENT`.

- Two options here: 

1) Use the Master Treatment DIST crosswalk google sheet (recommended)
2) Create a new agreed-upon crosswalk google sheet (requires additional steps) 

*developer note: if you look in `~/src/CreateDistLayer/create_treatments_custom.py`, line 54, this is actually how it is now - using a custom one for pc448, you would just need to swap out that custom Google Sheet string ID for the master one (refer to identical line in ff-fuels) or another custom one*

2) Receive valid treatments shapefile(s)

Schema requirements are very minimal 

Each shapefile must contain fields `TREATMENT` and `YEAR`. 

- The `TREATMENT` field is what crosswalks to an identical `TREATMENT` field in the Treatment DIST crosswalk google sheet. Ensure all `TREATMENT` values have a match, or they'll be dropped in the process.

- `YEAR`: please make sure this field is filled out even if it seems unnecessary, there are checks in the code for type conversion and the code will let you know if YEAR values given will cause unexpected results (i.e. YEAR=200 or 1985 or something else dumb)

3)  Establish total Area of Interest via a template raster or AOI shapefile

- you typically want to model updated fuels and wildfire behavior further out than the minimum bbox of the treatment polygons.
- For pc448, I used this raster (projects/pyregence-ee/assets/pc448/templateImg) that Dave gave that we used as the template for extent. 
- the AOI extent dataset is a script input to `~/src/CreateDistLayer/rasterize_treatments_ee_custom.py`
- I *believe* i already wrote the code for this working with a ee.FeatureCollection AOI as well..

## Running Code

Creating the DIST raster from the treatments shapefile is a two-step process

These are all python CLI scripts with their own dedicated .log file that is generated at run-time:
1) Run `~/src/CreateDistLayer/create_treatments_custom.py`
```
python src/CreateDistLayer/create_treatments_custom.py -d /path/to/op-tx/repo -f /path/to/trt-shapefile.shp
```

2) Run `~/src/CreateDistLayer/rasterize_treatments_ee_custom.py` which takes the `dist_w_ranks_*` ee asset resulting from step 1 above as input
```
python src/CreateDistLayer/rasterize_treatments_ee_custom.py -c /path/to/config.yml -i input/ee/asset/path/to/dist_w_ranks_asset -o output/ee/asset/path/to/DIST_asset -r [DIST|ranks] (you'll want DIST) -a /ee/asset/path/to/AOI/asset
```

Create updated Fuelscape(s)

Open and Run `UpdateFuels.ipynb` - should be self-explanatory!

Some useful(?) notes:

- At the moment, you're probably better off to save-as it to a project-specific notebook and make the modifications you need specifically for the project at hand. 
- For instance, for PC448 we ran a few scenarios simultaneously on the first iteration, and then later on, only one scenario. The EE paths to the DIST image(s) will change for every run ..
- From a developer perspective it might be most streamlined as a CLI script, but you'd need to build in the logic for polling EE export tasks and only executing the next line of code to run the next step after the previous EE export tasks have finished.. definitely possible but I opted not to.

The final product is a 5-band (4canopy + 1surface) GeoTiff saved off in a Google Drive folder, at which point you can deliver it elsewhere if need-be.