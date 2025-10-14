#############################################
##        CONFIGURATION VARIABLES          ##
#############################################

from pathlib import Path
import os
from dotenv import load_dotenv

# create a list of ENCs to download
charts_to_download = ["US4NY1BY.zip", "US4RI1CB.zip", "US4MA1CC.zip", "US4MA1CD.zip", 
"US4MA1CE.zip", "US4MA1DE.zip", "US4MA1DD.zip", "US4MA1DC.zip", "US4RI1DB.zip", 
"US4NY1CY.zip", "US4NJ1FH.zip", "US4NJ1FG.zip", "US4NY1BM.zip", "US5RI1BE.zip", 
"US4NY1BX.zip", "US4VA1AG.zip", "US4VA1AH.zip", "US4VA1AI.zip", "US4VA1BG.zip", 
"US4VA1BH.zip", "US4VA1BI.zip"]

# set a folder location on disc to download the chart data to 
base_dir = Path(__file__).resolve().parent.parent.parent.parent
target_folder_path = base_dir / "data" / "ENC"

env_path = Path.home()/'.config'/'secrets'/'.env' 
load_dotenv(dotenv_path=env_path)
# Get item ID credentials securely using os.getenv()
turbine_id = os.getenv("TURBINE_ITEM_ID")
buoy_id = os.getenv("BUOY_ITEM_ID")
cable_id = os.getenv("CABLE_ITEM_ID")
substation_id = os.getenv("SUBSTATION_ITEM_ID")

# define the features to extract from ENC
extraction_features = {
    "Wind_Turbines": {
        "layer_name": "LNDMRK", # name of ENC landmark layer
        "filter_col": "CATLMK",
        "filter_val": "19",  # turbines are 19 catlmk
        "output_name": "NOAA_ENC_WTG",
        "agol_item_id": turbine_id,
        "agol_layer_index": 0},

    "Submarine_Cables":{
        "layer_name": "CBLSUB", # name of ENC submarine cable layer  
        "filter_col": "CATCBL",
        "filter_val": 1, #power cables are 1 catcbl
        "output_name": "NOAA_ENC_PowerCables",
         "agol_item_id": cable_id,
         "agol_layer_index": 0},
    
    "Offshore_Substations":{
        "layer_name": "OFSPLF", # name of ENC offshore substation layer  
        "filter_col": None, # no filter for substations
        "filter_val": None,
        "output_name": "NOAA_ENC_OSS",
        "agol_item_id": substation_id,
        "agol_layer_index": 0},

    "Buoys":{
        "layer_name": "BOYSPP", # name of ENC buoy layer 
        "filter_col": None, # could filter using "CATSPM", need to determine correct filter values
        "filter_val": None,
        "output_name": "NOAA_ENC_Buoys",
        "agol_item_id": buoy_id,
        "agol_layer_index": 0},
}
