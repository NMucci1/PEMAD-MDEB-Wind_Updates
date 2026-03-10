######################################################
## FUNCTION TO PROCESS BOULDER RELOCATION FILES AND ##
##   UPDATE EXISTING AGOL HOSTED FEATURE SERVICES   ##
######################################################

import io
import zipfile
import requests
import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

def update_boulder_layer(gis, item_id, urls):
    """
    Downloads zipped GeoJSONs, merges them in memory, and overwrites an AGOL layer.
      """
    all_dfs = []

    for url in urls:
        print(f"Downloading: {url}")
        response = requests.get(url)
        
        if response.status_code == 200:
            # Open the zip file in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Assuming there is one .geojson file per zip
                for filename in z.namelist():
                    if filename.endswith('.geojson'):
                        with z.open(filename) as f:
                            # Read GeoJSON into a Spatially Enabled DataFrame
                            geojson_data = f.read().decode('utf-8')
                            # Temporary save or direct load depending on pandas version
                            # SEDF can read directly from JSON strings via GeoAccessor
                            df = pd.read_json(io.StringIO(geojson_data))
                            all_dfs.append(df)
        else:
            print(f"Failed to download {url}: {response.status_code}")

    if not all_dfs:
        print("No data was collected.")
        return

    # Merge all dataframes into one
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Get the existing Feature Layer Item
    target_item = gis.content.get(item_id)
    flc = FeatureLayerCollection.fromitem(target_item)

    # Overwrite the service
    print(f"Overwriting layer: {target_item.title}...")
    
    # Using the manager to overwrite with the dataframe directly
    flc.manager.overwrite(final_df)
    
    print("Update complete.")