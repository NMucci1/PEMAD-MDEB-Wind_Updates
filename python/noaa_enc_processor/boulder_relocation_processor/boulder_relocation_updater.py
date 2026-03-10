######################################################
## FUNCTION TO PROCESS BOULDER RELOCATION FILES AND ##
##   UPDATE EXISTING AGOL HOSTED FEATURE SERVICES   ##
######################################################

import io
import zipfile
import requests
import pandas as pd
import json
import os
import tempfile
from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection

def update_boulder_layer(gis, item_id, urls):
    all_features = []

    # Collect all features from all URLs into one list
    for url in urls:
        print(f"Downloading: {url}")
        response = requests.get(url)
        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for filename in z.namelist():
                    if filename.endswith('.geojson'):
                        with z.open(filename) as f:
                            data = json.load(f)
                            all_features.extend(data['features'])
        else:
            print(f"Failed to download {url}")

    if not all_features:
        print("No data found in URLs.")
        return

    # Create a combined GeoJSON object
    combined_geojson = {
        "type": "FeatureCollection",
        "features": all_features
    }

    # Save to a temporary file
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "data.geojson")
        with open(temp_file_path, 'w') as f:
            json.dump(combined_geojson, f)

        # Access the existing item
        target_item = gis.content.get(item_id)
        
        # Check if the item is empty/new
        print(f"Syncing data to: {target_item.title}...")
        flc = FeatureLayerCollection.fromitem(target_item)
        
        try:
            result = flc.manager.overwrite(temp_file_path)
            print(f"Successfully initialized/updated layer: {result}")
        except Exception as e:
            print(f"Error during sync: {e}")
            
    print("Update complete.")