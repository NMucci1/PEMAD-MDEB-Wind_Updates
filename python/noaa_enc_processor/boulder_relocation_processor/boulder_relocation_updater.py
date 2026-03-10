######################################################
## FUNCTION TO PROCESS BOULDER RELOCATION FILES AND ##
##   UPDATE EXISTING AGOL HOSTED FEATURE SERVICES   ##
######################################################

import io
import zipfile
import requests
import json
from arcgis.gis import GIS

def update_boulder_layer(gis, item_id, urls):
    all_features = []

    # Download and parse GeoJSONs into features
    for url in urls:
        print(f"Downloading: {url}")
        try:
            response = requests.get(url)
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for filename in z.namelist():
                    if filename.endswith('.geojson'):
                        with z.open(filename) as f:
                            data = json.load(f)
                            # Add all features to master list
                            all_features.extend(data['features'])
        except Exception as e:
            print(f"Failed to process {url}: {e}")

    if not all_features:
        print("No features found to upload.")
        return

    # Get the specific layer object
    target_item = gis.content.get(item_id)
    flayer = target_item.layers[0] 

    print(f"Pushing {len(all_features)} features to: {target_item.title}...")

    # Clear existing features before adding new ones
    #flayer.delete_features(where="1=1")
    # Use add_features to populate the empty service
    result = flayer.edit_features(adds=all_features)
    
    # Check the result for success
    if result['addResults']:
        success_count = len([r for r in result['addResults'] if r['success']])
        print(f"Successfully added {success_count} features.")
    else:
        print(f"Update failed: {result}")
            
    print("Update complete.")