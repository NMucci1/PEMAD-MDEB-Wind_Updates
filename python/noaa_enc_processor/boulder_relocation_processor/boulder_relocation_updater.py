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
    all_esri_features = []

    for url in urls:
        print(f"Downloading: {url}")
        response = requests.get(url)
        if response.status_code != 200:
            continue
            
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            for filename in z.namelist():
                if filename.endswith('.geojson'):
                    with z.open(filename) as f:
                        gj_data = json.load(f)
                        
                        # Convert GeoJSON Feature to Esri Feature format
                        for feat in gj_data['features']:
                            esri_feat = {
                                "attributes": feat['properties'],
                                "geometry": {
                                    "x": feat['geometry']['coordinates'][0],
                                    "y": feat['geometry']['coordinates'][1],
                                    "spatialReference": {"wkid": 4326}
                                }
                            }
                            all_esri_features.append(esri_feat)

    if not all_esri_features:
        print("No features found.")
        return

    target_item = gis.content.get(item_id)
    flayer = target_item.layers[0]

    # Conditional Delete: Only if records exist
    try:
        current_count = flayer.query(where="1=1", return_count_only=True)
        if current_count > 0:
            print(f"Found {current_count} existing features. Clearing layer...")
            flayer.delete_features(where="1=1")
        else:
            print("Layer is already empty. Skipping delete step.")
    except Exception as e:
        # If the layer is so new it has no table yet, query might fail
        print("Layer schema not yet initialized. Skipping delete step.")

    # UPLOAD: Push in batches of 1000 to avoid timeout/size errors
    print(f"Pushing {len(all_esri_features)} features to: {target_item.title}...")
    
    for i in range(0, len(all_esri_features), 1000):
        chunk = all_esri_features[i:i + 1000]
        result = flayer.edit_features(adds=chunk)
        
        # Check for errors in the batch
        if 'addResults' in result:
            fails = [r for r in result['addResults'] if not r['success']]
            if fails:
                print(f"Batch {(i//1000)+1} had {len(fails)} failures. Error: {fails[0].get('error')}")
    
    print("Sync complete.")
    print("Update complete.")