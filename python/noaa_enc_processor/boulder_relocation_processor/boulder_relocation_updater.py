######################################################
## FUNCTION TO PROCESS BOULDER RELOCATION FILES AND ##
##   UPDATE EXISTING AGOL HOSTED FEATURE SERVICES   ##
######################################################

import io
import zipfile
import requests
import json
from arcgis.gis import GIS
from pyproj import Transformer

def update_boulder_layer(gis, item_id, urls):
    all_esri_features = []

    # Setup Coordinate Transformer
    # From WGS84 (4326) to Web Mercator (3857)
    transformer = Transformer.from_crs("epsg:4326", "epsg:3857", always_xy=True)

    for url in urls:
        print(f"Downloading: {url}")
        # Determine project name based on url
        if "8/23/246" in url:
            project_name = "South Fork Wind"
        elif "8/28/130" in url:
            project_name = "Sunrise Wind"
        elif "8/29/88" in url:
            project_name = "Revolution Wind"
        elif "5/16/34" in url:
            project_name = "Vineyard Wind 1"
        else:
            project_name = ""  
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
                            
                            props = feat['properties']
                            # Map GeoJSON column name to AGOL column name
                            formatted_props = {
                                "Boulder_ID": props.get('name'),
                                "Information": props.get('description'),
                                "Project": project_name
                            }

                            # COORDINATE TRANSFORMATION
                            lon = feat['geometry']['coordinates'][0]
                            lat = feat['geometry']['coordinates'][1]
                            
                            # Convert degrees to meters
                            x_meters, y_meters = transformer.transform(lon, lat)
                                                       
                            esri_feat = {
                                "attributes": formatted_props,
                                "geometry": {
                                    "x": x_meters,
                                    "y": y_meters,
                                    "spatialReference": {"wkid": 102100} # Set to Web Mercator
                                }
                            }
                            all_esri_features.append(esri_feat)
                            

    if not all_esri_features:
        print("No features found.")
        return

    # Schema initalization 
    target_item = gis.content.get(item_id)
    flayer = target_item.layers[0]

    # 1. Define the columns to keep
    target_fields = [
        {"name": "Boulder_ID", "type": "esriFieldTypeString", "alias": "Boulder ID", "nullable": True},
        {"name": "Information", "type": "esriFieldTypeString", "alias": "Information", "nullable": True},
        {"name": "Project", "type": "esriFieldTypeString", "alias": "Project", "nullable": True} 
    ]

    # 2. Check if the layer is currently empty of fields
    if not flayer.properties.fields:
        print("Initializing layer schema...")
        flayer.manager.add_to_definition({
            "fields": target_fields
        })

    # 3. Filter features to ONLY include these attributes
    allowed_keys = [f['name'] for f in target_fields]
    
    cleaned_features = []
    for feat in all_esri_features:
        # Create a new attribute dict containing only allowed keys
        filtered_attributes = {k: v for k, v in feat['attributes'].items() if k in allowed_keys}
        feat['attributes'] = filtered_attributes
        cleaned_features.append(feat)

    # Conditional Delete: Only if records exist
    try:
        current_count = flayer.query(where="1=1", return_count_only=True)
        if current_count > 0:
            print(f"Found {current_count} existing features. Clearing layer...")
            flayer.delete_features(where="1=1")
        else:
            print("Layer is already empty. Skipping delete step.")
    except Exception:
        # If the layer is so new it has no table yet, query might fail
        print("Layer schema not yet initialized. Skipping delete step.")

    # UPLOAD: Push in batches of 1000 to avoid timeout/size errors
    print(f"Pushing {len(cleaned_features)} features to: {target_item.title}...")
    
    for i in range(0, len(cleaned_features), 1000):
        chunk = cleaned_features[i:i + 1000]
        result = flayer.edit_features(adds=chunk)
        
        # Check for errors in the batch
        if 'addResults' in result:
            fails = [r for r in result['addResults'] if not r['success']]
            if fails:
                print(f"Batch {(i//1000)+1} had {len(fails)} failures. Error: {fails[0].get('error')}")
    
    # Refesh the layer extent
    flayer.manager.refresh()
    target_item.update(item_properties={'extent': flayer.properties.extent})
    
    print("Sync complete.")
    print("Update complete.")