#####################################################
## FUNCTION TO PROCESS ENCS FILES INTO SEDFS AND   ##
##  UPDATE EXISTING AGOL HOSTED FEATURE SERVICES   ##
#####################################################

import pandas as pd
import geopandas as gpd
import zipfile
import tempfile
import os
import fiona
from arcgis.features import FeatureLayerCollection
from .enc_preprocessor import read_enc_layer
from .code_mapper import map_column_codes

def process_and_update_features(gis, data_dir, feature_config):
    """
    Processes ENC data from ZIP files, extracts specified features, and creates SEDF 
    (spatially enabled dataframe). Then, utilizes SEDF to update corresponding hosted 
    feature layers in AGOL.
    """
    feature_results = {key: [] for key in feature_config}

    # Loop through ZIP files and extract data
    print("Starting data extraction from ENC files...")
    for zip_file in os.listdir(data_dir):
        if not zip_file.endswith(".zip"):
            continue

        print(f"Processing source file: {zip_file}")
        # Create temp folder and extract data from zip to temp folder
        zip_path = os.path.join(data_dir, zip_file)
        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(temp_dir)
            except Exception as e:
                print(f"Failed to unzip {zip_file}: {e}")
                continue
            # Walk through all folders in temp folder to find .000 file    
            enc_files = [os.path.join(root, file) for root, _, files in os.walk(temp_dir) for file in files if file.endswith(".000")]
            if not enc_files:
                print(f"No .000 ENC file found in {zip_file}")
                continue
            # Read first file in folder that has .000 file extentions
            # There should only be one file with this extention
            enc_path = enc_files[0]
            try:
                layers = fiona.listlayers(enc_path)
            except Exception as e:
                print(f"Could not read layers in {zip_file}: {e}")
                continue
            # Get layer from extraction_features, set in config.py  
            for name, info in feature_config.items():
                layer_name = info["layer_name"]
                if layer_name not in layers:
                    continue

                try:
                    print(f"Calling read_enc_layer function for layer: {layer_name}")
                    # Call the function to convert any list columns to comma-separated strings
                    gdf = read_enc_layer(enc_path, layer_name)
                    
                    if gdf.empty:
                        continue
                    # Filter using the columns and values from extraction_features, set in config.py
                    filter_col = info.get("filter_col")
                    filter_val = info.get("filter_val")
                    if filter_col and filter_val and filter_col in gdf.columns:
                        gdf = gdf[gdf[filter_col] == filter_val]

                    if not gdf.empty:
                        # Add a column to the gdf stating the source file name
                        gdf["source_file"] = os.path.basename(zip_file)
                        feature_results[name].append(gdf)
                        print(f"Found {len(gdf)} features for '{name}' in layer '{layer_name}'")

                except Exception as e:
                    print(f"Error processing layer '{layer_name}' from {zip_file}: {e}")

    # Consolidate data and update AGOL directly
    print("Finished extraction. Starting ArcGIS Online updates...")
    for name, gdf_list in feature_results.items():
        if not gdf_list:
            print(f"[{name}] No data was extracted. Skipping AGOL update.")
            continue

        try:
            # Combine all alike features into one gdf
            full_gdf = gpd.GeoDataFrame(pd.concat(gdf_list, ignore_index=True), crs=gdf_list[0].crs)
            print(f"[{name}] Successfully combined {len(full_gdf)} total features.")
            print(gdf.head())
        except Exception as e:
            print(f"[{name}] failed to combine GeoDataFrames: {e}")
            continue
        
        mapping_csv_path = feature_config[name].get("mapping_csv")

        # Call mapping function to convert any coded data into a non-coded, readable value    
        if mapping_csv_path:
            # If a mapping file is defined in the config, call the function
            # and overwrite full_gdf with the processed result.
            full_gdf = map_column_codes(full_gdf, mapping_csv_path)
        else:
            print(f"[{name}] No mapping CSV configured. Proceeding with original data.")

        # Define AGOL item ID
        agol_id = feature_config[name].get("agol_item_id")
        if not agol_id:
            print(f"[{name}] No AGOL item ID configured. Skipping upload.")
            continue
            
        try:
            print(f"[{name}] Connecting to AGOL item {agol_id}...")
            # Get AGOL item using item ID
            item = gis.content.get(agol_id)
            flc = FeatureLayerCollection.fromitem(item)
            agol_layer_index = feature_config[name].get("agol_layer_index", 0)
            # Get target layer to be updated
            target_layer = flc.layers[agol_layer_index]

            # Define layer coordinate system and reproject data to match 
            target_crs_wkid = target_layer.properties.extent['spatialReference']['wkid']
            print(f"[{name}] Reprojecting data to match AGOL layer (EPSG:{target_crs_wkid})...")
            full_gdf = full_gdf.to_crs(epsg=target_crs_wkid)
            
            # Convert gdf to spatially enabled dataframe
            print(f"[{name}] Converting to Spatially Enabled DataFrame...")
            sdf = pd.DataFrame.spatial.from_geodataframe(full_gdf)
            
            # Delete existing features from AGOL layer
            print(f"[{name}] Truncating all existing features in AGOL layer...")
            target_layer.manager.truncate()
            
            # Add new features to layer using spatially enabled dataframe
            print(f"[{name}] Appending {len(sdf)} new features to AGOL layer...")
            result = target_layer.edit_features(adds=sdf)
            
            # Check results
            add_results = result.get('addResults', [])
            add_errors = [r['error'] for r in add_results if not r['success']]
            
            if not add_errors:
                print(f"[{name}] AGOL update successful.")
            else:
                print(f"[{name}] failed to add {len(add_errors)} features.")
                print(f"Example error: {add_errors[0]}")

        except Exception as e:
            print(f"[{name}] An unexpected error occurred during the AGOL update process: {e}")