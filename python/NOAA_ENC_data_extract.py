################################################################################################
##  This script extracts data from the NOAA Nautical Charts (ENCs) related to offshore wind   ##
##  activities, including turbine locations, cables, buoys, and offshore substations.         ##
##  It creates ArcGIS feature classes for each of the associated data layers.                 ##
################################################################################################

#IMPORT LIBRARIES 

import arcpy
import os
from osgeo import ogr
import requests
import zipfile

#CREATE VARIABLES

#create a list of ENCs to download
#list will need to be updated as more windfarms are constructed
charts_to_download = ["US4NY1BY.zip", "US4RI1CB.zip", "US4MA1CC.zip", "US4MA1CD.zip", 
"US4MA1BD.zip", "US4MA1CE.zip", "US4MA1DE.zip", "US4MA1DD.zip", "US4MA1DC.zip", "US4RI1DB.zip", 
"US4NY1CY.zip", "US4NJ1FF.zip", "US4NJ1FH.zip", "US4NJ1FG.zip", "US4NJ1FI.zip", "US4NY1BM.zip", 
"US4NY1BX.zip", "US4VA1AG.zip", "US4VA1AH.zip", "US4VA1AI.zip", "US4VA1AJ.zip", "US4VA1BG.zip", 
"US4VA1BH.zip", "US4VA1BI.zip", "US4VA1BJ.zip"]

#set a folder location on disc to download the chart data to 
current_dir = os.getcwd()
target_folder_path = os.path.join(current_dir, 'data', 'ENC')
print(f"Constructed path: {target_folder_path}")

enc_zip_folder = target_folder_path

#set a file geodatabase location to store the final feature classes
home_directory = os.path.expanduser('~')
output_gdb = os.path.join(home_directory, 'Documents', 'ArcGIS', 'Projects', 'NOAA_ENC', 'NOAA_ENC.gdb')

#ENC CHART DOWNLOAD FUNCTION
#create a function to extract data from the noaa charts enc website

def download_charts_to_disk(target_filenames, destination_folder):
    """
    Downloads a list of charts by constructing their URLs and puts them 
    on a folder on disc.

    target_filenames (list): A list of filenames to download.
    destination_folder (str): The path to the folder where files will be saved.
    """
    base_url = "https://charts.noaa.gov/ENCs/"

    # Ensure the destination folder exists before starting
    os.makedirs(destination_folder, exist_ok=True)
    
    print("--- Starting Download Process ---")
    print(f"Saving files to: {destination_folder}\n")

    # Keep track of successful and failed downloads
    successful_downloads = []
    failed_downloads = []

    for filename in target_filenames:
        #Construct the full URL and the full local path for the file
        file_url = f"{base_url}{filename}"
        output_filepath = os.path.join(destination_folder, filename)
        
        try:
            print(f"Downloading '{filename}'...")
            
            #Use stream=True to avoid loading the whole file into memory
            with requests.get(file_url, stream=True) as response:
                # Check if the download was successful (status code 200)
                response.raise_for_status() 
                
                #Open the local file in binary-write mode and save the content in chunks
                with open(output_filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192): # 8KB chunks
                        f.write(chunk)
                
                print(f"Successfully saved to '{output_filepath}'\n")
                successful_downloads.append(filename)

        except requests.exceptions.HTTPError as e:
            print(f"Failed to download '{filename}'. Server responded: {e.response.status_code} {e.response.reason}\n")
            failed_downloads.append(filename)
        except requests.exceptions.RequestException as e:
            print(f"Failed to download '{filename}'. An error occurred: {e}\n")
            failed_downloads.append(filename)

    print("--- Download complete! ---")
    print(f"Total successful: {len(successful_downloads)}")
    print(f"Total failed: {len(failed_downloads)}")

#run function
if __name__ == "__main__":

    # Run the function to download files directly to the target folder
    download_charts_to_disk(charts_to_download, enc_zip_folder)

#EXTRACT DATA FROM ENC FILES
#put all downloaded zip files into a list
try:
    zip_files = [f for f in os.listdir(enc_zip_folder) if f.lower().endswith('.zip')]
    if not zip_files:
        print(f"No .zip files found in '{enc_zip_folder}'.")
except FileNotFoundError:
    print(f"Error: The directory '{enc_zip_folder}' was not found.")
    zip_files = []


#define the features to extract from ENC
extraction_features = {
    "Wind_Turbines": {
        "layer_name": "LNDMRK", #name of ENC landmark layer
        "geometry_type": ogr.wkbPoint,
        "filter_attribute": "CATLMK",
        "filter_value": "19",  #turbines are 19 catlmk
        "output_name": "NOAA_ENC_WTG"},

    "Submarine_Cables":{
        "layer_name": "CBLSUB", #name of ENC submarine cable layer  
        "geometry_type": ogr.wkbLineString,
        "filter_attribute": "CATCBL",
        "filter_value": "1", #power cables are 1 catcbl
        "output_name": "NOAA_ENC_PowerCables" },
    
    "Offshore_Substations":{
        "layer_name": "OFSPLF", #name of ENC offshore substation layer  
        "geometry_type": ogr.wkbPoint,
        "filter_attribute": None, #no filter for substations
        "filter_value": None,
        "output_name": "NOAA_ENC_OSS"},

    "Buoys":{
        "layer_name": "BOYSPP", #name of ENC buoy layer 
        "geometry_type": ogr.wkbPoint,
        "filter_attribute": None, #not sure if there is a way to filter out only research buoys
        "filter_value": None,
        "output_name": "NOAA_ENC_Buoys"},
}
 

#create empty lists to put all the data from zip files
collected_features = {target: [] for target in extraction_features}
schemas = {target: None for target in extraction_features}

#Loop through each zip file to find the .000 file (ENC file)
#Then, find data within ENC file and put into collected features dictionary

for zip_filename in sorted(list(set(charts_to_download))):
    print(f"\n--- Reading {zip_filename} ---")
    full_zip_path = os.path.join(enc_zip_folder, zip_filename)
    
    # Use a try-except block for file operations
    try:
        with zipfile.ZipFile(full_zip_path, 'r') as zf:
            enc_file_in_zip = next((name for name in zf.namelist() if name.upper().endswith('.000')), None)
    except zipfile.BadZipFile:
        print(f"  Error: '{zip_filename}' is a corrupted zip file.")
        continue

    if not enc_file_in_zip:
        print(f"  Error: Could not find a .000 file inside '{zip_filename}'.")
        continue

    path_to_open = f"/vsizip/{full_zip_path}/{enc_file_in_zip}"
    data_source = ogr.Open(path_to_open, 0)
    if data_source is None:
        print(f"  Error: GDAL failed to open the data source at '{path_to_open}'.")
        continue

    # Loop through each of the defined extraction features (turbines, cables, OSS, buoys)
    for target_name, params in extraction_features.items():
        layer = data_source.GetLayerByName(params["layer_name"])
        
        if layer and layer.GetFeatureCount() > 0:
            print(f"  Found '{params['layer_name']}' layer with {layer.GetFeatureCount()} features. Checking for '{target_name}'...")
            
            # Capture the schema for this layer type 
            if schemas[target_name] is None:
                schemas[target_name] = []
                layer_defn = layer.GetLayerDefn()
                for i in range(layer_defn.GetFieldCount()):
                    field_defn = layer_defn.GetFieldDefn(i)
                    schemas[target_name].append({
                        "name": field_defn.GetName(), "type": field_defn.GetType(), "width": field_defn.GetWidth()
                    })
                print(f"Captured schema for {target_name}.")

            # Process features in the layer
            for source_feature in layer:
                geom = source_feature.GetGeometryRef()
                if geom and geom.GetGeometryType() == params["geometry_type"]:
                    
                    # Extract attributes and handle list-to-string conversion
                    attributes = {}
                    for field_info in schemas[target_name]:
                        field_name = field_info["name"]
                        value = source_feature.GetField(field_name)
                        attributes[field_name] = ", ".join(map(str, value)) if isinstance(value, list) else value

                    # Apply the filter for the current target
                    # Check if a filter is defined for this target
                    filter_attr = params.get("filter_attribute")

                    # By default, assume the feature doesn't pass unless a condition is met
                    passes_filter = False

                    if not filter_attr:
                        # If no filter attribute is specified, the feature automatically passes
                        passes_filter = True
                    else:
                        # Otherwise, apply the defined filter
                        filter_val = attributes.get(filter_attr)
                        if filter_val and params["filter_value"] in str(filter_val):
                            passes_filter = True

                    # If the feature passed the filter (or if no filter was needed), add it to collected features dictionary
                    if passes_filter:
                        feature_data = {
                            "geometry": geom.ExportToWkt(), # Export geometry as WKT
                            "attributes": attributes,
                            "source_file": zip_filename
                        }
                        collected_features[target_name].append(feature_data)

    data_source = None # Close the file

#WRITE DATA TO FEATURE CLASSES
arcpy.env.workspace = output_gdb
arcpy.env.overwriteOutput = True

#Loop through the targets and create a feature class for each one
for target_name, features in collected_features.items():
    if not features:
        print(f"\nNo '{target_name}' features were found to process.")
        continue

    params = extraction_features[target_name]
    output_name = params["output_name"]
    schema_definition = schemas[target_name]
    
    print(f"\nProcessing {len(features)} '{target_name}' features...")
    print(f"Creating feature class: {output_name}")

    try:
        # Determine geometry type for ArcPy
        geometry_type = "POLYLINE" if params["geometry_type"] == ogr.wkbLineString else "POINT"
        
        arcpy.CreateFeatureclass_management(output_gdb, output_name, geometry_type, spatial_reference=arcpy.SpatialReference(4326))

        # Add fields based on the captured schema
        ogr_to_arcpy_type = {ogr.OFTInteger: "LONG", ogr.OFTReal: "DOUBLE", ogr.OFTString: "TEXT", ogr.OFTDate: "DATE", ogr.OFTDateTime: "DATE"}
        for field_info in schema_definition:
            ogr_type = field_info["type"]
            arcpy_type = "TEXT" if ogr_type in [ogr.OFTIntegerList, ogr.OFTStringList] else ogr_to_arcpy_type.get(ogr_type, "TEXT")
            field_length = 255 if arcpy_type == "TEXT" else None
            arcpy.AddField_management(output_name, field_info["name"], arcpy_type, field_length=field_length)
        
        arcpy.AddField_management(output_name, "SOURCE_FILE", "TEXT", field_length=50)

        # Insert the features
        field_names = ["SHAPE@WKT"] + [f["name"] for f in schema_definition] + ["SOURCE_FILE"]
        with arcpy.da.InsertCursor(output_name, field_names) as cursor:
            for feature_data in features:
                row = [feature_data["geometry"]] + [feature_data["attributes"].get(f["name"]) for f in schema_definition] + [feature_data["source_file"]]
                cursor.insertRow(row)

        final_count = int(arcpy.management.GetCount(output_name)[0])
        print(f"Successfully wrote {final_count} features to '{output_name}'.")

    except Exception as e:
        print(f"An error occurred while creating '{output_name}': {e}")