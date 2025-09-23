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
from arcgis.gis import GIS
import shutil
import tempfile

#CREATE VARIABLES

#create a list of ENCs to download
#list will need to be updated as more windfarms are constructed
#US5RI1BE is in band 5 (block island wind farm submarine cables are in this band), all other data is band 4 (approach band)
charts_to_download = ["US4NY1BY.zip", "US4RI1CB.zip", "US4MA1CC.zip", "US4MA1CD.zip", 
"US4MA1CE.zip", "US4MA1DE.zip", "US4MA1DD.zip", "US4MA1DC.zip", "US4RI1DB.zip", 
"US4NY1CY.zip", "US4NJ1FH.zip", "US4NJ1FG.zip", "US4NY1BM.zip", "US5RI1BE.zip", 
"US4NY1BX.zip", "US4VA1AG.zip", "US4VA1AH.zip", "US4VA1AI.zip", "US4VA1BG.zip", 
"US4VA1BH.zip", "US4VA1BI.zip"]

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

    #ensure the destination folder exists before starting
    os.makedirs(destination_folder, exist_ok=True)
    
    print("--- Starting Download Process ---")
    print(f"Saving files to: {destination_folder}\n")

    #keep track of successful and failed downloads
    successful_downloads = []
    failed_downloads = []

    for filename in target_filenames:
        #construct the full URL and the full local path for the file
        file_url = f"{base_url}{filename}"
        output_filepath = os.path.join(destination_folder, filename)
        
        try:
            print(f"Downloading '{filename}'...")
            
            #use stream=True to avoid loading the whole file into memory
            with requests.get(file_url, stream=True) as response:
                #Check if the download was successful (status code 200)
                response.raise_for_status() 
                
                #open the local file in binary-write mode and save the content in chunks
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

    #run the function to download files directly to the target folder
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
 

#create empty dictionaries to put all the data from zip files
collected_features = {target: [] for target in extraction_features}
schemas = {target: None for target in extraction_features}

#loop through each zip file to find the .000 file (ENC file)
#then, find data within ENC file and put into collected features dictionary

for zip_filename in sorted(list(set(charts_to_download))):
    print(f"\n--- Reading {zip_filename} ---")
    full_zip_path = os.path.join(enc_zip_folder, zip_filename)
    
    #use a try-except block for file operations
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
        print(f"Error: GDAL failed to open the data source at '{path_to_open}'.")
        continue

    #loop through each of the defined extraction features (turbines, cables, OSS, buoys)
    for target_name, params in extraction_features.items():
        layer = data_source.GetLayerByName(params["layer_name"])
        
        if layer and layer.GetFeatureCount() > 0:
            print(f"  Found '{params['layer_name']}' layer with {layer.GetFeatureCount()} features. Checking for '{target_name}'...")
            
            #capture the schema for this layer 
            if schemas[target_name] is None:
                schemas[target_name] = []
                layer_defn = layer.GetLayerDefn()
                for i in range(layer_defn.GetFieldCount()):
                    field_defn = layer_defn.GetFieldDefn(i)
                    schemas[target_name].append({
                        "name": field_defn.GetName(), "type": field_defn.GetType(), "width": field_defn.GetWidth()
                    })
                print(f"Captured schema for {target_name}.")

            #process features in the layer
            for source_feature in layer:
                geom = source_feature.GetGeometryRef()
                if geom and geom.GetGeometryType() == params["geometry_type"]:
                    
                    #extract attributes and handle list-to-string conversion
                    #some ENC columns store data as lists, ArcGIS feature classes don't like this
                    attributes = {}
                    for field_info in schemas[target_name]:
                        field_name = field_info["name"]
                        value = source_feature.GetField(field_name)
                        attributes[field_name] = ", ".join(map(str, value)) if isinstance(value, list) else value

                    #apply the filter for the current target (from the extraction_features)
                    #check if a filter is defined for this target
                    filter_attr = params.get("filter_attribute")

                    #by default, assume the feature doesn't pass unless a condition is met
                    passes_filter = False

                    if not filter_attr:
                        #if no filter attribute is specified, the feature automatically passes
                        passes_filter = True
                    else:
                        #otherwise, apply the defined filter
                        filter_val = attributes.get(filter_attr)
                        if filter_val and params["filter_value"] in str(filter_val):
                            passes_filter = True

                    #if the feature passed the filter (or if no filter was needed), add it to collected features dictionary
                    if passes_filter:
                        feature_data = {
                            "geometry": geom.ExportToWkt(), # Export geometry as WKT
                            "attributes": attributes,
                            "source_file": zip_filename
                        }
                        collected_features[target_name].append(feature_data)

    data_source = None #close the file

#WRITE DATA TO LOCAL FEATURE CLASSES AND PUBLISH/UPDATE HOSTED FEATURE SERVICES ON AGOL

#authenticate AGOL credentials
gis= GIS("PRO")

#set local file geodatabase parameters
arcpy.env.workspace = output_gdb
arcpy.env.overwriteOutput = True

#loop through the targets to create a feature class and publish for each one
for target_name, features in collected_features.items():
    if not features:
        print(f"\nNo '{target_name}' features were found to process.")
        continue

    #get parameters and create the local feature class
    params = extraction_features[target_name]
    output_name = params["output_name"]
    final_fc_path = os.path.join(output_gdb, output_name)
    
    print(f"\nProcessing {len(features)} '{target_name}' features...")
    
    final_count = int(arcpy.management.GetCount(final_fc_path)[0])
    print(f"Successfully wrote {final_count} features to '{final_fc_path}'.")

    #publish to AGOL or update if feature service is already published
    print(f"Starting ArcGIS Online process for '{output_name}'...")
    try:
        # Search for the item on AGOL to see if it already exists
        search_result = gis.content.search(f"title:'{output_name}'", item_type="Feature Service")


        # Package the feature class into a zipped File GDB for uploading
        with tempfile.TemporaryDirectory() as temp_dir:
            gdb_name = "data.gdb"
            gdb_path = os.path.join(temp_dir, gdb_name)
            arcpy.CreateFileGDB_management(temp_dir, gdb_name)
            arcpy.CopyFeatures_management(final_fc_path, os.path.join(gdb_path, output_name))
            
            zip_path = os.path.join(temp_dir, f"{output_name}.zip")
            shutil.make_archive(os.path.join(temp_dir, output_name), 'zip', temp_dir, gdb_name)

            # Check if the service exists and decide whether to update or publish
            ##### NEEDS WORK #######
            if search_result:
                # UPDATE (Overwrite) existing service
                print(f"  Service '{output_name}' found. Overwriting data...")
                feature_service = search_result[0]
                manager = feature_service.manager
                feature_service.overwrite(zip_path)
                print("Successfully updated service.")
            else:
                #publish new service
                print(f"Service '{output_name}' not found. Publishing new service...")
                item_properties = {
                    "title": output_name,
                    "type": "File Geodatabase"
                }
                fgdb_item = gis.content.add(item_properties, data=zip_path)
                published_item = fgdb_item.publish()

                print(f"  Successfully published '{published_item.title}'.")
                fgdb_item.delete() # Clean up the uploaded GDB item

    except Exception as e:
        print(f"An error occurred with service '{output_name}': {e}")