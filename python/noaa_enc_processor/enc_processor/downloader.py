#############################################
##     ENC CHART DOWNLOAD FUNCTION         ##
#############################################

import os
import requests
from pathlib import Path 

def download_charts_to_disk(target_filenames, destination_folder):
    """
    Downloads a list of charts from NOAA, overwriting any existing files.
    """
    base_url = "https://charts.noaa.gov/ENCs/"
    # Ensure destination_folder is a Path object for modern path handling
    destination_folder = Path(destination_folder)
    os.makedirs(destination_folder, exist_ok=True)
    
    print(f"--- Starting Download Process --- Saving files to: {destination_folder}.")
    
    for filename in target_filenames:
        file_url = f"{base_url}{filename}"
        output_filepath = destination_folder / filename
        
        # Check if file exists
        if output_filepath.exists():
            print(f"{filename} exists. Overwriting with the latest version...")
        else:
            print(f"Downloading new file: {filename}...")
            
        try:
            with requests.get(file_url, stream=True) as response:
                response.raise_for_status() # Raise an exception for bad status codes
                with open(output_filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):  # Download in chunks
                        f.write(chunk)
            print(f"Successfully saved {filename}.")
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {filename}. Error: {e}.")
            
    print("--- Download complete! ---")