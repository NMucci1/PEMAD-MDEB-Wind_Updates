#############################################
##      MAIN SCRIPT TO RUN WORKFLOW        ##
#############################################

import os
from dotenv import load_dotenv
from pathlib import Path
from arcgis.gis import GIS
from enc_processor import config, downloader, processor, field_updater

def run_workflow():
    """
    Executes the full workflow: download ENCs, process files, update AGOL features services,
    and update feature service field defintions.

    """
    # 1. Define .env variables to log into AGOL
    env_path = Path.home()/'.config'/'secrets'/'.env' 
    load_dotenv(dotenv_path=env_path)
    # Get credentials securely using os.getenv()
    portal_url = os.getenv("ARCGIS_URL")
    client_id = os.getenv("ARCGIS_CLIENT_ID")
    client_secret = os.getenv("ARCGIS_CLIENT_SECRET")

    # 2. Connect to ArcGIS Online
    try:
        print("---  Connecting to ArcGIS Online...  ---")
        gis = GIS(url=portal_url, client_id=client_id, client_secret=client_secret)
        #gis = GIS("PRO")
        print("---  Successfully connected  ---")
    except Exception as e:
        print(f"Could not connect to ArcGIS Online. Error: {e}")
        return

    # 3. Download the chart data using the downloader
    downloader.download_charts_to_disk(
        config.charts_to_download, 
        config.target_folder_path
    )

    # 4. Process the downloaded files and update AGOL
    processor.process_and_update_features(
        gis=gis,
        data_dir=config.target_folder_path,
        feature_config=config.extraction_features
    )

    # 5. Update the AGOL field names (aliases) and descriptions for increased user interoperability
    field_updater.update_field_definitions(
        gis=gis,
        mapper = config.item_id_csv_map
    )

# Run the workflow
if __name__ == "__main__":
    run_workflow()
    print("Workflow complete.")