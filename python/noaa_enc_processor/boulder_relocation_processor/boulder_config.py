#############################################
##        CONFIGURATION VARIABLES          ##
#############################################

from pathlib import Path
import os

# Map the URL to the specific Project Name
boulder_projects = {
    "https://www.quintham.com//EMIN/8/23/246/GeoJson.zip": "South Fork Wind",
    "https://www.quintham.com//EMIN/8/28/130/GeoJson.zip": "Sunrise Wind",
    "https://www.quintham.com//EMIN/8/29/88/GeoJson.zip": "Revolution Wind",
    "https://www.quintham.com//EMIN/5/16/34/GeoJson.zip": "Vineyard Wind 1"
}

urls_to_process = list(boulder_projects.keys())

boulder_agol_id = os.getenv("BOULDER_ITEM_ID")