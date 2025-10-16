#############################################
##  FUNCTION TO CONVERT COLUMNS IN ENC     ##
##  FROM LIST TO COMMA-SEPARATED STRING    ##
#############################################

import geopandas as gpd
import fiona

def read_enc_layer(data_dir, layer_name):
    """
    Reads an ENC layer while converting list-type fields to strings
    to prevent them from being skipped by GeoPandas. This ensures all
    attribute columns are retained.
    """
    processed_features = []
    
    try:
        with fiona.open(data_dir, layer=layer_name) as source:
            # Define the coordinate system from the source file
            source_crs = source.crs

            for feature in source:
                properties = feature['properties']
                
                for key, value in list(properties.items()):
                    # Convert any list-type cols to a comma-separated string col
                    if isinstance(value, list):
                        print(f"Found and converted list in field: '{key}'")
                        properties[key] = ', '.join(map(str, value))
                
                feature['properties'] = properties
                processed_features.append(feature)

        if not processed_features:
            return gpd.GeoDataFrame([], crs=source_crs)

        gdf = gpd.GeoDataFrame.from_features(processed_features, crs=source_crs)
        return gdf
        
    except Exception as e:
        print(f"Critical error in read_enc_layer for layer '{layer_name}': {e}")
        return gpd.GeoDataFrame([])