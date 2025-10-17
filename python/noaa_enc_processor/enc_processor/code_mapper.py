#####################################################
##            CODE MAPPING FUNCTION                ##
#####################################################

import pandas as pd
import numpy as np
import os

def map_column_codes_with_logging(gdf, csv_path):
    """
    Replaces coded values in a GeoDataFrame with string values from a mapping CSV.
    """
    print(f"Applying code-to-value mapping from {os.path.basename(csv_path)}...")
    try:
        # Read in the data dictionary csv, make sure code col is read as type string
        mapping_df = pd.read_csv(csv_path, dtype={'code': str}, engine='python')
    except FileNotFoundError:
        print("Error: Mapping file not found. Aborting mapping.")
        return gdf

    mapping_dict = {}
    # Loop through each unique column name found in the mapping CSV
    for col_name in mapping_df['column_name'].unique():
        # Check if the column exists in the GDF
        if col_name not in gdf.columns:
            print(f"Column {col_name} from CSV not in GDF. Skipping.")
            continue

        target_dtype = gdf[col_name].dtype
        col_map_df = mapping_df[mapping_df['column_name'] == col_name].copy()
        
        # Robust type handling
        if np.issubdtype(target_dtype, np.number):
            col_map_df['code'] = pd.to_numeric(col_map_df['code'], errors='coerce')
            col_map_df.dropna(subset=['code'], inplace=True)

        try:
            col_map_df['code'] = col_map_df['code'].astype(target_dtype)
        except (ValueError, TypeError):
 
            print(f"Warning: Type conversion failed for {col_name}. Skipping.")
            continue
            
        mapping_dict[col_name] = pd.Series(
            col_map_df.value.values, index=col_map_df.code
        ).to_dict()
        
        print(f"Success: Prepared mapping for column {col_name}.")

    gdf_copy = gdf.copy()
    gdf_copy.replace(mapping_dict, inplace=True)
    
    print("Mapping complete.")
    return gdf_copy