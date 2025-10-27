#####################################################
##            CODE MAPPING FUNCTION                ##
#####################################################

import pandas as pd
import numpy as np
import os

def map_column_codes(gdf, csv_path):
    """
    Replaces coded values in a GeoDataFrame with string values from a mapping CSV.
    Handles both single values and comma-separated string values.
    """
    print(f"Applying code-to-value mapping from {os.path.basename(csv_path)}...")
    try:
        # Read in the data dictionary csv, make sure code col is read as type string
        mapping_df = pd.read_csv(csv_path, dtype={'code': str}, engine='python')
    except FileNotFoundError:
        print("Error: Mapping file not found. Aborting mapping.")
        return gdf

    # Fix incorrect reading of csv 'code' column
    # Pandas can read numeric-looking strings as float then cast to
    # string, resulting in '19.0' instead of '19'. Clean this to 
    # ensures codes are clean strings (e.g., '19') before any maps are built.
    mapping_df['code'] = mapping_df['code'].astype(str).str.replace(r'\.0$', '', regex=True)

    mapping_dict = {}
    # Loop through each unique column name found in the mapping CSV
    for col_name in mapping_df['column_name'].unique():
        # Check if the column exists in the GDF
        if col_name not in gdf.columns:
            print(f"Column {col_name} from CSV not in GDF. Skipping.")
            continue

        target_dtype = gdf[col_name].dtype
        col_map_df = mapping_df[mapping_df['column_name'] == col_name].copy()
        
        # Keep a version of the codes as strings for object/string mapping
        # This is crucial for the comma-separated values
        string_code_map = pd.Series(
            col_map_df.value.values, index=col_map_df.code
        ).to_dict()
        
        # Robust type handling for non-string columns
        if np.issubdtype(target_dtype, np.number):
            col_map_df['code'] = pd.to_numeric(col_map_df['code'], errors='coerce')
            col_map_df.dropna(subset=['code'], inplace=True)
            try:
                col_map_df['code'] = col_map_df['code'].astype(target_dtype)
            except (ValueError, TypeError):
                print(f"Warning: Type conversion failed for {col_name} numeric. Skipping.")
                continue
        elif target_dtype != 'object':
             # Handle other types if necessary, e.g., boolean
            try:
                col_map_df['code'] = col_map_df['code'].astype(target_dtype)
            except (ValueError, TypeError):
                print(f"Warning: Type conversion failed for {col_name} non-object. Skipping.")
                continue

        # Create the specific mapping for this column
        # If target is object, index will be strings (e.g., '11', '14')
        # If target is int, index will be int (e.g., 11, 14)
        col_map = pd.Series(
            col_map_df.value.values, index=col_map_df.code
        ).to_dict()
        
        # Store both the potentially type-converted map and the pure string map
        mapping_dict[col_name] = {
            'map': col_map,
            'string_map': string_code_map, # Used for comma-separated strings
            'dtype': target_dtype
        }
        
        print(f"Success: Prepared mapping for column {col_name}.")

    gdf_copy = gdf.copy()

    # Define a helper function for string mapping
    def map_string_values(cell_value, string_mapping_dict):
        """
        Helper to split, map, and re-join comma-separated string values.
        See column COLOUR for example of comma-separated string values in data.
        """
        if pd.isna(cell_value) or not isinstance(cell_value, str):
            return cell_value
        
        # Split the string, strip whitespace from each part
        split_values = [v.strip() for v in cell_value.split(',')]
        
        # Map each value, keeping original if not in dict. Convert to string.
        mapped_values = [str(string_mapping_dict.get(v, v)) for v in split_values]
        
        # Join them back together
        return ', '.join(mapped_values)

    # Iterate through the prepared mappings and apply them
    for col_name, maps in mapping_dict.items():
        if col_name not in gdf_copy.columns:
            continue
            
        # If the target column is a string/object type, use the split-and-map logic
        if maps['dtype'] == 'object':
            print(f"Applying string-based mapping to {col_name}...")
            # Use the string_map, which guarantees string keys ('11', '14', etc.)
            gdf_copy[col_name] = gdf_copy[col_name].apply(
                lambda x: map_string_values(x, maps['string_map'])
            )
        else:
            # Otherwise, use the standard (faster) replace method
            print(f"Applying standard mapping to {col_name}...")
            gdf_copy[col_name] = gdf_copy[col_name].replace(maps['map'])
    
    print("Mapping complete.")
    return gdf_copy