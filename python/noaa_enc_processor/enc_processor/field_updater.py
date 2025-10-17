#############################################
##       AGOL FIELD UPDATER FUNCTION       ##
#############################################

import pandas as pd
import json

def update_field_definitions(gis, mapper, layer_indices: list = [0]):
    """
    Connects to AGOL and updates feature layer field aliases and descriptions using CSVs.
    Formats descriptions as a JSON string for proper AGOL pop-up configuration.
    """
    try:
        for item_id, csv_path in mapper.items():
            print(f"--- Processing Item ID: {item_id} ---")
            
            try:
                # Get the feature layer item from AGOL
                item = gis.content.get(item_id)
                if not item:
                    print(f"Warning: Item ID {item_id} not found. Skipping.")
                    continue
                
                print(f"Found item: {item.title}")

                # Load and prepare the field definitions from the CSV
                try:
                    field_info_df = pd.read_csv(csv_path)
                    # Use fillna('') to handle empty cells for descriptions
                    field_lookup = field_info_df.fillna('').set_index('name').to_dict('index')
                except FileNotFoundError:
                    print(f"Error: CSV file not found at {csv_path}. Skipping item {item_id}.")
                    continue
                except KeyError:
                    print(f"Error: CSV at {csv_path} must contain a 'name' column. Skipping item {item_id}.")
                    continue

                # Loop through the specified layers in the feature service
                for index in layer_indices:
                    if index >= len(item.layers):
                        print(f"Warning: Layer index {index} is out of bounds for item {item_id}. Skipping this layer.")
                        continue

                    layer_to_update = item.layers[index]
                    print(f"Processing layer {index}: {layer_to_update.properties.name}")
                    
                    # Get the layer definition and modify it
                    layer_definition = layer_to_update.properties
                    fields_updated_count = 0

                    for field in layer_definition['fields']:
                        field_name = field['name']
                        if field_name in field_lookup:
                            # Update alias from the lookup dictionary
                            field['alias'] = field_lookup[field_name].get('alias', field['alias'])
                            
                            # Format description for AGOL pop-ups
                            # Get the simple text description from the CSV
                            simple_desc = field_lookup[field_name].get('description', '')

                            # Create the dictionary with the required structure
                            structured_desc_dict = {
                                "value": simple_desc,
                                "fieldValueType": ""
                            }
                            
                            # Convert the dictionary to a JSON formatted string
                            final_description_string = json.dumps(structured_desc_dict)
                            
                            # Assign the JSON string to the field's description
                            field['description'] = final_description_string

                            print(f"     - Staging update for field: {field_name}")
                            fields_updated_count += 1
                    
                    # Apply the changes if any were staged
                    if fields_updated_count > 0:
                        update_dict = {"fields": layer_definition['fields']}
                        layer_to_update.manager.update_definition(update_dict)
                        print(f"Successfully applied {fields_updated_count} updates to layer '{layer_to_update.properties.name}'.")
                    else:
                        print("No matching fields found in CSV. No updates needed for this layer.")

            except Exception as e:
                print(f"An error occurred while processing item {item_id}: {e}")

    except Exception as e:
        print(f"A critical error occurred: {e}")