import pandas as pd

def update_field_definitions(gis, map):
    """
    Connects to AGOL and updates feature layer field aliases and descriptions from CSVs.
    """
    try:

        for item_id, csv_path in map.items():
            print(f"--- Processing Item ID: {item_id} ---")
            
            try:
                feature_service_item = gis.content.get(item_id)
                if not feature_service_item:
                    print(f"Error: Item ID {item_id} not found. Skipping.")
                    continue
                
                print(f"Found item: {feature_service_item.title}")

                field_info_df = pd.read_csv(csv_path)
                field_lookup = field_info_df.set_index('name').to_dict('index')

                layer_to_update = feature_service_item.layers[0]
                
                # Get the layer definition AND IMMEDIATELY convert it to a standard dictionary.
                layer_definition_dict = dict(layer_to_update.properties)
                
                updated_fields_list = []

                # Loop through the fields from the new dictionary
                for field in layer_definition_dict['fields']:
                    field_name = field['name']
                    
                    if field_name in field_lookup:
                        # Update the alias and description from our CSV lookup
                        new_alias = field_lookup[field_name].get('alias')
                        new_description = field_lookup[field_name].get('description')
                        
                        field['alias'] = new_alias
                        field['description'] = new_description
                        print(f"  - Staging update for field '{field_name}'")
                    
                    # Add the field to the list
                    updated_fields_list.append(field)
                
                # Prepare a new, clean dictionary for the update.
                # It contains only the information to change.
                update_payload = {"fields": updated_fields_list}
                
                # Apply the changes using the clean, JSON-serializable payload
                layer_to_update.manager.update_definition(update_payload)
                print(f"Successfully updated definitions for layer: {layer_to_update.properties.name}")

            except Exception as e:
                print(f"An error occurred while processing item {item_id}: {e}")

    except Exception as e:
        print(f"A critical error occurred: {e}")