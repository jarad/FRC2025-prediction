import pandas as pd
import glob
import json
import os

def flatten_json_files_to_csv(directory_path, output_csv_path):
    """
    Flattens all nested JSON files in a directory and saves them to a single CSV file.

    Args:
        directory_path (str): Path to the directory containing JSON files (e.g., './json_data/*.json').
        output_csv_path (str): Path for the output CSV file (e.g., 'flattened_output.csv').
    """
    all_dataframes = []
    
    # Find all JSON files in the specified directory path
    files = glob.glob(directory_path)

    if not files:
        print(f"No JSON files found in: {directory_path}")
        return

    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                json_data = json.load(f)

            # json_normalize is powerful for nested dicts/lists. It might return a list of dfs
            # if the top level is a list of records.
            if isinstance(json_data, list):
                df = pd.json_normalize(json_data)
            else:
                df = pd.json_normalize([json_data])
            
            # Handle any remaining columns that still contain lists by exploding them
            for col in df.columns:
                if df[col].apply(lambda x: isinstance(x, list)).any():
                    df = df.explode(col).reset_index(drop=True)
                    # If the exploded items are dicts, further normalize them and join
                    if df[col].apply(lambda x: isinstance(x, dict)).any():
                         df_exploded = pd.json_normalize(df.pop(col))
                         df_exploded.columns = [f"{col}_{c}" for c in df_exploded.columns]
                         df = df.join(df_exploded)

            all_dataframes.append(df)
            print(f"Processed {file_path}")

        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

    # Concatenate all dataframes into a single one
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
        # Write the result to a single CSV file
        final_df.to_csv(output_csv_path, index=False, encoding='utf-8')
        print(f"\nSuccessfully created single CSV file at: {output_csv_path}")
    else:
        print("No data to concatenate. Output CSV file not created.")

# Example Usage:
# Place your JSON files in a directory named 'my_json_files'
# The output file will be named 'master_output.csv' in the current directory
if __name__ == '__main__':
    # Make sure to replace './my_json_files/*.json' with your actual directory path pattern
    input_directory_pattern = 'data/2026/2026mnwi/*.json' 
    output_csv_filename = '2026mnwi.csv'
    
    flatten_json_files_to_csv(input_directory_pattern, output_csv_filename)

