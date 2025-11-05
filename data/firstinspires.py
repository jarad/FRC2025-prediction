import requests
import pandas as pd
from pandas import json_normalize
import os
import time

# Define the base URL and API key
BASE_URL = "https://www.thebluealliance.com/api/v3"
API_KEY = "evMtKpcBGlJiRvCpiFxYHWUAFgIsw2Ti2QXBsODyqqplwoiIgswJlhIld7Eg0ZFd"

# Define headers for the API request
HEADERS = {
    "X-TBA-Auth-Key": API_KEY,
    "Accept": "application/json"
}

def get_season_events(year):
    """
    Fetch all events for a specific year
    """
    url = f"{BASE_URL}/events/{year}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching events: {e}")
        return None

def get_event_matches(event_key):
    """
    Fetch all matches for a specific event
    """
    url = f"{BASE_URL}/event/{event_key}/matches"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching event matches: {e}")
        return None

def get_match_data(match_key):
    """
    Fetch data for a specific match
    """
    url = f"{BASE_URL}/match/{match_key}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching match data: {e}")
        return None

def process_match_data(match_data):
    """
    Process match data into a pandas DataFrame with flattened nested structures
    """
    if not match_data or 'alliances' not in match_data or 'score_breakdown' not in match_data:
        return None
    
    try:
        # Create separate dataframes for each nested structure
        main_df = pd.DataFrame([{k: v for k, v in match_data.items() 
                               if not isinstance(v, (dict, list))}])
        
        # Alliance data
        red_alliance = pd.json_normalize(match_data['alliances']['red'], sep='_')
        blue_alliance = pd.json_normalize(match_data['alliances']['blue'], sep='_')
        
        # Score breakdown data - red
        red_score = pd.json_normalize(match_data['score_breakdown']['red'], sep='_', record_path=None)
        
        # Extract red reef data
        red_auto_reef = pd.json_normalize(match_data['score_breakdown']['red']['autoReef'], sep='_')
        red_teleop_reef = pd.json_normalize(match_data['score_breakdown']['red']['teleopReef'], sep='_')
        
        # Score breakdown data - blue
        blue_score = pd.json_normalize(match_data['score_breakdown']['blue'], sep='_', record_path=None)
        
        # Extract blue reef data
        blue_auto_reef = pd.json_normalize(match_data['score_breakdown']['blue']['autoReef'], sep='_')
        blue_teleop_reef = pd.json_normalize(match_data['score_breakdown']['blue']['teleopReef'], sep='_')
        
        # Videos data if present
        if 'videos' in match_data and match_data['videos']:
            videos_df = pd.json_normalize(match_data['videos'])
            videos_df = videos_df.add_prefix('video_')
        else:
            videos_df = pd.DataFrame({'video_key': [None], 'video_type': [None]})
        
        # Add prefixes to distinguish between different components
        red_alliance = red_alliance.add_prefix('red_alliance_')
        blue_alliance = blue_alliance.add_prefix('blue_alliance_')
        red_score = red_score.add_prefix('red_score_')
        blue_score = blue_score.add_prefix('blue_score_')
        red_auto_reef = red_auto_reef.add_prefix('red_auto_reef_')
        red_teleop_reef = red_teleop_reef.add_prefix('red_teleop_reef_')
        blue_auto_reef = blue_auto_reef.add_prefix('blue_auto_reef_')
        blue_teleop_reef = blue_teleop_reef.add_prefix('blue_teleop_reef_')

        # Combine all dataframes
        dfs = [main_df, red_alliance, blue_alliance, red_score, blue_score,
               red_auto_reef, red_teleop_reef, blue_auto_reef, blue_teleop_reef,
               videos_df]
        
        return pd.concat(dfs, axis=1)
    except Exception as e:
        print(f"Error processing match data: {e}")
        return None

def main():
    year = 2025
    output_file = f'data/frc_{year}_all_matches.csv'
    all_match_dfs = []
    
    # Fetch all events for the season
    events = get_season_events(year)
    if not events:
        print("No events found for the season")
        return
    
    print(f"Found {len(events)} events for {year}")
    
    # Process each event
    for event in events:
        event_key = event['key']
        print(f"\nProcessing event: {event_key}")
        
        # Fetch all matches for the event
        matches = get_event_matches(event_key)
        if not matches:
            print(f"No matches found for event {event_key}")
            continue
        
        print(f"Found {len(matches)} matches for event {event_key}")
        
        # Process each match
        for match in matches:
            match_key = match['key']
            print(f"Processing match {match_key}...")
            
            # Add a small delay to avoid hitting API rate limits
            time.sleep(0.1)
            
            match_data = get_match_data(match_key)
            if match_data:
                match_df = process_match_data(match_data)
                if match_df is not None:
                    all_match_dfs.append(match_df)
    
    if all_match_dfs:
        # Combine all matches into one DataFrame
        final_df = pd.concat(all_match_dfs, ignore_index=True)
        # Save to CSV
        final_df.to_csv(output_file, index=False)
        print(f"\nAll match data has been saved to {output_file}")
        print(f"Total matches processed: {len(all_match_dfs)}")
    else:
        print("No match data was successfully processed")

if __name__ == "__main__":
    main()


