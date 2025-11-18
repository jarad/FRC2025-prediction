import requests
import json
import os
import time
from pathlib import Path

# Define the base URL and API key
BASE_URL = "https://www.thebluealliance.com/api/v3"
API_KEY = "evMtKpcBGlJiRvCpiFxYHWUAFgIsw2Ti2QXBsODyqqplwoiIgswJlhIld7Eg0ZFd"

# Define headers for the API request
HEADERS = {
    "X-TBA-Auth-Key": API_KEY,
    "Accept": "application/json"
}

def get_year_events(year):
    """
    Fetch all events for a specific year
    """
    url = f"{BASE_URL}/events/{year}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching events for year {year}: {e}")
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

def save_match_data(match_data, year):
    """
    Save match data as a JSON file in the appropriate directory structure
    """
    if not match_data:
        return False
    
    try:
        # Extract event key from match key (e.g., "2025mose_f1m1" -> "2025mose")
        event_key = match_data['event_key']
        match_key = match_data['key']
        
        # Create directory structure
        base_dir = Path(__file__).parent.parent / 'data' / str(year) / event_key
        base_dir.mkdir(parents=True, exist_ok=True)
        
        # Create JSON file path
        file_path = base_dir / f"{match_key}.json"
        
        # Check if file already exists
        if file_path.exists():
            print(f"Skipping {match_key} - file already exists")
            return True
        
        # Save match data as JSON
        with open(file_path, 'w') as f:
            json.dump(match_data, f, indent=2)
            
        return True
    except Exception as e:
        print(f"Error saving match data: {e}")
        return False

def get_match_file_path(match_key, year):
    """
    Get the file path for a match without creating directories
    """
    event_key = match_key.split('_')[0]
    return Path(__file__).parent.parent / 'data' / str(year) / event_key / f"{match_key}.json"

def main():
    # Start from current year and go back to 1992 (first year of FRC)
    end_year = 2011  # Current year
    start_year = 2005
    
    print(f"Fetching FRC events from {end_year} back to {start_year}")
    total_matches_processed = 0
    total_matches_skipped = 0
    
    # Reverse the range to go from most recent to oldest
    for year in range(end_year, start_year - 1, -1):
        print(f"\nProcessing year {year}...")
        year_matches_processed = 0
        year_matches_skipped = 0
        
        # Fetch events for the year
        events_data = get_year_events(year)
        if events_data:
            print(f"Found {len(events_data)} events for {year}")
            
            # Process each event's matches
            for event in events_data:
                event_key = event['key']
                print(f"\nProcessing matches for event: {event_key}")
                
                # Fetch matches for the event
                matches = get_event_matches(event_key)
                if matches:
                    print(f"Found {len(matches)} matches for event {event_key}")
                    
                    # Process each match
                    for match in matches:
                        match_key = match['key']
                        
                        # Check if file already exists
                        file_path = get_match_file_path(match_key, year)
                        if file_path.exists():
                            print(f"Skipping {match_key} - file already exists")
                            year_matches_skipped += 1
                            total_matches_skipped += 1
                            continue
                        
                        print(f"Processing match {match_key}...")
                        
                        # Add a small delay to avoid hitting API rate limits
                        time.sleep(0.01)
                        
                        match_data = get_match_data(match_key)
                        if match_data:
                            if save_match_data(match_data, year):
                                year_matches_processed += 1
                                total_matches_processed += 1
        
        print(f"Processed {year_matches_processed} matches for year {year}")
        print(f"Skipped {year_matches_skipped} existing matches for year {year}")
        # Add a small delay between years
        time.sleep(0.5)
    
    print(f"\nTotal matches processed: {total_matches_processed}")
    print(f"Total matches skipped: {total_matches_skipped}")
    print("Match data has been saved as individual JSON files in the data directory")

if __name__ == "__main__":
    main()
