import requests
import json
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any
from cloud_storage import get_storage_manager
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# def get_event_matches() -> pd.DataFrame:
#     """
#     Fetch matches for a given time range
    
#     Returns:
#         pd.DataFrame: DataFrame containing match information
#     """
#     # Current time in UTC
#     now = datetime.utcnow()

#     # Calculate the start time and end time
#     start_time = now.replace(minute=0, second=0, microsecond=0)
#     start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
#     end_time = (now + timedelta(weeks=2)).replace(
#         hour=23,
#         minute=59,
#         second=59,
#         microsecond=0
#     )
#     end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

#     # Initialize an empty list to store each batch of matches data
#     all_matches = []

#     # Loop over drillDownTagIds ranges in steps of 200
#     for start in range(200, 14000, 200):
#         # Generate a comma-separated string of drillDownTagIds for the current range
#         drilldown_ids = ','.join(str(i) for i in range(start, start + 200))

#         # Define the request URL
#         url = (
#             f"https://content.toto.nl/content-service/api/v1/q/event-list?"
#             f"startTimeFrom={start_time}"
#             f"startTimeTo={end_time}&"
#             "liveNow=false&"
#             "maxEvents=190&"
#             "orderEventsBy=popularity&"
#             "orderMarketsBy=displayOrder&"
#             "marketSortsIncluded=--,CS,DC,DN,HH,HL,MH,MR,WH&"
#             "marketGroupTypesIncluded=CUSTOM_GROUP,DOUBLE_CHANCE,DRAW_NO_BET,MATCH_RESULT,"
#             "MATCH_WINNER,MONEYLINE,ROLLING_SPREAD,ROLLING_TOTAL,STATIC_SPREAD,STATIC_TOTAL&"
#             "eventSortsIncluded=MTCH&"
#             "includeChildMarkets=true&"
#             "prioritisePrimaryMarkets=true&"
#             "includeCommentary=true&"
#             "includeMedia=true&"
#             f"drilldownTagIds={drilldown_ids}&"
#             "lang=nl-NL&"
#             "channel=I"
#         )

#         # Headers for the request
#         headers = {
#             'accept': 'application/json',
#             'accept-language': 'en-US,en;q=0.9,nl;q=0.8',
#         }

#         try:
#             # Make the request
#             response = requests.get(url, headers=headers)
#             response.raise_for_status()  # Raise HTTPError for bad responses

#             # Parse the JSON response
#             data = response.json()

#             # Extract match data if available
#             matches = []
#             for event in data.get('data', {}).get('events', []):
#                 match = {
#                     "event_id": event.get("id"),
#                     "match_name": event.get("name"),
#                     "start_time": event.get("startTime"),
#                     "home_team": next((team['name'] for team in event.get('teams', []) if team['side'] == "HOME"), None),
#                     "away_team": next((team['name'] for team in event.get('teams', []) if team['side'] == "AWAY"), None),
#                     "competition": event.get('type', {}).get('name'),
#                     "country": event.get('class', {}).get('name'),
#                     "sport": event.get('category', {}).get('name'),
#                 }

#                 # Extract odds if available
#                 outcomes = event.get("markets", [{}])[0].get("outcomes", [])
#                 match["home_odds"] = next((outcome['prices'][0]['decimal'] for outcome in outcomes if outcome.get('subType') == "H"), None)
#                 match["away_odds"] = next((outcome['prices'][0]['decimal'] for outcome in outcomes if outcome.get('subType') == "A"), None)
                
#                 matches.append(match)

#             # Add the batch of matches to the all_matches list
#             all_matches.extend(matches)

#         except Exception as e:
#             print(f"An error occurred for drilldown ID range {start}–{start + 200}: {e}")

#     # Combine all match data into a single DataFrame
#     return pd.DataFrame(all_matches)

import requests
import time
import random
import pandas as pd
from datetime import datetime, timedelta
from itertools import islice

def get_event_matches() -> pd.DataFrame:
    """
    Fetch matches using both range-based drilldown IDs and specific country drilldown IDs
    
    Returns:
        pd.DataFrame: DataFrame containing match information
    """
    # Define the country drilldown IDs
    COUNTRY_MARKET_DRILLDOWNS = {'Nederland': 1052, 
        'Engeland': 40, 
        'Spanje': 44, 
        'Italië': 43, 
        'Duitsland': 42, 
        'Frankrijk': 41, 
        'België': 864, 
        'Portugal': 551, 
        'Turkije': 537, 
        'Saudi Arabië': 987, 
        'VS': 595, 
        'Europees': 3214, 
        'Wereldwijd': 667, 
        'Americas': 9342, 
        'Africa': 6505, 
        'Andorra': 4578, 
        'Algerije': 1797, 
        'Argentinië': 1670, 
        'Australië': 825,  
        'Azerbeidzjan': 786, 
        'Bahrein': 1000, 
        'Bosnia and Herzegovina': 963, 
        'Brazilië': 896, 
        'Bulgarije': 619, 
        'Burundi': 519, 
        'Chili': 1132, 
        'Cambodja': 4125, 
        'Costa Rica': 475, 
        'Colombia': 1225, 
        'Cyprus': 1068, 
        'Denemarken': 479, 
        'Egypte': 994, 
        'El Salvador': 1607, 
        'Ethiopia': 3396, 
        'Finland': 692, 
        'Filipijnen': 5317, 
        'Guatemala': 1548, 
        'Griekenland': 513, 
        'Honduras': 1471, 
        'Hong Kong': 1353, 
        'Hongarije': 484, 
        'IJsland': 617, 
        'Ierland': 925, 
        'Indonesia': 3528, 
        'India': 1799, 
        'Israël': 542, 
        'Iran': 795, 
        'Japan': 581, 
        'Jordanië': 988, 
        'Koeweit': 1022, 
        'Kroatië': 544, 
        'Luxemburg': 1355,
        'Malta': 1590, 
        'Maleisië': 3143, 
        'Mexico': 888, 
        'Marokko': 919, 
        'Nicaragua': 970, 
        'Noord-Ierland': 915, 
        'Noorwegen': 583, 
        'Oekraine': 501, 
        'Oman': 1645, 
        'Oostenrijk': 534, 
        'Panama': 3013, 
        'Paraguay': 883, 
        'Peru': 971, 
        'Polen': 477, 
        'Qatar': 910, 
        'Roemenië': 555, 
        'Schotland': 873, 
        'Servië': 487, 
        'Singapore': 1610, 
        'Slowakije': 611, 
        'Slovenie': 605, 
        'Tanzania': 763, 
        'Thailand': 1511, 
        'Tsjechië': 489, 
        'Tunesië': 967, 
        'Uruguay': 1007, 
        'Verenigde Arabische Emiraten': 1475, 
        'Venezuela': 1546, 
        'Vietnam': 620, 
        'Wales': 1262, 
        'Zuid-Afrika': 1003, 
        'Zuid-Korea': 696, 
        'Zweden': 539, 
        'Zwitserland': 661,
        'Champions leaugue': 3216,
        'Europa League': 3218,
        'Nations leaugue': 9641,
        'Conference leaugue': 3217,
        'ATP': 1055,
        'WTA': 47,
        'Challenger': 1033,
        'Grand Slam': 656
    }

    def batch_dict(data, batch_size):
        it = iter(data)
        for i in range(0, len(data), batch_size):
            batch = {k: data[k] for k in islice(it, batch_size)}
            yield batch

    def fetch_matches(drilldown_ids: str, batch_info: str) -> list:
        """Helper function to fetch matches for given drilldown IDs"""
        url = (
            f"https://content.toto.nl/content-service/api/v1/q/event-list?"
            f"startTimeFrom={start_time}"
            f"startTimeTo={end_time}&"
            "liveNow=false&"
            "maxEvents=190&"
            "orderEventsBy=popularity&"
            "orderMarketsBy=displayOrder&"
            "marketSortsIncluded=--,CS,DC,DN,HH,HL,MH,MR,WH&"
            "marketGroupTypesIncluded=CUSTOM_GROUP,DOUBLE_CHANCE,DRAW_NO_BET,MATCH_RESULT,"
            "MATCH_WINNER,MONEYLINE,ROLLING_SPREAD,ROLLING_TOTAL,STATIC_SPREAD,STATIC_TOTAL&"
            "eventSortsIncluded=MTCH&"
            "includeChildMarkets=true&"
            "prioritisePrimaryMarkets=true&"
            "includeCommentary=true&"
            "includeMedia=true&"
            f"drilldownTagIds={drilldown_ids}&"
            "lang=nl-NL&"
            "channel=I"
        )

        headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,nl;q=0.8',
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            matches = []
            for event in data.get('data', {}).get('events', []):
                match = {
                    "event_id": event.get("id"),
                    "match_name": event.get("name"),
                    "start_time": event.get("startTime"),
                    "home_team": next((team['name'] for team in event.get('teams', []) if team['side'] == "HOME"), None),
                    "away_team": next((team['name'] for team in event.get('teams', []) if team['side'] == "AWAY"), None),
                    "competition": event.get('type', {}).get('name'),
                    "country": event.get('class', {}).get('name'),
                    "sport": event.get('category', {}).get('name'),
                }
                
                outcomes = event.get("markets", [{}])[0].get("outcomes", [])
                match["home_odds"] = next((outcome['prices'][0]['decimal'] for outcome in outcomes if outcome.get('subType') == "H"), None)
                match["away_odds"] = next((outcome['prices'][0]['decimal'] for outcome in outcomes if outcome.get('subType') == "A"), None)
                
                matches.append(match)
            
            print(f"Successfully fetched data for {batch_info}")
            return matches
            
        except Exception as e:
            print(f"An error occurred for {batch_info}: {e}")
            return []

    # Current time in UTC
    now = datetime.utcnow()

    # Calculate the start time and end time
    start_time = now.replace(minute=0, second=0, microsecond=0)
    start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = (now + timedelta(weeks=10)).replace(
        hour=23,
        minute=59,
        second=59,
        microsecond=0
    )
    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Initialize list to store all matches
    all_matches = []

    # # Part 1: Fetch matches using range-based drilldown IDs
    # print("\nProcessing range-based drilldown IDs...")
    # for start in range(200, 14000, 200):
    #     drilldown_ids = ','.join(str(i) for i in range(start, start + 200))
    #     matches = fetch_matches(drilldown_ids, f"range {start}-{start + 200}")
    #     all_matches.extend(matches)
        # time.sleep(random.uniform(1, 2))

    # Part 2: Fetch matches using country-specific drilldown IDs
    print("\nProcessing country-specific drilldown IDs...")
    for batch_num, country_batch in enumerate(batch_dict(COUNTRY_MARKET_DRILLDOWNS, 1)):
        drilldown_ids = ','.join(str(id) for id in country_batch.values())
        batch_info = f"countries: {', '.join(country_batch.keys())}"
        matches = fetch_matches(drilldown_ids, batch_info)
        all_matches.extend(matches)
        
        if batch_num < len(COUNTRY_MARKET_DRILLDOWNS) // 5:  # Don't sleep after the last batch
            continue
            # time.sleep(random.uniform(1, 2))

    # Create and return the final DataFrame
    return pd.DataFrame(all_matches)

def get_headers() -> Dict[str, str]:
    """
    Generate headers for API requests
    
    Returns:
        Dict[str, str]: Headers for requests
    """
    return {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9,nl;q=0.8',
        'Cookie': 'incap_ses_1581_2280942=FvpNPI919nl1UsyZNNfwFR6GL2cAAAAA8yX5UHfYkaWD/ax05SY+Jg==; visid_incap_2280942=CzAo1Y6yTMa654jo8RfVRtBiL2cAAAAAQkIPAAAAAAAE7rpFtkP20364qTw5VQQQ'
    }

# def fetch_market_ids_for_event_batch(batch_ids: List[int]) -> Dict[int, List[int]]:
#     """
#     Fetch market IDs for a small batch of event IDs
    
#     Args:
#         batch_ids (List[int]): List of event IDs to process
    
#     Returns:
#         Dict[int, List[int]]: Dictionary mapping event IDs to their market IDs
#     """
#     headers = get_headers()
#     event_market_ids_dict = {}
    
#     event_ids_str = ','.join(map(str, batch_ids))
    
#     url = (
#         "https://content.toto.nl/content-service/api/v1/q/events-by-ids?"
#         f"eventIds={event_ids_str}&"
#         "includeChildMarkets=true&includeCollections=true&"
#         "includePriorityCollectionChildMarkets=true&includePriceHistory=true&"
#         "includeCommentary=true&includeIncidents=true&includeRace=true&"
#         "includePools=true&includeNonFixedOdds=true&lang=nl-NL&channel=I"
#     )
    
#     try:
#         # Add random sleep to reduce request rate
#         time.sleep(random.uniform(0.1, 0.3))
        
#         response = requests.get(url, headers=headers, timeout=30)
#         json_data = response.json()
        
#         for event in json_data['data']['events']:
#             event_id = event['id']
#             market_ids = []
#             for collection in event.get('collections', []):
#                 if collection['name'] in ['Alles', 'UNASSIGNED']:
#                     market_ids = collection.get('marketIds', [])
#                     break
#             event_market_ids_dict[event_id] = market_ids
        
#     except Exception as e:
#         print(f"Error collecting market IDs: {e} for event_ids: {batch_ids}")
    
#     return event_market_ids_dict
def fetch_market_ids_for_event_batch(batch_ids: List[int]) -> Dict[int, List[int]]:
    """
    Fetch market IDs for a small batch of event IDs.
    
    Args:
        batch_ids (List[int]): List of event IDs to process.
    
    Returns:
        Dict[int, List[int]]: Dictionary mapping event IDs to their market IDs.
    """
    headers = get_headers()
    event_market_ids_dict = {}
    
    event_ids_str = ','.join(map(str, batch_ids))
    
    url = (
        "https://content.toto.nl/content-service/api/v1/q/events-by-ids?"
        f"eventIds={event_ids_str}&"
        "includeChildMarkets=true&includeCollections=true&"
        "includePriorityCollectionChildMarkets=true&includePriceHistory=true&"
        "includeCommentary=true&includeIncidents=true&includeRace=true&"
        "includePools=true&includeNonFixedOdds=true&lang=nl-NL&channel=I"
    )
    
    try:
        # Add random sleep to reduce request rate
        time.sleep(random.uniform(0.1, 0.3))
        
        response = requests.get(url, headers=headers, timeout=30)
        json_data = response.json()
        
        preferred_collections = ['Alles', 'UNASSIGNED']
        fallback_collections = ['Wedstrijd', 'Doelpunten', 'Schoten', 'Schoten op doel']
        
        for event in json_data['data']['events']:
            event_id = event['id']
            market_ids = []

            # Eerst zoeken in 'Alles' en 'UNASSIGNED'
            for collection in event.get('collections', []):
                if collection['name'] in preferred_collections:
                    market_ids = collection.get('marketIds', [])
                    if market_ids:
                        break  # Stop als een van deze collecties markten heeft
            
            # Als geen markten gevonden, zoek in de fallback-lijst
            if not market_ids:
                for collection in event.get('collections', []):
                    if collection['name'] in fallback_collections:
                        market_ids = collection.get('marketIds', [])
                        if market_ids:
                            break  # Stop bij de eerste geldige fallback-markt
            
            event_market_ids_dict[event_id] = market_ids
        
    except Exception as e:
        print(f"Error collecting market IDs: {e} for event_ids: {batch_ids}")
    
    return event_market_ids_dict

def collect_market_ids(event_ids: List[int], max_workers: int = 10, batch_size: int = 7) -> Dict[int, List[int]]:
    """
    Concurrently collect market IDs for all events
    
    Args:
        event_ids (List[int]): Full list of event IDs
        max_workers (int): Maximum number of concurrent threads
        batch_size (int): Number of event IDs to process in each batch
    
    Returns:
        Dict[int, List[int]]: Comprehensive dictionary of event IDs to market IDs
    """
    event_market_ids_dict = {}
    
    # Split event_ids into batches
    event_id_batches = [event_ids[i:i + batch_size] for i in range(0, len(event_ids), batch_size)]
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(fetch_market_ids_for_event_batch, batch): batch 
            for batch in event_id_batches
        }
        
        # Collect results
        for future in as_completed(future_to_batch):
            try:
                batch_results = future.result()
                event_market_ids_dict.update(batch_results)
                # print(f"Processed batch, total events so far: {len(event_market_ids_dict)}")
            except Exception as e:
                print(f"Batch processing error: {e}")
    
    return event_market_ids_dict

def fetch_market_data_for_batch(batch_market_ids: List[int]) -> List[Dict[str, Any]]:
    """
    Fetch market data for a batch of market IDs
    
    Args:
        batch_market_ids (List[int]): List of market IDs to process
    
    Returns:
        List[Dict[str, Any]]: List of extracted market data
    """
    headers = get_headers()
    batch_data = []
    
    market_ids_str = ','.join(map(str, batch_market_ids))
    
    url = (
        "https://content.toto.nl/content-service/api/v1/q/events-by-ids?"
        f"marketIds={market_ids_str}&"
        "includeChildMarkets=true&includePriceHistory=false&"
        "includeCommentary=false&includeMedia=false&"
        "includePoolsInfo=false&includeNonFixedOdds=false&"
        "lang=nl-NL&channel=I"
    )
    
    try:
        # Add random sleep to reduce request rate
        time.sleep(random.uniform(0.1, 0.3))
        
        response = requests.get(url, headers=headers, timeout=30)
        json_data = response.json()
        
        for event in json_data['data']['events']:
            event_id = event['id']
            event_name = event['name']
            
            for market in event['markets']:
                for outcome in market['outcomes']:
                    batch_data.append({
                        'event_id': event_id,
                        'Event Name': event_name,
                        'Market Name': market['name'],
                        'Outcome Name': outcome['name'],
                        'Odds (Decimal)': outcome['prices'][0]['decimal'],
                        'Price Numerator': outcome['prices'][0]['numerator'],
                        'Price Denominator': outcome['prices'][0]['denominator'],
                        'Outcome Type': outcome['type'],
                        'Outcome SubType': outcome.get('subType', '')
                    })
        
    except Exception as e:
        print(f"Error processing market data: {e} for batch: {batch_market_ids}")
    
    return batch_data

def process_market_data(
    market_ids: List[int], 
    max_workers: int = 20, 
    batch_size: int = 7
) -> pd.DataFrame:
    """
    Concurrently process market data for all market IDs
    
    Args:
        market_ids (List[int]): Full list of market IDs
        max_workers (int): Maximum number of concurrent threads
        batch_size (int): Number of market IDs to process in each batch
    
    Returns:
        pd.DataFrame: DataFrame containing processed market data
    """
    # Split market IDs into batches
    market_id_batches = [market_ids[i:i + batch_size] for i in range(0, len(market_ids), batch_size)]
    
    # Collected data from all batches
    all_data = []
    
    # Use ThreadPoolExecutor for concurrent processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches
        future_to_batch = {
            executor.submit(fetch_market_data_for_batch, batch): batch 
            for batch in market_id_batches
        }
        
        # Collect results
        for future in as_completed(future_to_batch):
            try:
                batch_results = future.result()
                all_data.extend(batch_results)
            except Exception as e:
                print(f"Batch processing error: {e}")
    
    # Convert to DataFrame
    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

def main() -> Dict[str, pd.DataFrame]:
    """
    Main function to scrape and process TOTO betting data
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary containing matches and market data
    """
    try:
        # Step 1: Get event matches
        logging.info("Toto: Fetching matches...")
        matches_df = get_event_matches()
        
        # Step 2: Collect market IDs
        logging.info("Toto: Fetching Market IDs...")
        event_market_ids_dict = collect_market_ids(
            matches_df['event_id'].tolist(), 
            max_workers=20,  
            batch_size=1
        )
        
        # Step 3: Flatten market IDs into a single list
        all_market_ids = []
        for market_ids in event_market_ids_dict.values():
            all_market_ids.extend(market_ids)
        
        # Step 4: Process market data
        logging.info("Toto: Fetching odds data from Market IDs...")
        market_data_df = process_market_data(
            all_market_ids, 
            max_workers=100,  
            batch_size=100
        )

        # Step 5: Create final DataFrame
        final_df = market_data_df.merge(
            matches_df[['event_id', 'sport', 'competition', 'match_name', 'home_team', 'away_team', 'start_time']], 
            on='event_id', 
            how='left'
        ).drop_duplicates()

        # Step 6: Upload to Google Cloud Storage
        storage_mgr = get_storage_manager()
        blob_path = storage_mgr.upload_dataframe(final_df, 'toto')
        logging.info(f"Toto: Data uploaded to cloud storage: {blob_path}")
        
        return final_df
        
    except Exception as e:
        logging.error(f"Error in Toto scraper: {str(e)}")
        raise

if __name__ == "__main__":
    final_df = main()