import requests
import json
import pandas as pd
import regex as re
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

@dataclass
class APIConfig:
    """Configuration for API endpoints and parameters"""
    BASE_URL = "https://content.toto.nl/content-service/api/v1/q"
    HEADERS = {
        'accept': 'application/json',
        'accept-language': 'en-US,en;q=0.9,nl;q=0.8'
    }
    BATCH_SIZE = 30
    DRILLDOWN_BATCH_SIZE = 200
    MAX_DRILLDOWN_ID = 14000

class DataFetcher:
    def __init__(self, config: APIConfig = APIConfig()):
        self.config = config
        self.now = datetime.utcnow()
        
        # Round down to the current hour
        start_datetime = self.now.replace(minute=0, second=0, microsecond=0)
        self.start_time = start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # Round up to end of day in two weeks
        end_datetime = (self.now + timedelta(weeks=2)).replace(
            hour=23,
            minute=59,
            second=59,
            microsecond=0
        )
        self.end_time = end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _make_request(self, url: str, error_msg: str) -> dict:
        """Make API request with error handling and rate limiting"""
        try:
            response = requests.get(url, headers=self.config.HEADERS)
            response.raise_for_status()
            time.sleep(random.uniform(0.02, 1.81))  # Rate limiting
            return response.json()
        except Exception as e:
            print(f"{error_msg}: {str(e)}")
            return {}

    def fetch_matches(self) -> pd.DataFrame:
        """Fetch initial matches data"""
        all_matches = []
        
        for start in range(200, self.config.MAX_DRILLDOWN_ID, self.config.DRILLDOWN_BATCH_SIZE):
            drilldown_ids = ','.join(str(i) for i in range(start, start + self.config.DRILLDOWN_BATCH_SIZE))
            
            url = (
                f"{self.config.BASE_URL}/event-list?"
                f"startTimeFrom={self.start_time}&"
                f"startTimeTo={self.end_time}&"
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

            data = self._make_request(url, f"Error fetching matches for drilldown IDs {start}-{start + self.config.DRILLDOWN_BATCH_SIZE}")
            
            if not data:
                continue

            matches = self._parse_matches(data)
            all_matches.extend(matches)

        return pd.DataFrame(all_matches)

    def _parse_matches(self, data: dict) -> List[dict]:
        """Parse matches data from API response"""
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
        return matches

    def fetch_detailed_odds(self, matches_df: pd.DataFrame) -> pd.DataFrame:
        """Fetch detailed odds data for matches"""
        all_data = []
        
        for i in range(0, len(matches_df['event_id']), self.config.BATCH_SIZE):
            batch_ids = matches_df['event_id'][i:i + self.config.BATCH_SIZE]
            event_ids_str = ','.join(map(str, batch_ids))
            
            url = (
                f"{self.config.BASE_URL}/events-by-ids?"
                f"eventIds={event_ids_str}&"
                "includeChildMarkets=true&includeCollections=true&"
                "includePriorityCollectionChildMarkets=true&"
                "includePriceHistory=false&includeCommentary=true&"
                "includeIncidents=false&includeRace=false&"
                "includePools=false&includeNonFixedOdds=false&"
                "lang=nl-NL&channel=I"
            )
            
            data = self._make_request(url, f"Error fetching odds for batch {i}-{i + self.config.BATCH_SIZE}")
            if not data:
                continue

            batch_df = self._parse_odds(data)
            if not batch_df.empty:
                all_data.append(batch_df)

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def _parse_odds(self, data: dict) -> pd.DataFrame:
        """Parse odds data from API response"""
        parsed_data = {
            'event_id': [], 'Event Name': [], 'Market Name': [],
            'Outcome Name': [], 'Odds (Decimal)': [], 'Price Numerator': [],
            'Price Denominator': [], 'Outcome Type': [], 'Outcome SubType': []
        }
        
        for event in data.get('data', {}).get('events', []):
            for market in event.get('markets', []):
                for outcome in market.get('outcomes', []):
                    parsed_data['event_id'].append(event.get('id'))
                    parsed_data['Event Name'].append(event.get('name'))
                    parsed_data['Market Name'].append(market.get('name'))
                    parsed_data['Outcome Name'].append(outcome.get('name'))
                    
                    # Safely access prices
                    prices = outcome.get('prices', [])
                    if prices:  # Check if prices list is not empty
                        price = prices[0]
                        parsed_data['Odds (Decimal)'].append(price.get('decimal'))
                        parsed_data['Price Numerator'].append(price.get('numerator'))
                        parsed_data['Price Denominator'].append(price.get('denominator'))
                    else:
                        # Append None if prices are missing
                        parsed_data['Odds (Decimal)'].append(None)
                        parsed_data['Price Numerator'].append(None)
                        parsed_data['Price Denominator'].append(None)
                    
                    parsed_data['Outcome Type'].append(outcome.get('type'))
                    parsed_data['Outcome SubType'].append(outcome.get('subType'))
        
        return pd.DataFrame(parsed_data)

def main():
    # Initialize fetcher
    fetcher = DataFetcher()

    # Fetch matches
    print("Toto: Fetching matches...")
    matches_df = fetcher.fetch_matches()
    print(f"Toto: Found {len(matches_df)} matches")

    # Fetch detailed odds
    print("Toto: Fetching detailed odds...")
    detailed_odds_df = fetcher.fetch_detailed_odds(matches_df)
    print(f"Toto: Processed {len(detailed_odds_df)} odds entries")

    # Perform the join on 'event_id' to get 'sport', 'competition', etc.
    print("Toto: Merging matches and odds data...")
    final_df = detailed_odds_df.merge(
        matches_df[['event_id', 'sport', 'competition', 'match_name', 'home_team', 'away_team']],
        on='event_id',
        how='left'
    ).drop_duplicates()

    # Clean up column names and handle replacements
    final_df = final_df[[col for col in final_df.columns if not col.endswith('_x')]]  # Remove '_x' columns
    final_df.columns = final_df.columns.str.replace('_y', '', regex=False)  # Remove '_y' suffix from column names

    # Replace 'A' with '2' and 'H' with '1' in 'Outcome SubType' column
    if 'Outcome SubType' in final_df.columns:
        final_df['Outcome SubType'] = final_df['Outcome SubType'].replace({'A': '2', 'H': '1'})

    # Save the final DataFrame to CSV
    csv_filename = f"Data/scrapers/Toto/totoAllSport{fetcher.now}.csv"
    final_df.to_csv(csv_filename, index=False)
    print(f"Toto: Data saved to {csv_filename}")

    return final_df

if __name__ == "__main__":
    final_df = main()