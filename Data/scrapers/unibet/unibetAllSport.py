import os
import pandas as pd
import requests
import regex as re
from datetime import datetime, timedelta
from cloud_storage import get_storage_manager
import logging
pd.options.mode.chained_assignment = None  # Suppress SettingWithCopyWarning

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

class BettingDataFetcher:
    def __init__(self):
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
        self.base_group_url = "https://eu-offering-api.kambicdn.com/offering/v2018/ubnl/group/highlight.json"
        self.event_url = "https://www.unibet.nl/sportsbook-feeds/views/filter/{}/all/matches"
        self.bet_offer_url = "https://eu-offering-api.kambicdn.com/offering/v2018/ubnl/betoffer/event/{event_id}.json"
        self.headers = {
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0"
        }
        self.cookies = {
            "INGRESSCOOKIE_SPORTSBOOK_FEEDS": "d3eab806ac9dba5607cc45c4508ad6ad|e6f03e039bb9fba9ad84e4dd980ef8c9",
            "clientId": "polopoly_desktop",
        }
        self.params = {
            "lang": "nl_NL",
            "market": "NL",
            "channel_id": "1",
            "client_id": "2",
            "depth": "0",
            "ncid": "1731338297307"
        }

    def fetch_groups(self):
        """Fetch group data from the API."""
        response = requests.get(self.base_group_url, headers=self.headers, params=self.params)
        if response.status_code == 200:
            data = response.json()
            extracted_data = [
                {
                    "name": group.get("name"),
                    "sport": group.get("sport"),
                    "pathTermId": group.get("pathTermId")
                }
                for group in data.get("groups", [])
            ]
            return pd.DataFrame(extracted_data)
        else:
            raise Exception(f"Failed to fetch groups: {response.status_code}")

    def fetch_events(self, path_term_ids, added_path_terms):
        """Fetch events based on pathTermId."""
        all_events_data = []
        all_path_terms = list(set(list(set(path_term_ids)) + added_path_terms))
        for path_term_id in all_path_terms:
            url = self.event_url.format(path_term_id)
            response = requests.get(url, headers=self.headers, cookies=self.cookies)
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Check if 'layout' and the necessary sections exist
                    if 'layout' in data and 'sections' in data['layout'] and len(data['layout']['sections']) > 1:
                        sections = data['layout']['sections'][1]
                        if 'widgets' in sections and len(sections['widgets']) > 0:
                            widgets = sections['widgets'][0]
                            if 'matches' in widgets:
                                matches = widgets['matches']
                                
                                # Check if 'groups' is present in matches
                                if 'groups' in matches and len(matches['groups']) > 0:
                                    # Loop over all groups
                                    for group in matches['groups']:
                                        group_name = group.get('name', 'N/A')  # Get the group name, if available
                                        if 'events' in group:
                                            # Loop over events and gather the necessary information
                                            for event in group['events']:
                                                event_info = event.get('event', {})
                                                event_data = {
                                                    'event_id': event_info.get('id'),
                                                    'event_name': event_info.get('englishName'),
                                                    'start_time': event_info.get('start'),
                                                    'sport': event_info.get('sport'),
                                                    'country/sport': path_term_id.split('/')[-1],
                                                    'group_name': group_name  # Include the group name here
                                                }
                                                all_events_data.append(event_data)
                                        else:
                                            print(f"No 'events' key in group {group_name} for {path_term_id}")
                                elif 'events' in matches:
                                    # Fallback: If 'groups' is not found, check if 'events' is directly under 'matches'
                                    for event in matches['events']:
                                        event_info = event.get('event', {})
                                        event_data = {
                                            'event_id': event_info.get('id'),
                                            'event_name': event_info.get('englishName'),
                                            'start_time': event_info.get('start'),
                                            'sport': event_info.get('sport'),
                                            'group_name': 'N/A'  # No group in this case
                                        }
                                        all_events_data.append(event_data)
                                else:
                                    print(f"No 'groups' or 'events' in matches for {path_term_id}")
                            else:
                                print(f"No 'matches' in widgets for {path_term_id}")
                        else:
                            print(f"No 'widgets' in section for {path_term_id}")
                    else:
                        print(f"Invalid data structure for {path_term_id}")

                except Exception as e:
                    print(f"Error processing {path_term_id}: {e}")
            else:
                print(f"Request failed for {path_term_id} with status code: {response.status_code}")
        return pd.DataFrame(all_events_data), data

    def fetch_bet_offers(self, event_ids):
        """Fetch bet offers for a list of event IDs."""
        all_rows = []
        for event_id in event_ids:
            url = self.bet_offer_url.format(event_id=event_id)
            response = requests.get(url, headers=self.headers, params=self.params)
            if response.status_code == 200:
                data = response.json()
                for offer in data.get("betOffers", []):
                    for outcome in offer.get("outcomes", []):
                        all_rows.append({
                        'bet_offer_id': offer['id'],
                        'criterion_id': offer['criterion']['id'],
                        'criterion_label': offer['criterion']['label'],
                        'criterion_english_label': offer['criterion'].get('englishLabel', None),  # Safe access
                        'occurrence_type': offer['criterion'].get('occurrenceType', None),  # Safe access
                        'lifetime': offer['criterion'].get('lifetime', None),  # Safe access
                        'bet_offer_type_id': offer['betOfferType']['id'],
                        'bet_offer_type_name': offer['betOfferType']['name'],
                        'bet_offer_type_english_name': offer['betOfferType']['englishName'],
                        'event_id': offer['eventId'],
                        'outcome_id': outcome['id'],
                        'outcome_label': outcome['label'],
                        'outcome_english_label': outcome['englishLabel'],
                        'odds': outcome.get('odds', None),  # Safe access
                        'line': outcome.get('line', None),  # New field added
                        'participant': outcome.get('participant', None),  # New field added
                        'type': outcome['type'],
                        'changed_date': outcome['changedDate'],
                        'odds_fractional': outcome.get('oddsFractional', None),  # Safe access
                        'odds_american': outcome.get('oddsAmerican', None),  # Safe access
                        'status': outcome['status'],
                        'cash_out_status': outcome['cashOutStatus'],
                        'home_score': outcome.get('homeScore', None),  # Safe access
                        'away_score': outcome.get('awayScore', None)   # Safe access
                    })
            else:
                print(f"Kambi: Failed to fetch bet offers for event ID {event_id}: {response.status_code}")
        return pd.DataFrame(all_rows)

    def run(self):
        """Run the full data fetching process."""
        try:
            logging.info("Kambi: Fetching groups...")
            groups_df = self.fetch_groups()
            sport_list = ['/football/costa_rica',
                '/football/world_cup_qualifying_-_south_america',
                '/football/uefa_womens_euro__w_',
                '/football/australia',
                '/football/scotland',
                '/football/romania',
                '/football/finland',
                '/football/argentina',
                '/football/south_africa',
                '/football/poland',
                '/football/northern_ireland',
                '/football/usa',
                '/football/colombia',
                '/football/paraguay',
                '/football/bahrain',
                '/football/israel',
                '/football/african_nations_cup',
                '/football/iraq',
                '/football/fifa_club_world_cup',
                '/football/conference_league',
                '/football/norway',
                '/football/russia',
                '/football/ukraine',
                '/football/england',
                '/football/england/fa_cup',
                '/football/denmark',
                '/football/sweden',
                '/football/spain',
                '/football/club_friendly_matches',
                '/football/champions_league__w_',
                '/football/iceland',
                '/football/champions_league',
                '/football/belgium',
                '/football/uefa_nations_league',
                '/football/france',
                '/football/turkey',
                '/football/europa_league',
                '/football/brazil',
                '/football/guatemala',
                '/football/mexico',
                '/football/greece',
                '/football/australia',
                '/football/italy',
                '/football/ethiopia',
                '/football/copa_libertadores',
                '/football/saudi_arabia',
                '/football/qatar',
                '/football/united_arab_emirates',
                '/football/egypt',
                '/football/germany',
                '/football/portugal',
                '/football/india',
                '/football/netherlands',
                '/football/cyprus',
                '/football/world_cup_qualifying_-_europe',
                '/tennis/atp',
                '/tennis/wta',
                '/tennis/challenger']
            logging.info(f"Kambi: Fetched {len(groups_df)} groups.")

            logging.info("Kambi: Fetching events...")
            events_df, all_path_terms = self.fetch_events(groups_df["pathTermId"], sport_list)
            logging.info(f"Kambi: Fetched {len(events_df)} events.")

            logging.info("Kambi: Fetching bet offers...")
            offers_df = self.fetch_bet_offers(events_df["event_id"].unique())

            final_df = offers_df.merge(events_df[['event_id', 'event_name', 'sport', 'group_name', 'start_time']], on="event_id", how="left")
            
            # Function to reformat names from "Last, First" to "First Last"
            def reformat_name(match_name):
                # Use regex to match "Last, First - Last, First" format
                formatted_names = re.sub(r"(\w+), (\w+)", r"\2 \1", match_name)
                formatted_names = formatted_names.replace(" - ", " vs ")
                return formatted_names

            # Apply the function to the 'event_name' column
            final_df['event_name'] = final_df['event_name'].apply(reformat_name)

            # Define a function to swap and reformat the criterion_label for tennis
            def swap_name_format(row):
                if row['sport'] == 'TENNIS':
                    # Use regex to capture "Last name, First name" pattern and rearrange
                    match = re.search(r"(\w+), (\w+) wint minstens één set", row['criterion_label'])
                    if match:
                        last_name, first_name = match.groups()
                        # Reformat to "First name Last name Wint een Set"
                        return f"{first_name} {last_name} Wint een Set"
                # Return the original criterion_label if conditions are not met
                return row['criterion_label']

            # Apply the function to the criterion_label column
            final_df['criterion_label'] = final_df.apply(swap_name_format, axis=1)

            # Replace values in the 'type' column
            final_df['type'] = final_df['type'].replace({'OT_ONE': '1', 'OT_TWO': '2'})
            logging.info(f"Kambi: Fetched {len(final_df)} bet offers.")
            
            # Upload to Google Cloud Storage
            storage_mgr = get_storage_manager()
            blob_path = storage_mgr.upload_dataframe(final_df, 'unibet')
            logging.info(f"Unibet: Data uploaded to cloud storage: {blob_path}")
            
            return final_df
            
        except Exception as e:
            logging.error(f"Error in Unibet scraper: {str(e)}")
            raise


if __name__ == "__main__":
    fetcher = BettingDataFetcher()
    final_df = fetcher.run()
    final_df.to_csv(f'Data/scrapers/unibet/unibetAllSport{fetcher.now}.csv')