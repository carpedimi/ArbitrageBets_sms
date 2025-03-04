import os
import pandas as pd
from fuzzywuzzy import process, fuzz
from datetime import datetime
import pandas as pd
import re
import logging
from notifications import get_notifier
from cloud_storage import get_storage_manager
from dotenv import load_dotenv
pd.options.mode.chained_assignment = None  # Suppress SettingWithCopyWarning

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def get_latest_data():
    """
    Fetch the latest data from Google Cloud Storage for both Toto and Unibet.
    
    Returns:
        tuple: (toto_df, kambi_df) containing the latest data from both sources
    """
    try:
        storage_mgr = get_storage_manager()
        
        logging.info("Fetching latest Toto data from cloud storage...")
        toto_df = storage_mgr.get_latest_file('toto')
        
        logging.info("Fetching latest Unibet data from cloud storage...")
        kambi_df = storage_mgr.get_latest_file('unibet')
        
        return toto_df, kambi_df
        
    except Exception as e:
        logging.error(f"Error fetching data from cloud storage: {str(e)}")
        raise

def get_latest_file(directory: str, file_extension: str = "*.csv") -> str:
    """
    Get the latest file in a directory based on the modification time.

    Args:
    directory (str): The directory path to search for files.
    file_extension (str): The file extension filter (default is "*.csv").

    Returns:
    str: The path to the latest file.
    """
    files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith(file_extension.split('.')[-1]) and (file.startswith('toto') or file.startswith('unibet'))]
    if not files:
        raise FileNotFoundError(f"No files found in {directory} with extension {file_extension}")
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def preprocess_football_data(toto_file_path: str, kambi_file_path: str):
    """
    Preprocess and filter raw Toto and Kambi data for football betting opportunities.

    Args:
    toto_file_path (str): Path to the Toto raw CSV file.
    kambi_file_path (str): Path to the Kambi raw CSV file.

    Returns:
    tuple: Filtered Toto and Kambi DataFrames for football betting opportunities.
    """
    # Load and drop duplicates
    toto_raw = pd.read_csv(toto_file_path, index_col=0).drop_duplicates()
    kambi_raw = pd.read_csv(kambi_file_path, index_col=0).drop_duplicates()
    
    # Filter for football data
    toto_raw_football = toto_raw[toto_raw['sport'] == 'Voetbal']
    kambi_raw_football = kambi_raw[kambi_raw['sport'] == 'FOOTBALL']
    
    # Adjust odds and line in Kambi data
    kambi_raw_football['line'] = kambi_raw_football['line'] / 1000
    kambi_raw_football['odds'] = kambi_raw_football['odds'] / 1000

    # Adjust toto outcome
    toto_raw_football['Outcome SubType'] = toto_raw_football['Outcome SubType'].replace({'H': '1', 'A': '2'})

    # List of women's competitions
    women_competitions_toto = [
    'Portugal Campeonato Nacional, Vrouwen', 'Mexico League MX Vrouwen', 'Australië W-League', 'Italië Coppa Italia Vrouwen',
    'Scotland Women\'s Premier League', 'Nederland Eredivisie Vrouwen', 'England FA Cup Women', 'Engeland FA Super League Vrouwen',
    'Spain Primera División Vrouwen'
    ]

    # Create 'sex' column based on the competition
    toto_raw_football['sex'] = toto_raw_football['competition'].apply(
        lambda x: 'W' if x in women_competitions_toto else 'M'
    )

    # List of women's football competitions
    women_competitions_kambi = [
    'A-League (D)', 'Premier League Dames', 'Campeonato Nacional Feminino', 'Liga MX Femenil (D)', 
    'Frauen-Bundesliga', 'Super League (D)', 'Primera División (D)', 'Coppa Italia (D)', 'Liga MX Femenil'
    ]

    # Create 'sex' column
    kambi_raw_football['sex'] = kambi_raw_football['group_name'].apply(
        lambda x: 'W' if x in women_competitions_kambi else 'M'
    )

    kambi_filtered_football = kambi_raw_football
    toto_filtered_football = toto_raw_football
    
    # # Filter Kambi for suitable betting opportunities
    # kambi_filtered_football = kambi_raw_football[
    #     kambi_raw_football['outcome_english_label'].isin([
    #         'Match', 'Odd/Even', 'Player Occurrence Line', 'Asian Over/Under', 
    #         'Over/Under', 'Handicap', 'Asian Handicap', 'Yes/No', 'Head to Head'
    #     ])
    # ]
    
    # # Filter Toto for suitable outcome types
    # toto_filtered_football = toto_raw_football[
    #     toto_raw_football['Outcome Type'].isin(['DN', 'OE', 'HH', 'HL', 'AG'])
    # ]
    
    # Remove duplicates in Toto data
    toto_filtered_football = toto_filtered_football.drop_duplicates(
        subset=['Event Name', 'Market Name', 'Outcome Name' ,'Odds (Decimal)']
    )
    
    # Remove duplicates in Kambi data
    kambi_filtered_football = kambi_filtered_football.drop_duplicates(
        subset=['event_name', 'outcome_label', 'criterion_label', 'line', 'odds']
    )
    
    return toto_filtered_football, kambi_filtered_football


import pandas as pd
import unicodedata

def create_merged_df_winnaar(toto_filtered_football: pd.DataFrame, kambi_filtered_football: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess, match, and merge football betting data from Toto and Kambi for "winnaar" events.

    Args:
    toto_filtered_football (pd.DataFrame): Filtered Toto football DataFrame.
    kambi_filtered_football (pd.DataFrame): Filtered Kambi football DataFrame.

    Returns:
    pd.DataFrame: Merged DataFrame with matched events and filtered conditions.
    """
    # Filter Kambi data based on 'criterion_label'
    filtered_kambi_winnaar = kambi_filtered_football[
        kambi_filtered_football['criterion_label'].str.contains('Draw No Bet', na=False)
    ]

    # Filter Toto data based on 'Market Name'
    filtered_toto_winnaar = toto_filtered_football[
        toto_filtered_football['Market Name'].str.contains('Draw No Bet', na=False)
    ]

    # Preprocess strings: Replace '-' with spaces and remove accents
    def preprocess_text(text):
        return ''.join(
            char for char in unicodedata.normalize('NFKD', text.replace('-', ' '))
            if not unicodedata.combining(char)
        )

    # Apply preprocessing to 'event_name' in Kambi and 'Event Name' in Toto
    filtered_kambi_winnaar['event_name'] = filtered_kambi_winnaar['event_name'].apply(preprocess_text)
    filtered_toto_winnaar['Event Name'] = filtered_toto_winnaar['Event Name'].apply(preprocess_text)

    # Create 'Team1' and 'Team2'
    filtered_kambi_winnaar[['Team1', 'Team2']] = filtered_kambi_winnaar['event_name'].str.split(' vs ', expand=True)
    filtered_toto_winnaar[['Team1', 'Team2']] = filtered_toto_winnaar['Event Name'].str.split(' vs ', expand=True)

    # Create a list of Kambi event names
    kambi_events = filtered_kambi_winnaar['event_name'].tolist()

    # Function to find the best match based on Team1 and Team2 fuzzy matching
    def find_best_match(event_name):
        # Extract Team1 and Team2 from the event_name
        parts = event_name.split(' vs ')
        if len(parts) == 2:
            # Remove parts that otherwise result in 100% score in fuzzymatching
            team1 = parts[0].strip()
            team2 = parts[1].strip()

            # Perform fuzzy matching for Team1 and Team2 with all kambi events
            def match_teams(team, kambi_team):
                return fuzz.token_set_ratio(team, kambi_team)

            # Perform fuzzy matching for both Team1 and Team2
            kambi_matches = filtered_kambi_winnaar[
                (filtered_kambi_winnaar['Team1'].apply(lambda x: match_teams(team1, x) >= 80)) &
                (filtered_kambi_winnaar['Team2'].apply(lambda x: match_teams(team2, x) >= 80))
            ]
            
            # If there is a match, return the matched event_name and fuzzy match score
            if not kambi_matches.empty:
                # Get the best match score
                best_score_team1 = kambi_matches['Team1'].apply(lambda x: match_teams(team1, x)).max()
                best_score_team2 = kambi_matches['Team2'].apply(lambda x: match_teams(team2, x)).max()

                # Calculate the average score (or you can choose to return another metric)
                average_score = (best_score_team1 + best_score_team2) / 2

                # Get the matched event name
                matched_event_name = kambi_matches['event_name'].iloc[0]

                return matched_event_name, average_score  # Return the matched event and score

        return None, None  # Return None if no match found

    # Apply matching function to toto_filtered_football
    filtered_toto_winnaar[['matched_event', 'fuzzy_score']] = filtered_toto_winnaar['Event Name'].apply(
        lambda x: pd.Series(find_best_match(x))
    )

    # Get unique records from 'Event Name' and 'matched_event'
    matched_events = filtered_toto_winnaar[['Event Name', 'matched_event', 'fuzzy_score']].drop_duplicates()

    # Define a transformation function for standardization
    def standardize_draw_no_bet(value):
        if '1e Helft' in value:
            return 'Draw No Bet - 1e Helft'
        elif '2e Helft' in value:
            return 'Draw No Bet - 2e Helft'
        elif 'Draw No Bet' in value:
            return 'Draw No Bet'
        else:
            return value  # Keep original if no match

    # Apply the transformation to the relevant columns
    filtered_kambi_winnaar['standardized_label'] = filtered_kambi_winnaar['criterion_label'].apply(standardize_draw_no_bet)
    filtered_toto_winnaar['standardized_label'] = filtered_toto_winnaar['Market Name'].apply(standardize_draw_no_bet)

    # Merge the DataFrames using the matched event column
    merged_df_winnaar = pd.merge(
        filtered_toto_winnaar,
        filtered_kambi_winnaar,
        left_on=['matched_event', 'standardized_label', 'sex', 'start_time'], 
        right_on=['event_name', 'standardized_label', 'sex', 'start_time'], 
        how='inner'  # Perform an inner join
)

    # Filter merged DataFrame for specific conditions
    merged_df_winnaar = merged_df_winnaar[
        (merged_df_winnaar['matched_event'].notnull()) &
        (merged_df_winnaar['Outcome SubType'] != merged_df_winnaar['outcome_label'])
    ].drop_duplicates()

    return merged_df_winnaar, matched_events


# def create_merged_football_overunder(kambi_filtered_football, toto_filtered_football, matched_events):
#     """
#     Preprocess, match, and merge football betting data from Toto and Kambi for "Over/Under" events.

#     Args:
#     toto_filtered_football (pd.DataFrame): Filtered Toto football DataFrame.
#     kambi_filtered_football (pd.DataFrame): Filtered Kambi football DataFrame.

#     Returns:
#     pd.DataFrame: Merged DataFrame with matched "Ja/Nee" events and filtered conditions.
#     """
#     kambi_filtered_football_overunder = kambi_filtered_football[(kambi_filtered_football['outcome_english_label'].str.contains('Over')) |
#         (kambi_filtered_football['outcome_english_label'].str.contains('Under'))
#     ]
#     toto_filtered_football_overunder = toto_filtered_football[
#         (toto_filtered_football['Outcome Name'].str.contains('Over')) |
#         (toto_filtered_football['Outcome Name'].str.contains('Under')) |
#         (toto_filtered_football['Outcome Name'].str.match(r'^\d+\+$')) |  # matches strings that ONLY contain digit(s) followed by '+'
#         (toto_filtered_football['Outcome Name'].str.contains(r'\d{1,2} of meer')) &
#         ~(toto_filtered_football['Outcome Name'].str.contains(' en ')) &  # excludes strings containing ' en '
#         ~(toto_filtered_football['Outcome Name'].str.contains('&'))  # excludes strings containing '&'
#     ]

#     # Remove accents and replace '-' with spaces in the necessary columns
#     for column in ['event_name', 'criterion_label', 'criterion_english_label']:
#         kambi_filtered_football_overunder[column] = kambi_filtered_football_overunder[column].apply(
#             lambda x: ''.join(
#                 char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
#                 if not unicodedata.combining(char)
#             )
#         )
    
#     for column in ['Event Name', 'Market Name']:
#         toto_filtered_football_overunder[column] = toto_filtered_football_overunder[column].apply(
#             lambda x: ''.join(
#                 char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
#                 if not unicodedata.combining(char)
#             )
#         )

#     # Get event names for matching
#     kambi_events = kambi_filtered_football_overunder['event_name'].tolist()

#     # # Function to find the best match
#     # def find_best_match(event_name):
#     #     result = process.extractOne(event_name, kambi_events, scorer=fuzz.token_set_ratio, score_cutoff=90)
#     #     if result is None:
#     #         return None
#     #     match, _, _ = result
#     #     return match

#     # Perform a left join to bring in 'matched_event' and 'fuzzy_score' from matched_events
#     toto_filtered_football_overunder = toto_filtered_football_overunder.merge(
#         matched_events,
#         on='Event Name',  # Join on the 'Event Name' column
#         how='left'  # Ensure all rows in toto_filtered_football_overunder are retained
#     )

#     # Define OverUnderType
#     # Create 'OverUnderType'
#     kambi_filtered_football_overunder['OverUnderType'] = kambi_filtered_football_overunder['criterion_label'].apply(
#             lambda x: 'Goals' if ('Doelpunten' in x and 'Resultaat' not in x) 
#             else 'Wedstrijd schoten op doel' if ('Totaal Aantal Schoten op Doel' in x and ' & ' not in x)
#             else 'Team schoten op doel' if ('Totaal Aantal Schoten op Doel door' in x and ' & ' not in x) 
#             else 'Team schoten' if ('Totaal Aantal Schoten door' in x and ' & ' not in x) 
#             else 'Totaal schoten op doel' if ('Totaal Aantal Schoten op Doel' in x and ' & ' not in x)
#             else 'Totaal schoten' if ('Totaal Aantal Schoten' in x and ' & ' not in x)
#             else 'Speler schoten op doel' if ('Schoten van Speler op Doel' in x and ' & ' not in x)
#             else 'Speler schoten' if ('Schoten van Speler' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
#             else 'Dubbele Kans' if ('Dubbele Kans' in x or ' en ' in x or ' & ' in x)
#             else 'other'
#         )

#     toto_filtered_football_overunder['OverUnderType'] = toto_filtered_football_overunder['Market Name'].apply(
#         lambda x: 'Goals' if ('Goals' in x and 'Resultaat' not in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
#         # else 'Speler schoten op doel' if ('Speler schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
#         else 'Wedstrijd schoten op doel' if ('Wedstrijd schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
#         else 'Team schoten op doel' if ('Team schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
#         # else 'Schoten op doel van buiten 16 mtr' if ('schoten op doel van buiten 16 mtr' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) # via specials voor later
#         else 'Speler schoten op doel' if ('aantal schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) or ('Speler schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
#         else 'Speler schoten' if ('aantal schoten' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
#         else 'Dubbele Kans' if ('Dubbele Kans' in x or ' en ' in x or ' & ' in x)
#         else 'other'
#     )

#     # Define OverUnderTime
#     def determine_over_under_time(label):
#         label_lower = label.lower()
#         if '1e helft' in label_lower:
#             return '1e Helft'
#         elif '2e helft' in label_lower:
#             return '2e Helft'
#         elif 'eerste 10 minuten' in label_lower:
#             return '00:00 09:59'
#         # Check if the label contains a time range in the format 'XX:XX YY:YY'
#         time_match = re.search(r'\b\d{1,2}:\d{2} \d{1,2}:\d{2}\b', label)
#         if time_match:
#             return time_match.group()  # Return the matched time range
#         else:
#             return 'Full Time'

#     kambi_filtered_football_overunder['OverUnderTime'] = kambi_filtered_football_overunder['criterion_label'].apply(determine_over_under_time)
#     toto_filtered_football_overunder['OverUnderTime'] = toto_filtered_football_overunder['Market Name'].apply(determine_over_under_time)

#     # Split Team1 and Team2
#     kambi_filtered_football_overunder[['Team1', 'Team2']] = kambi_filtered_football_overunder['event_name'].str.split(' vs ', expand=True)
#     toto_filtered_football_overunder[['Team1', 'Team2']] = toto_filtered_football_overunder['Event Name'].str.split(' vs ', expand=True)

#     # Define OverUnderType2
#     kambi_filtered_football_overunder['OverUnderType2'] = kambi_filtered_football_overunder.apply(
#         lambda row: row['participant'] if row['OverUnderType'] in ['Speler schoten op doel', 'Speler schoten'] else (
#             '1' if row['Team1'] in row['criterion_english_label'] else 
#             '2' if row['Team2'] in row['criterion_english_label'] else 
#             'Total team 1 and team 2'
#         ),
#         axis=1
#     )

#     toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder.apply(
#         lambda row: (
#             '1' if row['OverUnderType'] == 'Goals' and row['Team1'] in row['Market Name'] else
#             '2' if row['OverUnderType'] == 'Goals' and row['Team2'] in row['Market Name'] else
#             'Total team 1 and team 2' if row['OverUnderType'] == 'Goals' else
#             'Total team 1 and team 2' if row['OverUnderType'] == 'Wedstrijd schoten op doel' else
#             '1' if row['OverUnderType'] == 'Team schoten op doel' and row['Team1'] in row['Outcome Name'] else
#             '2' if row['OverUnderType'] == 'Team schoten op doel' and row['Team2'] in row['Outcome Name'] else
#             row['Market Name'].split('aantal schoten')[0].strip() if row['OverUnderType'] in ['Speler schoten op doel', 'Speler schoten'] else # Get player name
#             row['OverUnderType']  # Default to OverUnderType if none of the conditions apply
#         ),
#         axis=1
#     )

#     def extract_line_toto(row):
#         market_name = row['Market Name']
#         outcome_name = row['Outcome Name']
#         over_under_type = row['OverUnderType']

#         # Extract number from 'Market Name' when it contains 'Over/Under '
#         if 'Over/Under ' in market_name:
#             return float(market_name.split('Over/Under ')[-1])

#         # Extract number from 'Outcome Name'
#         match = re.search(r'\d+(\.\d+)?', outcome_name)
#         if match:
#             value = float(match.group())  # Convert extracted number to float

#             # Adjust value based on conditions
#             if over_under_type in ['Wedstrijd schoten op doel', 'Team schoten op doel'] and 'of meer' in outcome_name:
#                 return value - 0.5
#             elif over_under_type in ['Speler schoten op doel', 'Speler schoten'] and '+' in outcome_name:
#                 return value - 0.5
#             else:
#                 return value

#         return None  # Default if no number found

#     # Apply function to create 'line' column
#     toto_filtered_football_overunder['line'] = toto_filtered_football_overunder.apply(extract_line_toto, axis=1)

#     # Adjust Outcome Name where {digit} + to over
#     toto_filtered_football_overunder['Outcome Name'] = toto_filtered_football_overunder['Outcome Name'].apply(
#         lambda outcome_name: 'Over' if re.search(r'\d+(\.\d+)?\+', outcome_name) else outcome_name
#     )

#     def find_best_fuzzy_match(overunder_type2, kambi_df):
#         try:
#             # Perform fuzzy matching with all Kambi OverUnderType2 values
#             match_scores = kambi_df['OverUnderType2'].apply(lambda x: fuzz.token_set_ratio(overunder_type2, x))
            
#             # Get the best match score and the corresponding OverUnderType2 value
#             best_match_idx = match_scores.idxmax()
#             best_score = match_scores.max()
            
#             if best_score >= 80:  # Threshold for fuzzy matching
#                 return kambi_df.loc[best_match_idx, 'OverUnderType2'], best_score
#             return None, None  # No match found
#         except Exception as e:
#             print(f"Error in fuzzy matching for {overunder_type2}: {e}")
#             return None, None

#     # When applying, handle potential None returns
#     def safe_fuzzy_match(x):
#         match, score = find_best_fuzzy_match(x, filtered_kambi)
#         return pd.Series([match if match is not None else x, 
#                         score if score is not None else 0])

#     filtered_toto[['matched_OverUnderType2', 'fuzzy_score']] = filtered_toto['OverUnderType2'].apply(
#         safe_fuzzy_match, 
#         result_type='expand'
#     )

#     # Filter records where OverUnderType contains 'Team' or 'Speler'
#     filtered_toto = toto_filtered_football_overunder[
#         toto_filtered_football_overunder['OverUnderType'].str.contains('Speler')
#     ]
#     filtered_kambi = kambi_filtered_football_overunder[
#         kambi_filtered_football_overunder['OverUnderType'].str.contains('Speler')
#     ]

#     # Apply fuzzy matching function
#     filtered_toto[['matched_OverUnderType2', 'fuzzy_score']] = filtered_toto['OverUnderType2'].apply(
#         lambda x: pd.Series(find_best_fuzzy_match(x, filtered_kambi), result_type='expand')
#     )

#     # Merge back to the original dataset (if needed)
#     toto_filtered_football_overunder = toto_filtered_football_overunder.merge(
#         filtered_toto[['OverUnderType2', 'matched_OverUnderType2', 'fuzzy_score']],
#         on='OverUnderType2',
#         how='left'
#     )

#     # Fill OverUnderType2 with matched_OverUnderType2 if it is null then OverUnderType2
#     toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder.apply(
#         lambda row: row['OverUnderType2'] if pd.isna(row['matched_OverUnderType2']) else row['matched_OverUnderType2'], axis=1
#     )

#     # Merge the DataFrames
#     merged_football_overunder = pd.merge(
#         toto_filtered_football_overunder,
#         kambi_filtered_football_overunder,
#         left_on=['line', 'OverUnderType', 'OverUnderTime', 'final_OverUnderType', 'matched_event', 'sex', 'start_time'],
#         right_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'event_name', 'sex', 'start_time'],
#         how='inner'
#     )

#     merged_football_overunder['Outcome Name cleaned'] = merged_football_overunder['Outcome Name'].apply(
#         lambda x: 'Over' if re.search(r'\b\d{1,2} of meer\b', x) else x
#     )

#     # Keep only records with opposite outcomes
#     merged_football_overunder = merged_football_overunder[
#         merged_football_overunder['outcome_english_label'] != merged_football_overunder['Outcome Name cleaned']
#     ]

#     return merged_football_overunder


def create_merged_football_overunder(kambi_filtered_football, toto_filtered_football, matched_events):
    """
    Merge football betting Over/Under data from Toto and Kambi with comprehensive filtering and matching.
    
    Args:
    kambi_filtered_football (pd.DataFrame): Kambi football betting data
    toto_filtered_football (pd.DataFrame): Toto football betting data
    matched_events (pd.DataFrame): Matched events data
    
    Returns:
    pd.DataFrame: Merged and filtered Over/Under betting data
    """
    # Filter Over/Under events
    kambi_filtered_football_overunder = kambi_filtered_football[
        (kambi_filtered_football['outcome_english_label'].str.contains('Over')) |
        (kambi_filtered_football['outcome_english_label'].str.contains('Under'))
    ]
    
    toto_filtered_football_overunder = toto_filtered_football[
        (toto_filtered_football['Outcome Name'].str.contains('Over')) |
        (toto_filtered_football['Outcome Name'].str.contains('Under')) |
        (toto_filtered_football['Outcome Name'].str.match(r'^\d+\+$')) |
        (toto_filtered_football['Outcome Name'].str.contains(r'\d{1,2} of meer')) &
        ~(toto_filtered_football['Outcome Name'].str.contains(' en ')) &
        ~(toto_filtered_football['Outcome Name'].str.contains('&'))
    ]

    # Normalize text function
    def normalize_text(x):
        return ''.join(
            char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
            if not unicodedata.combining(char)
        )

    # Normalize columns
    kambi_text_columns = ['event_name', 'criterion_label', 'criterion_english_label']
    for col in kambi_text_columns:
        kambi_filtered_football_overunder[col] = kambi_filtered_football_overunder[col].apply(normalize_text)

    toto_text_columns = ['Event Name', 'Market Name']
    for col in toto_text_columns:
        toto_filtered_football_overunder[col] = toto_filtered_football_overunder[col].apply(normalize_text)

    kambi_events = kambi_filtered_football_overunder['event_name'].tolist()

    # Merge with matched events
    toto_filtered_football_overunder = toto_filtered_football_overunder.merge(
        matched_events,
        on='Event Name',
        how='left'
    )

    # Create 'OverUnderType' for Kambi
    kambi_filtered_football_overunder['OverUnderType'] = kambi_filtered_football_overunder['criterion_label'].apply(
        lambda x: 'Goals' if ('Doelpunten' in x and 'Resultaat' not in x and 'Doelpuntenmaker' not in x) 
        else 'Team schoten op doel' if ('Totaal Aantal Schoten op Doel door' in x and ' & ' not in x) 
        else 'Wedstrijd schoten op doel' if ('Totaal Aantal Schoten op Doel' in x and ' & ' not in x)
        else 'Team schoten' if ('Totaal Aantal Schoten door' in x and ' & ' not in x) 
        else 'Wedstrijd schoten' if ('Totaal Aantal Schoten' in x and ' & ' not in x)
        else 'Speler schoten op doel' if ('Schoten van Speler op Doel' in x and ' & ' not in x)
        else 'Speler schoten' if ('Schoten van Speler' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
        else 'Dubbele Kans' if ('Dubbele Kans' in x or ' en ' in x or ' & ' in x)
        else 'other'
    )

    # Determine Over/Under Time
    def determine_over_under_time(label):
        label_lower = label.lower()
        if '1e helft' in label_lower:
            return '1e Helft'
        elif '2e helft' in label_lower:
            return '2e Helft'
        
        time_match = re.search(r'\b\d{1,2}:\d{2} \d{1,2}:\d{2}\b', label)
        if time_match:
            return time_match.group()
        
        return 'Full Time'

    # Apply time determination
    kambi_filtered_football_overunder['OverUnderTime'] = kambi_filtered_football_overunder['criterion_label'].apply(determine_over_under_time)

    # Split teams
    kambi_filtered_football_overunder[['Team1', 'Team2']] = kambi_filtered_football_overunder['event_name'].str.split(' vs ', expand=True)

    # Create OverUnderType2
    kambi_filtered_football_overunder['OverUnderType2'] = kambi_filtered_football_overunder.apply(
        lambda row: row['participant'] if row['OverUnderType'] in ['Speler schoten op doel', 'Speler schoten'] else (
            '1' if row['Team1'] in row['criterion_english_label'] else 
            '2' if row['Team2'] in row['criterion_english_label'] else 
            'Total team 1 and team 2'
        ),
        axis=1
    )

    # Create 'OverUnderType' for toto
    toto_filtered_football_overunder['OverUnderType'] = toto_filtered_football_overunder['Market Name'].apply(
        lambda x: 'Goals' if ('Goals' in x and 'Resultaat' not in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
        # else 'Speler schoten op doel' if ('Speler schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
        else 'Wedstrijd schoten op doel' if ('Wedstrijd schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
        else 'Team schoten op doel' if ('Team schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
        # else 'Schoten op doel van buiten 16 mtr' if ('schoten op doel van buiten 16 mtr' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) # via specials voor later
        else 'Speler schoten op doel' if ('aantal schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) or ('Speler schoten op doel' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x)
        else 'Speler schoten' if ('Aantal Schoten' in x and 'Dubbele Kans' not in x and ' en ' not in x and ' & ' not in x) 
        else 'Dubbele Kans' if ('Dubbele Kans' in x or ' en ' in x or ' & ' in x)
        else 'other'
    )

    # Create 'OverUnderTime'
    toto_filtered_football_overunder['OverUnderTime'] = toto_filtered_football_overunder['Market Name'].apply(
        lambda x: '1e Helft' if '1e helft' in x.lower() else '2e Helft' if '2e helft' in x.lower() 
        else  '00:00 09:59' if 'eerste 10 minuten' in x.lower() else 'Full Time'
    )

    # Create 'Team1' and 'Team2'
    toto_filtered_football_overunder[['Team1', 'Team2']] = toto_filtered_football_overunder['Event Name'].str.split(' vs ', expand=True)

    # Create 'OverUnderType2'
    toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder.apply(
        lambda row: (
            '1' if row['OverUnderType'] == 'Goals' and row['Team1'] in row['Market Name'] else
            '2' if row['OverUnderType'] == 'Goals' and row['Team2'] in row['Market Name'] else
            'Total team 1 and team 2' if row['OverUnderType'] == 'Goals' else
            'Total team 1 and team 2' if row['OverUnderType'] == 'Wedstrijd schoten op doel' else
            '1' if row['OverUnderType'] == 'Team schoten op doel' and row['Team1'] in row['Outcome Name'] else
            '2' if row['OverUnderType'] == 'Team schoten op doel' and row['Team2'] in row['Outcome Name'] else
            row['Market Name'].split('aantal schoten')[0].strip() if row['OverUnderType'] in ['Speler schoten op doel', 'Speler schoten'] 
            else 'other'  # Default to OverUnderType if none of the conditions apply
        ),
        axis=1
    )


    def extract_line_toto(row):
        market_name = row['Market Name']
        outcome_name = row['Outcome Name']
        over_under_type = row['OverUnderType']

        # Extract number from 'Market Name' when it contains 'Over/Under '
        if 'Over/Under ' in market_name:
            return float(market_name.split('Over/Under ')[-1])

        # Extract number from 'Outcome Name'
        match = re.search(r'\d+(\.\d+)?', outcome_name)
        if match:
            value = float(match.group())  # Convert extracted number to float

            # Adjust value based on conditions
            if over_under_type in ['Wedstrijd schoten op doel', 'Team schoten op doel'] and 'of meer' in outcome_name:
                return value - 0.5
            elif over_under_type in ['Speler schoten op doel', 'Speler schoten'] and '+' in outcome_name:
                return value - 0.5
            else:
                return value

        return None  # Default if no number found

    # Apply function to create 'line' column
    toto_filtered_football_overunder['line'] = toto_filtered_football_overunder.apply(extract_line_toto, axis=1)

    # Adjust Outcome Name where {digit} + to over
    toto_filtered_football_overunder['Outcome Name'] = toto_filtered_football_overunder['Outcome Name'].apply(
        lambda outcome_name: 'Over' if re.search(r'\d+(\.\d+)?\+', outcome_name) else outcome_name
    )

    def find_best_fuzzy_match(row, kambi_df):
        overunder_type2 = row['OverUnderType2']
        start_time = row['start_time']
        
        # First subset based on matching start_time
        time_matched_events = kambi_df[kambi_df['start_time'] == start_time]
        
        if not time_matched_events.empty:
            # Perform fuzzy matching with time-matched Kambi OverUnderType2 values
            match_scores = time_matched_events['OverUnderType2'].apply(
                lambda x: fuzz.token_set_ratio(str(overunder_type2), str(x))
            )
            
            # Get the best match score and the corresponding OverUnderType2 value
            if len(match_scores) > 0:
                best_match_idx = match_scores.idxmax()
                best_score = match_scores.max()
                
                if best_score >= 90:  # Threshold for fuzzy matching
                    return time_matched_events.loc[best_match_idx, 'OverUnderType2'], best_score
        return None, None  # No match found

    # Filter records
    filtered_toto = toto_filtered_football_overunder[
        toto_filtered_football_overunder['OverUnderType'].str.contains('Speler', na=False)
    ].copy()

    filtered_kambi = kambi_filtered_football_overunder[
        kambi_filtered_football_overunder['OverUnderType'].str.contains('Speler', na=False)
    ].copy()

    # Apply fuzzy matching function - now passing both OverUnderType2 and start_time
    fuzzy_matches = filtered_toto.apply(
        lambda row: find_best_fuzzy_match(row, filtered_kambi),
        axis=1
    )

    filtered_toto['matched_OverUnderType2'] = fuzzy_matches.apply(lambda x: x[0])
    filtered_toto['fuzzy_score'] = fuzzy_matches.apply(lambda x: x[1])

    filtered_toto.to_csv('examine_fuzzy_matches_player.csv', index=False)

    # Merge back to the original dataset
    toto_filtered_football_overunder = toto_filtered_football_overunder.merge(
        filtered_toto[['OverUnderType2', 'matched_OverUnderType2', 'fuzzy_score']],
        on='OverUnderType2',
        how='left'
    )

    # Fill OverUnderType2 with matched_OverUnderType2 if it is null
    toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder['matched_OverUnderType2'].fillna(toto_filtered_football_overunder['OverUnderType2'])

    # # Fill OverUnderType2 with matched_OverUnderType2 if it is null
    # toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder['matched_OverUnderType2'].fillna(toto_filtered_football_overunder['OverUnderType2'])

    # Merge DataFrames
    merged_football_overunder = pd.merge(
        toto_filtered_football_overunder,
        kambi_filtered_football_overunder,
        left_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'matched_event', 'sex', 'start_time'],
        right_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'event_name', 'sex', 'start_time'],
        how='inner'
    )

    # Clean Outcome Names
    merged_football_overunder['Outcome Name cleaned'] = merged_football_overunder['Outcome Name'].apply(
        lambda x: 'Over' if re.search(r'\b\d{1,2} of meer\b', x) else x
    )

    # Filter opposite outcomes
    merged_football_overunder = merged_football_overunder[
        merged_football_overunder['outcome_english_label'] != merged_football_overunder['Outcome Name cleaned']
    ].drop_duplicates()

    return merged_football_overunder 


def process_football_betting_data(toto_filtered_football, kambi_filtered_football):
    """Process football betting data and find arbitrage opportunities."""
    try:
        # Initialize SMS notifier
        notifier = get_notifier()
        
        # Process winnaar bets
        merged_df_winnaar, matched_events = create_merged_df_winnaar(toto_filtered_football, kambi_filtered_football)
        
        # Calculate arbitrage opportunities for winnaar bets
        for _, row in merged_df_winnaar.iterrows():
            toto_odds = row['Odds (Decimal)']
            kambi_odds = row['odds']
            
            # Calculate potential profit ratio
            profit_ratio = min(toto_odds, kambi_odds) / max(toto_odds, kambi_odds)
            
            if profit_ratio >= notifier.min_profit_threshold:
                notifier.send_arbitrage_notification(
                    event_name=row['Event Name'],
                    market_type=row['Market Name'],
                    profit_ratio=profit_ratio,
                    toto_odds=toto_odds,
                    kambi_odds=kambi_odds
                )
        
        # Process over/under bets
        merged_df_overunder = create_merged_football_overunder(kambi_filtered_football, toto_filtered_football, matched_events)
        
        # Calculate arbitrage opportunities for over/under bets
        for _, row in merged_df_overunder.iterrows():
            toto_odds = row['Odds (Decimal)']
            kambi_odds = row['odds']
            
            # Calculate potential profit ratio
            profit_ratio = min(toto_odds, kambi_odds) / max(toto_odds, kambi_odds)
            
            if profit_ratio >= notifier.min_profit_threshold:
                notifier.send_arbitrage_notification(
                    event_name=row['Event Name'],
                    market_type=f"Over/Under - {row['line']}",
                    profit_ratio=profit_ratio,
                    toto_odds=toto_odds,
                    kambi_odds=kambi_odds
                )
                
    except Exception as e:
        logging.error(f"Error processing football betting data: {str(e)}")
        raise

# toto_file = 'Data/scrapers/Toto/totoAllSports2025-01-26T14:23:09Z.csv'
# kambi_file = 'Data/scrapers/unibet/unibetAllSports2025-01-26T14:23:30Z.csv'

toto_directory = "Data/scrapers/Toto/"
kambi_directory = "Data/scrapers/unibet/"
start_time = datetime.utcnow()

toto_file_path = get_latest_file(toto_directory)
kambi_file_path = get_latest_file(kambi_directory)

print(f"Latest Toto file Football: {toto_file_path}")
print(f"Latest Kambi file Football: {kambi_file_path}")

# Process the files
toto_filtered, kambi_filtered = preprocess_football_data(toto_file_path, kambi_file_path)
toto_filtered_football, kambi_filtered_football = preprocess_football_data(toto_file_path, kambi_file_path)

# Perform the stacked union
total_football_results, merged_df_winnaar, merged_football_overunder= process_football_betting_data(toto_filtered_football, kambi_filtered_football)
total_football_results.to_csv(f'test_total_merge_Football_{start_time}.csv')

# Check if latest output file contains Arbitrage opportunities
try:
    arbitrage_found = False
    arbitrage_messages = []
    
    if total_football_results["Is Arbitrage"].any():
        arbitrage_messages.append(f"Arbitrage opportunity found in Football: test_total_merge_Football_{start_time}.csv")
        arbitrage_found = True
        
    if arbitrage_found:
        print("\n".join(arbitrage_messages))
    else:
        print("Football: No arbitrage opportunities found.")
        
except Exception as e:
    print(f"Football: Error checking for arbitrage opportunities: {str(e)}")

if __name__ == "__main__":
    start_time = datetime.utcnow()
    
    try:
        # Get latest data from cloud storage
        toto_filtered, kambi_filtered = get_latest_data()
        
        # Process the data
        toto_filtered_football, kambi_filtered_football = preprocess_football_data(toto_filtered, kambi_filtered)
        
        # Process betting data and find arbitrage opportunities
        process_football_betting_data(toto_filtered_football, kambi_filtered_football)
        
    except Exception as e:
        logging.error(f"Error in main process: {str(e)}")
        raise