import os
import pandas as pd
from fuzzywuzzy import process, fuzz
from datetime import datetime
import pandas as pd
pd.options.mode.chained_assignment = None  # Suppress SettingWithCopyWarning

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
    
    # Filter Kambi for suitable betting opportunities
    kambi_filtered_football = kambi_raw_football[
        kambi_raw_football['bet_offer_type_english_name'].isin([
            'Match', 'Odd/Even', 'Player Occurrence Line', 'Asian Over/Under', 
            'Over/Under', 'Handicap', 'Asian Handicap', 'Yes/No', 'Head to Head'
        ])
    ]
    
    # Filter Toto for suitable outcome types
    toto_filtered_football = toto_raw_football[
        toto_raw_football['Outcome Type'].isin(['DN', 'OE', 'HH', 'HL', 'AG'])
    ]
    
    # Remove duplicates in Toto data
    toto_filtered_football = toto_filtered_football.drop_duplicates(
        subset=['Event Name', 'Market Name', 'Outcome Name']
    )
    
    # Remove duplicates in Kambi data
    kambi_filtered_football = kambi_filtered_football.drop_duplicates(
        subset=['event_name', 'outcome_label', 'criterion_label']
    )
    
    return toto_filtered_football, kambi_filtered_football


from rapidfuzz import process, fuzz
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
            team1 = parts[0].strip().replace('Athletic', '').replace('Atletico', '')
            team2 = parts[1].strip().replace('Athletic', '').replace('Atletico', '')

            # Perform fuzzy matching for Team1 and Team2 with all kambi events
            def match_teams(team, kambi_team):
                return fuzz.token_set_ratio(team, kambi_team)

            # Perform fuzzy matching for both Team1 and Team2
            kambi_matches = filtered_kambi_winnaar[
                (filtered_kambi_winnaar['Team1'].apply(lambda x: match_teams(team1, x) >= 65)) &
                (filtered_kambi_winnaar['Team2'].apply(lambda x: match_teams(team2, x) >= 65))
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
        left_on=['matched_event', 'standardized_label', 'sex'], 
        right_on=['event_name', 'standardized_label', 'sex'], 
        how='inner'  # Perform an inner join
)

    # Filter merged DataFrame for specific conditions
    merged_df_winnaar = merged_df_winnaar[
        (merged_df_winnaar['matched_event'].notnull()) &
        (merged_df_winnaar['Outcome SubType'] != merged_df_winnaar['type'])
    ]

    return merged_df_winnaar, matched_events


def create_merged_football_overunder(kambi_filtered_football, toto_filtered_football, matched_events):
    """
    Preprocess, match, and merge football betting data from Toto and Kambi for "Over/Under" events.

    Args:
    toto_filtered_football (pd.DataFrame): Filtered Toto football DataFrame.
    kambi_filtered_football (pd.DataFrame): Filtered Kambi football DataFrame.

    Returns:
    pd.DataFrame: Merged DataFrame with matched "Ja/Nee" events and filtered conditions.
    """
    kambi_filtered_football_overunder = kambi_filtered_football[kambi_filtered_football['bet_offer_type_name'].str.contains('Over')]
    toto_filtered_football_overunder = toto_filtered_football[
        (toto_filtered_football['Outcome Name'].str.contains('Over')) | 
        (toto_filtered_football['Outcome Name'].str.contains('Under'))
    ]

    # Remove accents and replace '-' with spaces in the necessary columns
    for column in ['event_name', 'criterion_label', 'criterion_english_label']:
        kambi_filtered_football_overunder[column] = kambi_filtered_football_overunder[column].apply(
            lambda x: ''.join(
                char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
                if not unicodedata.combining(char)
            )
        )
    
    for column in ['Event Name', 'Market Name']:
        toto_filtered_football_overunder[column] = toto_filtered_football_overunder[column].apply(
            lambda x: ''.join(
                char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
                if not unicodedata.combining(char)
            )
        )

    # Get event names for matching
    kambi_events = kambi_filtered_football_overunder['event_name'].tolist()

    # # Function to find the best match
    # def find_best_match(event_name):
    #     result = process.extractOne(event_name, kambi_events, scorer=fuzz.token_set_ratio, score_cutoff=90)
    #     if result is None:
    #         return None
    #     match, _, _ = result
    #     return match

    # Perform a left join to bring in 'matched_event' and 'fuzzy_score' from matched_events
    toto_filtered_football_overunder = toto_filtered_football_overunder.merge(
        matched_events,
        on='Event Name',  # Join on the 'Event Name' column
        how='left'  # Ensure all rows in toto_filtered_football_overunder are retained
    )

    # Define OverUnderType
    def determine_over_under_type(label):
        if 'Sets' in label and 'Games' not in label:
            return 'Sets'
        elif 'Games' in label and 'Set' in label:
            return 'Games in Set'
        elif 'Games' in label:
            return 'Games'
        elif 'Punten' in label:
            return 'Points'
        else:
            return None

    kambi_filtered_football_overunder['OverUnderType'] = kambi_filtered_football_overunder['criterion_label'].apply(
        lambda x: 'Goals' if 'Doelpunten' in x else 'Other'
    )

    toto_filtered_football_overunder['OverUnderType'] = toto_filtered_football_overunder['Market Name'].apply(
        lambda x: 'Goals' if 'Goals' in x else 'Other'
    )

    # Define OverUnderTime
    def determine_over_under_time(label):
        if '1e helft' in label.lower():
            return '1e Helft'
        elif '2e helft' in label.lower():
            return '2e Helft'
        elif label[-2:].isdigit():  # Check if the last two characters in the label are digits
            return label.split(' - ')[-1]
        else:
            return 'Full Time'

    kambi_filtered_football_overunder['OverUnderTime'] = kambi_filtered_football_overunder['criterion_label'].apply(determine_over_under_time)
    toto_filtered_football_overunder['OverUnderTime'] = toto_filtered_football_overunder['Market Name'].apply(determine_over_under_time)

    # Split Team1 and Team2
    kambi_filtered_football_overunder[['Team1', 'Team2']] = kambi_filtered_football_overunder['event_name'].str.split(' vs ', expand=True)
    toto_filtered_football_overunder[['Team1', 'Team2']] = toto_filtered_football_overunder['Event Name'].str.split(' vs ', expand=True)

    # Define OverUnderType2
    kambi_filtered_football_overunder['OverUnderType2'] = kambi_filtered_football_overunder.apply(
        lambda row: '1' if row['Team1'] in row['criterion_english_label'] else (
            '2' if row['Team2'] in row['criterion_english_label'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    toto_filtered_football_overunder['OverUnderType2'] = toto_filtered_football_overunder.apply(
        lambda row: '1' if row['Team1'] in row['Market Name'] else (
            '2' if row['Team2'] in row['Market Name'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    # Extract line value
    toto_filtered_football_overunder['line'] = toto_filtered_football_overunder['Market Name'].apply(
        lambda x: float(x.split('Over/Under ')[-1]) if 'Over/Under ' in x else None
    )

    # Merge the DataFrames
    merged_football_overunder = pd.merge(
        toto_filtered_football_overunder,
        kambi_filtered_football_overunder,
        left_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'matched_event', 'sex'],
        right_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'event_name', 'sex'],
        how='inner'
    )

    # Keep only records with opposite outcomes
    merged_football_overunder = merged_football_overunder[
        merged_football_overunder['outcome_english_label'] != merged_football_overunder['Outcome Name']
    ]

    return merged_football_overunder


def process_football_betting_data(toto_filtered_football, kambi_filtered_football):
    # Call the specific functions to process different bet types
    merged_df_winnaar, matched_events = create_merged_df_winnaar(toto_filtered_football, kambi_filtered_football)
    merged_football_overunder = create_merged_football_overunder(kambi_filtered_football, toto_filtered_football, matched_events)

    # Perform the stacked union
    total_football = pd.concat([merged_football_overunder, merged_df_winnaar], ignore_index=True, sort=True)

    # Calculate Arbitrage Percentage
    total_football['Arbitrage Percentage'] = (1 / total_football['Odds (Decimal)'] + 1 / total_football['odds']) * 100

    # Identify Arbitrage Opportunities
    total_football['Is Arbitrage'] = total_football['Arbitrage Percentage'] < 100

    # Calculate Optimal Stakes if Arbitrage
    total_stake = 1000  # Define the total stake for arbitrage
    total_football['Stake A'] = total_football.apply(
        lambda row: (1 / row['Odds (Decimal)'] / (1 / row['Odds (Decimal)'] + 1 / row['odds']) * total_stake) 
        if row['Is Arbitrage'] else 0,
        axis=1
    )
    total_football['Stake B'] = total_football.apply(
        lambda row: (1 / row['odds'] / (1 / row['Odds (Decimal)'] + 1 / row['odds']) * total_stake) 
        if row['Is Arbitrage'] else 0,
        axis=1
    )

    # Select and return the relevant columns
    result = total_football[[
        'Event Name', 'Market Name', 'Outcome Name', 'outcome_label', 
        'Odds (Decimal)', 'odds', 'Arbitrage Percentage', 
        'Is Arbitrage', 'Stake A', 'Stake B'
    ]]
    
    return result

# toto_file = 'Data/scrapers/Toto/totoAllSports2025-01-26T14:23:09Z.csv'
# kambi_file = 'Data/scrapers/unibet/unibetAllSports2025-01-26T14:23:30Z.csv'

toto_directory = "Data/scrapers/Toto/"
kambi_directory = "Data/scrapers/unibet/"
start_time = datetime.utcnow()

toto_file_path = get_latest_file(toto_directory)
kambi_file_path = get_latest_file(kambi_directory)

print(f"Latest Toto file: {toto_file_path}")
print(f"Latest Kambi file: {kambi_file_path}")

# Process the files
toto_filtered, kambi_filtered = preprocess_football_data(toto_file_path, kambi_file_path)
toto_filtered_football, kambi_filtered_football = preprocess_football_data(toto_file_path, kambi_file_path)

# Perform the stacked union
total_football_results = process_football_betting_data(toto_filtered_football, kambi_filtered_football)
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