import pandas as pd
from fuzzywuzzy import process, fuzz
from datetime import datetime
import pandas as pd
import os
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

def preprocess_tennis_data(toto_file_path: str, kambi_file_path: str):
    """
    Preprocess and filter raw Toto and Kambi data for tennis betting opportunities.

    Args:
    toto_file_path (str): Path to the Toto raw CSV file.
    kambi_file_path (str): Path to the Kambi raw CSV file.

    Returns:
    tuple: Filtered Toto and Kambi DataFrames for tennis betting opportunities.
    """
    # Load and drop duplicates
    toto_raw = pd.read_csv(toto_file_path, index_col=0).drop_duplicates()
    kambi_raw = pd.read_csv(kambi_file_path, index_col=0).drop_duplicates()
    
    # Filter for tennis data
    toto_raw_tennis = toto_raw[toto_raw['sport'] == 'Tennis']
    kambi_raw_tennis = kambi_raw[kambi_raw['sport'] == 'TENNIS']
    
    # Adjust odds and line in Kambi data
    kambi_raw_tennis['line'] = kambi_raw_tennis['line'] / 1000
    kambi_raw_tennis['odds'] = kambi_raw_tennis['odds'] / 1000
    
    # Adjust toto outcome
    toto_raw_tennis['Outcome SubType'] = toto_raw_tennis['Outcome SubType'].replace({'H': '1', 'A': '2'})
    
    # Filter Kambi for suitable betting opportunities
    kambi_filtered_tennis = kambi_raw_tennis[
        kambi_raw_tennis['bet_offer_type_english_name'].isin([
            'Match', 'Odd/Even', 'Player Occurrence Line', 'Asian Over/Under', 
            'Over/Under', 'Handicap', 'Asian Handicap', 'Yes/No', 'Head to Head'
        ])
    ]
    
    # Filter Toto for suitable outcome types
    toto_filtered_tennis = toto_raw_tennis[
        toto_raw_tennis['Outcome Type'].isin(['DN', 'OE', 'HH', 'HL', 'AG'])
    ]
    
    # Remove duplicates in Toto data
    toto_filtered_tennis = toto_filtered_tennis.drop_duplicates(
        subset=['Event Name', 'Market Name', 'Outcome Name']
    )
    
    # Remove duplicates in Kambi data
    kambi_filtered_tennis = kambi_filtered_tennis.drop_duplicates(
        subset=['event_name', 'outcome_label', 'criterion_label']
    )
    
    return toto_filtered_tennis, kambi_filtered_tennis


from rapidfuzz import process, fuzz
import pandas as pd
import unicodedata

def create_merged_df_winnaar(toto_filtered_tennis: pd.DataFrame, kambi_filtered_tennis: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess, match, and merge tennis betting data from Toto and Kambi for "winnaar" events.

    Args:
    toto_filtered_tennis (pd.DataFrame): Filtered Toto tennis DataFrame.
    kambi_filtered_tennis (pd.DataFrame): Filtered Kambi tennis DataFrame.

    Returns:
    pd.DataFrame: Merged DataFrame with matched events and filtered conditions.
    """
    # Filter Kambi data based on 'criterion_label'
    filtered_kambi_winnaar = kambi_filtered_tennis[
        kambi_filtered_tennis['criterion_label'].str.contains('Wedstrijdnotering', na=False)
    ]

    # Filter Toto data based on 'Market Name'
    filtered_toto_winnaar = toto_filtered_tennis[
        toto_filtered_tennis['Market Name'].str.contains('Wedstrijd', na=False)
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

    # Create a list of Kambi event names
    kambi_events = filtered_kambi_winnaar['event_name'].tolist()

    # Function to find the best match for an event name
    def find_best_match(event_name):
        result = process.extractOne(event_name, kambi_events, scorer=fuzz.token_set_ratio, score_cutoff=90)
        if result is None:
            return None
        match, score, _ = result
        return match

    # Apply matching function to Toto data
    filtered_toto_winnaar['matched_event'] = filtered_toto_winnaar['Event Name'].apply(find_best_match)

    # Merge the DataFrames using the matched event column
    merged_df_winnaar = pd.merge(
        filtered_toto_winnaar,
        filtered_kambi_winnaar,
        left_on=['matched_event', 'start_time'],
        right_on=['event_name', 'start_time'],
        how='inner'
    )

    # Filter merged DataFrame for specific conditions
    merged_df_winnaar = merged_df_winnaar[
        (merged_df_winnaar['matched_event'].notnull()) &
        (merged_df_winnaar['Outcome SubType'] != merged_df_winnaar['type'])
    ]

    return merged_df_winnaar


def create_merged_tennis_overunder(kambi_filtered_tennis, toto_filtered_tennis):
    """
    Preprocess, match, and merge tennis betting data from Toto and Kambi for "Over/Under" events.

    Args:
    toto_filtered_tennis (pd.DataFrame): Filtered Toto tennis DataFrame.
    kambi_filtered_tennis (pd.DataFrame): Filtered Kambi tennis DataFrame.

    Returns:
    pd.DataFrame: Merged DataFrame with matched "Ja/Nee" events and filtered conditions.
    """
    kambi_filtered_tennis_overunder = kambi_filtered_tennis[kambi_filtered_tennis['bet_offer_type_name'].str.contains('Over')]
    toto_filtered_tennis_overunder = toto_filtered_tennis[
        (toto_filtered_tennis['Outcome Name'].str.contains('Over')) | 
        (toto_filtered_tennis['Outcome Name'].str.contains('Under'))
    ]

    # Remove accents and replace '-' with spaces in the necessary columns
    for column in ['event_name', 'criterion_label', 'criterion_english_label']:
        kambi_filtered_tennis_overunder[column] = kambi_filtered_tennis_overunder[column].apply(
            lambda x: ''.join(
                char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
                if not unicodedata.combining(char)
            )
        )
    
    for column in ['Event Name', 'Market Name']:
        toto_filtered_tennis_overunder[column] = toto_filtered_tennis_overunder[column].apply(
            lambda x: ''.join(
                char for char in unicodedata.normalize('NFKD', x.replace('-', ' '))
                if not unicodedata.combining(char)
            )
        )

    # Get event names for matching
    kambi_events = kambi_filtered_tennis_overunder['event_name'].tolist()

    # Function to find the best match
    def find_best_match(event_name):
        result = process.extractOne(event_name, kambi_events, scorer=fuzz.token_set_ratio, score_cutoff=90)
        if result is None:
            return None
        match, _, _ = result
        return match

    # Apply matching
    toto_filtered_tennis_overunder['matched_event'] = toto_filtered_tennis_overunder['Event Name'].apply(find_best_match)

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

    kambi_filtered_tennis_overunder['OverUnderType'] = kambi_filtered_tennis_overunder['criterion_label'].apply(determine_over_under_type)
    toto_filtered_tennis_overunder['OverUnderType'] = toto_filtered_tennis_overunder['Market Name'].apply(determine_over_under_type)

    # Define OverUnderTime
    def determine_over_under_time(label):
        if 'set 1' in label.lower():
            return 'Set 1'
        elif 'set 2' in label.lower():
            return 'Set 2'
        elif 'set 3' in label.lower():
            return 'Set 3'
        elif 'set 4' in label.lower():
            return 'Set 4'
        elif 'set 5' in label.lower():
            return 'Set 5'
        else:
            return 'Full Time'

    kambi_filtered_tennis_overunder['OverUnderTime'] = kambi_filtered_tennis_overunder['criterion_label'].apply(determine_over_under_time)
    toto_filtered_tennis_overunder['OverUnderTime'] = toto_filtered_tennis_overunder['Market Name'].apply(determine_over_under_time)

    # Split Team1 and Team2
    kambi_filtered_tennis_overunder[['Team1', 'Team2']] = kambi_filtered_tennis_overunder['event_name'].str.split(' vs ', expand=True)
    toto_filtered_tennis_overunder[['Team1', 'Team2']] = toto_filtered_tennis_overunder['Event Name'].str.split(' vs ', expand=True)

    # Define OverUnderType2
    kambi_filtered_tennis_overunder['OverUnderType2'] = kambi_filtered_tennis_overunder.apply(
        lambda row: '1' if row['Team1'] in row['criterion_english_label'] else (
            '2' if row['Team2'] in row['criterion_english_label'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    toto_filtered_tennis_overunder['OverUnderType2'] = toto_filtered_tennis_overunder.apply(
        lambda row: '1' if row['Team1'] in row['Market Name'] else (
            '2' if row['Team2'] in row['Market Name'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    # Extract line value
    toto_filtered_tennis_overunder['line'] = toto_filtered_tennis_overunder['Market Name'].apply(
        lambda x: float(x.split('Over/Under ')[-1]) if 'Over/Under ' in x else None
    )

    # Merge the DataFrames
    merged_tennis_overunder = pd.merge(
        toto_filtered_tennis_overunder,
        kambi_filtered_tennis_overunder,
        left_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'matched_event', 'start_time'],
        right_on=['line', 'OverUnderType', 'OverUnderTime', 'OverUnderType2', 'event_name', 'start_time'],
        how='inner'
    )

    # Keep only records with opposite outcomes
    merged_tennis_overunder = merged_tennis_overunder[
        merged_tennis_overunder['outcome_english_label'] != merged_tennis_overunder['Outcome Name']
    ]

    return merged_tennis_overunder

def create_merged_tennis_yesno(toto_filtered_tennis: pd.DataFrame, kambi_filtered_tennis: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess, match, and merge tennis betting data from Toto and Kambi for "Ja/Nee" events.

    Args:
    toto_filtered_tennis (pd.DataFrame): Filtered Toto tennis DataFrame.
    kambi_filtered_tennis (pd.DataFrame): Filtered Kambi tennis DataFrame.

    Returns:
    pd.DataFrame: Merged DataFrame with matched "Ja/Nee" events and filtered conditions.
    """
    # Filter Kambi data for "Ja/Nee" events
    kambi_filtered_tennis_yesno = kambi_filtered_tennis[
        kambi_filtered_tennis['bet_offer_type_name'].str.contains('Ja/Nee', na=False)
    ]

    # Filter Toto data for "Ja" or "Nee" events
    toto_filtered_tennis_yesno = toto_filtered_tennis[
        (toto_filtered_tennis['Outcome Name'].str.contains('Ja', na=False)) |
        (toto_filtered_tennis['Outcome Name'].str.contains('Nee', na=False))
    ]

    # Preprocess strings: Replace '-' with spaces and remove accents
    def preprocess_text(text):
        return ''.join(
            char for char in unicodedata.normalize('NFKD', text.replace('-', ' '))
            if not unicodedata.combining(char)
        )

    # Apply preprocessing to relevant columns
    kambi_filtered_tennis_yesno['event_name'] = kambi_filtered_tennis_yesno['event_name'].apply(preprocess_text)
    kambi_filtered_tennis_yesno['criterion_label'] = kambi_filtered_tennis_yesno['criterion_label'].apply(preprocess_text)
    kambi_filtered_tennis_yesno['criterion_english_label'] = kambi_filtered_tennis_yesno['criterion_english_label'].apply(preprocess_text)
    toto_filtered_tennis_yesno['Event Name'] = toto_filtered_tennis_yesno['Event Name'].apply(preprocess_text)
    toto_filtered_tennis_yesno['Market Name'] = toto_filtered_tennis_yesno['Market Name'].apply(preprocess_text)

    # Create a list of Kambi event names
    kambi_events = kambi_filtered_tennis_yesno['event_name'].tolist()

    # Function to find the best match for an event name
    def find_best_match(event_name):
        result = process.extractOne(event_name, kambi_events, scorer=fuzz.token_set_ratio, score_cutoff=90)
        if result is None:
            return None
        match, score, _ = result
        return match

    # Apply matching function to Toto data
    toto_filtered_tennis_yesno['matched_event'] = toto_filtered_tennis_yesno['Event Name'].apply(find_best_match)

    # Create 'YesNoType' column
    kambi_filtered_tennis_yesno['YesNoType'] = kambi_filtered_tennis_yesno['criterion_label'].apply(
    lambda x: 'Set winst' if ('wint minstens een set' in x or 'Wint een Set' in x)
    else None  # Default case if none of the conditions match
    )

    # Create 'OverUnderTime' column
    def determine_over_under_time(label):
        if 'set 1' in label.lower():
            return 'Set 1'
        elif 'set 2' in label.lower():
            return 'Set 2'
        elif 'set 3' in label.lower():
            return 'Set 3'
        elif 'set 4' in label.lower():
            return 'Set 4'
        elif 'set 5' in label.lower():
            return 'Set 5'
        else:
            return 'Full Time'

    kambi_filtered_tennis_yesno['OverUnderTime'] = kambi_filtered_tennis_yesno['criterion_label'].apply(determine_over_under_time)

    # Split event names into 'Team1' and 'Team2'
    kambi_filtered_tennis_yesno[['Team1', 'Team2']] = kambi_filtered_tennis_yesno['event_name'].str.split(' vs ', expand=True)

    # Create 'YesNoType2' column
    kambi_filtered_tennis_yesno['YesNoType2'] = kambi_filtered_tennis_yesno.apply(
        lambda row: '1' if row['Team1'] in row['criterion_english_label'] else (
            '2' if row['Team2'] in row['criterion_english_label'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    # Repeat the same for Toto data
    toto_filtered_tennis_yesno['YesNoType'] = toto_filtered_tennis_yesno['Market Name'].apply(
        lambda x: 'Set winst' if 'Wint een Set' in x else None
    )
    toto_filtered_tennis_yesno['OverUnderTime'] = toto_filtered_tennis_yesno['Market Name'].apply(determine_over_under_time)
    toto_filtered_tennis_yesno[['Team1', 'Team2']] = toto_filtered_tennis_yesno['Event Name'].str.split(' vs ', expand=True)
    toto_filtered_tennis_yesno['YesNoType2'] = toto_filtered_tennis_yesno.apply(
        lambda row: '1' if row['Team1'] in row['Market Name'] else (
            '2' if row['Team2'] in row['Market Name'] else 'Total team 1 and team 2'
        ),
        axis=1
    )

    # Merge the DataFrames
    merged_tennis_yesno = pd.merge(
        toto_filtered_tennis_yesno,
        kambi_filtered_tennis_yesno,
        left_on=['YesNoType', 'OverUnderTime', 'YesNoType2', 'matched_event', 'start_time'],
        right_on=['YesNoType', 'OverUnderTime', 'YesNoType2', 'event_name', 'start_time'],
        how='inner'
    )

    # Keep only records with opposite outcomes
    merged_tennis_yesno = merged_tennis_yesno[
        merged_tennis_yesno['outcome_english_label'] != merged_tennis_yesno['Outcome Name']
    ]

    return merged_tennis_yesno


def process_tennis_betting_data(toto_filtered_tennis, kambi_filtered_tennis):
    # Call the specific functions to process different bet types
    merged_df_winnaar = create_merged_df_winnaar(toto_filtered_tennis, kambi_filtered_tennis)
    merged_tennis_overunder = create_merged_tennis_overunder(kambi_filtered_tennis, toto_filtered_tennis)
    merged_tennis_yesno = create_merged_tennis_yesno(toto_filtered_tennis, kambi_filtered_tennis)

    # Perform the stacked union
    total_tennis = pd.concat([merged_tennis_overunder, merged_df_winnaar, merged_tennis_yesno], ignore_index=True, sort=True)

    # Calculate Arbitrage Percentage
    total_tennis['Arbitrage Percentage'] = (1 / total_tennis['Odds (Decimal)'] + 1 / total_tennis['odds']) * 100

    # Identify Arbitrage Opportunities
    total_tennis['Is Arbitrage'] = total_tennis['Arbitrage Percentage'] < 100

    # Calculate Optimal Stakes if Arbitrage
    total_stake = 1000  # Define the total stake for arbitrage
    total_tennis['Stake A'] = total_tennis.apply(
        lambda row: (1 / row['Odds (Decimal)'] / (1 / row['Odds (Decimal)'] + 1 / row['odds']) * total_stake) 
        if row['Is Arbitrage'] else 0,
        axis=1
    )
    total_tennis['Stake B'] = total_tennis.apply(
        lambda row: (1 / row['odds'] / (1 / row['Odds (Decimal)'] + 1 / row['odds']) * total_stake) 
        if row['Is Arbitrage'] else 0,
        axis=1
    )

    # Select and return the relevant columns
    result = total_tennis[[
        'Event Name', 'Market Name', 'Outcome Name', 'outcome_label', 
        'Odds (Decimal)', 'odds', 'Arbitrage Percentage', 
        'Is Arbitrage', 'Stake A', 'Stake B'
    ]]
    
    return result

toto_directory = "Data/scrapers/Toto/"
kambi_directory = "Data/scrapers/unibet/"
start_time = datetime.utcnow()

toto_file_path = get_latest_file(toto_directory)
kambi_file_path = get_latest_file(kambi_directory)

print(f"Latest Toto file Tennis: {toto_file_path}")
print(f"Latest Kambi file Tennis: {kambi_file_path}")
start_time = datetime.utcnow()

toto_filtered_tennis, kambi_filtered_tennis = preprocess_tennis_data(toto_file_path, kambi_file_path)

# # Now, `merged_df_winnaar` contains the processed and filtered merged data
# merged_df_winnaar = create_merged_df_winnaar(toto_filtered_tennis, kambi_filtered_tennis)
# # Now, `merged_tennis_overunder` contains the processed and filtered merged data
# merged_tennis_overunder = create_merged_tennis_overunder(kambi_filtered_tennis, toto_filtered_tennis)
# # Now, `merged_tennis_yesno` contains the processed and filtered merged data
# merged_tennis_yesno = create_merged_tennis_yesno(toto_filtered_tennis, kambi_filtered_tennis)

# Perform the stacked union
total_tennis_results = process_tennis_betting_data(toto_filtered_tennis, kambi_filtered_tennis)
total_tennis_results.to_csv(f'test_total_merge_Tennis_{start_time}.csv')

# Check if latest output file contains Arbitrage opportunities
try:
    arbitrage_found = False
    arbitrage_messages = []
    
    if total_tennis_results["Is Arbitrage"].any():
        arbitrage_messages.append(f"Arbitrage opportunity found in Tennis: test_total_merge_Tennis_{start_time}.csv")
        arbitrage_found = True
        
    if arbitrage_found:
        print("\n".join(arbitrage_messages))
    else:
        print("Tennis: No arbitrage opportunities found.")
        
except Exception as e:
    print(f"Tennis: Error checking for arbitrage opportunities: {str(e)}")