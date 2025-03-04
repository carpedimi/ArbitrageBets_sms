import os
import subprocess
from threading import Thread
from flask import Flask
import ArbSignal_Football
import threading
import logging

# Paths to the Python scripts
BASE_DIR = "/Users/ddeboe01/Downloads/ArbitrageBets"
UNIBET_PY = os.path.join(BASE_DIR, "Data/scrapers/unibet/unibetAllSport.py")
TOTO_PY = os.path.join(BASE_DIR, "Data/scrapers/Toto/totoAllSport.py")
ARBSIGNAL_PY_FOOTBALL = os.path.join(BASE_DIR, "ArbSignal_Football.py")
ARBSIGNAL_PY_TENNIS = os.path.join(BASE_DIR, "ArbSignal_Tennis.py")

import os

def get_latest_file(sport: str, file_extension: str = "*.csv", directory: str = None) -> str:
    """
    Get the latest file in a directory based on the modification time.

    Args:
        sport (str): Sport name to filter files.
        file_extension (str): The file extension filter (default is "*.csv").
        directory (str): The directory path to search for files (default is current working directory).

    Returns:
        str: The path to the latest file.
    """
    if directory is None:
        directory = os.getcwd()
        
    files = [os.path.join(directory, file) 
            for file in os.listdir(directory) 
            if file.endswith(file_extension.split('.')[-1]) and sport in file]
    
    if not files:
        raise FileNotFoundError(f"No files found in {directory} with extension {file_extension} with {sport} in the name")
    
    latest_file = max(files, key=os.path.getmtime)
    return latest_file


def run_py(script_path):
    """
    Executes a Python script using subprocess.
    """
    subprocess.run(['python3', script_path], check=True)


def main():
    os.chdir(BASE_DIR)  # Set the working directory

    # Threads for parallel execution of the first two scripts
    unibet_thread = Thread(target=run_py, args=(UNIBET_PY,))
    toto_thread = Thread(target=run_py, args=(TOTO_PY,))

    # Start the threads
    unibet_thread.start()
    toto_thread.start()

    # Wait for both threads to finish
    unibet_thread.join()
    toto_thread.join()

    # Threads for parallel execution of the first two scripts
    arbsignal_football_thread = Thread(target=run_py, args=(ARBSIGNAL_PY_FOOTBALL,))
    arbsignal_tennis_thread = Thread(target=run_py, args=(ARBSIGNAL_PY_TENNIS,))

    # Start the threads
    arbsignal_football_thread.start()
    arbsignal_tennis_thread.start()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_arbitrage_detection():
    """Background task to run arbitrage detection"""
    while True:
        try:
            logging.info("Starting arbitrage detection cycle")
            ArbSignal_Football.main()
            # Sleep for 5 minutes before next check
            threading.Event().wait(300)
        except Exception as e:
            logging.error(f"Error in arbitrage detection: {e}")
            # Sleep for 1 minute before retry on error
            threading.Event().wait(60)

@app.route('/')
def home():
    return 'Arbitrage Detection Service is running'

@app.route('/health')
def health():
    return 'OK'

if __name__ == '__main__':
    # Start the arbitrage detection in a background thread
    arbitrage_thread = threading.Thread(target=run_arbitrage_detection, daemon=True)
    arbitrage_thread.start()
    
    # Start the Flask server
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)