import os
import subprocess
from threading import Thread

# Paths to the Python scripts
BASE_DIR = "/Users/ddeboe01/Downloads/ArbitrageBets"
UNIBET_PY = os.path.join(BASE_DIR, "Data/scrapers/unibet/unibetAllSport.py")
TOTO_PY = os.path.join(BASE_DIR, "Data/scrapers/Toto/totoAllSport.py")
ARBSIGNAL_PY = os.path.join(BASE_DIR, "ArbSignal_Football.py")


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

    # Run the final script
    run_py(ARBSIGNAL_PY)


if __name__ == "__main__":
    main()