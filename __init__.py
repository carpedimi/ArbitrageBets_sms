import os
import subprocess
from threading import Thread

# Paths to the notebooks
BASE_DIR = "/Users/ddeboe01/Downloads/ArbitrageBets"
UNIBET_NOTEBOOK = os.path.join(BASE_DIR, "Data/scrapers/unibet/unibetAllSport.ipynb")
TOTO_NOTEBOOK = os.path.join(BASE_DIR, "Data/scrapers/Toto/totoAllSport.ipynb")
ARBSIGNAL_NOTEBOOK = os.path.join(BASE_DIR, "ArbSignal_Football_dev.ipynb")

JUPYTER_PATH = "/Users/ddeboe01/Library/Python/3.11/bin/jupyter"

def run_notebook(notebook_path):
    subprocess.run(
        [JUPYTER_PATH, 'nbconvert', '--to', 'notebook', '--execute', notebook_path],
        check=True
    )

def main():
    os.chdir(BASE_DIR)  # Set the working directory

    # Threads for parallel execution of the first two notebooks
    unibet_thread = Thread(target=run_notebook, args=(UNIBET_NOTEBOOK,))
    toto_thread = Thread(target=run_notebook, args=(TOTO_NOTEBOOK,))

    # Start the threads
    unibet_thread.start()
    toto_thread.start()

    # Wait for both threads to finish
    unibet_thread.join()
    toto_thread.join()

    # Run the final notebook
    run_notebook(ARBSIGNAL_NOTEBOOK)

if __name__ == "__main__":
    main()