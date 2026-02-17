import sqlite3
import os

# cONFIGURATION

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'output')
# Temporary Safe Path
DB_PATH = os.path.expanduser("database.db")


def create_schema():
    # create the output folder if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created output directory at {OUTPUT_DIR}")

    print(f"Initializing database at {DB_PATH}...")

    # connect to the db
    # URI=True allows us to pass special parameters
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # enable foreigh keys
    cursor.execute("PRAGMA foreign_keys = ON;")
