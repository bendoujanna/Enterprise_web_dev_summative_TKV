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

    # TABLES

    # 1. vendors

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vendors (
           VendorID INTEGER PRIMARY KEY,
           vendor_name TEXT NOT NULL
        );
    """)

    # 2. Zones

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS zones (
            LocationID INTEGER PRIMARY KEY,
            Borough TEXT,
            Zone TEXT,
            service_zone TEXT       
        );
    """)

    # 3. TRIPS
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            trip_id INTEGER PRIMARY KEY AUTOINCREMENT,
            VendorID INTEGER,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            tpep_pickup_datetime TEXT,
            tpep_dropoff_datetime TEXT,
            passenger_count INTEGER,
            trip_distance DECIMAL(10, 2),
            RatecodeID INTEGER,
            store_and_fwd_flag TEXT,
            payment_type INTEGER,
            fare_amount DECIMAL(10, 2),
            extra DECIMAL(10,2),
            mta_tax DECIMAL(10,2),
            tip_amount DECIMAL(10, 2),
            tolls_amount DECIMAL(10,2),
            improvement_surcharge DECIMAL(10,2),
            total_amount DECIMAL(10, 2),
            congestion_surcharge DECIMAL(10,2),
            trip_duration_seconds INTEGER,
            average_speed_mph DECIMAL(10,2),
            time_of_day TEXT,
            FOREIGN KEY (VendorID) REFERENCES vendors(VendorID),
            FOREIGN KEY (PULocationID) REFERENCES zones(LocationID),
            FOREIGN KEY (DOLocationID) REFERENCES zones(LocationID)
        );
    """)
 # creating indexes to speed up queries
    print("Creating indexes...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pickup ON trips(PULocationID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_dropoff ON trips(DOLocationID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON trips(tpep_pickup_datetime);")
 #  Inserting static data into vendors table
    print("Seeding vendors...")
    vendors = [
        (1, 'Creative Mobile Technologies, LLC'),
        (2, 'Curb Mobility, LLC')
    ]
    cursor.executemany("INSERT OR IGNORE INTO vendors (VendorID, vendor_name) VALUES (?, ?)", vendors)
