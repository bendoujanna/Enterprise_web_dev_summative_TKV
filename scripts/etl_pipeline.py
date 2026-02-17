import csv
import os
import sqlite3
from datetime import datetime
import pandas as pd

# --- cONFIGURATION ---
# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
# Temporary Safe Path
DB_PATH = os.path.expanduser("database.db")

# Input paths
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
TRIPS_FILE = os.path.join(DATA_DIR, 'yellow_tripdata_2019-01.parquet')
ZONE_FILE = os.path.join(DATA_DIR, 'taxi_zone_lookup.csv')
LOG_FILE = os.path.join(PROJECT_ROOT, 'output', 'suspicious_records.log')


# Helper functions
def calculate_features(pickup_str, dropoff_str, distance_miles):
    """Computes the duration, speed, and time category"""

    format = "%Y-%m-%d %H:%M:%S"
    try:
        # Handle both string and datetime objects
        if isinstance(pickup_str, str):
            t1 = datetime.strptime(pickup_str, format)
        else:
            t1 = pickup_str
            
        if isinstance(dropoff_str, str):
            t2 = datetime.strptime(dropoff_str, format)
        else:
            t2 = dropoff_str
            
        duration = (t2 - t1).total_seconds()

        # categorise time of day
        hour = t1.hour
        if 6 <= hour < 12:
            time_cat = 'Morning'
        elif 12 <= hour < 17:
            time_cat = 'Afternoon'
        elif 17 <= hour < 21:
            time_cat = 'Evening'
        else:
            time_cat = 'Night'

        # calculate speed (mph)
        if duration <= 0 or distance_miles <= 0:
            return None, None, None  # Invalid data

        hours = duration / 3600
        speed = distance_miles / hours

        return int(duration), round(speed, 2), time_cat

    except:
        return None, None, None  # In case of parsing errors


def run_pipeline():
    print("Starting ETL Pipeline...")

    # check if db exists
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        print("Make sure you ran init_db.py first")
        return

    # Load zones
    valid_zones = set()
    print(f"Loading zones from {ZONE_FILE}...")

    try:
        # URI=True allows us to pass special parameters
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        with open(ZONE_FILE, 'r') as f:
            reader = csv.DictReader(f)
            zone_rows = []
            for row in reader:
                lid = int(row['LocationID'])
                valid_zones.add(lid)
                zone_rows.append((lid, row['Borough'], row['Zone'], row['service_zone']))

            cursor.execute("DELETE FROM zones;")  # Clear existing data
            cursor.executemany("INSERT INTO zones VALUES (?, ?, ?, ?);", zone_rows)
            conn.commit()
            print(f"Loaded {len(zone_rows)} zones into the database")

    except Exception as e:
        print(f"Error loading zones: {e}")
        return

    # Process trips
    print("Processing trips... This may take a moment")
    print("Reading parquet file...")

    batch_data = []
    bad_count = 0
    total_count = 0

    try:
        # Read parquet file with pandas
        df = pd.read_parquet(TRIPS_FILE)
        print(f"Loaded {len(df)} rows from parquet file")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA journal_mode = MEMORY")

        with open(LOG_FILE, 'w') as log:
            log.write("Reason, RawData\n")  # CSV header for log file
            
            # Process each row
            for idx, row in df.iterrows():
                # if total_count >= 500000:
                #     print(f"\n Fast Mode limit reached (500,000 rows). Stopping.")
                #     break
                try:
                    # Extract basic fields
                    pul = int(row['PULocationID'])
                    dol = int(row['DOLocationID'])
                    fare = float(row['fare_amount'])
                    dist = float(row['trip_distance'])

                    # cleaning data
                    # 1. negative fare
                    if fare < 0:
                        log.write(f"Negative fare, {row.to_dict()}\n")
                        bad_count += 1
                        continue

                    # 2. unknown zones
                    if pul not in valid_zones or dol not in valid_zones:
                        log.write(f"Unknown zone, {row.to_dict()}\n")
                        bad_count += 1
                        continue

                    # 3. calculate features
                    # Convert datetime to string format
                    pickup_dt = row['tpep_pickup_datetime']
                    dropoff_dt = row['tpep_dropoff_datetime']
                    
                    # Format datetime for storage
                    pickup_str = pickup_dt.strftime('%Y-%m-%d %H:%M:%S')
                    dropoff_str = dropoff_dt.strftime('%Y-%m-%d %H:%M:%S')
                    
                    duration, speed, time_cat = calculate_features(
                        pickup_dt,
                        dropoff_dt,
                        dist
                    )

                    if duration is None:
                        log.write(f"Time reversal, {row.to_dict()}\n")
                        bad_count += 1
                        continue

                    if speed > 100:
                        log.write(f"Extreme speed, {row.to_dict()}\n")
                        bad_count += 1
                        continue

                    # prepare row for db
                    batch_data.append((
                        row['VendorID'], pickup_str, dropoff_str,
                        row['passenger_count'], dist, row['RatecodeID'], row['store_and_fwd_flag'],
                        pul, dol, row['payment_type'], fare, row['extra'], row['mta_tax'],
                        row['tip_amount'], row['tolls_amount'], row['improvement_surcharge'],
                        row['total_amount'], row.get('congestion_surcharge', 0),  # Default to 0 if missing
                        duration, speed, time_cat
                    ))

                    total_count += 1

                    # batch insert
                    if len(batch_data) >= 50000:
                        cursor.executemany("""
                                           INSERT INTO trips (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                                                              passenger_count,
                                                              trip_distance, RatecodeID, store_and_fwd_flag,
                                                              PULocationID, DOLocationID,
                                                              payment_type, fare_amount, extra, mta_tax, tip_amount,
                                                              tolls_amount,
                                                              improvement_surcharge, total_amount,
                                                              congestion_surcharge,
                                                              trip_duration_seconds, average_speed_mph, time_of_day)
                                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                           """, batch_data)

                        conn.commit()
                        batch_data = []  # Clear memory
                        print(f"Processed {total_count} rows...", end='\r')

                except Exception as e:
                    bad_count += 1
                    continue

        # Insert any remaining data
        if batch_data:
            cursor.executemany("""
                               INSERT INTO trips (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
                                                  passenger_count,
                                                  trip_distance, RatecodeID, store_and_fwd_flag, PULocationID,
                                                  DOLocationID,
                                                  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                                                  improvement_surcharge, total_amount, congestion_surcharge,
                                                  trip_duration_seconds, average_speed_mph, time_of_day)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                               """, batch_data)
        conn.commit()

        print(f"\nETL Completed!")
        print(f"    - Successful Trips: {total_count}")
        print(f"    - Rejected Records: {bad_count}")

    except FileNotFoundError:
        print(f"Error: Could not find data files")
        print(f"Looking for {TRIPS_FILE}")
        print("Make sure you downloaded the data and placed it in the 'data' folder")
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have pandas and pyarrow installed:")
        print("  pip install pandas pyarrow")
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline()