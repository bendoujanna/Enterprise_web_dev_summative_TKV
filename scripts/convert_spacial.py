import geopandas as gpd
import os

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SHP_FILE = os.path.join(PROJECT_ROOT, 'data', 'taxi_zones', 'taxi_zones.shp')
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'output', 'taxi_zones.json')

def convert_shapefile():
    print("Starting Spatial Data Conversion...")

    # Check if input file exists
    if not os.path.exists(SHP_FILE):
        print(f"Error: Could not find Shapefile at {SHP_FILE}")
        print(" Make sure you moved the 'taxi_zones' folder into 'data'")
        return

    try:
        # Read the Shapefile
        print("   Reading Shapefile...")
        gdf = gpd.read_file(SHP_FILE)

        # Convert Coordinate Reference System (CRS)
        if gdf.crs and gdf.crs.to_string() != 'EPSG:4326':
            print("   Converting coordinates to Lat/Lon (EPSG:4326)...")
            gdf = gdf.to_crs("EPSG:4326")
