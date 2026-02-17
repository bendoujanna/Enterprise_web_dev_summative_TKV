import geopandas as gpd
import os

# --- CONFIGURATION ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SHP_FILE = os.path.join(PROJECT_ROOT, 'data', 'taxi_zones', 'taxi_zones.shp')
OUTPUT_FILE = os.path.join(PROJECT_ROOT, 'output', 'taxi_zones.json')


