from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3
import os

# Import our custom sorting functions
from algorithms import my_sort_trips, sort_trips_descending, group_by_borough, calculate_average_by_group, find_top_n

app = Flask(__name__)
CORS(app)

# Configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'database.db')


def get_db_connection():
    """Establish a connection to the sqlite database with row mapping"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# utilities and metadata
@app.route('/api/health', methods=['GET'])
def health_check():
    """Returns the API and Database status"""
    return jsonify({
        "status": "online",
        "database_found": os.path.exists(DB_PATH)
    })


@app.route('/api/zones', methods=['GET'])
def get_zones():
    """Provides spatial metadata mapping LocationIDs to Borough/Zone names"""
    conn = get_db_connection()
    zones = conn.execute("SELECT LocationID, Borough, Zone FROM zones").fetchall()
    conn.close()
    # Returns a dictionary for O(1) lookup on the frontend
    return jsonify({row['LocationID']: {"Borough": row['Borough'], "Zone": row['Zone']} for row in zones})

#

@app.route('/api/stats/summary', methods=['GET'])
def get_summary():
    """KPIs for the dashboard header"""
    conn = get_db_connection()
    # Pulling total count and average revenue
    stats = conn.execute("""
                         SELECT COUNT(*)                    as total_trips,
                                ROUND(AVG(total_amount), 2) as avg_fare
                         FROM trips
                         """).fetchone()
    conn.close()
    return jsonify(dict(stats))


@app.route('/api/stats/charts/boroughs', methods=['GET'])
def get_borough_distribution():
    """Returns trip counts per Borough for the Bar Chart"""
    conn = get_db_connection()
    query = """
            SELECT z.Borough, COUNT(*) as trip_count
            FROM trips t
                     JOIN zones z ON t.PULocationID = z.LocationID
            GROUP BY z.Borough
            ORDER BY trip_count DESC \
            """
    data = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])


@app.route('/api/stats/charts/efficiency', methods=['GET'])
def get_time_efficiency():
    """Returns average speed per time of day for the Line Chart"""
    conn = get_db_connection()
    query = """
            SELECT time_of_day, ROUND(AVG(average_speed_mph), 2) as avg_speed
            FROM trips
            GROUP BY time_of_day \
            """
    data = conn.execute(query).fetchall()
    conn.close()
    return jsonify([dict(row) for row in data])


# raw data
@app.route('/api/trips', methods=['GET'])
def get_trips():
    """Returns a list of trips with optional borough filtering and pagination"""
    limit = request.args.get('limit', 200, type=int)
    offset = request.args.get('offset', 0, type=int)
    borough = request.args.get('borough', None)

    conn = get_db_connection()
    query = """
            SELECT t.*, p.Borough as Pickup_Borough, d.Borough as Dropoff_Borough
            FROM trips t
                     JOIN zones p ON t.PULocationID = p.LocationID
                     JOIN zones d ON t.DOLocationID = d.LocationID \
            """
    params = []
    if borough:
        query += " WHERE p.Borough = ?"
        params.append(borough)

    query += " LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    trips = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(row) for row in trips])


@app.route('/api/analytics/summary', methods=['GET'])
def get_analytics_summary():
    conn = get_db_connection()
    try:
        # Calculate Revenue and Duration
        stats = conn.execute("""
            SELECT 
                SUM(total_amount) as total_rev,
                AVG(trip_distance / (NULLIF(average_speed_mph, 0) / 60.0)) as avg_dur
            FROM trips
        """).fetchone()

        # Peak Hours Analysis
        hourly_data = conn.execute("""
            SELECT strftime('%H', tpep_pickup_datetime) as hr, COUNT(*) as count 
            FROM trips GROUP BY hr ORDER BY hr ASC
        """).fetchall()

        return jsonify({
            "kpis": {
                "total_revenue": f"${round((stats['total_rev'] or 0) / 1000000, 1)}M",
                "avg_trip_duration": f"{round(stats['avg_dur'] or 0, 1)} min"
            },
            "chart_data": [{"hour": f"{row['hr']}:00", "trips": row['count']} for row in hourly_data]
        })
    finally:
        conn.close()


@app.route('/api/stats/quality', methods=['GET'])
def get_data_quality():
    conn = get_db_connection()
    try:
        valid_records = conn.execute("SELECT COUNT(*) FROM trips").fetchone()[0]

        rejected_records = 66498

        issues = [
            {"issue": "Time Reversal", "count": 53662, "status": "critical"},
            {"issue": "Negative Fare", "count": 7131, "status": "critical"},
            {"issue": "Extreme Speed (>100MPH)", "count": 5705, "status": "warning"},
            {"issue": "Unknown Zones", "count": 0, "status": "success"}
        ]

        # Calculate the real-time quality score
        total_attempted = valid_records + rejected_records
        quality_score = round((valid_records / total_attempted) * 100, 2) if total_attempted > 0 else 0

        return jsonify({
            "overall_score": f"{quality_score}%",
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "detailed_issues": issues,
            "last_updated": "Feb 15, 2026"
        })
    finally:
        conn.close()


# NEW ENDPOINTS USING CUSTOM ALGORITHMS

@app.route('/api/trips/custom-sort', methods=['GET'])
def get_custom_sorted_trips():
    """
    This endpoint uses our CUSTOM SORTING ALGORITHM instead of SQL ORDER BY.
    We implemented bubble sort from scratch to show we understand sorting algorithms.
    """
    
    # Get parameters from the request
    sort_by = request.args.get('sort_by', 'total_amount')
    limit = request.args.get('limit', 100, type=int)
    
    conn = get_db_connection()
    
    # Get trips WITHOUT sorting (no ORDER BY in SQL)
    # We'll sort them ourselves using our algorithm
    query = """
        SELECT trip_id, total_amount, trip_distance, 
               tpep_pickup_datetime, PULocationID, DOLocationID,
               average_speed_mph
        FROM trips
        LIMIT 1000
    """
    
    results = conn.execute(query).fetchall()
    conn.close()
    
    # Convert to list of dictionaries
    trips_list = []
    for row in results:
        trip_dict = {
            'trip_id': row['trip_id'],
            'total_amount': row['total_amount'],
            'trip_distance': row['trip_distance'],
            'pickup_time': row['tpep_pickup_datetime'],
            'pickup_location': row['PULocationID'],
            'dropoff_location': row['DOLocationID'],
            'speed': row['average_speed_mph']
        }
        trips_list.append(trip_dict)
    
    # Use OUR CUSTOM SORTING FUNCTION
    sorted_trips = sort_trips_descending(trips_list, sort_by)
    
    # Only return the number requested
    final_list = sorted_trips[:limit]
    
    return jsonify({
        "message": "Sorted using custom bubble sort algorithm (not SQL ORDER BY)",
        "algorithm": "Bubble Sort - O(n²) time complexity",
        "sorted_by": sort_by,
        "total_processed": len(trips_list),
        "returned": len(final_list),
        "data": final_list
    })


@app.route('/api/trips/top-expensive', methods=['GET'])
def get_top_expensive_trips():
    """
    Find the most expensive trips using our custom algorithm.
    """
    
    n = request.args.get('n', 10, type=int)
    
    conn = get_db_connection()
    
    # Get data without sorting
    trips = conn.execute("""
        SELECT trip_id, total_amount, trip_distance, tpep_pickup_datetime
        FROM trips
        LIMIT 5000
    """).fetchall()
    conn.close()
    
    # Convert to list
    trips_list = []
    for trip in trips:
        trips_list.append({
            'trip_id': trip['trip_id'],
            'total_amount': trip['total_amount'],
            'trip_distance': trip['trip_distance'],
            'pickup_time': trip['tpep_pickup_datetime']
        })
    
    # Use our custom function to find top N
    top_trips = find_top_n(trips_list, 'total_amount', n)
    
    return jsonify({
        "message": f"Top {n} most expensive trips",
        "algorithm_used": "Custom sorting + selection",
        "data": top_trips
    })


@app.route('/api/analytics/borough-custom', methods=['GET'])
def get_borough_stats_custom():
    """
    Calculate borough statistics using CUSTOM GROUPING (not SQL GROUP BY).
    This shows we can manually implement aggregation logic.
    """
    
    conn = get_db_connection()
    
    # Get raw data WITHOUT grouping in SQL
    query = """
        SELECT z.Borough, t.total_amount
        FROM trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        LIMIT 10000
    """
    
    results = conn.execute(query).fetchall()
    conn.close()
    
    # Convert to simple list
    trips_with_borough = []
    for row in results:
        trips_with_borough.append({
            'borough': row['Borough'],
            'total_amount': row['total_amount']
        })
    
    # Use OUR CUSTOM GROUPING FUNCTION
    averages = calculate_average_by_group(trips_with_borough, 'borough', 'total_amount')
    
    # Format the response
    result_list = []
    for borough in averages:
        result_list.append({
            'borough': borough,
            'average_fare': round(averages[borough], 2)
        })
    
    return jsonify({
        "message": "Borough averages calculated with custom algorithm",
        "algorithm": "Manual grouping and aggregation (not SQL GROUP BY)",
        "data": result_list
    })


if __name__ == '__main__':
    print(f"API Server running at http://127.0.0.1:5000")
    print("Custom sorting endpoints available:")
    print("  - /api/trips/custom-sort")
    print("  - /api/trips/top-expensive")
    print("  - /api/analytics/borough-custom")
    app.run(debug=True, port=5000)