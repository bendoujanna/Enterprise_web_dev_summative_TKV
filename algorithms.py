"""
Algorithms for sorting and grouping trip data without using SQL.
"""


def my_sort_trips(trip_list, sort_by_field):
    """
Sort trips from lowest to highest based on a specified field.
    """
    
    # Make a copy so we don't mess up the original list
    trips = trip_list.copy()
    
    # Get the length
    n = len(trips)
    
    # Go through the list many times
    for i in range(n):
        # Flag to check if we made any swaps
        swapped = False
        
        # Compare each pair of items
        for j in range(0, n - i - 1):
            # Get the values we want to compare
            current_value = trips[j][sort_by_field]
            next_value = trips[j + 1][sort_by_field]
            
            # If current is bigger than next, swap them
            if current_value > next_value:
                # Do the swap
                temp = trips[j]
                trips[j] = trips[j + 1]
                trips[j + 1] = temp
                swapped = True
        
        # If we didn't swap anything, list is already sorted
        if not swapped:
            break
    
    return trips


def sort_trips_descending(trip_list, sort_by_field):
    """
    Sort trips from highest to lowest.
    Just calls my_sort_trips and then reverses the result.
    """
    sorted_list = my_sort_trips(trip_list, sort_by_field)
    
    # Reverse it manually
    reversed_list = []
    for i in range(len(sorted_list) - 1, -1, -1):
        reversed_list.append(sorted_list[i])
    
    return reversed_list


def group_by_borough(trip_list):
    """
Group trips by borough and count how many trips are in each one.
    """
    
    # Make an empty dictionary to store counts
    borough_counts = {}
    
    # Go through each trip
    for trip in trip_list:
        borough = trip['borough']
        
        # If we haven't seen this borough before, add it
        if borough not in borough_counts:
            borough_counts[borough] = 0
        
        # Add 1 to the count
        borough_counts[borough] = borough_counts[borough] + 1
    
    return borough_counts


def calculate_average_by_group(trip_list, group_field, value_field):
    """
Calculate the average of a value field for each group defined by group_field.
    """
    
    # Dictionary to store sum and count for each group
    groups = {}
    
    # First pass - collect all values
    for trip in trip_list:
        group = trip[group_field]
        value = trip[value_field]
        
        if group not in groups:
            groups[group] = {'sum': 0, 'count': 0}
        
        groups[group]['sum'] = groups[group]['sum'] + value
        groups[group]['count'] = groups[group]['count'] + 1
    
    # Second pass - calculate averages
    averages = {}
    for group in groups:
        total = groups[group]['sum']
        count = groups[group]['count']
        averages[group] = total / count
    
    return averages


def find_top_n(trip_list, field, n):
    """
    Find the top N trips based on a field.
    """
    
    # Sort the list first
    sorted_trips = sort_trips_descending(trip_list, field)
    
    # Return only the first N items
    top_trips = []
    for i in range(n):
        if i < len(sorted_trips):
            top_trips.append(sorted_trips[i])
    
    return top_trips