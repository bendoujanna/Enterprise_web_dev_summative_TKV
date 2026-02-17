from collections import Counter

with open('output/suspicious_records.log', 'r') as f:
    # Count how many times each error prefix appears
    counts = Counter(line.split(',')[0] for line in f)

print(f"Negative Fares: {counts['Negative fare']}")
print(f"Unknown Zones: {counts['Unknown zone']}")
print(f"Time Reversals: {counts['Time reversal']}")
print(f"Extreme Speeds: {counts['Extreme speed']}")