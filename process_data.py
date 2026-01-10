#!/usr/bin/env python3
"""
Process Census county population data to calculate under-5 population changes
for major cities (counties with population >= 250,000).
"""

import pandas as pd
import json

# Load the data
df = pd.read_csv('/Users/connorobrien/Downloads/cc-est2024-agesex-all.csv', encoding='latin-1')

# YEAR key:
# 1 = 4/1/2020 population estimates base
# 6 = 7/1/2024 population estimate

# Get baseline (April 2020) and latest (July 2024) data
baseline = df[df['YEAR'] == 1].copy()
latest = df[df['YEAR'] == 6].copy()

# Create FIPS code (state + county, zero-padded)
baseline['FIPS'] = baseline['STATE'].astype(str).str.zfill(2) + baseline['COUNTY'].astype(str).str.zfill(3)
latest['FIPS'] = latest['STATE'].astype(str).str.zfill(2) + latest['COUNTY'].astype(str).str.zfill(3)

# Filter for major cities: counties with population >= 250,000 in April 2020
POPULATION_THRESHOLD = 250000
major_counties_baseline = baseline[baseline['POPESTIMATE'] >= POPULATION_THRESHOLD].copy()

print(f"Total counties in data: {len(baseline)}")
print(f"Major counties (pop >= {POPULATION_THRESHOLD:,}): {len(major_counties_baseline)}")

# Merge baseline and latest for major counties
major_fips = set(major_counties_baseline['FIPS'].values)
major_latest = latest[latest['FIPS'].isin(major_fips)].copy()

# Merge the data
merged = major_counties_baseline[['FIPS', 'STNAME', 'CTYNAME', 'POPESTIMATE', 'UNDER5_TOT']].merge(
    major_latest[['FIPS', 'POPESTIMATE', 'UNDER5_TOT']],
    on='FIPS',
    suffixes=('_2020', '_2024')
)

# Calculate changes
merged['under5_absolute_change'] = merged['UNDER5_TOT_2024'] - merged['UNDER5_TOT_2020']
merged['under5_pct_change'] = ((merged['UNDER5_TOT_2024'] - merged['UNDER5_TOT_2020']) / merged['UNDER5_TOT_2020'] * 100).round(2)
merged['total_pop_change'] = merged['POPESTIMATE_2024'] - merged['POPESTIMATE_2020']
merged['total_pop_pct_change'] = ((merged['POPESTIMATE_2024'] - merged['POPESTIMATE_2020']) / merged['POPESTIMATE_2020'] * 100).round(2)

# Rename columns for clarity
merged = merged.rename(columns={
    'STNAME': 'state',
    'CTYNAME': 'county',
    'POPESTIMATE_2020': 'pop_2020',
    'POPESTIMATE_2024': 'pop_2024',
    'UNDER5_TOT_2020': 'under5_2020',
    'UNDER5_TOT_2024': 'under5_2024'
})

# Sort by absolute change (most negative first)
merged = merged.sort_values('under5_absolute_change')

# Display summary statistics
print("\n" + "="*80)
print("UNDER-5 POPULATION CHANGES IN MAJOR U.S. COUNTIES (April 2020 - July 2024)")
print("="*80)
print(f"\nDefinition: Major counties = population >= {POPULATION_THRESHOLD:,} in April 2020")
print(f"Number of major counties: {len(merged)}")

# Summary stats
total_under5_2020 = merged['under5_2020'].sum()
total_under5_2024 = merged['under5_2024'].sum()
total_change = total_under5_2024 - total_under5_2020
pct_change = (total_change / total_under5_2020) * 100

print(f"\nAggregate statistics for major counties:")
print(f"  Under-5 population in April 2020: {total_under5_2020:,}")
print(f"  Under-5 population in July 2024:  {total_under5_2024:,}")
print(f"  Absolute change: {total_change:,}")
print(f"  Percent change: {pct_change:.1f}%")

# Counties with biggest declines
print("\n" + "-"*80)
print("TOP 20 COUNTIES WITH LARGEST ABSOLUTE DECLINE IN UNDER-5 POPULATION:")
print("-"*80)
for i, row in merged.head(20).iterrows():
    print(f"  {row['county']}, {row['state']}: {row['under5_absolute_change']:,} ({row['under5_pct_change']:+.1f}%)")

# Counties with biggest percentage declines
print("\n" + "-"*80)
print("TOP 20 COUNTIES WITH LARGEST PERCENTAGE DECLINE IN UNDER-5 POPULATION:")
print("-"*80)
pct_sorted = merged.sort_values('under5_pct_change')
for i, row in pct_sorted.head(20).iterrows():
    print(f"  {row['county']}, {row['state']}: {row['under5_pct_change']:+.1f}% ({row['under5_absolute_change']:,})")

# Counties that GAINED under-5 population
gainers = merged[merged['under5_absolute_change'] > 0].sort_values('under5_absolute_change', ascending=False)
print("\n" + "-"*80)
print(f"COUNTIES THAT GAINED UNDER-5 POPULATION ({len(gainers)} counties):")
print("-"*80)
for i, row in gainers.head(20).iterrows():
    print(f"  {row['county']}, {row['state']}: +{row['under5_absolute_change']:,} ({row['under5_pct_change']:+.1f}%)")

# Export to JSON for map
output_data = merged.to_dict(orient='records')

with open('/Users/connorobrien/cities-for-families/data/county_changes.json', 'w') as f:
    json.dump(output_data, f, indent=2)

print(f"\n\nData exported to: /Users/connorobrien/cities-for-families/data/county_changes.json")

# Also create a summary CSV
merged.to_csv('/Users/connorobrien/cities-for-families/data/county_changes.csv', index=False)
print(f"Data exported to: /Users/connorobrien/cities-for-families/data/county_changes.csv")
