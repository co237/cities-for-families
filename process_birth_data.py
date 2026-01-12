#!/usr/bin/env python3
"""
Process birth rate data by EIG county typology for Cities for Families site.
Focus on large urban counties. Uses pre-computed RBIRTH columns.
"""

import pandas as pd
import json

print("="*80)
print("CITIES FOR FAMILIES - BIRTH RATE ANALYSIS")
print("Using EIG County Typology")
print("="*80)

# Load EIG county typology
print("\nLoading EIG county typology...")
county_types = pd.read_csv('/Users/connorobrien/Downloads/county_summary_final.csv', encoding='latin-1')
county_types['county_fips'] = county_types['county_fips'].astype(str).str.zfill(5)

print(f"Total counties: {len(county_types)}")
print("\nCounty types:")
for t, c in county_types['locale_type'].value_counts().items():
    print(f"  {t}: {c}")

# Load 2020-2024 data
print("\nLoading 2020-2024 components data...")
df_24 = pd.read_csv('/Users/connorobrien/Downloads/co-est2024-alldata.csv', encoding='latin-1')
df_24['COUNTY'] = df_24['COUNTY'].astype(str).str.zfill(3)
df_24['STATE'] = df_24['STATE'].astype(str).str.zfill(2)
df_24['FIPS'] = df_24['STATE'] + df_24['COUNTY']
df_24 = df_24[df_24['COUNTY'] != '000']
df_24 = df_24.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Load 2010-2020 data
print("Loading 2010-2020 components data...")
df_10 = pd.read_csv('/Users/connorobrien/Downloads/co-est2020-alldata.csv', encoding='latin-1')
df_10['COUNTY'] = df_10['COUNTY'].astype(str).str.zfill(3)
df_10['STATE'] = df_10['STATE'].astype(str).str.zfill(2)
df_10['FIPS'] = df_10['STATE'] + df_10['COUNTY']
df_10 = df_10[df_10['COUNTY'] != '000']
df_10 = df_10.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Calculate population-weighted birth rates by county type
print("\nCalculating population-weighted birth rates by county type...")

locale_order = ['Large urban', 'Mid-sized urban', 'Small urban', 'Suburban', 'Small town', 'Rural']
birth_rate_ts = []

for locale in locale_order:
    # 2011-2020 from old dataset
    subset_10 = df_10[df_10['locale_type'] == locale]
    for year in range(2011, 2021):
        pop_col = f'POPESTIMATE{year}'
        rate_col = f'RBIRTH{year}'
        if rate_col in subset_10.columns and pop_col in subset_10.columns:
            weighted_rate = (subset_10[rate_col] * subset_10[pop_col]).sum() / subset_10[pop_col].sum()
            birth_rate_ts.append({
                'locale_type': locale,
                'year': year,
                'birth_rate': round(weighted_rate, 2)
            })

    # 2021-2024 from new dataset
    subset_24 = df_24[df_24['locale_type'] == locale]
    for year in range(2021, 2025):
        pop_col = f'POPESTIMATE{year}'
        rate_col = f'RBIRTH{year}'
        if rate_col in subset_24.columns and pop_col in subset_24.columns:
            weighted_rate = (subset_24[rate_col] * subset_24[pop_col]).sum() / subset_24[pop_col].sum()
            birth_rate_ts.append({
                'locale_type': locale,
                'year': year,
                'birth_rate': round(weighted_rate, 2)
            })

ts_df = pd.DataFrame(birth_rate_ts)

# Calculate change 2011-2024
print("\nBirth rate changes 2011-2024 by county type:")
print("-" * 60)
change_data = []
for locale in locale_order:
    locale_df = ts_df[ts_df['locale_type'] == locale]
    rate_2011 = locale_df[locale_df['year'] == 2011]['birth_rate'].values[0]
    rate_2024 = locale_df[locale_df['year'] == 2024]['birth_rate'].values[0]
    change = rate_2024 - rate_2011
    pct_change = (change / rate_2011) * 100
    print(f"  {locale}: {rate_2011:.1f} -> {rate_2024:.1f} ({pct_change:+.1f}%)")
    change_data.append({
        'locale_type': locale,
        'rate_2011': rate_2011,
        'rate_2024': rate_2024,
        'change': round(change, 2),
        'pct_change': round(pct_change, 1)
    })

# Load under-5 population data
print("\nLoading under-5 population data...")
pop_data = pd.read_csv('/Users/connorobrien/Downloads/cc-est2024-agesex-all.csv', encoding='latin-1')
pop_data['COUNTY'] = pop_data['COUNTY'].astype(str).str.zfill(3)
pop_data['STATE'] = pop_data['STATE'].astype(str).str.zfill(2)
pop_data['FIPS'] = pop_data['STATE'] + pop_data['COUNTY']
pop_data = pop_data.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Under-5 change by county type
print("\nUnder-5 population changes by county type (April 2020 - July 2024):")
print("-" * 60)
under5_data = []
for locale in locale_order:
    subset = pop_data[pop_data['locale_type'] == locale]
    u5_2020 = subset[subset['YEAR'] == 1]['UNDER5_TOT'].sum()
    u5_2024 = subset[subset['YEAR'] == 6]['UNDER5_TOT'].sum()
    change = u5_2024 - u5_2020
    pct_change = (change / u5_2020) * 100
    print(f"  {locale}: {u5_2020:,} -> {u5_2024:,} ({pct_change:+.1f}%)")
    under5_data.append({
        'locale_type': locale,
        'under5_2020': int(u5_2020),
        'under5_2024': int(u5_2024),
        'change': int(change),
        'pct_change': round(pct_change, 1)
    })

# Large urban county details
print("\n" + "="*80)
print("LARGE URBAN COUNTIES DETAIL")
print("="*80)

large_urban_pop = pop_data[pop_data['locale_type'] == 'Large urban']
lu_baseline = large_urban_pop[large_urban_pop['YEAR'] == 1][['FIPS', 'STNAME', 'CTYNAME', 'POPESTIMATE', 'UNDER5_TOT']].copy()
lu_latest = large_urban_pop[large_urban_pop['YEAR'] == 6][['FIPS', 'POPESTIMATE', 'UNDER5_TOT']].copy()
lu_merged = lu_baseline.merge(lu_latest, on='FIPS', suffixes=('_2020', '_2024'))
lu_merged['under5_change'] = lu_merged['UNDER5_TOT_2024'] - lu_merged['UNDER5_TOT_2020']
lu_merged['under5_pct_change'] = ((lu_merged['UNDER5_TOT_2024'] - lu_merged['UNDER5_TOT_2020']) / lu_merged['UNDER5_TOT_2020'] * 100).round(1)

# Add birth rate data for large urban counties
lu_births = df_24[df_24['locale_type'] == 'Large urban'][['FIPS', 'RBIRTH2021', 'RBIRTH2024']].copy()
lu_births_10 = df_10[df_10['locale_type'] == 'Large urban'][['FIPS', 'RBIRTH2011']].copy()
lu_births = lu_births.merge(lu_births_10, on='FIPS', how='left')
lu_births['br_change'] = lu_births['RBIRTH2024'] - lu_births['RBIRTH2011']
lu_births['br_pct_change'] = ((lu_births['RBIRTH2024'] - lu_births['RBIRTH2011']) / lu_births['RBIRTH2011'] * 100).round(1)

lu_merged = lu_merged.merge(lu_births, on='FIPS', how='left')

print(f"\nNumber of large urban counties: {len(lu_merged)}")
print(f"Total under-5 change: {lu_merged['under5_change'].sum():,}")

print("\nLargest under-5 declines (absolute):")
for _, row in lu_merged.nsmallest(10, 'under5_change').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['under5_change']:,} ({row['under5_pct_change']:+.1f}%)")

print("\nLargest under-5 declines (percentage):")
for _, row in lu_merged.nsmallest(10, 'under5_pct_change').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['under5_pct_change']:+.1f}% ({row['under5_change']:,})")

print("\nLargest birth rate declines (2011-2024):")
for _, row in lu_merged.dropna(subset=['br_pct_change']).nsmallest(10, 'br_pct_change').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['RBIRTH2011']:.1f} -> {row['RBIRTH2024']:.1f} ({row['br_pct_change']:+.1f}%)")

# Export data
print("\n" + "="*80)
print("EXPORTING DATA FOR WEBSITE")
print("="*80)

# 1. Birth rate time series by county type
with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/birth_rate_ts.json', 'w') as f:
    json.dump(birth_rate_ts, f)
print(f"Exported birth_rate_ts.json: {len(birth_rate_ts)} records")

# 2. Birth rate time series as JS (grouped by locale)
ts_by_type = {}
for locale in locale_order:
    locale_data = ts_df[ts_df['locale_type'] == locale].sort_values('year')
    ts_by_type[locale] = [{'year': int(row['year']), 'rate': row['birth_rate']} for _, row in locale_data.iterrows()]

with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/birth_rate_ts.js', 'w') as f:
    f.write('const birthRateTS = ')
    json.dump(ts_by_type, f)
    f.write(';')
print("Exported birth_rate_ts.js")

# 3. Under-5 by type
with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/under5_by_type.json', 'w') as f:
    json.dump(under5_data, f)
print("Exported under5_by_type.json")

# 4. Birth rate change by type
with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/birth_rate_change.json', 'w') as f:
    json.dump(change_data, f)
print("Exported birth_rate_change.json")

# 5. Large urban county data for map (compact format)
county_data_embed = {}
for _, row in lu_merged.iterrows():
    county_data_embed[row['FIPS']] = {
        'n': row['CTYNAME'],
        's': row['STNAME'],
        'p0': int(row['POPESTIMATE_2020']),
        'u0': int(row['UNDER5_TOT_2020']),
        'u4': int(row['UNDER5_TOT_2024']),
        'ac': int(row['under5_change']),
        'pc': float(row['under5_pct_change']),
        'br11': float(row['RBIRTH2011']) if pd.notna(row.get('RBIRTH2011')) else None,
        'br24': float(row['RBIRTH2024']) if pd.notna(row.get('RBIRTH2024')) else None,
        'brch': float(row['br_pct_change']) if pd.notna(row.get('br_pct_change')) else None
    }

with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/county_data_embed.js', 'w') as f:
    f.write('const countyData = ')
    json.dump(county_data_embed, f)
    f.write(';')
print(f"Exported county_data_embed.js: {len(county_data_embed)} large urban counties")

# 6. Summary stats
summary = {
    'large_urban': {
        'count': len(lu_merged),
        'under5_2020': int(lu_merged['UNDER5_TOT_2020'].sum()),
        'under5_2024': int(lu_merged['UNDER5_TOT_2024'].sum()),
        'under5_change': int(lu_merged['under5_change'].sum()),
        'under5_pct_change': round((lu_merged['under5_change'].sum() / lu_merged['UNDER5_TOT_2020'].sum()) * 100, 1)
    },
    'nationwide': {
        'under5_2020': int(pop_data[pop_data['YEAR'] == 1]['UNDER5_TOT'].sum()),
        'under5_2024': int(pop_data[pop_data['YEAR'] == 6]['UNDER5_TOT'].sum())
    },
    'birth_rate_change': change_data,
    'under5_by_type': under5_data
}
summary['nationwide']['under5_change'] = summary['nationwide']['under5_2024'] - summary['nationwide']['under5_2020']
summary['nationwide']['under5_pct_change'] = round((summary['nationwide']['under5_change'] / summary['nationwide']['under5_2020']) * 100, 1)

with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/summary_stats.json', 'w') as f:
    json.dump(summary, f, indent=2)
print("Exported summary_stats.json")

print("\n" + "="*80)
print("DONE!")
print("="*80)
