#!/usr/bin/env python3
"""
Process fertility rate data (births per 1,000 women age 15-49) by EIG county typology.
Focus on large urban counties.
"""

import pandas as pd
import json

print("="*80)
print("CITIES FOR FAMILIES - FERTILITY RATE ANALYSIS")
print("Births per 1,000 women age 15-49")
print("="*80)

# Load EIG county typology
print("\nLoading EIG county typology...")
county_types = pd.read_csv('/Users/connorobrien/Downloads/county_summary_final.csv', encoding='latin-1')
county_types['county_fips'] = county_types['county_fips'].astype(str).str.zfill(5)

print(f"Total counties: {len(county_types)}")
print("\nCounty types:")
for t, c in county_types['locale_type'].value_counts().items():
    print(f"  {t}: {c}")

# Load age-sex data (has female population by age groups)
print("\nLoading age-sex population data...")
pop_data = pd.read_csv('/Users/connorobrien/Downloads/cc-est2024-agesex-all.csv', encoding='latin-1')
pop_data['COUNTY'] = pop_data['COUNTY'].astype(str).str.zfill(3)
pop_data['STATE'] = pop_data['STATE'].astype(str).str.zfill(2)
pop_data['FIPS'] = pop_data['STATE'] + pop_data['COUNTY']
pop_data = pop_data.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Calculate women 15-49 for each county/year
# Columns: AGE1519_FEM, AGE2024_FEM, AGE2529_FEM, AGE3034_FEM, AGE3539_FEM, AGE4044_FEM, AGE4549_FEM
pop_data['WOMEN_15_49'] = (pop_data['AGE1519_FEM'] + pop_data['AGE2024_FEM'] +
                           pop_data['AGE2529_FEM'] + pop_data['AGE3034_FEM'] +
                           pop_data['AGE3539_FEM'] + pop_data['AGE4044_FEM'] +
                           pop_data['AGE4549_FEM'])

print(f"Year codes in age-sex data: {sorted(pop_data['YEAR'].unique())}")
# YEAR 1 = April 2020, YEAR 2 = July 2020, YEAR 3 = July 2021, etc.

# Load birth data (2020-2024)
print("\nLoading 2020-2024 components data...")
df_24 = pd.read_csv('/Users/connorobrien/Downloads/co-est2024-alldata.csv', encoding='latin-1')
df_24['COUNTY'] = df_24['COUNTY'].astype(str).str.zfill(3)
df_24['STATE'] = df_24['STATE'].astype(str).str.zfill(2)
df_24['FIPS'] = df_24['STATE'] + df_24['COUNTY']
df_24 = df_24[df_24['COUNTY'] != '000']
df_24 = df_24.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Load 2010-2020 birth data
print("Loading 2010-2020 components data...")
df_10 = pd.read_csv('/Users/connorobrien/Downloads/co-est2020-alldata.csv', encoding='latin-1')
df_10['COUNTY'] = df_10['COUNTY'].astype(str).str.zfill(3)
df_10['STATE'] = df_10['STATE'].astype(str).str.zfill(2)
df_10['FIPS'] = df_10['STATE'] + df_10['COUNTY']
df_10 = df_10[df_10['COUNTY'] != '000']
df_10 = df_10.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

# Need age-sex data for 2010-2020 period as well
# Check if there's a 2020 age-sex file
print("\nLoading 2010-2020 age-sex data...")
try:
    pop_data_10 = pd.read_csv('/Users/connorobrien/Downloads/cc-est2020-agesex-all.csv', encoding='latin-1', low_memory=False)
    pop_data_10['COUNTY'] = pop_data_10['COUNTY'].astype(str).str.zfill(3)
    pop_data_10['STATE'] = pop_data_10['STATE'].astype(str).str.zfill(2)
    pop_data_10['FIPS'] = pop_data_10['STATE'] + pop_data_10['COUNTY']
    pop_data_10 = pop_data_10.merge(county_types[['county_fips', 'locale_type']], left_on='FIPS', right_on='county_fips', how='left')

    # Convert age columns to numeric (some may have mixed types)
    age_cols = ['AGE1519_FEM', 'AGE2024_FEM', 'AGE2529_FEM', 'AGE3034_FEM',
                'AGE3539_FEM', 'AGE4044_FEM', 'AGE4549_FEM']
    for col in age_cols:
        pop_data_10[col] = pd.to_numeric(pop_data_10[col], errors='coerce').fillna(0)

    # Calculate women 15-49
    pop_data_10['WOMEN_15_49'] = (pop_data_10['AGE1519_FEM'] + pop_data_10['AGE2024_FEM'] +
                                  pop_data_10['AGE2529_FEM'] + pop_data_10['AGE3034_FEM'] +
                                  pop_data_10['AGE3539_FEM'] + pop_data_10['AGE4044_FEM'] +
                                  pop_data_10['AGE4549_FEM'])
    print(f"2010-2020 age-sex data loaded: {len(pop_data_10)} rows")
    print(f"Year codes: {sorted(pop_data_10['YEAR'].unique())}")
    has_old_agesex = True
except FileNotFoundError:
    print("No 2010-2020 age-sex file found, will only calculate 2020-2024 fertility rates")
    has_old_agesex = False

# Calculate fertility rates by county type
print("\nCalculating fertility rates by county type...")

locale_order = ['Large urban', 'Mid-sized urban', 'Small urban', 'Suburban', 'Small town', 'Rural']
fertility_rate_ts = []

# For 2020-2024 data
# Map YEAR codes to calendar years in age-sex data:
# YEAR 2 = July 2020, YEAR 3 = July 2021, YEAR 4 = July 2022, YEAR 5 = July 2023, YEAR 6 = July 2024
year_map_24 = {2020: 2, 2021: 3, 2022: 4, 2023: 5, 2024: 6}

for locale in locale_order:
    # 2021-2024 fertility rates using new data
    subset_births = df_24[df_24['locale_type'] == locale]
    subset_pop = pop_data[pop_data['locale_type'] == locale]

    for year in range(2021, 2025):
        births_col = f'BIRTHS{year}'
        year_code = year_map_24.get(year)

        if births_col in subset_births.columns and year_code:
            # Get women 15-49 for this year
            pop_year = subset_pop[subset_pop['YEAR'] == year_code]
            women_15_49 = pop_year.groupby('FIPS')['WOMEN_15_49'].sum()

            # Merge with births
            births = subset_births.set_index('FIPS')[births_col]

            # Calculate weighted fertility rate
            total_births = births.sum()
            total_women = women_15_49.sum()

            if total_women > 0:
                fertility_rate = (total_births / total_women) * 1000
                fertility_rate_ts.append({
                    'locale_type': locale,
                    'year': year,
                    'fertility_rate': round(fertility_rate, 2),
                    'births': int(total_births),
                    'women_15_49': int(total_women)
                })
                print(f"  {locale} {year}: {fertility_rate:.2f} per 1000 ({int(total_births):,} births / {int(total_women):,} women)")

# For 2011-2020 data (if we have old age-sex data)
if has_old_agesex:
    # Map YEAR codes for 2010-2020 data:
    # YEAR 3 = 7/1/2010, YEAR 4 = 7/1/2011, ... YEAR 13 = 7/1/2020
    year_map_10 = {2010: 3, 2011: 4, 2012: 5, 2013: 6, 2014: 7, 2015: 8, 2016: 9, 2017: 10, 2018: 11, 2019: 12, 2020: 13}

    for locale in locale_order:
        subset_births = df_10[df_10['locale_type'] == locale]
        subset_pop = pop_data_10[pop_data_10['locale_type'] == locale]

        for year in range(2011, 2021):
            births_col = f'BIRTHS{year}'
            year_code = year_map_10.get(year)

            if births_col in subset_births.columns and year_code and year_code in subset_pop['YEAR'].values:
                pop_year = subset_pop[subset_pop['YEAR'] == year_code]
                women_15_49 = pop_year.groupby('FIPS')['WOMEN_15_49'].sum()

                births = subset_births.set_index('FIPS')[births_col]

                total_births = births.sum()
                total_women = women_15_49.sum()

                if total_women > 0:
                    fertility_rate = (total_births / total_women) * 1000
                    fertility_rate_ts.append({
                        'locale_type': locale,
                        'year': year,
                        'fertility_rate': round(fertility_rate, 2),
                        'births': int(total_births),
                        'women_15_49': int(total_women)
                    })

# Create DataFrame and sort
ts_df = pd.DataFrame(fertility_rate_ts)
ts_df = ts_df.sort_values(['locale_type', 'year'])

print("\n" + "="*80)
print("FERTILITY RATE SUMMARY BY COUNTY TYPE")
print("="*80)

# Show summary
for locale in locale_order:
    locale_df = ts_df[ts_df['locale_type'] == locale].sort_values('year')
    if len(locale_df) > 0:
        first_year = locale_df.iloc[0]
        last_year = locale_df.iloc[-1]
        change = last_year['fertility_rate'] - first_year['fertility_rate']
        pct_change = (change / first_year['fertility_rate']) * 100
        print(f"\n{locale}:")
        print(f"  {int(first_year['year'])}: {first_year['fertility_rate']:.1f} per 1,000 women")
        print(f"  {int(last_year['year'])}: {last_year['fertility_rate']:.1f} per 1,000 women")
        print(f"  Change: {change:+.1f} ({pct_change:+.1f}%)")

# Load under-5 population data for context
print("\n" + "="*80)
print("UNDER-5 POPULATION CHANGES")
print("="*80)

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
lu_baseline = large_urban_pop[large_urban_pop['YEAR'] == 1][['FIPS', 'STNAME', 'CTYNAME', 'POPESTIMATE', 'UNDER5_TOT', 'WOMEN_15_49']].copy()
lu_latest = large_urban_pop[large_urban_pop['YEAR'] == 6][['FIPS', 'POPESTIMATE', 'UNDER5_TOT', 'WOMEN_15_49']].copy()
lu_merged = lu_baseline.merge(lu_latest, on='FIPS', suffixes=('_2020', '_2024'))
lu_merged['under5_change'] = lu_merged['UNDER5_TOT_2024'] - lu_merged['UNDER5_TOT_2020']
lu_merged['under5_pct_change'] = ((lu_merged['UNDER5_TOT_2024'] - lu_merged['UNDER5_TOT_2020']) / lu_merged['UNDER5_TOT_2020'] * 100).round(1)

# Add fertility rate data for large urban counties
lu_births_24 = df_24[df_24['locale_type'] == 'Large urban'][['FIPS', 'BIRTHS2021', 'BIRTHS2024']].copy()

# Calculate fertility rate for 2024 and 2021
lu_merged = lu_merged.merge(lu_births_24, on='FIPS', how='left')
lu_merged['fertility_2024'] = (lu_merged['BIRTHS2024'] / lu_merged['WOMEN_15_49_2024'] * 1000).round(1)
lu_merged['fertility_2021'] = (lu_merged['BIRTHS2021'] / lu_merged['WOMEN_15_49_2020'] * 1000).round(1)
lu_merged['fertility_change'] = (lu_merged['fertility_2024'] - lu_merged['fertility_2021']).round(1)
lu_merged['fertility_pct_change'] = ((lu_merged['fertility_2024'] - lu_merged['fertility_2021']) / lu_merged['fertility_2021'] * 100).round(1)

print(f"\nNumber of large urban counties: {len(lu_merged)}")
print(f"Total under-5 change: {lu_merged['under5_change'].sum():,}")

print("\nLargest under-5 declines (absolute):")
for _, row in lu_merged.nsmallest(10, 'under5_change').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['under5_change']:,} ({row['under5_pct_change']:+.1f}%)")

print("\nLowest fertility rates 2024:")
for _, row in lu_merged.nsmallest(10, 'fertility_2024').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['fertility_2024']:.1f} per 1,000 women")

print("\nHighest fertility rates 2024:")
for _, row in lu_merged.nlargest(10, 'fertility_2024').iterrows():
    print(f"  {row['CTYNAME']}, {row['STNAME']}: {row['fertility_2024']:.1f} per 1,000 women")

# Export data
print("\n" + "="*80)
print("EXPORTING DATA FOR WEBSITE")
print("="*80)

# 1. Fertility rate time series by county type
with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/fertility_rate_ts.json', 'w') as f:
    json.dump(fertility_rate_ts, f)
print(f"Exported fertility_rate_ts.json: {len(fertility_rate_ts)} records")

# 2. Fertility rate time series as JS (grouped by locale)
ts_by_type = {}
for locale in locale_order:
    locale_data = ts_df[ts_df['locale_type'] == locale].sort_values('year')
    ts_by_type[locale] = [{'year': int(row['year']), 'rate': row['fertility_rate']} for _, row in locale_data.iterrows()]

with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/fertility_rate_ts.js', 'w') as f:
    f.write('const fertilityRateTS = ')
    json.dump(ts_by_type, f)
    f.write(';')
print("Exported fertility_rate_ts.js")

# 3. Under-5 by type
with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/under5_by_type.json', 'w') as f:
    json.dump(under5_data, f)
print("Exported under5_by_type.json")

# 4. Large urban county data for map (compact format)
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
        'w0': int(row['WOMEN_15_49_2020']),
        'w4': int(row['WOMEN_15_49_2024']),
        'fr21': float(row['fertility_2021']) if pd.notna(row.get('fertility_2021')) else None,
        'fr24': float(row['fertility_2024']) if pd.notna(row.get('fertility_2024')) else None,
        'frch': float(row['fertility_pct_change']) if pd.notna(row.get('fertility_pct_change')) else None
    }

with open('/Users/connorobrien/Documents/GitHub/cities-for-families/data/county_data_embed.js', 'w') as f:
    f.write('const countyData = ')
    json.dump(county_data_embed, f)
    f.write(';')
print(f"Exported county_data_embed.js: {len(county_data_embed)} large urban counties")

# 5. Summary stats
summary = {
    'large_urban': {
        'count': len(lu_merged),
        'under5_2020': int(lu_merged['UNDER5_TOT_2020'].sum()),
        'under5_2024': int(lu_merged['UNDER5_TOT_2024'].sum()),
        'under5_change': int(lu_merged['under5_change'].sum()),
        'under5_pct_change': round((lu_merged['under5_change'].sum() / lu_merged['UNDER5_TOT_2020'].sum()) * 100, 1),
        'avg_fertility_2021': round(lu_merged['fertility_2021'].mean(), 1),
        'avg_fertility_2024': round(lu_merged['fertility_2024'].mean(), 1)
    },
    'nationwide': {
        'under5_2020': int(pop_data[pop_data['YEAR'] == 1]['UNDER5_TOT'].sum()),
        'under5_2024': int(pop_data[pop_data['YEAR'] == 6]['UNDER5_TOT'].sum())
    },
    'fertility_by_type': ts_by_type,
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
