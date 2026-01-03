import polars as pl
import folium
import requests
import json
import glob
import os

# 1. Find the latest P2000 TSV file
tsv_files = glob.glob("p2000_*.tsv")
if not tsv_files:
    print("No TSV files found!")
    exit(1)
latest_tsv = max(tsv_files, key=os.path.getmtime)
print(f"Using latest data file: {latest_tsv}")

# 2. Load and Clean Data
df = pl.read_csv(latest_tsv, separator='\t', quote_char=None)

df_clean = (
    df
    .with_columns(pl.col("Timestamp").str.to_datetime("%d-%m-%Y %H:%M:%S"))
    .filter(
        (pl.col("Message") != "TESTOPROEP MOB") &
        (~pl.col("Message").str.contains(r"(?i)\btest\b"))
    )
)

# 3. Aggregation per Region
stats = (
    df_clean
    .group_by("Region")
    .len()
    .rename({"len": "Total"})
    .sort("Total", descending=True)
)

# 4. Fetch GeoJSON from PDOK (CBS 2024)
wfs_url = "https://service.pdok.nl/cbs/gebiedsindelingen/2024/wfs/v1_0"
params = {
    "request": "GetFeature",
    "service": "WFS",
    "version": "2.0.0",
    "typeName": "gebiedsindelingen:veiligheidsregio_gegeneraliseerd",
    "outputFormat": "application/json"
}

print("Fetching GeoJSON from PDOK...")
try:
    response = requests.get(wfs_url, params=params)
    response.raise_for_status()
    geo_data = response.json()
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# 5. Mapping Strategy (P2000 -> GeoJSON statnaam)
# Based on inspection, statnaam is just the name (e.g. "Groningen", "Haaglanden")
region_mapping = {
    "amsterdam-amstelland": "Amsterdam-Amstelland",
    "brabant noord": "Brabant-Noord",
    "brabant zuid-oost": "Brabant-Zuidoost",
    "midden- en west brabant": "Midden- en West-Brabant",
    "drenthe": "Drenthe",
    "flevoland": "Flevoland",
    "fryslân": "Fryslân",
    "friesland": "Fryslân",
    "gelderland-midden": "Gelderland-Midden",
    "gelderland-zuid": "Gelderland-Zuid",
    "noord- en oost gelderland": "Noord- en Oost-Gelderland",
    "groningen": "Groningen",
    "haaglanden": "Haaglanden",
    "hollands midden": "Hollands-Midden",
    "ijsselland": "IJsselland",
    "kennemerland": "Kennemerland",
    "limburg noord": "Limburg-Noord",
    "limburg zuid": "Limburg-Zuid",
    "noord-holland noord": "Noord-Holland-Noord",
    "rotterdam-rijnmond": "Rotterdam-Rijnmond",
    "twente": "Twente",
    "utrecht": "Utrecht",
    "zaanstreek-waterland": "Zaanstreek-Waterland",
    "zeeland": "Zeeland",
    "zuid-holland zuid": "Zuid-Holland-Zuid",
    "gooi en vechtstreek": "Gooi en Vechtstreek"
}

def get_official_name(p2000_name):
    if p2000_name is None: return "Unknown"
    ln = p2000_name.lower().strip()
    return region_mapping.get(ln, p2000_name)

stats_pd = stats.to_pandas().dropna(subset=['Region'])
stats_pd['normalized_name'] = stats_pd['Region'].apply(get_official_name)

# Debug Mismatches
geo_names = [f['properties']['statnaam'] for f in geo_data['features']]
print(f"Sample GeoJSON names: {geo_names[:5]}")

matches = stats_pd[stats_pd['normalized_name'].isin(geo_names)]
mismatches = stats_pd[~stats_pd['normalized_name'].isin(geo_names)]

print(f"Matches: {len(matches)} / {len(stats_pd)}")
if not mismatches.empty:
    print(f"Mismatches: {mismatches['normalized_name'].tolist()}")

# 6. Create Map
m = folium.Map(location=[52.1326, 5.2913], zoom_start=7, tiles="cartodbpositron")

folium.Choropleth(
    geo_data=geo_data,
    data=stats_pd,
    columns=["normalized_name", "Total"],
    key_on="feature.properties.statnaam",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Emergency Calls",
    highlight=True
).add_to(m)

m.save("p2000_map.html")
print("Map saved to p2000_map.html")
