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

# 3. Create Rich Stats (Pivot Table)
stats_grouped = (
    df_clean
    .group_by(["Region", "Service"])
    .len()
    .rename({"len": "Count"})
)

stats_pivot = (
    stats_grouped
    .pivot(values="Count", index="Region", on="Service", aggregate_function="sum")
    .fill_null(0)
)

for service in ["Ambulance", "Brandweer", "Politie"]:
    if service not in stats_pivot.columns:
        stats_pivot = stats_pivot.with_columns(pl.lit(0).alias(service))

stats_pivot = stats_pivot.with_columns(
    (pl.col("Ambulance") + pl.col("Brandweer") + pl.col("Politie")).alias("Total")
)

# 4. Fetch GeoJSON from PDOK (EPSG:4326)
wfs_url = "https://service.pdok.nl/cbs/gebiedsindelingen/2024/wfs/v1_0"
params = {
    "request": "GetFeature",
    "service": "WFS",
    "version": "2.0.0",
    "typeName": "gebiedsindelingen:veiligheidsregio_gegeneraliseerd",
    "outputFormat": "application/json",
    "srsName": "EPSG:4326"
}

print("Fetching GeoJSON from PDOK...")
try:
    response = requests.get(wfs_url, params=params)
    response.raise_for_status()
    geo_data = response.json()
except Exception as e:
    print(f"Error: {e}")
    exit(1)

# 5. Name Normalization
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
    "noord- en oost-gelderland": "Noord- en Oost-Gelderland",
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

stats_rows = stats_pivot.to_dicts()
data_lookup = {}
for row in stats_rows:
    if row["Region"]:
        norm_name = get_official_name(row["Region"])
        data_lookup[norm_name] = row

for feature in geo_data['features']:
    props = feature['properties']
    region_name = props['statnaam']
    stats = data_lookup.get(region_name, {"Total": 0, "Ambulance": 0, "Brandweer": 0, "Politie": 0})
    props['Total Calls'] = stats['Total']
    props['Ambulance'] = stats['Ambulance']
    props['Firefighters'] = stats['Brandweer']
    props['Police'] = stats['Politie']

# 6. Create Map
m = folium.Map(location=[52.1326, 5.2913], zoom_start=7, tiles="cartodbpositron")

# Prepare pandas DF for Choropleth
stats_pd = stats_pivot.to_pandas()
stats_pd['normalized_name'] = stats_pd['Region'].apply(get_official_name)

choropleth = folium.Choropleth(
    geo_data=geo_data,
    name="Emergency Calls",
    data=stats_pd, 
    columns=["normalized_name", "Total"],
    key_on="feature.properties.statnaam",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name="Emergency Calls",
    highlight=True
).add_to(m)

# Tooltip layer
folium.GeoJson(
    geo_data,
    style_function=lambda x: {'fillColor': 'transparent', 'color': 'transparent', 'weight': 0},
    tooltip=folium.GeoJsonTooltip(
        fields=['statnaam', 'Total Calls', 'Ambulance', 'Firefighters', 'Police'],
        aliases=['Region:', 'Total:', 'Ambulance:', 'Firefighters:', 'Police:'],
        style=("background-color: white; color: #333333; font-family: arial; font-size: 12px; padding: 10px;")
    )
).add_to(m)

m.save("p2000_map.html")
print("Map saved to p2000_map.html with rich tooltips.")
