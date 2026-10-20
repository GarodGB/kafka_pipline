import json
import pandas as pd
from cleaning import clean_city

raw = pd.read_excel("data/synthetic_dirty_transactions_v4.xlsx", dtype=str)


raw["CITY"] = raw["MERCHANT_CITY"].apply(lambda v: clean_city(v)[0])


real = raw[~raw["CITY"].isin(["ONLINE", "UNKNOWN"])]


city_country = {}
for city, group in real.groupby("CITY"):
    city_country[city] = group["MERCHANT_COUNTRY"].str.upper().str.strip().value_counts().index[0]


with open("config/city_country.json", "w") as f:
    json.dump(city_country, f, indent=2)
print(f"Saved {len(city_country)} cities to config/city_country.json")