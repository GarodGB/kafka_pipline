import json
import re
import pandas as pd
from cleaning import clean_merchant

raw = pd.read_excel("data/synthetic_dirty_transactions_v4.xlsx", dtype=str)
names = set()
for v in raw["MERCHANT_NAME"]:
    name, _ = clean_merchant(v, use_map=False)
    if name:
        names.add(name)
print("Before map:", len(names))


ABBREV = {"STR": "STORE", "STN": "STATION", "MKT": "MARKET", "CTR": "CENTER",
          "PHCY": "PHARMACY", "SPRMKT": "SUPERMARKET", "TRF": "TRANSFER",
          "SVCS": "SERVICES"}

def expand(n):      # "NIKE STR" -> "NIKE STORE"
    return " ".join(ABBREV.get(w, w) for w in n.split())

def squash(n):      # "BOOKING COM" / "BOOKINGCOM.COM" -> "BOOKING"
    n = n.replace(".COM", "").replace(" ", "")
    return re.sub(r"COM$", "", n)

def skeleton(n):    # "BEIRUT ROASTERY" / "BRT ROASTERY" -> "BRTRSTRY"
    return re.sub(r"[AEIOU]", "", squash(n))

def score(n):       # best spelling = most letters, has spaces, no .COM
    return (len(squash(n)), " " in n, ".COM" not in n)

full = {n: expand(n) for n in names}     # original -> expanded
best = {}                                # original -> best name

# ---------- RULE 1: same skeleton = same merchant (typos, .COM) ----------
groups = {}
for n in names:
    groups.setdefault(skeleton(full[n]), []).append(n)
for group in groups.values():
    winner = full[max(group, key=lambda n: score(full[n]))]
    for n in group:
        best[n] = winner

# ---------- RULE 2: cut-off  ("BOB F" -> "BOB FINANCE") ----------
for n in names:
    longer = [m for m in names if m != n and full[m].startswith(full[n])]
    if longer:
        best[n] = best[max(longer, key=len)]

# ---------- RULE 3: initials  ("BEAUTY LH" -> "BEAUTY LOUNGE HAMRA") ----------
for n in names:
    w = full[n].split()
    if len(w) < 2:
        continue
    start, last = w[:-1], w[-1]
    for m in names:
        mw = full[m].split()
        if len(mw) > len(w) and mw[:len(start)] == start:
            if "".join(x[0] for x in mw[len(start):]) == last:
                best[n] = best[m]

# ---------- RULE 4: manual fixes (rules can't guess these) ----------
MANUAL = {
    "ALLIANZ D": "ALLIANZ DIRECT",  "ALLNZ DIRECT": "ALLIANZ DIRECT",
    "INTERACTIVE B": "INTERACTIVE BROKERS",
    "INTERACTIVEBROKERS.COM": "INTERACTIVE BROKERS",
    "AWSCLOUDSERVICES.COM": "AWS CLOUD SERVICES",
    "AWS CLOUD SERVI": "AWS CLOUD SERVICES",
    "JM NIGERIA": "JUMIA NIGERIA",  "JUMIA N": "JUMIA NIGERIA",
    "JUMEIRAH BEACH HOTEL B": "JUMEIRAH BEACH HOTEL",
    "TSC S": "TESCO EXPRESS",
    "UPWORK E": "UPWORK ESCROW",    "UPWRK ESCROW": "UPWORK ESCROW",
    "WYFR": "WAYFAIR",              "WAYFAIR.COM": "WAYFAIR",
    "ALLIANZDIRECT.COM": "ALLIANZ DIRECT",              "DR NH": "DR NADIA HADDAD",
    "VSTPRNT": "VISTAPRINT",        "STRBCKS": "STARBUCKS",
    "CNBS": "COINBASE",             "BNNC": "BINANCE",
}
for n in names:
    if best[n] in MANUAL:
        best[n] = MANUAL[best[n]]
    if n in MANUAL:
        best[n] = MANUAL[n]

# ---------- follow chains  (SPHR -> SEPHORA -> SEPHORA MIDDLE EAST) ----------
for n in names:
    while best[n] in best and best[best[n]] != best[n]:
        best[n] = best[best[n]]


mapping = {n: best[n] for n in sorted(names) if best[n] != n}
with open("config/merchant_map.json", "w") as f:
    json.dump(mapping, f, indent=2)

after = sorted({best[n] for n in names})
print("Map entries:", len(mapping))
print("After map:", len(after))