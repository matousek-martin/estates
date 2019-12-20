import numpy as np

items = [0, 1]
desc = {}
for item in items:
    if item["name"] == "Stavba":
        try:
            desc["stavba"] = item["value"]
        except:
            desc["stavba"] = np.nan
    if item["name"] == "Stav objektu":
        try:
            desc["property_state"] = item["value"]
        except:
            desc["property_state"] = np.nan
    if item["name"] == "Vlastnictvi":
        try:
            desc["ownership"] = item["value"]
        except:
            desc["ownership"] = np.nan
    if item["name"] == "Podlaží":
        try:
            desc["floor"] = item["value"]
        except:
            desc["floor"] = np.nan
    if item["name"] == "Užitná plocha":
        try:
            desc["area"] = item["value"]
        except:
            desc["area"] = np.nan
        try:
            desc["area_unit"] = item["unit"]
        except:
            desc["area_unit"] = np.nan
    if item["name"] == "Balkón":
        try:
            desc["balcony_area"] = item["value"]
        except:
            desc["balcony_area"] = np.nan
        try:
            desc["balcony_unit"] = item["unit"]
        except:
            desc["balcony_unit"] = np.nan
    if item["name"] == "Sklep":
        try:
            desc["cellar"] = item["value"]
        except:
            desc["cellar"] = np.nan
    if item["name"] == "Garáž":
        try:
            desc["garage"] = item["value"]
        except:
            desc["garage"] = np.nan
    if item["name"] == "Výtah":
        try:
            desc["elavator"] = item["value"]
        except:
            desc["elavator"] = np.nan
    if item["name"] == "Výtah":
        try:
            desc["elavator"] = item["value"]
        except:
            desc["elavator"] = np.nan
    if item["name"] == "Energetická náročnost budovy":
        try:
            desc["efficiency"] = item["value"]
        except:
            desc["efficiency"] = np.nan

