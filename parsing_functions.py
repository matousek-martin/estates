import pandas as pd
import numpy as np

rename_mapping = {
    "efficiency": "efficiency_category",
    "floor": "floor",
    "balcony_area": "has_balcony",
    "elavator": "has_elevator",
    "garage": "has_garage",
    "property_state": "property_state",
    "stavba": "building_type",
    "area": "area",
    "prague_district": "prague_district",
    "cena_czk_value": "price",
}


building_type_mapping = {"Kamenná": "Other", "Dřevěná": "Other", "Montovaná": "Other"}

property_state_mapping = {
    "Dobrý": "Other",
    "Před rekonstrukcí": "Other",
    "Špatný": "Other",
}

feature_transformation_mapping = {
    "efficiency_category": lambda x: "Other" if pd.isnull(x) else x.split(" - ")[0],
    "floor": lambda x: 0 if "přízemí" in x else x.split(". podlaží")[0],
    "has_balcony": lambda x: not pd.isnull(x),
    "has_elevator": lambda x: False if (pd.isnull(x) or not x) else True,
    "has_garage": lambda x: pd.notnull(x),
    "property_state": lambda x: property_state_mapping[x]
    if x in property_state_mapping
    else x,
    "building_type": lambda x: building_type_mapping[x]
    if x in building_type_mapping
    else x,
    "prague_district": lambda x: np.nan if x == 47 else x,
}


features = [
    "efficiency_category",
    "has_balcony",
    "has_elevator",
    "has_garage",
    "property_state",
    "building_type",
    "area",
    "prague_district"
]
target = ["price"]


type_mapping = {
    "efficiency_category": np.str_,
    "has_balcony": np.bool_,
    "has_elevator": np.bool_,
    "has_garage": np.bool_,
    "property_state": np.str_,
    "building_type": np.str_,
    "area": np.str_,
    "prague_district": np.float32,
    "price": np.float64,
}
