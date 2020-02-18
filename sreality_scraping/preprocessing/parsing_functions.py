import numpy as np

def is_prague_rental(estate):
    """Parse flats that are for rent only in the Prague area.

    
    Parameters
    ----------
    estate : dict
        Dictionary containing estate information. Includes houses, commercial space, lands and flats.
    
    Returns
    -------
    dict
        Dictionary containing estate information. Includes flats only.
    """
    is_rental = estate['seo']['category_type_cb'] == 2
    is_flat = estate['seo']['category_main_cb'] == 1
    prague_districts = [56, 57, 5001, 5002, 5003, 5004, 5005, 5006, 5007, 5008, 5009, 5010]
    in_prague = estate['locality_district_id'] in prague_districts
    return is_rental and is_flat and in_prague


def flatten_json(dictionary):
    """Transforms any nested dictionary into a one level dictionary.
    
    Parameters
    ----------
    dictionary : dict
        nested input dictionary
    
    Returns
    -------
    dict
        dictionary
    """
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(dictionary)
    return out


### OLD FUNCTIONS
def transform(estate):
    return rename(flatten(estate))


def flatten(estate):
    """Parse Sreality API response into a flat dictionary.
    Every line is wrapped in a try-except block due to the varying content of the response/API.

    Args:
        estate (dict): a dictionary containing estate information in the Sreality API format
    
    Returns:
        dict: flattened dictionary with data for the price prediction model and data for the website
    """
    estate_flat = {}
    # for website
    # DESCRIPTION
    try:
        estate_flat["description_short"] = estate["meta_description"]
    except:
        estate_flat["description_short"] = np.nan
    # LOCATION
    try:
        estate_flat["longitude"] = estate["map"]["lon"]
    except:
        estate_flat["longitude"] = np.nan
    try:
        estate_flat["latitude"] = estate["map"]["lat"]
    except:
        estate_flat["latitude"] = np.nan
    # SELLER INFO
    try:
        estate_flat["seller_ico"] = estate["_embedded"]["seller"]["_embedded"][
            "premise"
        ]["ico"]
    except:
        estate_flat["seller_ico"] = np.nan
    try:
        estate_flat["seller_email"] = estate["_embedded"]["seller"]["_embedded"][
            "premise"
        ]["email"]
    except:
        estate_flat["seller_email"] = np.nan
    try:
        estate_flat["seller_numbers"] = [
            tel["code"] + tel["number"]
            for tel in estate["_embedded"]["seller"]["_embedded"]["premise"]["phones"]
        ]
    except:
        estate_flat["seller_numbers"] = np.nan
    try:
        estate_flat["seller_web"] = estate["_embedded"]["seller"]["_embedded"][
            "premise"
        ]["www"]
    except:
        estate_flat["seller_web"] = np.nan
    try:
        estate_flat["seller_address"] = estate["_embedded"]["seller"]["_embedded"][
            "premise"
        ]["address"]
    except:
        estate_flat["seller_address"] = np.nan
    try:
        estate_flat["seller_name"] = estate["_embedded"]["seller"]["_embedded"][
            "premise"
        ]["name"]
    except:
        estate_flat["seller_name"] = np.nan
    # IMAGES
    try:
        estate_flat["images"] = [
            image["_links"]["view"]["href"] for image in estate["_embedded"]["images"]
        ]
    except:
        estate_flat["images"] = np.nan
    # NAME
    try:
        estate_flat["title"] = estate["name"]["value"]
    except:
        estate_flat["title"] = np.nan
    # TEXT/POPIS
    try:
        estate_flat["description_long"] = estate["text"]["value"]
    except:
        estate_flat["description_long"] = np.nan

    # for modeling
    # seo
    try:
        estate_flat["property_type"] = estate["seo"]["category_main_cb"]
    except:
        estate_flat["property_type"] = np.nan
    try:
        estate_flat["disposition"] = estate["seo"]["category_sub_cb"]
    except:
        estate_flat["disposition"] = np.nan
    try:
        estate_flat["rental_or_sell"] = estate["seo"]["category_type_cb"]
    except:
        estate_flat["rental_or_sell"] = np.nan
    try:
        estate_flat["locality_district"] = estate["locality_district_id"]
    except:
        estate_flat["locality_district"] = np.nan
    # below are dicts that need to be parsed iteratively
    # points of interest
    if "poi" in estate.keys():
        for poi in estate["poi"]:
            estate_flat[poi["name"]] = poi["distance"]
    # items
    if "items" in estate.keys():
        for item in estate["items"]:
            estate_flat[item["name"]] = item["value"]
    # code items
    if "codeItems" in estate.keys():
        for name, value in estate["codeItems"].items():
            estate_flat[name] = value
    return estate_flat


def rename(estate):
    """Renames the flattened Sreality api dict.
    
    Args:
        estate (dict): a dictionary containing estate information in the Sreality API format
    
    Returns:
        dict: renamed input dict based on the mapping inside the func
    """
    mapping = {
        "Aktualizace": "last_update",
        "Balkón": "balcony",
        "Bankomat": "atm",
        "Bezbariérový": "disabled_access",
        "Bus MHD": "bus",
        "Celková cena": "price",
        "Cena": "price",
        "Cukrárna": "candy_shop",
        "Datum nastěhování": "move_in_date",
        "Datum prohlídky": "viewing_date",
        "Divadlo": "theatre",
        "Doprava": "commute",
        "Elektřina": "electricity",
        "Energetická náročnost budovy": "energy_cost_level",
        "Garáž": "garage",
        "Hospoda": "pub",
        "Hřiště": "playground",
        "Kino": "cinema",
        "Komunikace": "street",
        "Kulturní památka": "is_cultural_heritage",
        "Lodžie": "loggia",
        "Lékař": "doctor",
        "Lékárna": "pharmacy",
        "Metro": "subway",
        "Náklady na bydlení": "living_cost",
        "Obchod": "shop",
        "Odpad": "waste_management",
        "Parkování": "parking",
        "Plocha podlahová": "area_floor",
        "Plocha pozemku": "area_land",
        "Plocha zastavěná": "area_building",
        "Plyn": "gas",
        "Podlaží": "floor",
        "Poloha domu": "house_location",
        "Poznámka k ceně": "price_notes",
        "Pošta": "post_office",
        "Půdní vestavba": "attic",
        "Restaurace": "restaurant",
        "Rok kolaudace": "year_occupancy_permit",
        "Rok rekonstrukce": "year_reconstruction",
        "Sklep": "cellar",
        "Sportoviště": "playground",
        "Stav objektu": "object_state",
        "Stavba": "building",
        "Telekomunikace": "telecommunications",
        "Terasa": "terace",
        "Topení": "heating",
        "Tram": "tram",
        "Typ domu": "building_type",
        "Ukazatel energetické náročnosti budovy": "energy_cost_level",
        "Umístění objektu": "object_location",
        "Užitná plocha": "area",
        "Veterinář": "veterinary",
        "Večerka": "convenience_store",
        "Vlak": "train",
        "Vlastnictví": "ownership",
        "Voda": "water",
        "Vybavení": "furnished",
        "Výtah": "elevator",
        "building_type_search": "building_type_search",
        "description_long": "description_long",
        "description_short": "description_short",
        "disposition": "disposition",
        "estate_area": "estate_area",
        "images": "images",
        "latitude": "latitude",
        "locality_district": "locality_district",
        "longitude": "longitude",
        "object_type": "object_type",
        "ownership": "ownership",
        "property_type": "property_type",
        "rental_or_sell": "rental_or_sell",
        "seller_address": "seller_address",
        "seller_email": "seller_email",
        "seller_ico": "seller_ico",
        "seller_name": "seller_name",
        "seller_numbers": "seller_numbers",
        "seller_web": "seller_web",
        "something_more1": "something_more1",
        "something_more2": "something_more2",
        "something_more3": "something_more3",
        "title": "title",
        "Škola": "school",
        "Školka": "kindergarten",
    }
    estate_renamed = {}
    for key in estate.keys():
        if key in mapping.keys():
            new_key = mapping[key]
            new_value = estate[key]
            estate_renamed[new_key] = new_value
    return estate_renamed

