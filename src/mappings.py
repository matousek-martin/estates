# Data
columns = [
    # Target
    'Celková cena',

    # Features
    'Užitná plocha',
    'Podlaží',
    'estate_locality_district',
    'estate_disposition',
    'estate_category_main_cb',
    'Stavba',
    'Stav objektu',
    'Poznámka k ceně',
    'Energetická náročnost budovy',
    'Vlastnictví',
    'Tram',
    'Vybavení',
    'Výtah',
    'Divadlo',
    'Kino',
    'Obchod',
    'Cukrárna',
    'Veterinář',
    'Vlak',
    'Lékárna',
    'Bankomat',
    'Sportoviště',
    'Bus MHD',
    'Lékař',
    'Škola',
    'Školka',
    'Hospoda',
    'Pošta',
    'Restaurace',
    'Večerka',
    'Hřiště',
]

locality_district_mapping = {
    1: "ceske budejovice",
    12: "plzen-mesto",
    28: "hradec kralove",
    32: "pardubice",
    42: "olomouc",
    5001: "praha 1",
    5002: "praha 2",
    5003: "praha 3",
    5004: "praha 4",
    5005: "praha 5",
    5006: "praha 6",
    5007: "praha 7",
    5008: "praha 8",
    5009: "praha 9",
    5010: "praha 10",
    62: "karvina",
    65: "ostrava-mesto",
    72: "brno-mesto"
}

disposition_mapping = {
    2: "1+kk",
    3: "1+1",
    4: "2+kk",
    5: "2+1",
    6: "3+kk",
    7: "3+1",
    8: "4+kk",
    9: "4+1",
    10: "5+kk",
    11: "5+1",
    12: "6 a vice",
    16: "atypicky",
    37: "rodinny",
    39: "vila",
    43: "chalupa",
    33: "chata",
}

category_mapping = {
    1: 'flat',
    2: 'house'
}

efficiency_mapping = {
    'Třída A': 1,
    'Třída B': 2,
    'Třída C': 3,
    'Třída D': 4,
    'Třída E': 5,
    'Třída F': 6,
    'Třída G': 7
}

furnishing_mapping = {
    'True': 1,
    'Částečně': 0.5,
    'False': 0,
}


# Features
poi_columns = [
    'theatre', 'cinema', 'groceries', 'candy_shop', 'tram',
    'veterinary', 'train', 'pharmacist', 'atm', 'sports',
    'bus', 'doctors', 'school', 'kindergarten', 'pub',
    'post_office', 'restaurant', 'seven_eleven', 'playground'
]

room_mapping = {
    '2+kk': 2,
    '1+kk': 1,
    '2+1': 2,
    '3+kk': 3,
    '1+1': 1,
    '3+1': 3,
    '4+kk': 4,
    'rodinny': 5,
    '4+1': 4,
    '5+kk': 5,
    'vila': 6,
    '5+1': 5,
    '6 a vice': 6,
    'atypicky': 3,
    'chata': 2,
    'chalupa': 4,
    'ostatni': 1
}

overall_quality_mapping = {
    'K demolici': 1,
    'Projekt': 2,
    'Špatný': 3,
    'Ve výstavbě': 4,
    'Dobrý': 5,
    'Před rekonstrukcí': 6,
    'Po rekonstrukci': 7,
    'Novostavba': 8,
    'Velmi dobrý': 9,
}
