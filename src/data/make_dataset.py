import os
import re
from typing import Any

import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer

from mappings import *
from utils import apply_column_mapping, GroupImputer, DataFrameTransformer


def preprocess_price(rentals: pd.DataFrame) -> pd.Series:
    return rentals.loc[:, 'Celková cena'].str.replace(r'\s', '', regex=True).astype(int)


def preprocess_efficiency(rentals: pd.DataFrame) -> pd.Series:
    return (
        rentals
        .loc[:, 'Energetická náročnost budovy']
        .apply(lambda x: re.match(r'Třída \w', x)[0] if isinstance(x, str) else x)
        .map(efficiency_mapping)
    )


def preprocess_floor(rentals: pd.DataFrame) -> pd.Series:
    def parse_floor(x: Any) -> float:
        if not x:
            return None
        elif isinstance(x, float) or isinstance(x, int):
            return x
        elif 'přízemí' in x:
            return 0

        parsed_x = str(x).split('.')[0]
        if 'včetně' in parsed_x:
            return int(parsed_x[0])

        try:
            x = int(parsed_x)
            x = max(x, -1)
            x = min(x, 20)
            return x
        except ValueError:
            return None

    return rentals.loc[:, 'Podlaží'].apply(lambda x: parse_floor(x))


def make_rentals(folder: str) -> pd.DataFrame:
    # Concatenate csv files into dataframe
    files = os.listdir(folder)
    dataframes = []
    for file in files:
        csv = pd.read_csv(f'{folder}/{file}')
        dataframes.append(csv)
    df = pd.concat(dataframes, axis=0, ignore_index=True)

    # Load only rentals (flats & houses)
    is_rental = df.loc[:, 'estate_rental_or_sell'] == 2
    is_flat_or_house = df.loc[:, 'estate_category_main_cb'].isin([1, 2])
    has_price = ~df.loc[:, 'Celková cena'].isna()
    rentals = (
        df
        .loc[is_rental & is_flat_or_house & has_price, columns]
        .drop_duplicates()
    )

    # Filter features
    rentals = pd.DataFrame().assign(
        area_m2=rentals.loc[:, 'Užitná plocha'],
        district=apply_column_mapping(rentals, 'estate_locality_district', locality_district_mapping),
        disposition=apply_column_mapping(rentals, 'estate_disposition', disposition_mapping),
        category=apply_column_mapping(rentals, 'estate_category_main_cb', category_mapping),
        furnishing=apply_column_mapping(rentals, 'Vybavení', furnishing_mapping),
        efficiency=preprocess_efficiency(rentals),
        floor=preprocess_floor(rentals),
        building_type=rentals.loc[:, 'Stavba'],
        building_state=rentals.loc[:, 'Stav objektu'],
        ownership=rentals.loc[:, 'Vlastnictví'],
        tram=rentals.loc[:, 'Tram'],
        elevator=rentals.loc[:, 'Výtah'].astype(float),
        theatre=rentals.loc[:, 'Divadlo'],
        cinema=rentals.loc[:, 'Kino'],
        groceries=rentals.loc[:, 'Obchod'],
        candy_shop=rentals.loc[:, 'Cukrárna'],
        veterinary=rentals.loc[:, 'Veterinář'],
        train=rentals.loc[:, 'Vlak'],
        pharmacist=rentals.loc[:, 'Lékárna'],
        atm=rentals.loc[:, 'Bankomat'],
        sports=rentals.loc[:, 'Sportoviště'],
        bus=rentals.loc[:, 'Bus MHD'],
        doctors=rentals.loc[:, 'Lékař'],
        school=rentals.loc[:, 'Škola'],
        kindergarten=rentals.loc[:, 'Školka'],
        pub=rentals.loc[:, 'Hospoda'],
        post_office=rentals.loc[:, 'Pošta'],
        restaurant=rentals.loc[:, 'Restaurace'],
        seven_eleven=rentals.loc[:, 'Večerka'],
        playground=rentals.loc[:, 'Hřiště'],
        price=preprocess_price(rentals),
    )
    return rentals[(rentals.area_m2 < 2000) & (rentals.price < 200000)]


def preprocess_rentals(dataframe: pd.DataFrame) -> pd.DataFrame:
    # EDA-based imputations
    constant_imputer = DataFrameTransformer(
        transformers=[(
            'constant_imputer',
            SimpleImputer(strategy='constant', fill_value='ostatni'),
            ['district', 'disposition']
        )],
        remainder='passthrough'
    )

    zero_imputer = DataFrameTransformer(
        transformers=[(
            'zero_imputer',
            SimpleImputer(strategy='constant', fill_value=0),
            ['furnishing', 'elevator']
        )],
        remainder='passthrough'
    )

    mode_imputer = DataFrameTransformer(
        transformers=[(
            'mode_imputer',
            SimpleImputer(strategy='most_frequent'),
            ['category', 'efficiency', 'floor', 'building_type', 'building_state', 'ownership']
        )],
        remainder='passthrough'
    )

    group_imputer = DataFrameTransformer(
        transformers=[(
            'mode_imputer',
            GroupImputer(group_cols=['disposition'], target='area_m2', metric='median'),
            ['disposition', 'area_m2']
        )],
        remainder='passthrough'
    )

    imputer = Pipeline(steps=[
        ('constant_imputer', constant_imputer),
        ('mode_imputer', mode_imputer),
        ('zero_imputer', zero_imputer),
        ('group_imputer', group_imputer)
    ])

    # Convert POI columns to vicinity indicators
    binarizer = DataFrameTransformer(
        transformers=[(
            'binarizer',
            FunctionTransformer(lambda distance: distance < 500),
            poi_columns
        )],
        remainder='passthrough'
    )

    # Transform and keep as dataframe
    preprocessor = Pipeline(steps=[
        ('imputer', imputer),
        ('binarizer', binarizer)
    ])
    return preprocessor.fit_transform(dataframe)
