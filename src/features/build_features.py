from typing import List

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

from mappings import *
from utils import apply_column_mapping


def number_of_pois(df: pd.DataFrame, columns: List):
    return df.loc[:, columns].sum(axis=1)


def build_features(df: pd.DataFrame):
    df = df.assign(
        num_pois=number_of_pois(df, poi_columns),
        rooms=apply_column_mapping(df, 'disposition', room_mapping),
        overall_quality=apply_column_mapping(df, 'building_state', overall_quality_mapping)
    )

    encoder = ColumnTransformer([(
        'encoder',
        OneHotEncoder(),
        ['district', 'disposition', 'category', 'building_type', 'building_state', 'ownership'])],
        remainder='passthrough'
    )

    return pd.DataFrame(encoder.fit_transform(df), columns=encoder.get_feature_names())
