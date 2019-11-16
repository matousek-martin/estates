import pandas as pd
import numpy as np

from parsing_functions import (
    rename_mapping,
    feature_transformation_mapping,
    features,
    target,
    type_mapping,
)

raw_df = pd.read_csv("sreality_output_16112019.csv")
raw_df = raw_df.rename(rename_mapping, axis=1)

raw_df.loc[:, feature_transformation_mapping.keys()] = raw_df.apply(
    feature_transformation_mapping
)
raw_df = raw_df.loc[:, features + target]

raw_df = raw_df.astype(type_mapping)

raw_df.to_csv("modeling_data.csv", index=False)

