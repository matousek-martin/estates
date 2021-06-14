from typing import List, Dict

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.utils.validation import check_is_fitted
from sklearn.compose import ColumnTransformer


def apply_column_mapping(df: pd.DataFrame, column: str, mapping: Dict) -> pd.Series:
    return df.loc[:, column].map(mapping)


class GroupImputer(BaseEstimator, TransformerMixin):
    """Class used for imputing missing values in a pd.DataFrame
    using either mean or median of a group.

    Args:
        group_cols (list): List of columns used for calculating the aggregated value
        target (str): The name of the column to impute
        metric (str): The metric to be used for remplacement, can be one of ['mean', 'median']

    Returns:
        array-like: The array with imputed values in the target column
    """

    def __init__(self, group_cols, target, metric='mean'):
        assert metric in ['mean', 'median'], 'Unrecognized value for metric, should be mean/median'
        assert isinstance(group_cols, list), 'group_cols should be a list of columns'
        assert isinstance(target, str), 'target should be a string'

        self.group_cols = group_cols
        self.target = target
        self.metric = metric

    def fit(self, X, y=None):
        assert not pd.isnull(X[self.group_cols]).any(axis=None), 'Missing values in group_cols'

        impute_map = X.groupby(self.group_cols)[self.target].agg(self.metric) \
            .reset_index(drop=False)

        self.impute_map_ = impute_map

        return self

    def transform(self, X, y=None):
        # make sure that the imputer was fitted
        check_is_fitted(self, 'impute_map_')

        X = X.copy()

        for index, row in self.impute_map_.iterrows():
            ind = (X[self.group_cols] == row[self.group_cols]).all(axis=1)
            X.loc[ind, self.target] = X.loc[ind, self.target].fillna(row[self.target])

        return X.values


class DataFrameTransformer(ColumnTransformer):
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        columns = self._get_cols(X)
        dataframe = pd.DataFrame(super().transform(X), columns=columns, index=X.index)
        dataframe = dataframe.astype(X.dtypes.to_dict())
        return dataframe

    def fit_transform(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        columns = self._get_cols(X)
        dataframe = pd.DataFrame(super().fit_transform(X), columns=columns, index=X.index)
        dataframe = dataframe.astype(X.dtypes.to_dict())
        return dataframe

    def _get_cols(self, X: pd.DataFrame) -> List:
        self._validate_column_callables(X)
        selected_cols = self._columns[0]
        remainder_cols = [col for col in X.columns if col not in selected_cols]
        return selected_cols + remainder_cols

