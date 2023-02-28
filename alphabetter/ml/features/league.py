import pandas as pd

from sklearn.preprocessing import OneHotEncoder
from alphabetter.ml.core import Features
from dataclasses import dataclass


@dataclass
class LeagueFeatures(Features):
    _league_columns = ['match.sport', 'match.league']

    def __post_init__(self):
        super().__init__()
        self._encoder = OneHotEncoder(sparse_output=False)

    def fit(self, df: pd.DataFrame, y=None):
        self._encoder.fit(df[self._league_columns])
        return self

    def transform(self, df: pd.DataFrame, y=None):
        return self._encoder.transform(df[self._league_columns])
