import pandas as pd

from alphabetter.ml.core import Features
from dataclasses import dataclass
from sklearn.preprocessing import OneHotEncoder, MultiLabelBinarizer


@dataclass(kw_only=True)
class TeamFeatures(Features):
    _team_columns = ['match.home_team', 'match.away_team']

    encode_venue: bool = False
    sparse: bool = True

    def __post_init__(self):
        if self.encode_venue:
            self._encoder = OneHotEncoder(sparse_output=self.sparse)
        else:
            self._encoder = MultiLabelBinarizer(sparse_output=self.sparse)

    def fit(self, df: pd.DataFrame, y=None):
        self._encoder.fit(df[self._team_columns].values)
        return self

    def transform(self, df: pd.DataFrame, y=None):
        return self._encoder.transform(df[self._team_columns].values)
