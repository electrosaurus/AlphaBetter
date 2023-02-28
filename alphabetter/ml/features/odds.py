import pandas as pd

from typing import List
from alphabetter.core import Outcome, SINGLE_OUTCOMES
from alphabetter.ml.core import Features
from dataclasses import dataclass, field


@dataclass(kw_only=True)
class OddsFeatures(Features):
    reversed: bool = False
    outcomes: List[Outcome] = field(default_factory=lambda: SINGLE_OUTCOMES)

    def __post_init__(self):
        self._odds_df_mean = None

    def fit(self, df: pd.DataFrame, y=None):
        self._odds_df_mean = df.odds.df[self.outcomes].mean()
        return self

    def transform(self, df: pd.DataFrame, y=None):
        odds_df = df.odds.df[self.outcomes].fillna(1.0) #fillna(self._odds_df_mean)
        return 1 / odds_df if self.reversed else odds_df
