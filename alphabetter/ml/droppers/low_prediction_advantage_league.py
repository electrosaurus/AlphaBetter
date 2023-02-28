import pandas as pd

from alphabetter.ml.core import Dropper
from dataclasses import dataclass


@dataclass
class LowPredictionAdvantageLeagueDroppeer(Dropper):
    threshold: float = 0.0

    def __post_init__(self):
        self._high_prediction_advantage_leagues = None

    def fit(self, df: pd.DataFrame):
        lpa_df = df.groupby('match.league').apply(lambda league_df:
            league_df.prediction.accuracy() - league_df
                .assign(
                    **league_df.odds.prediction()
                    .add_prefix('prediction.')
                    .to_dict(orient='list')
                )
                .odds.dropna()
                .prediction.accuracy()
        )
        self._high_prediction_advantage_leagues = set(lpa_df[lpa_df >= self.threshold].index)

    def drop(self, df: pd.DataFrame):
        return df[df.match.league.isin(self._high_prediction_advantage_leagues)]
