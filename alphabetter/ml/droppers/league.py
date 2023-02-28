import pandas as pd

from alphabetter.ml.core import Dropper
from typing import Set


class LeagueDropper(Dropper):
    def __init__(self, allowed_leagues: Set[str]):
        self._allowed_leagues = allowed_leagues

    def drop(self, df: pd.DataFrame):
        new_df = df[df['match.league'].isin(self._allowed_leagues)].copy()
        r = len(new_df) / len(df)
        new_df.attrs['representativeness'] *= r
        new_df.attrs['match.per_day'] *= r
        return new_df
