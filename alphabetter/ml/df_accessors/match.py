import pandas as pd
import numpy as np

from pandas.api.extensions import register_dataframe_accessor
from typing import Optional, Literal, overload
from alphabetter.ml.core import SINGLE_OUTCOMES
from alphabetter.ml.core import DataFrameAccessor


@register_dataframe_accessor('match')
class MatchDataFrameAccessor(DataFrameAccessor):
    @classmethod
    def _get_namespace(cls):
        return 'match'

    @classmethod
    def _get_required_columns(cls):
        return {} # TODO

    @property
    def outcome_conditions(self) -> pd.DataFrame:
        data = {
            '1': self.home_points > self.away_points,
            'X': self.home_points == self.away_points,
            '2': self.home_points < self.away_points,
            '1X': self.home_points >= self.away_points,
            '12': self.home_points != self.away_points,
            '2X': self.home_points <= self.away_points,
        }
        outcome_condition_df = pd.DataFrame(
            index=self._df.index,
            data=data,
        )
        return outcome_condition_df

    def outcome(self) -> pd.Series:
        single_outcome_condition_df = self.outcome_conditions[SINGLE_OUTCOMES]
        data = np.select(single_outcome_condition_df.values.T, SINGLE_OUTCOMES) # type: ignore
        outcomes = pd.Series(
            name='outcome',
            index=self._df.index,
            data=pd.Categorical(data, SINGLE_OUTCOMES),
        )
        return outcomes

    def per_day(self) -> float:
        if 'match.per_day' in self._df.attrs:
            return self._df.attrs['match.per_day']
        if len(self._df) < 2:
            return np.nan
        played_at = self._df.match.played_at.dropna()
        days = (played_at.max() - played_at.min()).total_seconds() / (3600 * 24)
        return len(self._df) / days

    def per_week(self) -> float:
        return self.per_day() * 7

    def per_month(self) -> float:
        return self.per_day() * 365.25 / 12

    @overload
    def drop_without_points(self, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def drop_without_points(self, *, inplace: Literal[True] = ...): ...

    def drop_without_points(self, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        condition = (self.home_points < 0) | (self.away_points < 0)
        return self._df.drop(self._df[condition].index, inplace=inplace)

    @overload
    def drop_draws(self, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def drop_draws(self, *, inplace: Literal[True] = ...): ...

    def drop_draws(self, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        condition = self.home_points == self.away_points
        return self._df.drop(self._df[condition].index, inplace=inplace)

    def has_points(self) -> pd.Series:
        return pd.Series(
            name='has_points',
            index=self._df.index,
            data=(self.home_points >= 0) & (self.away_points >= 0),
        )

    @property
    def df(self) -> pd.DataFrame:
        df = super().df
        df.reset_index('bookmaker', drop=True, inplace=True)
        df.drop_duplicates(inplace=True)
        return df
