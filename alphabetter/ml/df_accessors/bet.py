import pandas as pd
import numpy as np

from pandas.api.extensions import register_dataframe_accessor
from typing import Literal, overload
from alphabetter.ml.core import Accountant, Outcome, OUTCOMES
from alphabetter.ml.core import DataFrameAccessor


@register_dataframe_accessor('bet')
class BetDataFrameAccessor(DataFrameAccessor):
    @classmethod
    def _get_namespace(cls):
        return 'bet'

    @classmethod
    def _get_required_columns(cls):
        return {'outcome'}

    def is_winning(self) -> pd.Series:
        match_outcome = self._df.match.outcome()
        bet_outcome = self.outcome
        condition = (
            (bet_outcome == '1') & (match_outcome == '1') |
            (bet_outcome == 'X') & (match_outcome == 'X') |
            (bet_outcome == '2') & (match_outcome == '2') |
            (bet_outcome == '1X') & ((match_outcome == '1') | (match_outcome == 'X')) |
            (bet_outcome == '12') & ((match_outcome == '1') | (match_outcome == '2')) |
            (bet_outcome == '2X') & ((match_outcome == '2') | (match_outcome == 'X'))
        )
        condition.rename('is_winning', inplace=True)
        return condition

    def win_rate(self) -> float:
        bet_count = self.count()
        return np.nan if bet_count == 0 else self.is_winning().sum() / bet_count

    def rate(self) -> float:
        if self._df.empty:
            return np.nan
        return (self.outcome != '0').sum() / len(self._df)

    @overload
    def drop_null(self, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def drop_null(self, *, inplace: Literal[True] = ...): ...

    def drop_null(self, *, inplace: bool = False):
        condition = self.outcome == '0'
        return self._df.drop(self._df[condition].index, inplace=inplace)

    def count(self, *outcomes: Outcome):
        return sum(self.outcome.isin(outcomes or OUTCOMES))

    def per_month(self) -> float:
        return self.rate() * self._df.match.per_month()

    def roi(self) -> pd.Series:
        outcome_condition_df: pd.DataFrame = self._df.match.outcome_conditions.assign(**{'0': True})
        bet_df = pd.get_dummies(self.outcome).astype(bool)
        bet_win_condition_df = (outcome_condition_df & bet_df)
        odds_df: pd.DataFrame = self._df.odds.df.assign(**{'0': 1.})
        roi_df = odds_df * bet_win_condition_df - 1
        roi = roi_df.max(axis=1)
        roi.rename('roi', inplace=True)
        roi[~self._df.match.has_points()] = np.nan
        return roi

    def roi_per_bet(self) -> float:
        return self.roi().replace(0, np.nan).mean()
