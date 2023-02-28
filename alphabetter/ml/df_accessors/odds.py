import pandas as pd
import numpy as np

from pandas.api.extensions import register_dataframe_accessor
from typing import Optional, Literal, overload
from alphabetter.ml.core import SINGLE_OUTCOMES, DOUBLE_OUTCOMES, DataFrameAccessor


@register_dataframe_accessor('odds')
class OddsDataFrameAccessor(DataFrameAccessor):
    @classmethod
    def _get_namespace(cls):
        return 'odds'

    @classmethod
    def _get_required_columns(cls):
        return {'1', 'X', '2'}

    def isna(self) -> pd.Series:
        return self.df.isna().any(axis=1)

    def clip(self, lower: Optional[float] = None, upper: Optional[float] = None, inplace: bool = False):
        df = self._df if inplace else self._df.copy()
        df[self.prefix + self.columns] = self.df.clip(lower, upper)
        if not inplace:
            return df

    @overload
    def dropna(self, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def dropna(self, *, inplace: Literal[True] = ...): ...

    def dropna(self, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        condition = self.isna()
        r = self._df.attrs.get('representativeness', 1.0) * (1 - condition.mean())
        if not inplace:
            df = self._df.drop(self._df[condition].index)
            df.attrs['representativeness'] = r
            return df
        self._df.drop(self._df[condition].index, inplace=True)
        self._df.attrs['representativeness'] = r

    @overload
    def drop_extreme(self, threshold: float = ..., *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def drop_extreme(self, threshold: float = ..., *, inplace: Literal[True] = ...): ...

    def drop_extreme(self, threshold: float = 15.0, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        condition = (self.df > threshold).any(axis=1)
        return self._df.drop(self._df[condition].index, inplace=inplace)

    def single(self) -> pd.DataFrame:
        return self.df[SINGLE_OUTCOMES]

    def double(self) -> pd.DataFrame:
        return self.df[DOUBLE_OUTCOMES]

    def most_probable_outcome(self) -> pd.Series:
        odds_df = self.df[SINGLE_OUTCOMES]
        most_probable_outcome_indices = odds_df.values.argmin(axis=1)
        data = np.array(SINGLE_OUTCOMES).take(most_probable_outcome_indices)
        most_probable_outcomes = pd.Series(
            name='most_probable_outcome',
            index=self._df.index,
            data=pd.Categorical(data, SINGLE_OUTCOMES),
        )
        return most_probable_outcomes

    def prediction(self) -> pd.DataFrame:
        odds = self.df[SINGLE_OUTCOMES]
        prediction_df = 1 / (odds - 1)
        prediction_df_sum = prediction_df.sum(axis=1)
        prediction_df = prediction_df.apply(lambda p: p / prediction_df_sum)
        return prediction_df

    def accuracy(self) -> float:
        df = self.dropna()
        return (df.odds.most_probable_outcome() == df.match.outcome()).mean()
