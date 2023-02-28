import pandas as pd
import numpy as np

from pandas.api.extensions import register_dataframe_accessor
from typing import Any
from alphabetter.ml.core import SINGLE_OUTCOMES, DOUBLE_OUTCOMES
from alphabetter.ml.core import DataFrameAccessor


@register_dataframe_accessor('prediction')
class PredictionDataFrameAccessor(DataFrameAccessor):
    @classmethod
    def _get_namespace(cls):
        return 'prediction'

    @classmethod
    def _get_required_columns(cls):
        return {'1', 'X', '2'}

    def __getitem__(self, key) -> Any:
        match key:
            case '1X':
                return self['1'] + self['X']
            case '12':
                return self['1'] + self['2']
            case '2X':
                return self['2'] + self['X']
            case _:
                return super().__getitem__(key)

    def mixed_outcomes(self) -> pd.DataFrame:
        return self.df.assign(**{outcome: self[outcome] for outcome in DOUBLE_OUTCOMES})

    def most_probable_outcome(self) -> pd.Series:
        prediction_df = self.df[SINGLE_OUTCOMES]
        most_probable_outcome_indices = prediction_df.values.argmax(axis=1)
        data = np.array(SINGLE_OUTCOMES).take(most_probable_outcome_indices)
        most_probable_outcomes = pd.Series(
            name='most_probable_outcome',
            index=self._df.index,
            data=pd.Categorical(data, SINGLE_OUTCOMES),
        )
        return most_probable_outcomes

    def accuracy(self) -> float:
        return (self._df.match.outcome() == self.most_probable_outcome()).mean()

    def odds_probability_convolution(self, accuracy_factor: float = 1.0) -> pd.DataFrame:
        odds_df = self._df.odds.df
        probability_df = self.mixed_outcomes()
        opc_df = odds_df * probability_df ** accuracy_factor
        return opc_df
