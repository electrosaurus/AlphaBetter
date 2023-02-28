import numpy as np
import pandas as pd

from alphabetter.core import *
from alphabetter.ml.core import Better, Features, Predictor
from sklearn.base import TransformerMixin, RegressorMixin
from sklearn.pipeline import make_union, Pipeline
from typing import overload, Literal, List, Optional
from dataclasses import dataclass, field
from sklearn.compose import make_column_selector, make_column_transformer
from sklearn.preprocessing import OneHotEncoder


@dataclass(kw_only=True)
class ROIRegressionBetter(Better):
    bet_rate: float
    features: List[Features]
    regressor: RegressorMixin
    scaler: Optional[TransformerMixin] = None
    outcomes: List[Outcome] = field(default_factory=lambda: OUTCOMES)
    predictor: Optional[Predictor]

    def __post_init__(self):
        pipeline_steps = []
        features = (
            make_column_transformer(
                (
                    OneHotEncoder(sparse_output=False), # type: ignore
                    make_column_selector('bet.outcome'),
                ),
            ),
            *self.features,
        )
        pipeline_steps.append(('feature_extractor', make_union(*features)))
        if self.scaler:
            pipeline_steps.append(('scaler', self.scaler))
        pipeline_steps.append(('regressor', self.regressor))
        self._pipeline: Pipeline = Pipeline(pipeline_steps)

    def fit(self, df: pd.DataFrame, /):
        if self.predictor:
            self.predictor.fit(df)
            self.predictor.predict(df, inplace=True)
        df = df.odds.dropna() # type: ignore
        df['bet.outcome'] = [self.outcomes] * len(df)
        df = df.explode('bet.outcome')
        self._pipeline.fit(df, df.bet.roi())

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[True] = ...): ...

    def bet(self, df: pd.DataFrame, /, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        if self.predictor:
            self.predictor.predict(df, inplace=True)
        df['bet.outcome'] = [self.outcomes] * len(df)
        exploded_df = df.explode('bet.outcome')
        exploded_expected_roi = self._pipeline.predict(exploded_df)
        expected_roi_table = exploded_expected_roi.reshape(-1, len(self.outcomes))
        expected_roi = expected_roi_table.max(axis=1)
        bet_condition = expected_roi >= np.quantile(expected_roi, 1 - self.bet_rate)
        bet_outcomes = np.where(
            bet_condition,
            np.array(self.outcomes).take(expected_roi_table.argmax(axis=1)),
            '0',
        )
        bet_df = pd.DataFrame(
            data={
                'outcome': pd.Categorical(bet_outcomes, OPTIONAL_OUTCOMES),
                'expediency': 0.5,
            },
            index=df.index,
        )
        if not inplace:
            return bet_df
        df['bet.' + bet_df.columns] = bet_df
