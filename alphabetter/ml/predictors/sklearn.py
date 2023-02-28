from __future__ import annotations

import pandas as pd
import sklearn as sk
import sklearn.base

from alphabetter.ml.core import Predictor
from sklearn.pipeline import Pipeline, make_union
from typing import Optional, List
from alphabetter.ml.core import Features, SINGLE_OUTCOMES
from dataclasses import dataclass


@dataclass(kw_only=True)
class SKLearnPredictor(Predictor):
    features: List[Features]
    scaler: Optional[sklearn.base.TransformerMixin] = None
    classifier: sk.base.ClassifierMixin
    raw: bool = False

    def __post_init__(self):
        pipeline_steps = []
        pipeline_steps.append(('feature_extractor', make_union(*self.features)))
        if self.scaler:
            pipeline_steps.append(('scaler', self.scaler))
        pipeline_steps.append(('classifier', self.classifier))
        self._pipeline: Pipeline = Pipeline(pipeline_steps)

    def determine(self, random_state: int, /):
        if hasattr(self._pipeline['classifier'], 'random_state'):
            self._pipeline['classifier'].random_state = random_state # type: ignore

    def fit(self, df: pd.DataFrame, y=None):
        self._pipeline.fit(df, df.match.outcome().cat.codes)

    def reset(self):
        self._pipeline = sklearn.base.clone(self._pipeline) # type: ignore

    def predict(self, df: pd.DataFrame, inplace: bool = False):
        probas = self._pipeline.predict_proba(df)
        if self.raw:
            return probas
        if probas.shape[1] == 3:
            prediction_df = pd.DataFrame(
                index=df.index,
                columns=SINGLE_OUTCOMES,
                data=probas,
                dtype='f4',
            )
        else:
            prediction_df = pd.DataFrame(
                index=df.index,
                columns=['1', '2'],
                data=probas,
                dtype='f4',
            )
            prediction_df.insert(1, 'X', 0.0) # type: ignore
        if not inplace:
            return prediction_df
        df['prediction.' + prediction_df.columns] = prediction_df
