import pandas as pd

from alphabetter.ml.core import Features
from dataclasses import dataclass


@dataclass
class OPCFeatures(Features):
    accuracy_factor: float = 1.0

    def __post_init__(self):
        self._mean_opc = None

    def fit(self, X: pd.DataFrame, y=None):
        self._mean_opc = X.prediction.odds_probability_convolution(self.accuracy_factor).mean()
        return self

    def transform(self, X: pd.DataFrame, y=None):
        return X.prediction.odds_probability_convolution(self.accuracy_factor).fillna(self._mean_opc)
