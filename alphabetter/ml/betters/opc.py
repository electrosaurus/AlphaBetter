import numpy as np
import pandas as pd

from alphabetter.core import *
from alphabetter.ml.core import Better, Predictor
from scipy.optimize import minimize_scalar
from typing import overload, Literal, List, Optional
from dataclasses import dataclass, field


@dataclass(kw_only=True)
class OPCBetter(Better):
    bet_rate: float = 0.03
    win_rate: float = 0.55
    expediency_contrast: float = 1.0
    outcomes: List[Outcome] = field(default_factory=lambda: OUTCOMES)
    accuracy_factor: Optional[float] = None
    accuracy_factor_tolerance: float = 0.05
    max_odds: float = np.inf
    predictor: Optional[Predictor] = None

    def __post_init__(self):
        self._min_opc = np.nan
        self._max_opc = np.nan
        self._accuracy_factor = np.nan

    def fit(self, df: pd.DataFrame, /):
        self._allowed_leagues = set(zip(df.match.sport, df.match.league))
        bet_df = df.copy()
        if self.predictor:
            self.predictor.fit(bet_df)
            self.predictor.predict(df, inplace=True)
        bet_df.odds.dropna(inplace=True)
        def set_accuracy_factor(accuracy_factor):
            self._accuracy_factor = accuracy_factor
            opc_df = bet_df.prediction.odds_probability_convolution(accuracy_factor)[self.outcomes]
            opc = opc_df.max(axis=1)
            self._min_opc = np.quantile(opc, 1 - self.bet_rate)
            self._max_opc = opc.max()
        if self.accuracy_factor is not None:
            set_accuracy_factor(self.accuracy_factor)
        else:
            bet_df.odds.clip(upper=self.max_odds)
            # TODO clip odds
            def win_rate_error(accuracy_factor):
                set_accuracy_factor(accuracy_factor)
                self.bet(bet_df, inplace=True)
                return abs(bet_df.bet.win_rate() - self.win_rate)
            accuracy_factor = minimize_scalar(win_rate_error, tol=self.accuracy_factor_tolerance).x
            set_accuracy_factor(accuracy_factor)

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[True] = ...): ...

    def bet(self, df: pd.DataFrame, /, *, inplace: bool = False):
        if self.predictor:
            self.predictor.predict(df, inplace=True)
        if self._min_opc is None or self._max_opc is None:
            raise RuntimeError('Better is not fit.')
        opc_matrix: np.ndarray = (
            df.odds.clip(upper=self.max_odds)
            .prediction.odds_probability_convolution(self._accuracy_factor)[self.outcomes]
            .fillna(-np.inf)
            .values
        )
        max_opc_indices = opc_matrix.argmax(axis=1)
        opc = np.take_along_axis(opc_matrix, max_opc_indices.reshape(-1,1), axis=1).reshape(-1)
        bet_conditions = opc >= self._min_opc
        bet_outcomes = np.where(bet_conditions, np.array(self.outcomes).take(max_opc_indices), '0')
        f = (opc - self._min_opc) / (self._max_opc - self._min_opc)
        expediency = np.where(bet_conditions, np.clip(f, 0, 1) ** (1 / self.expediency_contrast), np.nan)
        bet_df_data = {'outcome': pd.Categorical(bet_outcomes, OPTIONAL_OUTCOMES), 'expediency': expediency}
        bet_df = pd.DataFrame(index=df.index, data=bet_df_data)
        if not inplace:
            return bet_df
        df['bet.' + bet_df.columns] = bet_df
