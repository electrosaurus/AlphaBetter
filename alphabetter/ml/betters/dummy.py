from alphabetter.ml.core import Better, Outcome
import numpy as np
import pandas as pd
from typing import overload, Literal
from dataclasses import dataclass


@dataclass
class DummyBetter(Better):
    ''' Allways bets on the same outcome. '''

    outcome: Outcome = '1'

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[False]) -> pd.DataFrame: ...

    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[True]): ...

    def bet(self, df: pd.DataFrame, /, *, inplace: bool = False):
        n = len(df)
        bet_df = pd.DataFrame(
            index=df.index,
            data={
                'outcome': np.full(n, self.outcome),
                'expediency': np.full(n, 0.5),
            },
        )
        if not inplace:
            return bet_df
        df[bet_df.add_prefix('bet.').columns] = bet_df
