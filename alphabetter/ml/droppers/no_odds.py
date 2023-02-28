import pandas as pd

from alphabetter.ml.core import Dropper
from typing import Set


class NoOddsDropper(Dropper):
    def drop(self, df: pd.DataFrame):
        return df.odds.dropna()
