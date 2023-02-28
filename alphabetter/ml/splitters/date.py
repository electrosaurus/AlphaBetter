import pandas as pd

from alphabetter.ml.core import Splitter
from datetime import date, datetime


class DateSplitter(Splitter):
    def __init__(self, threshold: date, **kwargs):
        super().__init__(**kwargs)
        self._threshold = datetime.combine(threshold, datetime.min.time())

    def _split(self, df: pd.DataFrame):
        train_condition = df.match.played_at < self._threshold
        train_df = df.loc[train_condition]
        test_df = df.loc[~train_condition]
        return train_df, test_df
