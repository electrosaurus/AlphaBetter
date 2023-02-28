import pandas as pd

from alphabetter.ml.core import Splitter


class OddsSplitter(Splitter):
    def _split(self, df: pd.DataFrame):
        train_condition = df.odds.isna()
        train_df = df.loc[train_condition]
        test_df = df.loc[~train_condition]
        return train_df, test_df
