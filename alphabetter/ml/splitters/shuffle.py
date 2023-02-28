import pandas as pd

from alphabetter.ml.core import Splitter
from typing import Optional, Iterable, Callable
from sklearn.model_selection import StratifiedShuffleSplit


class ShuffleSplitter(Splitter):
    @staticmethod
    def _stratify_default(df: pd.DataFrame):
        stratification_factors = [
            df.index.get_level_values('bookmaker').to_series(index=df.index) \
                .cat.add_categories('N/A').fillna('N/A'),
            df.match.sport,
            df.match.league,
            df.odds.isna(),
        ]
        strata_df = pd.concat(stratification_factors, axis=1)
        return strata_df.values

    def __init__(
        self,
        *,
        test_frac: float = 0.5,
        stratifier: Optional[Callable[[pd.DataFrame], Iterable]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._test_frac = test_frac
        self._stratifier = stratifier or self._stratify_default

    def _split(self, df: pd.DataFrame):
        if self._test_frac == 1:
            train_df = df.iloc[:1]
            test_df = df.sample(frac=1)
        else:
            splitter = StratifiedShuffleSplit(
                n_splits=1,
                test_size=self._test_frac,
                random_state=self._random_state,
            )
            labels = self._stratifier(df)
            train_index, test_index = next(splitter.split(df, labels))
            train_df = df.iloc[train_index]
            test_df = df.iloc[test_index]
        return train_df, test_df
