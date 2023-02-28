import pandas as pd
import numpy as np

from alphabetter.ml.core import CrossValidator, Accountant
from datetime import timedelta
from typing import Optional


class AccountantCrossValidator(CrossValidator):
    _seconds_in_month = 60 * 60 * 24 * 365.25 / 12

    @staticmethod
    def _timedelta_months(td: Optional[timedelta]):
        if td is None:
            return np.nan
        return td.total_seconds() / AccountantCrossValidator._seconds_in_month

    @classmethod
    def _fit_score(cls, accountant: Accountant, train_df: pd.DataFrame, test_df: pd.DataFrame):
        accountant.fit(train_df)
        accountant.invest_serial(test_df, inplace=True)
        return {
            'annual_roc': test_df.accounting.annual_roc(),
            'roi': test_df.accounting.roi(),
            'doc_months': cls._timedelta_months(test_df.accounting.moc(2)),
            'qoc_months': cls._timedelta_months(test_df.accounting.moc(5)),
        }
