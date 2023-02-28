import numpy as np

from pandas.api.extensions import register_dataframe_accessor
from typing import Optional
from datetime import timedelta
from alphabetter.ml.core import DataFrameAccessor


@register_dataframe_accessor('accounting')
class AccountingDataFrameAccessor(DataFrameAccessor):
    _seconds_in_month = 60 * 60 * 24 * 365.25 / 12

    @classmethod
    def _get_namespace(cls):
        return 'accounting'

    @classmethod
    def _get_required_columns(cls):
        return {'investment'}

    def roi(self) -> float:
        ''' Return of investments. '''
        return self.profit.sum() / self.investment.sum()

    def roc(self) -> float:
        ''' Return of capital. '''
        capital = self.balance.iloc[0] + self.investment.iloc[0]
        final_balance = self.balance.iloc[-1]
        return final_balance / capital - 1

    def annual_roc(self) -> float:
        matches_per_year = round(365.25 * self._df.match.per_day())
        if matches_per_year >= len(self._df):
            return np.nan
        return self._df.iloc[:matches_per_year].accounting.roc()

    def moc(self, n: int, /, *, stable: bool = True) -> Optional[timedelta]:
        ''' Multiplication of capital '''
        capital = self.balance.iloc[0] + self.investment.iloc[0]
        if stable:
            index = self._df.loc[self.balance < capital * n].index[-1]
            n = self._df.index.get_loc(index) + 1
            if n >= len(self._df):
                return None
        else:
            index = self._df.loc[self.balance >= capital * n].index
            if index.empty:
                return None
            n = self._df.index.get_loc(index[0])
        return timedelta(days=n / self._df.match.per_day())
