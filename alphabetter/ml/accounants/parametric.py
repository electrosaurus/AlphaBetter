import numpy as np
from alphabetter.ml.core import Accountant
from dataclasses import dataclass, KW_ONLY


@dataclass
class ParametricAccountant(Accountant):
    capital: float = 1.0
    min_investment_fraction: float = 0.0
    max_investment_fraction: float = 1.0
    _: KW_ONLY
    min_investment: float = 0.0
    max_investment: float = np.inf
    alpha: float = 1.0
    credit: float = 0.0

    def __post_init__(self):
        if self.credit > 0 and self.min_investment <= 0:
            raise ValueError('Min investment must be positive if a credit is allowed.')
        super().__init__(self.capital)

    def invest(self, expediency: float = 0.5, *, dry: bool = False) -> float:
        if expediency < 0 or expediency > 1:
            raise ValueError(f'Invalid profitability score {expediency}.')
        investment_fraction = self.min_investment_fraction + \
            (self.max_investment_fraction - self.min_investment_fraction) * expediency
        if self._balance <= 0:
            if self.min_investment > self.credit:
                investment = 0
            else:
                investment = self.min_investment
        else:
            w = (self._balance / self._capital) ** self.alpha
            investment = self._capital * w * investment_fraction
            investment = round(investment, -int(np.floor(np.log10(investment))) - 1 + 2)
            if investment < self.min_investment:
                investment = self.min_investment
            elif investment > self.max_investment:
                investment = self.max_investment
            if investment > self._capital + self.credit:
                if self.min_investment > self._capital + self.credit:
                    investment = 0
                else:
                    investment = self.min_investment
        if not dry:
            self._balance -= investment
        return investment
