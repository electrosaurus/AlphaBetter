import pandas as pd
from alphabetter.ml.core import CrossValidator, Better


class BetterCrossValidator(CrossValidator):
    @classmethod
    def _fit_score(cls, better: Better, train_df: pd.DataFrame, test_df: pd.DataFrame):
        better.fit(train_df)
        better.bet(test_df, inplace=True)
        roi_per_bet = test_df.bet.roi_per_bet()
        return {
            'roi': roi_per_bet,
            'roi_per_week': roi_per_bet * test_df.match.per_week() * test_df.bet.rate(),
            'bet_rate': test_df.bet.rate(),
            'win_rate': test_df.bet.win_rate(),
            'bets_per_month': test_df.bet.per_month(),
        }
