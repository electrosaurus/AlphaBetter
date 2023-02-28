import pandas as pd

from alphabetter.ml.core import Predictor


class OddsPredictor(Predictor):
    def predict(self, df: pd.DataFrame, inplace: bool = False):
        prediction_df = df.odds.prediction()
        if not inplace:
            return prediction_df
        df['prediction.' + prediction_df.columns] = prediction_df

    def __repr__(self):
        return 'OddsPredictor()'
