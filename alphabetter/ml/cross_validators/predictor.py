import pandas as pd
from alphabetter.ml.core import CrossValidator, Predictor


class PredictorCrossValidator(CrossValidator):
    @classmethod
    def _fit_score(cls, predictor: Predictor, train_df: pd.DataFrame, test_df: pd.DataFrame, /):
        predictor.fit(train_df)
        predictor.predict(test_df, inplace=True)
        return {
            'accuracy': test_df.prediction.accuracy(),
        }
