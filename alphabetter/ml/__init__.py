''' Machine learning facilities. '''

import alphabetter.ml.df_accessors.accounting
import alphabetter.ml.df_accessors.bet
import alphabetter.ml.df_accessors.match
import alphabetter.ml.df_accessors.odds
import alphabetter.ml.df_accessors.prediction

from alphabetter.ml.core import (
    Outcome,
    SINGLE_OUTCOMES,
    DOUBLE_OUTCOMES,
    OUTCOMES,
    OPTIONAL_OUTCOMES,
    Features,
    Predictor,
    Splitter,
    CrossValidator,
    Better,
    Accountant,
    Dropper,
)
from alphabetter.ml.accounants.parametric import ParametricAccountant
from alphabetter.ml.betters.dummy import DummyBetter
from alphabetter.ml.betters.opc import OPCBetter
from alphabetter.ml.betters.roi_regression import ROIRegressionBetter
from alphabetter.ml.cross_validators.accountant import AccountantCrossValidator
from alphabetter.ml.cross_validators.better import BetterCrossValidator
from alphabetter.ml.cross_validators.predictor import PredictorCrossValidator
from alphabetter.ml.droppers.league import LeagueDropper
from alphabetter.ml.droppers.rare_team import RareTeamDropper
from alphabetter.ml.droppers.no_odds import NoOddsDropper
from alphabetter.ml.droppers.low_prediction_advantage_league import LowPredictionAdvantageLeagueDroppeer
from alphabetter.ml.features.team import TeamFeatures
from alphabetter.ml.features.team_kpi import TeamKPI, TeamKPIFeatures
from alphabetter.ml.features.league import LeagueFeatures
from alphabetter.ml.features.odds import OddsFeatures
from alphabetter.ml.features.opc import OPCFeatures
from alphabetter.ml.predictors.sklearn import SKLearnPredictor
from alphabetter.ml.predictors.odds import OddsPredictor
from alphabetter.ml.splitters.date import DateSplitter
from alphabetter.ml.splitters.odds import OddsSplitter
from alphabetter.ml.splitters.shuffle import ShuffleSplitter
from alphabetter.ml.methods import (
    select_dataset,
    read_dataset,
    read_match_dataset,
    read_predicted_match_dataset,
    read_bet_match_dataset,
    save_dataset,
    save_match_dataset,
    save_predicted_match_dataset,
    save_bet_match_dataset,
    describe_dataset,
    summarize_dataset,
)
