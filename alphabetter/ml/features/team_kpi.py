import pandas as pd
import numpy as np

from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
from alphabetter.ml.core import Features
from dataclasses import dataclass
from typing import Literal, get_args, Set


TeamKPI = Literal[
    'win_rate',
    'draw_rate',
    'loss_rate',
    'shotout_win_rate',
    'shotout_loss_rate',
    'home_win_rate',
    'away_win_rate',
    'mean_points_ratio',
    'mean_points',
    'mean_opponent_points',
    'mean_points_diff',
    'mean_draw_points',
    'points_diff_growth',
]


@dataclass(kw_only=True)
class TeamKPIFeatures(Features):
    _seconds_in_year = 60 * 60 * 24 * 365
    _nanoseconds_in_year = 1e9 * _seconds_in_year
    _allowed_kpi = frozenset(get_args(TeamKPI))

    kpi: Set[TeamKPI]
    age_factor: float = 0.0

    def __post_init__(self):
        invalid_kpi = set(self.kpi) - self._allowed_kpi
        if invalid_kpi:
            raise KeyError(f'Invalid KPI: {", ".join(map("{!r}".format, invalid_kpi))}')
        self._team_kpi_df = None

    def fit(self, X: pd.DataFrame, y=None):
        if X.empty:
            return
        match_df: pd.DataFrame = X.match.df
        match_df = match_df.query('`home_points` >= 0 and `away_points` >= 0')
        home_match_df = match_df.drop(columns=['away_country', 'away_team']).rename(
            columns={
                'home_country': 'country',
                'home_team': 'team',
                'home_points': 'points',
                'away_points': 'opponent_points',
            }
        ).assign(venue='home')
        away_match_df = match_df.drop(columns=['home_country', 'home_team']).rename(
            columns={
                'away_country': 'country',
                'away_team': 'team',
                'home_points': 'opponent_points',
                'away_points': 'points',
            }
        ).assign(venue='away')
        team_match_df = pd.concat([home_match_df, away_match_df])
        self._team_kpi_df = (
            team_match_df
            .groupby(['country', 'team'])
            .apply(self._calc_team_kpi)
            .droplevel(2)
        )
        self._team_kpi_df.fillna(self._team_kpi_df.mean(), inplace=True)
        return self

    def transform(self, X: pd.DataFrame, y=None):
        if self._team_kpi_df is None:
            raise ValueError('Not fit.')
        home_team_kpi = X.merge(
            self._team_kpi_df,
            how='left',
            left_on=('match.home_country', 'match.home_team'),
            right_index=True,
        )[self._team_kpi_df.columns]
        away_team_kpi = X.merge(
            self._team_kpi_df,
            how='left',
            left_on=('match.away_country', 'match.away_team'),
            right_index=True,
        )[self._team_kpi_df.columns]
        team_kpi_diff = home_team_kpi - away_team_kpi
        return team_kpi_diff.fillna(0.0)

    def _calc_team_kpi(self, df: pd.DataFrame):
        age_seconds = (datetime.now() - df.played_at).dt.total_seconds() # type: ignore
        age_weights = (1 - self.age_factor) ** (age_seconds / self._seconds_in_year)
        recent_mask = df.played_at >= datetime.now() - timedelta(days=365*1)
        win_condition = df.points > df.opponent_points
        draw_condition = df.points == df.opponent_points
        loss_condition = df.points < df.opponent_points
        points_diff = df.points - df.opponent_points
        weighed_win_count = np.dot(age_weights, win_condition)
        weighed_draw_count = np.dot(age_weights, draw_condition)
        weighed_loss_count = np.dot(age_weights, loss_condition)
        weighed_match_count = age_weights.sum()
        kpi_dict = {}
        for kpi in self.kpi:
            match kpi:
                case 'win_rate':
                    kpi_value = weighed_win_count / weighed_match_count
                case 'draw_rate':
                    kpi_value = weighed_draw_count / weighed_match_count
                case 'loss_rate':
                    kpi_value = weighed_loss_count / weighed_match_count
                case 'shotout_win_rate':
                    if weighed_win_count > 0:
                        shotout_win_mask = win_condition & (df.opponent_points == 0)
                        weighted_shotout_win_count = np.dot(age_weights, shotout_win_mask)
                        kpi_value = weighted_shotout_win_count / weighed_win_count
                    else:
                        kpi_value = np.nan
                case 'shotout_loss_rate':
                    if weighed_loss_count > 0:
                        shotout_loss_mask = loss_condition & (df.points == 0)
                        weighted_shotout_loss_count = np.dot(age_weights, shotout_loss_mask)
                        kpi_value = weighted_shotout_loss_count / weighed_loss_count
                    else:
                        kpi_value = np.nan
                case 'home_win_rate':
                    if weighed_win_count > 0:
                        home_win_mask = win_condition & (df.venue == 'home')
                        weighted_home_win_count = np.dot(age_weights, home_win_mask)
                        kpi_value = weighted_home_win_count / weighed_win_count
                    else:
                        kpi_value = np.nan
                case 'away_win_rate':
                    if weighed_win_count > 0:
                        away_win_mask = win_condition & (df.venue == 'away')
                        weighted_away_win_count = np.dot(age_weights, away_win_mask)
                        kpi_value = weighted_away_win_count / weighed_win_count
                    else:
                        kpi_value = np.nan
                case 'mean_points_ratio':
                    weighted_points_sum = np.dot(df.points, age_weights)
                    total_points = df.points + df.opponent_points
                    weighted_total_points_sum = np.dot(total_points, age_weights)
                    kpi_value = weighted_points_sum / weighted_total_points_sum
                case 'mean_points':
                    weighted_points_sum = np.dot(df.points, age_weights)
                    kpi_value = weighted_points_sum / weighed_match_count
                case 'mean_opponent_points':
                    weighted_opponent_points_sum = np.dot(df.opponent_points, age_weights)
                    kpi_value = weighted_opponent_points_sum / weighed_match_count
                case 'mean_points_diff':
                    weighted_points_diff = np.dot(points_diff, age_weights)
                    kpi_value = weighted_points_diff / weighed_match_count
                case 'mean_draw_points':
                    kpi_value = (df.points * age_weights)[draw_condition].mean()
                case 'points_diff_growth':
                    # TODO
                    if sum(recent_mask) >= 5:
                        points_diff_lr = LinearRegression()
                        points_diff_lr.fit(
                            df.loc[recent_mask, ['played_at']],
                            points_diff[recent_mask],
                        )
                        kpi_value = points_diff_lr.coef_[0] * self._nanoseconds_in_year # type: ignore
                    else:
                        kpi_value  = 0.0
                case _:
                    raise KeyError(kpi)
            kpi_dict[kpi] = kpi_value
        return pd.DataFrame([kpi_dict])
