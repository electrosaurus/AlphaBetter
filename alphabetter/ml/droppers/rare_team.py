import pandas as pd

from alphabetter.ml.core import Dropper


class RareTeamDropper(Dropper):
    def __init__(self, min_matches: int = 30):
        self._min_matches = min_matches
        self._ok_teams = set()

    def fit(self, df: pd.DataFrame):
        if df.empty:
            return
        match_df = df.match.df
        #match_df = match_df.query('`home_points` >= 0 and `away_points` >= 0') # type: ignore
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
        for team_index, team_df in team_match_df.groupby(['country', 'team']):
            if len(team_df) >= self._min_matches:
                self._ok_teams.add(team_index)

    def drop(self, df: pd.DataFrame):
        home_teams = df[['match.home_country', 'match.home_team']].apply(tuple, axis=1)
        away_teams = df[['match.away_country', 'match.away_team']].apply(tuple, axis=1)
        new_df = df[home_teams.isin(self._ok_teams) & away_teams.isin(self._ok_teams)].copy()
        r = len(new_df) / len(df)
        new_df.attrs['representativeness'] *= r
        new_df.attrs['match.per_day'] *= r
        return new_df
