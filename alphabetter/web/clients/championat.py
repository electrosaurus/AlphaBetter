import aiohttp
import logging
import pandas as pd
import asyncio

from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, TextIO, Set
from dataclasses import dataclass, field
from alphabetter.core.model import Sport, Country
import yaml


logger = logging.getLogger(__name__)


class ChampionatClient():
    ''' Asyncronous HTTP client to download tournaments from https://www.championat.com. '''

    _url_pattern = 'https://www.championat.com/{sport}/{league}/tournament/{tournament}/calendar/'
    _teams_prefixes = ['F1', 'SF1', 'SF2']

    @dataclass(frozen=True, slots=True)
    class Config:
        ''' Stores the rules of data transformation between https://www.championat.com and
            `ChampionatClient`. '''

        sport_api_params: Dict[Sport, str]
        ''' Sport identifiers used in championat's URLs. '''
    
        league_api_params: Dict[Sport, Dict[str, str]]
        ''' League identifiers used in championat's URLs. '''

        tournament_api_params: Dict[Sport, Dict[str, Dict[str, str]]]
        ''' Tournament identifiers used in championat's URLs. '''

        league_countries: Dict[Sport, Dict[str, Country]]
        ''' Countries of leagues. '''

        team_aliases: Dict[Sport, Dict[Country, Dict[str, Set[str]]]]
        ''' Same teams found by different names in https://www.championat.com. '''
    
        _team_alias_index: Dict[Sport, Dict[Country, Dict[str, str]]] = field(
            init=False, repr=True, compare=False, default_factory=dict)

        @classmethod
        def from_yaml(cls, stream: TextIO, /):
            ''' Load from a YAML file. '''
            data = yaml.safe_load(stream)
            api_params = data['api_params']
            return cls(
                sport_api_params={
                    Sport.from_string(sport_name): sport_api_params
                    for sport_name, sport_api_params in api_params['sports'].items()
                },
                league_api_params={
                    Sport.from_string(sport_name): league_api_params
                    for sport_name, league_api_params in api_params['leagues'].items()
                },
                tournament_api_params={
                    Sport.from_string(sport_name): league_tournament_api_params
                    for sport_name, league_tournament_api_params in api_params['tournaments'].items()
                },
                league_countries={
                    Sport.from_string(sport_name): {
                        league_name: Country(country_iso2)
                        for league_name, country_iso2 in league_countries.items()
                    }
                    for sport_name, league_countries in data['league_countries'].items()
                },
                team_aliases={
                    Sport.from_string(sport_name): {
                        Country(country_name): {
                            name: set(aliases)
                            for name, aliases in team_aliases.items()
                        }
                        for country_name, team_aliases in country_team_aliases.items()
                    }
                    for sport_name, country_team_aliases in data['team_aliases'].items()
                },
            )

        def __post_init__(self):
            for sport, country_team_alieases in self.team_aliases.items():
                self._team_alias_index[sport] = {}
                for country, team_aliases in country_team_alieases.items():
                    self._team_alias_index[sport][country] = {}
                    for team_name, team_aliases in team_aliases.items():
                        for team_alias in team_aliases:
                            self._team_alias_index[sport][country][team_alias] = team_name

        def get_sports(self):
            ''' Get all sports supported by the client. '''
            return list(self.tournament_api_params)

        def get_league_names(self, sport: Sport):
            ''' Get names of all leagues of a given sport supported by the client. '''
            try:
                return list(self.tournament_api_params[sport])
            except KeyError as error:
                raise KeyError(f'No leagues found for sport "{sport}".') from error

        def get_seasons(self, league_sport: Sport, league_name: str):
            ''' Get all seasons of a given league supported by the client. '''
            try:
                return list(self.tournament_api_params[league_sport][league_name])
            except KeyError as error:
                raise KeyError(f'No seasons found for {league_sport:l} league "{league_name}".') from error
                
        def get_sport_api_param(self, sport: Sport):
            ''' Get the identifier of a given sport used in championat's URLs. '''
            try:
                return self.sport_api_params[sport]
            except KeyError as error:
                raise KeyError(f'No API param found for sport "{sport}".') from error

        def get_league_api_param(self, league_sport: Sport, league_name: str):
            ''' Get the identifier of a given league used in championat's URLs. '''
            try:
                return self.league_api_params[league_sport][league_name]
            except KeyError as error:
                raise KeyError(f'No API param found for {league_sport:l} league "{league_name}".') from error

        def get_tournament_api_param(self, league_sport: Sport, league_name: str, season: str):
            ''' Get the identifier of a given season of a given league used in championat's URLs. '''
            try:
                return self.tournament_api_params[league_sport][league_name][season]
            except KeyError as error:
                raise KeyError(f'No API param found for the {season} tournament of {league_sport:l} '
                    'league "{league_name}".') from error

        def get_league_country(self, league_sport: Sport, league_name: str):
            ''' Get the country of a given league. '''
            try:
                return self.league_countries[league_sport][league_name]
            except KeyError:
                return None

        def find_team_name_by_alias(self, sport: Sport, country: Country, alias: str):
            ''' Find a team of given sport and country by it's alias used on championat. '''
            try:
                return self._team_alias_index[sport][country][alias]
            except KeyError:
                return None
    
    def __init__(
        self,
        config: Config,
        http_session: aiohttp.ClientSession,
        *,
        concurrency_limit: int = 10,
    ):
        '''
        Parameters
        ----------
        config : `ChampionatClient.Config`
            The rules of data transformation between https://www.championat.com and the client.
        http_session : `aiohttp.ClientSession`
            Client session used for requests to https://www.championat.com.
        concurrency_limit : `int`, default `10`
            Max number of concurrent requests to https://www.championat.com.
        '''
        self._config = config
        self._http_session = http_session
        self._request_semaphore = asyncio.Semaphore(concurrency_limit)

    @property
    def config(self):
        ''' The rules of data transformation between https://www.championat.com and the client. '''
        return self._config

    def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.drop(df.columns[[0,1,-1]], axis=1, inplace=True) # type: ignore
        df.rename(
            columns = {
                'Тур': 'tour',
                'Дата, время': 'played_at',
                df.columns[-2]: 'teams',
                'Счет': 'points',
            },
            inplace = True,
        )
        if 'Группа' in df.columns:
            df.rename(columns={'Группа': 'group'}, inplace=True)
        return df

    async def download_tournament(
        self,
        sport: Sport,
        league_name: str,
        season: str,
    ) -> pd.DataFrame:
        '''
        Download a tournament from https://www.championat.com.

        Parameters
        ----------
        sport : `Sport`
            League's sport.
        league_name : `str`
            Name of the tournament's league.
        season : `str`
            Tournament's season.

        Returns
        -------
        A data frame with each row corresponding to a single tournament's match and the following
        columns:
            `played_at` : `datetime`
                Date and time of the beginning of a match.
            `home_country` : `str`
                Home team's country (ISO-2).
            `away_country` : `str`
                Away team's country (ISO-2).
            `home_team` : `str`
                Home team's name.
            `away_team` : `str`
                Away team's name.
            `home_points`: int
                Points gained by a home team.
            `away_points`: int
                Points gained by an away team.

        Notes
        -----
        When a team is disqualified, match points have negative values.
        '''
        async with self._request_semaphore:
            headers = {'User-Agent': 'Mozilla/5.0'}
            url = self._url_pattern.format(
                sport=self._config.get_sport_api_param(sport),
                league=self._config.get_league_api_param(sport, league_name),
                tournament=self._config.get_tournament_api_param(sport, league_name, season),
            )
            async with self._http_session.get(url, headers=headers) as response:
                soup = BeautifulSoup(await response.text(), 'html.parser')
                table_html = (
                    soup.find('table', {'class':
                        'table table-stripe-with-class table-row-hover stat-results__table'}) or
                    soup.find('table', {'class':
                        'table table-stripe-with-class table-row-hover stat-results__table _is-active'})
                )
                df = pd.read_html(str(table_html))[0]
                df = self._prepare_df(df)
                country = self._config.get_league_country(sport, league_name)
                df['home_country'] = df['away_country'] = country and str(country)
                df[['home_team','away_team']] = df.apply(
                    self._parse_teams,
                    axis=1,
                    result_type='expand',
                    sport=sport,
                    country=country,
                )
                df.dropna(subset=['home_team', 'away_team'], inplace=True)
                df[['home_points', 'away_points']] = df.apply(
                    self._parse_points,
                    axis=1,
                    result_type='expand',
                )
                df.dropna(subset=['home_points', 'away_points'], inplace=True)
                df.played_at = df.played_at.apply(self._parse_played_at) # type: ignore
                df.dropna(subset='played_at', inplace=True)
                df.drop(columns=['tour', 'points', 'teams'], inplace=True)
                if 'group' in df.columns:
                    df.drop(columns=['group'], inplace=True)
                return df

    @staticmethod
    def _parse_played_at(raw_played_at: str):
        try:
            return datetime.strptime(raw_played_at, '%d.%m.%Y %H:%M')
        except ValueError:
            pass
        try:
            return datetime.strptime(raw_played_at, '%d.%m.%Y')
        except ValueError:
            logger.error(f'Invalid match time "{raw_played_at}".')
            return None

    def _parse_teams(self, match: pd.Series, sport: Sport, country: Country):
        if 'group' in match.index:
            prefix = f'{match.group}, Тур {match.tour} {match.played_at}'
        else:
            prefix = f'Тур {match.tour} {match.played_at}'
        for teams_prefix in self._teams_prefixes:
            teams_prefix += ',  '
            if match.teams.startswith(teams_prefix):
                prefix = teams_prefix + prefix
                break
        if prefix != match.teams[:len(prefix)]:
            logger.error(f'Invalid teams format: prefix = "{prefix}"; teams = "{match.teams}".')
            return None, None
        home_team, away_team = match.teams[len(prefix)+1:].split(' – ')
        home_team = self._config.find_team_name_by_alias(sport, country, home_team) or home_team
        away_team = self._config.find_team_name_by_alias(sport, country, away_team) or away_team
        return home_team, away_team

    @staticmethod
    def _parse_points(match: pd.Series):
        raw_home_points, sep, raw_away_points = match.points.split()[:3]
        if sep != ':':
            logger.error(f'Invalid points separator "{sep}" in "{match.points}".')
        home_points = ChampionatClient._encode_points(raw_home_points)
        away_points = ChampionatClient._encode_points(raw_away_points)
        return home_points, away_points

    @staticmethod
    def _encode_points(raw_points: str):
        match raw_points:
            case raw_point if raw_points.isnumeric():
                return int(raw_points)
            case '-':
                return -1
            case '+':
                return -2
            case '–':
                return -3
            case _:
                logger.error(f'Invalid points "{raw_point}".')
                return None
            
