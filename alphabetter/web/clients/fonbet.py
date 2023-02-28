from dataclasses import dataclass
from typing import Dict, TextIO, Any
from datetime import datetime, timedelta
import dateutil.parser
import pandas as pd
import yaml
import logging
from alphabetter.core.logging import LOG_LEVEL_STATUS
import re
import numpy as np

from alphabetter.core.model import Sport
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


logger = logging.getLogger(__name__)


class FonbetClient:
    _url_pattern = 'https://www.fon.bet/sports/{sport}/{league}/'
    _match_data_offset = 17
    _match_data_chunk_size = 14
    _html_class_name = 'sport-section-virtual-list--6lYPYe'
    _match_data_substrings_to_remove = ['МАТЧ ДНЯ']
    _month_translations = {
        'января': 'of January',
        'февраля': 'of February',
        'марта': 'of March',
        'апреля': 'of April',
        'мая': 'of May',
        'июня': 'of June',
        'июля': 'of July',
        'августа': 'of August',
        'сентября': 'of September',
        'октября': 'of October',
        'ноября': 'of November',
        'декабря': 'of December',
    }

    @dataclass
    class Config:
        sport_api_params: Dict[Sport, str]
        league_api_params: Dict[Sport, Dict[str, str]]

        def get_sport_api_param(self, sport: Sport):
            try:
                return self.sport_api_params[sport]
            except KeyError as error:
                raise KeyError(f'No API param found for sport "{sport}".') from error

        def get_league_api_param(self, league_sport: Sport, league_name: str):
            try:
                return self.league_api_params[league_sport][league_name]
            except KeyError as error:
                raise KeyError(f'No API param found for {league_sport:l} league "{league_name}".') from error

        def get_sports(self):
            return list(self.league_api_params)

        def get_league_names(self, sport: Sport):
            try:
                return list(self.league_api_params[sport])
            except KeyError as error:
                raise KeyError(f'No leagues found for sport "{sport}".') from error

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
                    Sport.from_string(sport_name): {
                        league_name: league_api_param
                        for league_name, league_api_param in league_api_params.items()
                        if league_api_param
                    } for sport_name, league_api_params in api_params['leagues'].items()
                },
            )

    def __init__(
        self,
        config: Config,
        webdriver: webdriver.Firefox,
        *,
        timeout: timedelta = timedelta(seconds=10),
    ):
        self._config = config
        self._webdriver = webdriver
        self._timeout = timeout

    def download_upcoming_matches(self, sport: Sport, league_name: str):
        url = self._url_pattern.format(
            sport=self._config.get_sport_api_param(sport),
            league=self._config.get_league_api_param(sport, league_name),
        )
        logger.log(LOG_LEVEL_STATUS, f'Downloading the page of {sport:_} league "{league_name}"...')
        self._webdriver.get(url)
        logger.log(LOG_LEVEL_STATUS, f'Waiting for the matches of {sport:_} league "{league_name}" to load...')
        upcoming_matches_element = (
            WebDriverWait(self._webdriver, self._timeout.total_seconds())
            .until(EC.presence_of_element_located((By.CLASS_NAME, self._html_class_name)))
        )
        logger.log(LOG_LEVEL_STATUS, f'Parsing the matches of {sport:_} league "{league_name}"...')
        upcoming_match_dataset = self._parse_matches(upcoming_matches_element.text) # type: ignore
        return upcoming_match_dataset


    def _parse_match_played_at(self, raw_played_at, /):
        raw_played_at = raw_played_at.replace('Сегодня в', str(datetime.now().date()))
        raw_played_at = raw_played_at.replace('Завтра в', str((datetime.now() + timedelta(days=1)).date()))
        for ru_month, en_month in self._month_translations.items():
            raw_played_at = raw_played_at.replace(ru_month, en_month)
        raw_played_at = raw_played_at.replace('в ', '')
        return dateutil.parser.parse(raw_played_at)

    def _parse_teams(self, raw_teams: str, /):
        home_team, away_team = raw_teams.split(' — ')
        return home_team, away_team

    def _parse_match_duration(self, raw_duration: str, /):
        m = re.match(r'^\d{1,2}:\d{2}$', raw_duration)
        if not m:
            raise ValueError(f'Invalid match duration: "{raw_duration}"')
        raw_minutes, raw_seconds = m.groups()
        minutes = int(raw_minutes) # type: ignore
        seconds = int(raw_seconds) # type: ignore
        return timedelta(minutes=minutes, seconds=seconds)

    def _parse_match_points(self, raw_points: str, /):
        raw_home_points, raw_away_points = raw_points.split(':')
        home_points = int(raw_home_points)
        away_points = int(raw_away_points)
        return home_points, away_points

    def _parse_odds(self, raw_odds: str, /):
        if raw_odds == '-':
            return np.nan
        return float(raw_odds)

    def _is_raw_teams(self, text: str, /):
        return ' — ' in text

    def _parse_matches(self, raw_matches: str, /):
        words = raw_matches.split('\n')
        for substring_to_remove in self._match_data_substrings_to_remove:
            if substring_to_remove in words:
                words.remove(substring_to_remove)
        words = words[self._match_data_offset:]
        logger.debug(f'Match data words: {words}.')
        match_data_list = []
        while len(words) >= 8:
            while words and not self._is_raw_teams(words[0]):
                words.pop(0)
            if not words:
                break
            raw_teams = words.pop(0)
            if words[0] == '...':
                words.pop(0)
                raw_match_duration = words.pop(0)
                raw_played_at = ''
                raw_match_points = words.pop(0)
                words.pop(0)
            else:
                raw_match_duration = None
                raw_played_at = words.pop(0)
                raw_match_points = ''
            raw_odds = {}
            raw_odds['1'] = words.pop(0)
            raw_odds['X'] = words.pop(0)
            raw_odds['2'] = words.pop(0)
            raw_odds['1X'] = words.pop(0)
            raw_odds['12'] = words.pop(0)
            raw_odds['2X'] = words.pop(0)
            try:
                home_team, away_team = self._parse_teams(raw_teams)
            except:
                logger.error(f'Invalid match teams "{raw_teams}".')
                continue
            if home_team.endswith('Хозяева') and away_team == 'Гости':
                continue
            if raw_match_duration:
                try:
                    match_duration = self._parse_match_duration(raw_match_duration)
                except:
                    logger.error(f'Invalid match duration "{raw_match_duration}".')
                    continue
                played_at = datetime.now() - match_duration
                try:
                    home_points, away_points = self._parse_match_points(raw_match_points)
                except:
                    logger.error(f'Invalid match points "{raw_match_points}".')
                    continue
            else:
                try:
                    played_at = self._parse_match_played_at(raw_played_at)
                except:
                    logger.error(f'Invalid match datetime "{raw_played_at}".')
                    continue
                home_points = -1
                away_points = -1
            odds = {}
            for outcome, raw_odds in raw_odds.items():
                try:
                    outcome_odds = self._parse_odds(raw_odds)
                except:
                    logger.error(f'Invalid {outcome} odds "{raw_odds}".')
                    continue
                odds[outcome] = outcome_odds
            match_data_dict: Dict[str, Any] = {
                'match.played_at': played_at,
                'match.home_team': home_team,
                'match.away_team': away_team,
                'match.home_points': home_points,
                'match.away_points': away_points,
                **{'odds.' + outcome: outcome_odds for outcome, outcome_odds in odds.items()},
                # TODO: dtypes
            }
            match_data_list.append(match_data_dict)
        return pd.DataFrame(match_data_list)
