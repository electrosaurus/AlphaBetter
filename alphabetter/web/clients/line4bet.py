from __future__ import annotations

import aiohttp
import asyncio
import pandas as pd
import logging
import re
import yaml

from bs4 import BeautifulSoup
from concurrent.futures.process import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from http.client import HTTPException
from typing import Dict, AsyncIterator, Set, TextIO
from alphabetter.core.model import *
from abc import ABC


logger = logging.getLogger(__name__)


class Line4BetClient:
    ''' Asyncronous HTTP client to search and download scans of odds from https://line4bet.ru. '''

    _url = 'https://line4bet.ru/wp-content/themes/twentyseventeen/action_sport.php'
    _table_header_separator_pattern = re.compile(r'\s*\.\s*')
    _month_numbers = {
        name: number for number, name in
            enumerate('янв фев мар апр май июн июл авг сен окт ноя дек'.split(), start=1)
    }
    _column_names = {
        'Дата-время скана линии': 'scanned_at',
        'П1': '1',
        'Х': 'X',
        'П2': '2',
        '1Х': '1X',
        '2Х': '2X',
    }

    @dataclass(frozen=True)
    class Config:
        ''' Stores the rules of data transformation between https://line4bet.ru and
            `Line4BetClient`. '''

        sport_api_parms: Dict[Sport, str]
        ''' Sport identifiers used in line4bet's request parameters. '''

        bookmaker_api_params: Dict[Bookmaker, str]
        league_headers: Dict[Sport, Dict[str, Set[str]]]
        _league_header_index: Dict[Sport, Dict[str, str]] = field(
            init=False, repr=False, compare=False, default_factory=dict)

        def __post_init__(self):
            for sport, league_names in self.league_headers.items():
                self._league_header_index[sport] = {}
                for league_name, league_headers in league_names.items():
                    for league_header in league_headers:
                        self._league_header_index[sport][league_header] = league_name

        @classmethod
        def from_yaml(cls, stream: TextIO):
            data = yaml.safe_load(stream)
            api_params = data['api_params']
            return cls(
                sport_api_parms={
                    Sport.from_string(sport_name): api_param
                    for sport_name, api_param in api_params['sports'].items()
                },
                bookmaker_api_params={
                    Bookmaker.from_string(bookmaker_name): api_param
                    for bookmaker_name, api_param in api_params['bookmakers'].items()
                },
                league_headers={
                    Sport.from_string(sport_name): {
                        name: set(titles)
                        for name, titles in league_headers.items()
                    }
                    for sport_name, league_headers in data['league_headers'].items()
                },
            )

        def find_league_name_by_header(self, sport: Sport, league_header: str):
            try:
                return self._league_header_index[sport][league_header]
            except KeyError:
                return None

        def get_sport_api_param(self, sport: Sport):
            try:
                return self.sport_api_parms[sport]
            except KeyError as error:
                raise KeyError(f'No API param found for sport "{sport}".') from error

        def get_bookmaker_api_param(self, bookmaker: Bookmaker):
            try:
                return self.bookmaker_api_params[bookmaker]
            except KeyError as error:
                raise KeyError(f'No API param found for bookmaker "{bookmaker}".') from error

        def get_bookmakers(self):
            return list(self.bookmaker_api_params)

        def get_sports(self):
            return list(self.league_headers)

        def get_league_names(self, sport: Sport):
            try:
                return list(self.league_headers[sport])
            except KeyError as error:
                raise KeyError(f'No leagues found for sport "{sport}".') from error

        def get_last_league_header(self, sport: Sport) -> str:
            ''' Find the last (alphabetically) league header. '''
            return max(set().union(*list(self.league_headers[sport].values())))

    @dataclass(frozen=True, slots=True)
    class MatchInfo(Decapitalizaple):
        played_at: datetime
        home_team: str
        away_team: str

        def __str__(self):
            return (
                f'Match between {self.home_team} and {self.away_team} '
                f'played {self.played_at:on %b %d, %Y at %H:%M}'
            )

    @dataclass(frozen=True, slots=True)
    class Event(ABC):
        pass

    @dataclass(frozen=True, slots=True)
    class OddsDownloaded(Event):
        league_name: str
        match_info: Line4BetClient.MatchInfo
        odds_scans: pd.DataFrame

    @dataclass(frozen=True, slots=True)
    class LeagueHeaderScanned(Event):
        league_header: str

    @dataclass(frozen=True, slots=True)
    class MatchHeaderParsingError(Event):
        league_name: str
        match_header: str

    @dataclass(frozen=True, slots=True)
    class NoOddsScansWarning(Event):
        league_name: str
        match_info: Line4BetClient.MatchInfo

    def __init__(
        self,
        config: Config,
        http_session: aiohttp.ClientSession,
        executor: ProcessPoolExecutor,
        *,
        concurrency_limit: int = 10,
        min_odds_scanning_period: timedelta = timedelta(hours=1),
        odds_scanning_time_span: timedelta = timedelta(days=1),
        scan_odds_after_match_started: bool = False,
    ):
        self._config = config
        self._http_session = http_session
        self._executor = executor
        self._request_semaphore = asyncio.Semaphore(concurrency_limit)
        self._min_odds_scanning_period = min_odds_scanning_period
        self._odds_scanning_time_span = odds_scanning_time_span
        self._scan_odds_after_match_started = scan_odds_after_match_started

    @property
    def config(self):
        return self._config

    async def download_odds(
        self,
        sport: Sport,
        bookmaker: Bookmaker,
        match_date: date,
    ) -> AsyncIterator[Event]:
        page_1_coro = self._download_page(
            sport=sport,
            bookmaker=bookmaker,
            match_date=match_date,
            page_number=1,
        )
        page_1 = await page_1_coro
        page_1_soup = BeautifulSoup(page_1, 'html.parser')
        page_count = len(page_1_soup.find_all('a', {'class': 'pages'}))
        page_coros = [
            self._download_page(
                sport=sport,
                bookmaker=bookmaker,
                match_date=match_date,
                page_number=page_number,
            ) for page_number in range(2, page_count + 1)
        ]
        odds_extraction_kwargs = {
            'sport': sport,
            'config': self._config,
            'min_odds_scanning_period': self._min_odds_scanning_period,
            'odds_scanning_time_span': self._odds_scanning_time_span,
            'scan_odds_after_match_started': self._scan_odds_after_match_started,
        }
        odds_extraction = self._executor.submit(self._extract_odds, page_1, **odds_extraction_kwargs)
        odds_extractions = [odds_extraction]
        for page_coro in asyncio.as_completed(page_coros):
            page = await page_coro
            future = self._executor.submit(self._extract_odds, page, **odds_extraction_kwargs)
            odds_extractions.append(future)
        for odds_extraction in odds_extractions:
            events = odds_extraction.result()
            for event in events:
                yield event

    async def _download_page(
        self,
        bookmaker: Bookmaker,
        sport: Sport,
        match_date: date,
        page_number: int,
    ):
        async with self._request_semaphore:
            request_data = {
                'MIME-тип': 'application/x-www-form-urlencoded; charset=UTF-8',
                'data_p': match_date.strftime('%d-%m-%Y'),
                'sport_p': self._config.get_sport_api_param(sport),
                'buk_p': self._config.get_bookmaker_api_param(bookmaker),
                'par_p': f'fb{page_number}'
            }
            async with self._http_session.post(self._url, data=request_data) as response:
                response_text = await response.text()
                if response.status != 200:
                    raise HTTPException(response_text)
                return response_text

    @staticmethod
    def _extract_odds(
        page,
        sport: Sport,
        config: Config,
        min_odds_scanning_period: timedelta,
        odds_scanning_time_span: timedelta,
        scan_odds_after_match_started: bool,
    ) -> List[Event]:
        page_soup = BeautifulSoup(page, 'html.parser')
        last_league_header = config.get_last_league_header(sport)
        match_info = None
        league_name = None
        events = []
        for table_soup in page_soup.find_all('table'):
            df = pd.read_html(str(table_soup), decimal=',', thousands='.')[0]
            match table_soup.get('class', [None])[0]:
                case 'liga':
                    league_header = df.iloc[0, 0]
                    if not isinstance(league_header, str):
                        raise TypeError()
                    events.append(Line4BetClient.LeagueHeaderScanned(league_header))
                    league_name = config.find_league_name_by_header(sport, league_header)
                    if league_header > last_league_header:
                        break
                case 'event':
                    if league_name:
                        match_header = df.iloc[0, 0]
                        if not isinstance(match_header, str):
                            raise TypeError()
                        try:
                            match_info = Line4BetClient._parse_match_header(match_header)
                        except Exception as error:
                            events.append(Line4BetClient.MatchHeaderParsingError(league_name, match_header))
                            match_info = None
                case None:
                    odds_scans = df
                    if league_name and match_info:
                        odds_scans.rename(columns=Line4BetClient._column_names, inplace=True)
                        odds_scans = odds_scans.filter(['scanned_at', '1', 'X', '2', '1X', '12', '2X'])
                        odds_scans = odds_scans.loc[odds_scans.scanned_at != 'ИТОГО (%)'].copy()
                        odds_scans.scanned_at = odds_scans.scanned_at.apply(
                            Line4BetClient._parse_odds_scan_datetime,
                            match_date=match_info.played_at.date(),
                        )
                        if not scan_odds_after_match_started:
                            odds_scans.drop(odds_scans[odds_scans.scanned_at > match_info.played_at].index, inplace=True)
                        earliest_scanning = match_info.played_at - odds_scanning_time_span
                        odds_scans.drop(odds_scans[odds_scans.scanned_at < earliest_scanning].index, inplace=True)
                        if odds_scans.empty:
                            events.append(Line4BetClient.NoOddsScansWarning(league_name, match_info))
                            match_info = None
                            continue
                        odds_scans.drop_duplicates('scanned_at', inplace=True)
                        resampling_rule = f'{min_odds_scanning_period.total_seconds()}S'
                        odds_scans = odds_scans.resample(resampling_rule, on='scanned_at').first()
                        odds_scans.drop(odds_scans[odds_scans['1 X 2 1X 12 2X'.split()].isna().all(axis=1)].index, inplace=True)
                        events.append(Line4BetClient.OddsDownloaded(league_name, match_info, odds_scans))
                        match_info = None
                case table_class:
                    raise ValueError(f'Unknown table class "{table_class}".')
        return events

    @staticmethod
    def _parse_league_header(source: str):
        fields = re.split(Line4BetClient._table_header_separator_pattern, source)
        name = '. '.join(fields[1:])
        return name

    @staticmethod
    def _parse_match_header(source: str):
        source = source.split(';')[0]
        fields = source.split()
        played_at = datetime.strptime(' '.join(fields[:2]), '%d.%m.%Y %H:%M')
        home_team, away_team = Line4BetClient._parse_teams(' '.join(fields[2:]))
        return Line4BetClient.MatchInfo(played_at, home_team, away_team)

    @staticmethod
    def _parse_teams(teams: str):
        home_team, away_team = teams.split(' - ')
        return Line4BetClient._parse_team(home_team), Line4BetClient._parse_team(away_team)

    @staticmethod
    def _parse_team(team: str):
        if team.endswith(' (ж)'):
            team = team[:-4]
        return team

    @staticmethod
    def _parse_odds_scan_datetime(scan_datetime: str, match_date: date):
        scan_datetime_words = scan_datetime.split()
        day = int(scan_datetime_words[0])
        month_name = scan_datetime_words[1]
        month = Line4BetClient._month_numbers[month_name]
        time = datetime.strptime(scan_datetime_words[2], '%H:%M')
        scan_datetime_1, scan_datetime_2 = (
            datetime(year=year, month=month, day=1, hour=time.hour, minute=time.minute)
            for year in [match_date.year - 1, match_date.year]
        )
        match_datetime = datetime.combine(date.today(), datetime.min.time())
        if abs(match_datetime - scan_datetime_1) < abs(match_datetime - scan_datetime_2):
            year = scan_datetime_1.year
        else:
            year = scan_datetime_2.year
        return datetime(year=year, month=month, day=day, hour=time.hour, minute=time.minute)
