''' High-level methods of the applitation. '''

import pandas as pd
import aiohttp
import asyncio
import humanize
import selenium.webdriver
import selenium.webdriver.firefox.options

from alphabetter.config import default as config
from typing import Optional, List, Tuple, overload, Literal
from datetime import datetime, timedelta, date
from pathlib import Path
from alphabetter.web import *
from alphabetter.core import *
from alphabetter.sql import SQLSession
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from alphabetter.ml import *

import logging


async def etl_championat_tournaments(
    *,
    config_path: Optional[Path] = None,
    sports: Optional[List[Sport]] = None,
    league_names: Optional[List[str]] = None,
    seasons: Optional[List[str]] = None,
    **client_args,
) -> Optional[pd.DataFrame]:
    started_at = datetime.now()
    with open(config_path or config.championat_config_path) as championat_config_file:
        championat_config = ChampionatClient.Config.from_yaml(championat_config_file)
    sports = sports or championat_config.get_sports()
    if league_names:
        if len(sports) != 1:
            raise ValueError('Can\'t specify leagues when sport is unspecified.')
        league_keys = [(sports[0], league_name) for league_name in league_names]
    else:
        league_keys = [
            (sport, league_name)
            for sport in sports
            for league_name in championat_config.get_league_names(sport)
        ]
    if seasons:
        if len(league_keys) != 1:
            raise ValueError('Can\'t specify seasons when league is unspecified.')
        season_map = {league_keys[0]: seasons}
    else:
        season_map = {
            (sport, league_name): championat_config.get_seasons(sport, league_name)
            for sport, league_name in league_keys
        }
    league_progress_bar = create_progress_bar(
        iterable=league_keys,
        total=len(league_keys),
        unit='league',
        disable=(len(league_keys) == 1),
    )
    sql_session = SQLSession.from_url()
    summary_list = []
    summary_df = None
    def build_summary_df():
        nonlocal summary_df
        summary_df = pd.DataFrame(summary_list)
        summary_df.set_index(['sport', 'league', 'season'], inplace=True)
    try:
        async with aiohttp.ClientSession() as http_session:
            championat_client = ChampionatClient(
                config=championat_config,
                http_session=http_session,
                **client_args,
            )
            logging.log(
                LOG_LEVEL_STATUS,
                'In total, processing {} of {}: {}...'.format(
                    humanize_match_count(sum(len(season_map[league_key]) for league_key in league_keys)),
                    humanize_league_count(len(league_keys)),
                    humanize_list([
                        f'{league_name} ({sport:_}, {humanize_season_count(len(season_map[sport, league_name]))})'
                        for sport, league_name in league_keys
                    ]),
                ),
            )
            for sport, league_name in league_progress_bar:
                logging.log(LOG_LEVEL_STATUS, f'Processing {sport:_} league "{league_name}"...')
                league_progress_bar.set_description(f'{sport}, {league_name}')
                tournament_etl_coros = [
                    process_championat_tournament(
                        championat_client=championat_client,
                        sql_session=sql_session,
                        sport=sport,
                        league_name=league_name,
                        season=season,
                    )
                    for season in season_map[sport, league_name]
                ]
                season_progress_bar = create_progress_bar(
                    iterable=asyncio.as_completed(tournament_etl_coros),
                    total=len(tournament_etl_coros),
                    unit='season',
                    disable=(len(league_keys) == 1 and len(tournament_etl_coros) == 1),
                )
                for tournament_etl_coro in season_progress_bar:
                    tournament: Tournament = await tournament_etl_coro
                    summary_data = {
                        'sport': tournament.league.sport,
                        'league': tournament.league.name,
                        'season': tournament.season,
                        'matches': len(tournament.matches),
                    }
                    summary_list.append(summary_data)
                    logging.info(
                        'Loaded {} of the {}.'.format(
                            humanize_match_count(len(tournament.matches)),
                            tournament,
                        ),
                    )
                sql_session.commit()
        build_summary_df()
        return summary_df
    except BaseException as exception:
        build_summary_df()
        raise type(exception)(*exception.args, summary_df)
    finally:
        if not summary_list:
            logging.warning('In total, loaded nothing.')
            return
        assert isinstance(summary_df, pd.DataFrame)
        duration = datetime.now() - started_at
        logging.info(
            'In total, loaded {} from {} and {} in {}.'.format(
                humanize_match_count(summary_df.matches.sum()),
                humanize_season_count(len(summary_df)),
                humanize_league_count(len(summary_df.reset_index()[['sport', 'league']].drop_duplicates())),
                humanize.naturaldelta(duration),
            ),
        )


async def etl_line4bet_odds(
    *,
    config_path: Optional[Path] = None,
    processes: Optional[int] = None,
    sports: Optional[List[Sport]] = None,
    league_names: Optional[List[str]] = None,
    bookmakers: Optional[List[Bookmaker]] = None,
    min_date: date = date(2020, 1, 1),
    max_date: date = (datetime.now() - timedelta(days=1)).date(),
    verbose: bool = False,
    **client_args,
) -> Optional[pd.DataFrame]:
    started_at = datetime.now()
    if min_date >= max_date:
        raise ValueError('Min date must be smaller than max date.')
    with open(config_path or config.line4bet_config_path) as line4bet_config_file:
        line4bet_config = Line4BetClient.Config.from_yaml(line4bet_config_file)
    sports = sports or line4bet_config.get_sports()
    if league_names:
        if len(sports) != 1:
            raise ValueError('Can\'t specify leagues when sport is unspecified.')
        # Drop unspecified leagues from the config.
        league_headers = line4bet_config.league_headers[sports[0]]
        for league_name in league_headers:
            if league_name not in league_names:
                del league_headers[league_name]
    bookmakers = bookmakers or line4bet_config.get_bookmakers()
    dates = [min_date + timedelta(days=i) for i in range((max_date - min_date).days + 1)]
    sport_progress_bar = create_progress_bar(
        iterable=sports,
        total=len(sports),
        unit='sport',
        disable=(len(sports) == 1),
    )
    bookmaker_progress_bar = create_progress_bar(
        iterable=bookmakers,
        total=len(bookmakers),
        unit='bookmaker',
        disable=(len(bookmakers) == 1),
    )
    date_progress_bar = create_progress_bar(
        iterable=dates,
        total=len(dates),
        unit='date',
        disable=(len(dates) == 1),
    )
    sql_session = SQLSession.from_url()
    summary_list = []
    summary_df = None
    def build_summary_df():
        nonlocal summary_df
        if not summary_list:
            summary_df = None
            return
        summary_df = pd.concat(summary_list)
        summary_df.reset_index(inplace=True)
        summary_df.set_index(['date', 'bookmaker', 'sport'], inplace=True)
        if not verbose:
            summary_df = summary_df.groupby(['bookmaker', 'sport', 'league']).sum()
    last_date = min_date
    try:
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False)) as http_session:
            with ProcessPoolExecutor(processes) as executor:
                line4bet_client = Line4BetClient(
                    config=line4bet_config,
                    http_session=http_session,
                    executor=executor,
                    **client_args,
                )
                league_reprs = [
                    f'{league_name} ({sport:_})'
                    for sport in sports
                    for league_name in line4bet_config.get_league_names(sport)
                ]
                logging.log(
                    LOG_LEVEL_STATUS,
                    'In total, searching {} odds for the matches of {}: {}...'.format(
                        humanize_list(bookmakers),
                        humanize_league_count(len(league_reprs)),
                        humanize_list(league_reprs),
                    ),
                )
                for match_date in date_progress_bar:
                    date_scans_of_odds_count = 0
                    date_progress_bar.set_description(match_date.isoformat())
                    for bookmaker in bookmaker_progress_bar:
                        bookmaker_progress_bar.set_description(str(bookmaker))
                        for sport in sport_progress_bar:
                            logging.log(
                                LOG_LEVEL_STATUS,
                                'Searching {} odds for {:_} matches of {:%b %d, %Y}...'.format(
                                    bookmaker,
                                    sport,
                                    match_date
                                ),
                            )
                            sport_progress_bar.set_description(str(sport))
                            summary_df = await process_line4bet_odds(
                                line4bet_client=line4bet_client,
                                sql_session=sql_session,
                                sport=sport,
                                bookmaker=bookmaker,
                                match_date=match_date,
                            )
                            date_scans_of_odds_count += summary_df.odds_scans.sum()
                            summary_df.insert(0, 'date', match_date) # type: ignore
                            summary_df.insert(1, 'bookmaker', str(bookmaker))
                            summary_df.insert(2, 'sport', str(sport))
                            summary_list.append(summary_df)
                            last_date = match_date
                    sql_session.commit()
                    logging.info(
                        'Loaded {:,} scans of odds for {:%b %d, %Y}.'.format(
                            date_scans_of_odds_count,
                            match_date,
                        ),
                    )
        build_summary_df()
        return summary_df
    except BaseException as exception:
        print(exception, type(exception))
        build_summary_df()
        raise type(exception)(*exception.args, summary_df)
    finally:
        if not summary_list:
            logging.warning('In total, loaded nothing.')
            return
        assert isinstance(summary_df, pd.DataFrame)
        bookmaker_metrics_reprs = []
        for bookmaker, bookmaker_metrics in summary_df.groupby('bookmaker'):
            matches_with_odds_percent = 100 * \
                bookmaker_metrics.matched_matches.sum() / bookmaker_metrics.stored_matches.sum()
            bookmaker_metrics_repr = (
                f'{bookmaker_metrics.odds_scans.sum():,} scans of {bookmaker} odds for '
                f'{bookmaker_metrics.matched_matches.sum():,} matches '
                f'({matches_with_odds_percent:.0f}% of the stored)'
            )
            bookmaker_metrics_reprs.append(bookmaker_metrics_repr)
        duration = datetime.now() - started_at
        logging.info(
            'In total, loaded {} from {} to {} in {}.'.format(
                humanize_list(bookmaker_metrics_reprs),
                min_date,
                last_date,
                humanize.naturaldelta(duration),
            )
        )


async def etl_fonbet_odds(
    *,
    config_path: Optional[Path] = None,
    processes: Optional[int] = None,
    sports: Optional[List[Sport]] = None,
    leagues: Optional[List[str]] = None,
    **client_args,
) -> Optional[pd.DataFrame]:
    started_at = datetime.now()
    with open(config_path or config.fonbet_config_path) as fonbet_config_file:
        fonbet_config = FonbetClient.Config.from_yaml(fonbet_config_file)
    sports = sports or fonbet_config.get_sports()
    if leagues:
        if len(sports) != 1:
            raise ValueError('Can\'t specify leagues when sport is unspecified.')
        league_keys = [(sports[0], league) for league in leagues]
    else:
        league_keys = [
            (sport, league_name)
            for sport in sports
            for league_name in fonbet_config.get_league_names(sport)
        ]
    firefox_options = selenium.webdriver.firefox.options.Options()
    firefox_options.headless = True
    def process_league_key(league_key: Tuple[Sport, str]):
        sport, league_name = league_key
        sql_session = SQLSession.from_url()
        with selenium.webdriver.Firefox(options=firefox_options) as firefox:
            fonbet_client = FonbetClient(
                config=fonbet_config,
                webdriver=firefox,
                **client_args,
            )
            df = process_fonbet_odds(
                fonbet_client=fonbet_client,
                sql_session=sql_session,
                sport=sport,
                league_name=league_name,
            )
            sql_session.commit()
            return sport, league_name, df
    summary_df = None
    summary_list = []
    def build_summary_df():
        nonlocal summary_df
        summary_df = pd.DataFrame(summary_list)
        summary_df.set_index(['sport', 'league'], inplace=True)
    logging.log(
        LOG_LEVEL_STATUS,
        'In total, processing {}: {}...'.format(
            humanize_league_count(len(league_keys)),
            humanize_list([f'{league_name} ({sport:_})' for sport, league_name in league_keys]),
        ),
    )
    try:
        with ThreadPoolExecutor(processes) as executor:
            league_progress_bar = create_progress_bar(
                iterable=executor.map(process_league_key, league_keys),
                total=len(league_keys),
                unit='league',
                disable=(len(league_keys) == 1),
            )
            for league_processing_result in league_progress_bar:
                league_processing_result: Tuple[Sport, str, pd.DataFrame]
                sport, league_name, df = league_processing_result
                df.insert(0, 'Sport', str(sport))
                df.insert(1, 'League', league_name)
                df.insert(2, 'Date', df['match.played_at'].dt.strftime('%b %d'))
                df.insert(3, 'Time', df['match.played_at'].dt.strftime('%H:%M'))
                df.drop(
                    columns=['match.played_at', 'match.home_points', 'match.away_points'],
                    inplace=True,
                )
                df.rename(
                    columns={'match.home_team': 'Home team', 'match.away_team': 'Away team'},
                    inplace=True,
                )
                abbreviate_columns(
                    df=df,
                    columns=['found_in_database', *('odds.' + outcome for outcome in OUTCOMES)],
                    abbreviations=['F', *OUTCOMES],
                    legends=['match is found in the database', *OUTCOMES],
                    inplace=True,
                )
                df_legend = df.attrs['legend']
                for outcome in OUTCOMES:
                    del df_legend[outcome]
                df_legend[', '.join(OUTCOMES)] = 'odds'
                limit_df_width(df, {'League': 20, 'Home team': 20, 'Away team': 20}, inplace=True)
                df.F = df.F.map({True: '✓', False: '✗'})
                logging.info(
                    'Loaded {} of {:_} league "{}":\n\n{}'.format(
                        humanize_match_count(len(df)),
                        sport,
                        league_name,
                        tabulate_df(df, index=False),
                    )
                )
                summary_data = {'sport': sport, 'league': league_name, 'matches': len(df)}
                summary_list.append(summary_data)
        build_summary_df()
        return summary_df
    except BaseException as exception:
        build_summary_df()
        raise type(exception)(*exception.args, summary_df)
    finally:
        if not summary_list:
            logging.warning('In total, loaded nothing.')
            return
        assert isinstance(summary_df, pd.DataFrame)
        duration = datetime.now() - started_at
        logging.info(
            'In total, loaded {} of {} in {}.'.format(
                humanize_match_count(summary_df.matches.sum()),
                humanize_league_count(len(summary_df)),
                humanize.naturaldelta(duration),
            )
        )


@overload
def recommend_bets(
    df: pd.DataFrame,
    predictor: Predictor,
    better: Better,
    accountant: Accountant,
    dropppers: Optional[List[Dropper]] = None,
    *,
    inplace: Literal[False] = ...
) -> pd.DataFrame: ...


@overload
def recommend_bets(
    df: pd.DataFrame,
    predictor: Predictor,
    better: Better,
    accountant: Accountant,
    dropppers: Optional[List[Dropper]] = None,
    *,
    inplace: Literal[True] = ...
): ...


def recommend_bets(
    df: pd.DataFrame,
    predictor: Predictor,
    better: Better,
    accountant: Accountant,
    dropppers: Optional[List[Dropper]] = None,
    *,
    inplace: bool = False,
) -> Optional[pd.DataFrame]:
    if not inplace:
        df = df.copy()
    df.odds.dropna(inplace=True)
    logging.log(LOG_LEVEL_STATUS, 'Dropping matches...')
    # TODO drop
    logging.log(LOG_LEVEL_STATUS, 'Predicting match outcomes...')
    predictor.predict(df, inplace=True)
    logging.log(LOG_LEVEL_STATUS, 'Betting on matches...')
    better.bet(df, inplace=True)
    logging.log(LOG_LEVEL_STATUS, 'Calculating investments...')
    accountant.invest_parallel(df, inplace=True)
    if inplace:
        return
    bet_recommendation_df = (
        df[['bet.outcome', 'accounting.investment']]
        .rename(
            columns={
                'bet.outcome': 'outcome',
                'accounting.investment': 'investment',
            }
        )
    )
    return bet_recommendation_df
