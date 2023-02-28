''' Methods implementing ETL-processes. '''

import pandas as pd

from datetime import date
from alphabetter.core.model import *
from alphabetter.sql import *
from alphabetter.web import *
from typing import Dict
from sqlalchemy import cast, Date
from alphabetter.core.logging import LOG_LEVEL_STATUS

import logging


async def process_championat_tournament(
    championat_client: ChampionatClient,
    sql_session: SQLSession,
    sport: Sport,
    league_name: str,
    season: str,
) -> Tournament:
    league = League(sport=sport, name=league_name)
    tournament = (
        sql_session
        .query(Tournament)
        .filter_by(season=season)
        .join(League)
        .filter_by(sport=sport, name=league_name)
        .one_or_none()
    )
    # TODO: Allow disable deletion
    if tournament:
        logging.log(LOG_LEVEL_STATUS, f'Deleting the {tournament}...')
        sql_session.delete(tournament)
        logging.info(f'Deleted the {tournament}.')
    tournament = Tournament(league=league, season=season)
    df = await championat_client.download_tournament(sport, league_name, season)
    for row in df.itertuples():
        match = Match(
            tournament=tournament,
            played_at=row.played_at,
            home_team=Team(
                sport=tournament.league.sport,
                country=Country(row.home_country),
                name=row.home_team,
            ),
            away_team=Team(
                sport=tournament.league.sport,
                country=Country(row.home_country),
                name=row.away_team,
            ),
            home_points=row.home_points,
            away_points=row.away_points,
        )
        tournament.matches.append(match)
    sql_session.merge(tournament)
    return tournament


async def process_line4bet_odds(
    line4bet_client: Line4BetClient,
    sql_session: SQLSession,
    bookmaker: Bookmaker,
    sport: Sport,
    match_date: date,
) -> pd.DataFrame:
    summary_df = pd.DataFrame(
        data={
            'stored_matches': 0,
            'scanned_matches': 0,
            'matched_matches': 0,
            'stored_matches_without_odds': 0,
            'scanned_matches_without_odds': 0,
            'odds_scans': 0,
            'parsing_errors': 0,
        },
        index=pd.Index(line4bet_client.config.get_league_names(sport), name='league'),
    )
    stored_matches: Dict[str, set] = {}
    matches_with_odds: Dict[str, set] = {}
    for league_name in line4bet_client.config.get_league_names(sport):
        stored_matches[league_name] = set((sql_session
            .query(Match).filter(cast(Match.played_at, Date) == match_date)
            .join(Tournament)
            .join(League).filter(League.sport == sport, League.name == league_name)
            .all()
        ))
        summary_df.loc[league_name, 'stored_matches'] = len(stored_matches[league_name])
    if all(not league_stored_matches for league_stored_matches in stored_matches.values()):
        logging.info(f'No matches found in the database for {match_date:%b %d, %Y}.')
        return summary_df
    event_generator = line4bet_client.download_odds(
        sport=sport,
        bookmaker=bookmaker,
        match_date=match_date,
    )
    async for event in event_generator:
        match event:
            case Line4BetClient.LeagueHeaderScanned(league_header):
                logging.debug(f'Scanned league header "{league_header}".')
            case Line4BetClient.MatchHeaderParsingError(league_name, match_header):
                logging.info(f'Failed to parse {league_name} match header "{match_header}".')
                summary_df.loc[league_name, 'parsing_errors'] += 1 # type: ignore
            case Line4BetClient.NoOddsScansWarning(league_name, match_data):
                logging.warning(f'No scans of {bookmaker} odds are available for the {league_name} {sport:_} {match_data:_}.')
                summary_df.loc[league_name, 'scanned_matches_without_odds'] += 1 # type: ignore
            case Line4BetClient.OddsDownloaded(league_name, match_data, odds_scans):
                summary_df.loc[league_name, 'scanned_matches'] += 1 # type: ignore
                match = sql_session.find_match(
                    sport=sport,
                    league_name=league_name,
                    played_at=match_data.played_at,
                    home_team_name=match_data.home_team,
                    away_team_name=match_data.away_team,
                )
                if not match:
                    logging.error(f'The {league_name} {sport:_} {match_data:_} is not found in the database.')
                    continue
                matches_with_odds.setdefault(league_name, set()).add(match)
                summary_df.loc[league_name, 'matched_matches'] += 1 # type: ignore
                match.odds.clear()
                for scanned_at, odds in odds_scans.iterrows():
                    odds = Odds(
                        bookmaker=bookmaker,
                        scanned_at=scanned_at.to_pydatetime(), # type: ignore
                        home_win=odds['1'],
                        draw=odds['X'],
                        away_win=odds['2'],
                        home_win_or_draw=odds['1X'],
                        win=odds['12'],
                        away_win_or_draw=odds['2X'],
                    )
                    match.odds.append(odds)
                logging.info(
                    f'Found {len(odds_scans)} scans of {bookmaker} odds for '
                    f'the {league_name} {sport:_} {match_data:_}.'
                )
                summary_df.loc[league_name, 'odds_scans'] += len(odds_scans) # type: ignore
    for league_name, stored_league_matches in stored_matches.items():
        league_matches_with_odds = matches_with_odds.get(league_name, set())
        stored_league_matches_without_odds = stored_league_matches - league_matches_with_odds
        for match in stored_league_matches_without_odds:
            logging.warning(f'The {league_name} {sport:_} {match:_} has no scans of {bookmaker} odds.')
            summary_df.loc[league_name, 'stored_matches_without_odds'] += 1 # type: ignore
    return summary_df


def process_fonbet_odds(
    fonbet_client: FonbetClient,
    sql_session: SQLSession,
    sport: Sport,
    league_name: str,
) -> pd.DataFrame:
    upcoming_match_dataset = fonbet_client.download_upcoming_matches(sport, league_name)
    upcoming_match_dataset.insert(3, 'found_in_database', False)
    odds_scanned_at = datetime.now()
    for match_index, match_data in upcoming_match_dataset.iterrows():
        match = sql_session.find_match(
            sport=sport,
            league_name=league_name,
            played_at=match_data['match.played_at'],
            home_team_name=match_data['match.home_team'],
            away_team_name=match_data['match.away_team'],
        )
        if not match:
            error_message = (
                f'The {league_name} {sport:_} match '
                f'between {match_data["match.home_team"]} and {match_data["match.away_team"]} '
                f'played {match_data["match.played_at"]:on %b %d, %Y at %H:%M} '
                'is not found in the database.'
            )
            logging.error(error_message)
            continue
        upcoming_match_dataset.loc[match_index, 'found_in_database'] = True # type: ignore
        odds = Odds(
            bookmaker=Bookmaker.FONBET,
            scanned_at=odds_scanned_at,
            home_win=match_data['odds.1'],
            draw=match_data['odds.X'],
            away_win=match_data['odds.2'],
            home_win_or_draw=match_data['odds.1X'],
            win=match_data['odds.12'],
            away_win_or_draw=match_data['odds.2X'],
        )
        match.odds.append(odds)
    return upcoming_match_dataset
