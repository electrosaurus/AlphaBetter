''' High-level methods for machine learning. '''

import pandas as pd
import os

from alphabetter.config import default as config
from pathlib import Path
from typing import Optional, Set, Set, Dict, Any
from datetime import datetime
from alphabetter.core import *
from uuid import UUID
from hashlib import md5
from alphabetter.ml.core import *
from datetime import datetime
from alphabetter.sql import SQLSession
from enum import Flag

import logging


class DatasetKind(Flag):
    MATCH = 1
    PREDICTED_MATCH = 1 & 2
    BET_MATCH = 1 & 2 & 4
    ACCOUNTED_MATCH = 1 & 2 & 4 & 8


_df_selection_query = '''
    SELECT * FROM (
        SELECT  match.id AS "match.id",
                league.sport AS "match.sport",
                league.name AS "match.league",
                tournament.season AS "match.season",
                match.played_at AS "match.played_at",
                home_team.country AS "match.home_country",
                away_team.country AS "match.away_country",
                home_team.name AS "match.home_team",
                away_team.name AS "match.away_team",
                match.home_points AS "match.home_points",
                match.away_points AS "match.away_points",
                odds.bookmaker AS "bookmaker",
                odds."1" AS "odds.1",
                odds."X" AS "odds.X",
                odds."2" AS "odds.2",
                odds."1X" AS "odds.1X",
                odds."12" AS "odds.12",
                odds."2X" AS "odds.2X",
                ROW_NUMBER() OVER(
                    PARTITION BY match_id, odds.bookmaker ORDER BY odds.scanned_at DESC
                ) AS "odds.scan_rank"
        FROM (
            SELECT * FROM match
            WHERE match.played_at BETWEEN %(played_after)s AND %(played_before)s
        ) match
        JOIN tournament
            ON match.tournament_id = tournament.id
        JOIN league
            ON tournament.league_id = league.id
        JOIN team AS home_team
            ON home_team_id = home_team.id
        JOIN team AS away_team
            ON away_team_id = away_team.id
        LEFT JOIN odds
            ON odds.match_id = match.id
            AND odds.bookmaker IN %(bookmakers)s
            AND odds.scanned_at < match.played_at
    ) a
    WHERE bookmaker IS NULL OR "odds.scan_rank" = 1
    ORDER BY ("match.played_at", "bookmaker", "match.sport", "match.league")
    '''


_match_column_dtypes = {
    'match.id': 'a36',
    'match.sport': 'category',
    'match.league': 'category',
    'match.season': 'category',
    'match.home_country': 'category',
    'match.away_country': 'category',
    'match.home_team': 'category',
    'match.away_team': 'category',
    'match.home_points': 'i1',
    'match.away_points': 'i1',
    'bookmaker': 'category',
    'odds.1': 'f4',
    'odds.X': 'f4',
    'odds.2': 'f4',
    'odds.1X': 'f4',
    'odds.12': 'f4',
    'odds.2X': 'f4',
}

_predicted_match_column_dtypes = {
    'prediction.1': 'f4',
    'prediction.X': 'f4',
    'prediction.2': 'f4',
}

_bet_match_column_dtypes = {
    'bet.outcome': 'category',
    'bet.expediency': 'f4',
}

_column_dtypes = {
    DatasetKind.MATCH: _match_column_dtypes,
    DatasetKind.PREDICTED_MATCH: {
        **_match_column_dtypes,
        **_predicted_match_column_dtypes,
    },
    DatasetKind.BET_MATCH: {
        **_match_column_dtypes,
        **_predicted_match_column_dtypes,
        **_bet_match_column_dtypes,
    },
}

def read_dataset(
    kind: DatasetKind,
    dir: Path,
    name: Optional[str] = None,
) -> pd.DataFrame:
    if not dir.is_absolute():
        dir = config.data_dir / dir
    if name:
        path = dir / (name + '.csv')
    else:
        path = max(dir.glob('*.csv'), key=os.path.getctime)
    with open(path) as file:
        attrs = {}
        file.readline()
        while True:
            line = file.readline().strip()
            if not line.startswith('#'):
                break
            key, raw_value = map(str.strip, line[1:].split(':', 1))
            match key:
                case 'version':
                    value = raw_value
                case 'created_at':
                    continue
                case 'representativeness':
                    value = float(raw_value)
                case 'match.per_day':
                    value = float(raw_value)
                case _:
                    logging.warning(f'Unknown dataset attribute "{key}".')
                    continue
            attrs[key] = value
        assert line == ''
        df = pd.read_csv(
            file,
            low_memory=True,
            index_col=['match.id', 'bookmaker'],
            parse_dates=['match.played_at'],
            dtype=_column_dtypes[kind], # type: ignore
        )
    df.attrs = attrs
    logging.info(f'Loaded match dataset from {path.absolute()}.')
    return df


def read_match_dataset(name: Optional[str] = None) -> pd.DataFrame:
    return read_dataset(DatasetKind.MATCH, Path('datasets', 'match'), name)


def read_predicted_match_dataset(name: Optional[str] = None) -> pd.DataFrame:
    return read_dataset(DatasetKind.PREDICTED_MATCH, Path('datasets', 'predicted_match'), name)


def read_bet_match_dataset(name: Optional[str] = None) -> pd.DataFrame:
    return read_dataset(DatasetKind.BET_MATCH, Path('datasets', 'bet_match'), name)


def select_dataset(
    db_url: Optional[str] = None,
    *,
    played_after: datetime = datetime.min,
    played_before: datetime = datetime.max,
    bookmakers: Set[Bookmaker] = set(Bookmaker),
) -> pd.DataFrame:
    sql_session = SQLSession.from_url(db_url or config.db_url)
    query_params = {
        'played_after': played_after,
        'played_before': played_before,
        'bookmakers': tuple(bookmaker.name for bookmaker in bookmakers),
    }
    df = pd.read_sql_query(
        sql=_df_selection_query,
        con=sql_session.bind,
        params=query_params,
        parse_dates=['match.played_at'],
        dtype=_match_column_dtypes, # type: ignore
    )
    df.drop(columns=['odds.scan_rank'], inplace=True)
    df['match.sport'] = df['match.sport'].apply(lambda x: str(Sport.from_string(x)))
    df['bookmaker'] = df['bookmaker'].apply(lambda x: x and str(Bookmaker.from_string(x)))
    df.set_index(['match.id', 'bookmaker'], drop=True, inplace=True)
    return df


def save_dataset(df: pd.DataFrame, /, dir: Path, name: Optional[str] = None) -> Optional[Path]:
    if config.dry:
        return None
    if not dir.is_absolute():
        dir = config.data_dir / dir
    df = df.copy()
    df.insert(10, 'bookmaker', df.index.get_level_values('bookmaker'))
    df.insert(0, 'match.id', df.index.get_level_values('match.id').astype(str))
    df.reset_index(drop=True, inplace=True)
    df_csv = df.to_csv(index=False)
    if not name:
        name = str(UUID(md5(df_csv.encode('utf-8')).hexdigest()))
    path = dir / (name + '.csv')
    attrs = {
        'version': config.version,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'representativeness': str(round(df.attrs.get("representativeness", 1.0), 4)),
        'match.per_day': str(round(df.match.per_day(), 4)),
    }
    with open(path, 'w') as file:
        file.write(f'# ALPHABETTER DATASET\n')
        for key, value in attrs.items():
            file.write(f'# {key}: {value}\n')
        file.write('\n')
        file.write(df_csv)
    return path


def save_match_dataset(df: pd.DataFrame, /, name: Optional[str] = None) -> Optional[Path]:
    return save_dataset(df, Path('datasets', 'match'), name)


def save_predicted_match_dataset(df: pd.DataFrame, /, name: Optional[str] = None) -> Optional[Path]:
    return save_dataset(df, Path('datasets', 'predicted_match'), name)


def save_bet_match_dataset(df: pd.DataFrame, /, name: Optional[str] = None) -> Optional[Path]:
    return save_dataset(df, Path('datasets', 'bet_match'), name)


def describe_dataset(df: pd.DataFrame, /) -> str:
    text = (
        f'{len(df):,} matches from {df.match.played_at.min():%Y-%m-%d} '
        f'to {df.match.played_at.max():%Y-%m-%d}'
    )
    return text


def summarize_dataset(df: pd.DataFrame, /) -> Dict[str, Any]:
    data = df.attrs
    data['length'] = len(df)
    data['match.min_date'] = df.match.played_at.min().date()
    data['match.max_date'] = df.match.played_at.max().date()
    return {key: data[key] for key in sorted(data)} # type: ignore
