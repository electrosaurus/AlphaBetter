import argparse
import pandas as pd

from typing import Optional
from datetime import date, datetime, timedelta
from pathlib import Path
from subprogram import Subprogram
from alphabetter.methods import etl_line4bet_odds
from alphabetter.core import *


class ETLLine4BetOddsSubprogram(Subprogram):
    _abbreviated_metrics = [
        'stored_matches',
        'scanned_matches',
        'matched_matches',
        'stored_matches_without_odds',
        'scanned_matches_without_odds',
        'odds_scans',
        'parsing_errors',
    ]

    _metrics_abbreviations = ['A', 'B', 'X', 'M', 'N', 'S', 'E']

    _metrics_legends = [
        'number of matches stored in the database',
        'number of matches found on line4bet',
        'number of stored matches matched with matches found on line4bet',
        'number of stored matches that have no scans of odds',
        'number of matches found on line4bet that have no suitable scans of odds',
        'number of scans of odds loaded from line4bet',
        'number of parsing errors',
    ]

    _renamed_metrics = {
        'date': 'Date',
        'bookmaker': 'Bookmaker',
        'sport': 'Sport',
        'league': 'League',
    }

    _metrics_width_limits = {
        'League': 20,
    }

    @classmethod
    def get_command(cls):
        return 'etl-line4bet-odds'

    @classmethod
    def get_help(cls):
        return 'extract, transform and load historical odds from https://line4bet.ru'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        super().__init__(arg_parser)
        arg_parser.add_argument(
            '--config',
            type=Path,
            default='configs/line4bet.yaml',
            help='path to the line4bet client config',
        )
        arg_parser.add_argument(
            '--sport',
            type=Sport.from_string,
            action='append',
            help='sport(s) to process',
        )
        arg_parser.add_argument(
            '--bookmaker',
            type=Bookmaker.from_string,
            action='append',
            help='bookmaker(s) to process',
        )
        arg_parser.add_argument(
            '--min-date',
            type=date.fromisoformat,
            default=date(2020, 1, 1),
            help='lower bound of the date of processed matches',
        )
        arg_parser.add_argument(
            '--max-date',
            type=date.fromisoformat,
            default=(datetime.now() - timedelta(days=1)).date(),
            help='upper bound of the date processed matches',
        )
        arg_parser.add_argument(
            '--concurrency',
            type=int,
            default=10,
            help='max number of concurrent HTTP-requests',
        )
        arg_parser.add_argument(
            '--min-odds-scanning-period-minutes',
            type=int,
            default=60,
            help='min distance in minutes between loading scans of odds',
        )
        arg_parser.add_argument(
            '--odds-scanning-time-span-hours',
            type=int,
            default=48,
            help='time span in hours before a match in which odds are scanned',
        )
        arg_parser.add_argument(
            '--scan-odds-after-match-started',
            action='store_true',
            help='process odds scanned after a match had started',
        )

    async def __call__(self, args: argparse.Namespace):
        await super().__call__(args)
        summary_df: Optional[pd.DataFrame] = None
        try:
            summary_df = await etl_line4bet_odds(
                config_path=args.config,
                processes=args.processes,
                sports=args.sport,
                bookmakers=args.bookmaker,
                min_date=args.min_date,
                max_date=args.max_date,
                verbose=args.verbose,
                concurrency_limit=args.concurrency,
                min_odds_scanning_period=timedelta(minutes=args.min_odds_scanning_period_minutes),
                odds_scanning_time_span=timedelta(hours=args.odds_scanning_time_span_hours),
                scan_odds_after_match_started=args.scan_odds_after_match_started,
            )
        except BaseException as exception:
            if exception.args and isinstance(exception.args[-1], pd.DataFrame):
                *exception.args, summary_df = exception.args
            raise
        finally:
            if summary_df is not None:
                summary_df.reset_index(inplace=True)
                summary_df.rename(columns=self._renamed_metrics, inplace=True)
                abbreviate_columns(
                    df=summary_df,
                    columns=self._abbreviated_metrics,
                    abbreviations=self._metrics_abbreviations,
                    legends=self._metrics_legends,
                    inplace=True,
                )
                limit_df_width(summary_df, self._metrics_width_limits, inplace=True)
                print(tabulate_df(summary_df, index=False))
