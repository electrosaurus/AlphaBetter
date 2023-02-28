import argparse
import pandas as pd

from pathlib import Path
from subprogram import Subprogram
from alphabetter.core import *
from alphabetter.methods import etl_championat_tournaments
from typing import Optional


class ETLChampionatTournamentsSubprogram(Subprogram):
    _renamed_summary_columns = {
        'sport': 'Sport',
        'league': 'League',
        'season': 'Season',
        'matches': 'Matches',
    }

    _metrics_width_limits = {
        'League': 20,
    }

    @classmethod
    def get_command(cls):
        return 'etl-championat-tournament'

    @classmethod
    def get_help(cls):
        return 'extract, transform and load tournaments from https://www.championat.com'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        super().__init__(arg_parser)
        arg_parser.add_argument(
            '--config',
            type=Path,
            default='configs/championat.yaml',
            help='path to the championat client config',
        )
        arg_parser.add_argument(
            '--sport',
            action='append',
            type=Sport.from_string,
            help='sport(s) to process',
        )
        arg_parser.add_argument(
            '--league',
            action='append',
            help='league(s) to process',
        )
        arg_parser.add_argument(
            '--season',
            action='append',
            help='seasons(s) to process',
        )
        arg_parser.add_argument(
            '--concurrency',
            type=int,
            default=4,
            help='max number of concurrent HTTP-requests',
        )

    async def __call__(self, args: argparse.Namespace):
        await super().__call__(args)
        summary_df: Optional[pd.DataFrame] = None
        try:
            summary_df = await etl_championat_tournaments(
                config_path=args.config,
                concurrency_limit=args.concurrency,
                sports=args.sport,
                league_names=args.league,
                seasons=args.season,
            )
        except BaseException as exception:
            if exception.args and isinstance(exception.args[-1], pd.DataFrame):
                *exception.args, summary_df = exception.args
            raise
        finally:
            if summary_df is not None:
                summary_df.reset_index(inplace=True)
                summary_df.rename(columns=self._renamed_summary_columns, inplace=True)
                limit_df_width(summary_df, self._metrics_width_limits, inplace=True)
                print(tabulate_df(summary_df, index=False))
