import argparse

from datetime import timedelta
from pathlib import Path
from subprogram import Subprogram
from alphabetter.core import *
from alphabetter.methods import etl_fonbet_odds


class ETLFonbetOddsSubprogram(Subprogram):
    @classmethod
    def get_command(cls):
        return 'etl-fonbet-odds'

    @classmethod
    def get_help(cls):
        return 'extract, transform and load Fonbet odds of upcoming matches'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        super().__init__(arg_parser)
        arg_parser.add_argument(
            '--config',
            type=Path,
            default='configs/fonbet.yaml',
            help='path to the Fonbet client config',
        )
        arg_parser.add_argument(
            '--sport',
            type=Sport.from_string,
            action='append',
            help='sport(s) to process',
        )
        arg_parser.add_argument(
            '--league',
            action='append',
            help='league(s) to process',
        )
        arg_parser.add_argument(
            '--timeout',
            type=int,
            default=120,
            help='timeout of HTTP-requests in seconds',
        )

    async def __call__(self, args: argparse.Namespace):
        await super().__call__(args)
        await etl_fonbet_odds(
            config_path=args.config,
            processes=args.processes,
            sports=args.sport,
            leagues=args.league,
            timeout=timedelta(seconds=args.timeout),
        )
