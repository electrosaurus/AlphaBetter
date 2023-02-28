import argparse
import logging
import iteround

from datetime import datetime
from subprogram import Subprogram
from alphabetter.ml import Predictor, Better, Accountant, select_dataset
from alphabetter.methods import recommend_bets
from alphabetter.core import *


class RecommendBetsSubprogram(Subprogram):
    @classmethod
    def get_command(cls):
        return 'recommend-bets'

    @classmethod
    def get_help(cls):
        return 'recommend bets'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        super().__init__(arg_parser)
        arg_parser.add_argument(
            '--predictor',
            help='path to the predictor',
        )
        arg_parser.add_argument(
            '--better',
            help='path to the better',
        )
        arg_parser.add_argument(
            '--accountant',
            help='path to the accountant',
        )
        arg_parser.add_argument(
            '--balance',
            type=float,
            default=1.0,
            help='current money balance'
        )
        arg_parser.add_argument(
            '--played-after',
            type=datetime.fromisoformat,
            default=datetime.now(),
            help='lower bound of the date of matches for bet advising',
        )
        arg_parser.add_argument(
            '--days',
            type=int,
            default=365,
            help='time span in days of matches for bet advising',
        )
        arg_parser.add_argument(
            '--odds',
            action='store_true',
            help='display odds',
        )
        arg_parser.add_argument(
            '--prediction',
            action='store_true',
            help='display prediction',
        )
        arg_parser.add_argument(
            '--expediency',
            action='store_true',
            help='display expediency of bets',
        )

    async def __call__(self, args: argparse.Namespace):
        await super().__call__(args)
        logging.log(LOG_LEVEL_STATUS, 'Selecting upcoming matches...')
        df = select_dataset(played_after=datetime.now())
        logging.info(f'Selected {humanize_match_count(len(df))}.')
        predictor = Predictor.load(args.predictor)
        logging.info(f'Loaded predictor: {predictor}.')
        better = Better.load(args.better)
        logging.info(f'Loaded better: {better}.')
        accountant = Accountant.load(args.accountant)
        logging.info(f'Loaded accountant: {accountant}.')
        accountant.set_balance(args.balance)
        recommend_bets(df, predictor, better, accountant, inplace=True)
        df.bet.drop_null(inplace=True)
        df.prediction.df = df.prediction.df.apply(
            lambda x: iteround.saferound(100 * x, 0),
            axis=1,
            result_type='broadcast',
        )
        df.bet.expediency = (100 * df.bet.expediency).round()
        df.reset_index(inplace=True)
        df.insert(2, 'Date', df.match.played_at.dt.strftime('%b %d'))
        df.insert(3, 'Time', df.match.played_at.dt.strftime('%H:%M'))
        df.drop(
            columns=df.columns.difference(
                [
                    'bookmaker',
                    'match.league',
                    'Date',
                    'Time',
                    'match.home_team',
                    'match.away_team',
                    *('odds.' + outcome for outcome in OUTCOMES),
                    *('prediction.' + outcome for outcome in SINGLE_OUTCOMES),
                    'bet.outcome',
                    'bet.expediency',
                    'accounting.investment',
                ],
            ),
            inplace=True,
        )
        limit_df_width(
            df,
            {
                'match.league': 20,
                'match.home_team': 20,
                'match.away_team': 20,
            },
            inplace=True,
        )
        abbreviations = {}
        legends = {}
        abbreviations['bookmaker'] = 'BM'
        legends['BM'] = 'bookmaker'
        if args.odds:
            abbreviations.update(dict(zip(df.odds.prefixed_columns, OUTCOMES)))
            legends[', '.join(OUTCOMES)] = 'odds'
        else:
            df.drop(columns=df.odds.prefixed_columns, inplace=True)
        if args.prediction:
            columns = ('P' + outcome for outcome in SINGLE_OUTCOMES)
            abbreviations.update(dict(zip(df.prediction.prefixed_columns, columns)))
            legends[', '.join('P' + outcome for outcome in SINGLE_OUTCOMES)] = 'predicted outcome probabilities'
        else:
            df.drop(columns=df.prediction.prefixed_columns, inplace=True)
        abbreviations['bet.outcome'] = 'M'
        legends['M'] = 'recommended outcome to bet on'
        if args.expediency:
            abbreviations['bet.expediency'] = 'E'
            legends['E'] = 'investment profitability (from 0 to 100)'
        else:
            df.drop(columns='bet.expediency', inplace=True)
        abbreviations['accounting.investment'] = '$'
        legends['$'] = 'recommended bet money'
        abbreviate_columns(df, abbreviations, abbreviations.values(), legends, inplace=True)
        df.rename(
            columns={
                'match.league': 'League',
                'match.home_team': 'Home team',
                'match.away_team': 'Away team',
            },
            inplace=True,
        )
        print(tabulate_df(df, index=False))
