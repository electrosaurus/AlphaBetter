import argparse

from alphabetter.sql import SQLSession
from alphabetter.sql.schema import sql_schema
from subprogram import Subprogram


class CreateSchemaSubprogram(Subprogram):
    @classmethod
    def get_command(cls):
        return 'create-schema'

    @classmethod
    def get_help(cls):
        return 'create database schema'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        super().__init__(arg_parser)
        arg_parser.add_argument(
            '--drop',
            action='store_true',
            help='drop the existing schema'
        )
        arg_parser.add_argument(
            '--force',
            action='store_true',
            help='don\'t ask for consent before dropping the schema'
        )

    async def __call__(self, args: argparse.Namespace):
        sql_session = SQLSession.from_url()
        db_engine = sql_session.bind
        if args.drop:
            if args.force:
                confirmed = True
            else:
                prompt = 'Are you sure you want to delete the database? [y/N] '
                match input(prompt).lower():
                    case 'y':
                        confirmed = True
                    case 'n':
                        confirmed = False
                    case answer:
                        raise ValueError(f'Invalid answer "{answer}"')
            if confirmed:
                sql_schema.drop_all(db_engine)
        sql_schema.create_all(db_engine)
