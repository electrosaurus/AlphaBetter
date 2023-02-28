import argparse

from abc import ABC, abstractmethod, abstractclassmethod
from typing import Optional
from alphabetter.config import default


class Subprogram(ABC):
    default_db_url = 'postgresql://alphabetter@localhost/alphabetter'

    def __init__(self, arg_parser: argparse.ArgumentParser):
        self._arg_parser = arg_parser
        arg_parser.add_argument(
            '-v',
            '--verbose',
            action='store_true',
            help='print more information',
        )
        arg_parser.add_argument(
            '-d',
            '--database',
            metavar='URL',
            help='database URL (default - postgresql://alphabetter@localhost/alphabetter)',
        )
        arg_parser.add_argument(
            '-p',
            '--processes',
            type=int,
            metavar='N',
            help='number of processes used by the program (default — the number of CPUs)',
        )
        arg_parser.add_argument(
            '-l',
            '--log-level',
            choices=['error', 'warning', 'info', 'status', 'debug'],
            metavar='LEVEL',
            help='log level (default - warning)',
        )
        arg_parser.add_argument(
            '--log-format',
            metavar='FORMAT',
            help='log format',
        )
        arg_parser.add_argument(
            '--log-file',
            metavar='PATH',
            help='log file',
        )
        arg_parser.add_argument(
            '--hide-progress',
            action='store_true',
            help='don\'t show the progress bar',
        )
        arg_parser.add_argument(
            '--dry',
            action='store_true',
            help='don\'t commit to the database or create files',
        )
        self._arg_parser = arg_parser

    @abstractclassmethod
    def get_command(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def get_help(cls) -> Optional[str]:
        return None

    @abstractmethod
    async def __call__(self, args: argparse.Namespace, /):
        # Override default config with command line arguments:
        if args.database:
            default.db_url = args.database
        if args.hide_progress:
            default.progress_bar_class = None
        if args.log_level:
            default.log_level = args.log_level
        if args.log_file:
            default.log_file = args.log_file
        if args.processes:
            default.n_processes = args.processes
        if args.dry:
            default.dry = True
