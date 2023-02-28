''' Logging facilities. '''

import logging
import termcolor

from typing import Optional, Dict


LOG_LEVEL_STATUS = logging.INFO - 1


logging.addLevelName(LOG_LEVEL_STATUS, 'STATUS')


class ColoredLogFormatter(logging.Formatter):
    _default_level_colors = {
        logging.DEBUG: 'light_grey',
        logging.INFO: 'green',
        LOG_LEVEL_STATUS: 'cyan',
        logging.WARNING: 'yellow',
        logging.ERROR: 'red',
        logging.CRITICAL: 'light_red',
    }

    def __init__(self, level_colors: Optional[Dict[int, str]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._formatters = {
            level: logging.Formatter(self._fmt and termcolor.colored(self._fmt, color) or self._fmt)
            for level, color in (level_colors or self._default_level_colors).items()
        }

    def format(self, record):
        formatter = self._formatters.get(record.levelno)
        return formatter.format(record) if formatter else super().format(record)
