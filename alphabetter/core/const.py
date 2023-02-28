''' Constants. '''

from typing import List
from alphabetter.core.typing import Outcome


SINGLE_OUTCOMES: List[Outcome] = ['1', 'X', '2']
DOUBLE_OUTCOMES: List[Outcome] = ['1X', '12', '2X']
OUTCOMES: List[Outcome] = SINGLE_OUTCOMES + DOUBLE_OUTCOMES
OPTIONAL_OUTCOMES = OUTCOMES + ['0']
