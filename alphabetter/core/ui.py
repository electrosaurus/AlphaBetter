''' Tools to implement user interface. '''

from alphabetter.config import default
from tqdm import tqdm


def create_progress_bar(*args, disable: bool = False, **kwargs) -> tqdm:
    progress_bar_class = default.progress_bar_class or tqdm
    return progress_bar_class(
        *args,
        **kwargs,
        disable=disable or not default.progress_bar_class,
        leave=default.leave_progress_bar,
    )
