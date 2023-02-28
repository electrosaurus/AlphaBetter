''' Methods to process pandas data frames. '''

import pandas as pd

from typing import Literal, Optional, overload, Iterable, Dict
from alphabetter.config import default as config
from io import StringIO
from alphabetter.core.text import wrap_text_middle, abbreviate_name


@overload
def abbreviate_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    abbreviations: Iterable[str],
    legends: Optional[Iterable[str]] = None,
    *,
    inplace: Literal[False] = ...,
) -> pd.DataFrame: ...

@overload
def abbreviate_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    abbreviations: Iterable[str],
    legends: Optional[Iterable[str]] = None,
    *,
    inplace: Literal[True] = ...,
): ...

def abbreviate_columns(
    df: pd.DataFrame,
    columns: Iterable[str],
    abbreviations: Iterable[str],
    legends: Optional[Iterable[str] | Dict[str, str]] = None,
    *,
    inplace: bool = False,
) -> Optional[pd.DataFrame]:
    if not legends:
        legend_dict = {}
    elif isinstance(legends, Dict):
        legend_dict = legends
    elif isinstance(legends, Iterable):
        legend_dict = dict(zip(abbreviations, legends))
    else:
        raise TypeError(legends)
    df.attrs.setdefault('legend', {}).update(legend_dict)
    return df.rename(columns=dict(zip(columns, abbreviations)), inplace=inplace)


def tabulate_df(df: pd.DataFrame, /, legend_indent: int = 2, **kwargs) -> str:
    kwargs.setdefault('tablefmt', config.table_format)
    buffer = StringIO()
    df.to_markdown(buffer, **kwargs)
    legend: Optional[Dict[str, str]] = df.attrs.get('legend')
    buffer.write('\n')
    if legend:
        buffer.write('\n')
        for n, (column, description) in enumerate(legend.items(), start=1):
            if n == 1:
                buffer.write('*' + ' ' * (legend_indent - 1))
            else:
                buffer.write(' ' * legend_indent)
            buffer.write(f'{column} - {description}')
            if n < len(legend):
                buffer.write('\n')
    buffer.seek(0)
    return buffer.read()


@overload
def limit_df_width(
    df: pd.DataFrame,
    /,
    column_width_limits: Dict[str, int],
    *,
    abbreviate: bool = False,
    inplace: Literal[False] = ...,
) -> pd.DataFrame: ...

@overload
def limit_df_width(
    df: pd.DataFrame,
    /,
    column_width_limits: Dict[str, int],
    *,
    abbreviate: bool = False,
    inplace: Literal[True] = ...,
): ...

def limit_df_width(
    df: pd.DataFrame,
    /,
    column_width_limits: Dict[str, int],
    *,
    abbreviate: bool = False,
    inplace: bool = False,
) -> Optional[pd.DataFrame]:
    if not inplace:
        df = df.copy()
    if abbreviate:
        f = lambda x: wrap_text_middle(abbreviate_name(x), width_limit)
    else:
        f = lambda x: wrap_text_middle(x, width_limit)
    for column, width_limit in column_width_limits.items():
        df[column] = df[column].apply(f)
    if not inplace:
        return df
