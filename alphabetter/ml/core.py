''' Base classes for machine learning. '''

from __future__ import annotations

import pandas as pd
import numpy as np
import pickle
import os

from abc import ABC, abstractmethod, abstractclassmethod
from typing import Tuple, Any, List, Set
from sklearn.base import TransformerMixin
from typing import overload, Optional, Literal, Dict, Any, Self
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from uuid import UUID
from hashlib import md5
from alphabetter.config import default as config
from alphabetter.core import *


class Features(ABC, TransformerMixin):
    def fit(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


class Splitter(ABC):
    def __init__(
        self,
        *,
        max_train_size: Optional[int] = None,
        train_draws: bool = True,
        random_state: Optional[int] = None,
        sort: bool = False,
        droppers: Optional[List[Dropper]] = None,
    ):
        self._max_train_size = max_train_size
        self._train_draws = train_draws
        self._random_state = random_state
        self._sort = sort
        self._droppers = droppers or []

    def __call__(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        df = df.match.drop_without_points()
        train_df, test_df = self._split(df)
        if not self._train_draws:
            train_df = train_df.match.drop_draws()
        if self._max_train_size is not None and len(train_df) > self._max_train_size:
            train_df = train_df.sample(self._max_train_size, random_state=self._random_state)
        for dropper in self._droppers:
            dropper.fit(train_df)
            test_df = dropper.drop(test_df)
        if self._sort:
            train_df = train_df.sort_values('match.played_at')
            test_df = test_df.sort_values('match.played_at')
        else:
            train_df = train_df.copy()
            test_df = test_df.copy()
        base_repr = df.attrs.get('representativeness', 1.0)
        train_df.attrs['representativeness'] = base_repr * len(train_df) / len(df)
        test_df.attrs['representativeness'] = base_repr * len(test_df) / len(df)
        return train_df, test_df

    @abstractmethod
    def _split(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        raise NotImplementedError()


class CrossValidator(ABC):
    def __init__(
        self,
        splitter: Splitter,
        n_splits: int,
        *,
        median_split_metrics: Optional[str] = None,
    ):
        self._splitter = splitter
        self._n_splits = n_splits
        self._median_split_metrics = median_split_metrics

    @abstractclassmethod
    def _fit_score(cls, subj, train_df: pd.DataFrame, test_df: pd.DataFrame, /) -> Dict[str, Any]:
        raise NotImplementedError()

    @staticmethod
    def _process_split(arg):
        cls, n, subj, train_df, test_df = arg
        score_dict = cls._fit_score(subj, train_df, test_df)
        score_dict['Split'] = n + 1
        return score_dict, test_df

    def _process_iterator(self, iterable):
        progress_bar = create_progress_bar(
            iterable=iterable,
            total=self._n_splits,
            unit='split',
        )
        splitting_scores = []
        best_splitting_score = np.inf
        median_test_df = None
        score_dicts = []
        for score_dict, test_df in progress_bar:
            score_dicts.append(score_dict)
            if self._median_split_metrics is None:
                continue                
            splitting_score = score_dict[self._median_split_metrics]
            splitting_scores.append(splitting_score)
            median_splitting_score = np.median(splitting_scores)
            new_dist = abs(splitting_score - median_splitting_score)
            old_dist = abs(best_splitting_score - median_splitting_score)
            if new_dist < old_dist:
                best_splitting_score = splitting_score
                median_test_df = test_df
        score_df = pd.DataFrame(score_dicts).set_index('Split', drop=True)
        if self._median_split_metrics is None:
            return score_df
        return score_df, median_test_df

    @overload
    def __call__(self, subj: Any, df: pd.DataFrame, /, *, n_processes: Optional[int] = None) -> \
        pd.DataFrame: ...

    @overload
    def __call__(self, subj: Any, df: pd.DataFrame, /, *, n_processes: Optional[int] = None) -> \
        Tuple[pd.DataFrame, pd.DataFrame]: ...

    def __call__(self, subj: Any, df: pd.DataFrame, /, *, n_processes: Optional[int] = None):
        args = [(type(self), n, subj, *self._splitter(df)) for n in range(self._n_splits)]
        if n_processes == 1:
            return self._process_iterator(map(CrossValidator._process_split, args))
        else:
            with ProcessPoolExecutor(n_processes or config.n_processes) as executor:
                return self._process_iterator(executor.map(CrossValidator._process_split, args))


class Dropper(ABC):
    def fit(self, df: pd.DataFrame):
        pass

    @abstractmethod
    def drop(self, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()


class Model(ABC):
    @abstractclassmethod
    def _get_subdir(cls) -> Path:
        raise NotImplementedError()

    @abstractmethod
    def __repr__(self):
        raise NotImplementedError()

    def save(self, dir: Optional[Path] = None, name: Optional[str] = None) -> Path:
        if not dir:
            dir = config.data_dir / 'models' / self._get_subdir()
        if not name:
            name = str(UUID(md5(repr(self).encode('utf-8')).hexdigest()))
        path = dir / (name + '.pickle')
        with open(path, 'wb') as file:
            pickle.dump(self, file)
        return path

    @classmethod
    def load(cls, dir: Optional[Path] = None, name: Optional[str] = None) -> Self:
        if not dir:
            dir = config.data_dir / 'models' / cls._get_subdir()
        if name:
            path = dir / (name + '.pickle')
        else:
            path = max(dir.glob('*.pickle'), key=os.path.getctime)
        with open(path, 'rb') as file:
            obj = pickle.load(file)
            if not isinstance(obj, cls):
                raise TypeError()
            return obj


class Predictor(Model, ABC):
    def fit(self, df: pd.DataFrame):
        pass

    @overload
    @abstractmethod
    def predict(self, df: pd.DataFrame, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    @abstractmethod
    def predict(self, df: pd.DataFrame, *, inplace: Literal[True] = ...): ...

    @abstractmethod
    def predict(self, df: pd.DataFrame, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        raise NotImplementedError()

    @classmethod
    def _get_subdir(cls):
        return Path('predictors')


class Better(Model, ABC):
    def fit(self, df: pd.DataFrame, /):
        pass

    @abstractmethod
    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @abstractmethod
    @overload
    def bet(self, df: pd.DataFrame, /, *, inplace: Literal[True] = ...): ...

    @abstractmethod
    def bet(self, df: pd.DataFrame, /, *, inplace: bool = False) -> Optional[pd.DataFrame]:
        raise NotImplementedError()

    @classmethod
    def _get_subdir(cls):
        return Path('betters')


class Accountant(Model, ABC):
    def __init__(self, capital: float = 1.0):
        self._balance = capital
        self._capital = capital

    def reset(self):
        self._balance = self._capital

    def set_balance(self, balance: float, /):
        self._balance = balance

    @abstractmethod
    def invest(self, expediency: float = 0.5, *, dry: bool = False) -> float:
        raise NotImplementedError()

    def inc_balance(self, value: float, /):
        if value < 0:
            raise ValueError(f'Negative balance increment {value}.')
        self._balance += value

    @property
    def capital(self) -> float:
        return self._capital

    @property
    def balance(self) -> float:
        return self._balance

    @classmethod
    def _get_data_subdir(cls):
        return Path('accountants')

    def fit(self, df: pd.DataFrame, /):
        self.reset()

    @overload
    def invest_serial(self, df: pd.DataFrame, /, *, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def invest_serial(self, df: pd.DataFrame, /, *, inplace: Literal[True] = ...): ...

    def invest_serial(self, df: pd.DataFrame, /, *, inplace: bool = False):
        accounting_data_list = []
        serial_bet_roi_df = df.bet.df.join(df.bet.roi())
        for bet in serial_bet_roi_df.itertuples():
            if bet.outcome == '0':
                investment = 0.0
                profit = 0.0
            else:
                investment = self.invest(bet.expediency)
                profit = investment * bet.roi
                self.inc_balance(investment + profit)
            accounting_data = {
                'investment': investment,
                'profit': profit,
                'balance': self.balance,
            }
            accounting_data_list.append(accounting_data)
        accounting_df = pd.DataFrame(
            index=df.index,
            data=accounting_data_list,
        ).reindex(df.index).astype('f4')
        if not inplace:
            return accounting_df
        df['accounting.' + accounting_df.columns] = accounting_df

    @overload
    def invest_parallel(self, df: pd.DataFrame, /, *, inplace: Literal[False] = ...) -> pd.Series: ...

    @overload
    def invest_parallel(self, df: pd.DataFrame, /, *, inplace: Literal[True] = ...): ...

    def invest_parallel(self, df: pd.DataFrame, /, *, inplace: bool = False):
        balance_backup = self.balance
        def invest_from_backup(isp):
            self.set_balance(balance_backup)
            return self.invest(isp)
        investment: pd.Series = df.bet.expediency.apply(lambda expediency: np.nan if np.isnan(expediency) else invest_from_backup(expediency))
        self.set_balance(balance_backup)
        if not inplace:
            investment.rename('investment', inplace=True)
            return investment
        df['accounting.investment'] = investment

    @classmethod
    def _get_subdir(cls):
        return Path('accountants')


class DataFrameAccessor(ABC):
    @abstractclassmethod
    def _get_namespace(cls) -> str:
        raise NotImplementedError()

    @abstractclassmethod
    def _get_required_columns(cls) -> Set[str]:
        raise NotImplementedError()

    @property
    def prefix(self):
        return self._get_namespace() + '.'

    def __init__(self, df: pd.DataFrame, /):
        self._df = df
        if not self:
            columns = [f'"{self.prefix}{column}"' for column in self._get_required_columns()]
            raise TypeError(f'DataFrame must have the following columns: {humanize_list(columns)}.')

    def __getitem__(self, key: str, /) -> Any:
        return self._df[self.prefix + key]

    def __setitem__(self, key: str, value, /):
        if column := self.prefix + key in self._df.columns:
            self._df[column] = value
            return
        for i, column in reversed(list(enumerate(self._df.columns))):
            if column.startswith(self.prefix): # type: ignore
                column_index = i + 1
                break
        else:
            column_index = self._df.shape[1]
        self._df.insert(column_index, self.prefix + key, value)

    def __getattr__(self, name: str, /) -> Any:
        return self[name]

    def __setattr__(self, name: str, value: Any, /):
        if (column := self.prefix + name) in self.__dict__.get('_df', set()):
            self._df[column] = value
            return
        super().__setattr__(name, value)

    def __bool__(self) -> bool:
        return all(column in self.columns for column in self._get_required_columns())

    @property
    def _column_mapping(self):
        return {
            column: column[len(self.prefix):] # type: ignore
            for column in self._df.columns
            if column.startswith(self.prefix) # type: ignore
        }

    @property
    def columns(self) -> pd.Index:
        return pd.Index(self._column_mapping.values())

    @property
    def prefixed_columns(self) -> pd.Index:
        return self.prefix + self.columns

    @property
    def df(self) -> pd.DataFrame:
        column_mapping = self._column_mapping
        df = self._df[list(column_mapping)].rename(columns=column_mapping)
        index_names = list(df.index.names)
        for i, index_name in enumerate(index_names):
            if index_name.startswith(self.prefix):
                index_names[i] = index_name[len(self.prefix):]
            else:
                index_names[i] = '_'.join(index_name.split('.', 1))
        df.index.names = index_names
        return df

    @df.setter
    def df(self, value):
        self._df[self.prefix + self.columns] = value

    @overload
    def drop(self, inplace: Literal[False] = ...) -> pd.DataFrame: ...

    @overload
    def drop(self, inplace: Literal[True] = ...): ...

    def drop(self, inplace: bool = False) -> Optional[pd.DataFrame]:
        return self._df.drop(columns=list(self._column_mapping), inplace=inplace)
