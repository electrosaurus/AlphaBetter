''' Data models. '''

import pycountry

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Any, Optional
from enum import Enum
from alphabetter.core.text import decapitalize


class Decapitalizaple(object):
    def __format__(self, format_spec):
        match format_spec:
            case '_':
                return decapitalize(str(self))
            case unmatched_format_spec:
                return super().__format__(unmatched_format_spec)


class AlphaBetterObject(Decapitalizaple):
    pass


class AlphaBetterEnum(AlphaBetterObject, Enum):
    __names__ = {}

    @classmethod
    def from_string(cls, name: str, /):
        return getattr(cls, name.upper())

    def __repr__(self):
        return f'{self.__class__.__name__}.{self.name}'

    def __str__(self):
        return self.__names__[self.value]


class Country(AlphaBetterObject):
    __slots__ = ('_backbone', )

    def __init__(self, source: str, /):
        if len(source) == 2 and source.isupper():
            self._backbone = pycountry.countries.get(alpha_2=source)
        else:
            self._backbone = pycountry.countries.get(name=source)

    def __repr__(self):
        return f"Country('{self._backbone.alpha_2}')"

    def __str__(self):
        return self._backbone.alpha_2

    def __hash__(self):
        return hash(self._backbone.alpha_2)

    def __eq__(self, other):
        return isinstance(other, Country) and self._backbone == other._backbone

    def __lt__(self, other):
        return str(self) < str(other)


class Sport(AlphaBetterEnum):
    FOOTBALL = 1

    __names__ = {
        FOOTBALL: 'Football',
    }


class Bookmaker(AlphaBetterEnum):
    FONBET = 1

    __names__ = {
        FONBET: 'Fonbet',
    }


@dataclass
class Team:
    sport: Sport
    country: Country
    name: str

    def __hash__(self):
        return hash(self.sport) ^ hash(self.country) ^ hash(self.name)


@dataclass
class Odds(AlphaBetterObject):
    bookmaker: Bookmaker
    scanned_at: datetime
    home_win: Optional[float] = None
    draw: Optional[float] = None
    away_win: Optional[float] = None
    home_win_or_draw: Optional[float] = None
    win: Optional[float] = None
    away_win_or_draw: Optional[float] = None


@dataclass
class League(AlphaBetterObject):
    sport: Sport
    name: str
    tournaments: List[Any] = field(default_factory=list, repr=False)

    def __str__(self):
        return f'{self.sport} league "{self.name}"'

    def __format__(self, format_spec):
        match format_spec:
            case 'l':
                return decapitalize(str(self))
            case unmatched_format_spec:
                return super().__format__(unmatched_format_spec)

    def __hash__(self):
        return hash(self.sport) ^ hash(self.name)


@dataclass
class Tournament(AlphaBetterObject):
    league: League
    season: str
    matches: List[Any] = field(default_factory=list, repr=False)

    def __str__(self):
        return f'{self.season} tournament of {self.league:l}'

    def __hash__(self):
        return hash(self.league) ^ hash(self.season)


@dataclass
class Match(AlphaBetterObject):
    tournament: Tournament
    played_at: datetime
    home_team: Team
    away_team: Team
    home_points: int
    away_points: int
    odds: List[Odds] = field(default_factory=list)
    predictions: List[Odds] = field(default_factory=list, repr=False)

    @property
    def is_home_disqualified(self):
        return self.home_points == -1

    @property
    def is_away_disqualified(self):
        return self.away_points == -1

    def __hash__(self): # TODO
        return hash(self.tournament) ^ hash(self.played_at) ^ hash(self.home_team) ^ hash(self.away_points)

    def __str__(self):
        return f'Match between {self.home_team.name} and {self.away_team.name} played {self.played_at:on %b %d, %Y at %H:%M}'
