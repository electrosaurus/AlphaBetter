''' ORM implementation. '''

import sqlalchemy.orm

from alphabetter.core.model import *
from alphabetter.sql.schema import *


sqlalchemy.orm.mapper(
    Team,
    team_table,
    confirm_deleted_rows=False,
)

sqlalchemy.orm.mapper(
    League,
    league_table,
    properties = {
        'tournaments': sqlalchemy.orm.relation(
            Tournament,
            order_by=(tournament_table.c.season),
            lazy=True,
        ),
    },
)

sqlalchemy.orm.mapper(
    Tournament,
    tournament_table,
    properties = {
        'league': sqlalchemy.orm.relation(League, back_populates='tournaments'),
        'matches': sqlalchemy.orm.relation(Match,
            order_by=(match_table.c.played_at),
            cascade='save-update, merge, delete',
        ),
    },
)

sqlalchemy.orm.mapper(
    Match,
    match_table,
    properties = {
        'tournament': sqlalchemy.orm.relation(Tournament, back_populates='matches'),
        'home_team': sqlalchemy.orm.relationship(
            Team,
            foreign_keys=[match_table.c.home_team_id],
        ),
        'away_team': sqlalchemy.orm.relationship(
            Team,
            foreign_keys=[match_table.c.away_team_id],
        ),
        'odds': sqlalchemy.orm.relationship(
            Odds,
            order_by=(
                odds_table.c.bookmaker,
                odds_table.c.scanned_at,
            ),
            cascade='save-update, merge, delete, delete-orphan',
        ),
    },
)

sqlalchemy.orm.mapper(
    Odds,
    odds_table,
    properties = {
        'home_win': odds_table.c['1'],
        'draw': odds_table.c['X'],
        'away_win': odds_table.c['2'],
        'home_win_or_draw': odds_table.c['1X'],
        'win': odds_table.c['12'],
        'away_win_or_draw': odds_table.c['2X'],
    },
)
