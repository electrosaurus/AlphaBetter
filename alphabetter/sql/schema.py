import alphabetter.sql.types
import sqlalchemy as sa
import alphabetter.sql.types

from datetime import datetime
from alphabetter.core.model import *
from uuid import uuid4


sql_schema = sa.MetaData()

team_table = sa.Table(
    'team',
    sql_schema,
    sa.Column('id', sa.String(36), default=uuid4, unique=True),
    sa.Column('sport', sa.Enum(Sport), primary_key=True),
    sa.Column('country', alphabetter.sql.types.Country(), primary_key=True),
    sa.Column('name', sa.Text(), primary_key=True),
    sa.Index('team_id_idx', 'id'),
)

league_table = sa.Table(
    'league',
    sql_schema,
    sa.Column('id', sa.String(36), default=uuid4, unique=True),
    sa.Column('sport', sa.Enum(Sport), primary_key=True),
    sa.Column('name', sa.Text(), primary_key=True),
    sa.Column('category', sa.Text()),
    sa.Index('league_id_idx', 'id'),
)

tournament_table = sa.Table(
    'tournament',
    sql_schema,
    sa.Column('id', sa.String(36), default=uuid4, unique=True),
    sa.Column('league_id', sa.String(36), sa.ForeignKey(league_table.c.id), primary_key=True),
    sa.Column('season', sa.Text(), primary_key=True),
    sa.Index('tournament_league_idx', 'league_id'),
    sa.Index('tournament_id_idx', 'id'),
)

match_table = sa.Table(
    'match',
    sql_schema,
    sa.Column('id', sa.String(36), default=uuid4, unique=True),
    sa.Column('loaded_at', sa.DateTime(), default=datetime.now),
    sa.Column('tournament_id', sa.String(36), sa.ForeignKey(tournament_table.c.id), primary_key=True),
    sa.Column('tour', sa.SmallInteger),
    sa.Column('played_at', sa.DateTime(), primary_key=True),
    sa.Column('home_team_id', sa.String(36), sa.ForeignKey(team_table.c.id), primary_key=True),
    sa.Column('away_team_id', sa.String(36), sa.ForeignKey(team_table.c.id), primary_key=True),
    sa.Column('home_points', sa.Integer()),
    sa.Column('away_points', sa.Integer()),
    sa.Index('match_id_idx', 'id'),
)

odds_table = sa.Table(
    'odds',
    sql_schema,
    sa.Column('loaded_at', sa.DateTime(), default=datetime.now),
    sa.Column('bookmaker', sa.Enum(Bookmaker), primary_key=True),
    sa.Column('match_id', sa.String(36), sa.ForeignKey(match_table.c.id), primary_key=True),
    sa.Column('scanned_at', sa.DateTime(), primary_key=True),
    sa.Column('1', sa.Float()),
    sa.Column('X', sa.Float()),
    sa.Column('2', sa.Float()),
    sa.Column('1X', sa.Float()),
    sa.Column('12', sa.Float()),
    sa.Column('2X', sa.Float()),
    sa.CheckConstraint('"1" IS NULL OR "1" > 1'),
    sa.CheckConstraint('"X" IS NULL OR "X" > 1'),
    sa.CheckConstraint('"2" IS NULL OR "2" > 1'),
    sa.CheckConstraint('"1X" IS NULL OR "1X" > 1'),
    sa.CheckConstraint('"12" IS NULL OR "12" > 1'),
    sa.CheckConstraint('"2X" IS NULL OR "2X" > 1'),
)
