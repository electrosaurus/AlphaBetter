import sqlalchemy.orm
import sqlalchemy as sa
import logging

from datetime import datetime, timedelta
from alphabetter.core.model import Sport, Country, Team, Match, Tournament, League
from alphabetter.config import default as config
from difflib import SequenceMatcher
from typing import Optional, Self


logger = logging.getLogger(__name__)


class SQLSession(sqlalchemy.orm.Session):
    def find_match(
        self,
        sport: Sport,
        league_name: str,
        played_at: datetime,
        home_team_name: str,
        away_team_name: str,
        home_team_country: Optional[Country] = None,
        away_team_country: Optional[Country] = None,
        played_at_precision: timedelta = timedelta(days=1),
        team_name_precision: float = 0.5,
    ) -> Optional[Match]:
        HomeTeam = sqlalchemy.orm.aliased(Team)
        AwayTeam = sqlalchemy.orm.aliased(Team)
        matches = (self.query(Match)
            .join(Tournament)
            .join(League)
            .join(HomeTeam, Match.home_team)
            .join(AwayTeam, Match.away_team)
            .filter(
                League.sport == sport,
                League.name == league_name,
                Match.played_at >= played_at - played_at_precision,
                Match.played_at <= played_at + played_at_precision,
                not home_team_country or HomeTeam.country == home_team_country,
                not away_team_country or AwayTeam.country == away_team_country,
            )
            .all()
        )
        matches_team_name_similarity = (
            (
                match, (
                    SequenceMatcher(None, match.home_team.name, home_team_name).ratio()**2 +
                    SequenceMatcher(None, match.away_team.name, away_team_name).ratio()**2
                ) ** 0.5
            )
            for match in matches
        )
        matches_team_name_similarity = [
            (match, team_name_similarity)
            for match, team_name_similarity in matches_team_name_similarity
            if team_name_similarity >= team_name_precision
        ]
        if not matches_team_name_similarity:
            return None
        return max(matches_team_name_similarity, key=lambda x: x[1])[0]

    @classmethod
    def from_url(cls, url: Optional[str] = None) -> Self:
        db_engine = sa.create_engine(url or config.db_url)
        sql_session_maker.configure(bind=db_engine)
        sql_session = sql_session_maker()
        assert isinstance(sql_session, cls)
        return sql_session

    def commit(self):
        if config.dry:
            return
        logger.debug('Commited to the database.')
        super().commit()


sql_session_maker = sqlalchemy.orm.sessionmaker(class_=SQLSession, autoflush=True)
