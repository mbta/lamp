import logging
import os
import urllib.parse

import sqlalchemy as sa
from sqlalchemy.engine import URL as DbUrl  # type: ignore
from sqlalchemy.ext.declarative import declarative_base


def get_local_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:  # type: ignore
    """
    Get an SQL Alchemy engine that connects to a locally Postgres RDS stood up
    via docker using env variables
    """
    try:
        host = os.environ["DB_HOST"]
        dbname = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        port = os.environ["DB_PORT"]
        password = urllib.parse.quote_plus(os.environ["DB_PASSWORD"])

        database_url = DbUrl.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=dbname,
        )
        logging.info("creating engine for %s", database_url.render_as_string())
        engine = sa.create_engine(database_url, echo=echo, future=True)
        return engine
    except Exception as exception:
        logging.error("Error Creating Sql Engine")
        logging.exception(exception)
        raise exception


def get_experimental_engine(
    echo: bool = False,
) -> sa.future.engine.Engine:  # type: ignore
    """
    return lightweight engine using local memeory that doens't require a
    database to be stood up. great for testing from within the shell.
    """
    engine = sa.create_engine(
        "sqlite+pysqlite:///:memory:", echo=echo, future=True
    )
    return engine


def get_aws_engine() -> sa.future.engine.Engine:  # type: ignore
    """
    return an engine connected to our aws rds
    """
    # TODO(zap) - figure out how to connect to the AWS RDS for Writing


SqlBase = declarative_base()


class StaticSubHeadway(SqlBase):  # pylint: disable=too-few-public-methods
    """Table for Static Subway Headway information"""

    __tablename__ = "staticSubwayHeadways"

    id = sa.Column(sa.Integer, primary_key=True)
    trip_id = sa.Column(sa.String(100))
    arrival_time = sa.Column(sa.String(10))
    departure_time = sa.Column(sa.String(10))
    stop_id = sa.Column(sa.Integer)
    stop_sequence = sa.Column(sa.Integer)
    pickup_type = sa.Column(sa.Integer)
    drop_off_type = sa.Column(sa.Integer)
    # TODO(zap) this should be an optional int, but it currently throws an
    # error if its a pandas.nan
    timepoint = sa.Column(sa.String)
    checkpoint_id = sa.Column(sa.String)
    route_type = sa.Column(sa.Integer)
    route_id = sa.Column(sa.String(60))
    service_id = sa.Column(sa.String(60))
    direction_id = sa.Column(sa.Integer)
    parent_station = sa.Column(sa.String(60))
    departure_time_sec = sa.Column(sa.Float)
    prev_departure_time_sec = sa.Column(sa.Float)
    head_way = sa.Column(sa.Integer)

    def __repr__(self) -> str:
        """this is just a helper string for debugging."""
        return f"( id='{self.id}', trip_id='{self.trip_id}', arrival_time='{self.arrival_time}', departure_time='{self.departure_time}', stop_id='{self.stop_id}', stop_sequence='{self.stop_sequence}', pickup_type='{self.pickup_type}', drop_off_type='{self.drop_off_type}', timepoint='{self.timepoint}', checkpoint_id='{self.checkpoint_id}', route_type='{self.route_type}', route_id='{self.route_id}', service_id='{self.service_id}', direction_id='{self.direction_id}', parent_station='{self.parent_station}', departure_time_sec='{self.departure_time_sec}', prev_departure_time_sec='{self.prev_departure_time_sec}', head_way='{self.head_way}')"
