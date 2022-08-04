import boto3
import logging
import os

from datetime import datetime

from sqlalchemy import (
    create_engine,
    TypeDecorator,
    Table,
    Column,
    Integer,
    String,
    Time,
    Float,
    BigInteger
)

from sqlalchemy.engine import URL as EngineUrl
from sqlalchemy.ext.declarative import declarative_base

import urllib.parse  # used for correctly escaping passwords


def get_local_engine():
    try:
        host = os.environ["DB_HOST"]
        dbname = os.environ["DB_NAME"]
        user = os.environ["DB_USER"]
        port = os.environ["DB_PORT"]
        password = urllib.parse.quote_plus(os.environ["DB_PASSWORD"])

        database_url = EngineUrl.create(
            drivername="postgresql+psycopg2",
            username=user,
            password=password,
            host=host,
            port=port,
            database=dbname,
        )
        logging.info("creating engine for %s", database_url.render_as_string())
        engine = create_engine(database_url, echo=False, future=True)
        return engine
    except Exception as exception:
        logging.error("Error Creating Sql Engine")
        logging.exception(exception)
        raise exception


def get_experimental_engine():
    """
    return lightweight engine using local memeory that doens't require a db to
    be stood up. great for testing from within the shell.
    """
    return create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)


def get_aws_connection():
    # TODO - figure out how to connect to the AWS RDS for Writing
    #
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Python.html

    # gets the credentials from .aws/credentials
    session = boto3.Session()
    client = session.client("rds")

SqlBase = declarative_base()

class StaticSubHeadway(SqlBase):
    __tablename__ = 'staticSubwayHeadways'

    id = Column(Integer, primary_key=True)
    trip_id = Column(String(100))
    arrival_time = Column(String(10))
    departure_time = Column(String(10))
    stop_id = Column(BigInteger)
    stop_sequence = Column(Integer)
    pickup_type = Column(Integer)
    drop_off_type = Column(Integer)
    # TODO this should be an optional int, but it currently throws an error if
    # its a pandas.nan
    timepoint = Column(String)
    checkpoint_id = Column(String)
    route_type = Column(Integer)
    route_id = Column(String(60))
    service_id = Column(String(60))
    direction_id = Column(Integer)
    parent_station = Column(String(60))
    departure_time_sec = Column(Float)
    prev_departure_time_sec = Column(Float)
    head_way = Column(Integer)

    def __repr__(self):
        """ this is just a helper string for debugging.  """
        return f"( id='{self.id}', trip_id='{self.trip_id}', arrival_time='{self.arrival_time}', departure_time='{self.departure_time}', stop_id='{self.stop_id}', stop_sequence='{self.stop_sequence}', pickup_type='{self.pickup_type}', drop_off_type='{self.drop_off_type}', timepoint='{self.timepoint}', checkpoint_id='{self.checkpoint_id}', route_type='{self.route_type}', route_id='{self.route_id}', service_id='{self.service_id}', direction_id='{self.direction_id}', parent_station='{self.parent_station}', departure_time_sec='{self.departure_time_sec}', prev_departure_time_sec='{self.prev_departure_time_sec}', head_way='{self.head_way}')"
