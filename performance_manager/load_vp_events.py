#!/usr/bin/env python

import argparse
import logging
import os
import sys
import pathlib

from typing import List

import sqlalchemy
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.orm import sessionmaker

from lib import (
    SqlBase,
    MetadataLog,
    get_experimental_engine,
    get_local_engine,
    get_vp_dataframe,
    transform_vp_dtyes,
    transform_vp_timestamps,
    merge_vehicle_position_events,
)

logging.getLogger().setLevel("INFO")
DESCRIPTION = """Entry Point to RDS Manipulating Performance Manager Scripts"""

LOAD_PATHS = [
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=0/ade29e12f6144ebdaa27c86083a358b3-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=0/eecaaf159f804e9686f4d16f816a708f-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=1/2e14b3724450454d8bd2578e63995dae-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=1/3338ab3b2f9b4026881f98c902f38fa9-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=10/de3b915cfcef44209b5ace740fc1ef10-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=11/b43713047a7f4c7fa73a4fcdc940e705-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=12/fd6d28ac53aa423ca42d5fc80a979ce9-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=13/7e1adf4011154fd386901d5b28a2c28e-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=13/a921124425c944d1946ad1fc46355d53-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=14/071933d3aed3425fa31bca7f5f74f1d9-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=14/63734a60780648feb58935c34f2a03df-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=15/2345a4a28378478a995b37f48c583b57-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=15/a4bdc90df46c42138d2454d02d5ec122-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=16/49c65138958c46b8a364658634ce30c7-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=16/75a6529321104c4fbd08909eba65db9a-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=17/495f76d247634450b22328a258f371fe-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=17/cee084eb6b4c471da3533a8500da46cc-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=18/23e064972bdc473f91dfada3f6b6a625-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=18/2798428d505c4c59be110d1d4e78f60e-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=19/42e48facf2f84915afafe61231120f13-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=2/8ee0a38332a0483b908dae17f0161a56-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=2/9de631e257734c51afbfbc8c3d702671-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=20/d734fc0c435241a38d5a11bf27dbdde1-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=21/6f2cdf8025e6430883fcc9fee4df5724-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=22/280d109b4a2b441f8a327288ba552415-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=23/b7fae2ba0f9f4be699610d27ca23abe5-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=23/e0e670a6703d43418decf83a8fe3649f-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=3/5ad9bdd8bc384fdd86cf96dd7d2b701c-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=3/fef931562fa542f096017842dc178d1a-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=4/b1438241339d42c3b05ee992e0ee0c71-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=4/f68bc49b8eee4f44bb1023089168f313-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=5/8273ddeb965f4d10a39906b2c19289a5-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=5/c1302ae948f14b8187df0e8db0e4fab6-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=6/12fca6fd9db24fa7a78eb00d9c676bd4-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=6/853ffa7819da48a19afa1b9455d3c650-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=6/a7edbddbff6e48ce9990a051f40e6ac7-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=7/1ee62ea8fd854168afbd6c6bc2c601e7-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=7/ae1cbb8f7747449688891e92005c7524-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=7/c3b900489cf04bdc9c1d5e6ca1b92a2f-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=8/343572b5307249209d022cb4690930b0-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=8/580de8523f39459486f3e34b2ca40344-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=9/647c1c01e703466fb9679a380ad89ac4-0.parquet"
    },
    {
        "path": "mbta-ctd-dataplatform-dev-springboard/lamp/RT_VEHICLE_POSITIONS/year=2022/month=7/day=20/hour=9/6635c9551e4b4c2ca64152bd167f751a-0.parquet"
    },
]


def load_environment() -> None:
    """
    boostrap .env file for local development
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        here = os.path.dirname(os.path.abspath(__file__))
        env_file = os.path.join(here, "..", ".env")
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    except Exception as exception:
        logging.error("error while trying to bootstrap")
        logging.exception(exception)
        raise exception


def parse_args(args: List[str]) -> argparse.Namespace:
    """parse args for running this entrypoint script"""
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        "--experimental",
        action="store_true",
        dest="experimental",
        help="if set, use a sqllite engine for quicker development",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        dest="seed",
        help="if set, seed metadataLog table with paths",
    )

    return parser.parse_args(args)


def get_db_engine(
    experimental: bool, seed: bool
) -> sqlalchemy.orm.sessionmaker:
    """
    create database orm objects, return engine for db operations
    """
    if experimental:
        sql_engine = get_experimental_engine(echo=True)
    else:
        sql_engine = get_local_engine(echo=True)

    SqlBase.metadata.drop_all(sql_engine)
    SqlBase.metadata.create_all(sql_engine)

    db_session = sessionmaker(bind=sql_engine)

    if seed:
        with db_session.begin() as cur:  # type: ignore
            cur.execute(insert(MetadataLog.__table__), LOAD_PATHS)
            cur.commit()

    return db_session


if __name__ == "__main__":
    load_environment()

    parsed_args = parse_args(sys.argv[1:])

    session = get_db_engine(parsed_args.experimental, parsed_args.seed)

    """
    pull list of objects that need processing from metadata table
    group objects by similar hourly folders
    """
    read_md_log = select((MetadataLog.id, MetadataLog.path)).where(
        MetadataLog.processed == 0
    )
    with session.begin() as s:  # type: ignore
        paths_to_load: dict[str, dict[str, list]] = {}
        for path_id, path in s.execute(read_md_log):
            path = pathlib.Path(path)
            if path.parent not in paths_to_load:
                paths_to_load[path.parent] = {"ids": [], "paths": []}
            paths_to_load[path.parent]["ids"].append(path_id)
            paths_to_load[path.parent]["paths"].append(str(path))

    # for folder in list(paths_to_load.keys())[:2]:
    for folder in paths_to_load.keys():
        ids = paths_to_load[folder]["ids"]
        paths = paths_to_load[folder]["paths"]

        try:
            new_events = get_vp_dataframe(paths)
            logging.info(
                "Size of dataframe from %s is %d", folder, new_events.shape[0]
            )
            new_events = transform_vp_dtyes(new_events)
            logging.info(
                "Size of dataframe with updated dtypes is %d",
                new_events.shape[0],
            )
            new_events = transform_vp_timestamps(new_events)
            logging.info(
                "Size of dataframe with transformed timestamps is %d",
                new_events.shape[0],
            )

            merge_vehicle_position_events(str(folder), new_events, session)
        except Exception as e:
            logging.exception(e)
        else:
            update_md_log = (
                update(MetadataLog.__table__)
                .where(MetadataLog.id.in_(ids))
                .values(processed=1)
            )
            with session.begin() as s:  # type: ignore
                s.execute(update_md_log)
