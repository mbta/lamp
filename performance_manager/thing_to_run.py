#!/usr/bin/env python

import pandas as pd
import numpy as np
import boto3
import time
import pathlib
import pickle
import psycopg2
import os
import logging

DBNAME=os.environ['DB_NAME']
HOST = os.environ['DB_HOST']
PASSWORD=os.environ['DB_PASSWORD']
PORT=os.environ['DB_PORT']
USER=os.environ['DB_USER']

def get_aws_connection():
    # TODO - figure out how to connect to the AWS RDS for Writing
    #
    # https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Python.html

    #gets the credentials from .aws/credentials
    session = boto3.Session()
    client = session.client('rds')

def get_local_connection():
    """get a connection to the locally running postgres db"""
    return psycopg2.connect(
        host=HOST,
        dbname=DBNAME,
        user=USER,
        port=PORT,
        password=PASSWORD
    )


def main():
    try:
        logging.info("Connecting to %s", DBNAME)
        connection = get_local_connection()
        cur = connection.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        logging.info(query_results)
    except Exception as exception:
        logging.error("Database connection failed")
        logging.exception(exception)

if __name__ == "__main__":
    main()
