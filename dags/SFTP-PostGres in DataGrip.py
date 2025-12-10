from airflow.decorators import dag, task
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import re
import zipfile
import pandas as pd
from io import StringIO


@dag(
    default_args={
        "owner": "airflow",
        "retries": 0,
        "backfill": False
    },
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["sftp", "postgres", "taskflow"],
)


def sftp_corelogic():
    @task()
    # Check and print latest table in the SFTP transfer
    