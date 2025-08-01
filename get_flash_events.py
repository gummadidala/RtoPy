# -*- coding: utf-8 -*-
"""
get_flash_events.py

Created on Tue Feb 25 19:13:16 2020

@author: V0010894
"""

import os
import sys
import yaml
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as sq
import io
import boto3
import logging

from mark1_logger import mark1_logger
from config import get_date_from_string, query_athena


s3 = boto3.client("s3")
ath = boto3.client("athena")

base_path = "."

logs_path = os.path.join(base_path, "logs")
if not os.path.exists(logs_path):
    os.mkdir(logs_path)
logger = mark1_logger(
    os.path.join(logs_path, f'get_flash_events_{datetime.today().strftime("%F")}.log')
)
logger.setLevel(logging.INFO)


if __name__ == "__main__":

    with open("Monthly_Report.yaml") as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.FullLoader)

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = get_date_from_string(
            conf["start_date"], s3bucket=conf["bucket"], s3prefix="mark/flash_events"
        )
        end_date = conf["end_date"]
    end_date = get_date_from_string(end_date)

    # Placeholder for manual override of start/end dates
    # start_date = '2019-12-18'
    # end_date = '2019-11-09'

    # engine = get_atspm_engine()

    bucket = conf["bucket"]

    dates = pd.date_range(start_date, end_date, freq="1D")

    for date_ in dates:
        date_str = date_.strftime("%F")

        athena = conf["athena"]

        query = f"""
            SELECT signalid as SignalID, 
                   timestamp as TimeStamp, 
                   eventparam as EventParam
            FROM {athena['atspm_table']} 
            WHERE eventcode = 173 
            AND date = '{date_str}'
        """
        flashes = query_athena(
            query, database=athena["database"], staging_dir=athena["staging_dir"]
        )

        if not flashes.empty:
            flashes["SignalID"] = flashes["SignalID"].astype(str)
            flashes["TimeStamp"] = pd.to_datetime(flashes["TimeStamp"])
            flashes = flashes.sort_values(["SignalID", "TimeStamp"]).reset_index(drop=True)
            flashes["EventChange"] = flashes.groupby(["SignalID"])[["EventParam"]].diff()
            flashes = flashes[flashes.EventChange != 0].drop(columns=["EventChange"])

            flashes["FlashDuration"] = (
                flashes.groupby(["SignalID"])[["TimeStamp"]]
                .diff()
                .shift(-1)["TimeStamp"]
                .dt.total_seconds()
            )
            flashes["EndParam"] = flashes.groupby(["SignalID"])[["EventParam"]].shift(-1, fill_value=0)
            flashes = flashes[~flashes["EventParam"].isin([2, 3, 4])]

            flash_s3object = (
                f"mark/flash_events/date={date_str}/flashes_{date_str}.parquet"
            )
            with io.BytesIO() as data:
                flashes.to_parquet(data)
                data.seek(0)
                s3.upload_fileobj(data, bucket, flash_s3object)

            flash_s3key = os.path.join("s3://", bucket, flash_s3object)
            logger.info(f"{flash_s3key} written")
        else:
            logger.warning(f"No flash events on {date_str}")
