# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
import pytz
from multiprocessing import get_context
import pandas as pd
import sqlalchemy as sq
import time
import os
import itertools
import boto3
import yaml
import io
import re
import psutil

from spm_events import etl_main
from parquet_lib import read_parquet_file
from config import get_date_from_string

from mark1_logger import mark1_logger

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

s3 = boto3.client('s3')
ath = boto3.client('athena')


def now(tz):
    return datetime.now().astimezone(pytz.timezone(tz))


base_path = "."

logs_path = os.path.join(base_path, "logs")
if not os.path.exists(logs_path):
    os.mkdir(logs_path)
# logger = mark1_logger(
#     os.path.join(logs_path, f'etl_{now("US/Eastern").strftime("%F")}.log')
# )
logfilename = os.path.join(logs_path, f'etl_{now("US/Eastern").strftime("%F")}.log')

'''
    df:
        SignalID [int64]
        TimeStamp [datetime]
        EventCode [str or int64]
        EventParam [str or int64]

    det_config:
        SignalID [int64]
        IP [str]
        PrimaryName [str]
        SecondaryName [str]
        Detector [int64]
        Call Phase [int64]
'''

def etl2(s, date_, det_config, conf):

    date_str = date_.strftime('%Y-%m-%d')

    det_config_good = det_config[det_config.SignalID==s]

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)

    t0 = time.time()

    try:
        bucket = conf['bucket']
        key = f'atspm/date={date_str}/atspm_{s}_{date_str}.parquet'
        df = read_parquet_file(bucket, key)


        if len(df)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No event data for this signal\n')


        if len(det_config_good)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No detector configuration data for this signal\n')

        if len(df) > 0 and len(det_config_good) > 0:

            c, d = etl_main(df, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet(f's3://{bucket}/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

                # Because DetTimeStamp gets adjusted by time from stop bar, it can go into tomorrow.
                # Limit detector data to today to make downstream easier to interpret.
                # May lose 1-2 vehs at midnight.
                d = d[d.DetTimeStamp < date_ + timedelta(days=1)]
                d.to_parquet(f's3://{bucket}/detections/date={date_str}/de_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

            else:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No cycles\n')


    except Exception as e:
        with open(logfilename, 'a') as f:
            f.write(f'{s}: {e}\n')







def main(start_date, end_date):


    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    #start_date = '2019-06-04'
    #end_date = '2019-06-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    bucket = conf['bucket']
    athena = conf['athena']

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)

        logfilename = f'etl_{date_str}.log'
        if os.path.exists(logfilename): os.remove(logfilename)

        with io.BytesIO() as data:
            s3.download_fileobj(
                Bucket=bucket,
                Key=f'config/atspm_det_config_good/date={date_str}/ATSPM_Det_Config_Good.feather',
                Fileobj=data)

            det_config_raw = pd.read_feather(data)\
                .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                .assign(Detector = lambda x: x.Detector.astype('int64'))\
                .rename(columns={'CallPhase': 'Call Phase'})

        signalids = det_config_raw['SignalID'].drop_duplicates().astype('int64').values

        try:
            bad_detectors = pd.read_parquet(
                f's3://{bucket}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet')\
                        .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                        .assign(Detector = lambda x: x.Detector.astype('int64'))

            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])


            det_config = (left.join(right, how='left')
                .fillna(value={'Good_Day': 1})
                .query('Good_Day == 1')
                .reset_index(level='Detector')
                .set_index('Call Phase', append=True)
                .assign(
                    minCountPriority = lambda x: x.CountPriority.groupby(level=['SignalID', 'Call Phase']).min()))
            det_config['CountDetector'] = det_config['CountPriority'] == det_config['minCountPriority']
            det_config = det_config.drop(columns=['minCountPriority']).reset_index()

        except FileNotFoundError:
            det_config = pd.DataFrame()

        if len(det_config) > 0:
            nthreads = round(psutil.virtual_memory().available/1e9)  # ensure 1 MB memory per thread

            #-----------------------------------------------------------------------------------------
            with get_context('spawn').Pool(processes=nthreads) as pool:
                result = pool.starmap_async(
                    etl2, list(itertools.product(signalids, [date_], [det_config], [conf])), chunksize=(nthreads-1)*4)
                pool.close()
                pool.join()
            #-----------------------------------------------------------------------------------------
        else:
            print('No good detectors. Skip this day.')

    print(f'{len(signalids)} signals in {len(dates)} days. Done in {int((time.time()-t0)/60)} minutes')


    # Add a partition for each day. If more than ten days, update all partitions in one command.
    if len(dates) > 10:
        response_repair_cycledata = ath.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE cycledata;",
            QueryExecutionContext={'Database': athena['database']},
            ResultConfiguration={'OutputLocation': athena['staging_dir']})

        response_repair_detection_events = ath.start_query_execution(
            QueryString=f"MSCK REPAIR TABLE detectionevents",
            QueryExecutionContext={'Database': athena['database']},
            ResultConfiguration={'OutputLocation': athena['staging_dir']})
    else:
        for date_ in dates:
            date_str = date_.strftime('%Y-%m-%d')
            response_repair_cycledata = ath.start_query_execution(
                QueryString=f"ALTER TABLE cycledata ADD PARTITION (date = '{date_str}');",
                QueryExecutionContext={'Database': athena['database']},
                ResultConfiguration={'OutputLocation': athena['staging_dir']})

            response_repair_detection_events = ath.start_query_execution(
                QueryString=f"ALTER TABLE detectionevents ADD PARTITION (date = '{date_str}');",
                QueryExecutionContext={'Database': athena['database']},
                ResultConfiguration={'OutputLocation': athena['staging_dir']})


    # Check if the partitions for the last day were successfully added before moving on
    while True:
        response1 = s3.list_objects(
            Bucket=os.path.basename(athena['staging_dir']),
            Prefix=response_repair_cycledata['QueryExecutionId'])
        response2 = s3.list_objects(
            Bucket=os.path.basename(athena['staging_dir']),
            Prefix=response_repair_detection_events['QueryExecutionId'])

        if 'Contents' in response1 and 'Contents' in response2:
            print('done.')
            break
        else:
            time.sleep(2)
            print('.', end='')


if __name__=='__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)



    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(
        start_date, s3bucket=conf['bucket'], s3prefix="mark/arrivals_on_green"
    )
    end_date = get_date_from_string(end_date)


    main(start_date, end_date)

