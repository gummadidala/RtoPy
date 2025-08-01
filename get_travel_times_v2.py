# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:42:51 2018

@author: Alan.Toppen
"""


import sys
import os
import pandas as pd
import numpy as np
import requests
import uuid
import polling
import time
import yaml
from datetime import date, datetime, timedelta
import pytz
from zipfile import ZipFile
import json
import io
import boto3
import dask.dataframe as dd
from config import get_date_from_string
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

s3 = boto3.client('s3')


def is_success(response):
    x = json.loads(response.content.decode('utf-8'))
    if type(x) == dict and 'state' in x.keys() and x['state']=='SUCCEEDED':
        return True
    elif type(x) == str:
        print(x)
        time.sleep(60)
        return False
    else:
        # print(f"state: {x['state']} | progress: {x['progress']}")
        return False


def get_tmc_data(start_date, end_date, tmcs, key, dow=[2,3,4], bin_minutes=60, initial_sleep_sec=0):

    # date range is exclusive of end_date, so add a day to end_date to include it.
    end_date = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%F')

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    uri = 'https://pda-api.ritis.org/v2/{}'

    #----------------------------------------------------------
    payload = {
      "dates": [
        {
          "end": end_date,
          "start": start_date
        }
      ],
      "dow": dow,
      "dsFields": [
        {
          "columns": [
            "speed",
            "reference_speed",
            "travel_time_minutes",
            "confidence_score"
          ],
          "id": "here_tmc"
        }
      ],
      "granularity": {
        "type": "minutes",
        "value": bin_minutes
      },
      "segments": {
        "ids": tmcs,
        "type": "tmc"
      },
      "times": [ #pulling all 24 hours
        {
          "end": None,
          "start": "00:00:00.000"
        }
      ],
      "travelTimeUnits": "minutes",
      "uuid": str(uuid.uuid1())
    }
    #----------------------------------------------------------
    response = requests.post(uri.format('submit/export'),
                             params = {'key': key},
                             json = payload)
    print('travel times response status code:', response.status_code)

    if response.status_code == 200: # Only if successful response

        # retry at intervals of 'step' until results return (status code = 200)
        jobid = response.json()['id']

        polling.poll(
            lambda: requests.get(uri.format('jobs/status'), params={'key': key, 'jobId': jobid}),
            check_success=is_success,
            step=30,
            step_function=polling.step_linear_double,
            timeout=1800)

        results = requests.get(uri.format('results/export'),
                               params={'key': key, 'uuid': payload['uuid']})
        print('travel times results received')

        # Save results (binary zip file with one csv)
        with io.BytesIO() as f:
            f.write(results.content)
            f.seek(0)
            with ZipFile(f, 'r') as zf:
                df = pd.read_csv(zf.open('Readings.csv'))
                #tmci = pd.read_csv(zf.open('TMC_Identification.csv'))

    else:
        df = pd.DataFrame()

    print(f'{len(df)} travel times records')

    return df

def get_corridor_travel_times(df, corr_grouping, bucket, table_name):

    # -- Raw Hourly Travel Time Data --
    def uf(df):
        date_string = df.date.values[0]
        filename = 'travel_times_{}.parquet'.format(date_string)
        df = df.drop(columns=['date'])
        df.to_parquet(f's3://{bucket}/{s3root}/{table_name}/date={date_string}/{filename}')

    # Write to parquet files and upload to S3
    df.groupby(['date']).apply(uf)


def get_corridor_travel_time_metrics(df, corr_grouping, bucket, table_name):

    df = df.groupby(corr_grouping + ['Hour'], as_index=False)[
        ['travel_time_minutes', 'reference_minutes', 'miles']].sum()

    # -- Travel Time Metrics Summarized by tti, pti by hour --
    df['Hour'] = df['Hour'].apply(lambda x: x.replace(day=1))
    df['speed'] = df['miles']/(df['travel_time_minutes']/60)

    desc = df.groupby(corr_grouping + ['Hour']).describe(percentiles = [0.90])
    tti = desc['travel_time_minutes']['mean'] / desc['reference_minutes']['mean']
    pti = desc['travel_time_minutes']['90%'] / desc['reference_minutes']['mean']
    bi = pti - tti
    speed = desc['speed']['mean']

    summ_df = pd.DataFrame({'tti': tti, 'pti': pti, 'bi': bi, 'speed_mph': speed})

    def uf(df): # upload parquet file
        date_string = df.date.values[0]
        filename = 'travel_time_metrics_{}.parquet'.format(date_string)
        df = df.drop(columns=['date'])
        df.to_parquet(f's3://{bucket}/{s3root}/{table_name}/date={date_string}/{filename}')

    # Write to parquet files and upload to S3
    summ_df.reset_index().assign(date = lambda x: x.Hour.dt.date).groupby(['date']).apply(uf)


if __name__=='__main__':

    s3root = sys.argv[1]

    with open(sys.argv[2]) as yaml_file:
        tt_conf = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    if len(sys.argv) > 3:
        start_date = sys.argv[3]
        end_date = sys.argv[4]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(
        start_date, s3bucket=conf['bucket'], s3prefix=f'{s3root}/cor_travel_times_'
    )
    end_date = get_date_from_string(end_date)

    bucket = conf['bucket']

    os.environ['AWS_ACCESS_KEY_ID'] = cred['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = cred['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = cred['AWS_DEFAULT_REGION']

    suff = tt_conf['table_suffix']
    cor_table = f'cor_travel_times_{suff}'
    cor_metrics_table = f'cor_travel_time_metrics_{suff}'
    sub_table = f'sub_travel_times_{suff}'
    sub_metrics_table = f'sub_travel_time_metrics_{suff}'


    # pull in file that matches up corridors/subcorridors/TMCs from S3
    tmc_df = (pd.read_excel(f"s3://{bucket}/{conf['corridors_TMCs_filename_s3']}")
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = tmc_df[tmc_df.Corridor != 'None']

    tmc_list = list(set(tmc_df.tmc.values))

    #start_date = '2019-11-01'
    #end_date = '2019-12-01'

    print(f'travel times: {start_date} - {end_date}')

    try:
        tt_df = get_tmc_data(
            start_date,
            end_date,
            tmc_list,
            cred['RITIS_KEY'],
            dow=tt_conf['dow'],
            bin_minutes=tt_conf['bin_minutes'],
            initial_sleep_sec=0
        )

    except Exception as e:
        print(f'ERROR retrieving tmc records - {str(e)}')
        tt_df = pd.DataFrame()

    if len(tt_df) > 0:
        df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor', 'Subcorridor']], tt_df, left_on=['tmc'], right_on=['tmc_code'])
                .drop(columns=['tmc'])
                .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))

        df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
        df = (df.reset_index(drop=True)
                .assign(measurement_tstamp = lambda x: pd.to_datetime(x.measurement_tstamp, format='%Y-%m-%d %X'),
                        date = lambda x: x.measurement_tstamp.dt.date)
                .rename(columns = {'measurement_tstamp': 'Hour'}))
        df = df.drop_duplicates()

        get_corridor_travel_times(
            df[df.Corridor != 'None'], ['Corridor'], conf['bucket'], cor_table)

        get_corridor_travel_times(
            df[df.Subcorridor != 'None'], ['Corridor', 'Subcorridor'], conf['bucket'], sub_table)

        months = list(set([pd.Timestamp(d).strftime('%Y-%m') for d in pd.date_range(start_date, end_date, freq='D')]))

        for yyyy_mm in months:
            try:
                df = dd.read_parquet(f's3://{bucket}/{s3root}/{cor_table}/date={yyyy_mm}-*/*').compute()
                if not df.empty:
                    get_corridor_travel_time_metrics(
                        df, ['Corridor'], conf['bucket'], cor_metrics_table)

                df = dd.read_parquet(f's3://{bucket}/{s3root}/{sub_table}/date={yyyy_mm}-*/*').compute()
                if not df.empty:
                    get_corridor_travel_time_metrics(
                        df, ['Corridor', 'Subcorridor'], conf['bucket'], sub_metrics_table)
            except IndexError:
                print(f'No data for {yyyy_mm}')

    else:
        print('No records returned.')
