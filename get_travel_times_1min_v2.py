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
            lambda: requests.get(uri.format('jobs/status'), params = {'key': key, 'jobId': jobid}),
            check_success = is_success,
            step=60,
            step_function=polling.step_linear_double,
            timeout=3600)

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


def get_rsi(df, df_speed_limits, corridor_grouping): #relative speed index = 90th % speed / speed limit
    df_rsi = df.compute().groupby(corridor_grouping).speed.quantile(0.90).reset_index()
    df_rsi = pd.merge(df_rsi, df_speed_limits[corridor_grouping + ['Speed Limit']])
    df_rsi['rsi'] = df_rsi['speed'] / df_rsi['Speed Limit']
    return df_rsi


def clean_up_tt_df_for_bpsi(df):
    df = df[['tmc_code', 'Corridor', 'Subcorridor', 'Minute', 'speed']]
    # df = df[df.speed >= 20]
    df['hr'] = df.Minute.dt.hour  # takes a while - put after filter
    df = df[(df.speed >= 20) & (df.hr.between(9,10) | df.hr.between(19,20))]  # only take rows w/ speed > 20 and between 9-11 AM/7-9 PM
    return df.compute()


def get_serious_injury_pct(df, df_reference, corridor_grouping):
    #get count of each grouping for calculating percentages, merge back into overall df
    df_totals = df.groupby(corridor_grouping).size().reset_index(name='totals')
    df = pd.merge(df, df_totals)

    #get count for each speed bin by grouping, calculating the % out of the total for that grouping, and again merge back into overall df
    df_count = (df[corridor_grouping + ['speed','totals']].groupby(corridor_grouping + ['speed']).agg(['count','mean'])).reset_index()
    count_pct = df_count['totals']['count'] / df_count['totals']['mean']
    df_count['count_pct'] = count_pct
    df_summary = df_count[corridor_grouping + ['speed','count_pct']].droplevel(1, axis = 1)

    #merge w/ reference_df and calculate overall serious injury %
    df_summary = pd.merge(df_summary, df_reference, left_on=['speed'], right_on=['mph_bin'])
    overall_pct = df_summary['count_pct'] * df_summary['pct']
    df_summary['overall_pct'] = overall_pct
    df_summary = df_summary.groupby(corridor_grouping, as_index=False)['overall_pct'].sum()
    return df_summary


if __name__=='__main__':

    s3root = sys.argv[1]

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    if len(sys.argv) > 2:
        start_date = sys.argv[2]
        end_date = sys.argv[3]
    else:
        start_date = conf['start_date']
        end_date = conf['end_date']

    start_date = get_date_from_string(
        start_date, s3bucket=conf['bucket'], s3prefix=f'{s3root}/travel_times_1min'
    )
    end_date = get_date_from_string(end_date)

    bucket = conf['bucket']

    os.environ['AWS_ACCESS_KEY_ID'] = cred['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = cred['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = cred['AWS_DEFAULT_REGION']

    # pull in file that matches up corridors/subcorridors/TMCs from S3
    tmc_df = (pd.read_excel(f"s3://{bucket}/{conf['corridors_TMCs_filename_s3']}")
                .rename(columns={'length': 'miles'})
                .fillna(value={'Corridor': 'None', 'Subcorridor': 'None'}))
    tmc_df = tmc_df[tmc_df.Corridor != 'None']

    tmc_list = list(set(tmc_df.tmc.values))

    dates = pd.date_range(start_date, end_date)
    number_of_days = len(dates)

    for date_ in dates:

        date_str = date_.strftime('%F')

        try:
            start_time = time.perf_counter()
            tt_df = get_tmc_data(date_str, date_str, tmc_list, cred['RITIS_KEY'], bin_minutes=1, initial_sleep_sec=0)
            end_time = time.perf_counter()
            process_time = end_time - start_time
            print(f'Time to pull {len(tmc_list)} TMCs for {date_str}: {round(process_time/60)} minutes')

        except Exception as e:
            print(f'ERROR retrieving tmc records - {str(e)}')
            tt_df = pd.DataFrame()


        if len(tt_df) > 0:
            tt_df['measurement_tstamp'] = pd.to_datetime(tt_df.measurement_tstamp)
            tt_df['date'] = tt_df.measurement_tstamp.dt.date

            df = (pd.merge(tmc_df[['tmc', 'miles', 'Corridor', 'Subcorridor']],
                           tt_df,
                           left_on=['tmc'],
                           right_on=['tmc_code'])
                    .drop(columns=['tmc'])
                    .sort_values(['Corridor', 'tmc_code', 'measurement_tstamp']))

            df['reference_minutes'] = df['miles'] / df['reference_speed'] * 60
            df = (df.reset_index(drop=True)
                    .rename(columns = {'measurement_tstamp': 'Minute'}))
            df = df.drop_duplicates()

            #write 1-min monthly travel times/speed df to parquet on S3
            table_name = 'travel_times_1min'
            filename = f'travel_times_1min_{date_str}.parquet'
            df.drop(columns=['date'])\
                .to_parquet(f's3://{bucket}/{s3root}/{table_name}/date={date_str}/{filename}')
        else:
            print('No records returned.')


    fn = conf['corridors_filename_s3']
    df_speed_limits = pd.read_excel(
        f's3://{bucket}/{fn}',
        sheet_name='Contexts')

    months = [d.strftime('%Y-%m') for d in dates]
    months = list(set(months))

    for yyyy_mm in months:

        try:
            df = dd.read_parquet(f's3://{bucket}/{s3root}/travel_times_1min/date={yyyy_mm}*/*.parquet')
        except IndexError:
            df = pd.DataFrame()
            print(f'No data for {yyyy_mm}')
        except KeyError as e:
            df = pd.DataFrame()
            print(f'KeyError for {yyyy_mm} - {str(e)}')
        except Exception as e:
            df = pd.DataFrame()
            print(f'Unhandled exception for {yyyy_mm} - {str(e)}')

        if len(df.columns) > 0:

            # relative speed index for month

            df_rsi_sub = get_rsi(df, df_speed_limits, ['Corridor','Subcorridor'])
            df_rsi_cor = get_rsi(df, df_speed_limits[df_speed_limits['Subcorridor'].isnull()], ['Corridor'])

            table_name = 'relative_speed_index'

            df_rsi_sub.to_parquet(f's3://{bucket}/{s3root}/{table_name}/rsi_sub_{yyyy_mm}-01.parquet')
            df_rsi_cor.to_parquet(f's3://{bucket}/{s3root}/{table_name}/rsi_cor_{yyyy_mm}-01.parquet')

            # bike-ped safety index for month

            df_reference = pd.read_csv(f's3://{bucket}/serious_injury_pct.csv')

            df_bpsi = clean_up_tt_df_for_bpsi(df)

            df_bpsi_sub = get_serious_injury_pct(df_bpsi, df_reference, ['Corridor','Subcorridor'])
            df_bpsi_cor = get_serious_injury_pct(df_bpsi, df_reference, ['Corridor'])

            #do we need to add a column to this that has month? right now is just grouping/serious injury %
            table_name = 'bike_ped_safety_index'

            df_bpsi_sub.to_parquet(f's3://{bucket}/{s3root}/{table_name}/bpsi_sub_{yyyy_mm}-01.parquet')
            df_bpsi_cor.to_parquet(f's3://{bucket}/{s3root}/{table_name}/bpsi_cor_{yyyy_mm}-01.parquet')
