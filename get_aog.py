# -*- coding: utf-8 -*-
"""
Created on Sat Feb 15 15:15:31 2020

@author: Alan.Toppen
"""

import os
import yaml
import time
import sys
from datetime import datetime, timedelta
import boto3
import pandas as pd
import io
import re
from multiprocessing import get_context
import itertools
from config import get_date_from_string


def get_signalids(date_, conf):

    bucket = conf['bucket']
    prefix = 'detections/date={d}'.format(d=date_.strftime('%Y-%m-%d'))

    objs = boto3.resource('s3').Bucket(bucket).objects.filter(Prefix=prefix)
    for page in objs.pages():
        for x in page:
            try:
                signalid = re.search('(?<=_)\d+(?=_)', x.key).group()
                yield signalid
            except:
                pass


def get_det_config(date_, conf):
    '''
    date_ [Timestamp]
    '''
    date_str = date_.strftime('%Y-%m-%d')

    bucket = conf['bucket']

    # Read bad detectors
    bd_key = f's3://{bucket}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet'
    bd = pd.read_parquet(bd_key).assign(
            SignalID = lambda x: x.SignalID.astype('int64'),
            Detector = lambda x: x.Detector.astype('int64'))

    # Read detector config
    dc_key = f'config/atspm_det_config_good/date={date_str}/ATSPM_Det_Config_Good.feather'
    with io.BytesIO() as data:
        boto3.resource('s3').Bucket(bucket).download_fileobj(
                Key=dc_key, Fileobj=data)
        dc = pd.read_feather(data)[['SignalID', 'Detector', 'DetectionTypeDesc']]
    dc.loc[dc.DetectionTypeDesc.isna(), 'DetectionTypeDesc'] = '[]'

    # Anti-join: detectors in dc (detector config table) not in bd (bad detectors table)
    df = pd.merge(dc, bd, how='outer', on=['SignalID','Detector']).fillna(value={'Good_Day': 1})
    df = df.loc[df.Good_Day==1].drop(columns=['Good_Day'])

    return df


def get_aog(signalid, date_, det_config, conf, per='H'):
    '''
    date_ [Timestamp]
    '''
    try:
        date_str = date_.strftime('%Y-%m-%d')

        bucket = conf['bucket']

        all_hours = pd.date_range(date_, date_ + pd.Timedelta(1, unit='days'), freq=per, inclusive='left')

        de_fn = f'../detections/Date={date_str}/SignalID={signalid}/de_{signalid}_{date_str}.parquet'
        if os.path.exists(de_fn):
            detection_events = pd.read_parquet(de_fn).drop_duplicates()
        else:
            de_fn = f's3://{bucket}/detections/date={date_str}/de_{signalid}_{date_str}.parquet'
            detection_events = pd.read_parquet(de_fn).drop_duplicates()

        # Join detector type with detection events table. Choose only 'Advanced Count'
        df = (pd.merge(
                detection_events,
                det_config[det_config.DetectionTypeDesc.str.contains('Advanced Count')],
                on=['SignalID', 'Detector'],
                how='left'))
        df = df[~df.DetectionTypeDesc.isna()]

        if df.empty:
            #print(f'{signalid}|{date_str}: No detectors for Arrivals on Green')
            print('#', end='')
            return pd.DataFrame()
        else:
            # Get number and fraction of arrivals by Interval in every period, per
            df_aog = (df.assign(Hour=lambda x: x.DetTimeStamp.dt.floor(per))
                      .rename(columns={'Detector': 'Arrivals',
                                       'EventCode': 'Interval'})
                      .groupby(['Hour', 'SignalID', 'Phase', 'Interval'])
                      .count()[['Arrivals']])
            df_aog['All_Arrivals'] = df_aog.groupby(level=['Hour', 'SignalID', 'Phase']).transform('sum')
            df_aog['AOG'] = df_aog['Arrivals']/df_aog['All_Arrivals']

            # Filter only the arrivals on Interval=1, Green
            aog = (df_aog.reset_index('Interval')
                   .query('Interval == 1')
                   .drop(columns=['Interval'])
                   .rename(columns={'Arrivals': 'Green_Arrivals'}))

            # For Progress Ratio, get the fraction of green time in each hour
            df_gc = (df[['SignalID', 'Phase', 'PhaseStart', 'EventCode']]
                     .drop_duplicates()
                     .rename(columns={'PhaseStart': 'IntervalStart',
                                      'EventCode': 'Interval'})
                     .assign(IntervalDuration=0)
                     .set_index(['SignalID', 'Phase', 'IntervalStart']))

            x = pd.DataFrame(
                    data={'Interval': None, 'IntervalDuration': 0},
                    index=pd.MultiIndex.from_product(
                            [df_gc.index.levels[0],
                             df_gc.index.levels[1],
                             all_hours],
                    names=['SignalID', 'Phase', 'IntervalStart']))

            df_gc = (pd.concat([df_gc, x])
                     .sort_index()
                     .ffill() # fill forward missing Intervals for on the hour rows
                     .reset_index(level=['IntervalStart']))
            df_gc['IntervalEnd'] = df_gc.groupby(level=['SignalID','Phase']).shift(-1)['IntervalStart']
            df_gc['IntervalDuration'] = (df_gc.IntervalEnd - df_gc.IntervalStart).dt.total_seconds()
            df_gc['Hour'] = df_gc.IntervalStart.dt.floor(per)

            df_gc = df_gc.reset_index(drop=False).set_index(['Hour', 'SignalID', 'Phase', 'Interval'])

            interval_duration = df_gc.groupby(level=['Hour', 'SignalID', 'Phase', 'Interval'])['IntervalDuration'].sum()
            phase_duration = interval_duration.groupby(level=['Hour', 'SignalID', 'Phase']).sum()
            gC = interval_duration/phase_duration

            gC = (gC.reset_index('Interval')
                  .query('Interval == 1')
                  .drop(columns=['Interval'])
                  .rename(columns={'IntervalDuration': 'gC'}))

            aog = pd.merge(aog, gC, left_index=True, right_index=True).assign(pr=lambda x: x.AOG/x.gC)
            aog = aog[~aog.Green_Arrivals.isna()]

            print('.', end='')

            return aog

    except Exception as e:
        print(f'\n{signalid}|{date_str}: Error--{e}')
        return pd.DataFrame()


def main(conf, start_date, end_date):

    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:
      try:
        t0 = time.time()

        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)

        print('Getting detector configuration...', end='')
        det_config = get_det_config(date_, conf)
        print('done.')
        print('Getting signals...', end='')
        signalids = list(get_signalids(date_, conf))
        print('done.')

        bucket = conf['bucket']

        print('1 hour')
        # get_context()
        with get_context('spawn').Pool(processes=24) as pool:
            results = pool.starmap_async(
                get_aog,
                list(itertools.product(signalids, [date_], [det_config], [conf], ['H'])))
            pool.close()
            pool.join()

        dfs = results.get()
        df = (pd.concat(dfs)
              .reset_index()[['SignalID', 'Phase', 'Hour', 'AOG', 'pr', 'All_Arrivals']]
              .rename(columns={'Phase': 'CallPhase',
                               'Hour': 'Date_Hour',
                               'AOG': 'aog',
                               'All_Arrivals': 'vol'})
              .sort_values(['SignalID', 'Date_Hour', 'CallPhase'])
              .fillna(value={'vol': 0})
              .assign(SignalID=lambda x: x.SignalID.astype('str'),
                      CallPhase=lambda x: x.CallPhase.astype('str'),
                      vol=lambda x: x.vol.astype('int32')))

        df.to_parquet(f's3://{bucket}/mark/arrivals_on_green/date={date_str}/aog_{date_str}.parquet')

        num_signals = len(list(set(df.SignalID.values)))
        t1 = round(time.time() - t0, 1)
        print(f'\n{num_signals} signals done in {t1} seconds.')


        print('\n15 minutes')

        with get_context('spawn').Pool(processes=24) as pool:
            results = pool.starmap_async(
                get_aog,
                list(itertools.product(signalids, [date_], [det_config], [conf], ['15min'])))
            pool.close()
            pool.join()

        dfs = results.get()
        df = (pd.concat(dfs)
              .reset_index()[['SignalID', 'Phase', 'Hour', 'AOG', 'pr', 'All_Arrivals']]
              .rename(columns={'Phase': 'CallPhase',
                               'Hour': 'Date_Period',
                               'AOG': 'aog',
                               'All_Arrivals': 'vol'})
              .sort_values(['SignalID', 'Date_Period', 'CallPhase'])
              .fillna(value={'vol': 0})
              .assign(SignalID=lambda x: x.SignalID.astype('str'),
                      CallPhase=lambda x: x.CallPhase.astype('str'),
                      vol=lambda x: x.vol.astype('int32')))

        df.to_parquet(f's3://{bucket}/mark/arrivals_on_green_15min/date={date_str}/aog_{date_str}.parquet')

        num_signals = len(list(set(df.SignalID.values)))
        t1 = round(time.time() - t0, 1)
        print(f'\n{num_signals} signals done in {t1} seconds.')


      except Exception as e:
        print(f'\n{date_}: Error: {e}')



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


    main(conf, start_date, end_date)

