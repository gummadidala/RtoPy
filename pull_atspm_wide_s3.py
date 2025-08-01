# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 16:32:22 2019

@author: Alan.Toppen
"""

import pandas as pd
import numpy as np
import sqlalchemy as sq
import io
import boto3
from multiprocessing import get_context, Pool
import itertools
import re
import os
import sys
from datetime import datetime, timedelta
import time
import random
import urllib.parse

s3 = boto3.client('s3')
events_bucket = 'gdot-spm'
config_bucket = events_bucket


def read_atspm_query(query):
    dsn = 'atspm'
    uid = os.environ['ATSPM_USERNAME']
    pwd = urllib.parse.quote_plus(os.environ['ATSPM_PASSWORD'])
    engine = sq.create_engine(f'mssql+pyodbc://{uid}:{pwd}@{dsn}', pool_size=20)

    with engine.connect() as con:
        df = pd.read_sql_query(query, con=con)
    return df


def get_eventlog_data_db(signalid, date_str):

    start_date = date_str  # date_.strftime('%Y-%m-%d %H:%M:%S.%f')[:-5]
    end_date = (pd.Timestamp(date_str) + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1))\
                .strftime('%Y-%m-%d %H:%M:%S.%f')[:-5]

    df = read_atspm_query("""
            SELECT * FROM Controller_Event_Log
            WHERE SignalID = '{}'
			AND Timestamp BETWEEN '{}' AND '{}'
            ORDER BY SignalID, Timestamp, EventCode, EventParam
            """.format(signalid.zfill(5),
                        start_date,
                        end_date))
    return df


def get_eventlog_data(bucket, signalid, dates):
    for date_ in dates:
        date_str = date_.strftime('%F')
        df = pd.read_parquet('s3://{b}/atspm/date={d}/atspm_{s}_{d}.parquet'.format(
                b=bucket, d=date_str, s=signalid))
        df.Timestamp = df.Timestamp.dt.tz_localize(None)
        df.SignalID = df.SignalID.astype('str')
        df['date'] = date_
        yield df




def get_det_config(bucket, date_, leading_zeros=False):
    date_str = date_.strftime('%F')
    objs = s3.list_objects(Bucket=bucket, Prefix=f'config/atspm_det_config_good/date={date_str}/')
    keys = [obj['Key'] for obj in objs['Contents']]

    def f(bucket, key):
        with io.BytesIO() as data:
            s3.download_fileobj(
                    Bucket=bucket,
                    Key=key,
                    Fileobj=data)

            dc = pd.read_feather(data)\
                .assign(SignalID = lambda x: x.SignalID.astype('str'))\
                .assign(Detector = lambda x: x.Detector.astype('int64'))\
                .reset_index(drop=True)

            dc['date'] = date_

            if leading_zeros:
                dc['SignalID'] = dc['SignalID'].str.zfill(5)

        return dc

    return pd.concat(map(lambda k: f(bucket, k), keys))
            #.rename(columns={'CallPhase': 'Call Phase'})\


def get_det_configs(bucket, dates, leading_zeros=False):
    return pd.concat([get_det_config(bucket, date_, leading_zeros) for date_ in dates])


def get_det_config_local(filename):

    dc = pd.read_feather(filename)\
        .assign(SignalID = lambda x: x.SignalID.astype('str'))\
        .assign(Detector = lambda x: x.Detector.astype('int64'))\
        .reset_index(drop=True)
        #.rename(columns={'CallPhase': 'Call Phase'})\
    return dc


def get_det_config_future(bucket, date_str):
    key = 's3://{b}/atspm_det_config_good/date={d}/ATSPM_Det_Config_Good_Ozark.parquet'.format(
            b=bucket, d=date_str)
    print(key)
    dc = pd.read_parquet(key).reset_index(drop=True)
    return dc


# Works. Doesn't copy down. Use this for new grouping variable and copy_down to apply value across and down
def create_new_grouping_field(df, eventcodes, grouping_field, new_grouping_field, transform_func = lambda x: x):

    if type(eventcodes) is list:
        mask = df.EventCode.isin(eventcodes)
    else:
        eventcode = eventcodes
        mask = df.EventCode==eventcode

    df.loc[mask, new_grouping_field] = df.loc[mask, grouping_field].apply(transform_func)

    return df



# Works. Two-step create new field and copy down. May need just a copy down.
def copy_updown(
        df, eventcodes, new_field_name, group_fields, copy_field,
        off_eventcode=None, direction='down', apply_to_timestamp='all'):
    '''
    df - eventlog dataframe
    eventcodes - EventCode(s) signifying event(s) to carry forward to subsequent events, e.g., 0 for PhaseStart
    new_field_name - name of Event corresponding to EventCode, e.g., PhaseStart
    group_fields - grouping(s) to which eventcode applies, e.g., [SignalID, EventParam] (Phase) for PhaseStart
    copy_field - field identifying eventcode, e.g., Timestamp for PhaseStart
    off_eventcode - optional value for where to stop copying up or down, otherwise goes to next value in eventcodes
    direction - 'up' for copy up, 'down' for copy down new_field_name
    apply_to_timestamp - 'all' to fill all rows with timestamps of the eventcodes before copying up or down,
                           Example would be 31-Barrier which should renew with all events at that same timestamp,
                           of which there are many starts and ends to phase intervals
                         'group' to fill all rows at the timestamp in the group
                           Example would be Recorded Split
                         None to not fill all rows at the timestamp.
                           Example detector off (81) or call off (44) events
    '''
    if type(eventcodes) is list:
        if sum(df.EventCode.isin(eventcodes)) == 0:
            #print('Event Codes {} not in data frame'.format(','.join(map(str, eventcodes))))
            return df
        else:
            df.loc[df.EventCode.isin(eventcodes), new_field_name] = df.loc[df.EventCode.isin(eventcodes), copy_field]
    else:
        eventcode = eventcodes
        if sum(df.EventCode==eventcode) == 0:
            #print('Event Code {} not in data frame'.format(eventcode))
            return df
        else:
            df.loc[df.EventCode==eventcode, new_field_name] = df.loc[df.EventCode==eventcode, copy_field]

    if apply_to_timestamp=='all':
        df[new_field_name] = df.groupby(['SignalID','Timestamp'], group_keys=False)[new_field_name].transform('max') ## This seems to work
    elif apply_to_timestamp=='group':
        group_vars = list(set(['SignalID','Timestamp'] + group_fields))
        df[new_field_name] = df.groupby(group_vars, group_keys=False)[new_field_name].transform('max') ## This seems to work

    if off_eventcode is not None:
        df.loc[df.EventCode==off_eventcode, new_field_name] = -1

    if direction == 'down':
        df[new_field_name] = df.groupby(group_fields)[new_field_name].ffill()
    elif direction == 'up':
        df[new_field_name] = df.groupby(group_fields)[new_field_name].bfill()

    if off_eventcode is not None:
        df.loc[df[new_field_name]==-1, new_field_name] = None

    return df








def copy_down(
        df, eventcodes, new_field_name, group_fields, copy_field,
        off_eventcode=None,
        apply_to_timestamp='all'):
    return copy_updown(
            df, eventcodes, new_field_name, group_fields, copy_field,
            off_eventcode=off_eventcode,
            apply_to_timestamp=apply_to_timestamp,
            direction='down')


def copy_up(
        df, eventcodes, new_field_name, group_fields, copy_field,
        off_eventcode=None,
        apply_to_timestamp='all'):
    return copy_updown(
            df, eventcodes, new_field_name, group_fields, copy_field,
            off_eventcode=off_eventcode,
            apply_to_timestamp=apply_to_timestamp,
            direction='up')


def widen(s, date_, det_config=None, source='s3'): # or source = 'db'

    signalid = s
    date0_ = date_ - pd.Timedelta(1, unit='D')
    # date0_str = date0_.strftime('%F')
    date_str = date_.strftime('%F')

    print('{} | {} started.'.format(date_str, s))

    if det_config is None:
        det_config = get_det_configs(config_bucket, [date0_, date_])

    dc = det_config[['SignalID','Detector','CallPhase','TimeFromStopBar','date']]
    dc = dc[dc.SignalID==s]

    if source=='s3':
        df = pd.concat(get_eventlog_data(events_bucket, signalid, [date0_, date_]))
    elif source=='db':
        df = get_eventlog_data_db(signalid, date_str)

    df = df[~df.EventCode.isin([43, 44])]

    df = df.rename(columns={'TimeStamp':'Timestamp'})
    df = df.sort_values(['SignalID','Timestamp','EventCode','EventParam']).reset_index(drop=True)

    print('{} | {} data queried from database.'.format(date_str, s))

    # Map Detectors to Phases.
    # Replace EventParam with Phase to align with other event types
    # Add new field for Detector from original EventParam field
    detector_codes = list(range(81,89))
    dc2 = pd.concat([dc.assign(EventCode=d).rename(columns={'Detector':'EventParam'}) for d in detector_codes])

    df = pd.merge(
            left=df,
            right=dc2,
            on=['SignalID','EventCode','EventParam','date'],
            how='left')\
        .reset_index(drop=True)

    # Adjust Timestamp for detectors by adding TimeFromStopBar
    df.Timestamp = df.Timestamp + pd.to_timedelta(df.TimeFromStopBar.fillna(0), 's')
    df = df.sort_values(['SignalID','Timestamp','EventCode','EventParam'])

    codes = df.EventCode.drop_duplicates().values.tolist()

    # Rename Detector, Phase columns
    df.loc[df.EventCode.isin(detector_codes), 'Detector'] = df.loc[df.EventCode.isin(detector_codes), 'EventParam']
    df.loc[df.EventCode.isin(detector_codes), 'Phase'] = df.loc[df.EventCode.isin(detector_codes), 'CallPhase']
    df = df.drop(columns=['CallPhase','TimeFromStopBar'])

    df = create_new_grouping_field(df, list(range(83,89)), ['SignalID', 'Detector'], 'DetectorFault')
    df = copy_down(df, list(range(84,89)), 'DetectorFault', ['SignalID','Detector'], 'EventParam', off_eventcode=83)

    ped_input_codes = [89, 90]
    df.loc[df.EventCode.isin(ped_input_codes), 'PedInput'] = df.loc[df.EventCode.isin(ped_input_codes), 'EventParam']

    # Global (Signal-wide) copy-downs. Uses two-step function to create new field and copy down
    if 31 in codes:
        df = copy_down(df, 31, 'Ring', ['SignalID'], 'EventParam')
    else:
        df['Ring'] = None

    # Temporary workaround. copy_updown doesn't take EventCode AND EventParam--just EventCode.
    # Create temporary EventCode based on desired combination of EventCode and EventParam
    # then copy down and revert when done.
    if 31 in codes:
        df.loc[(df.EventCode==31) & (df.EventParam==1), 'EventCode'] = 100031
        df = copy_down(df, 100031, 'CycleStart', ['SignalID'], 'Timestamp')
        df.loc[df.EventCode==100031, 'EventCode'] = 31
    else:
        df['CycleStart'] = None

    if 131 in codes:
        df = copy_down(df, 131, 'CoordPattern', ['SignalID'], 'EventParam')
    else:
        df['CoordPattern'] = None

    if 132 in codes:
        df = copy_down(df, 132, 'CycleLength', ['SignalID'], 'EventParam')
    else:
        df['CycleLength'] = None

    if 316 in codes:
        df = copy_down(df, 316, 'ActualCycleLength', ['SignalID'], 'EventParam') # New 7/20/21
    else:
        df['ActualCycleLength'] = None

    if 317 in codes:
        df = copy_down(df, 317, 'ActualNaturalCycleLength', ['SignalID'], 'EventParam') # New 7/20/21
    else:
        df['ActualNaturalCycleLength'] = None

    if 133 in codes:
        df = copy_down(df, 133, 'CycleOffset', ['SignalID'], 'EventParam')
    else:
        df['CycleOffset'] = None

    if 318 in codes:
        df = copy_down(df, 318, 'ActualCycleOffset', ['SignalID'], 'EventParam') # New 7/20/21
    else:
        df['ActualCycleOffset'] = None

    if 150 in codes:
        df = copy_down(df, 150, 'CoordState', ['SignalID'], 'EventParam')
    else:
        df['CoordState'] = None

    if 173 in codes:
        df = copy_down(df, 173, 'FlashStatus', ['SignalID'], 'EventParam')
    else:
        df['FlashStatus'] = None

    split_eventcodes = list(range(134,150))
    df = create_new_grouping_field(df, split_eventcodes, 'EventCode', 'Phase', lambda x: x-133)

    actual_split_eventcodes = list(range(300, 316))
    df = create_new_grouping_field(df, actual_split_eventcodes, 'EventCode', 'Phase', lambda x: x-299)

    ped_wait_eventcodes = list(range(612, 652))
    df = create_new_grouping_field(df, ped_wait_eventcodes, 'EventCode', 'Phase', lambda x: x-611)

    phase_eventcodes = list(range(0,25)) + list(range(41,50)) + [151]
    df = create_new_grouping_field(df, phase_eventcodes, 'EventParam', 'Phase')
    df = copy_down(df, 0, 'PhaseStart', ['SignalID','Phase'], 'Timestamp')
    df = copy_down(df, [1,8,10], 'Interval', ['SignalID','Phase'], 'EventCode', apply_to_timestamp='group')
    df.loc[df.EventCode==4, 'TermType'] = 4
    df.loc[df.EventCode==5, 'TermType'] = 5
    df.loc[df.EventCode==6, 'TermType'] = 6

    # TODO: See if we can get mapping between Vehicle Detector ID and Phase using (82, 81) and (43, 44).
    #       Seems we can.
    # TODO: See if we can get mapping between Pedestrian Detector ID and Phase using (90), (45)

    df = copy_down(df, split_eventcodes, 'ProgrammedSplit', ['SignalID','Phase'], 'EventParam', apply_to_timestamp='group')
    df = copy_up(df, actual_split_eventcodes, 'RecordedSplit', ['SignalID','Phase'], 'EventParam', apply_to_timestamp='group')
    df = copy_down(df, ped_wait_eventcodes, 'PedWait', ['SignalID','Phase'], 'EventParam', apply_to_timestamp='all')


    df = copy_down(df, [183, 184], 'PowerFailure', ['SignalID'], 'EventParam', off_eventcode=182)

    # Pair up detector on/offs under eventcode 82
    df = copy_up(df, 81, 'DetectorOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    df.loc[df.EventCode != 82, 'DetectorOff'] = np.nan
    df['DetectorOff'] = pd.to_datetime(df['DetectorOff'])
    df['DetectorDuration'] = (df['DetectorOff'] - df['Timestamp'])/pd.Timedelta(1, 's')

    # Pair up ped input on/offs under eventcode 90
    df = copy_up(df, 89, 'PedInputOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    df.loc[df.EventCode != 90, 'PedInputOff'] = np.nan
    df['PedInputOff'] = pd.to_datetime(df['PedInputOff'])
    df['PedInputDuration'] = (df['PedInputOff'] - df['Timestamp'])/pd.Timedelta(1, 's')

    df = df[~df.EventCode.isin([43, 44, 81, 89])]

    # Pair up phase call on/offs under eventcode 43
    #df = copy_up(df, 44, 'PhaseCallOff', ['SignalID','Detector'], 'Timestamp', apply_to_timestamp=None)
    #df.loc[df.EventCode != 43, 'PhaseCallOff'] = np.nan
    #df['PhaseCallDuration'] = (df['PhaseCallOff'] - df['Timestamp'])/pd.Timedelta(1, 's')
    #df = df[~df.EventCode.isin([43, 44,81,89])]

    # TODO: Need a way to account for multiple detectors, inputs, etc. that overlap.
    #       Add a new column for each? e.g., Detector1, Detector2, etc.?

    # Possible update to copy_down for phase interval status. but needs to be grouped by phase
    #df.loc[df.EventCode.isin(eventcodes), 'Interval'] = df.loc[df.EventCode.isin(eventcodes), 'EventCode']

    df = df[df['Timestamp'].dt.date==date_.date()].reset_index(drop=True)
    df = df[['Timestamp','SignalID','EventCode','EventParam','date',
             'Ring','CycleStart','CoordPattern','CoordState',
             'CycleLength','ActualCycleLength','ActualNaturalCycleLength','CycleOffset','ActualCycleOffset',
             'Phase','PhaseStart','Interval','TermType','ProgrammedSplit','RecordedSplit',
             'Detector','DetectorFault','DetectorOff','DetectorDuration',
             'PedInput','PedWait','PedInputOff','PedInputDuration']]

    print('{} | {} done.'.format(date_str, s))

    df.to_parquet(f's3://{events_bucket}/atspm_wide/date={date_str}/atspm_wide_{s}_{date_str}.parquet')

    return df



def get_signalids(bucket, prefix):

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix)

    for contents in [page['Contents'] for page in page_iterator]:
        keys = [content['Key'] for content in contents]
        for key in keys:
            try:
                signalid = re.search('atspm_(.+?)_', key).group(1)
            except AttributeError:
                signalid = ''

            yield signalid



def get_channel_phase_mapping(df, channel_num=82, phase_num=43):

    df = df.rename(columns={'TimeStamp': 'Timestamp'})
    df = df.sort_values(['SignalID', 'Timestamp', 'EventCode', 'EventParam']).reset_index(drop=True)

    is_phase = df.EventCode==phase_num
    is_channel = df.EventCode==channel_num
    df.loc[is_phase, 'Timestamp'] = df.loc[is_phase, 'Timestamp'] - timedelta(seconds=0.1)

    dfc = df[is_channel].set_index(['SignalID', 'Timestamp'])['EventParam']
    dfp = df[is_phase].set_index(['SignalID', 'Timestamp'])['EventParam']

    dfcp = (pd.merge(left=dfc, right=dfp, on=['SignalID', 'Timestamp'], how='outer', suffixes=['_c', '_p'])
            .dropna('EventParam_p')
            .apply(tuple, axis=1))

    dfcp = (dfcp[dfcp.groupby(level=['SignalID', 'Timestamp']).transform('count') == 1]
         .reset_index(level='Timestamp', drop=True)
         .drop_duplicates()
         .sort_values())

    dfcp = (pd.DataFrame.from_records(
                dfcp,
                columns=['Detector', 'CallPhase'],
                index=dfcp.index)
           .astype(int)
           .reset_index(drop=False))

    return dfcp



def arrivals_on_green(dw, interval='H'): # or '15min'
    # dw is widened data frame
    # dw = widen('217', date_, det_config, 's3')
    intervals = {1.0: 'Green', 8.0: 'Yellow', 10.0: 'Red'}
    dw = dw.replace({'Interval': intervals})

    dw['Timeperiod'] = dw.Timestamp.dt.floor(interval)

    aog_veh = (
        dw[~dw.DetectorOff.isna()]
        .groupby(['SignalID', 'Timeperiod', 'Phase', 'Interval'])
        .count()['Detector']
        .sort_index())
    aog_pct = (
        aog_veh
        .groupby(level=['SignalID', 'Timeperiod', 'Phase'])
        .transform(lambda x: x/sum(x)))
    aog = pd.concat([aog_veh, aog_pct], axis=1)
    aog.columns = ['Vehicles', 'Percent']

    return aog



def counts(dw, interval='H'):
    dw['Timeperiod'] = dw.Timestamp.dt.floor(interval)

    vol = (
        dw[~dw.DetectorOff.isna()]
        .groupby(['SignalID', 'Timeperiod', 'Detector', 'Phase'], dropna=False)
        .count()['Timestamp']
        .reset_index()
        .rename(columns={'Timestamp': 'Vehicles'}))

    # Convert types to match older R code
    vol['Detector'] = vol.Detector.astype('Int64').astype('str')
    vol['Phase'] = vol.Phase.astype('Int64').astype('str')
    vol['Vehicles'] = vol.Vehicles.astype('int32')
    vol = vol.replace({'Phase': {'<NA>': None}})

    return vol

def uptime(dw, interval='D'):
    dw['Timeperiod'] = dw.Timestamp.dt.floor(interval)

    successes = (
        dw[(dw.EventCode==502) & (dw.EventParam < 100)]
        .filter(['SignalID', 'Timeperiod', 'Timestamp'])
        .groupby(['SignalID', 'Timeperiod'])
        .count())

    attempts = (
        dw[(dw.EventCode==502) & (dw.EventParam >= 0)]
        .filter(['SignalID', 'Timeperiod', 'Timestamp'])
        .groupby(['SignalID', 'Timeperiod'])
        .count())

    uptime = (successes/attempts).rename(columns={'Timestamp': 'uptime'})

    response_ms = (
        dw[dw.EventCode==503]
        .filter(['SignalID', 'Timeperiod', 'EventParam'])
        .groupby(['SignalID', 'Timeperiod'])
        .mean()
        .rename(columns={'EventParam': 'response_ms'}))

    uptime = pd.concat([uptime, response_ms], axis=1).reset_index()
    uptime['CallPhase'] = 0
    uptime['Date_Hour'] = date_
    uptime['DOW'] = uptime.Timeperiod.dt.day_name()

    return uptime


# This is correct relative to the R code in Utilities.R
def week(d):
    d0 = pd.Timestamp('2016-12-25')
    week = int((d - d0)/pd.Timedelta(1, 'W'))
    return week



if __name__=='__main__':

    if len(sys.argv) > 1:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
    else:
        #start_date = '2021-07-14'
        #end_date = '2021-07-14'
        sys.exit('Need start_date and end_date as command line parameters')

    if start_date == 'yesterday':
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if end_date == 'yesterday':
        end_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')


    dates = pd.date_range(start_date, end_date, freq='1D')

    for date_ in dates:
        t0 = time.time()

        date0_ = date_ - pd.Timedelta(1, unit='D')
        date_str = date_.strftime('%Y-%m-%d')
        print(date_str)

        signalids = get_signalids(events_bucket, prefix=f'atspm/date={date_str}')
        det_config = get_det_configs(config_bucket, [date0_, date_])

        # with get_context('spawn').Pool(processes=os.cpu_count()-1) as pool:
        with Pool(os.cpu_count()-1) as pool:
            pool.starmap_async(widen, itertools.product(signalids, [date_], [det_config], ['s3']))
            pool.close()
            pool.join()

        print(f'{len(signalids)} signals in {round(time.time()-t0, 1)} seconds.')

# df[~df.DetectorDuration.isna()].groupby(['SignalID','CycleStart','Phase','Interval']).count()['Timestamp'].unstack('Interval', fill_value=0)
# df[~df.TermType.isna()][['SignalID','CycleStart','Phase','TermType']]
