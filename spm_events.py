# -*- coding: utf-8 -*-
"""
spm_events.py

Created on Fri Jun  2 09:25:34 2017

@author: Alan.Toppen
"""
import pandas as pd
import numpy as np

pd.options.display.width = 120

def get_pairs(df, a, b):
    # Get the start time, end time and duration of all chronological pairs of EventCodes
    # Example: a=1, b=8 gives the start, end and duration of green for every SignalID, EventParam

    g = df.query('EventCode in [%s,%s]' % (str(a), str(b)))

    rdf = (g.query('EventCode=={}'.format(str(a)))
            .rename(columns={'TimeStamp':'StartTimeStamp'})
            .sort_values('StartTimeStamp'))
    ldf = (g.query('EventCode=={}'.format(str(b)))
            .rename(columns={'TimeStamp':'EndTimeStamp','EventCode':'delete'})
            .sort_values('EndTimeStamp'))

    j = (pd.merge_asof(left=ldf,
                            right=rdf,
                            left_on=['EndTimeStamp'],
                            right_on=['StartTimeStamp'],
                            by=['SignalID','EventParam'])
            .drop('delete', axis=1)
            .reset_index()
            .sort_values(['SignalID','EventParam','StartTimeStamp'])
            .set_index(['SignalID','EventParam']))

    j['Duration'] = (j.EndTimeStamp - j.StartTimeStamp) / np.timedelta64(1, 's')
    j = j[['StartTimeStamp', 'EndTimeStamp', 'EventCode', 'Duration']]
    j = j.astype({'StartTimeStamp': 'datetime64[us]', 'EndTimeStamp': 'datetime64[us]'})

    # Remove secondary matches when there are multiple matches on the first item
    j = j.set_index('StartTimeStamp', append=True).sort_index()
    j['rank'] = j.groupby(level=[0,1,2]).rank()['Duration']
    j = j.loc[j['rank']==1].drop(columns='rank').reset_index(level=-1, drop=False)

    # returns SignalID|EventParam || StartTimeStamp|EndTimeStamp|EventCode|Duration
    return j

def get_green_time(df):
    # start of green to start of yellow
    return get_pairs(df, 1, 8)

def get_yellow_time(df):
    # start of yellow to start of red
    return get_pairs(df, 8, 9)

def get_red_time(df):
    # start of red to start of next green
    return get_pairs(df, 9, 1)

def get_phase_cycle(df):
    # start of green to start of green
    return get_pairs(df, 1, 1)

def assign_cycle(df, cycles):

    ldf = df.reset_index().sort_values(['StartTimeStamp']).dropna()
    rdf = cycles.reset_index().sort_values(['TimeStamp']).dropna()

    df = (pd.merge_asof(left=ldf,
                        right=rdf,
                        left_on=['StartTimeStamp'],
                        right_on=['TimeStamp'],
                        left_by=['SignalID'],
                        right_by=['SignalID'])
           .dropna()
           .rename(columns={'EventParam':'Phase',
                            'TimeStamp':'CycleStart'})
           .set_index(['SignalID','Phase'])
           .sort_index())

    df['TimeInCycle'] = (df.StartTimeStamp - df.CycleStart) / np.timedelta64(1, 's')

    # return SignalID|EventParam||...<see below>
    return df[['CycleStart','StartTimeStamp','EndTimeStamp','TimeInCycle',
               'Duration','EventCode']]

def get_detector_pairs(df, det_config):
    # df is what comes from Event Table
    # det_config is what comes from MaxTime Detector Plans
    dc = (det_config.rename(columns = {'Detector': 'EventParam'})
                    .fillna(value = {'TimeFromStopBar': 0})
                    .set_index(['SignalID','EventParam'])
                    .filter(['Call Phase', 'TimeFromStopBar', 'CountDetector']))

    df = (get_pairs(df, 82, 81)
            .join(dc).dropna() # map detector ID to phase
            .astype({'Call Phase': 'int64', 'EventCode': 'int64'}))

    df.StartTimeStamp = df.StartTimeStamp + pd.to_timedelta(df.TimeFromStopBar, 's')

    df = (df.reset_index(level=1, drop=False)
            .rename(columns={'EventParam':'Detector'})
            .rename(columns={'Call Phase':'Phase'})
            .set_index(['Phase'], append=True)
            .drop(['EndTimeStamp'], axis=1)
            .astype({'StartTimeStamp': 'datetime64[us]', 'Detector': 'int64'}))

    # returns SignalID|Phase||StartTimeStamp|82|Duration
    return df

def get_volume_by_phase(gyr, detections, aggregate=True):

    ldf = (detections.reset_index()
                     .dropna()
                     .sort_values(['StartTimeStamp'])
                     .rename(columns={'StartTimeStamp':'DetTimeStamp'})
                     .astype({'SignalID': 'int64', 'Phase': 'int64'}))

    rdf = (gyr.reset_index()
              .dropna()
              .sort_values(['StartTimeStamp'])
              .rename(columns={'StartTimeStamp':'PhaseStart',
                               'Duration':'gyrDuration'})
              .filter(['SignalID','Phase','EventCode','CycleStart','PhaseStart','TimeInCycle','gyrDuration']))

    df = (pd.merge_asof(left=ldf,
                        right=rdf,
                        left_on=['DetTimeStamp'],
                        right_on=['PhaseStart'],
                        left_by=['SignalID','Phase'],
                        right_by=['SignalID','Phase'])
           .dropna()
           .set_index(['SignalID','Phase'])
           .sort_values(['PhaseStart','DetTimeStamp']).sort_index())

    df['DetTimeInCycle'] = (df.DetTimeStamp - df.CycleStart) / np.timedelta64(1, 's')
    df['DetTimeInPhase'] = (df.DetTimeStamp - df.PhaseStart) / np.timedelta64(1, 's')

    df = (df.drop('EventCode_x', axis=1)
            .rename(columns={'EventCode_y':'EventCode',
                             'Duration':'DetDuration'})
            .astype({'EventCode': 'int64'}))

    # Disaggregate. All detection events at point in cycle/phase
    detections_by_cycle = df[['Detector', 'CycleStart','PhaseStart','EventCode','DetTimeStamp',
                               'DetDuration','DetTimeInCycle','DetTimeInPhase']].reset_index()


    # Aggregate. Group detection events to get volumes by gyr
    df = df[df.CountDetector == True].drop('CountDetector', axis=1)

    volumes = (df.set_index(['EventCode','PhaseStart'], append=True)
                 .groupby(level=[0,1,2,3])
                 .count()[['DetTimeStamp']]
                 .rename(columns={'DetTimeStamp':'Volume'})
                 .reset_index()
                 .astype({'EventCode': 'int64', 'Volume': 'int64'}))

    # clean up
    df = (gyr.reset_index()
             .merge(right=volumes,
                    how='outer',
                    left_on=['SignalID','Phase','EventCode','StartTimeStamp'],
                    right_on=['SignalID','Phase','EventCode','PhaseStart'])
             .drop('PhaseStart', axis=1)
             .rename(columns={'StartTimeStamp':'PhaseStart',
                              'EndTimeStamp':'PhaseEnd',
                              'DetTimeStamp':'Volume',
                              'gyrDuration':'Duration'})
             .set_index(['SignalID','Phase']))

    df.Volume = df.Volume.fillna(0).astype('int64')

    cycles = df[['CycleStart','PhaseStart','PhaseEnd','EventCode','Duration','Volume']]

    if aggregate==True:

        return cycles

    else:

        return (cycles, detections_by_cycle)

def etl_main(df, det_config):
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
    df = df.astype({'TimeStamp': 'datetime64[us]', 'EventCode': 'int64', 'EventParam': 'int64'})

    # cycles are common to all phases
    cycles = (df.query('EventCode==31 and EventParam==1')
                .set_index(['SignalID'])['TimeStamp'])

    terminations = (df.query('EventCode in [4,5,6]')
                      .set_index(['SignalID','EventParam'])
                      .sort_values('TimeStamp').sort_index())

    if len(cycles) > 0 and len(terminations) > 0:
        detections = get_detector_pairs(df, det_config)

        gt = get_green_time(df)
        yt = get_yellow_time(df)
        rt = get_red_time(df)

        gyr = (pd.concat([assign_cycle(gt, cycles),
                          assign_cycle(yt, cycles),
                          assign_cycle(rt, cycles)])
                 .sort_values(['CycleStart','StartTimeStamp']).sort_index()
                 .astype({'EventCode': 'int64'}))

        gyrv, detections_by_cycle = get_volume_by_phase(gyr, detections, aggregate=False)

        # Add termination type to green phase
        gyrvt = (pd.merge(left=gyrv.reset_index(),
                          right=terminations.reset_index(),
                          how='outer',
                          left_on=['SignalID','Phase','PhaseEnd'],
                          right_on=['SignalID','EventParam','TimeStamp'])
                   .set_index(['SignalID','Phase'])
                   .drop(['TimeStamp','EventParam'], axis=1)
                   .rename(columns={'EventCode_x':'EventCode', 'EventCode_y':'TermType'})
                   .dropna(axis=0, how='any', subset=['EventCode'])
                   .reset_index())
        cycles = (gyrvt.assign(TermType = gyrvt.TermType.fillna(0).astype('int64'),
                               EventCode = gyrvt.EventCode.astype('int64'),
                               SignalID = gyrvt.SignalID.astype('int64'),
                               Phase = gyrvt.Phase.astype('int64'),
                               Volume = gyrvt.Volume.astype('int64'))
                       .filter(['SignalID','Phase',
                                'CycleStart','PhaseStart','PhaseEnd',
                                'EventCode','TermType','Duration','Volume']))

        return cycles, detections_by_cycle

    else:
        return pd.DataFrame(), pd.DataFrame()


if __name__=='__main__':

    print('')
