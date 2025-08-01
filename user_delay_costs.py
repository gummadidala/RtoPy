"""
Created on Tue Nov 26 09:46:47 2019

@author: Anthony.Gallo
"""

import os
import shutil
import io
import pandas as pd
import requests
import uuid
import polling
import random
import time  #unix
import yaml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  #needed for 1 year subtraction
import boto3
import json
import re
import dask.dataframe as dd
from multiprocessing import get_context
from config import get_date_from_string
import warnings

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

os.environ['TZ'] = 'America/New_York'

s3 = boto3.client('s3')

pd.options.display.max_columns = 10


# modify corridor string so it can be a filename
def get_filename(corridor):
    return re.sub(pattern=r'[^A-Za-z0-9_\-\\]', repl='_', string=corridor) + '.parquet'


# run query, return hourly UDC totals for the month/corridor/zone
def get_udc_response(start_date, end_date, threshold, zone, corridor, tmcs,
                     key):
    uri = 'http://pda-api.ritis.org:8080/{}'

    month = (datetime.strptime(start_date,
                               '%Y-%m-%d').date()).strftime('%b %Y')

    payload = {
        "aadtPriority": ["gdot_2018", "inrix_2019"],  # ["GDOT_2018", "INRIX"],
        "calculateAgainst":
        "AVERAGE_SPEED",  #choices are FREEFLOW or AVERAGE_SPEED
        "costs": {
            "catt.inrix.udc.commercial":  #need to enter costs for each year
            {
                "2021": 100.49,
                "2022": 100.49
            },
            "catt.inrix.udc.passenger": {
                "2021": 17.91,
                "2022": 17.91
            }
        },
        "dataSource": "vpp_here",
        "dates": [{
            "end": end_date,
            "start": start_date
        }],
        "percentCommercial": 10,  #fallback for % trucks if the data isn't available
        "threshold": threshold,  #delay calculated where speed falls below threshold type by threshold mph
        "thresholdType": "AVERAGE",  #choices are AVERAGE (historic avg speed), REFERENCE (freeflow speed), or ABSOLUTE/NONE
        "times": [{
            "end": None,
            "start": "00:00:00.000"
        }],
        "tmcPercentMappings":
        {},  #can consider some % of TMC length in UDC calc; can leave blank
        "tmcs": tmcs,
        "useDefaultPercent":
        False,  #if true, uses the percentCommercial value for all segments instead of as a fallback
        "uuid": str(uuid.uuid1())
    }

    start_year = pd.Timestamp(start_date).strftime('%Y')
    end_year = pd.Timestamp(end_date).strftime('%Y')

    payload['costs']['catt.inrix.udc.commercial'][start_year] = 100.49
    payload['costs']['catt.inrix.udc.passenger'][start_year] = 17.91

    payload['costs']['catt.inrix.udc.commercial'][end_year] = 100.49
    payload['costs']['catt.inrix.udc.passenger'][end_year] = 17.91
    #----------------------------------------------------------
    try:

        response = requests.post(uri.format('jobs/udc'),
                                 params={'key': key},
                                 json=payload)
        print('response status code:', response.status_code)

        while response.status_code == 429:
            time.sleep(random.uniform(30, 60))
            print('waiting...')

        if response.status_code == 200:  # Only if successful response
            print(
                f'{datetime.now().strftime("%H:%M:%S")} Starting query for {month}, {zone}, {corridor}'
            )

            # retry at intervals of 'step' until results return (status code = 200)
            jobid = json.loads(response.content.decode('utf-8'))['id']
            while True:
                x = requests.get(uri.format('jobs/status'),
                                 params={
                                     'key': key,
                                     'jobId': jobid
                                 })
                print(f'jobid: {jobid} | status code: {x.status_code}',
                      end=' | ')
                if x.status_code == 429:  # this code means too many requests. wait longer.
                    time.sleep(random.uniform(90, 120))
                elif x.status_code == 200:
                    print(
                        f"state: {json.loads(x.content.decode('utf-8'))['state']}"
                    )
                    if json.loads(
                            x.content.decode('utf-8'))['state'] == 'SUCCEEDED':
                        break
                    else:
                        time.sleep(random.uniform(30, 60))
                else:
                    time.sleep(30)

            results = requests.get(uri.format('jobs/udc/results'),
                                   params={
                                       'key': key,
                                       'uuid': payload['uuid']
                                   })

            print(
                f'{month} {datetime.now().strftime("%H:%M:%S")}: {zone}, {corridor} results received'
            )

            strResults = results.content.decode('utf-8')
            jsResults = json.loads(strResults)

            df = pd.json_normalize(jsResults['daily_results'])
            dfDailyValues = pd.concat(
                [pd.json_normalize(x) for x in df['values']])
            dfDailyValues.insert(0, 'corridor', corridor, True)
            dfDailyValues.insert(0, 'zone', zone, True)
            dfDailyValues['date'] = pd.to_datetime(dfDailyValues['date'], format='%B %d, %Y %H:%M:%S')

            udc_path = f'user_delay_costs/{start_date}'
            if not os.path.exists(udc_path):
                os.makedirs(udc_path)
            dfDailyValues.to_parquet(os.path.join(udc_path, get_filename(corridor)))

            print(f'{month}: {zone}, {corridor} - SUCCESS')
            return dfDailyValues

        else:
            print(
                f'{month}: {zone}, {corridor}: no results received - {response.content.decode("utf-8")}'
            )
            return pd.DataFrame()

    except Exception as e:
        print(('{}: {}, {}: Error: {}').format(month, zone, corridor, e))
        return pd.DataFrame()


def get_udc_data(start_date,
                 end_date,
                 zone_corridor_tmc_df,
                 key,
                 initial_sleep_sec=0):

    # Allow sleep time to space out requests when running in a loop
    time.sleep(initial_sleep_sec)

    # 10 mph below historic avg speed;
    # this may change for certain corridors in which case script will need to be more dynamic
    threshold = 10
    start_date_1yr = (pd.Timestamp(start_date) - pd.DateOffset(years=1)).strftime('%F')
    end_date_1yr = (pd.Timestamp(end_date) - pd.DateOffset(years=1)).strftime('%F')

    # Get TMCs list by Zone/Corridor combinations
    zcdf = zone_corridor_tmc_df.groupby(
        ['Zone', 'Corridor']).apply(lambda x: x.tmc.values.tolist())
    zcdf = zcdf.reset_index().rename(columns={0: 'tmcs'})
    zcdf = zcdf[zcdf.Zone.str.startswith('Zone')]  # RTOP only. Takes too long to run otherwise.
    zcdf['threshold'] = threshold
    zcdf['key'] = key


    # Results are stored locally. Don't re-run corridors if script is interrupted and we need to run again.
    # Current Month
    zcdf0 = zcdf.copy()
    zcdf0['start_date'] = start_date
    zcdf0['end_date'] = end_date
    zcdf0 = zcdf0[[
        'start_date', 'end_date', 'threshold', 'Zone', 'Corridor', 'tmcs',
        'key'
    ]]

    corridors_to_run = list(filter(
        lambda x: not os.path.exists(f'user_delay_costs/{start_date}/{get_filename(x)}'),
        zcdf0.Corridor))
    print(f'{len(corridors_to_run)} corridors to run')
    zcdf0 = zcdf0[zcdf0.Corridor.isin(corridors_to_run)]


    # Results are stored locally. Don't re-run corridors if script is interrupted and we need to run again.
    # Current month, one year ago
    zcdf1 = zcdf.copy()
    zcdf1['start_date'] = start_date_1yr
    zcdf1['end_date'] = end_date_1yr
    zcdf1 = zcdf1[[
        'start_date', 'end_date', 'threshold', 'Zone', 'Corridor', 'tmcs',
        'key'
    ]]

    corridors_to_run = list(filter(
        lambda x: not os.path.exists(f'user_delay_costs/{start_date_1yr}/{get_filename(x)}'),
        zcdf1.Corridor))
    print(f'{len(corridors_to_run)} corridors to run')
    zcdf1 = zcdf1[zcdf1.Corridor.isin(corridors_to_run)]


    # Get user delay costs for each Zone/Corridor for current month.
    if len(zcdf0):
        with get_context('spawn').Pool(2) as pool:
            pool.starmap_async(
                get_udc_response,
                [list(row.values()) for row in zcdf0.to_dict(orient='records')])
            pool.close()
            pool.join()

    # Get user delay costs for each Zone/Corridor for current month one year ago.
    if len(zcdf1):
        with get_context('spawn').Pool(2) as pool:
            pool.starmap_async(
                get_udc_response,
                [list(row.values()) for row in zcdf1.to_dict(orient='records')])
            pool.close()
            pool.join()

    df0 = pd.read_parquet(f'user_delay_costs/{start_date}')
    df1 = pd.read_parquet(f'user_delay_costs/{start_date_1yr}')
    df = pd.concat([df0, df1])

    shutil.rmtree(f'user_delay_costs/{start_date}/')
    shutil.rmtree(f'user_delay_costs/{start_date_1yr}/')

    print(f'{len(df)} records')
    return df


if __name__ == '__main__':

    with open('Monthly_Report_AWS.yaml') as yaml_file:
        cred = yaml.load(yaml_file, Loader=yaml.Loader)

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    start_date = conf['start_date']
    end_date = conf['end_date']

    start_date = get_date_from_string(
        start_date, s3bucket=conf['bucket'], s3prefix="mark/user_delay_costs"
    )
    end_date = get_date_from_string(end_date)

    # start_date is the first day of the month
    start_date = datetime.fromisoformat(start_date).strftime('%Y-%m-01')

    # end date is either the given date + 1 or today.
    # end_date is not included in the query results
    ed = datetime.fromisoformat(end_date)
    end_date = min(ed + timedelta(days=1), datetime.today()).strftime('%Y-%m-%d')

    #-- manual start and end dates
    # start_date = '2022-07-01'
    # end_date = '2022-07-31'
    #---

    print(start_date)
    print(end_date)

    os.environ['AWS_ACCESS_KEY_ID'] = cred['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = cred['AWS_SECRET_ACCESS_KEY']
    os.environ['AWS_DEFAULT_REGION'] = cred['AWS_DEFAULT_REGION']

    with io.BytesIO() as data:
        s3.download_fileobj(Bucket=conf['bucket'],
                            Key=conf['corridors_TMCs_filename_s3'],
                            Fileobj=data)
        tmc_df = pd.read_excel(data)
    with io.BytesIO() as data:
        s3.download_fileobj(Bucket=conf['bucket'],
                            Key=conf['corridors_filename_s3'],
                            Fileobj=data)
        corridors_zones_df = pd.read_excel(data)

    zone_corridor_df = corridors_zones_df.drop_duplicates(
        ['Zone', 'Corridor'])[['Zone', 'Corridor']]
    zone_corridor_tmc_df = pd.merge(zone_corridor_df,
                                    tmc_df[['tmc', 'Corridor']],
                                    on='Corridor')
    zone_corridor_tmc_df = zone_corridor_tmc_df[~zone_corridor_tmc_df.Corridor.isna()]


    #--- test w/ just one zone and 2 corridors - takes a while to run
    #zone_test = 'Zone 1'
    #corridors_test = ['SR 237', 'SR 141S']
    #zone_corridor_tmc_df = zone_corridor_tmc_df[(zone_corridor_tmc_df.Zone == zone_test) & (zone_corridor_tmc_df.Corridor.isin(corridors_test))]
    #---

    try:

        udc_df = get_udc_data(start_date, end_date, zone_corridor_tmc_df,
                              cred['RITIS_KEY'], 0)

    except Exception as e:
        print('error retrieving records')
        print(e)
        udc_df = pd.DataFrame()

    if len(udc_df) > 0:
        filename = f'user_delay_costs_{start_date}.parquet'
        udc_df.to_parquet(filename)
        df = udc_df.sort_values(by=['zone', 'corridor', 'date'])

        bucket = conf['bucket']
        df.to_parquet(f's3://{bucket}/mark/user_delay_costs/date={start_date}/{filename}')

    else:
        print('No records returned.')


