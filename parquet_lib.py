#import feather
import pandas as pd
import boto3
import os
import re
import io
from pandas.tseries.offsets import Day
from datetime import datetime, timedelta
from glob import glob
import random
import string
from retrying import retry
from multiprocessing import get_context


def random_string(length):
    x = ''.join([random.choice(string.ascii_letters + string.digits) for n in range(length)])
    return x +  datetime.now().strftime('%H%M%S%f')

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

ath = boto3.client('athena')
s3 = boto3.client('s3')


@retry(wait_random_min=5000, wait_random_max=10000, stop_max_attempt_number=10)
def upload_parquet(Bucket, Key, Filename, Database):
    feather_filename = Filename
    df = pd.read_feather(feather_filename).drop(columns = ['Date'], errors = 'ignore')
    df.to_parquet('s3://{b}/{k}'.format(b=Bucket, k=Key))

    date_ = re.search('\d{4}-\d{2}-\d{2}', Key).group(0)
    table_name = re.search('mark/(.*?)/date', Key).groups()[0]

    template_string = 'ALTER TABLE {t} add partition (date="{d}") location "s3://{b}/{p}/"'
    partition_query = template_string.format(t = table_name,
                                             d = date_,
                                             b = Bucket,
                                             p = os.path.dirname(Key))
    print(partition_query)

    response = ath.start_query_execution(QueryString = partition_query,
                                         QueryExecutionContext={'Database': Database},
                                         ResultConfiguration={'OutputLocation': 's3://{}-athena'.format(Bucket)})
    #print('Response HTTPStatusCode:', response['HTTPStatusCode'])


@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
def get_keys_older(bucket, prefix):
    objs = s3.list_objects(Bucket = bucket, Prefix = prefix)
    if 'Contents' in objs:
        return [contents['Key'] for contents in objs['Contents']]


@retry(wait_random_min=1000, wait_random_max=2000, stop_max_attempt_number=10)
def get_keys_(bucket, prefix):
    paginator = s3.get_paginator('list_objects')

    # Create a PageIterator from the Paginator
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix)

    for contents in [page['Contents'] for page in page_iterator if 'Contents' in page]:
        keys = [content['Key'] for content in contents]
        for key in keys:
            yield key


def get_keys(bucket, table_name, start_date, end_date):
    # Need to add a day to end_date because
    # mark/date=2019-03-31/sf_2019-03-31.parquet isn't equal to or less than
    # mark/date=2019-03-31, which would be our end_date for March, for example
    dt_format = '%Y-%m-%d'
    end_date_plus1 = (datetime.strptime(end_date, dt_format) + timedelta(days=1)).strftime(dt_format)

    prefix = 'mark/{t}'.format(t=table_name)
    start_prefix = '{pre}/date={d}'.format(pre=prefix, d=start_date)
    end_prefix = '{pre}/date={d}'.format(pre=prefix, d=end_date_plus1)

    all_keys = list(get_keys_(bucket, prefix))
    # end_prefix is based on end_date_plus1 so we use key < end_prefix
    keys = [key for key in all_keys if (start_prefix <= key < end_prefix)]

    return keys


def download_and_read_parquet(bucket_key):
    df = (pd.read_parquet('s3://{}'.format(bucket_key)).
            assign(Date = re.search('\d{4}-\d{2}-\d{2}', bucket_key).group(0)))

    return df


def read_parquet(bucket, table_name, start_date, end_date, signals_list = None):

    keys = list(get_keys(bucket, table_name, start_date, end_date))

    if not keys:
        return None
    elif len(keys) == 1:
        df = download_and_read_parquet(bucket + '/' + keys[0])
    else: #len(keys) > 1:
        bucket_keys = [bucket + '/' + key for key in keys]
        with get_context('spawn').Pool() as pool:
            results = pool.map_async(download_and_read_parquet, bucket_keys)
            pool.close()
            pool.join()
        dfs = results.get()

        df = pd.concat(dfs, sort=True)

    if signals_list:
        df = df[df.SignalID.isin(signals_list)]

    feather_filename = '{t}_{d}_{r}.feather'.format(t=table_name, d=start_date, r=random_string(12))
    df.reset_index().drop(columns=['index']).to_feather(feather_filename)

    return feather_filename


def read_parquet_local(table_name, start_date, end_date, signals_list = None):

    def read_parquet(fn):
        filename = os.path.basename(fn)
        date_ = re.search('\d{4}-\d{2}-\d{2}', fn).group(0)
        df = pd.read_parquet(filename).assign(Date = date_)

        return df

    def in_date_range(start_filename, end_filename):
        return lambda x: x >= start_filename and x <= end_filename

    start_filename = '/home/rstudio/Code/GDOT/MARK/{t}/date={d}'.format(t=table_name, d=start_date)
    end_filename = '/home/rstudio/Code/GDOT/MARK/{t}/date={d}'.format(t=table_name, d=end_date)

    check = in_date_range(start_filename, end_filename)

    feather_filename = table_name + '.feather'
    fns = list(filter(check, list(glob('/home/rstudio/Code/GDOT/MARK/{t}/*/*'.format(t=table_name)))))
    df = pd.concat([read_parquet(fn) for fn in fns], sort = True)
    if signals_list:
        df = df[df.SignalID.isin(signals_list)]
    df.reset_index().to_feather(feather_filename)

    return feather_filename


def read_parquet_file(bucket, key):

    if 'Contents' in s3.list_objects(Bucket = bucket, Prefix = key):

        date_ = re.search('\d{4}-\d{2}-\d{2}', key).group(0)

        df = (pd.read_parquet('s3://{b}/{k}'.format(b=bucket, k=key))
                .assign(Date = lambda x: pd.to_datetime(date_, format='%Y-%m-%d'))
                .rename(columns = {'Timestamp': 'TimeStamp'}))

    else:
        df = pd.DataFrame()

    return df


def get_s3data_dask(bucket, prefix):

    df = dd.read_parquet('s3://{b}/{p}'.format(b=bucket, p=prefix))

    # Can't have None data to convert to R data frame
    if sum(df.isnull().any(1).compute()) > 0:
        for c in df.select_dtypes(include=['int8', 'int16', 'int32', 'int64', 'float']).columns:
            df[c] = df[c].fillna(-1)
        for c in df.select_dtypes(include='object').columns:
            df[c] = df[c].fillna('')

    return df.compute()


def query_athena(query, database, output_bucket):

    response = ath.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': 's3://{}'.format(output_bucket)
        }
    )
    print ('Started query.')
    # Wait for s3 object to be created
    polling.poll(
            lambda: 'Contents' in s3.list_objects(Bucket=output_bucket,
                                                  Prefix=response['QueryExecutionId']),
            step=0.5,
            timeout=30)
    print ('Query complete.')
    key = '{}.csv'.format(response['QueryExecutionId'])
    time.sleep(1)
    s3.download_file(Bucket=output_bucket, Key=key, Filename=key)
    df = pd.read_csv(key)
    os.remove(key)

    print ('Results downloaded.')
    return df


if __name__ == '__main__':

    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.Loader)

    Bucket = conf['bucket']
    Key = 'mark/comm_uptime/date=2019-02-15/cu_2019-02-15.parquet'
    Filename = 'cu_2019-02-15.feather'

    upload_parquet(Bucket, Key, Filename)

    read_parquet(conf['bucket'], 'comm_uptime', '2019-02-15', '2019-02-15')


