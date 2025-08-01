"""
Author Justin Gerry

Original script modified from Alan Toppen's original python code

The purpose of this is to verify all database connections prior to running a script
We've burned hours only to have a connection be an issue. Its easier if we check up front

Modified for rstudio so we can test everything with one script

"""

import os
from datetime import datetime, timedelta
import boto3
from datetime import datetime, timedelta
import yaml
import pprint
import time
import re
import sqlalchemy as sq
import pyodbc
import urllib.parse
import configparser

base_path = '/data/sdb'

#logs_path = os.path.join(base_path, 'logs')
#if not os.path.exists(logs_path):
#    os.mkdir(logs_path)
#logger = mark1_logger(os.path.join(logs_path, f'database_connection_check_{datetime.today().strftime("%F")}.log'))

def check_db_name():
    # Read database connection parameters from ~/.odbc.ini file
    config = configparser.ConfigParser()
    odbc_ini_path = os.path.join(os.path.expanduser('~'), '.odbc.ini')
    config.read(odbc_ini_path)
    db = config['maxview']

    muid = os.environ['MAXV_USERNAME']
    mpwd = os.environ['MAXV_PASSWORD']

    # Connect to host without specifying database and get the list of databases.
    # This will always work regardless of whether the database names have changed.
    cxn_str = f"DRIVER={db['driver']};SERVER={db['server']};PORT={db['port']};UID={muid};PWD={mpwd}"
    with pyodbc.connect(cxn_str) as conn:
        with conn.cursor() as cursor:
            cursor.execute('SELECT name FROM sys.databases')
            databases = [row[0] for row in cursor.fetchall()]

    # Using regular expressions extract the latest MaxView and MaxView_EventLog databases.
    r = re.compile("MaxView_\d.*")
    maxview_database = max(filter(r.match, databases))

    r = re.compile("MaxView_EventLog_\d.*")
    maxview_eventlog_database = max(filter(r.match, databases))

    # Search and Replace the database parameter in ~/.odbc.ini using sed (Linux command) if necessary.
    if maxview_database != db['database']:
        print('Updating ~/.odbc.ini file')
        os.system(f"cp {odbc_ini_path} {odbc_ini_path}.bak") # create backup first
        os.system(f"sed -i 's/MaxView_\d.*/{maxview_database}/g' {odbc_ini_path}")
        os.system(f"sed -i 's/MaxView_EventLog_\d.*/{maxview_eventlog_database}/g' {odbc_ini_path}")


def get_db_engine(dsn, uid, pwd):
    engine = sq.create_engine(f'mssql+pyodbc://{uid}:{pwd}@{dsn}', pool_size=20)
    return engine


def get_atspm_engine():
    dsn = 'atspm'
    uid = os.environ['ATSPM_USERNAME']
    pwd = urllib.parse.quote_plus(os.environ['ATSPM_PASSWORD'])
    engine = get_db_engine(dsn, uid, pwd)
    return engine


def get_maxv_engine():
    check_db_name()
    dsn = 'maxview'
    muid = os.environ['MAXV_USERNAME']
    mpwd = urllib.parse.quote_plus(os.environ['MAXV_PASSWORD'])
    engine = get_db_engine(dsn, muid, mpwd)
    return engine


def get_mvel_engine():
    check_db_name()
    dsn = 'maxview_eventlog'
    muid = os.environ['MAXV_USERNAME']
    mpwd = urllib.parse.quote_plus(os.environ['MAXV_PASSWORD'])
    engine = get_db_engine(dsn, muid, mpwd)
    return engine


def get_mysql_engine():
    uid = os.environ['RDS_USERNAME']
    pwd = urllib.parse.quote_plus(os.environ['RDS_PASSWORD'])
    host = os.environ['RDS_HOST']
    db = os.environ['RDS_DATABASE']
    connection_string = f'mysql+mysqldb://{uid}:{pwd}@{host}/{db}'
    engine = sq.create_engine(connection_string, pool_size=20)
    return engine


def get_new_mysql_engine():
    with open('SigOps/Monthly_Report_AWS.yaml') as yaml_file:
                conf = yaml.load(yaml_file, Loader=yaml.FullLoader)
    host = conf['RDS_HOST']
    db = conf['RDS_DATABASE']
    uid = conf['RDS_USERNAME']
    pwd = conf['RDS_PASSWORD']
    connection_string = f'mysql+mysqldb://{uid}:{pwd}@{host}/{db}'
    engine = sq.create_engine(connection_string, pool_size=20)
    return engine


def check_db_conn(engine_name):
   try:
     testconn = engine_name.connect()
     testconn.close()
     return 1
   except Exception as e:
     print('Connection Error: '+str(engine_name)+':'+str(e))
     return 0


def check_s3_conn(key,conf,date_string):
    try:
      s3_objects=s3.list_objects(Bucket=conf['bucket'], Prefix=key)
      print(str(s3_objects['ResponseMetadata']['HTTPStatusCode']))
      print(str(s3_objects['Contents'][0]['Key']))
      print('S3 connection ok!')
      return 1
    except Exception as e:
      print('Error with s3: '+str(e))
      return 0


def check_athena_conn(dbquery,conf):
   try:
      response = ath.start_query_execution(
       QueryString = dbquery,
       QueryExecutionContext = {
        'Database': conf['database']
       },
       ResultConfiguration={'OutputLocation': athena['staging_dir']}
       )
      print('athena database is: '+ conf['database'])
      execution = ath.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
      while execution['QueryExecution']['Status']['State'] == 'QUEUED':
          execution = ath.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
          print('athena query is QUEUED')
          time.sleep(1)
      while execution['QueryExecution']['Status']['State'] == 'RUNNING':
          execution = ath.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
          print('athena query is RUNNING')
          time.sleep(1)
      print('athena result '+execution['QueryExecution']['Status']['State'])
      if execution['QueryExecution']['Status']['State']=='SUCCEEDED':
          return 1
      elif execution['QueryExecution']['Status']['State']=='FAILED':
          print(execution['QueryExecution']['Status']['AthenaError']['ErrorMessage'])
          return 0
      return 3
   except:
      print('Connection error with Athena')
      return 0

if __name__=='__main__':

    #DB check
    connectionlist= []
    try:
       connectionlist.append(get_atspm_engine())
       connectionlist.append(get_maxv_engine())
       connectionlist.append(get_mvel_engine())
       connectionlist.append(get_mysql_engine())
       connectionlist.append(get_new_mysql_engine())
    except Exception as e:
       print('Configuration error: '+str(e))

    for connection in connectionlist:
       if check_db_conn(connection) == 1:
          print('connection: '+str(connection)+' ok')
       else:
          print('connection error:  '+str(connection))

    #S3 check
    s3 = boto3.client('s3')
    with open('Monthly_Report.yaml') as yaml_file:
        conf = yaml.load(yaml_file, Loader=yaml.FullLoader)

    date_ = datetime.now() - timedelta(days=5)
    date_string = date_.strftime('%Y-%m-%d')

    bucketlist = []
    bucketlist.append('config/maxtime_ped_plans')

    for bucket in bucketlist:
        if check_s3_conn(bucket,conf,date_string) == 1:
            print(str(bucket)+' is ok')
        else:
            print(str(bucket)+' has a problem')

    #Athena check
    athena = conf['athena']
    ath = boto3.client('athena')
    dbquerylist = []
    dbquerylist.append('SHOW TABLES')

    for dbquery in dbquerylist:
       athenacheck=check_athena_conn(dbquery,athena)
       print(athenacheck)
       if athenacheck == 1:
          print(str(dbquery)+' is ok')
       elif athenacheck != 1:
          print(str(dbquery)+' has a problem')



