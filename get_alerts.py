import pandas as pd
import boto3
from io import BytesIO
import zipfile
import yaml
from datetime import datetime, timedelta, date
import os
import logging
from pathlib import Path
import pyarrow.parquet as pq
import pickle

# Import custom modules (assuming these exist)
# from monthly_report_package_init import *
# from write_sigops_to_db import *

def read_zipped_feather(file_obj):
    """Read feather file from zip archive"""
    with zipfile.ZipFile(file_obj, 'r') as zip_ref:
        # Assuming single file in zip
        filename = zip_ref.namelist()[0]
        with zip_ref.open(filename) as f:
            return pd.read_feather(f)

# Setup logging directory
log_path = "./logs"
Path(log_path).mkdir(parents=True, exist_ok=True)

# Read configuration
with open("Monthly_Report.yaml", 'r') as file:
    conf = yaml.safe_load(file)

def get_alerts(conf):
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Get bucket objects with prefix
    response = s3_client.list_objects_v2(
        Bucket=conf['bucket'],
        Prefix='mark/watchdog/'
    )
    
    alerts_list = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            print(key)
            
            df = None
            if key.endswith("feather.zip"):
                # Download and read zipped feather
                obj_response = s3_client.get_object(Bucket=conf['bucket'], Key=key)
                df = read_zipped_feather(BytesIO(obj_response['Body'].read()))
            elif key.endswith("parquet") and not key.endswith("alerts.parquet"):
                # Download and read parquet
                obj_response = s3_client.get_object(Bucket=conf['bucket'], Key=key)
                df = pd.read_parquet(BytesIO(obj_response['Body'].read()))
            
            if df is not None:
                # Convert to categorical and process dates
                df['SignalID'] = df['SignalID'].astype('category')
                df['Detector'] = df['Detector'].astype('category')
                df['Date'] = pd.to_datetime(df['Date']).dt.date
                alerts_list.append(df)
    
    # Combine all dataframes
    if alerts_list:
        alerts = pd.concat(alerts_list, ignore_index=True)
    else:
        # Return empty dataframe with expected columns
        return pd.DataFrame(columns=[
            'Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 
            'Detector', 'Date', 'Name', 'Alert', 'ApproachDesc', 'streak'
        ])
    
    # Filter and clean data
    alerts = alerts.dropna(subset=['Corridor'])
    
    # Fill NA values
    alerts['Detector'] = alerts['Detector'].fillna('0').astype('category')
    alerts['CallPhase'] = alerts['CallPhase'].fillna('0').astype('category')
    
    # Select and transform columns
    alerts = alerts.assign(
        Zone_Group=alerts['Zone_Group'].astype('category'),
        Zone=alerts['Zone'].astype('category'),
        Corridor=alerts['Corridor'].astype('category'),
        SignalID=alerts['SignalID'].astype('category'),
        CallPhase=alerts['CallPhase'].astype('category'),
        Detector=alerts['Detector'].astype('category'),
        Date=alerts['Date'],
        Name=alerts['Name'].astype(str),
        Alert=alerts['Alert'].astype('category'),
        ApproachDesc=alerts.get('ApproachDesc', '')
    )[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 
       'Detector', 'Date', 'Name', 'Alert', 'ApproachDesc']]
    
    # Filter by date (last 180 days)
    cutoff_date = date.today() - timedelta(days=180)
    alerts = alerts[alerts['Date'] > cutoff_date]
    
    # Remove duplicates and sort
    alerts = alerts.drop_duplicates().sort_values([
        'Alert', 'SignalID', 'CallPhase', 'Detector', 'Date'
    ])
    
    # Fill ApproachDesc forward within groups
    alerts['ApproachDesc'] = (alerts
                             .groupby(['SignalID', 'Detector'])['ApproachDesc']
                             .fillna(method='bfill'))
    alerts['ApproachDesc'] = alerts['ApproachDesc'].fillna('')
    
    # Separate ramp meters
    rms_alerts = alerts[alerts['Zone'] == 'Ramp Meters'].copy()
    alerts = alerts[alerts['Zone'] != 'Ramp Meters'].copy()
    
    if not rms_alerts.empty:
        # Process ramp meters
        # Fix ApproachDesc for ramp meters
        rms_alerts['ApproachDesc'] = (rms_alerts
                                     .groupby(['SignalID', 'Detector'])['ApproachDesc']
                                     .transform('max'))
        
        # Extract CallPhase from ApproachDesc
        rms_alerts['CallPhase'] = rms_alerts['ApproachDesc'].str.extract(r'^([^ -]+)')[0]
        valid_phases = ['Mainline', 'Passage', 'Demand', 'Queue']
        rms_alerts['CallPhase'] = rms_alerts['CallPhase'].apply(
            lambda x: x if x in valid_phases else 'Other'
        )
        
        # Clean ApproachDesc
        rms_alerts['ApproachDesc'] = rms_alerts['ApproachDesc'].str.replace(
            r'Mainline-\S+', 'Mainline', regex=True
        )
        
        # Group mainline detectors
        ml_dets = (rms_alerts
                  .groupby(['SignalID', 'ApproachDesc'])['Detector']
                  .apply(lambda x: '/'.join(sorted(set(x.astype(str)))))
                  .reset_index()
                  .rename(columns={'Detector': 'detector'}))
        
        rms_alerts = rms_alerts.merge(ml_dets, on=['SignalID', 'ApproachDesc'])
        rms_alerts['Detector'] = rms_alerts['detector'].astype('category')
        rms_alerts = rms_alerts.drop('detector', axis=1).drop_duplicates()
    
    # Combine alerts back together
    if not rms_alerts.empty:
        alerts = pd.concat([alerts, rms_alerts], ignore_index=True)
    
    # Calculate streaks
    def calculate_streaks(group):
        group = group.sort_values('Date')
        group['date_diff'] = group['Date'].diff().dt.days
        group['start_streak'] = group['Date'].where(
            (group['date_diff'] > 1) | (group['Date'] == group['Date'].min())
        )
        group['start_streak'] = group['start_streak'].fillna(method='ffill')
        
        # Calculate streak (simplified version of streak_run with k=90)
        streak_groups = group.groupby('start_streak')
        streaks = []
        for _, streak_group in streak_groups:
            if len(streak_group) <= 90:
                streaks.extend([1] * len(streak_group))
            else:
                streaks.extend([1] * 90 + [0] * (len(streak_group) - 90))
        group['streak'] = streaks
        return group.drop(['date_diff', 'start_streak'], axis=1)
    
    if not alerts.empty:
        alerts = (alerts
                 .groupby(['Zone_Group', 'Zone', 'SignalID', 'CallPhase', 'Detector', 'Alert'])
                 .apply(calculate_streaks)
                 .reset_index(drop=True))
    else:
        alerts['streak'] = []
    
    return alerts

# Execute main function
alerts = get_alerts(conf)

# Upload to S3 Bucket
try:
    s3_client = boto3.client('s3')
    
    # Save as pickle (equivalent to .qs)
    pickle_buffer = BytesIO()
    pickle.dump(alerts, pickle_buffer)
    pickle_buffer.seek(0)
    
    s3_client.put_object(
        Bucket=conf['bucket'],
        Key='mark/watchdog/alerts.qs',
        Body=pickle_buffer.getvalue()
    )
    
    # Save as parquet
    parquet_buffer = BytesIO()
    alerts.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)
    
    s3_client.put_object(
        Bucket=conf['bucket'],
        Key='mark/watchdog/alerts.parquet',
        Body=parquet_buffer.getvalue()
    )
    
    # Log success
    log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|SUCCESS|get_alerts.py|get_alerts|Line 173|Uploaded {conf['bucket']}/mark/watchdog/alerts.qs\n"
    with open(os.path.join(log_path, f"get_alerts_{date.today()}.log"), 'a') as f:
        f.write(log_message)

except Exception as e:
    # Log error
    log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}|ERROR|get_alerts.py|get_alerts|Line 173|Failed to upload to S3 - {str(e)}\n"
    with open(os.path.join(log_path, f"get_alerts_{date.today()}.log"), 'a') as f:
        f.write(log_message)