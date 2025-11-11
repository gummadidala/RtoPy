#!/usr/bin/env python3
"""
get_alerts.py - Python conversion of get_alerts.R
Processes watchdog alerts from S3 and uploads consolidated alerts

This script runs after the main monthly report pipeline.
"""

import pandas as pd
import boto3
from io import BytesIO
import zipfile
import yaml
from datetime import datetime, timedelta, date
import os
from pathlib import Path
import pyarrow.parquet as pq
import pickle
import warnings
import logging
from typing import Dict, List, Optional, Any

warnings.simplefilter("ignore", category=FutureWarning)


logger = logging.getLogger(__name__)

def read_zipped_feather(file_obj):
    """Read feather file from zip archive"""
    with zipfile.ZipFile(file_obj, 'r') as zip_ref:
        # Assuming single file in zip
        filename = zip_ref.namelist()[0]
        with zip_ref.open(filename) as f:
            return pd.read_feather(f)


# Read configuration
with open("Monthly_Report.yaml", 'r') as file:
    conf = yaml.safe_load(file)

def get_alerts(conf):
    """
    Get and process watchdog alerts from S3
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        DataFrame with processed alerts
    """
    logger.info("Starting get_alerts processing")
    s3_client = boto3.client('s3')
    
    # Get bucket objects with prefix
    logger.info(f"Reading watchdog data from s3://{conf['bucket']}/mark/watchdog/")
    response = s3_client.list_objects_v2(
        Bucket=conf['bucket'],
        Prefix='mark/watchdog/'
    )
    
    alerts_list = []
    
    if 'Contents' in response:
        logger.info(f"Found {len(response['Contents'])} files in watchdog folder")
        for obj in response['Contents']:
            key = obj['Key']
            logger.debug(f"Processing: {key}")
            
            df = None
            if key.endswith("feather.zip"):
                obj_response = s3_client.get_object(Bucket=conf['bucket'], Key=key)
                df = read_zipped_feather(BytesIO(obj_response['Body'].read()))
            elif key.endswith("parquet") and not key.endswith("alerts.parquet"):
                obj_response = s3_client.get_object(Bucket=conf['bucket'], Key=key)
                df = pd.read_parquet(BytesIO(obj_response['Body'].read()))
            
            if df is not None:
                df['SignalID'] = df['SignalID'].astype('category')
                df['Detector'] = df['Detector'].astype('category')
                df['Date'] = pd.to_datetime(df['Date'])
                alerts_list.append(df)
    
    if alerts_list:
        alerts = pd.concat(alerts_list, ignore_index=True)
        logger.info(f"Loaded {len(alerts)} alert records from S3")
    else:
        logger.warning("No alert data found in watchdog folder")
        return pd.DataFrame(columns=[
            'Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 
            'Detector', 'Date', 'Name', 'Alert', 'ApproachDesc', 'streak'
        ])
    
    alerts = alerts.dropna(subset=['Corridor'])
    logger.info(f"After filtering: {len(alerts)} alerts with corridor assignment")
    
    alerts['Detector'] = alerts['Detector'].fillna('0').astype('category')
    alerts['CallPhase'] = alerts['CallPhase'].fillna('0').astype('category')
    
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
    
    cutoff_date = pd.Timestamp.today().normalize() - pd.Timedelta(days=180)
    alerts = alerts[alerts['Date'] > cutoff_date]
    
    alerts = alerts.drop_duplicates().sort_values([
        'Alert', 'SignalID', 'CallPhase', 'Detector', 'Date'
    ])
    
    alerts['ApproachDesc'] = (alerts
                             .groupby(['SignalID', 'Detector'], observed=False)['ApproachDesc']
                             .bfill())
    alerts['ApproachDesc'] = alerts['ApproachDesc'].fillna('')
    
    rms_alerts = alerts[alerts['Zone'] == 'Ramp Meters'].copy()
    alerts = alerts[alerts['Zone'] != 'Ramp Meters'].copy()
    logger.info(f"Split into {len(rms_alerts)} ramp meter alerts and {len(alerts)} regular alerts")
    
    if not rms_alerts.empty:
        rms_alerts['ApproachDesc'] = (rms_alerts
                                     .groupby(['SignalID', 'Detector'], observed=False)['ApproachDesc']
                                     .transform('max'))
        
        rms_alerts['CallPhase'] = rms_alerts['ApproachDesc'].str.extract(r'^([^ -]+)')[0]
        valid_phases = ['Mainline', 'Passage', 'Demand', 'Queue']
        rms_alerts['CallPhase'] = rms_alerts['CallPhase'].apply(
            lambda x: x if x in valid_phases else 'Other'
        )
        
        rms_alerts['ApproachDesc'] = rms_alerts['ApproachDesc'].str.replace(
            r'Mainline-\S+', 'Mainline', regex=True
        )
        
        ml_dets = (rms_alerts
                  .groupby(['SignalID', 'ApproachDesc'], observed=False)['Detector']
                  .apply(lambda x: '/'.join(sorted(set(x.astype(str))))).reset_index()
                  .rename(columns={'Detector': 'detector'}))
        
        rms_alerts = rms_alerts.merge(ml_dets, on=['SignalID', 'ApproachDesc'])
        rms_alerts['Detector'] = rms_alerts['detector'].astype('category')
        rms_alerts = rms_alerts.drop('detector', axis=1).drop_duplicates()
    
    if not rms_alerts.empty:
        alerts = pd.concat([alerts, rms_alerts], ignore_index=True)
        logger.info(f"Recombined into {len(alerts)} total alerts")
    
    def calculate_streaks(group):
        """Calculate 90-day streaks for alerts - matches R streak_run(k=90)"""
        group = group.sort_values('Date')
        group['date_diff'] = group['Date'].diff().dt.days
        group['start_streak'] = group['Date'].where(
            (group['date_diff'] > 1) | (group['Date'] == group['Date'].min())
        )
        group['start_streak'] = group['start_streak'].ffill()
        
        streak_groups = group.groupby('start_streak')
        streaks = []
        for _, streak_group in streak_groups:
            if len(streak_group) <= 90:
                streaks.extend([1] * len(streak_group))
            else:
                streaks.extend([1] * 90 + [0] * (len(streak_group) - 90))
        group['streak'] = streaks
        return group.drop(['date_diff', 'start_streak'], axis=1)
    
    logger.info("Calculating alert streaks...")
    if not alerts.empty:
        alerts = (
            alerts
            .groupby(
                ['Zone_Group', 'Zone', 'SignalID', 'CallPhase', 'Detector', 'Alert'],
                observed=False,
                group_keys=False
            )
            .apply(calculate_streaks)
            .reset_index(drop=True)
        )
        logger.info(f"Streak calculation complete - {len(alerts)} alert records")
    else:
        alerts['streak'] = []
        logger.warning("No alerts to process")
    
    return alerts

def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """
    Setup logging configuration
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    
    # Convert string level to logging constant
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Setup console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(console_handler)
    
    # Setup file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

if __name__ == "__main__":
    setup_logging("INFO", "get_alerts.log")
    logger.info("=" * 80)
    logger.info("GET ALERTS - Watchdog Alert Processing")
    logger.info("=" * 80)
    
    try:
        start_time = datetime.now()
        logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        alerts = get_alerts(conf)
        
        if alerts.empty:
            logger.warning("No alerts data available - exiting")
            exit(0)
        
        logger.info(f"Total alerts to upload: {len(alerts)}")
        logger.info(f"Date range: {alerts['Date'].min()} to {alerts['Date'].max()}")
        logger.info(f"Unique signals: {alerts['SignalID'].nunique()}")
        logger.info(f"Alert types: {alerts['Alert'].nunique()}")
    
    except Exception as e:
        logger.error(f"Error processing alerts: {e}")
        import traceback
        logger.error(traceback.format_exc())
        exit(1)
    
    # Upload to S3
    try:
        logger.info("Uploading alerts to S3...")
        s3_client = boto3.client('s3')
        
        # Convert to strings for upload
        alerts['SignalID'] = alerts['SignalID'].astype(str)
        alerts['Detector'] = alerts['Detector'].astype(str)
        
        # Upload as pickle (.qs equivalent)
        pickle_buffer = BytesIO()
        pickle.dump(alerts, pickle_buffer)
        pickle_buffer.seek(0)
        s3_client.put_object(
            Bucket=conf['bucket'],
            Key='mark/watchdog/alerts.qs',
            Body=pickle_buffer.getvalue()
        )
        logger.info(f"Uploaded alerts.qs to s3://{conf['bucket']}/mark/watchdog/")

        # Upload as parquet
        parquet_buffer = BytesIO()
        alerts.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        s3_client.put_object(
            Bucket=conf['bucket'],
            Key='mark/watchdog/alerts.parquet',
            Body=parquet_buffer.getvalue()
        )
        logger.info(f"Uploaded alerts.parquet to s3://{conf['bucket']}/mark/watchdog/")
        
        # Success
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"✅ Processing completed successfully in {duration}")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ Failed to upload to S3: {e}")
        import traceback
        logger.error(traceback.format_exc())
        exit(1)