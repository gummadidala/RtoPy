import os
import sys
import pandas as pd
import numpy as np
import boto3
from datetime import datetime, timedelta, date
import tempfile
import zipfile
import yaml
from pathlib import Path
import logging
from typing import List, Optional
import traceback
from io import BytesIO

# Add the parent directory to the path to import local modules
sys.path.append(str(Path(__file__).parent.parent))

from s3_parquet_io import s3read_using, s3write_using
from utilities import keep_trying
from write_sigops_to_db import load_bulk_data
from database_functions import get_aurora_connection
import pyarrow.parquet as pq
import pyarrow.feather as feather

def read_zipped_feather(file_path: str) -> pd.DataFrame:
    """
    Read a zipped feather file
    
    Args:
        file_path: Path to the zipped feather file
    
    Returns:
        DataFrame from the feather file
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        # Extract the first file (assuming single file in zip)
        extracted_files = zip_ref.namelist()
        if not extracted_files:
            raise ValueError("No files found in zip archive")
        
        with zip_ref.open(extracted_files[0]) as f:
            with tempfile.NamedTemporaryFile() as temp_file:
                temp_file.write(f.read())
                temp_file.flush()
                return feather.read_feather(temp_file.name)

def streak_run(start_dates: pd.Series, k: int = 90) -> pd.Series:
    """
    Calculate streak runs similar to R's streak_run function
    
    Args:
        start_dates: Series of start dates
        k: Maximum streak length
    
    Returns:
        Series with streak indicators
    """
    # This is a simplified implementation
    # You may need to adjust based on the exact R streak_run behavior
    streaks = []
    current_streak = 0
    
    for i, date_val in enumerate(start_dates):
        if pd.isna(date_val):
            current_streak += 1
        else:
            current_streak = 1
        
        if current_streak >= k:
            streaks.append(k)
        else:
            streaks.append(current_streak)
    
    return pd.Series(streaks, index=start_dates.index)

def setup_logging():
    """Setup logging configuration"""
    log_path = "./logs"
    os.makedirs(log_path, exist_ok=True)
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s|%(levelname)s|get_alerts_sigops.py|%(funcName)s|%(message)s',
        handlers=[
            logging.FileHandler(f"{log_path}/get_alerts_{date.today()}.log"),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_alerts(conf: dict) -> pd.DataFrame:
    """
    Get alerts data from S3 bucket
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        DataFrame with processed alerts
    """
    logger = logging.getLogger(__name__)
    
    try:
        s3_client = boto3.client('s3')
        
        # List objects with prefix 'sigops/watchdog/'
        response = s3_client.list_objects_v2(
            Bucket=conf['bucket'],
            Prefix='sigops/watchdog/'
        )
        
        if 'Contents' not in response:
            logger.warning("No objects found with prefix 'sigops/watchdog/'")
            return pd.DataFrame()
        
        alerts_list = []
        
        for obj in response['Contents']:
            key = obj['Key']
            logger.info(f"Processing: {key}")
            
            try:
                if key.endswith("feather.zip"):
                    # Read zipped feather file
                    df = s3read_using(
                        read_zipped_feather,
                        bucket=conf['bucket'],
                        object=key
                    )
                elif key.endswith("parquet") and not key.endswith("alerts.parquet"):
                    # Read parquet file
                    df = s3read_using(
                        pd.read_parquet,
                        bucket=conf['bucket'],
                        object=key
                    )
                else:
                    continue
                
                if df is not None and not df.empty:
                    # Convert to proper data types
                    df['SignalID'] = df['SignalID'].astype('category')
                    if 'Detector' in df.columns:
                        df['Detector'] = df['Detector'].astype('category')
                    # df['Date'] = pd.to_datetime(df['Date']).dt.date
                    df['Date'] = pd.to_datetime(df['Date'])
                    
                    alerts_list.append(df)
                    
            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                continue
        
        if not alerts_list:
            logger.warning("No valid alert data found")
            return pd.DataFrame()
        
        # Combine all dataframes
        alerts = pd.concat(alerts_list, ignore_index=True)
        
        # Filter out rows where Corridor is NA
        alerts = alerts.dropna(subset=['Corridor'])
        
        # Fill NA values
        if 'Detector' not in alerts.columns:
            alerts['Detector'] = 0
        if 'CallPhase' not in alerts.columns:
            alerts['CallPhase'] = 0
            
        alerts['Detector'] = alerts['Detector'].fillna(0).astype('category')
        alerts['CallPhase'] = alerts['CallPhase'].fillna(0).astype('category')
        
        # Select and transform columns
        columns_to_keep = [
            'Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 
            'Detector', 'Date', 'Name', 'Alert'
        ]
        
        # Add ApproachDesc if it exists
        if 'ApproachDesc' in alerts.columns:
            columns_to_keep.append('ApproachDesc')
        
        # Keep only existing columns
        existing_columns = [col for col in columns_to_keep if col in alerts.columns]
        alerts = alerts[existing_columns].copy()
        
        # Convert categorical columns
        categorical_columns = ['Zone_Group', 'Zone', 'Corridor', 'SignalID', 
                             'CallPhase', 'Detector', 'Alert']
        for col in categorical_columns:
            if col in alerts.columns:
                alerts[col] = alerts[col].astype('category')
        
        # Convert Name to string
        if 'Name' in alerts.columns:
            alerts['Name'] = alerts['Name'].astype(str)
        
        # Filter dates (last 180 days)
        # cutoff_date = date.today() - timedelta(days=180)
        # alerts = alerts[alerts['Date'] > cutoff_date]
        cutoff_date = pd.Timestamp(date.today() - timedelta(days=180))
        alerts = alerts[alerts['Date'] > cutoff_date]
        
        # Remove duplicates
        alerts = alerts.drop_duplicates()
        
        # Sort data
        sort_columns = ['Alert', 'SignalID', 'CallPhase', 'Detector', 'Date']
        existing_sort_columns = [col for col in sort_columns if col in alerts.columns]
        alerts = alerts.sort_values(existing_sort_columns)
        
        # Calculate streaks
        alerts = alerts.reset_index(drop=True)
        
        # Group by specified columns for streak calculation
        group_columns = ['Zone_Group', 'Zone', 'SignalID', 'CallPhase', 'Detector', 'Alert']
        existing_group_columns = [col for col in group_columns if col in alerts.columns]
        
        def calculate_streaks(group):
            group = group.sort_values('Date')
            
            # Calculate date differences
            date_diffs = group['Date'].diff().dt.days
            
            # Mark start of new streaks
            start_streak = group['Date'].copy()
            for i in range(1, len(group)):
                if date_diffs.iloc[i] <= 1:  # If difference is 1 day or less
                    start_streak.iloc[i] = np.nan
            
            # Forward fill start_streak
            # start_streak = start_streak.fillna(method='ffill')
            start_streak = start_streak.ffill()

            
            # Calculate streak runs
            streak = streak_run(start_streak, k=90)
            group['streak'] = streak
            
            return group
        
        if existing_group_columns:
            # alerts = alerts.groupby(existing_group_columns, observed=True).apply(calculate_streaks)
            alerts = alerts.groupby(existing_group_columns, observed=True, group_keys=False).apply(calculate_streaks)
            alerts = alerts.reset_index(drop=True)
        
        logger.info(f"Processed {len(alerts)} alert records")
        return alerts
        
    except Exception as e:
        logger.error(f"Error in get_alerts: {e}")
        logger.error(traceback.format_exc())
        return pd.DataFrame()

def main():
    """Main function to process alerts"""
    logger = setup_logging()
    
    try:
        # Read configuration
        config_path = "Monthly_Report.yaml"
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            return
        
        with open(config_path, 'r') as file:
            conf = yaml.safe_load(file)
        
        # Get alerts data
        logger.info("Starting alerts processing...")
        alerts = get_alerts(conf)
        
        if alerts.empty:
            logger.warning("No alerts data to process")
            return
        
        # Upload to S3 Bucket
        try:
            s3_client = boto3.client("s3")

            # ---- Pickle (qs replacement) ----
            pickle_buffer = BytesIO()
            alerts.to_pickle(pickle_buffer)
            pickle_buffer.seek(0)

            s3_client.put_object(
                Bucket=conf['bucket'],
                Key="sigops/watchdog/alerts.qs",
                Body=pickle_buffer.getvalue()
            )

            # ---- Parquet ----
            parquet_buffer = BytesIO()
            alerts.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)

            s3_client.put_object(
                Bucket=conf['bucket'],
                Key="sigops/watchdog/alerts.parquet",
                Body=parquet_buffer.getvalue()
            )
            
            logger.info(f"Successfully uploaded alerts to {conf['bucket']}/sigops/watchdog/")
            
        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            logger.error(traceback.format_exc())
        
        # Write to Angular Database
        try:
            conn = keep_trying(get_aurora_connection, n_tries=5)
            
            # Truncate table
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE WatchdogAlerts")
            
            # Load data
            load_bulk_data(conn, "WatchdogAlerts", alerts)
            
            conn.close()
            
            logger.info("Successfully wrote to Angular Aurora Database: WatchdogAlerts table")
            
        except Exception as e:
            logger.error(f"Failed to write to Angular Database: {e}")
            logger.error(traceback.format_exc())
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()