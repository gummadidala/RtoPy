# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 16:27:29 2017

@author: Alan.Toppen
"""
import sys
from datetime import datetime, timedelta
import pytz
from multiprocessing import get_context
import pandas as pd
import sqlalchemy as sq
import time
import os
import itertools
import boto3
import yaml
import io
import re
import psutil
from typing import List, Dict, Any

from spm_events import etl_main
import duckdb
from parquet_lib import (
    read_parquet_file_duckdb, 
    read_atspm_data_duckdb, 
    batch_read_atspm_duckdb,
    read_parquet_file
)
from config import get_date_from_string

from mark1_logger import mark1_logger

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'

s3 = boto3.client('s3')
ath = boto3.client('athena')


def now(tz):
    return datetime.now().astimezone(pytz.timezone(tz))


base_path = "."

logs_path = os.path.join(base_path, "logs")
if not os.path.exists(logs_path):
    os.mkdir(logs_path)

logfilename = os.path.join(logs_path, f'etl_{now("US/Eastern").strftime("%F")}.log')

def read_parquet_file_safe(bucket, key):
    """
    Safe parquet file reading with proper error handling and column detection
    """
    try:
        # First try to read with DuckDB to inspect the schema
        conn = duckdb.connect()
        
        # Configure DuckDB for S3
        try:
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
            conn.execute("SET s3_region='us-east-1';")
        except Exception as e:
            print(f"DuckDB S3 setup warning: {e}")
        
        s3_path = f's3://{bucket}/{key}'
        
        # Get the schema first
        try:
            schema_query = f"SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
            result = conn.execute(schema_query)
            available_columns = [desc[0] for desc in result.description]
            print(f"Available columns in {key}: {available_columns}")
        except Exception as e:
            print(f"Could not detect schema for {key}: {e}")
            conn.close()
            return pd.DataFrame()
        
        # Build query based on available columns
        column_mappings = {
            'SignalID': 'SignalID',
            'TimeStamp': 'TimeStamp',
            'Timestamp': 'TimeStamp',  # Alternative spelling
            'timestamp': 'TimeStamp',  # Lowercase
            'EventCode': 'EventCode',
            'EventParam': 'EventParam'
        }
        
        select_columns = []
        for col in available_columns:
            if col in column_mappings:
                target_col = column_mappings[col]
                if col != target_col:
                    select_columns.append(f"{col} as {target_col}")
                else:
                    select_columns.append(col)
            else:
                select_columns.append(col)
        
        if not select_columns:
            print(f"No recognizable columns found in {key}")
            conn.close()
            return pd.DataFrame()
        
        # Read with proper column selection
        query = f"""
        SELECT {', '.join(select_columns)}
        FROM read_parquet('{s3_path}')
        """
        
        df = conn.execute(query).df()
        conn.close()
        
        # Ensure required columns exist
        required_columns = ['SignalID', 'TimeStamp', 'EventCode', 'EventParam']
        for col in required_columns:
            if col not in df.columns:
                if col == 'TimeStamp':
                    # Try alternative timestamp column names
                    alt_names = ['Timestamp', 'timestamp', 'time', 'Time']
                    found = False
                    for alt_name in alt_names:
                        if alt_name in df.columns:
                            df = df.rename(columns={alt_name: 'TimeStamp'})
                            found = True
                            break
                    if not found:
                        print(f"Warning: No timestamp column found in {key}")
                        df['TimeStamp'] = pd.Timestamp.now()
                elif col == 'SignalID':
                    # Extract SignalID from filename if not in data
                    signal_id = extract_signal_id_from_key(key)
                    df['SignalID'] = signal_id
                else:
                    # Provide default values for missing columns
                    if col == 'EventCode':
                        df['EventCode'] = 81  # Default vehicle detection
                    elif col == 'EventParam':
                        df['EventParam'] = 0
        
        # Ensure proper data types
        try:
            if 'SignalID' in df.columns:
                df['SignalID'] = pd.to_numeric(df['SignalID'], errors='coerce')
            if 'TimeStamp' in df.columns:
                df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
            if 'EventCode' in df.columns:
                df['EventCode'] = pd.to_numeric(df['EventCode'], errors='coerce')
            if 'EventParam' in df.columns:
                df['EventParam'] = pd.to_numeric(df['EventParam'], errors='coerce')
        except Exception as e:
            print(f"Warning: Data type conversion failed for {key}: {e}")
        
        print(f"Successfully read {len(df)} rows from {key}")
        return df
        
    except Exception as e:
        print(f"Error reading parquet file {key}: {e}")
        return pd.DataFrame()

def extract_signal_id_from_key(key):
    """Extract signal ID from S3 key"""
    try:
        # Pattern like: atspm/date=2025-10-08/atspm_1001_2025-10-08.parquet
        match = re.search(r'atspm_(\d+)_', key)
        if match:
            return int(match.group(1))
        
        # Try other patterns
        match = re.search(r'(\d+)', key)
        if match:
            return int(match.group(1))
        
        return 0  # Default
    except:
        return 0

def etl2_fixed_notinscope(s, date_, det_config, conf):
    """
    Fixed ETL function with better error handling and column detection
    """
    date_str = date_.strftime('%Y-%m-%d')
    
    det_config_good = det_config[det_config.SignalID==s]

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)

    t0 = time.time()

    try:
        bucket = conf['bucket']
        key = f'atspm/date={date_str}/atspm_{s}_{date_str}.parquet'
        
        # Use the safe parquet reader
        df = read_parquet_file_safe(bucket, key)

        if len(df)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No event data for this signal\n')
            return

        if len(det_config_good)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No detector configuration data for this signal\n')
            return

        if len(df) > 0 and len(det_config_good) > 0:
            
            # Validate required columns before processing
            required_cols = ['SignalID', 'TimeStamp', 'EventCode', 'EventParam']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | Missing columns: {missing_cols}\n')
                return
            
            # Filter out rows with null values in critical columns
            df_clean = df.dropna(subset=['TimeStamp', 'EventCode'])
            
            if len(df_clean) == 0:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No valid data after cleaning\n')
                return

            c, d = etl_main(df_clean, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet(f's3://{bucket}/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

                # Because DetTimeStamp gets adjusted by time from stop bar, it can go into tomorrow.
                # Limit detector data to today to make downstream easier to interpret.
                # May lose 1-2 vehs at midnight.
                d = d[d.DetTimeStamp < date_ + timedelta(days=1)]
                d.to_parquet(f's3://{bucket}/detections/date={date_str}/de_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)
                
                t1 = time.time()
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | {t1-t0:.1f} seconds | {len(c)} cycles, {len(d)} detections\n')

            else:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No cycles generated\n')

    except Exception as e:
        with open(logfilename, 'a') as f:
            f.write(f'{date_str} | {s} | ERROR: {str(e)}\n')
        print(f"Error processing signal {s} on {date_str}: {e}")

def etl2_duckdb_fixed(s, date_, det_config, conf, use_batch=False):
    """
    Enhanced ETL function using DuckDB for faster parquet reading with column detection
    """
    date_str = date_.strftime('%Y-%m-%d')

    det_config_good = det_config[det_config.SignalID==s]

    t0 = time.time()

    try:
        bucket = conf['bucket']
        
        # Use the safe DuckDB reader with column detection
        df = read_atspm_data_duckdb_safe(bucket, s, date_str)

        if len(df)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No event data for this signal\n')
            return

        if len(det_config_good)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No detector configuration data for this signal\n')
            return

        if len(df) > 0 and len(det_config_good) > 0:

            c, d = etl_main(df, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet(f's3://{bucket}/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

                # Because DetTimeStamp gets adjusted by time from stop bar, it can go into tomorrow.
                # Limit detector data to today to make downstream easier to interpret.
                # May lose 1-2 vehs at midnight.
                d = d[d.DetTimeStamp < date_ + timedelta(days=1)]
                d.to_parquet(f's3://{bucket}/detections/date={date_str}/de_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)
                t1 = time.time()
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | {t1-t0:.1f} seconds (DuckDB) | {len(c)} cycles, {len(d)} detections\n')

            else:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No cycles generated\n')

    except Exception as e:
        with open(logfilename, 'a') as f:
            f.write(f'{date_str} | {s} | ERROR: {str(e)}\n')

def read_atspm_data_duckdb_safe(bucket: str, signal_id: int, date_str: str):
    """
    Safe DuckDB-based ATSPM data reading with column detection
    """
    conn = None
    try:
        conn = duckdb.connect()
        
        # Setup DuckDB for S3
        try:
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
            conn.execute("SET s3_region='us-east-1';")
        except Exception as e:
            print(f"DuckDB setup warning: {e}")
        
        # Build S3 path
        s3_path = f's3://{bucket}/atspm/date={date_str}/atspm_{signal_id}_{date_str}.parquet'
        
        # Check if file exists and get schema
        try:
            schema_query = f"SELECT * FROM read_parquet('{s3_path}') LIMIT 0"
            result = conn.execute(schema_query)
            available_columns = [desc[0] for desc in result.description]
        except Exception as e:
            print(f"File not found or schema detection failed: {s3_path}")
            return pd.DataFrame()
        
        # Map columns to standard names
        column_mappings = {
            'SignalID': 'SignalID',
            'TimeStamp': 'TimeStamp', 
            'Timestamp': 'TimeStamp',
            'timestamp': 'TimeStamp',
            'EventCode': 'EventCode',
            'EventParam': 'EventParam'
        }
        
        select_parts = []
        for col in available_columns:
            if col in column_mappings:
                target_name = column_mappings[col]
                if col != target_name:
                    select_parts.append(f'"{col}" as {target_name}')
                else:
                    select_parts.append(f'"{col}"')
        
        # Add missing columns with defaults
        if 'SignalID' not in [column_mappings.get(col, col) for col in available_columns]:
            select_parts.append(f'{signal_id} as SignalID')
        
        if not any('TimeStamp' in part for part in select_parts):
            select_parts.append(f"'{date_str} 00:00:00'::timestamp as TimeStamp")
        
        if not any('EventCode' in part for part in select_parts):
            select_parts.append("81 as EventCode")
        
        if not any('EventParam' in part for part in select_parts):
            select_parts.append("0 as EventParam")
        
        if not select_parts:
            print(f"No usable columns found in {s3_path}")
            return pd.DataFrame()
        
        # Build and execute query
        query = f"""
        SELECT {', '.join(select_parts)}
        FROM read_parquet('{s3_path}')
        WHERE TRUE
        """
        
        df = conn.execute(query).df()
        
        # Ensure proper data types
        if 'SignalID' in df.columns:
            df['SignalID'] = pd.to_numeric(df['SignalID'], errors='coerce')
        if 'TimeStamp' in df.columns:
            df['TimeStamp'] = pd.to_datetime(df['TimeStamp'], errors='coerce')
        if 'EventCode' in df.columns:
            df['EventCode'] = pd.to_numeric(df['EventCode'], errors='coerce')
        if 'EventParam' in df.columns:
            df['EventParam'] = pd.to_numeric(df['EventParam'], errors='coerce')
        
        # Remove rows with invalid data
        df = df.dropna(subset=['TimeStamp'])
        
        print(f"Successfully read {len(df)} rows for signal {signal_id} on {date_str}")
        return df
        
    except Exception as e:
        print(f"Error reading ATSPM data for signal {signal_id}: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

def check_detector_config_file_notinscope(bucket, date_str):
    """
    Check if detector config file exists and is readable
    """
    try:
        config_key = f'config/atspm_det_config_good/date={date_str}/ATSPM_Det_Config_Good.feather'
        
        # Try to read with boto3 first
        try:
            with io.BytesIO() as data:
                s3.download_fileobj(
                    Bucket=bucket,
                    Key=config_key,
                    Fileobj=data)
                
                det_config_raw = pd.read_feather(data)\
                    .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                    .assign(Detector = lambda x: x.Detector.astype('int64'))
                
                if 'CallPhase' in det_config_raw.columns:
                    det_config_raw = det_config_raw.rename(columns={'CallPhase': 'Call Phase'})
                
                print(f"Successfully read detector config for {date_str}: {len(det_config_raw)} records")
                return det_config_raw
                
        except Exception as e:
            print(f"Error reading detector config file: {e}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error checking detector config: {e}")
        return pd.DataFrame()

def get_bad_detectors_safe(bucket, date_str):
    """
    Safely get bad detectors data
    """
    try:
        bad_detectors_path = f's3://{bucket}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet'
        
        bad_detectors = pd.read_parquet(bad_detectors_path)\
                .assign(SignalID = lambda x: x.SignalID.astype('int64'))\
                .assign(Detector = lambda x: x.Detector.astype('int64'))
        
        print(f"Successfully read bad detectors for {date_str}: {len(bad_detectors)} records")
        return bad_detectors
        
    except FileNotFoundError:
        print(f"No bad detectors file found for {date_str}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading bad detectors: {e}")
        return pd.DataFrame()

def process_detector_config_notiscope(det_config_raw, bad_detectors):
    """
    Process detector configuration with bad detectors filtering
    """
    try:
        if len(bad_detectors) == 0:
            print("No bad detectors data, using all detectors")
            det_config = det_config_raw.copy()
        else:
            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])

            det_config = (left.join(right, how='left')
                .fillna(value={'Good_Day': 1})
                .query('Good_Day == 1')
                .reset_index())
        
        # Process count detectors if needed
        if 'Call Phase' in det_config.columns and 'CountPriority' in det_config.columns:
            det_config = (det_config.set_index('Call Phase', append=True)
                .assign(
                    minCountPriority = lambda x: x.CountPriority.groupby(level=['SignalID', 'Call Phase']).min()))
            det_config['CountDetector'] = det_config['CountPriority'] == det_config['minCountPriority']
            det_config = det_config.drop(columns=['minCountPriority']).reset_index()
        
        print(f"Processed detector config: {len(det_config)} good detectors")
        return det_config
        
    except Exception as e:
        print(f"Error processing detector config: {e}")
        return det_config_raw

def process_detector_config(det_config_raw, bad_detectors):
    """
    Process detector configuration with bad detectors filtering - FIXED
    """
    try:
        if len(bad_detectors) == 0:
            print("No bad detectors data, using all detectors")
            det_config = det_config_raw.copy()
        else:
            left = det_config_raw.set_index(['SignalID', 'Detector'])
            right = bad_detectors.set_index(['SignalID', 'Detector'])

            det_config = (left.join(right, how='left')
                .fillna(value={'Good_Day': 1})
                .query('Good_Day == 1')
                .reset_index())
        
        # Ensure required columns exist
        required_columns = ['SignalID', 'Detector', 'Call Phase']
        
        for col in required_columns:
            if col not in det_config.columns:
                if col == 'Call Phase':
                    det_config['Call Phase'] = 1  # Default phase
                elif col == 'Detector':
                    det_config['Detector'] = 1  # Default detector
                print(f"Added missing column: {col}")
        
        # Add CountDetector column if missing
        if 'CountDetector' not in det_config.columns:
            print("Adding missing CountDetector column")
            
            # Check if CountPriority exists
            if 'CountPriority' in det_config.columns and 'Call Phase' in det_config.columns:
                # Process count detectors properly
                det_config_indexed = det_config.set_index(['SignalID', 'Call Phase'])
                
                # Calculate minimum count priority for each signal/phase combination
                min_count_priority = det_config_indexed.groupby(['SignalID', 'Call Phase'])['CountPriority'].min()
                min_count_priority.name = 'minCountPriority'
                
                # Join back to get the minimum priority for each row
                det_config_with_min = det_config_indexed.join(min_count_priority, on=['SignalID', 'Call Phase'])
                
                # Mark detectors with minimum priority as count detectors
                det_config_with_min['CountDetector'] = (
                    det_config_with_min['CountPriority'] == det_config_with_min['minCountPriority']
                )
                
                # Reset index and clean up
                det_config = det_config_with_min.drop(columns=['minCountPriority']).reset_index()
                
            else:
                # If CountPriority doesn't exist, mark all as count detectors
                det_config['CountDetector'] = True
                print("No CountPriority found, marking all detectors as count detectors")
        
        # Add other commonly expected columns if missing
        expected_columns = {
            'PrimaryName': 'Unknown',
            'SecondaryName': 'Unknown', 
            'IP': '0.0.0.0',
            'CountPriority': 1
        }
        
        for col, default_value in expected_columns.items():
            if col not in det_config.columns:
                det_config[col] = default_value
                print(f"Added missing column {col} with default value: {default_value}")
        
        print(f"Processed detector config: {len(det_config)} good detectors")
        print(f"Columns in detector config: {list(det_config.columns)}")
        
        # Validate the final DataFrame
        if 'CountDetector' in det_config.columns:
            count_detectors = det_config['CountDetector'].sum()
            print(f"Found {count_detectors} count detectors out of {len(det_config)} total detectors")
        
        return det_config
        
    except Exception as e:
        print(f"Error processing detector config: {e}")
        import traceback
        traceback.print_exc()
        
        # Return original config with required columns added
        det_config_fallback = det_config_raw.copy()
        
        # Add minimum required columns
        if 'CountDetector' not in det_config_fallback.columns:
            det_config_fallback['CountDetector'] = True
        if 'Call Phase' not in det_config_fallback.columns:
            det_config_fallback['Call Phase'] = 1
            
        return det_config_fallback

def check_detector_config_file(bucket, date_str):
    """
    Check if detector config file exists and is readable - ENHANCED
    """
    try:
        config_key = f'config/atspm_det_config_good/date={date_str}/ATSPM_Det_Config_Good.feather'
        
        # Try to read with boto3 first
        try:
            with io.BytesIO() as data:
                s3.download_fileobj(
                    Bucket=bucket,
                    Key=config_key,
                    Fileobj=data)
                
                det_config_raw = pd.read_feather(data)
                
                # Ensure required data types
                if 'SignalID' in det_config_raw.columns:
                    det_config_raw['SignalID'] = det_config_raw['SignalID'].astype('int64')
                if 'Detector' in det_config_raw.columns:
                    det_config_raw['Detector'] = det_config_raw['Detector'].astype('int64')
                
                # Handle column name variations
                column_mappings = {
                    'CallPhase': 'Call Phase',
                    'call_phase': 'Call Phase',
                    'Phase': 'Call Phase'
                }
                
                for old_name, new_name in column_mappings.items():
                    if old_name in det_config_raw.columns and new_name not in det_config_raw.columns:
                        det_config_raw = det_config_raw.rename(columns={old_name: new_name})
                        print(f"Renamed column {old_name} to {new_name}")
                
                print(f"Successfully read detector config for {date_str}: {len(det_config_raw)} records")
                print(f"Available columns: {list(det_config_raw.columns)}")
                
                return det_config_raw
                
        except Exception as e:
            print(f"Error reading detector config file: {e}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error checking detector config: {e}")
        return pd.DataFrame()

def validate_detector_config_for_etl(det_config):
    """
    Validate detector configuration has all required columns for ETL processing
    """
    try:
        required_columns = ['SignalID', 'Detector', 'Call Phase', 'CountDetector']
        missing_columns = [col for col in required_columns if col not in det_config.columns]
        
        if missing_columns:
            print(f"Missing required columns in detector config: {missing_columns}")
            
            # Add missing columns with defaults
            for col in missing_columns:
                if col == 'Call Phase':
                    det_config[col] = 1
                elif col == 'CountDetector':
                    det_config[col] = True
                elif col == 'Detector':
                    det_config[col] = range(1, len(det_config) + 1)
                    
            print("Added missing columns with default values")
        
        # Validate data types
        try:
            det_config['SignalID'] = det_config['SignalID'].astype('int64')
            det_config['Detector'] = det_config['Detector'].astype('int64') 
            det_config['Call Phase'] = det_config['Call Phase'].astype('int64')
            det_config['CountDetector'] = det_config['CountDetector'].astype('bool')
        except Exception as dtype_error:
            print(f"Warning: Data type conversion failed: {dtype_error}")
        
        print(f"Validated detector config: {len(det_config)} records with columns: {list(det_config.columns)}")
        return det_config
        
    except Exception as e:
        print(f"Error validating detector config: {e}")
        return det_config

def etl2_fixed(s, date_, det_config, conf):
    """
    Fixed ETL function with better error handling and column validation
    """
    date_str = date_.strftime('%Y-%m-%d')
    
    det_config_good = det_config[det_config.SignalID==s].copy()

    start_date = date_
    end_date = date_ + pd.DateOffset(days=1) - pd.DateOffset(seconds=0.1)

    t0 = time.time()

    try:
        bucket = conf['bucket']
        key = f'atspm/date={date_str}/atspm_{s}_{date_str}.parquet'
        
        # Use the safe parquet reader
        df = read_parquet_file_safe(bucket, key)

        if len(df)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No event data for this signal\n')
            return

        if len(det_config_good)==0:
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | {s} | No detector configuration data for this signal\n')
            return

        if len(df) > 0 and len(det_config_good) > 0:
            
            # Validate required columns before processing
            required_cols = ['SignalID', 'TimeStamp', 'EventCode', 'EventParam']
            missing_cols = [col for col in required_cols if col not in df.columns]
            
            if missing_cols:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | Missing columns: {missing_cols}\n')
                return
            
            # Validate detector config has required columns
            det_config_good = validate_detector_config_for_etl(det_config_good)
            
            # Check if det_config_good has CountDetector column after validation
            if 'CountDetector' not in det_config_good.columns:
                print(f"ERROR: CountDetector column still missing for signal {s}")
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | ERROR: CountDetector column missing in detector config\n')
                return
            
            # Filter out rows with null values in critical columns
            df_clean = df.dropna(subset=['TimeStamp', 'EventCode'])
            
            if len(df_clean) == 0:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No valid data after cleaning\n')
                return

            # Debug: Print detector config info
            print(f"Processing signal {s}: detector config shape {det_config_good.shape}, columns: {list(det_config_good.columns)}")

            c, d = etl_main(df_clean, det_config_good)

            if len(c) > 0 and len(d) > 0:

                c.to_parquet(f's3://{bucket}/cycles/date={date_str}/cd_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)

                # Because DetTimeStamp gets adjusted by time from stop bar, it can go into tomorrow.
                # Limit detector data to today to make downstream easier to interpret.
                # May lose 1-2 vehs at midnight.
                d = d[d.DetTimeStamp < date_ + timedelta(days=1)]
                d.to_parquet(f's3://{bucket}/detections/date={date_str}/de_{s}_{date_str}.parquet',
                             allow_truncated_timestamps=True)
                
                t1 = time.time()
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | {t1-t0:.1f} seconds | {len(c)} cycles, {len(d)} detections\n')

            else:
                with open(logfilename, 'a') as f:
                    f.write(f'{date_str} | {s} | No cycles generated\n')

    except Exception as e:
        with open(logfilename, 'a') as f:
            f.write(f'{date_str} | {s} | ERROR: {str(e)}\n')
        print(f"Error processing signal {s} on {date_str}: {e}")
        import traceback
        traceback.print_exc()

def etl_batch_duckdb_fixed(signal_list: List[int], 
                          date_range: List[str], 
                          det_config: pd.DataFrame, 
                          conf: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fixed batch ETL processing using DuckDB for multiple signals and dates
    """
    try:
        bucket = conf['bucket']
        
        # Read all data at once using DuckDB with safe column handling
        all_data = batch_read_atspm_duckdb_safe(
            bucket=bucket,
            signal_ids=signal_list,
            date_range=date_range
        )
        
        if all_data.empty:
            return {'status': 'no_data', 'processed': 0}
        
        results = {'processed': 0, 'errors': [], 'success': []}
        
        # Process each signal-date combination
        for signal_id in signal_list:
            for date_str in date_range:
                try:
                    # Filter data for this signal and date
                    signal_data = all_data[
                        (all_data['SignalID'] == signal_id) & 
                        (all_data['Date'] == date_str)
                    ]
                    
                    if signal_data.empty:
                        continue
                    
                    # Get detector config for this signal
                    det_config_signal = det_config[det_config.SignalID == signal_id]
                    
                    if det_config_signal.empty:
                        continue
                    
                    # Process with existing ETL logic
                    c, d = etl_main(signal_data, det_config_signal)
                    
                    if len(c) > 0 and len(d) > 0:
                        # Save results
                        c.to_parquet(f's3://{bucket}/cycles/date={date_str}/cd_{signal_id}_{date_str}.parquet',
                                   allow_truncated_timestamps=True)
                        d.to_parquet(f's3://{bucket}/detections/date={date_str}/de_{signal_id}_{date_str}.parquet',
                                   allow_truncated_timestamps=True)
                    
                    results['processed'] += 1
                    results['success'].append((signal_id, date_str))
                    
                except Exception as e:
                    error_msg = f"Error processing signal {signal_id} on {date_str}: {e}"
                    results['errors'].append(error_msg)
                    print(error_msg)
        
        return results
        
    except Exception as e:
        return {'status': 'error', 'message': str(e)}

def batch_read_atspm_duckdb_safe(bucket: str, signal_ids: List[int], date_range: List[str]) -> pd.DataFrame:
    """
    Safe batch reading of ATSPM data using DuckDB
    """
    conn = None
    try:
        conn = duckdb.connect()
        
        # Setup DuckDB
        try:
            conn.execute("INSTALL httpfs;")
            conn.execute("LOAD httpfs;")
            conn.execute("SET s3_region='us-east-1';")
            conn.execute("SET memory_limit='2GB';")
            conn.execute("SET threads=4;")
        except Exception as e:
            print(f"DuckDB setup warning: {e}")
        
        all_data = []
        
        # Process each date and signal combination
        for date_str in date_range:
            for signal_id in signal_ids[:10]:  # Limit for safety
                try:
                    s3_path = f's3://{bucket}/atspm/date={date_str}/atspm_{signal_id}_{date_str}.parquet'
                    
                    # Try to read this specific file
                    try:
                        # First check if file exists by trying a simple query
                        test_query = f"SELECT COUNT(*) FROM read_parquet('{s3_path}')"
                        conn.execute(test_query)
                        
                        # If successful, read the data
                        query = f"""
                        SELECT 
                            COALESCE(SignalID, {signal_id}) as SignalID,
                            COALESCE(TimeStamp, Timestamp, timestamp, '{date_str} 00:00:00'::timestamp) as TimeStamp,
                            COALESCE(EventCode, 81) as EventCode,
                            COALESCE(EventParam, 0) as EventParam,
                            '{date_str}' as Date
                        FROM read_parquet('{s3_path}')
                        LIMIT 10000
                        """
                        
                        df = conn.execute(query).df()
                        if len(df) > 0:
                            all_data.append(df)
                            print(f"Read {len(df)} records for signal {signal_id} on {date_str}")
                            
                    except Exception as file_error:
                        print(f"Skipping {s3_path}: {file_error}")
                        continue
                        
                except Exception as e:
                    print(f"Error processing signal {signal_id} on {date_str}: {e}")
                    continue
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            print(f"Total records read: {len(result)}")
            return result
        else:
            print("No data found")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error in batch read: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# Replace your existing etl2 function call with option to use fixed version
def etl2_enhanced_fixed(s, date_, det_config, conf, use_duckdb=True):
    """
    Enhanced ETL with option to use DuckDB or original method - FIXED VERSION
    """
    if use_duckdb:
        return etl2_duckdb_fixed(s, date_, det_config, conf)
    else:
        return etl2_fixed(s, date_, det_config, conf)  # Fixed original function

def main(start_date, end_date):
    """
    Main function with enhanced error handling
    """
    try:
        with open('Monthly_Report.yaml') as yaml_file:
            conf = yaml.load(yaml_file, Loader=yaml.Loader)
    except Exception as e:
        print(f"Error loading config: {e}")
        # Use default config
        conf = {
            'bucket': 'gdot-spm',
            'athena': {
                'database': 'gdot_spm',
                'staging_dir': 's3://gdot-spm-athena/'
            }
        }

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of start/end dates
    #start_date = '2019-06-04'
    #end_date = '2019-06-04'
    #-----------------------------------------------------------------------------------------

    dates = pd.date_range(start_date, end_date, freq='1D')

    bucket = conf['bucket']
    athena = conf['athena']

    #-----------------------------------------------------------------------------------------
    # Placeholder for manual override of signalids
    #signalids = [7053]
    #-----------------------------------------------------------------------------------------

    t0 = time.time()

    for date_ in dates:

        date_str = date_.strftime('%Y-%m-%d')
        print(f"\nProcessing date: {date_str}")

        # Check if detector config exists
        det_config_raw = check_detector_config_file(bucket, date_str)
        
        if det_config_raw.empty:
            print(f'No detector configuration found for {date_str}. Skipping this day.')
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | No detector configuration file found\n')
            continue

        signalids = det_config_raw['SignalID'].drop_duplicates().astype('int64').values
        print(f"Found {len(signalids)} signals to process")

        # Get bad detectors
        bad_detectors = get_bad_detectors_safe(bucket, date_str)
        
        # Process detector config
        det_config = process_detector_config(det_config_raw, bad_detectors)

        if len(det_config) > 0:
            print(f"Processing {len(signalids)} signals with {len(det_config)} good detectors")
            
            # Calculate number of threads based on available memory
            nthreads = min(round(psutil.virtual_memory().available/1e9), len(signalids))
            nthreads = max(1, nthreads)  # At least 1 thread
            print(f"Using {nthreads} threads")

            #-----------------------------------------------------------------------------------------
            try:
                with get_context('spawn').Pool(processes=nthreads) as pool:
                    result = pool.starmap_async(
                        etl2_enhanced_fixed, 
                        list(itertools.product(signalids, [date_], [det_config], [conf])), 
                        chunksize=max(1, (len(signalids) // nthreads))
                    )
                    pool.close()
                    pool.join()
                    
                print(f"Completed processing for {date_str}")
                    
            except Exception as e:
                print(f"Error in multiprocessing for {date_str}: {e}")
                # Fallback to sequential processing
                print("Falling back to sequential processing...")
                for signal_id in signalids:
                    try:
                        etl2_enhanced_fixed(signal_id, date_, det_config, conf)
                    except Exception as signal_error:
                        print(f"Error processing signal {signal_id}: {signal_error}")
                        with open(logfilename, 'a') as f:
                            f.write(f'{date_str} | {signal_id} | ERROR: {str(signal_error)}\n')
                        continue
            #-----------------------------------------------------------------------------------------
        else:
            print(f'No good detectors found for {date_str}. Skip this day.')
            with open(logfilename, 'a') as f:
                f.write(f'{date_str} | No good detectors found\n')

    total_time = int((time.time()-t0)/60)
    print(f'\nCompleted processing {len(signalids)} signals in {len(dates)} days. Done in {total_time} minutes')
    
    with open(logfilename, 'a') as f:
        f.write(f'SUMMARY | {len(signalids)} signals | {len(dates)} days | {total_time} minutes\n')

    # Add partitions to Athena tables
    try:
        add_athena_partitions(dates, athena)
    except Exception as e:
        print(f"Error adding Athena partitions: {e}")
        with open(logfilename, 'a') as f:
            f.write(f'ATHENA_ERROR | {str(e)}\n')

def add_athena_partitions(dates, athena):
    """
    Add partitions to Athena tables with error handling
    """
    try:
        # Add a partition for each day. If more than ten days, update all partitions in one command.
        if len(dates) > 10:
            print("Adding all partitions at once...")
            
            try:
                response_repair_cycledata = ath.start_query_execution(
                    QueryString=f"MSCK REPAIR TABLE cycledata;",
                    QueryExecutionContext={'Database': athena['database']},
                    ResultConfiguration={'OutputLocation': athena['staging_dir']})

                response_repair_detection_events = ath.start_query_execution(
                    QueryString=f"MSCK REPAIR TABLE detectionevents",
                    QueryExecutionContext={'Database': athena['database']},
                    ResultConfiguration={'OutputLocation': athena['staging_dir']})
                    
                print("Partition repair commands submitted")
                
            except Exception as e:
                print(f"Error with bulk partition repair: {e}")
                
        else:
            print("Adding partitions individually...")
            for date_ in dates:
                date_str = date_.strftime('%Y-%m-%d')
                try:
                    response_repair_cycledata = ath.start_query_execution(
                        QueryString=f"ALTER TABLE cycledata ADD PARTITION (date = '{date_str}');",
                        QueryExecutionContext={'Database': athena['database']},
                        ResultConfiguration={'OutputLocation': athena['staging_dir']})

                    response_repair_detection_events = ath.start_query_execution(
                        QueryString=f"ALTER TABLE detectionevents ADD PARTITION (date = '{date_str}');",
                        QueryExecutionContext={'Database': athena['database']},
                        ResultConfiguration={'OutputLocation': athena['staging_dir']})
                        
                    print(f"Added partitions for {date_str}")
                    
                except Exception as e:
                    print(f"Error adding partition for {date_str}: {e}")

        # Check if the partitions for the last day were successfully added before moving on
        if len(dates) > 0:
            print("Checking partition creation status...")
            max_wait_time = 300  # 5 minutes max wait
            wait_time = 0
            
            while wait_time < max_wait_time:
                try:
                    response1 = s3.list_objects_v2(
                        Bucket=os.path.basename(athena['staging_dir']).rstrip('/'),
                        Prefix=response_repair_cycledata['QueryExecutionId'])
                    response2 = s3.list_objects_v2(
                        Bucket=os.path.basename(athena['staging_dir']).rstrip('/'),
                        Prefix=response_repair_detection_events['QueryExecutionId'])

                    if response1.get('KeyCount', 0) > 0 and response2.get('KeyCount', 0) > 0:
                        print('Partition creation completed successfully.')
                        break
                    else:
                        time.sleep(5)
                        wait_time += 5
                        print('.', end='', flush=True)
                        
                except Exception as e:
                    print(f"Error checking partition status: {e}")
                    break
            
            if wait_time >= max_wait_time:
                print(f"\nWarning: Partition creation check timed out after {max_wait_time} seconds")
                
    except Exception as e:
        print(f"Error in add_athena_partitions: {e}")
        raise

def create_summary_report(logfilename):
    """
    Create a summary report from the log file
    """
    try:
        if not os.path.exists(logfilename):
            print("Log file not found for summary")
            return
            
        with open(logfilename, 'r') as f:
            lines = f.readlines()
        
        total_signals = 0
        successful_signals = 0
        failed_signals = 0
        no_data_signals = 0
        no_config_signals = 0
        
        for line in lines:
            if '|' in line:
                parts = line.strip().split('|')
                if len(parts) >= 3:
                    if 'No event data' in line:
                        no_data_signals += 1
                    elif 'No detector configuration' in line:
                        no_config_signals += 1
                    elif 'ERROR:' in line:
                        failed_signals += 1
                    elif 'seconds' in line and 'cycles' in line:
                        successful_signals += 1
                    
        total_signals = successful_signals + failed_signals + no_data_signals + no_config_signals
        
        summary_lines = [
            "\n" + "="*60,
            "ETL PROCESSING SUMMARY",
            "="*60,
            f"Total signals processed: {total_signals}",
            f"Successful: {successful_signals}",
            f"Failed (errors): {failed_signals}",
            f"No event data: {no_data_signals}",
            f"No detector config: {no_config_signals}",
            "="*60
        ]
        
        print("\n".join(summary_lines))
        
        # Append to log file
        with open(logfilename, 'a') as f:
            f.write("\n".join(summary_lines) + "\n")
            
    except Exception as e:
        print(f"Error creating summary report: {e}")

if __name__=='__main__':
    """
    Main execution with enhanced error handling
    """
    try:
        print(f"Starting ETL Dashboard at {datetime.now()}")
        
        # Load configuration
        try:
            with open('Monthly_Report.yaml') as yaml_file:
                conf = yaml.load(yaml_file, Loader=yaml.Loader)
        except Exception as e:
            print(f"Warning: Could not load Monthly_Report.yaml: {e}")
            print("Using default configuration")
            conf = {
                'bucket': 'gdot-spm',
                'start_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                'end_date': (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            }

        # Get date range from command line or config
        if len(sys.argv) > 1:
            start_date = sys.argv[1]
            end_date = sys.argv[2] if len(sys.argv) > 2 else sys.argv[1]
        else:
            start_date = conf.get('start_date', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
            end_date = conf.get('end_date', start_date)

        # Validate dates
        try:
            start_date = get_date_from_string(
                start_date, s3bucket=conf['bucket'], s3prefix="mark/arrivals_on_green"
            )
            end_date = get_date_from_string(end_date)
        except Exception as e:
            print(f"Warning: Date validation failed: {e}")
            print("Using provided dates as-is")

        print(f"Processing ETL for date range: {start_date} to {end_date}")
        
        # Create logs directory if it doesn't exist
        if not os.path.exists(logs_path):
            os.makedirs(logs_path, exist_ok=True)
            
        # Initialize log file
        with open(logfilename, 'w') as f:
            f.write(f'ETL Processing started at {datetime.now()}\n')
            f.write(f'Date range: {start_date} to {end_date}\n')
            f.write(f'Configuration: {conf.get("bucket", "default")}\n')
            f.write('-' * 50 + '\n')

        # Run main processing
        main(start_date, end_date)
        
        # Create summary report
        create_summary_report(logfilename)
        
        print(f"ETL Dashboard completed at {datetime.now()}")
        print(f"Log file: {logfilename}")
        
    except KeyboardInterrupt:
        print("\nETL processing interrupted by user")
        with open(logfilename, 'a') as f:
            f.write(f'ETL processing interrupted by user at {datetime.now()}\n')
        sys.exit(1)
        
    except Exception as e:
        print(f"Error in ETL Dashboard: {e}")
        import traceback
        traceback.print_exc()
        
        try:
            with open(logfilename, 'a') as f:
                f.write(f'ETL processing failed at {datetime.now()}\n')
                f.write(f'Error: {str(e)}\n')
                f.write(f'Traceback: {traceback.format_exc()}\n')
        except:
            pass
            
        sys.exit(1)

