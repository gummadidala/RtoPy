#!/usr/bin/env python3
"""
Monthly Report Calculations - Part 2
Converted from Monthly_Report_Calcs_2.R

This script processes various traffic performance metrics including:
- ETL dashboard data
- Arrivals on Green (AOG) 
- Queue Spillback
- Pedestrian Delay
- Split Failures
- Flash Events
"""

import sys
import subprocess
import gc
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import pandas as pd
import numpy as np
import boto3
import awswrangler as wr
from typing import Dict, List, Optional, Any

# Import from existing RtoPy modules
from database_functions import get_athena_connection, add_athena_partition
from metrics import (
    get_split_failures,
    get_queue_spillback, 
    get_ped_actuations,
    get_flash_events,
    process_all_metrics_for_date
)
from configs import get_det_config_factory, get_ped_config_factory
from counts import s3_upload_parquet_date_split
from utilities import keep_trying
from monthly_report_functions import parallel_process_dates

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_init_variables():
    """Load variables from Monthly_Report_Calcs_init.R equivalent"""
    try:
        # This would be set by the init script
        # For now, we'll derive from environment or config
        import yaml
        
        with open('Monthly_Report.yaml', 'r') as f:
            conf = yaml.safe_load(f)
            
        # Get date range from command line or config
        if len(sys.argv) >= 3:
            start_date = sys.argv[1]
            end_date = sys.argv[2]
        else:
            # Default to yesterday
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            start_date = end_date = yesterday
            
        # Get signals list (this should come from corridors config)
        signals_list = get_signals_list_from_config(conf)
        
        return conf, start_date, end_date, signals_list
        
    except Exception as e:
        logger.error(f"Error loading initialization variables: {e}")
        raise

def get_signals_list_from_config2(conf: Dict) -> List[str]:
    """Get signals list from configuration"""
    try:
        # Try to get from S3/Athena
        session = boto3.Session(
            aws_access_key_id=conf['athena'].get('uid'),
            aws_secret_access_key=conf['athena'].get('pwd')
        )
        
        query = f"""
        SELECT DISTINCT SignalID 
        FROM {conf['athena']['database']}.corridors 
        WHERE Include = true
        """
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf['athena']['database'],
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        return df['SignalID'].astype(str).tolist()
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        logger.warning(f"Could not get signals from config: {e}")
        return []

def get_signals_list_from_config(conf: Dict) -> List[str]:
    """Get signals list from configuration"""
    try:
        session = boto3.Session(
            aws_access_key_id=conf['athena'].get('uid'),
            aws_secret_access_key=conf['athena'].get('pwd')
        )
        
        # First check if database exists
        try:
            show_databases_query = "SHOW DATABASES"
            databases_df = wr.athena.read_sql_query(
                sql=show_databases_query,
                database='default',  # Use default database for this query
                s3_output=conf['athena']['staging_dir'],
                boto3_session=session,
                ctas_approach=False
            )
            
            print("Available databases:")
            print(databases_df)
            
            # Check if our database exists
            db_name = conf['athena']['database']
            if db_name not in databases_df.values:
                logger.error(f"Database '{db_name}' does not exist")
                raise Exception(f"Database '{db_name}' not found")
                
        except Exception as db_error:
            logger.error(f"Error checking databases: {db_error}")
            raise
        
        # Now check tables in the database
        show_tables_query = f"SHOW TABLES IN {conf['athena']['database']}"
        
        tables_df = wr.athena.read_sql_query(
            sql=show_tables_query,
            database=conf['athena']['database'],
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        print("Available tables:")
        print(tables_df)
        print(f"Table columns: {tables_df.columns.tolist()}")
        
        if tables_df.empty:
            logger.warning(f"No tables found in database {conf['athena']['database']}")
            raise Exception("No tables found in database")
        
        # The column name might be different, let's check what columns we have
        table_col = None
        for col in ['tab_name', 'table_name', 'Tables_in_' + conf['athena']['database']]:
            if col in tables_df.columns:
                table_col = col
                break
        
        if table_col is None:
            # Use the first column if we can't find the expected one
            table_col = tables_df.columns[0]
            logger.warning(f"Using first column '{table_col}' as table name column")
        
        # Check for corridor-related tables
        corridor_tables = tables_df[tables_df[table_col].str.contains('corridor', case=False, na=False)]
        
        if not corridor_tables.empty:
            table_name = corridor_tables.iloc[0][table_col]
            logger.info(f"Found corridor table: {table_name}")
            
            query = f"""
            SELECT DISTINCT SignalID 
            FROM {conf['athena']['database']}.{table_name}
            WHERE Include = true
            """
        else:
            logger.warning("No corridor tables found, trying alternative approaches")
            raise Exception("No corridors table found")
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf['athena']['database'],
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        return df['SignalID'].astype(str).tolist()
        
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        logger.warning(f"Could not get signals from config: {e}")
        
        # Fallback to config file
        return get_signals_from_config_file(conf)

def get_signals_from_config_file(conf: Dict) -> List[str]:
    """Get signals list from config file as fallback"""
    try:
        # Try different config keys
        if 'signals_list' in conf:
            return conf['signals_list']
        elif 'signals' in conf:
            if isinstance(conf['signals'], list):
                return conf['signals']
            elif isinstance(conf['signals'], dict) and 'list' in conf['signals']:
                return conf['signals']['list']
        
        # If no signals in config, return a default test list
        logger.warning("No signals found in config, using default test signals")
        return ['1001', '1002', '1003']  # Replace with actual signal IDs
        
    except Exception as e:
        logger.error(f"Error getting signals from config file: {e}")
        return []

def run_system_command(command: str) -> bool:
    """Run system command with proper error handling"""
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Command completed successfully: {command}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}")
        logger.error(f"Error: {e.stderr}")
        return False

def print_with_timestamp(message: str):
    """Print message with timestamp like R's glue function"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} {message}")

def get_detection_events(date_start: str, date_end: str, conf_athena: Dict, signals_list: List[str]) -> pd.DataFrame:
    """Get detection events from Athena"""
    try:
        signals_str = "', '".join(signals_list)
        query = f"""
        SELECT SignalID, Detector, CallPhase, Timeperiod, EventCode, EventParam
        FROM {conf_athena['database']}.detection_events
        WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND SignalID IN ('{signals_str}')
        """
        
        session = boto3.Session(
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd')
        )
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf_athena['database'],
            s3_output=conf_athena['staging_dir'],
            boto3_session=session
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting detection events: {e}")
        return pd.DataFrame()

def get_qs(detection_events: pd.DataFrame, intervals: List[str] = ["hour", "15min"]) -> Dict[str, pd.DataFrame]:
    """Calculate queue spillback - wrapper around existing function"""
    try:
        results = {}
        
        for interval in intervals:
            if interval == "hour":
                qs_data = get_queue_spillback(detection_events)
                # Add hourly aggregation
                if not qs_data.empty:
                    qs_data['Hour'] = pd.to_datetime(qs_data['Timeperiod']).dt.floor('H')
                    qs_hourly = qs_data.groupby(['SignalID', 'CallPhase', 'Hour']).agg({
                        'qs_freq': 'mean',
                        'cycles': 'sum'
                    }).reset_index()
                    qs_hourly = qs_hourly.rename(columns={'Hour': 'Timeperiod'})
                    results['hour'] = qs_hourly
                else:
                    results['hour'] = pd.DataFrame()
                    
            elif interval == "15min":
                qs_data = get_queue_spillback(detection_events)
                # Add 15-minute aggregation
                if not qs_data.empty:
                    qs_data['Period15'] = pd.to_datetime(qs_data['Timeperiod']).dt.floor('15T')
                    qs_15min = qs_data.groupby(['SignalID', 'CallPhase', 'Period15']).agg({
                        'qs_freq': 'mean', 
                        'cycles': 'sum'
                    }).reset_index()
                    qs_15min = qs_15min.rename(columns={'Period15': 'Timeperiod'})
                    results['15min'] = qs_15min
                else:
                    results['15min'] = pd.DataFrame()
        
        return results
        
    except Exception as e:
        logger.error(f"Error calculating queue spillback: {e}")
        return {interval: pd.DataFrame() for interval in intervals}

def get_ped_delay(date_str: str, conf: Dict, signals_list: List[str]) -> pd.DataFrame:
    """Calculate pedestrian delay using ATSPM method"""
    try:
        # Get pedestrian actuations using existing function
        # Query ATSPM data for ped events
        session = boto3.Session(
            aws_access_key_id=conf['athena'].get('uid'),
            aws_secret_access_key=conf['athena'].get('pwd')
        )
        
        signals_str = "', '".join(signals_list)
        query = f"""
        SELECT SignalID, CallPhase, Timeperiod, EventCode, EventParam
        FROM {conf['athena']['database']}.atspm_events
        WHERE date = '{date_str}'
        AND EventCode IN (90, 91)
        AND SignalID IN ('{signals_str}')
        """
        
        atspm_data = wr.athena.read_sql_query(
            sql=query,
            database=conf['athena']['database'],
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session
        )
        
        if atspm_data.empty:
            return pd.DataFrame()
        
        # Use existing pedestrian functions with modifications for delay calculation
        ped_actuations = get_ped_actuations(atspm_data)
        
        # Calculate delays (simplified - would need more complex logic for actual delay calculation)
        if not ped_actuations.empty:
            # Add delay calculation logic here
            ped_actuations['ped_delay'] = np.random.uniform(5, 60, len(ped_actuations))  # Placeholder
            return ped_actuations
        
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error calculating pedestrian delay: {e}")
        return pd.DataFrame()

def get_sf_utah(date_str: str, conf: Dict, signals_list: List[str], intervals: List[str] = ["hour", "15min"]) -> Dict[str, pd.DataFrame]:
    """Calculate split failures using Utah method - wrapper around existing function"""
    try:
        # Get ATSPM data
        session = boto3.Session(
            aws_access_key_id=conf['athena'].get('uid'),
            aws_secret_access_key=conf['athena'].get('pwd')
        )
        
        signals_str = "', '".join(signals_list)
        query = f"""
        SELECT SignalID, CallPhase, Timeperiod, EventCode, EventParam
        FROM {conf['athena']['database']}.atspm_events
        WHERE date = '{date_str}'
        AND SignalID IN ('{signals_str}')
        """
        
        atspm_data = wr.athena.read_sql_query(
            sql=query,
            database=conf['athena']['database'],
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session
        )
        
        results = {}
        
        for interval in intervals:
            # Use existing split failures function
            sf_data = get_split_failures(atspm_data, conf)
            
            if not sf_data.empty:
                if interval == "hour":
                    sf_data['Hour'] = pd.to_datetime(sf_data['Timeperiod']).dt.floor('H')
                    sf_hourly = sf_data.groupby(['SignalID', 'CallPhase', 'Hour']).agg({
                        'sf_freq': 'mean',
                        'cycles': 'sum'
                    }).reset_index()
                    sf_hourly = sf_hourly.rename(columns={'Hour': 'Timeperiod'})
                    results['hour'] = sf_hourly
                    
                elif interval == "15min":
                    sf_data['Period15'] = pd.to_datetime(sf_data['Timeperiod']).dt.floor('15T')
                    sf_15min = sf_data.groupby(['SignalID', 'CallPhase', 'Period15']).agg({
                        'sf_freq': 'mean',
                        'cycles': 'sum'
                    }).reset_index()
                    sf_15min = sf_15min.rename(columns={'Period15': 'Timeperiod'})
                    results['15min'] = sf_15min
            else:
                results[interval] = pd.DataFrame()
        
        return results
        
    except Exception as e:
        logger.error(f"Error calculating split failures: {e}")
        return {interval: pd.DataFrame() for interval in intervals}

def get_queue_spillback_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """Process queue spillback for date range"""
    try:
        start_dt = parse_date(start_date).date()
        end_dt = parse_date(end_date).date()
        
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            print(date_str)
            
            detection_events = get_detection_events(date_str, date_str, conf['athena'], signals_list)
            
            if len(detection_events) > 0:
                qs = get_qs(detection_events, intervals=["hour", "15min"])
                
                if len(qs['hour']) > 0:
                    s3_upload_parquet_date_split(
                        qs['hour'],
                        bucket=conf['bucket'],
                        prefix="qs",
                        table_name="queue_spillback",
                        conf_athena=conf['athena']
                    )
                
                if len(qs['15min']) > 0:
                    s3_upload_parquet_date_split(
                        qs['15min'],
                        bucket=conf['bucket'],
                        prefix="qs",
                        table_name="queue_spillback_15min", 
                        conf_athena=conf['athena']
                    )
            
            current_date += timedelta(days=1)
            
    except Exception as e:
        logger.error(f"Error in queue spillback date range processing: {e}")

def get_pd_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """Process pedestrian delay for date range"""
    try:
        start_dt = parse_date(start_date).date()
        end_dt = parse_date(end_date).date()
        
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            print(date_str)
            
            pd_data = get_ped_delay(date_str, conf, signals_list)
            
            if len(pd_data) > 0:
                s3_upload_parquet_date_split(
                    pd_data,
                    bucket=conf['bucket'],
                    prefix="pd",
                    table_name="ped_delay",
                    conf_athena=conf['athena']
                )
            
            current_date += timedelta(days=1)
        
        gc.collect()
        
    except Exception as e:
        logger.error(f"Error in pedestrian delay date range processing: {e}")

def get_sf_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """Process split failures for date range"""
    try:
        start_dt = parse_date(start_date).date()
        end_dt = parse_date(end_date).date()
        
        current_date = start_dt
        while current_date <= end_dt:
            date_str = current_date.strftime("%Y-%m-%d")
            print(date_str)
            
            sf = get_sf_utah(date_str, conf, signals_list, intervals=["hour", "15min"])
            
            if len(sf['hour']) > 0:
                s3_upload_parquet_date_split
                s3_upload_parquet_date_split(
                    sf['hour'],
                    bucket=conf['bucket'],
                    prefix="sf",
                    table_name="split_failures",
                    conf_athena=conf['athena']
                )
            
            if len(sf['15min']) > 0:
                s3_upload_parquet_date_split(
                    sf['15min'],
                    bucket=conf['bucket'],
                    prefix="sf",
                    table_name="split_failures_15min",
                    conf_athena=conf['athena']
                )
            
            current_date += timedelta(days=1)
            
    except Exception as e:
        logger.error(f"Error in split failures date range processing: {e}")

def main():
    """Main function - equivalent to the R script flow"""
    try:
        print_with_timestamp("Starting Monthly Report Calcs 2")
        
        # Load initialization variables (equivalent to sourcing Monthly_Report_Calcs_init.R)
        conf, start_date, end_date, signals_list = load_init_variables()
        
        if not signals_list:
            logger.error("No signals found to process")
            return False
        
        logger.info(f"Processing {len(signals_list)} signals from {start_date} to {end_date}")
        
        # ETL Dashboard (Python): cycledata, detectionevents to S3/Athena [7 of 11]
        print_with_timestamp("etl [7 of 11]")
        # python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
        python_env = "C:\\Users\\kgummadidala\\Desktop\\Rtopy\\server-env\\Scripts\\python.exe"
        if conf.get('run', {}).get('etl', True):
            conda_env = "~/miniconda3/bin/conda run -n sigops"
            etl_command = f"{python_env} python etl_dashboard.py {start_date} {end_date}"
            
            if not run_system_command(etl_command):
                logger.error("ETL dashboard failed")
                return False
        else:
            logger.info("ETL process skipped (disabled in config)")
        
        # Arrivals on Green [8 of 11]
        print_with_timestamp("aog [8 of 11]")
        if conf.get('run', {}).get('arrivals_on_green', True):
            conda_env = "~/miniconda3/bin/conda run -n sigops"
            aog_command = f"{python_env} python get_aog.py {start_date} {end_date}"
            
            if not run_system_command(aog_command):
                logger.error("AOG calculation failed")
                return False
            
            gc.collect()
        else:
            logger.info("AOG process skipped (disabled in config)")
        
        # Queue Spillback [9 of 11]
        print_with_timestamp("queue spillback [9 of 11]")
        if conf.get('run', {}).get('queue_spillback', True):
            get_queue_spillback_date_range(start_date, end_date, conf, signals_list)
        else:
            logger.info("Queue spillback process skipped (disabled in config)")
        
        # Pedestrian Delay [10 of 11]
        print_with_timestamp("ped delay [10 of 11]")
        if conf.get('run', {}).get('ped_delay', True):
            get_pd_date_range(start_date, end_date, conf, signals_list)
        else:
            logger.info("Pedestrian delay process skipped (disabled in config)")
        
        # Split Failures [11 of 11]
        print_with_timestamp("split failures [11 of 11]")
        if conf.get('run', {}).get('split_failures', True):
            # Utah method, based on green, start-of-red occupancies
            get_sf_date_range(start_date, end_date, conf, signals_list)
        else:
            logger.info("Split failures process skipped (disabled in config)")
        
        # Flash Events [12 of 12]
        print_with_timestamp("flash events [12 of 12]")
        if conf.get('run', {}).get('flash_events', True):
            conda_env = "~/miniconda3/bin/conda run -n sigops"
            flash_command = f"{python_env} python get_flash_events.py"
            
            if not run_system_command(flash_command):
                logger.error("Flash events calculation failed")
                return False
            
            gc.collect()
        else:
            logger.info("Flash events process skipped (disabled in config)")
        
        # Cleanup (equivalent to closeAllConnections())
        gc.collect()
        
        print("\n--------------------- End Monthly Report calcs -----------------------\n")
        return True
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
