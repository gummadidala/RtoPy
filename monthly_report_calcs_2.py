#!/usr/bin/env python3
"""
Monthly Report Calculations - Part 2
Converted from Monthly_Report_Calcs_2.R
Optimized with Athena cloud processing and parallel execution for better performance
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
import yaml
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import partial
import os

# Setup logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import from the init script
from monthly_report_calcs_init import main as init_main

# Import functions with proper error handling
try:
    from database_functions import get_detection_events, get_athena_connection
except ImportError:
    logger.warning("database_functions not available")
    get_detection_events = None
    get_athena_connection = None

try:
    from metrics import get_qs, get_sf_utah, get_ped_delay
except ImportError as e1:
    logger.warning(f"metrics functions not available: {e1}, trying fallback")
    try:
        from missing_functions_fallback import get_qs, get_sf_utah, get_ped_delay
        logger.info("Loaded functions from missing_functions_fallback")
    except ImportError as e2:
        logger.error(f"missing_functions_fallback not available either: {e2}")
        get_qs = None
        get_sf_utah = None
        get_ped_delay = None

try:
    from s3_parquet_io import s3_upload_parquet_date_split
except ImportError:
    logger.warning("s3_parquet_io not available, trying fallback")
    try:
        from missing_functions_fallback import s3_upload_parquet_date_split
        logger.info("Loaded s3_upload_parquet_date_split from fallback")
    except ImportError:
        logger.error("s3_upload_parquet_date_split not available")
        s3_upload_parquet_date_split = None

try:
    from utilities import keep_trying
except ImportError:
    logger.warning("utilities.keep_trying not available, trying fallback")
    try:
        from missing_functions_fallback import keep_trying
        logger.info("Loaded keep_trying from fallback")
    except ImportError:
        logger.warning("keep_trying fallback not available, using inline version")
        # Simple fallback keep_trying
        def keep_trying(func, n_tries=3, timeout=None, **kwargs):
            for attempt in range(n_tries):
                try:
                    return func(**kwargs)
                except Exception as e:
                    if attempt == n_tries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {e}")
            return None

try:
    from monthly_report_functions import (
        generate_processing_summary,
        create_checkpoint_file,
        send_notification,
        monitor_system_resources,
        setup_logging as setup_logging_func
    )
    # Rename to avoid conflict with local setup
    setup_logging = setup_logging_func
except ImportError:
    logger.warning("monthly_report_functions not fully available")
    generate_processing_summary = None
    create_checkpoint_file = None
    send_notification = None
    monitor_system_resources = None
    setup_logging = None


def print_with_timestamp(message: str):
    """Equivalent to R's glue("{Sys.time()} message")"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} {message}")

def run_system_command(command: str) -> bool:
    """Equivalent to R's system() function - waits for completion"""
    try:
        # R's system() waits for completion by default
        result = subprocess.run(command, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}")
        return False

def get_detection_events_wrapper(date_start: str, date_end: str, conf_athena: Dict, signals_list: List[str]) -> pd.DataFrame:
    """
    Get detection events from Athena - wrapper for the imported function
    """
    try:
        # If the imported function exists and is not None, use it
        if get_detection_events is not None:
            return get_detection_events(date_start, date_end, conf_athena, signals_list)
        else:
            # Fallback implementation
            return get_detection_events_fallback(date_start, date_end, conf_athena, signals_list)
    except Exception as e:
        import traceback
        logger.error(f"Error getting detection events: {type(e).__name__}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def get_detection_events_fallback(date_start: str, date_end: str, conf_athena: Dict, signals_list: List[str]) -> pd.DataFrame:
    """Fallback implementation for getting detection events from ATSPM table"""
    try:
        # Join signal IDs as comma-separated integers (no quotes)
        signals_str = ", ".join([str(s) for s in signals_list])
        # Query detector events from ATSPM table (EventCode 81=Detector Off, 82=Detector On)
        query = f"""
        SELECT 
            SignalID,
            EventParam AS Detector,
            EventParam AS CallPhase,
            timestamp AS Timeperiod,
            EventCode,
            EventParam
        FROM {conf_athena['database']}.{conf_athena.get('atspm_table', 'atspm')}
        WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND SignalID IN ({signals_str})
        AND EventCode IN (81, 82)
        """
        
        session = boto3.Session(
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd')
        )
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf_athena['database'],
            s3_output=conf_athena['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting detection events: {e}")
        return pd.DataFrame()

def process_single_date_queue_spillback(date_str: str, conf: Dict, signals_list: List[str]) -> bool:
    """
    Process queue spillback for a single date with retry logic
    Module-level function for parallel processing
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        conf: Configuration dictionary
        signals_list: List of signal IDs
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing queue spillback for: {date_str}")
        
        # Get detection events with retry
        detection_events = keep_trying(
            get_detection_events_wrapper,
            n_tries=3,
            timeout=120,
            date_start=date_str,
            date_end=date_str,
            conf_athena=conf['athena'],
            signals_list=signals_list
        )
        
        if detection_events is None or len(detection_events) == 0:
            logger.info(f"No detection events for {date_str}")
            return False
        
        # Calculate queue spillback
        if get_qs is not None:
            qs = get_qs(detection_events, intervals=["hour", "15min"])
        else:
            logger.warning(f"get_qs function not available for {date_str}")
            return False
        
        # Upload hourly results
        if len(qs.get('hour', pd.DataFrame())) > 0:
            s3_upload_parquet_date_split(
                qs['hour'],
                bucket=conf['bucket'],
                prefix="qs",
                table_name="queue_spillback",
                conf_athena=conf['athena']
            )
        
        # Upload 15-min results
        if len(qs.get('15min', pd.DataFrame())) > 0:
            s3_upload_parquet_date_split(
                qs['15min'],
                bucket=conf['bucket'],
                prefix="qs",
                table_name="queue_spillback_15min",
                conf_athena=conf['athena']
            )
        
        logger.info(f"Completed queue spillback for: {date_str}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing queue spillback for {date_str}: {e}")
        return False


def get_queue_spillback_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str], usable_cores: int = None):
    """
    Process queue spillback for date range with parallel processing and retry logic
    Enhanced version of R's lapply with better performance
    
    Args:
        start_date: Start date string
        end_date: End date string
        conf: Configuration dictionary
        signals_list: List of signal IDs
        usable_cores: Number of cores for parallel processing
    """
    logger.info(f"Processing queue spillback from {start_date} to {end_date}")
    
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
    
    logger.info(f"Processing queue spillback for {len(date_strings)} dates")
    
    if len(date_strings) == 1:
        # Single date - process directly
        process_single_date_queue_spillback(date_strings[0], conf, signals_list)
    else:
        # Multiple dates - parallel processing (faster than R's lapply)
        if usable_cores is None:
            usable_cores = min(len(date_strings), os.cpu_count() or 4)
        
        logger.info(f"Using parallel processing with {usable_cores} workers")
        
        with ThreadPoolExecutor(max_workers=usable_cores) as executor:
            futures = {
                executor.submit(
                    process_single_date_queue_spillback,
                    date_str,
                    conf,
                    signals_list
                ): date_str
                for date_str in date_strings
            }
            
            success_count = 0
            error_count = 0
            
            for future in as_completed(futures):
                date_str = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Exception for {date_str}: {e}")
            
            logger.info(f"Queue spillback completed: {success_count} success, {error_count} errors")
    
    gc.collect()

def process_single_date_ped_delay(date_str: str, conf: Dict, signals_list: List[str]) -> bool:
    """
    Process pedestrian delay for a single date with retry logic
    Module-level function for parallel processing
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        conf: Configuration dictionary
        signals_list: List of signal IDs
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing pedestrian delay for: {date_str}")
        
        # Calculate ped delay with retry
        if get_ped_delay is not None:
            pd_data = keep_trying(
                get_ped_delay,
                n_tries=3,
                timeout=120,
                date_=date_str,
                conf=conf,
                signals_list=signals_list
            )
        else:
            logger.warning(f"get_ped_delay function not available for {date_str}")
            return False
        
        if pd_data is None or len(pd_data) == 0:
            logger.info(f"No pedestrian delay data for {date_str}")
            return False
        
        # Upload results
        s3_upload_parquet_date_split(
            pd_data,
            bucket=conf['bucket'],
            prefix="pd",
            table_name="ped_delay",
            conf_athena=conf['athena']
        )
        
        logger.info(f"Completed pedestrian delay for: {date_str}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing pedestrian delay for {date_str}: {e}")
        return False


def get_pd_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str], usable_cores: int = None):
    """
    Process pedestrian delay for date range with parallel processing and retry logic
    Enhanced version of R's lapply with better performance
    
    Args:
        start_date: Start date string
        end_date: End date string
        conf: Configuration dictionary
        signals_list: List of signal IDs
        usable_cores: Number of cores for parallel processing
    """
    logger.info(f"Processing pedestrian delay from {start_date} to {end_date}")
    
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
    
    logger.info(f"Processing pedestrian delay for {len(date_strings)} dates")
    
    if len(date_strings) == 1:
        # Single date - process directly
        process_single_date_ped_delay(date_strings[0], conf, signals_list)
    else:
        # Multiple dates - parallel processing
        if usable_cores is None:
            usable_cores = min(len(date_strings), os.cpu_count() or 4)
        
        logger.info(f"Using parallel processing with {usable_cores} workers")
        
        with ThreadPoolExecutor(max_workers=usable_cores) as executor:
            futures = {
                executor.submit(
                    process_single_date_ped_delay,
                    date_str,
                    conf,
                    signals_list
                ): date_str
                for date_str in date_strings
            }
            
            success_count = 0
            error_count = 0
            
            for future in as_completed(futures):
                date_str = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Exception for {date_str}: {e}")
            
            logger.info(f"Pedestrian delay completed: {success_count} success, {error_count} errors")
    
    gc.collect()

def process_single_date_split_failures(date_str: str, conf: Dict, signals_list: List[str]) -> bool:
    """
    Process split failures for a single date with retry logic
    Module-level function for parallel processing
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        conf: Configuration dictionary
        signals_list: List of signal IDs
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logger.info(f"Processing split failures for: {date_str}")
        
        # Calculate split failures with retry
        if get_sf_utah is not None:
            sf = keep_trying(
                get_sf_utah,
                n_tries=3,
                timeout=120,
                date_=date_str,
                conf=conf,
                signals_list=signals_list,
                intervals=["hour", "15min"]
            )
        else:
            logger.warning(f"get_sf_utah function not available for {date_str}")
            return False
        
        if sf is None:
            logger.info(f"No split failures data for {date_str}")
            return False
        
        # Upload hourly results
        if len(sf.get('hour', pd.DataFrame())) > 0:
            s3_upload_parquet_date_split(
                sf['hour'],
                bucket=conf['bucket'],
                prefix="sf",
                table_name="split_failures",
                conf_athena=conf['athena']
            )
        
        # Upload 15-min results
        if len(sf.get('15min', pd.DataFrame())) > 0:
            s3_upload_parquet_date_split(
                sf['15min'],
                bucket=conf['bucket'],
                prefix="sf",
                table_name="split_failures_15min",
                conf_athena=conf['athena']
            )
        
        logger.info(f"Completed split failures for: {date_str}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing split failures for {date_str}: {e}")
        return False


def get_sf_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str], usable_cores: int = None):
    """
    Process split failures for date range with parallel processing and retry logic
    Enhanced version of R's lapply with better performance
    
    Args:
        start_date: Start date string
        end_date: End date string
        conf: Configuration dictionary
        signals_list: List of signal IDs
        usable_cores: Number of cores for parallel processing
    """
    logger.info(f"Processing split failures from {start_date} to {end_date}")
    
    # Generate date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
    
    logger.info(f"Processing split failures for {len(date_strings)} dates")
    
    if len(date_strings) == 1:
        # Single date - process directly
        process_single_date_split_failures(date_strings[0], conf, signals_list)
    else:
        # Multiple dates - parallel processing
        if usable_cores is None:
            usable_cores = min(len(date_strings), os.cpu_count() or 4)
        
        logger.info(f"Using parallel processing with {usable_cores} workers")
        
        with ThreadPoolExecutor(max_workers=usable_cores) as executor:
            futures = {
                executor.submit(
                    process_single_date_split_failures,
                    date_str,
                    conf,
                    signals_list
                ): date_str
                for date_str in date_strings
            }
            
            success_count = 0
            error_count = 0
            
            for future in as_completed(futures):
                date_str = futures[future]
                try:
                    result = future.result()
                    if result:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Exception for {date_str}: {e}")
            
            logger.info(f"Split failures completed: {success_count} success, {error_count} errors")
    
    gc.collect()

def log_output_paths_part2(conf: Dict, start_date: str, end_date: str):
    """
    Log all output paths for Part 2 processing
    
    Args:
        conf: Configuration dictionary
        start_date: Start date string
        end_date: End date string
    """
    bucket = conf['bucket']
    
    logger.info("=" * 80)
    logger.info("OUTPUT DATA LOCATIONS - PART 2")
    logger.info("=" * 80)
    
    # Show date range
    logger.info("\n📅 DATE RANGE PROCESSED:")
    logger.info(f"  • Start Date: {start_date}")
    logger.info(f"  • End Date:   {end_date}")
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    logger.info(f"  • Total days: {len(date_range)}")
    
    # Performance metrics outputs
    logger.info("\n📊 PERFORMANCE METRICS DATA:")
    logger.info(f"  (Note: {{YYYY-MM-DD}} = actual dates like {start_date})")
    logger.info(f"  • Queue Spillback (hour):   s3://{bucket}/qs/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Queue Spillback (15min):  s3://{bucket}/qs/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Pedestrian Delay:         s3://{bucket}/pd/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Split Failures (hour):    s3://{bucket}/sf/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Split Failures (15min):   s3://{bucket}/sf/date={{YYYY-MM-DD}}/")
    
    # ETL outputs
    logger.info("\n📋 ETL DATA:")
    logger.info(f"  • Detection Events:         s3://{bucket}/detection_events/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Cycle Data:               s3://{bucket}/cycle_data/date={{YYYY-MM-DD}}/")
    
    # Arrivals on Green
    logger.info("\n🚦 ARRIVALS ON GREEN:")
    logger.info(f"  • AOG Data:                 s3://{bucket}/aog/date={{YYYY-MM-DD}}/")
    
    # Flash Events
    logger.info("\n⚡ FLASH EVENTS:")
    logger.info(f"  • Flash Events:             s3://{bucket}/flash_events/")
    
    # Example path
    logger.info(f"\n  ✓ Actual path example:")
    logger.info(f"    s3://{bucket}/qs/date={start_date}/")
    
    # Athena tables
    database = conf['athena']['database']
    logger.info("\n🗄️  ATHENA TABLES UPDATED:")
    logger.info(f"  • {database}.queue_spillback")
    logger.info(f"  • {database}.queue_spillback_15min")
    logger.info(f"  • {database}.ped_delay")
    logger.info(f"  • {database}.split_failures")
    logger.info(f"  • {database}.split_failures_15min")
    logger.info(f"  • {database}.detection_events")
    logger.info(f"  • {database}.cycle_data")
    
    # AWS Console links
    region = conf['athena'].get('region', 'us-east-1')
    logger.info("\n🌐 AWS S3 CONSOLE:")
    logger.info(f"  • https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={region}&tab=objects")
    
    logger.info("\n🔍 AWS ATHENA CONSOLE:")
    logger.info(f"  • https://console.aws.amazon.com/athena/home?region={region}#/query-editor")
    
    logger.info("=" * 80)


def main():
    """
    Main function for Monthly Report Calculations Part 2
    Optimized with parallel processing and retry logic
    """
    start_time = datetime.now()
    logger.info(f"Starting Monthly Report Calculations Part 2 at {start_time}")
    
    try:
        # Initialize
        init_results = init_main()
        conf = init_results['conf']
        start_date = init_results['start_date']
        end_date = init_results['end_date']
        signals_list = init_results.get('signals_list', [])
        usable_cores = init_results.get('usable_cores', os.cpu_count() or 8)
        
        logger.info(f"Configuration loaded - using {usable_cores} cores for parallel processing")
        
        if signals_list and len(signals_list) > 0:
            logger.info(f"Processing {len(signals_list)} signals from {start_date} to {end_date}")
        else:
            logger.warning("No signals found - will skip signal-based operations")
            logger.info(f"Processing subprocess operations from {start_date} to {end_date}")
        
        # Python path for subprocess calls
        python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
        
        # ETL Dashboard [7 of 11]
        logger.info(f"{datetime.now()} etl [7 of 11]")
        run_etl = conf.get('run', {}).get('etl')
        if run_etl is True or run_etl is None:
            logger.info("Running ETL dashboard...")
            command = f'"{python_env}" etl_dashboard.py {start_date} {end_date}'
            try:
                result = subprocess.run(command, shell=True, check=True, timeout=1800)
                logger.info("ETL dashboard completed successfully")
            except subprocess.CalledProcessError as e:
                logger.warning(f"ETL command failed: {e}, continuing...")
            except subprocess.TimeoutExpired:
                logger.warning("ETL command timed out, continuing...")
        
        # Arrivals on Green [8 of 11]
        logger.info(f"{datetime.now()} aog [8 of 11]")
        run_aog = conf.get('run', {}).get('arrivals_on_green')
        if run_aog is True or run_aog is None:
            logger.info("Running Arrivals on Green...")
            command = f'"{python_env}" get_aog.py {start_date} {end_date}'
            try:
                result = subprocess.run(command, shell=True, check=True, timeout=1800)
                logger.info("Arrivals on Green completed successfully")
            except subprocess.CalledProcessError as e:
                logger.warning(f"AOG command failed: {e}, continuing...")
            except subprocess.TimeoutExpired:
                logger.warning("AOG command timed out, continuing...")
        
        gc.collect()
        
        # Queue Spillback [9 of 11] - with parallel processing (requires signals)
        logger.info(f"{datetime.now()} queue spillback [9 of 11]")
        run_qs = conf.get('run', {}).get('queue_spillback')
        if (run_qs is True or run_qs is None) and signals_list:
            get_queue_spillback_date_range(start_date, end_date, conf, signals_list, usable_cores)
        elif not signals_list:
            logger.warning("Skipping queue spillback - no signals available")
        
        # Pedestrian Delay [10 of 11] - with parallel processing (requires signals)
        logger.info(f"{datetime.now()} ped delay [10 of 11]")
        run_pd = conf.get('run', {}).get('ped_delay')
        if (run_pd is True or run_pd is None) and signals_list:
            get_pd_date_range(start_date, end_date, conf, signals_list, usable_cores)
        elif not signals_list:
            logger.warning("Skipping pedestrian delay - no signals available")
        
        # Split Failures [11 of 11] - with parallel processing (requires signals)
        logger.info(f"{datetime.now()} split failures [11 of 11]")
        run_sf = conf.get('run', {}).get('split_failures')
        if (run_sf is True or run_sf is None) and signals_list:
            get_sf_date_range(start_date, end_date, conf, signals_list, usable_cores)
        elif not signals_list:
            logger.warning("Skipping split failures - no signals available")
        
        # Flash Events [12 of 12]
        logger.info(f"{datetime.now()} flash events [12 of 12]")
        run_flash = conf.get('run', {}).get('flash_events')
        if run_flash is True or run_flash is None:
            logger.info("Running Flash Events...")
            command = f'"{python_env}" get_flash_events.py'
            try:
                result = subprocess.run(command, shell=True, check=True, timeout=600)
                logger.info("Flash Events completed successfully")
            except subprocess.CalledProcessError as e:
                logger.warning(f"Flash events command failed: {e}, continuing...")
            except subprocess.TimeoutExpired:
                logger.warning("Flash events command timed out, continuing...")
        
        gc.collect()
        
        # Calculate duration
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 80)
        logger.info("Monthly Report Calculations Part 2 completed")
        logger.info(f"Total processing time: {duration}")
        
        # Log processing summary
        if signals_list and len(signals_list) > 0:
            logger.info(f"Processed {len(signals_list)} signals")
            logger.info("All signal-based metrics calculated")
        else:
            logger.warning(f"No signals processed (signals_list was empty)")
            logger.info("Subprocess operations (ETL, AOG, Flash Events) completed")
        
        logger.info("=" * 80)
        
        # Log all output paths (if signals were processed)
        if signals_list and len(signals_list) > 0:
            log_output_paths_part2(conf, start_date, end_date)
        else:
            logger.info("Output paths not logged - no signal-based processing occurred")
        
        # Create completion checkpoint
        try:
            if create_checkpoint_file is not None:
                create_checkpoint_file(
                    'calcs_part2_complete',
                    {
                        'timestamp': end_time.isoformat(),
                        'duration_seconds': duration.total_seconds(),
                        'start_date': start_date,
                        'end_date': end_date,
                        'signals_count': len(signals_list),
                        'processing_method': 'parallel_with_retry'
                    },
                    conf['bucket']
                )
        except Exception as e:
            logger.warning(f"Could not create checkpoint: {e}")
        
        logger.info("\n--------------------- End Monthly Report calcs Part 2 -----------------------\n")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Send error notification if configured
        try:
            if send_notification is not None and 'notifications' in conf:
                send_notification(
                    f"Monthly Report Calculations Part 2 failed: {e}",
                    "error",
                    conf.get('notifications', {}).get('email'),
                    conf.get('notifications', {}).get('slack')
                )
        except Exception as notify_error:
            logger.error(f"Could not send notification: {notify_error}")
        
        return False

if __name__ == "__main__":
    """
    Main execution block
    Optimized for parallel processing and robustness
    """
    
    # Setup logging
    if setup_logging is not None:
        setup_logging("INFO", "monthly_report_calcs_2.log")
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('monthly_report_calcs_2.log')
            ]
        )
    
    try:
        logger.info(f"Starting Monthly Report Calcs 2 at {datetime.now()}")
        logger.info("Optimized with parallel processing and retry logic")
        
        success = main()
        
        if success:
            logger.info(f"Completed Monthly Report Calcs 2 successfully at {datetime.now()}")
            sys.exit(0)
        else:
            logger.error(f"Monthly Report Calcs 2 failed at {datetime.now()}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
        
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


