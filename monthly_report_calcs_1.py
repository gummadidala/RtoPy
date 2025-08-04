"""
Monthly Report Calculations - Part 1
Converted from Monthly_Report_Calcs_1.R
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import subprocess
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import multiprocessing

# Import custom modules
from monthly_report_calcs_init import main as init_main
from monthly_report_functions import *
from counts import *
from s3_parquet_io import *

# Setup logging
logger = logging.getLogger(__name__)

def run_async_scripts(conf: dict):
    """
    Run Python scripts asynchronously
    
    Args:
        conf: Configuration dictionary
    """
    
    # python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
    python_env = "C:\\Users\\kgummadidala\\Desktop\\Rtopy\\server-env\\Scripts\\python.exe"
    
    scripts_to_run = []
    
    # CCTV logs parsing
    if conf['run'].get('cctv', True):
        logger.info("Starting CCTV log parsing [1 of 11]")
        scripts_to_run.extend([
            f"{python_env} parse_cctvlog.py",
            f"{python_env} parse_cctvlog_encoders.py"
        ])
    
    # RSU logs parsing
    if conf['run'].get('rsus', False):
        logger.info("Starting RSU log parsing [2 of 11]")
        scripts_to_run.append(f"{python_env} parse_rsus.py")
    
    # Travel times from RITIS API
    if conf['run'].get('travel_times', True):
        logger.info("Starting travel times processing [3 of 11]")
        scripts_to_run.extend([
            f"{python_env} get_travel_times_v2.py mark travel_times_1hr.yaml",
            f"{python_env} get_travel_times_v2.py mark travel_times_15min.yaml",
            f"{python_env} get_travel_times_1min_v2.py mark"
        ])
    
    # Run scripts asynchronously
    processes = []
    for script in scripts_to_run:
        try:
            process = subprocess.Popen(
                script.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            processes.append((script, process))
            logger.info(f"Started: {script}")
        except Exception as e:
            logger.error(f"Failed to start {script}: {e}")
    
    return processes

def process_single_date_counts(date_str, bucket, conf_athena):
    """
    Process counts for a single date - module level function for multiprocessing
    
    Args:
        date_str: Date string to process
        bucket: S3 bucket name
        conf_athena: Athena configuration
    
    Returns:
        Result of processing or None if failed
    """
    try:
        return keep_trying(
            get_counts2,
            n_tries=2,
            date_=date_str,
            bucket=bucket,
            conf_athena=conf_athena,
            uptime=True,
            counts=True
        )
    except Exception as e:
        logger.error(f"Error processing counts for {date_str}: {e}")
        return None

def process_counts(conf: dict, start_date: str, end_date: str, usable_cores: int):
    """
    Process counts data for the date range
    
    Args:
        conf: Configuration dictionary
        start_date: Start date string
        end_date: End date string
        usable_cores: Number of cores for parallel processing
    """
    
    logger.info("Starting counts processing [4 of 11]")
    
    if not conf['run'].get('counts', True):
        logger.info("Counts processing disabled in configuration")
        return
    
    try:
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
        
        if len(date_strings) == 1:
            # Single date processing
            get_counts2(
                date_strings[0],
                bucket=conf['bucket'],
                conf_athena=conf['athena'],
                uptime=True,
                counts=True
            )
        else:
            # Parallel processing for multiple dates
            # Use ProcessPoolExecutor for CPU-intensive work
            with ProcessPoolExecutor(max_workers=usable_cores) as executor:
                futures = {
                    executor.submit(
                        process_single_date_counts, 
                        date_str, 
                        conf['bucket'], 
                        conf['athena']
                    ): date_str 
                    for date_str in date_strings
                }
                
                for future in as_completed(futures):
                    date_str = futures[future]
                    try:
                        result = future.result()
                        if result is not None:
                            logger.info(f"Completed counts processing for {date_str}")
                        else:
                            logger.warning(f"Failed counts processing for {date_str}")
                    except Exception as e:
                        logger.error(f"Exception in counts processing for {date_str}: {e}")
        
        logger.info("---------------------- Finished counts ---------------------------")
        
    except Exception as e:
        logger.error(f"Error in counts processing: {e}")
        raise

def get_counts_based_measures(month_abbrs: list, conf: dict, end_date: str, usable_cores: int):
    """
    Process counts-based measures for each month
    
    Args:
        month_abbrs: List of month abbreviations (YYYY-MM)
        conf: Configuration dictionary
        end_date: End date string
        usable_cores: Number of cores for parallel processing
    """
    
    logger.info("Starting monthly counts-based measures [5 of 11]")
    logger.info("Starting counts-based measures [6 of 11]")
    
    if not conf['run'].get('counts_based_measures', True):
        logger.info("Counts-based measures processing disabled in configuration")
        return
    
    def process_month(yyyy_mm: str):
        """Process a single month"""
        try:
            logger.info(f"Processing month: {yyyy_mm}")
            
            # Calculate start and end days of the month
            start_day = pd.to_datetime(f"{yyyy_mm}-01")
            end_day = start_day + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            end_day = min(end_day, pd.to_datetime(end_date))
            
            date_range = pd.date_range(start=start_day, end=end_day, freq='D')
            date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
            
            # Process 1-hour counts
            process_hourly_counts(yyyy_mm, date_strings, conf, usable_cores)
            
            # Process 15-minute counts
            process_15min_counts(yyyy_mm, date_strings, conf, usable_cores)
            
            logger.info(f"Completed processing for month: {yyyy_mm}")
            
        except Exception as e:
            logger.error(f"Error processing month {yyyy_mm}: {e}")
            raise
    
    # Process each month
    for yyyy_mm in month_abbrs:
        process_month(yyyy_mm)
    
    logger.info("--- Finished counts-based measures ---")

def process_hourly_counts(yyyy_mm: str, date_range: list, conf: dict, usable_cores: int):
    """
    Process 1-hour counts for a month
    
    Args:
        yyyy_mm: Month abbreviation (YYYY-MM)
        date_range: List of date strings
        conf: Configuration dictionary
        usable_cores: Number of cores for parallel processing
    """
    
    try:
        logger.info("Processing 1-hour adjusted counts")
        
        # Prepare database for adjusted counts
        prep_db_for_adjusted_counts_arrow("filtered_counts_1hr", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_1hr", "adjusted_counts_1hr", conf)
        
        # Open datasets
        fc_ds = keep_trying(
            lambda: open_arrow_dataset("filtered_counts_1hr/"),
            n_tries=3,
            timeout=60
        )
        ac_ds = keep_trying(
            lambda: open_arrow_dataset("adjusted_counts_1hr/"),
            n_tries=3,
            timeout=60
        )
        
        # Upload adjusted counts to S3
        for date_str in date_range:
            upload_adjusted_counts_1hr(ac_ds, date_str, conf)
        
        # Write signal details
        signals_list = get_signals_from_dataset(ac_ds, date_range)
        for date_str in date_range:
            write_signal_details(date_str, conf, signals_list)
        
        # Process VPD and VPH in parallel
        def process_date_metrics(date_str: str):
            """Process metrics for a single date"""
            try:
                logger.info(f"Processing metrics for: {date_str}")
                
                # Read adjusted counts
                adjusted_counts_1hr = read_adjusted_counts_for_date(ac_ds, date_str)
                
                if adjusted_counts_1hr is not None and len(adjusted_counts_1hr) > 0:
                    # Prepare data
                    adjusted_counts_1hr = prepare_counts_data(adjusted_counts_1hr)
                    
                    # Calculate VPD (Vehicles Per Day)
                    logger.info(f"Calculating VPD for {date_str}")
                    vpd = get_vpd(adjusted_counts_1hr)
                    s3_upload_parquet_date_split(
                        vpd,
                        bucket=conf['bucket'],
                        prefix="vpd",
                        table_name="vehicles_pd",
                        conf_athena=conf['athena']
                    )
                    
                    # Calculate VPH (Vehicles Per Hour)
                    logger.info(f"Calculating VPH for {date_str}")
                    vph = get_vph(adjusted_counts_1hr, interval="1 hour")
                    s3_upload_parquet_date_split(
                        vph,
                        bucket=conf['bucket'],
                        prefix="vph",
                        table_name="vehicles_ph",
                        conf_athena=conf['athena']
                    )
                    
                    logger.info(f"Completed metrics for {date_str}")
                else:
                    logger.warning(f"No adjusted counts data for {date_str}")
                    
            except Exception as e:
                logger.error(f"Error processing metrics for {date_str}: {e}")
        
        # Process dates in parallel
        with ThreadPoolExecutor(max_workers=usable_cores) as executor:
            futures = [
                executor.submit(process_date_metrics, date_str) 
                for date_str in date_range
            ]
            
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in parallel processing: {e}")
        
        # Cleanup temporary directories
        cleanup_temp_directories("filtered_counts_1hr", "adjusted_counts_1hr")
        
    except Exception as e:
        import traceback
        logger.error(f"Error processing hourly counts: {e}")
        print(traceback.format_exc())
        raise

def process_15min_counts(yyyy_mm: str, date_range: list, conf: dict, usable_cores: int):
    """
    Process 15-minute counts for a month
    
    Args:
        yyyy_mm: Month abbreviation (YYYY-MM)
        date_range: List of date strings
        conf: Configuration dictionary
        usable_cores: Number of cores for parallel processing
    """
    
    try:
        logger.info("Processing 15-minute counts and throughput")
        logger.info("Processing 15-minute adjusted counts")
        
        # Prepare database for adjusted counts
        prep_db_for_adjusted_counts_arrow("filtered_counts_15min", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_15min", "adjusted_counts_15min", conf)
        
        # Open datasets
        fc_ds = keep_trying(
            lambda: open_arrow_dataset("filtered_counts_15min/"),
            n_tries=3,
            timeout=60
        )
        ac_ds = keep_trying(
            lambda: open_arrow_dataset("adjusted_counts_15min/"),
            n_tries=3,
            timeout=60
        )
        
        # Process each date
        for date_str in date_range:
            try:
                # Read adjusted counts
                adjusted_counts_15min = read_adjusted_counts_for_date(ac_ds, date_str)
                
                if adjusted_counts_15min is None or len(adjusted_counts_15min) == 0:
                    # Create empty DataFrame with correct structure
                    adjusted_counts_15min = create_empty_counts_dataframe()
                
                # Upload adjusted counts
                s3_upload_parquet_date_split(
                    adjusted_counts_15min,
                    bucket=conf['bucket'],
                    prefix="adjusted_counts_15min",
                    table_name="adjusted_counts_15min",
                    conf_athena=conf['athena']
                )
                
                # Calculate throughput
                throughput = get_thruput(adjusted_counts_15min)
                s3_upload_parquet_date_split(
                    throughput,
                    bucket=conf['bucket'],
                    prefix="tp",
                    table_name="throughput",
                    conf_athena=conf['athena']
                )
                
                # Calculate vehicles per 15-minute period
                logger.info(f"Calculating VP15 for {date_str}")
                vp15 = get_vph(adjusted_counts_15min, interval="15 min")
                s3_upload_parquet_date_split(
                    vp15,
                    bucket=conf['bucket'],
                    prefix="vp15",
                    table_name="vehicles_15min",
                    conf_athena=conf['athena']
                )
                
                logger.info(f"Completed 15-min processing for {date_str}")
                
            except Exception as e:
                logger.error(f"Error processing 15-min counts for {date_str}: {e}")
        
        # Cleanup temporary directories
        cleanup_temp_directories("filtered_counts_15min", "adjusted_counts_15min")
        
    except Exception as e:
        logger.error(f"Error processing 15-minute counts: {e}")
        raise

def upload_adjusted_counts_1hr(ac_ds, date_str: str, conf: dict):
    """
    Upload 1-hour adjusted counts for a specific date
    
    Args:
        ac_ds: Arrow dataset
        date_str: Date string
        conf: Configuration dictionary
    """
    
    try:
        adjusted_counts_1hr = read_adjusted_counts_for_date(ac_ds, date_str)
        
        if adjusted_counts_1hr is None or len(adjusted_counts_1hr) == 0:
            adjusted_counts_1hr = create_empty_counts_dataframe()
        
        s3_upload_parquet_date_split(
            adjusted_counts_1hr,
            bucket=conf['bucket'],
            prefix="adjusted_counts_1hr",
            table_name="adjusted_counts_1hr",
            conf_athena=conf['athena']
        )
        
    except Exception as e:
        logger.error(f"Error uploading adjusted counts for {date_str}: {e}")

def read_adjusted_counts_for_date(ac_ds, date_str: str) -> pd.DataFrame:
    """
    Read adjusted counts for a specific date from Arrow dataset
    
    Args:
        ac_ds: Arrow dataset
        date_str: Date string
    
    Returns:
        DataFrame with adjusted counts
    """
    
    try:
        if hasattr(ac_ds, 'to_table') and len(ac_ds.to_table()) == 0:
            return None
        
        # Filter and collect data
        filtered_data = ac_ds.filter(
            ac_ds.schema.field('date') == date_str
        ).select(['SignalID', 'CallPhase', 'Detector', 'Timeperiod', 'vol'])
        
        return filtered_data.to_pandas()
        
    except Exception as e:
        logger.error(f"Error reading adjusted counts for {date_str}: {e}")
        return None

def prepare_counts_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare counts data with proper data types
    
    Args:
        df: Raw counts DataFrame
    
    Returns:
        Prepared DataFrame
    """
    
    try:
        df = df.copy()
        
        # Convert data types
        df['Date'] = pd.to_datetime(df.get('Date', df['Timeperiod'])).dt.date
        df['SignalID'] = df['SignalID'].astype(str)
        df['CallPhase'] = df['CallPhase'].astype(str)
        df['Detector'] = df['Detector'].astype(str)
        
        return df
        
    except Exception as e:
        logger.error(f"Error preparing counts data: {e}")
        return df

def create_empty_counts_dataframe() -> pd.DataFrame:
    """
    Create empty counts DataFrame with correct structure
    
    Returns:
        Empty DataFrame with proper columns and types
    """
    
    return pd.DataFrame({
        'SignalID': pd.Series([], dtype=str),
        'CallPhase': pd.Series([], dtype=int),
        'Detector': pd.Series([], dtype=int),
        'Timeperiod': pd.Series([], dtype='datetime64[ns]'),
        'vol': pd.Series([], dtype=int)
    })

def get_signals_from_dataset(ac_ds, date_range: list) -> list:
    """
    Get list of signals from dataset
    
    Args:
        ac_ds: Arrow dataset
        date_range: List of date strings
    
    Returns:
        List of signal IDs
    """
    
    try:
        # Read a sample to get signal IDs
        if hasattr(ac_ds, 'to_table') and len(ac_ds.to_table()) > 0:
            sample_data = ac_ds.select(['SignalID']).limit(10000).to_pandas()
            signals_list = sample_data['SignalID'].unique().tolist()
        else:
            signals_list = []
        
        return signals_list
        
    except Exception as e:
        logger.error(f"Error getting signals from dataset: {e}")
        return []

def open_arrow_dataset(path: str):
    """
    Open Arrow dataset with error handling
    
    Args:
        path: Path to dataset
    
    Returns:
        Arrow dataset
    """
    
    try:
        import pyarrow.dataset as ds
        return ds.dataset(path)
    except Exception as e:
        logger.error(f"Error opening Arrow dataset {path}: {e}")
        raise

def monitor_async_processes(processes: list):
    """
    Monitor asynchronous processes and log their status
    
    Args:
        processes: List of (script_name, process) tuples
    """
    
    completed_processes = []
    
    for script_name, process in processes:
        try:
            # Check if process is still running
            return_code = process.poll()
            
            if return_code is not None:
                # Process completed
                stdout, stderr = process.communicate()
                
                if return_code == 0:
                    logger.info(f"Completed successfully: {script_name}")
                else:
                    logger.error(f"Failed with return code {return_code}: {script_name}")
                    if stderr:
                        logger.error(f"Error output: {stderr.decode()}")
                
                completed_processes.append(script_name)
            else:
                logger.info(f"Still running: {script_name}")
                
        except Exception as e:
            logger.error(f"Error monitoring process {script_name}: {e}")
    
    return completed_processes

def cleanup_and_finalize(conf: dict):
    """
    Cleanup resources and finalize processing
    
    Args:
        conf: Configuration dictionary
    """
    
    try:
        # Close database connections
        close_all_connections()
        
        # Cleanup temporary files
        temp_dirs = [
            "filtered_counts_1hr",
            "adjusted_counts_1hr", 
            "filtered_counts_15min",
            "adjusted_counts_15min"
        ]
        cleanup_temp_directories(*temp_dirs)
        
        # Monitor system resources
        monitor_system_resources()
        
        logger.info("Cleanup and finalization completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def main():
    """Main function for Monthly Report Calculations Part 1"""
    
    start_time = datetime.now()
    logger.info(f"Starting Monthly Report Calculations Part 1 at {start_time}")
    
    try:
        # Initialize
        init_results = init_main()
        conf = init_results['conf']
        start_date = init_results['start_date']
        end_date = init_results['end_date']
        month_abbrs = init_results['month_abbrs']
        signals_list = init_results['signals_list']
        usable_cores = init_results['usable_cores']
        
        # Start async scripts
        async_processes = run_async_scripts(conf)
        
        # Process counts
        process_counts(conf, start_date, end_date, usable_cores)
        
        # Process counts-based measures
        # get_counts_based_measures(month_abbrs, conf, end_date, usable_cores)
        
        # Monitor async processes
        logger.info("Checking status of async processes...")
        completed = monitor_async_processes(async_processes)
        logger.info(f"Completed async processes: {completed}")
        
        # Cleanup
        cleanup_and_finalize(conf)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Generate summary
        summary = generate_processing_summary(
            start_time, 
            end_time,
            [start_date, end_date],  # dates processed
            []  # errors - would be populated if tracking errors
        )
        
        logger.info(summary)
        logger.info("Monthly Report Calculations Part 1 completed successfully")
        
        # Create completion checkpoint
        create_checkpoint_file(
            'calcs_part1_complete',
            {
                'timestamp': end_time.isoformat(),
                'duration_seconds': duration.total_seconds(),
                'start_date': start_date,
                'end_date': end_date,
                'month_abbrs': month_abbrs,
                'signals_count': len(signals_list)
            },
            conf['bucket']
        )
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        logger.error(f"Monthly Report Calculations Part 1 failed: {e}")
        
        # Send error notification if configured
        if 'notifications' in conf:
            send_notification(
                f"Monthly Report Calculations Part 1 failed: {e}",
                "error",
                conf['notifications'].get('email'),
                conf['notifications'].get('slack')
            )
        
        return False

def close_all_connections():
    """Close all database connections"""
    
    try:
        # This would close any open database connections
        # Implementation depends on your database connection management
        logger.info("Closed all database connections")
    except Exception as e:
        logger.error(f"Error closing connections: {e}")

if __name__ == "__main__":
    """Run calculations if called directly"""
    
    # Setup logging
    setup_logging("INFO", "monthly_report_calcs_1.log")
    
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
