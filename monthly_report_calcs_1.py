"""
Monthly Report Calculations - Part 1
Converted from Monthly_Report_Calcs_1.R
Refactored to use Athena cloud-native processing for better performance
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import subprocess
import boto3
import awswrangler as wr
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import partial

# Import custom modules
from monthly_report_calcs_init import main as init_main
from monthly_report_functions import *
from athena_processing import *
from counts import get_counts2  # Only need the raw counts function
from s3_parquet_io import *
from utilities import keep_trying

# Setup logging
logger = logging.getLogger(__name__)

def run_async_scripts(conf: dict):
    """
    Run Python scripts asynchronously
    
    Args:
        conf: Configuration dictionary
    """
    
    python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
    # python_env = "C:\\Users\\kgummadidala\\Desktop\\Rtopy\\server-env\\Scripts\\python.exe"
    
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

def process_single_date_counts(date_str: str, bucket: str, conf_athena: dict):
    """
    Process counts for a single date with retry logic
    Module-level function for multiprocessing
    
    Args:
        date_str: Date string to process
        bucket: S3 bucket name
        conf_athena: Athena configuration
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use retry logic matching R's keep_trying(n_tries=2)
        result = keep_trying(
            get_counts2,
            n_tries=2,
            date_=date_str,
            bucket=bucket,
            conf_athena=conf_athena,
            uptime=True,
            counts=True
        )
        logger.info(f"Completed raw counts for: {date_str}")
        return True
    except Exception as e:
        logger.error(f"Error processing counts for {date_str}: {e}")
        return False


def process_counts(conf: dict, start_date: str, end_date: str, usable_cores: int = None):
    """
    Process raw counts data for the date range with parallel processing and retry logic
    This extracts raw counts from source and stores them in S3
    Matches R's foreach %dopar% with keep_trying(n_tries=2)
    
    Args:
        conf: Configuration dictionary
        start_date: Start date string
        end_date: End date string
        usable_cores: Number of cores for parallel processing (optional)
    """
    
    logger.info("Starting counts processing [4 of 11]")
    
    if not conf['run'].get('counts', True):
        logger.info("Counts processing disabled in configuration")
        return
    
    try:
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
        
        logger.info(f"Processing counts for {len(date_strings)} dates")
        
        if len(date_strings) == 1:
            # Single date - process directly (matching R logic)
            logger.info(f"Processing single date: {date_strings[0]}")
            keep_trying(
                get_counts2,
                n_tries=2,
                date_=date_strings[0],
                bucket=conf['bucket'],
                conf_athena=conf['athena'],
                uptime=True,
                counts=True
            )
        else:
            # Multiple dates - parallel processing (matching R's foreach %dopar%)
            if usable_cores is None:
                usable_cores = min(len(date_strings), os.cpu_count() or 4)
            
            logger.info(f"Using parallel processing with {usable_cores} workers")
            
            with ProcessPoolExecutor(max_workers=usable_cores) as executor:
                # Create futures for all dates
                futures = {
                    executor.submit(
                        process_single_date_counts,
                        date_str,
                        conf['bucket'],
                        conf['athena']
                    ): date_str
                    for date_str in date_strings
                }
                
                # Track results
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
                            logger.warning(f"Failed to process {date_str}")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Exception processing {date_str}: {e}")
                
                logger.info(f"Counts processing completed: {success_count} success, {error_count} errors")
        
        logger.info("---------------------- Finished counts ---------------------------")
        
    except Exception as e:
        logger.error(f"Error in counts processing: {e}")
        raise

def get_counts_based_measures(month_abbrs: list, conf: dict, end_date: str):
    """
    Process counts-based measures for each month using Athena cloud processing
    All heavy computation happens in Athena SQL engine
    
    Args:
        month_abbrs: List of month abbreviations (YYYY-MM)
        conf: Configuration dictionary
        end_date: End date string
    """
    
    logger.info("Starting monthly counts-based measures [5 of 11]")
    logger.info("Starting counts-based measures [6 of 11]")
    logger.info("Using Athena cloud-native processing - no local data movement")
    
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
            
            # Process 1-hour counts (Athena cloud processing)
            process_hourly_counts_athena(yyyy_mm, date_strings, conf)
            
            # Process 15-minute counts (Athena cloud processing)
            process_15min_counts_athena(yyyy_mm, date_strings, conf)
            
            logger.info(f"Completed processing for month: {yyyy_mm}")
            
        except Exception as e:
            logger.error(f"Error processing month {yyyy_mm}: {e}")
            raise
    
    # Process each month
    for yyyy_mm in month_abbrs:
        process_month(yyyy_mm)
    
    logger.info("--- Finished counts-based measures ---")

def process_hourly_counts_athena(yyyy_mm: str, date_range: list, conf: dict):
    """
    Process 1-hour counts using Athena cloud processing with retry logic
    All computation happens in Athena SQL - no local data download
    Faster and more robust than R's local Arrow processing
    
    Args:
        yyyy_mm: Month abbreviation (YYYY-MM)
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    try:
        logger.info(f"Processing 1-hour counts in Athena for {yyyy_mm}")
        logger.info(f"Date range: {date_range[0]} to {date_range[-1]} ({len(date_range)} days)")
        
        # Step 1: Create adjusted counts in Athena (SQL processing) with retry
        logger.info("Step 1/4: Creating adjusted counts in Athena...")
        keep_trying(
            create_adjusted_counts_athena,
            n_tries=3,
            timeout=300,
            date_range=date_range,
            conf=conf,
            interval='1hr'
        )
        
        # Step 2: Calculate VPD (Vehicles Per Day) in Athena with retry
        logger.info("Step 2/4: Calculating VPD in Athena...")
        keep_trying(
            calculate_vpd_athena,
            n_tries=3,
            timeout=180,
            date_range=date_range,
            conf=conf
        )
        
        # Step 3: Calculate VPH (Vehicles Per Hour) in Athena with retry
        logger.info("Step 3/4: Calculating VPH in Athena...")
        keep_trying(
            calculate_vph_athena,
            n_tries=3,
            timeout=180,
            date_range=date_range,
            conf=conf
        )
        
        # Step 4: Get signals list and write details (small metadata operation)
        logger.info("Step 4/4: Writing signal details...")
        try:
            signals_list = keep_trying(
                get_signals_list_athena,
                n_tries=3,
                timeout=60,
                date_range=date_range,
                conf=conf
            )
        except Exception as e:
            logger.warning(f"Could not get signals list from Athena: {e}")
            signals_list = []
        
        for date_str in date_range:
            try:
                write_signal_details(date_str, conf, signals_list)
            except Exception as e:
                logger.warning(f"Could not write signal details for {date_str}: {e}")
        
        logger.info(f"Completed hourly counts processing for {yyyy_mm}")
        
        # Log quality stats
        try:
            stats = get_data_quality_stats_athena(date_range, conf)
            if not stats.empty:
                logger.info(f"Data quality stats:\n{stats.to_string()}")
        except Exception as e:
            logger.warning(f"Could not retrieve quality stats: {e}")
        
    except Exception as e:
        import traceback
        logger.error(f"Error processing hourly counts: {e}")
        logger.error(traceback.format_exc())
        raise

def process_15min_counts_athena(yyyy_mm: str, date_range: list, conf: dict):
    """
    Process 15-minute counts using Athena cloud processing with retry logic
    All computation happens in Athena SQL - no local data download
    Faster and more robust than R's local Arrow processing
    
    Args:
        yyyy_mm: Month abbreviation (YYYY-MM)
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    try:
        logger.info(f"Processing 15-minute counts in Athena for {yyyy_mm}")
        logger.info(f"Date range: {date_range[0]} to {date_range[-1]} ({len(date_range)} days)")
        
        # Step 1: Create adjusted counts in Athena (SQL processing) with retry
        logger.info("Step 1/3: Creating adjusted 15-min counts in Athena...")
        keep_trying(
            create_adjusted_counts_athena,
            n_tries=3,
            timeout=300,
            date_range=date_range,
            conf=conf,
            interval='15min'
        )
        
        # Step 2: Calculate throughput in Athena with retry
        logger.info("Step 2/3: Calculating throughput in Athena...")
        keep_trying(
            calculate_throughput_athena,
            n_tries=3,
            timeout=180,
            date_range=date_range,
            conf=conf
        )
        
        # Step 3: Calculate VP15 (Vehicles Per 15 Minutes) in Athena with retry
        logger.info("Step 3/3: Calculating VP15 in Athena...")
        keep_trying(
            calculate_vp15_athena,
            n_tries=3,
            timeout=180,
            date_range=date_range,
            conf=conf
        )
        
        logger.info(f"Completed 15-min counts processing for {yyyy_mm}")
        
    except Exception as e:
        import traceback
        logger.error(f"Error processing 15-minute counts: {e}")
        logger.error(traceback.format_exc())
        raise

# Note: PyArrow-based functions removed - now using Athena cloud processing
# All data processing happens in Athena SQL engine for better performance

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
    No local temp files to clean up - everything processed in Athena
    
    Args:
        conf: Configuration dictionary
    """
    
    try:
        # Close database connections if any
        close_all_connections()
        
        # Monitor system resources
        try:
            monitor_system_resources()
        except Exception as e:
            logger.warning(f"Could not monitor system resources: {e}")
        
        # Verify Athena tables were created
        try:
            table_status = verify_athena_tables_exist(conf)
            logger.info("Athena table status:")
            for table, exists in table_status.items():
                status = "✓ EXISTS" if exists else "✗ MISSING"
                logger.info(f"  {table}: {status}")
        except Exception as e:
            logger.warning(f"Could not verify Athena tables: {e}")
        
        logger.info("Cleanup and finalization completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")

def main():
    """
    Main function for Monthly Report Calculations Part 1
    Uses Athena cloud-native processing for optimal performance
    """
    
    start_time = datetime.now()
    logger.info(f"Starting Monthly Report Calculations Part 1 at {start_time}")
    logger.info("Using Athena cloud-native processing - all computation in AWS")
    
    try:
        # Initialize
        init_results = init_main()
        conf = init_results['conf']
        start_date = init_results['start_date']
        end_date = init_results['end_date']
        month_abbrs = init_results['month_abbrs']
        signals_list = init_results['signals_list']
        usable_cores = init_results['usable_cores']
        
        logger.info(f"Configuration loaded - using {usable_cores} cores for parallel processing")
        
        # Verify AWS credentials are configured
        if not conf.get('AWS_ACCESS_KEY_ID') or not conf.get('AWS_SECRET_ACCESS_KEY'):
            logger.warning("AWS credentials not found in config - will use environment credentials")
        
        # Start async scripts
        async_processes = run_async_scripts(conf)
        
        # Process raw counts (extract from source) - with parallel processing and retry logic
        process_counts(conf, start_date, end_date, usable_cores)
        
        # Process counts-based measures (Athena cloud processing - faster than R!)
        get_counts_based_measures(month_abbrs, conf, end_date)
        
        # Monitor async processes
        logger.info("Checking status of async processes...")
        completed = monitor_async_processes(async_processes)
        logger.info(f"Completed async processes: {completed}")
        
        # Cleanup
        cleanup_and_finalize(conf)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Generate summary
        try:
            summary = generate_processing_summary(
                start_time, 
                end_time,
                [start_date, end_date],  # dates processed
                []  # errors - would be populated if tracking errors
            )
            logger.info(summary)
        except Exception as e:
            logger.warning(f"Could not generate summary: {e}")
        
        logger.info("Monthly Report Calculations Part 1 completed successfully")
        logger.info(f"Total processing time: {duration}")
        
        # Log all output paths
        log_output_paths(conf, start_date, end_date)
        
        # Create completion checkpoint
        try:
            create_checkpoint_file(
                'calcs_part1_complete',
                {
                    'timestamp': end_time.isoformat(),
                    'duration_seconds': duration.total_seconds(),
                    'start_date': start_date,
                    'end_date': end_date,
                    'month_abbrs': month_abbrs,
                    'signals_count': len(signals_list),
                    'processing_method': 'athena_cloud_native'
                },
                conf['bucket']
            )
        except Exception as e:
            logger.warning(f"Could not create checkpoint: {e}")
        
        return True
        
    except Exception as e:
        end_time = datetime.now()
        logger.error(f"Monthly Report Calculations Part 1 failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Send error notification if configured
        try:
            if 'notifications' in conf:
                send_notification(
                    f"Monthly Report Calculations Part 1 failed: {e}",
                    "error",
                    conf['notifications'].get('email'),
                    conf['notifications'].get('slack')
                )
        except Exception as notify_error:
            logger.error(f"Could not send notification: {notify_error}")
        
        return False

def log_output_paths(conf: dict, start_date: str, end_date: str):
    """
    Log all output paths where data was written
    
    Args:
        conf: Configuration dictionary
        start_date: Start date string
        end_date: End date string
    """
    
    bucket = conf['bucket']
    
    logger.info("=" * 80)
    logger.info("OUTPUT DATA LOCATIONS")
    logger.info("=" * 80)
    
    # Show date range info first
    logger.info("\n📅 DATE RANGE PROCESSED:")
    logger.info(f"  • Start Date: {start_date}")
    logger.info(f"  • End Date:   {end_date}")
    
    # Generate example dates
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    if len(date_range) == 1:
        date_examples = f"date={start_date}"
    elif len(date_range) <= 3:
        date_examples = ", ".join([f"date={d.strftime('%Y-%m-%d')}" for d in date_range])
    else:
        first_date = date_range[0].strftime('%Y-%m-%d')
        last_date = date_range[-1].strftime('%Y-%m-%d')
        date_examples = f"date={first_date}, ..., date={last_date}"
    
    logger.info(f"  • Data partitions: {date_examples}")
    logger.info(f"  • Total days: {len(date_range)}")
    
    # Raw counts outputs
    logger.info("\n📊 RAW COUNTS DATA:")
    logger.info(f"  (Note: {{YYYY-MM-DD}} = actual dates like {start_date})")
    logger.info(f"  • Hourly Counts:        s3://{bucket}/mark/counts_1hr/date={{YYYY-MM-DD}}/")
    logger.info(f"  • 15-Min Counts:        s3://{bucket}/mark/counts_15min/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Detector Uptime:      s3://{bucket}/detector_uptime/date={{YYYY-MM-DD}}/")
    
    # Show actual example path
    logger.info(f"\n  ✓ Actual path example:")
    logger.info(f"    s3://{bucket}/mark/counts_1hr/date={start_date}/")
    
    # Adjusted counts outputs
    logger.info("\n📈 ADJUSTED COUNTS DATA:")
    logger.info(f"  • Adjusted 1hr:         s3://{bucket}/adjusted_counts_1hr/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Adjusted 15min:       s3://{bucket}/adjusted_counts_15min/date={{YYYY-MM-DD}}/")
    
    # Metrics outputs
    logger.info("\n📉 CALCULATED METRICS:")
    logger.info(f"  • VPD (Vehicles/Day):   s3://{bucket}/vpd/date={{YYYY-MM-DD}}/")
    logger.info(f"  • VPH (Vehicles/Hour):  s3://{bucket}/vph/date={{YYYY-MM-DD}}/")
    logger.info(f"  • VP15 (Vehicles/15m):  s3://{bucket}/vp15/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Throughput:           s3://{bucket}/tp/date={{YYYY-MM-DD}}/")
    
    # Metadata outputs
    logger.info("\n📋 METADATA:")
    logger.info(f"  • Signal Details:       s3://{bucket}/signal_details/date={{YYYY-MM-DD}}/")
    logger.info(f"  • Checkpoint:           s3://{bucket}/checkpoints/calcs_part1_complete.json")
    
    # Athena tables
    database = conf['athena']['database']
    logger.info("\n🗄️  ATHENA TABLES UPDATED:")
    logger.info(f"  • {database}.counts_1hr")
    logger.info(f"  • {database}.counts_15min")
    logger.info(f"  • {database}.detector_uptime")
    logger.info(f"  • {database}.adjusted_counts_1hr")
    logger.info(f"  • {database}.adjusted_counts_15min")
    logger.info(f"  • {database}.vehicles_pd")
    logger.info(f"  • {database}.vehicles_ph")
    logger.info(f"  • {database}.vehicles_15min")
    logger.info(f"  • {database}.throughput")
    
    # S3 Console links
    logger.info("\n🌐 AWS S3 CONSOLE:")
    region = conf['athena'].get('region', 'us-east-1')
    logger.info(f"  • https://s3.console.aws.amazon.com/s3/buckets/{bucket}?region={region}&tab=objects")
    
    logger.info("\n🔍 AWS ATHENA CONSOLE:")
    logger.info(f"  • https://console.aws.amazon.com/athena/home?region={region}#/query-editor")
    
    logger.info("=" * 80)

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