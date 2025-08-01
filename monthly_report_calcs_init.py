"""
Monthly Report Calculations Initialization
Converted from Monthly_Report_Calcs_init.R
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import yaml
import boto3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
from functools import partial

# Import custom modules
from monthly_report_functions import *
from database_functions import *
from s3_parquet_io import *

# Setup logging
setup_logging("INFO")
logger = logging.getLogger(__name__)

def main():
    """Main initialization function"""
    
    print(f"\n\n{datetime.now()} Starting Calcs Script")
    
    # Load configuration
    conf, aws_conf = load_configuration()

    if not conf:
        logger.error("Failed to load configuration")
        sys.exit(1)
    
    # Setup parallel processing
    usable_cores = get_usable_cores()
    logger.info(f"Using {usable_cores} cores for parallel processing")
    
    # Define date range for calculations
    
    start_date = get_date_from_string(
        conf['start_date'], 
        s3bucket=conf['bucket'], 
        s3prefix="mark/split_failures"
    )
    end_date = get_date_from_string(conf['end_date'])
    
    # Manual overrides (uncomment if needed)
    # start_date = "2020-01-04"
    # end_date = "2020-01-04"
    
    # Validate date range
    if not validate_date_range(start_date, end_date):
        logger.error("Invalid date range")
        sys.exit(1)
    
    logger.info(f"Processing date range: {start_date} to {end_date}")
    
    month_abbrs = get_month_abbrs(start_date, end_date)
    logger.info(f"Month abbreviations: {month_abbrs}")
    
    # Get corridors configuration
    logger.info("Loading corridors configuration...")
    update_corridors_files(conf)
    
    # Get signals list
    logger.info("Getting signals list...")
    signals_list = get_signals_list(start_date, end_date, usable_cores, conf)
    logger.info(f"Found {len(signals_list)} unique signals")
    
    # Update detector configuration
    logger.info("Updating detector configuration...")
    update_detector_config(conf)
    
    # Setup Athena partitions
    logger.info("Setting up Athena partitions...")
    setup_athena_partitions(conf, start_date, end_date)
    
    logger.info("Initialization completed successfully")

    return {
        'conf': conf,
        'start_date': start_date,
        'end_date': end_date,
        'month_abbrs': month_abbrs,
        'signals_list': signals_list,
        'usable_cores': usable_cores
    }

def update_corridors_files(conf: dict):
    """
    Update corridors files from Excel and save in multiple formats
    
    Args:
        conf: Configuration dictionary
    """
    
    try:
        # Get corridors with filtered signals
        corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=True)
        
        # Save as feather
        feather_filename = conf['corridors_filename_s3'].replace('.xlsx', '.feather').replace('.xls', '.feather')
        corridors.to_feather(feather_filename)
        
        # Save as parquet (more compatible than feather)
        parquet_filename = conf['corridors_filename_s3'].replace('.xlsx', '.parquet').replace('.xls', '.parquet')
        corridors.to_parquet(parquet_filename)
        
        # Get all corridors (including inactive signals)
        all_corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=False)
        
        # Save all corridors
        all_feather_filename = "all_" + feather_filename
        all_corridors.to_feather(all_feather_filename)
        
        all_parquet_filename = "all_" + parquet_filename
        all_corridors.to_parquet(all_parquet_filename)
        
        # Upload to S3
        s3write_using(
            corridors.to_feather,
            bucket=conf['bucket'],
            object=feather_filename
        )
        
        s3write_using(
            all_corridors.to_feather,
            bucket=conf['bucket'],
            object=all_feather_filename
        )
        
        logger.info("Successfully updated corridors files")
        
    except Exception as e:
        logger.error(f"Error updating corridors files: {e}")
        raise

from functools import partial

def get_signals_list(start_date: str, end_date: str, usable_cores: int, conf: dict) -> list:
    """
    Get list of unique signal IDs for the date range
    
    Args:
        start_date: Start date string
        end_date: End date string
        usable_cores: Number of cores for parallel processing
        conf: Configuration dictionary
    
    Returns:
        List of unique signal IDs
    """
    
    try:
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
        
        # Create partial function with bucket parameter
        get_signalids_with_bucket = partial(get_signalids_from_s3, bucket=conf['bucket'])
        
        # Process dates in parallel
        with ProcessPoolExecutor(max_workers=usable_cores) as executor:
            results = list(executor.map(get_signalids_with_bucket, date_strings))
        
        # Flatten and get unique signals
        signals_list = []
        for result in results:
            if result:
                signals_list.extend(result)
        
        unique_signals = list(set(signals_list))
        unique_signals.sort()
        
        return unique_signals
        
    except Exception as e:
        logger.error(f"Error getting signals list: {e}")
        return []

def update_detector_config(conf: dict):
    """
    Update detector configuration and save to S3
    
    Args:
        conf: Configuration dictionary
    """
    
    try:
        # Get latest detector config
        det_config = get_latest_det_config(conf)
        
        if det_config.empty:
            logger.warning("No detector configuration found")
            return
        
        # Save as parquet for better compatibility with Python
        parquet_filename = "ATSPM_Det_Config_Good_Latest.parquet"
        det_config.to_parquet(parquet_filename)
        
        # Upload to S3
        s3write_using(
            det_config.to_parquet,
            bucket=conf['bucket'],
            object=parquet_filename
        )
        
        logger.info("Successfully updated detector configuration")
        
    except Exception as e:
        logger.error(f"Error updating detector configuration: {e}")

def setup_athena_partitions(conf: dict, start_date: str, end_date: str):
    """
    Setup Athena partitions for the date range
    
    Args:
        conf: Configuration dictionary
        start_date: Start date string
        end_date: End date string
    """
    
    try:
        # Connect to Athena
        athena_conn = get_athena_connection(conf['athena'])
        
        # Get existing partitions
        partitions_query = f"SHOW PARTITIONS {conf['athena']['atspm_table']}"
        partitions_df = pd.read_sql(partitions_query, athena_conn)
        
        if 'partition' in partitions_df.columns:
            existing_partitions = [
                partition.split('=')[1] 
                for partition in partitions_df['partition'].tolist()
            ]
        else:
            existing_partitions = []
        
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
        
        # Find missing partitions
        missing_partitions = [
            date for date in date_strings 
            if date not in existing_partitions
        ]
        
        if len(missing_partitions) > 10:
            # Too many missing partitions, use MSCK REPAIR
            logger.info("Adding missing partitions with MSCK REPAIR TABLE")
            repair_query = f"MSCK REPAIR TABLE {conf['athena']['atspm_table']}"
            athena_conn.execute(repair_query)
            
        elif missing_partitions:
            # Add individual partitions
            logger.info(f"Adding {len(missing_partitions)} missing partitions")
            for date_str in missing_partitions:
                add_athena_partition(
                    conf['athena'], 
                    conf['bucket'], 
                    conf['athena']['atspm_table'], 
                    date_str
                )
        else:
            logger.info("All partitions already exist")
        
        athena_conn.close()
        
    except Exception as e:
        logger.error(f"Error setting up Athena partitions: {e}")

def validate_initialization(conf: dict, signals_list: list) -> bool:
    """
    Validate that initialization completed successfully
    
    Args:
        conf: Configuration dictionary
        signals_list: List of signal IDs
    
    Returns:
        Boolean indicating if validation passed
    """
    
    validation_checks = []
    
    # Check S3 connectivity
    s3_valid = validate_s3_connectivity(conf['bucket'])
    validation_checks.append(('S3 Connectivity', s3_valid))
    
    # Check signals list
    signals_valid = len(signals_list) > 0
    validation_checks.append(('Signals List', signals_valid))
    
    # Check corridors file exists
    corridors_exist = os.path.exists('corridors.feather') or os.path.exists('corridors.parquet')
    validation_checks.append(('Corridors File', corridors_exist))
    
    # Log validation results
    logger.info("Validation Results:")
    all_valid = True
    for check_name, result in validation_checks:
        status = "PASS" if result else "FAIL"
        logger.info(f"  {check_name}: {status}")
        if not result:
            all_valid = False
    
    return all_valid

def create_initialization_summary(start_time: datetime, 
                                end_time: datetime,
                                conf: dict,
                                signals_list: list) -> str:
    """
    Create summary of initialization process
    
    Args:
        start_time: Initialization start time
        end_time: Initialization end time
        conf: Configuration dictionary
        signals_list: List of signal IDs
    
    Returns:
        Summary string
    """
    
    duration = end_time - start_time
    
    summary = f"""
    Initialization Summary
    =====================
    Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
    End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
    Duration: {duration}
    
    Configuration:
    - Bucket: {conf.get('bucket', 'N/A')}
    - Start Date: {conf.get('start_date', 'N/A')}
    - End Date: {conf.get('end_date', 'N/A')}
    
    Results:
    - Signals Found: {len(signals_list)}
    - Usable Cores: {get_usable_cores()}
    
    """
    
    return summary

if __name__ == "__main__":
    """Run initialization if called directly"""
    
    start_time = datetime.now()
    
    try:
        # Run main initialization
        init_results = main()
        
        # Validate results
        validation_passed = validate_initialization(
            init_results['conf'], 
            init_results['signals_list']
        )
        
        end_time = datetime.now()
        
        # Create summary
        summary = create_initialization_summary(
            start_time, 
            end_time,
            init_results['conf'],
            init_results['signals_list']
        )
        
        print(summary)
        
        if validation_passed:
            logger.info("Initialization completed successfully")
            
            # Create checkpoint
            create_checkpoint_file(
                'initialization_complete',
                {
                    'timestamp': end_time.isoformat(),
                    'start_date': init_results['start_date'],
                    'end_date': init_results['end_date'],
                    'signals_count': len(init_results['signals_list']),
                    'validation_passed': validation_passed
                },
                init_results['conf']['bucket']
            )
            
            sys.exit(0)
        else:
            logger.error("Initialization validation failed")
            sys.exit(1)
            
    except Exception as e:
        end_time = datetime.now()
        logger.error(f"Initialization failed: {e}")
        
        # Send error notification if configured
        if conf.get('notifications'):
            send_notification(
                f"Monthly Report initialization failed: {e}",
                "error",
                conf['notifications'].get('email'),
                conf['notifications'].get('slack')
            )
        
        sys.exit(1)
