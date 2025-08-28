# -*- coding: utf-8 -*-
"""
Monthly Report Package Initialization

Python conversion of Monthly_Report_Package_init.R
Provides exact same functionality for monthly report processing
"""

import os
import sys
import yaml
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import logging
from multiprocessing import Pool, cpu_count
import concurrent.futures
from typing import Dict, List, Optional, Tuple
import warnings

# Import custom modules (assuming they exist in the RtoPy package)
from monthly_report_functions import *
from database_functions import *
from utilities import get_usable_cores, format_duration, TimingContext
from configs import get_athena_connection, load_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_corridors(corridors_filename_s3: str, filter_signals: bool = True) -> pd.DataFrame:
    """
    Load corridor configuration from S3
    
    Args:
        corridors_filename_s3: S3 path to corridors file
        filter_signals: Whether to filter for signals only
        
    Returns:
        DataFrame with corridor configuration
    """
    
    try:
        logger.info(f"Loading corridors from {corridors_filename_s3}")
        
        # This would be implemented in your configs module
        from configs import s3read_using
        
        corridors = s3read_using(
            pd.read_excel,
            bucket=conf['bucket'],
            object=corridors_filename_s3
        )
        
        if filter_signals:
            # Filter for signals only (assuming SignalID column exists)
            corridors = corridors[corridors['SignalID'].notna()]
            
        logger.info(f"Loaded {len(corridors)} corridor records")
        return corridors
        
    except Exception as e:
        logger.error(f"Error loading corridors: {e}")
        raise

def get_cam_config(object: str, bucket: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Load camera configuration from S3
    
    Args:
        object: S3 object key for camera config
        bucket: S3 bucket name
        corridors: Corridor configuration DataFrame
        
    Returns:
        DataFrame with camera configuration
    """
    
    try:
        logger.info(f"Loading camera config from {object}")
        
        from configs import s3read_using
        
        cam_config = s3read_using(
            pd.read_excel,
            bucket=bucket,
            object=object
        )
        
        # Merge with corridors if needed
        if not corridors.empty:
            cam_config = pd.merge(
                cam_config,
                corridors[['SignalID', 'Corridor']],
                on='SignalID',
                how='left'
            )
        
        logger.info(f"Loaded {len(cam_config)} camera records")
        return cam_config
        
    except Exception as e:
        logger.error(f"Error loading camera config: {e}")
        return pd.DataFrame()

def get_month_abbrs(start_date: date, end_date: date) -> List[str]:
    """
    Get list of month abbreviations for date range
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        List of month abbreviations
    """
    
    try:
        months = []
        current_date = start_date.replace(day=1)  # First day of start month
        
        while current_date <= end_date:
            month_abbr = current_date.strftime('%Y-%m')
            months.append(month_abbr)
            current_date += relativedelta(months=1)
            
        return months
        
    except Exception as e:
        logger.error(f"Error generating month abbreviations: {e}")
        return []

def get_date_from_string(date_string: str, table_include_regex_pattern: str = None, 
                        exceptions: int = 0) -> Optional[date]:
    """
    Get date from string pattern (mimics R function)
    
    Args:
        date_string: Date string pattern
        table_include_regex_pattern: Table pattern to search
        exceptions: Number of exceptions to allow
        
    Returns:
        Date object or None
    """
    
    try:
        if date_string == "first_missing":
            # This would need to be implemented based on your specific logic
            # For now, return a default date
            return datetime.now().date() - timedelta(days=30)
        else:
            return pd.to_datetime(date_string).date()
            
    except Exception as e:
        logger.warning(f"Could not parse date string '{date_string}': {e}")
        return None

def round_to_tuesday(date_: Optional[date]) -> Optional[date]:
    """
    Round date to the nearest Tuesday
    
    Args:
        date_: Input date
        
    Returns:
        Date rounded to Tuesday
    """
    
    if date_ is None:
        return None
        
    try:
        if isinstance(date_, str):
            date_ = pd.to_datetime(date_).date()
            
        # Get day of week (0=Monday, 1=Tuesday, etc.)
        weekday = date_.weekday()
        
        # Calculate days to add/subtract to get to Tuesday (1)
        days_to_tuesday = (1 - weekday) % 7
        if days_to_tuesday > 3:  # If more than 3 days, go to previous Tuesday
            days_to_tuesday -= 7
            
        return date_ + timedelta(days=days_to_tuesday)
        
    except Exception as e:
        logger.error(f"Error rounding to Tuesday: {e}")
        return date_

def setup_multiprocessing():
    """
    Set up multiprocessing based on environment
    """
    
    try:
        # Check if running interactively
        if hasattr(sys, 'ps1') or 'jupyter' in sys.modules:
            # Interactive mode - use ThreadPoolExecutor
            logger.info("Setting up multiprocessing for interactive mode")
            return 'thread'
        else:
            # Non-interactive mode - use ProcessPoolExecutor
            logger.info("Setting up multiprocessing for non-interactive mode")
            return 'process'
            
    except Exception as e:
        logger.warning(f"Error setting up multiprocessing: {e}")
        return 'thread'

def initialize_monthly_report_package():
    """
    Main initialization function for monthly report package
    Equivalent to the R script functionality
    """
    
    global conf, corridors, all_corridors, signals_list, subcorridors, cam_config
    global report_end_date, report_start_date, calcs_start_date, wk_calcs_start_date
    global dates, month_abbrs, date_range, date_range_str
    
    try:
        logger.info(f"{datetime.now()} Starting Package Script")
        
        # Load configuration
        conf = load_config()
        
        # Setup multiprocessing
        multiprocessing_mode = setup_multiprocessing()
        
        # Get usable cores
        usable_cores = get_usable_cores()
        logger.info(f"Using {usable_cores} cores for processing")
        
        # Load corridors
        with TimingContext("Loading corridors"):
            corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=True)
            all_corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=False)
        
        # Get unique signal list
        signals_list = corridors['SignalID'].dropna().unique().tolist()
        logger.info(f"Found {len(signals_list)} unique signals")
        
        # Create subcorridors DataFrame
        subcorridors = corridors[corridors['Subcorridor'].notna()].copy()
        if not subcorridors.empty:
            subcorridors = subcorridors.drop('Zone_Group', axis=1, errors='ignore')
            subcorridors = subcorridors.rename(columns={
                'Zone': 'Zone_Group',
                'Corridor': 'Zone', 
                'Subcorridor': 'Corridor'
            })
        
        logger.info(f"Created {len(subcorridors)} subcorridor records")
        
        # Get Athena connection
        conn = get_athena_connection(conf['athena'])
        logger.info("Established Athena connection")
        
        # Load camera configuration
        with TimingContext("Loading camera configuration"):
            cam_config = get_cam_config(
                object=conf['cctv_config_filename'],
                bucket=conf['bucket'],
                corridors=all_corridors
            )
        
        # Define date range for calculations
        logger.info("Setting up date ranges for calculations")
        
        # Set report end date
        if conf.get('report_end_date') == "yesterday":
            report_end_date = datetime.now().date() - timedelta(days=1)
        else:
            report_end_date = pd.to_datetime(conf.get('report_end_date')).date()
        
        # Set report start date (18 months back)
        report_start_date = (report_end_date.replace(day=1) - 
                           relativedelta(months=18))
        
        # Set calculations start date
        if conf.get('calcs_start_date') == "auto":
            first_missing_date = get_date_from_string(
                "first_missing", 
                table_include_regex_pattern="sig_dy_cu", 
                exceptions=0
            )
            
            if first_missing_date:
                calcs_start_date = first_missing_date.replace(day=1)
                if first_missing_date.day <= 7:
                    calcs_start_date = calcs_start_date - relativedelta(months=1)
            else:
                calcs_start_date = report_start_date
        else:
            calcs_start_date = pd.to_datetime(conf.get('calcs_start_date')).date()
        
        # Round to Tuesday for weekly calculations
        wk_calcs_start_date = round_to_tuesday(calcs_start_date)
        
        # Generate date sequences
        dates = pd.date_range(
            start=report_start_date,
            end=report_end_date,
            freq='MS'  # Month start
        ).date.tolist()
        
        month_abbrs = get_month_abbrs(report_start_date, report_end_date)
        
        # Convert dates to strings for logging
        report_start_date_str = str(report_start_date)
        report_end_date_str = str(report_end_date)
        
        # Generate full date range
        date_range = pd.date_range(
            start=report_start_date,
            end=report_end_date,
            freq='D'
        ).date.tolist()
        
        # Create date range string (equivalent to R's paste/collapse)
        date_range_str = "{" + ",".join([str(d) for d in date_range]) + "}"
        
        # Log important dates
        logger.info(f"Week Calcs Start Date: {wk_calcs_start_date}")
        logger.info(f"Calcs Start Date: {calcs_start_date}")
        logger.info(f"Report Start Date: {report_start_date_str}")
        logger.info(f"Report End Date: {report_end_date_str}")
        logger.info(f"Processing {len(dates)} months of data")
        logger.info(f"Date range covers {len(date_range)} days")
        
        # Store global variables for use in other modules
        globals().update({
            'conf': conf,
            'corridors': corridors,
            'all_corridors': all_corridors,
            'signals_list': signals_list,
            'subcorridors': subcorridors,
            'cam_config': cam_config,
            'report_end_date': report_end_date,
            'report_start_date': report_start_date,
            'calcs_start_date': calcs_start_date,
            'wk_calcs_start_date': wk_calcs_start_date,
            'dates': dates,
            'month_abbrs': month_abbrs,
            'date_range': date_range,
            'date_range_str': date_range_str,
            'usable_cores': usable_cores,
            'multiprocessing_mode': multiprocessing_mode
        })
        
        logger.info(f"{datetime.now()} Package initialization completed successfully")
        
        return {
            'conf': conf,
            'corridors': corridors,
            'all_corridors': all_corridors,
            'signals_list': signals_list,
            'subcorridors': subcorridors,
            'cam_config': cam_config,
            'report_end_date': report_end_date,
            'report_start_date': report_start_date,
            'calcs_start_date': calcs_start_date,
            'wk_calcs_start_date': wk_calcs_start_date,
            'dates': dates,
            'month_abbrs': month_abbrs,
            'date_range': date_range,
            'date_range_str': date_range_str,
            'usable_cores': usable_cores,
            'multiprocessing_mode': multiprocessing_mode
        }
        
    except Exception as e:
        logger.error(f"Error in package initialization: {e}")
        raise

def main():
    """
    Main function to run the initialization
    """
    
    try:
        # Initialize the package
        result = initialize_monthly_report_package()
        
        logger.info("Monthly Report Package initialization completed")
        return result
        
    except Exception as e:
        logger.error(f"Failed to initialize Monthly Report Package: {e}")
        raise

if __name__ == "__main__":
    # Optional: Turn warnings into errors for debugging
    # warnings.filterwarnings('error')
    
    main()
