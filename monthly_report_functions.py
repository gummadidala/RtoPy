"""
Main functions and configurations for Monthly Report processing
Converted from Monthly_Report_Functions.R
"""

import os
import sys
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
import logging
from datetime import datetime, timedelta
import yaml
import boto3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
from pathlib import Path
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import other modules (assuming they exist)
try:
    from utilities import *
    from s3_parquet_io import *
    from configs import *
    from counts import *
    from metrics import *
    from map import *
    from teams import *
    from aggregations import *
    from database_functions import *
except ImportError as e:
    logger.warning(f"Could not import module: {e}")

# Color constants (from Colorbrewer Paired Palette)
LIGHT_BLUE = "#A6CEE3"
BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A"
GREEN = "#33A02C"
LIGHT_RED = "#FB9A99"
RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"
ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"
PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99"
BROWN = "#B15928"

RED2 = "#e41a1c"
GDOT_BLUE = "#256194"

BLACK = "#000000"
WHITE = "#FFFFFF"
GRAY = "#D0D0D0"
DARK_GRAY = "#7A7A7A"
DARK_DARK_GRAY = "#494949"

# Day of week constants
SUN, MON, TUE, WED, THU, FRI, SAT = 1, 2, 3, 4, 5, 6, 7

def load_configuration() -> Dict[str, Any]:
    """
    Load configuration from YAML files
    
    Returns:
        Configuration dictionary
    """
    
    try:
        # Load main configuration
        with open("Monthly_Report.yaml", 'r') as file:
            conf = yaml.safe_load(file)
        
        # Load AWS configuration
        aws_conf = {}
        if os.path.exists("Monthly_Report_AWS.yaml"):
            with open("Monthly_Report_AWS.yaml", 'r') as file:
                aws_conf = yaml.safe_load(file)
            
            # Set environment variables
            os.environ['AWS_ACCESS_KEY_ID'] = aws_conf['AWS_ACCESS_KEY_ID']
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_conf['AWS_SECRET_ACCESS_KEY']
            os.environ['AWS_DEFAULT_REGION'] = aws_conf['AWS_DEFAULT_REGION']
            
            # Update Athena configuration
            if 'athena' in conf:
                conf['athena']['uid'] = aws_conf['AWS_ACCESS_KEY_ID']
                conf['athena']['pwd'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        
        return conf, aws_conf
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return {}, {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return {}, {}

def get_corridors(corridors_filename: str, filter_signals: bool = True) -> pd.DataFrame:
    """
    Read and process corridors configuration
    Exact equivalent of R's get_corridors function
    
    Args:
        corridors_filename: Filename of corridors configuration
        filter_signals: Whether to filter for active signals only
    
    Returns:
        DataFrame with corridor configuration
    """
    
    try:
        # Check if it's an S3 path or local file
        if corridors_filename.startswith('s3://'):
            # Parse S3 path
            parts = corridors_filename.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1]
            
            # Download from S3 and read Excel file
            s3_client = boto3.client('s3')
            
            # Download to local temp file
            local_file = f"temp_{os.path.basename(key)}"
            s3_client.download_file(bucket, key, local_file)
            
            # Read Excel file
            corridors = pd.read_excel(local_file)
            
            # Clean up temp file
            os.remove(local_file)
            
        else:
            # Read local file
            corridors = pd.read_excel(corridors_filename)
        
        # Filter for active signals if requested (equivalent to R logic)
        if filter_signals:
            if 'Active' in corridors.columns:
                corridors = corridors[corridors['Active'] == True].copy()
            elif 'active' in corridors.columns:
                corridors = corridors[corridors['active'] == True].copy()
        
        # Ensure SignalID is string (equivalent to R's factor conversion)
        if 'SignalID' in corridors.columns:
            corridors['SignalID'] = corridors['SignalID'].astype(str)
        elif 'signalid' in corridors.columns:
            corridors['signalid'] = corridors['signalid'].astype(str)
        
        return corridors
        
    except Exception as e:
        logger.error(f"Error reading corridors: {e}")
        return pd.DataFrame()

def s3write_using(func, bucket: str, object: str, *args, **kwargs):
    """
    Write data to S3 using a specified function
    Equivalent to R's s3write_using function
    
    Args:
        func: Function to use for writing (e.g., df.to_parquet)
        bucket: S3 bucket name
        object: S3 object key
        *args: Arguments for the function
        **kwargs: Keyword arguments for the function
    """
    try:
        # Create temp local file
        local_file = f"temp_{os.path.basename(object)}"
        
        # Apply the function to save locally
        func(local_file, *args, **kwargs)
        
        # Upload to S3
        s3_client = boto3.client('s3')
        s3_client.upload_file(local_file, bucket, object)
        
        # Clean up local file
        if os.path.exists(local_file):
            os.remove(local_file)
            
        logger.info(f"Successfully uploaded {object} to bucket {bucket}")
        
    except Exception as e:
        logger.error(f"Error writing to S3: {e}")
        raise

def get_signalids_from_s3(date: str, bucket: str) -> List[str]:
    """
    Get list of signal IDs from S3 for a specific date
    Equivalent to R's get_signalids_from_s3 function
    
    Args:
        date: Date string (YYYY-MM-DD)
        bucket: S3 bucket name
    
    Returns:
        List of signal IDs
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # List objects for the date - check multiple possible prefixes
        prefixes_to_check = [
            f"mark/counts_1hr/date={date}/",
            f"counts_1hr/date={date}/",
            f"atspm/date={date}/",
            f"detection_events/date={date}/"
        ]
        
        signal_ids = set()
        
        for prefix in prefixes_to_check:
            try:
                response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        # Extract signal ID from key path
                        # Look for patterns like SignalID=1234 or signal_1234
                        signal_match = re.search(r'SignalID=(\d+)|signal[_-](\d+)|(\d{4,})', key)
                        if signal_match:
                            signal_id = signal_match.group(1) or signal_match.group(2) or signal_match.group(3)
                            if signal_id:
                                signal_ids.add(signal_id)
                
                # If we found signals, break (don't need to check other prefixes)
                if signal_ids:
                    break
                    
            except Exception as e:
                logger.debug(f"Could not check prefix {prefix}: {e}")
                continue
        
        result = list(signal_ids)
        logger.debug(f"Found {len(result)} signals for {date}")
        return result
        
    except Exception as e:
        logger.error(f"Error getting signal IDs from S3 for {date}: {e}")
        return []

# Keep all the existing functions from your original file...
# (I'm keeping the rest of your original monthly_report_functions.py as is)

def setup_environment():
    """
    Setup environment based on operating system
    """
    
    # Set timezone
    os.environ['TZ'] = 'America/New_York'
    
    # Determine system-specific paths
    if os.name == 'nt':  # Windows
        home_path = os.path.dirname(os.path.expanduser("~"))
        python_path = os.path.join(home_path, "Anaconda3", "python.exe")
    else:  # Linux/Unix
        home_path = os.path.expanduser("~")
        python_path = os.path.join(home_path, "miniconda3", "bin", "python")
    
    return {"home_path": home_path, "python_path": python_path}

def get_usable_cores() -> int:
    """
    Get number of usable CPU cores for parallel processing
    
    Returns:
        Number of cores to use
    """
    
    total_cores = multiprocessing.cpu_count()
    
    # Use all cores but leave one available for system
    usable = max(1, total_cores - 1)
    
    # Cap at reasonable number to avoid memory issues
    return min(usable, 8)

def get_date_from_string(
    x,
    s3bucket=None,
    s3prefix=None,
    table_include_regex_pattern="_dy_",
    table_exclude_regex_pattern="_outstand|_report|_resolv|_task|_tpri|_tsou|_tsub|_ttyp|_kabco|_maint|_ops|_safety|_alert|_udc|_summ",
):
    """Exact conversion of R's get_date_from_string function"""
    if type(x) == str:
        re_da = re.compile(r"\d+(?= *days ago)")
        if x == "today":
            x = datetime.today().strftime("%Y-%m-%d")
        elif x == "yesterday":
            x = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        elif re_da.search(x):
            d = int(re_da.search(x).group())
            x = (datetime.today() - timedelta(days=d)).strftime("%Y-%m-%d")
        elif x == "first_missing":
            if s3bucket is not None and s3prefix is not None:
                s3 = boto3.resource("s3")
                all_dates = [
                    re.search(r"(?<=date\=)(\d+-\d+-\d+)", obj.key)
                    for obj in s3.Bucket(s3bucket).objects.filter(Prefix=s3prefix)
                ]
                all_dates = [date_.group() for date_ in all_dates if date_ is not None]
                first_missing = datetime.strptime(max(all_dates), "%Y-%m-%d") + timedelta(days=1)
                first_missing = min(first_missing, datetime.today() - timedelta(days=1))
                x = first_missing.strftime("%Y-%m-%d")
            else:
                raise Exception("Must include arguments for s3bucket and s3prefix")
    else:
        x = x.strftime("%Y-%m-%d")
    return x

def get_month_abbrs(start_date: str, end_date: str) -> List[str]:
    """
    Get list of month abbreviations (YYYY-MM) between start and end dates
    
    Args:
        start_date: Start date string
        end_date: End date string
    
    Returns:
        List of month abbreviations
    """
    
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    
    # Get first day of start month and last day of end month
    start_month = start.replace(day=1)
    end_month = end.replace(day=1)
    
    # Generate month range
    month_range = pd.date_range(start=start_month, end=end_month, freq='MS')
    
    return [date.strftime('%Y-%m') for date in month_range]

def get_latest_det_config(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Get latest detector configuration
    
    Args:
        config: Configuration dictionary
    
    Returns:
        DataFrame with detector configuration
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # List detector config files
        response = s3_client.list_objects_v2(
            Bucket=config['bucket'],
            Prefix='ATSPM_Det_Config_Good'
        )
        
        if 'Contents' not in response:
            logger.warning("No detector config files found")
            return pd.DataFrame()
        
        # Get most recent file
        latest_key = max([obj['Key'] for obj in response['Contents']])
        
        # Read the file
        if latest_key.endswith('.parquet'):
            # Download and read parquet
            local_file = f"temp_{os.path.basename(latest_key)}"
            s3_client.download_file(config['bucket'], latest_key, local_file)
            det_config = pd.read_parquet(local_file)
            os.remove(local_file)
        else:
            logger.warning(f"Unsupported file format: {latest_key}")
            return pd.DataFrame()
        
        return det_config
        
    except Exception as e:
        logger.error(f"Error getting detector config: {e}")
        return pd.DataFrame()

# Keep all other functions from your original monthly_report_functions.py unchanged...
# [Rest of the functions remain as in your original file]

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

# Global configuration variables (loaded at module import)
try:
    conf, aws_conf = load_configuration()
    AM_PEAK_HOURS, PM_PEAK_HOURS = get_peak_hours(conf) if conf else ([7, 8, 9], [16, 17, 18])
    usable_cores = get_usable_cores()
    
    # Setup environment
    env_config = setup_environment()
    
    logger.info("Monthly Report Functions initialized successfully")
    
except Exception as e:
    logger.error(f"Error initializing Monthly Report Functions: {e}")
    conf = {}
    aws_conf = {}
    AM_PEAK_HOURS = [7, 8, 9]
    PM_PEAK_HOURS = [16, 17, 18]
    usable_cores = 1

