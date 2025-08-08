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

def load_configuration() -> tuple:
    """
    Load configuration from YAML files
    
    Returns:
        Tuple of (conf, aws_conf) configuration dictionaries
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

def get_peak_hours(config: Dict[str, Any]) -> tuple:
    """
    Get AM and PM peak hours from configuration
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Tuple of (AM_PEAK_HOURS, PM_PEAK_HOURS)
    """
    
    am_peak = config.get('AM_PEAK_HOURS', [7, 8, 9])
    pm_peak = config.get('PM_PEAK_HOURS', [16, 17, 18])
    
    return am_peak, pm_peak

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

def setup_proxy():
    """
    Setup proxy configuration for specific environments
    """
    
    import socket
    hostname = socket.gethostname()
    
    if hostname in ["GOTO3213490", "Lenny"]:  # The SAM or Lenny
        # Setup proxy configuration
        proxy_config = {
            'http': 'http://gdot-enterprise:8080',
            'https': 'http://gdot-enterprise:8080'
        }
        
        # Set proxy environment variables
        username = os.getenv("GDOT_USERNAME")
        password = os.getenv("GDOT_PASSWORD")
        
        if username and password:
            proxy_auth = f"http://{username}:{password}@gdot-enterprise:8080"
            proxy_config = {
                'http': proxy_auth,
                'https': proxy_auth
            }
        
        return proxy_config
    
    return None

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

def get_latest_det_config_notinscope(config: Dict[str, Any]) -> pd.DataFrame:
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
        elif latest_key.endswith('.qs'):
            # For .qs files, we'll need to handle them differently
            # Since Python doesn't have native .qs support, convert to parquet
            logger.warning(f"QS file format not directly supported: {latest_key}")
            return pd.DataFrame()
        else:
            logger.warning(f"Unsupported file format: {latest_key}")
            return pd.DataFrame()
        
        return det_config
        
    except Exception as e:
        logger.error(f"Error getting detector config: {e}")
        return pd.DataFrame()

def write_signal_details(date_str: str, config: Dict[str, Any], signals_list: List[str]):
    """
    Write signal details for a specific date
    
    Args:
        date_str: Date string
        config: Configuration dictionary
        signals_list: List of signal IDs
    """
    
    try:
        # Create signal details DataFrame
        signal_details = pd.DataFrame({
            'SignalID': signals_list,
            'Date': date_str,
            'Status': 'Active'
        })
        
        # Upload to S3 (this would need the s3_upload_parquet_date_split function)
        # For now, just log
        logger.info(f"Would write signal details for {date_str}: {len(signals_list)} signals")
        
    except Exception as e:
        logger.error(f"Error writing signal details: {e}")

def parallel_process_dates(date_range: List[str], 
                          process_function,
                          max_workers: Optional[int] = None,
                          **kwargs) -> List[Any]:
    """
    Process dates in parallel using ThreadPoolExecutor
    
    Args:
        date_range: List of dates to process
        process_function: Function to apply to each date
        max_workers: Maximum number of worker threads
        **kwargs: Additional arguments for the process function
    
    Returns:
        List of results
    """
    
    if max_workers is None:
        max_workers = get_usable_cores()
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_date = {
            executor.submit(process_function, date, **kwargs): date 
            for date in date_range
        }
        
        # Collect results
        for future in future_to_date:
            date = future_to_date[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed processing for {date}")
            except Exception as e:
                logger.error(f"Error processing {date}: {e}")
                results.append(None)
    
    return results

def keep_trying(func, n_tries: int = 3, timeout: int = 30, *args, **kwargs):
    """
    Retry function execution with exponential backoff
    
    Args:
        func: Function to execute
        n_tries: Number of attempts
        timeout: Timeout in seconds
        *args: Function arguments
        **kwargs: Function keyword arguments
    
    Returns:
        Function result
    """
    
    import time
    
    for attempt in range(n_tries):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt == n_tries - 1:
                logger.error(f"Function failed after {n_tries} attempts: {e}")
                raise
            else:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time} seconds: {e}")
                time.sleep(wait_time)

def cleanup_temp_directories(*directories: str):
    """
    Clean up temporary directories
    
    Args:
        *directories: Directory paths to clean up
    """
    
    import shutil
    
    for directory in directories:
        if os.path.exists(directory):
            try:
                shutil.rmtree(directory)
                logger.info(f"Cleaned up directory: {directory}")
            except Exception as e:
                logger.warning(f"Error cleaning up {directory}: {e}")

def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate that date range is reasonable
    
    Args:
        start_date: Start date string
        end_date: End date string
    
    Returns:
        Boolean indicating if range is valid
    """
    
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Check that start is before end
        if start > end:
            logger.error("Start date is after end date")
            return False
        
        # Check that range is not too large (e.g., more than 1 year)
        if (end - start).days > 365:
            logger.warning("Date range is longer than 1 year")
        
        # Check that dates are not in the future
        if end > pd.Timestamp.now():
            logger.warning("End date is in the future")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating date range: {e}")
        return False

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

def create_output_directories(*directories: str):
    """
    Create output directories if they don't exist
    
    Args:
        *directories: Directory paths to create
    """
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Error creating directory {directory}: {e}")

def monitor_system_resources():
    """
    Monitor system resources (memory, CPU) and log warnings if high usage
    """
    
    try:
        import psutil
        
        # Get memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Log warnings if usage is high
        if memory_percent > 80:
            logger.warning(f"High memory usage: {memory_percent:.1f}%")
        
        if cpu_percent > 80:
            logger.warning(f"High CPU usage: {cpu_percent:.1f}%")
        
        logger.info(f"System resources - Memory: {memory_percent:.1f}%, CPU: {cpu_percent:.1f}%")
        
    except ImportError:
        logger.debug("psutil not available for resource monitoring")
    except Exception as e:
        logger.debug(f"Error monitoring resources: {e}")

def get_s3_object_size(bucket: str, key: str) -> int:
    """
    Get size of S3 object in bytes
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
    
    Returns:
        Size in bytes, or 0 if object doesn't exist
    """
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.debug(f"Error getting S3 object size: {e}")
        return 0

def compress_and_upload_logs(log_directory: str, bucket: str, prefix: str = "logs/"):
    """
    Compress log files and upload to S3
    
    Args:
        log_directory: Directory containing log files
        bucket: S3 bucket name
        prefix: S3 prefix for logs
    """
    
    try:
        import gzip
        import shutil
        
        log_files = [f for f in os.listdir(log_directory) if f.endswith('.log')]
        
        for log_file in log_files:
            log_path = os.path.join(log_directory, log_file)
            compressed_path = f"{log_path}.gz"
            
            # Compress the log file
            with open(log_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Upload to S3
            s3_key = f"{prefix}{log_file}.gz"
            s3_client = boto3.client('s3')
            s3_client.upload_file(compressed_path, bucket, s3_key)
            
            # Clean up local compressed file
            os.remove(compressed_path)
            
            logger.info(f"Uploaded compressed log: {s3_key}")
            
    except Exception as e:
        logger.error(f"Error compressing and uploading logs: {e}")

def validate_s3_connectivity(bucket: str) -> bool:
    """
    Validate S3 connectivity and permissions
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Boolean indicating if S3 is accessible
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # Try to list objects (limited)
        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        
        logger.info(f"S3 connectivity validated for bucket: {bucket}")
        return True
        
    except Exception as e:
        logger.error(f"S3 connectivity failed for bucket {bucket}: {e}")
        return False

def create_checkpoint_file(checkpoint_name: str, data: Dict[str, Any], bucket: str):
    """
    Create a checkpoint file in S3 for resuming processing
    
    Args:
        checkpoint_name: Name of the checkpoint
        data: Data to store in checkpoint
        bucket: S3 bucket name
    """
    
    try:
        import json
        
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'checkpoint_name': checkpoint_name,
            'data': data
        }
        
        # Upload to S3
        s3_key = f"checkpoints/{checkpoint_name}.json"
        s3_client = boto3.client('s3')
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(checkpoint_data, indent=2)
        )
        
        logger.info(f"Created checkpoint: {checkpoint_name}")
        
    except Exception as e:
        logger.error(f"Error creating checkpoint: {e}")

def load_checkpoint_file(checkpoint_name: str, bucket: str) -> Optional[Dict[str, Any]]:
    """
    Load a checkpoint file from S3
    
    Args:
        checkpoint_name: Name of the checkpoint
        bucket: S3 bucket name
    
    Returns:
        Checkpoint data or None if not found
    """
    
    try:
        import json
        
        s3_key = f"checkpoints/{checkpoint_name}.json"
        s3_client = boto3.client('s3')
        
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        checkpoint_data = json.loads(response['Body'].read())
        
        logger.info(f"Loaded checkpoint: {checkpoint_name}")
        return checkpoint_data['data']
        
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"Checkpoint not found: {checkpoint_name}")
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {e}")
        return None
    
def send_notification(message: str, 
                     notification_type: str = "info",
                     email_config: Optional[Dict[str, str]] = None,
                     slack_config: Optional[Dict[str, str]] = None):
    """
    Send notification via email or Slack
    
    Args:
        message: Notification message
        notification_type: Type of notification (info, warning, error)
        email_config: Email configuration
        slack_config: Slack configuration
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted_message = f"[{timestamp}] {notification_type.upper()}: {message}"
    
    # Send email notification
    if email_config:
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from']
            msg['To'] = email_config['to']
            msg['Subject'] = f"Monthly Report {notification_type.title()}"
            
            msg.attach(MIMEText(formatted_message, 'plain'))
            
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            if email_config.get('use_tls'):
                server.starttls()
            if email_config.get('username'):
                server.login(email_config['username'], email_config['password'])
            
            text = msg.as_string()
            server.sendmail(email_config['from'], email_config['to'], text)
            server.quit()
            
            logger.info("Email notification sent")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {e}")
    
    # Send Slack notification
    if slack_config:
        try:
            import requests
            
            payload = {
                'text': formatted_message,
                'username': slack_config.get('username', 'Monthly Report Bot'),
                'icon_emoji': slack_config.get('icon', ':chart_with_upwards_trend:')
            }
            
            response = requests.post(slack_config['webhook_url'], json=payload)
            response.raise_for_status()
            
            logger.info("Slack notification sent")
            
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

def get_latest_det_config(config: Dict[str, Any]) -> pd.DataFrame:
    """
    Get latest detector configuration - fixed version
    
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
        logger.info(f"Found latest detector config: {latest_key}")
        
        # Read the file based on extension
        if latest_key.endswith('.parquet'):
            # Download and read parquet
            local_file = f"temp_{os.path.basename(latest_key)}"
            s3_client.download_file(config['bucket'], latest_key, local_file)
            det_config = pd.read_parquet(local_file)
            os.remove(local_file)
        elif latest_key.endswith('.qs'):
            # For .qs files, we'll need to handle them differently
            # Since Python doesn't have native .qs support, convert to parquet
            logger.warning(f"QS file format not directly supported: {latest_key}")
            return pd.DataFrame()
        elif latest_key.endswith('.feather'):
            # Handle feather files
            local_file = f"temp_{os.path.basename(latest_key)}"
            s3_client.download_file(config['bucket'], latest_key, local_file)
            det_config = pd.read_feather(local_file)
            os.remove(local_file)
        else:
            logger.warning(f"Unsupported file format: {latest_key}")
            return pd.DataFrame()
        
        logger.info(f"Successfully loaded detector config with {len(det_config)} records")
        return det_config
        
    except Exception as e:
        logger.error(f"Error getting detector config: {e}")
        return pd.DataFrame()


def write_signal_details(date_str: str, config: Dict[str, Any], signals_list: List[str]):
    """
    Write signal details for a specific date
    
    Args:
        date_str: Date string
        config: Configuration dictionary
        signals_list: List of signal IDs
    """
    
    try:
        # Create signal details DataFrame
        signal_details = pd.DataFrame({
            'SignalID': signals_list,
            'Date': date_str,
            'Status': 'Active'
        })
        
        # Upload to S3 (this would need the s3_upload_parquet_date_split function)
        # For now, just log
        logger.info(f"Would write signal details for {date_str}: {len(signals_list)} signals")
        
    except Exception as e:
        logger.error(f"Error writing signal details: {e}")

def parallel_process_dates(date_range: List[str], 
                          process_function,
                          max_workers: Optional[int] = None,
                          **kwargs) -> List[Any]:
    """
    Process dates in parallel using ThreadPoolExecutor
    
    Args:
        date_range: List of dates to process
        process_function: Function to apply to each date
        max_workers: Maximum number of worker threads
        **kwargs: Additional arguments for the process function
    
    Returns:
        List of results
    """
    
    if max_workers is None:
        max_workers = get_usable_cores()
    
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_date = {
            executor.submit(process_function, date, **kwargs): date 
            for date in date_range
        }
        
        # Collect results
        for future in future_to_date:
            date = future_to_date[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed processing for {date}")
            except Exception as e:
                logger.error(f"Error processing {date}: {e}")
                results.append(None)
    
    return results

def keep_trying(func, n_tries: int = 3, timeout: int = 30, *args, **kwargs):
    """
    Retry function execution with exponential backoff
    
    Args:
        func: Function to execute
        n_tries: Number of attempts
        timeout: Timeout in seconds
        *args: Function arguments
        **kwargs: Function keyword arguments
    
    Returns:
        Function result
    """
    
    import time
    
    for attempt in range(n_tries):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt == n_tries - 1:
                logger.error(f"Function failed after {n_tries} attempts: {e}")
                raise
            else:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"Attempt {attempt + 1} failed, retrying in {wait_time} seconds: {e}")
                time.sleep(wait_time)

def cleanup_temp_directories(*directories: str):
    """
    Clean up temporary directories
    
    Args:
        *directories: Directory paths to clean up
    """
    
    import shutil
    
    for directory in directories:
        if os.path.exists(directory):
            try:
                shutil.rmtree(directory)
                logger.info(f"Cleaned up directory: {directory}")
            except Exception as e:
                logger.warning(f"Error cleaning up {directory}: {e}")

def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate that date range is reasonable
    
    Args:
        start_date: Start date string
        end_date: End date string
    
    Returns:
        Boolean indicating if range is valid
    """
    
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Check that start is before end
        if start > end:
            logger.error("Start date is after end date")
            return False
        
        # Check that range is not too large (e.g., more than 1 year)
        if (end - start).days > 365:
            logger.warning("Date range is longer than 1 year")
        
        # Check that dates are not in the future
        if end > pd.Timestamp.now():
            logger.warning("End date is in the future")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating date range: {e}")
        return False

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

def create_output_directories(*directories: str):
    """
    Create output directories if they don't exist
    
    Args:
        *directories: Directory paths to create
    """
    
    for directory in directories:
        try:
            os.makedirs(directory, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        except Exception as e:
            logger.error(f"Error creating directory {directory}: {e}")

def monitor_system_resources():
    """
    Monitor system resources (memory, CPU) and log warnings if high usage
    """
    
    try:
        import psutil
        
        # Get memory usage
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        
        # Get CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Log warnings if usage is high
        if memory_percent > 80:
            logger.warning(f"High memory usage: {memory_percent:.1f}%")
        
        if cpu_percent > 80:
            logger.warning(f"High CPU usage: {cpu_percent:.1f}%")
        
        logger.info(f"System resources - Memory: {memory_percent:.1f}%, CPU: {cpu_percent:.1f}%")
        
    except ImportError:
        logger.debug("psutil not available for resource monitoring")
    except Exception as e:
        logger.debug(f"Error monitoring resources: {e}")

def get_s3_object_size(bucket: str, key: str) -> int:
    """
    Get size of S3 object in bytes
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
    
    Returns:
        Size in bytes, or 0 if object doesn't exist
    """
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
    except Exception as e:
        logger.debug(f"Error getting S3 object size: {e}")
        return 0

def compress_and_upload_logs(log_directory: str, bucket: str, prefix: str = "logs/"):
    """
    Compress log files and upload to S3
    
    Args:
        log_directory: Directory containing log files
        bucket: S3 bucket name
        prefix: S3 prefix for logs
    """
    
    try:
        import gzip
        import shutil
        
        log_files = [f for f in os.listdir(log_directory) if f.endswith('.log')]
        
        for log_file in log_files:
            log_path = os.path.join(log_directory, log_file)
            compressed_path = f"{log_path}.gz"
            
            # Compress the log file
            with open(log_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            # Upload to S3
            s3_key = f"{prefix}{log_file}.gz"
            s3_client = boto3.client('s3')
            s3_client.upload_file(compressed_path, bucket, s3_key)
            
            # Clean up local compressed file
            os.remove(compressed_path)
            
            logger.info(f"Uploaded compressed log: {s3_key}")
            
    except Exception as e:
        logger.error(f"Error compressing and uploading logs: {e}")

def validate_s3_connectivity(bucket: str) -> bool:
    """
    Validate S3 connectivity and permissions
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Boolean indicating if S3 is accessible
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # Try to list objects (limited)
        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        
        logger.info(f"S3 connectivity validated for bucket: {bucket}")
        return True
        
    except Exception as e:
        logger.error(f"S3 connectivity failed for bucket {bucket}: {e}")
        return False

def create_checkpoint_file(checkpoint_name: str, data: Dict[str, Any], bucket: str):
    """
    Create a checkpoint file in S3 for resuming processing
    
    Args:
        checkpoint_name: Name of the checkpoint
        data: Data to store in checkpoint
        bucket: S3 bucket name
    """
    
    try:
        import json
        
        checkpoint_data = {
            'timestamp': datetime.now().isoformat(),
            'checkpoint_name': checkpoint_name,
            'data': data
        }
        
        # Upload to S3
        s3_key = f"checkpoints/{checkpoint_name}.json"
        s3_client = boto3.client('s3')
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(checkpoint_data, indent=2)
        )
        
        logger.info(f"Created checkpoint: {checkpoint_name}")
        
    except Exception as e:
        logger.error(f"Error creating checkpoint: {e}")

def load_checkpoint_file(checkpoint_name: str, bucket: str) -> Optional[Dict[str, Any]]:
    """
    Load a checkpoint file from S3
    
    Args:
        checkpoint_name: Name of the checkpoint
        bucket: S3 bucket name
    
    Returns:
        Checkpoint data or None if not found
    """
    
    try:
        import json
        
        s3_key = f"checkpoints/{checkpoint_name}.json"
        s3_client = boto3.client('s3')
        
        response = s3_client.get_object(Bucket=bucket, Key=s3_key)
        checkpoint_data = json.loads(response['Body'].read())
        
        logger.info(f"Loaded checkpoint: {checkpoint_name}")
        return checkpoint_data['data']
        
    except s3_client.exceptions.NoSuchKey:
        logger.info(f"Checkpoint not found: {checkpoint_name}")
        return None
    except Exception as e:
        logger.error(f"Error loading checkpoint: {e}")
        return None

def send_notification(message: str, 
                     notification_type: str = "info",
                     email_config: Optional[Dict[str, str]] = None,
                     slack_config: Optional[Dict[str, str]] = None):
    """
    Send notification via email or Slack
    
    Args:
        message: Notification message
        notification_type: Type of notification (info, warning, error)
        email_config: Email configuration
        slack_config: Slack configuration
    """
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    formatted_message = f"[{timestamp}] {notification_type.upper()}: {message}"
    
    # Send email notification
    if email_config:
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from']
            msg['To'] = email_config['to']
            msg['Subject'] = f"Monthly Report {notification_type.title()}"
            
            msg.attach(MIMEText(formatted_message, 'plain'))
            
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            if email_config.get('use_tls'):
                server.starttls()
            if email_config.get('username'):
                server.login(email_config['username'], email_config['password'])
            
            text = msg.as_string()
            server.sendmail(email_config['from'], email_config['to'], text)
            server.quit()
            
            logger.info("Email notification sent")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {e}")
    
    # Send Slack notification
    if slack_config:
        try:
            import requests
            
            payload = {
                'text': formatted_message,
                'username': slack_config.get('username', 'Monthly Report Bot'),
                'icon_emoji': slack_config.get('icon', ':chart_with_upwards_trend:')
            }
            
            response = requests.post(slack_config['webhook_url'], json=payload)
            response.raise_for_status()
            
            logger.info("Slack notification sent")
            
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

def generate_processing_summary(start_time: datetime, 
                              end_time: datetime,
                              dates_processed: List[str],
                              errors: List[str]) -> str:
    """
    Generate a summary of processing results
    
    Args:
        start_time: Processing start time
        end_time: Processing end time
        dates_processed: List of successfully processed dates
        errors: List of error messages
    
    Returns:
        Summary string
    """
    
    duration = end_time - start_time
    
    summary = f"""
    Processing Summary
    ==================
    Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
    End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
    Duration: {duration}
    
    Dates Processed: {len(dates_processed)}
    Successful: {len(dates_processed)}
    Errors: {len(errors)}
    
    Date Range: {min(dates_processed) if dates_processed else 'None'} to {max(dates_processed) if dates_processed else 'None'}
    
    """
    
    if errors:
        summary += "\nErrors:\n"
        for i, error in enumerate(errors, 1):
            summary += f"{i}. {error}\n"
    
    return summary

# Global configuration variables (loaded at module import)
try:
    conf, aws_conf = load_configuration()
    AM_PEAK_HOURS, PM_PEAK_HOURS = get_peak_hours(conf) if conf else ([7, 8, 9], [16, 17, 18])
    usable_cores = get_usable_cores()
    
    # Setup environment
    env_config = setup_environment()
    proxy_config = setup_proxy()
    
    logger.info("Monthly Report Functions initialized successfully")
    
except Exception as e:
    logger.error(f"Error initializing Monthly Report Functions: {e}")
    conf = {}
    aws_conf = {}
    AM_PEAK_HOURS = [7, 8, 9]
    PM_PEAK_HOURS = [16, 17, 18]
    usable_cores = 1

# Module-level constants that can be imported by other modules
__all__ = [
    # Color constants
    'LIGHT_BLUE', 'BLUE', 'LIGHT_GREEN', 'GREEN', 'LIGHT_RED', 'RED',
    'LIGHT_ORANGE', 'ORANGE', 'LIGHT_PURPLE', 'PURPLE', 'LIGHT_BROWN', 'BROWN',
    'RED2', 'GDOT_BLUE', 'BLACK', 'WHITE', 'GRAY', 'DARK_GRAY', 'DARK_DARK_GRAY',
    
    # Day constants
    'SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT',
    
    # Configuration
    'conf', 'aws_conf', 'AM_PEAK_HOURS', 'PM_PEAK_HOURS', 'usable_cores',
    
    # Functions
    'load_configuration', 'setup_environment', 'setup_proxy', 'get_usable_cores',
    'get_peak_hours', 'get_date_from_string', 'get_month_abbrs', 'get_signalids_from_s3',
    'get_corridors', 'get_latest_det_config', 'write_signal_details',
    'parallel_process_dates', 'keep_trying', 'cleanup_temp_directories',
    'validate_date_range', 'setup_logging', 'create_output_directories',
    'monitor_system_resources', 'get_s3_object_size', 'compress_and_upload_logs',
    'validate_s3_connectivity', 'create_checkpoint_file', 'load_checkpoint_file',
    'send_notification', 'generate_processing_summary', 's3write_using'
]


