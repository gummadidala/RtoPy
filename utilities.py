"""
Utility functions
Converted from Utilities.R
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import logging
import boto3
import time
import os
import sys
import psutil
from pathlib import Path
from typing import Optional, Union, List, Dict, Any, Callable
import yaml
import json
from functools import wraps
import multiprocessing
import concurrent.futures
import threading
from contextlib import contextmanager

logger = logging.getLogger(__name__)

def setup_logging(level: str = "INFO", log_file: Optional[str] = None):
    """
    Setup logging configuration
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_file: Optional log file path
    """
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def get_usable_cores() -> int:
    """
    Get number of usable CPU cores, leaving one free
    
    Returns:
        Number of cores to use for parallel processing
    """
    
    try:
        total_cores = multiprocessing.cpu_count()
        usable_cores = max(1, total_cores - 1)
        logger.info(f"Using {usable_cores} of {total_cores} available cores")
        return usable_cores
    except Exception as e:
        logger.warning(f"Could not determine CPU count: {e}. Using 1 core.")
        return 1

def get_date_from_string(date_string: Optional[str], 
                        s3bucket: Optional[str] = None, 
                        s3prefix: Optional[str] = None) -> str:
    """
    Get date from string, with special handling for 'yesterday' and S3 latest
    
    Args:
        date_string: Date string or special value ('yesterday', 'latest')
        s3bucket: S3 bucket for 'latest' option
        s3prefix: S3 prefix for 'latest' option
    
    Returns:
        Date string in YYYY-MM-DD format
    """
    
    try:
        if date_string is None or date_string.lower() == 'yesterday':
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        elif date_string.lower() == 'latest' and s3bucket and s3prefix:
            # Get latest date from S3
            s3_client = boto3.client('s3')
            
            response = s3_client.list_objects_v2(
                Bucket=s3bucket,
                Prefix=s3prefix,
                Delimiter='/'
            )
            
            if 'CommonPrefixes' in response:
                dates = []
                for prefix in response['CommonPrefixes']:
                    # Extract date from prefix like "mark/split_failures/date=2023-01-01/"
                    if 'date=' in prefix['Prefix']:
                        date_part = prefix['Prefix'].split('date=')[1].rstrip('/')
                        dates.append(date_part)
                
                if dates:
                    latest_date = max(dates)
                    logger.info(f"Found latest date in S3: {latest_date}")
                    return latest_date
            
            # Fallback to yesterday if no S3 data found
            logger.warning("No dates found in S3, using yesterday")
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        else:
            # Try to parse the date string
            parsed_date = pd.to_datetime(date_string).date()
            return parsed_date.strftime('%Y-%m-%d')
            
    except Exception as e:
        logger.error(f"Error parsing date string '{date_string}': {e}")
        # Fallback to yesterday
        return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

def get_month_abbrs(start_date: Union[str, date], end_date: Union[str, date]) -> List[str]:
    """
    Get list of month abbreviations (YYYY-MM) between start and end dates
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        List of month strings in YYYY-MM format
    """
    
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()
        
        # Get first day of start month and last day of end month
        start_month = start_date.replace(day=1)
        end_month = end_date.replace(day=1)
        
        months = []
        current_month = start_month
        
        while current_month <= end_month:
            months.append(current_month.strftime('%Y-%m'))
            # Move to next month
            if current_month.month == 12:
                current_month = current_month.replace(year=current_month.year + 1, month=1)
            else:
                current_month = current_month.replace(month=current_month.month + 1)
        
        logger.info(f"Generated {len(months)} month abbreviations: {months}")
        return months
        
    except Exception as e:
        logger.error(f"Error generating month abbreviations: {e}")
        return []

def get_tuesdays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get Tuesday dates for each week in the dataframe
    
    Args:
        df: DataFrame with Date column and Week column
    
    Returns:
        DataFrame with Week and corresponding Tuesday Date
    """
    
    try:
        if 'Date' not in df.columns:
            raise ValueError("DataFrame must have 'Date' column")
        
        # Ensure Date is datetime
        df = df.copy()
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Add week number if not present
        if 'Week' not in df.columns:
            df['Week'] = df['Date'].dt.isocalendar().week
        
        # Find Tuesday for each week
        tuesdays = []
        for week in df['Week'].unique():
            week_dates = df[df['Week'] == week]['Date']
            if not week_dates.empty:
                # Find the Tuesday (weekday 1, where Monday=0)
                tuesday_dates = week_dates[week_dates.dt.weekday == 1]
                if not tuesday_dates.empty:
                    tuesday = tuesday_dates.iloc[0].date()
                else:
                    # If no Tuesday found, use the first date of the week
                    tuesday = week_dates.min().date()
                
                tuesdays.append({'Week': week, 'Date': tuesday})
        
        result = pd.DataFrame(tuesdays)
        logger.info(f"Generated {len(result)} Tuesday dates")
        return result
        
    except Exception as e:
        logger.error(f"Error getting Tuesday dates: {e}")
        return pd.DataFrame()

def get_signalids_from_s3(date_: Union[str, date]) -> List[str]:
    """
    Get list of signal IDs that have data for a specific date from S3
    
    Args:
        date_: Date to check for signal IDs
    
    Returns:
        List of signal IDs
    """
    
    try:
        if isinstance(date_, date):
            date_str = date_.strftime('%Y-%m-%d')
        else:
            date_str = str(date_)
        
        # This would need to be implemented based on your S3 structure
        # For now, return an empty list as placeholder
        logger.info(f"Getting signal IDs for {date_str}")
        return []
        
    except Exception as e:
        logger.error(f"Error getting signal IDs for {date_}: {e}")
        return []

def keep_trying(func: Callable, n_tries: int = 3, timeout: int = 30, *args, **kwargs) -> Any:
    """
    Keep trying to execute a function with retries
    
    Args:
        func: Function to execute
        n_tries: Number of attempts
        timeout: Timeout between attempts in seconds
        *args: Arguments to pass to function
        **kwargs: Keyword arguments to pass to function
    
    Returns:
        Result of function execution
    """
    
    last_exception = None
    
    for attempt in range(n_tries):
        try:
            logger.debug(f"Attempt {attempt + 1} of {n_tries} for {func.__name__}")
            result = func(*args, **kwargs)
            if attempt > 0:
                logger.info(f"Successfully executed {func.__name__} on attempt {attempt + 1}")
            return result
            
        except Exception as e:
            last_exception = e
            logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
            
            if attempt < n_tries - 1:
                logger.info(f"Waiting {timeout} seconds before retry...")
                time.sleep(timeout)
    
    logger.error(f"All {n_tries} attempts failed for {func.__name__}")
    raise last_exception

def convert_to_utc(df: pd.DataFrame, datetime_col: str = 'Timeperiod') -> pd.DataFrame:
    """
    Convert datetime column to UTC
    
    Args:
        df: DataFrame with datetime column
        datetime_col: Name of datetime column
    
    Returns:
        DataFrame with UTC datetime
    """
    
    try:
        if datetime_col not in df.columns:
            logger.warning(f"Column {datetime_col} not found in DataFrame")
            return df
        
        df = df.copy()
        
        # Convert to UTC if not already
        if df[datetime_col].dt.tz is None:
            # Assume Eastern Time if no timezone
            df[datetime_col] = df[datetime_col].dt.tz_localize('US/Eastern', ambiguous='infer')
        
        df[datetime_col] = df[datetime_col].dt.tz_convert('UTC')
        
        return df
        
    except Exception as e:
        logger.error(f"Error converting to UTC: {e}")
        return df

def cleanup_temp_directories(*directories: str):
    """
    Clean up temporary directories
    
    Args:
        *directories: Directory paths to clean up
    """
    
    for directory in directories:
        try:
            dir_path = Path(directory)
            if dir_path.exists() and dir_path.is_dir():
                import shutil
                shutil.rmtree(dir_path)
                logger.info(f"Cleaned up directory: {directory}")
        except Exception as e:
            logger.error(f"Error cleaning up directory {directory}: {e}")

def monitor_system_resources():
    """
    Monitor and log system resource usage
    """
    
    try:
        # Memory usage
        memory = psutil.virtual_memory()
        logger.info(f"Memory usage: {memory.percent}% ({memory.used / 1024**3:.1f}GB used of {memory.total / 1024**3:.1f}GB)")
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        logger.info(f"CPU usage: {cpu_percent}%")
        
        # Disk usage
        disk = psutil.disk_usage('/')
        logger.info(f"Disk usage: {disk.percent}% ({disk.used / 1024**3:.1f}GB used of {disk.total / 1024**3:.1f}GB)")
        
        # Check for high resource usage
        if memory.percent > 90:
            logger.warning("High memory usage detected!")
        
        if cpu_percent > 90:
            logger.warning("High CPU usage detected!")
        
        if disk.percent > 90:
            logger.warning("High disk usage detected!")
            
    except Exception as e:
        logger.error(f"Error monitoring system resources: {e}")

def create_checkpoint_file(checkpoint_name: str, data: Dict, bucket: str):
    """
    Create a checkpoint file in S3 to track processing progress
    
    Args:
        checkpoint_name: Name of the checkpoint
        data: Data to store in checkpoint
        bucket: S3 bucket name
    """
    
    try:
        s3_key = f"checkpoints/{checkpoint_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        checkpoint_data = {
            'checkpoint_name': checkpoint_name,
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(checkpoint_data, indent=2, default=str),
            ContentType='application/json'
        )
        
        logger.info(f"Created checkpoint: {s3_key}")
        
    except Exception as e:
        logger.error(f"Error creating checkpoint {checkpoint_name}: {e}")

def load_checkpoint_file(checkpoint_name: str, bucket: str) -> Optional[Dict]:
    """
    Load the most recent checkpoint file from S3
    
    Args:
        checkpoint_name: Name of the checkpoint
        bucket: S3 bucket name
    
    Returns:
        Checkpoint data dictionary or None if not found
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # List checkpoint files
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"checkpoints/{checkpoint_name}_"
        )
        
        if 'Contents' not in response:
            logger.info(f"No checkpoint files found for {checkpoint_name}")
            return None
        
        # Get the most recent checkpoint
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = files[0]['Key']
        
        # Read the checkpoint data
        obj = s3_client.get_object(Bucket=bucket, Key=latest_file)
        checkpoint_data = json.loads(obj['Body'].read().decode('utf-8'))
        
        logger.info(f"Loaded checkpoint: {latest_file}")
        return checkpoint_data
        
    except Exception as e:
        logger.error(f"Error loading checkpoint {checkpoint_name}: {e}")
        return None

@contextmanager
def timer(operation_name: str):
    """
    Context manager to time operations
    
    Args:
        operation_name: Name of the operation being timed
    """
    
    start_time = time.time()
    logger.info(f"Starting {operation_name}")
    
    try:
        yield
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Completed {operation_name} in {duration:.2f} seconds")

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry function on failure with exponential backoff
    
    Args:
        max_retries: Maximum number of retries
        delay: Initial delay between retries in seconds
        backoff: Backoff multiplier for delay
    """
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"Function {func.__name__} failed after {max_retries} retries")
                        raise e
                    
                    logger.warning(f"Attempt {attempt + 1} failed for {func.__name__}: {e}")
                    logger.info(f"Retrying in {current_delay:.1f} seconds...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            
            raise last_exception
        
        return wrapper
    return decorator

def validate_dataframe_schema(df: pd.DataFrame, required_columns: List[str], 
                            optional_columns: Optional[List[str]] = None) -> bool:
    """
    Validate DataFrame has required schema
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
        optional_columns: List of optional column names
    
    Returns:
        Boolean indicating if schema is valid
    """
    
    try:
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for unexpected columns (if optional_columns is provided)
        if optional_columns is not None:
            all_expected = set(required_columns + optional_columns)
            unexpected_columns = set(df.columns) - all_expected
            if unexpected_columns:
                logger.warning(f"Unexpected columns found: {unexpected_columns}")
        
        logger.debug("DataFrame schema validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating DataFrame schema: {e}")
        return False

def safe_divide(numerator: Union[float, pd.Series], 
               denominator: Union[float, pd.Series], 
               default: float = 0.0) -> Union[float, pd.Series]:
    """
    Safely divide two numbers/series, handling division by zero
    
    Args:
        numerator: Numerator value(s)
        denominator: Denominator value(s)
        default: Default value to return when denominator is zero
    
    Returns:
        Result of division or default value
    """
    
    try:
        if isinstance(denominator, pd.Series):
            return np.where(denominator != 0, numerator / denominator, default)
        else:
            return numerator / denominator if denominator != 0 else default
            
    except Exception as e:
        logger.error(f"Error in safe division: {e}")
        return default

def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 10000) -> List[pd.DataFrame]:
    """
    Split DataFrame into chunks for processing
    
    Args:
        df: DataFrame to split
        chunk_size: Size of each chunk
    
    Returns:
        List of DataFrame chunks
    """
    
    try:
        chunks = []
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size].copy()
            chunks.append(chunk)
        
        logger.info(f"Split DataFrame into {len(chunks)} chunks of max size {chunk_size}")
        return chunks
        
    except Exception as e:
        logger.error(f"Error chunking DataFrame: {e}")
        return [df]

def parallel_apply(df: pd.DataFrame, func: Callable, n_jobs: Optional[int] = None, 
                  chunk_size: int = 10000, **func_kwargs) -> pd.DataFrame:
    """
    Apply function to DataFrame in parallel
    
    Args:
        df: DataFrame to process
        func: Function to apply to each chunk
        n_jobs: Number of parallel jobs (default: usable cores)
        chunk_size: Size of chunks to process
        **func_kwargs: Additional arguments for the function
    
    Returns:
        Processed DataFrame
    """
    
    try:
        if n_jobs is None:
            n_jobs = get_usable_cores()
        
        # Split into chunks
        chunks = chunk_dataframe(df, chunk_size)
        
        if len(chunks) == 1:
            # No need for parallel processing
            return func(df, **func_kwargs)
        
        # Process chunks in parallel
        with concurrent.futures.ProcessPoolExecutor(max_workers=n_jobs) as executor:
            futures = [executor.submit(func, chunk, **func_kwargs) for chunk in chunks]
            
            results = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")
        
        # Combine results
        if results:
            combined_result = pd.concat(results, ignore_index=True)
            logger.info(f"Parallel processing completed. Result shape: {combined_result.shape}")
            return combined_result
        else:
            logger.warning("No results from parallel processing")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error in parallel apply: {e}")
        # Fallback to sequential processing
        return func(df, **func_kwargs)

def memory_usage_mb(df: pd.DataFrame) -> float:
    """
    Get memory usage of DataFrame in MB
    
    Args:
        df: DataFrame to measure
    
    Returns:
        Memory usage in MB
    """
    
    try:
        memory_usage = df.memory_usage(deep=True).sum() / 1024**2
        return memory_usage
    except Exception as e:
        logger.error(f"Error calculating memory usage: {e}")
        return 0.0

def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optimize DataFrame memory usage by downcasting numeric types
    
    Args:
        df: DataFrame to optimize
    
    Returns:
        Optimized DataFrame
    """
    
    try:
        original_memory = memory_usage_mb(df)
        df_optimized = df.copy()
        
        # Optimize integer columns
        int_cols = df_optimized.select_dtypes(include=['int64']).columns
        for col in int_cols:
            df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='integer')
        
        # Optimize float columns
        float_cols = df_optimized.select_dtypes(include=['float64']).columns
        for col in float_cols:
            df_optimized[col] = pd.to_numeric(df_optimized[col], downcast='float')
        
        # Convert object columns to category if they have few unique values
        object_cols = df_optimized.select_dtypes(include=['object']).columns
        for col in object_cols:
            if df_optimized[col].nunique() / len(df_optimized) < 0.5:  # Less than 50% unique
                df_optimized[col] = df_optimized[col].astype('category')
        
        optimized_memory = memory_usage_mb(df_optimized)
        memory_reduction = (original_memory - optimized_memory) / original_memory * 100
        
        logger.info(f"Memory optimized: {original_memory:.1f}MB -> {optimized_memory:.1f}MB "
                   f"({memory_reduction:.1f}% reduction)")
        
        return df_optimized
        
    except Exception as e:
        logger.error(f"Error optimizing DataFrame memory: {e}")
        return df

def filter_recent_data(df: pd.DataFrame, date_col: str, days: int = 30) -> pd.DataFrame:
    """
    Filter DataFrame to only include recent data
    
    Args:
        df: DataFrame to filter
        date_col: Name of date column
        days: Number of recent days to keep
    
    Returns:
        Filtered DataFrame
    """
    
    try:
        cutoff_date = datetime.now().date() - timedelta(days=days)
        df_filtered = df[pd.to_datetime(df[date_col]).dt.date >= cutoff_date].copy()
        
        logger.info(f"Filtered to recent {days} days: {len(df)} -> {len(df_filtered)} records")
        return df_filtered
        
    except Exception as e:
        logger.error(f"Error filtering recent data: {e}")
        return df

def create_date_range(start_date: Union[str, date], end_date: Union[str, date], 
                     freq: str = 'D') -> List[date]:
    """
    Create a list of dates between start and end dates
    
    Args:
        start_date: Start date
        end_date: End date
        freq: Frequency ('D' for daily, 'W' for weekly, 'M' for monthly)
    
    Returns:
        List of dates
    """
    
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()
        
        date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
        dates = [d.date() for d in date_range]
        
        logger.info(f"Created date range with {len(dates)} dates")
        return dates
        
    except Exception as e:
        logger.error(f"Error creating date range: {e}")
        return []

def load_config_file(config_path: str) -> Dict:
    """
    Load configuration from YAML file
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        Configuration dictionary
    """
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
        
    except Exception as e:
        logger.error(f"Error loading config file {config_path}: {e}")
        return {}

def validate_config(config: Dict, required_keys: List[str]) -> bool:
    """
    Validate configuration has required keys
    
    Args:
        config: Configuration dictionary
        required_keys: List of required keys
    
    Returns:
        Boolean indicating if validation passed
    """
    
    try:
        missing_keys = []
        
        for key in required_keys:
            if '.' in key:  # Nested key like 'database.host'
                keys = key.split('.')
                current = config
                for k in keys:
                    if k not in current:
                        missing_keys.append(key)
                        break
                    current = current[k]
            else:
                if key not in config:
                    missing_keys.append(key)
        
        if missing_keys:
            logger.error(f"Missing required configuration keys: {missing_keys}")
            return False
        
        logger.info("Configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating configuration: {e}")
        return False

def get_s3_object_size(bucket: str, key: str) -> int:
    """
    Get size of S3 object in bytes
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
    
    Returns:
        Object size in bytes, or 0 if not found
    """
    
    try:
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return response['ContentLength']
        
    except Exception as e:
        logger.error(f"Error getting S3 object size for {bucket}/{key}: {e}")
        return 0

def list_s3_objects_by_date(bucket: str, prefix: str, start_date: date, 
                           end_date: date) -> List[str]:
    """
    List S3 objects within a date range
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to search
        start_date: Start date for filtering
        end_date: End date for filtering
    
    Returns:
        List of S3 object keys
    """
    
    try:
        s3_client = boto3.client('s3')
        objects = []
        
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Extract date from object key (assuming date format in key)
                    obj_key = obj['Key']
                    
                    # Try to extract date from key using common patterns
                    import re
                    date_patterns = [
                        r'date=(\d{4}-\d{2}-\d{2})',
                        r'(\d{4}-\d{2}-\d{2})',
                        r'(\d{4}/\d{2}/\d{2})',
                        r'(\d{8})'  # YYYYMMDD format
                    ]
                    
                    obj_date = None
                    for pattern in date_patterns:
                        match = re.search(pattern, obj_key)
                        if match:
                            date_str = match.group(1)
                            try:
                                if len(date_str) == 8:  # YYYYMMDD format
                                    obj_date = datetime.strptime(date_str, '%Y%m%d').date()
                                else:
                                    obj_date = pd.to_datetime(date_str).date()
                                break
                            except:
                                continue
                    
                    # If we found a date and it's in range, add to results
                    if obj_date and start_date <= obj_date <= end_date:
                        objects.append(obj_key)
        
        logger.info(f"Found {len(objects)} S3 objects between {start_date} and {end_date}")
        return objects
        
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        return []

def ensure_directory_exists(directory_path: Union[str, Path]):
    """
    Ensure directory exists, create if it doesn't
    
    Args:
        directory_path: Path to directory
    """
    
    try:
        path = Path(directory_path)
        path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured directory exists: {directory_path}")
        
    except Exception as e:
        logger.error(f"Error creating directory {directory_path}: {e}")

def get_file_age_days(file_path: Union[str, Path]) -> float:
    """
    Get age of file in days
    
    Args:
        file_path: Path to file
    
    Returns:
        Age in days, or -1 if file doesn't exist
    """
    
    try:
        path = Path(file_path)
        if not path.exists():
            return -1
        
        modification_time = datetime.fromtimestamp(path.stat().st_mtime)
        age = (datetime.now() - modification_time).total_seconds() / (24 * 3600)
        
        return age
        
    except Exception as e:
        logger.error(f"Error getting file age for {file_path}: {e}")
        return -1

def clean_old_files(directory: Union[str, Path], max_age_days: int = 7, 
                   pattern: str = "*"):
    """
    Clean up old files in a directory
    
    Args:
        directory: Directory to clean
        max_age_days: Maximum age in days before deletion
        pattern: File pattern to match (glob pattern)
    """
    
    try:
        path = Path(directory)
        if not path.exists():
            return
        
        deleted_count = 0
        for file_path in path.glob(pattern):
            if file_path.is_file():
                age = get_file_age_days(file_path)
                if age > max_age_days:
                    file_path.unlink()
                    deleted_count += 1
        
        logger.info(f"Cleaned up {deleted_count} old files from {directory}")
        
    except Exception as e:
        logger.error(f"Error cleaning old files in {directory}: {e}")

def format_bytes(bytes_value: int) -> str:
    """
    Format bytes into human-readable string
    
    Args:
        bytes_value: Number of bytes
    
    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    
    try:
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
        
    except Exception as e:
        logger.error(f"Error formatting bytes: {e}")
        return "Unknown"

def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string
    
    Args:
        seconds: Duration in seconds
    
    Returns:
        Formatted string (e.g., "2h 15m 30s")
    """
    
    try:
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            remaining_minutes = (seconds % 3600) / 60
            if remaining_minutes > 0:
                return f"{int(hours)}h {int(remaining_minutes)}m"
            else:
                return f"{hours:.1f}h"
                
    except Exception as e:
        logger.error(f"Error formatting duration: {e}")
        return "Unknown"

def send_notification(message: str, webhook_url: Optional[str] = None, 
                     email_config: Optional[Dict] = None):
    """
    Send notification via webhook or email
    
    Args:
        message: Message to send
        webhook_url: Optional webhook URL (e.g., Slack, Teams)
        email_config: Optional email configuration
    """
    
    try:
        # Send webhook notification
        if webhook_url:
            import requests
            
            payload = {"text": message}
            response = requests.post(webhook_url, json=payload, timeout=10)
            
            if response.status_code == 200:
                logger.info("Webhook notification sent successfully")
            else:
                logger.error(f"Failed to send webhook notification: {response.status_code}")
        
        # Send email notification
        if email_config:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from']
            msg['To'] = email_config['to']
            msg['Subject'] = email_config.get('subject', 'Notification')
            
            msg.attach(MIMEText(message, 'plain'))
            
            with smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port']) as server:
                server.starttls()
                server.login(email_config['username'], email_config['password'])
                server.send_message(msg)
            
            logger.info("Email notification sent successfully")
            
    except Exception as e:
        logger.error(f"Error sending notification: {e}")

def create_progress_tracker(total_items: int, description: str = "Processing"):
    """
    Create a simple progress tracker
    
    Args:
        total_items: Total number of items to process
        description: Description of the process
    
    Returns:
        Progress tracker function
    """
    
    start_time = time.time()
    
    def update_progress(current_item: int):
        """Update progress and log status"""
        try:
            if total_items == 0:
                return
            
            progress_pct = (current_item / total_items) * 100
            elapsed_time = time.time() - start_time
            
            if current_item > 0:
                estimated_total_time = elapsed_time * (total_items / current_item)
                remaining_time = estimated_total_time - elapsed_time
                
                logger.info(f"{description}: {current_item}/{total_items} ({progress_pct:.1f}%) "
                           f"- Elapsed: {format_duration(elapsed_time)}, "
                           f"Remaining: {format_duration(remaining_time)}")
            else:
                logger.info(f"{description}: Starting...")
                
        except Exception as e:
            logger.error(f"Error updating progress: {e}")
    
    return update_progress

def validate_date_range(start_date: Union[str, date], end_date: Union[str, date], 
                       max_days: int = 365) -> bool:
    """
    Validate that date range is reasonable
    
    Args:
        start_date: Start date
        end_date: End date
        max_days: Maximum allowed days in range
    
    Returns:
        Boolean indicating if date range is valid
    """
    
    try:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date).date()
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date).date()
        
        # Check order
        if start_date > end_date:
            logger.error("Start date is after end date")
            return False
        
        # Check range size
        date_diff = (end_date - start_date).days
        if date_diff > max_days:
            logger.error(f"Date range too large: {date_diff} days (max: {max_days})")
            return False
        
        # Check for future dates
        if end_date > date.today():
            logger.warning("End date is in the future")
        
        logger.info(f"Date range validation passed: {date_diff} days")
        return True
        
    except Exception as e:
        logger.error(f"Error validating date range: {e}")
        return False

class PerformanceMonitor:
    """
    Context manager for monitoring performance of code blocks
    """
    
    def __init__(self, operation_name: str, log_memory: bool = True):
        self.operation_name = operation_name
        self.log_memory = log_memory
        self.start_time = None
        self.start_memory = None
    
    def __enter__(self):
        self.start_time = time.time()
        if self.log_memory:
            self.start_memory = psutil.Process().memory_info().rss / 1024**2
        
        logger.info(f"Starting {self.operation_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = time.time()
        duration = end_time - self.start_time
        
        log_msg = f"Completed {self.operation_name} in {format_duration(duration)}"
        
        if self.log_memory:
            end_memory = psutil.Process().memory_info().rss / 1024**2
            memory_change = end_memory - self.start_memory
            log_msg += f" (Memory: {memory_change:+.1f} MB)"
        
        if exc_type is None:
            logger.info(log_msg)
        else:
            logger.error(f"Failed {self.operation_name} after {format_duration(duration)}: {exc_val}")

def batch_process(items: List, batch_size: int, process_func: Callable, 
                 *args, **kwargs) -> List:
    """
    Process items in batches
    
    Args:
        items: List of items to process
        batch_size: Size of each batch
        process_func: Function to apply to each batch
        *args: Additional arguments for process_func
        **kwargs: Additional keyword arguments for process_func
    
    Returns:
        List of results from processing each batch
    """
    
    try:
        results = []
        total_batches = (len(items) + batch_size - 1) // batch_size
        
        logger.info(f"Processing {len(items)} items in {total_batches} batches of size {batch_size}")
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            
            logger.debug(f"Processing batch {batch_num}/{total_batches}")
            
            try:
                result = process_func(batch, *args, **kwargs)
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing batch {batch_num}: {e}")
                results.append(None)
        
        logger.info(f"Completed batch processing: {len([r for r in results if r is not None])}/{total_batches} successful")
        return results
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        return []

# Constants equivalent to R constants
WEEKDAYS = {
    'SUNDAY': 0,
    'MONDAY': 1, 
    'TUESDAY': 2,
    'WEDNESDAY': 3,
    'THURSDAY': 4,
    'FRIDAY': 5,
    'SATURDAY': 6
}

SUN = WEEKDAYS['SUNDAY']
MON = WEEKDAYS['MONDAY'] 
TUE = WEEKDAYS['TUESDAY']
WED = WEEKDAYS['WEDNESDAY']
THU = WEEKDAYS['THURSDAY']
FRI = WEEKDAYS['FRIDAY']
SAT = WEEKDAYS['SATURDAY']

# Color constants (equivalent to R color definitions)
COLORS = {
    'LIGHT_BLUE': "#A6CEE3",
    'BLUE': "#1F78B4",
    'LIGHT_GREEN': "#B2DF8A",
    'GREEN': "#33A02C",
    'LIGHT_RED': "#FB9A99",
    'RED': "#E31A1C",
    'LIGHT_ORANGE': "#FDBF6F",
    'ORANGE': "#FF7F00",
    'LIGHT_PURPLE': "#CAB2D6",
    'PURPLE': "#6A3D9A",
    'LIGHT_BROWN': "#FFFF99",
    'BROWN': "#B15928",
    'RED2': "#e41a1c",
    'GDOT_BLUE': "#256194",
    'BLACK': "#000000",
    'WHITE': "#FFFFFF",
    'GRAY': "#D0D0D0",
    'DARK_GRAY': "#7A7A7A",
    'DARK_DARK_GRAY': "#494949"
}

def get_color(color_name: str) -> str:
    """
    Get color hex code by name
    
    Args:
        color_name: Name of the color
    
    Returns:
        Hex color code
    """
    
    return COLORS.get(color_name.upper(), "#000000")

def is_business_day(date_obj: Union[str, date, datetime]) -> bool:
    """
    Check if date is a business day (Tuesday, Wednesday, Thursday)
    
    Args:
        date_obj: Date to check
    
    Returns:
        Boolean indicating if it's a business day
    """
    
    try:
        if isinstance(date_obj, str):
            date_obj = pd.to_datetime(date_obj).date()
        elif isinstance(date_obj, datetime):
            date_obj = date_obj.date()
        
        weekday = date_obj.weekday()  # Monday = 0, Sunday = 6
        return weekday in [TUE-1, WED-1, THU-1]  # Adjust for Python's Monday=0 indexing
        
    except Exception as e:
        logger.error(f"Error checking business day: {e}")
        return False

def filter_business_days(df: pd.DataFrame, date_col: str = 'Date') -> pd.DataFrame:
    """
    Filter DataFrame to only include business days (Tue, Wed, Thu)
    
    Args:
        df: DataFrame to filter
        date_col: Name of date column
    
    Returns:
        Filtered DataFrame
    """
    
    try:
        df_filtered = df.copy()
        df_filtered[date_col] = pd.to_datetime(df_filtered[date_col])
        
        # Add day of week (0=Monday, 6=Sunday)
        df_filtered['dow'] = df_filtered[date_col].dt.dayofweek
        
        # Filter for Tuesday (1), Wednesday (2), Thursday (3)
        business_days = df_filtered[df_filtered['dow'].isin([1, 2, 3])].copy()
        business_days = business_days.drop('dow', axis=1)
        
        logger.info(f"Filtered to business days: {len(df)} -> {len(business_days)} records")
        return business_days
        
    except Exception as e:
        logger.error(f"Error filtering business days: {e}")
        return df

def add_time_features(df: pd.DataFrame, datetime_col: str) -> pd.DataFrame:
    """
    Add time-based features to DataFrame
    
    Args:
        df: DataFrame to enhance
        datetime_col: Name of datetime column
    
    Returns:
        DataFrame with additional time features
    """
    
    try:
        df_enhanced = df.copy()
        dt_series = pd.to_datetime(df_enhanced[datetime_col])
        
        # Add various time features
        df_enhanced['year'] = dt_series.dt.year
        df_enhanced['month'] = dt_series.dt.month
        df_enhanced['day'] = dt_series.dt.day
        df_enhanced['hour'] = dt_series.dt.hour
        df_enhanced['minute'] = dt_series.dt.minute
        df_enhanced['dayofweek'] = dt_series.dt.dayofweek
        df_enhanced['dayofyear'] = dt_series.dt.dayofyear
        df_enhanced['week'] = dt_series.dt.isocalendar().week
        df_enhanced['quarter'] = dt_series.dt.quarter
        
        # Add categorical time features
        df_enhanced['month_name'] = dt_series.dt.strftime('%B')
        df_enhanced['day_name'] = dt_series.dt.strftime('%A')
        
        # Add business day indicator
        df_enhanced['is_business_day'] = dt_series.apply(lambda x: is_business_day(x.date()))
        
        # Add peak hour indicators (assuming AM: 6-10, PM: 15-19)
        df_enhanced['is_am_peak'] = df_enhanced['hour'].between(6, 9)
        df_enhanced['is_pm_peak'] = df_enhanced['hour'].between(15, 18)
        df_enhanced['is_peak_hour'] = df_enhanced['is_am_peak'] | df_enhanced['is_pm_peak']
        
        logger.info(f"Added time features to DataFrame with {len(df_enhanced)} records")
        return df_enhanced
        
    except Exception as e:
        logger.error(f"Error adding time features: {e}")
        return df

def calculate_percentiles(series: pd.Series, percentiles: List[float] = [25, 50, 75, 90, 95]) -> Dict[str, float]:
    """
    Calculate percentiles for a series
    
    Args:
        series: Pandas series to analyze
        percentiles: List of percentiles to calculate
    
    Returns:
        Dictionary of percentile values
    """
    
    try:
        result = {}
        for p in percentiles:
            result[f'p{p}'] = series.quantile(p / 100)
        
        # Add additional statistics
        result['mean'] = series.mean()
        result['std'] = series.std()
        result['min'] = series.min()
        result['max'] = series.max()
        result['count'] = series.count()
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating percentiles: {e}")
        return {}

def detect_outliers(series: pd.Series, method: str = 'iqr', 
                   threshold: float = 1.5) -> pd.Series:
    """
    Detect outliers in a series
    
    Args:
        series: Series to analyze
        method: Method to use ('iqr', 'zscore', 'modified_zscore')
        threshold: Threshold for outlier detection
    
    Returns:
        Boolean series indicating outliers
    """
    
    try:
        if method == 'iqr':
            Q1 = series.quantile(0.25)
            Q3 = series.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            outliers = (series < lower_bound) | (series > upper_bound)
            
        elif method == 'zscore':
            z_scores = np.abs((series - series.mean()) / series.std())
            outliers = z_scores > threshold
            
        elif method == 'modified_zscore':
            median = series.median()
            mad = np.median(np.abs(series - median))
            modified_z_scores = 0.6745 * (series - median) / mad
            outliers = np.abs(modified_z_scores) > threshold
            
        else:
            logger.error(f"Unknown outlier detection method: {method}")
            return pd.Series([False] * len(series), index=series.index)
        
        outlier_count = outliers.sum()
        outlier_pct = (outlier_count / len(series)) * 100
        logger.info(f"Detected {outlier_count} outliers ({outlier_pct:.1f}%) using {method} method")
        
        return outliers
        
    except Exception as e:
        logger.error(f"Error detecting outliers: {e}")
        return pd.Series([False] * len(series), index=series.index)

def rolling_statistics(df: pd.DataFrame, value_col: str, window: int = 7, 
                      stats: List[str] = ['mean', 'std', 'min', 'max']) -> pd.DataFrame:
    """
    Calculate rolling statistics for a column
    
    Args:
        df: DataFrame to process
        value_col: Column to calculate statistics for
        window: Rolling window size
        stats: List of statistics to calculate
    
    Returns:
        DataFrame with rolling statistics columns
    """
    
    try:
        df_result = df.copy()
        
        for stat in stats:
            col_name = f'{value_col}_rolling_{stat}_{window}d'
            
            if stat == 'mean':
                df_result[col_name] = df_result[value_col].rolling(window=window).mean()
            elif stat == 'std':
                df_result[col_name] = df_result[value_col].rolling(window=window).std()
            elif stat == 'min':
                df_result[col_name] = df_result[value_col].rolling(window=window).min()
            elif stat == 'max':
                df_result[col_name] = df_result[value_col].rolling(window=window).max()
            elif stat == 'sum':
                df_result[col_name] = df_result[value_col].rolling(window=window).sum()
            elif stat == 'median':
                df_result[col_name] = df_result[value_col].rolling(window=window).median()
        
        logger.info(f"Added {len(stats)} rolling statistics with {window}-day window")
        return df_result
        
    except Exception as e:
        logger.error(f"Error calculating rolling statistics: {e}")
        return df

def lag_features(df: pd.DataFrame, value_col: str, lags: List[int] = [1, 7, 30]) -> pd.DataFrame:
    """
    Create lag features for a column
    
    Args:
        df: DataFrame to process
        value_col: Column to create lags for
        lags: List of lag periods
    
    Returns:
        DataFrame with lag features
    """
    
    try:
        df_result = df.copy()
        
        for lag in lags:
            col_name = f'{value_col}_lag_{lag}'
            df_result[col_name] = df_result[value_col].shift(lag)
        
        logger.info(f"Added {len(lags)} lag features for {value_col}")
        return df_result
        
    except Exception as e:
        logger.error(f"Error creating lag features: {e}")
        return df

def calculate_change_metrics(df: pd.DataFrame, value_col: str, 
                           periods: List[int] = [1, 7, 30]) -> pd.DataFrame:
    """
    Calculate change metrics (absolute and percentage change)
    
    Args:
        df: DataFrame to process
        value_col: Column to calculate changes for
        periods: List of periods for change calculation
    
    Returns:
        DataFrame with change metrics
    """
    
    try:
        df_result = df.copy()
        
        for period in periods:
            # Absolute change
            abs_change_col = f'{value_col}_change_{period}d'
            df_result[abs_change_col] = df_result[value_col] - df_result[value_col].shift(period)
            
            # Percentage change
            pct_change_col = f'{value_col}_pct_change_{period}d'
            df_result[pct_change_col] = df_result[value_col].pct_change(periods=period) * 100
        
        logger.info(f"Added change metrics for {len(periods)} periods")
        return df_result
        
    except Exception as e:
        logger.error(f"Error calculating change metrics: {e}")
        return df

def resample_timeseries(df: pd.DataFrame, datetime_col: str, value_col: str,
                       freq: str = 'H', agg_func: str = 'mean') -> pd.DataFrame:
    """
    Resample time series data to different frequency
    
    Args:
        df: DataFrame to resample
        datetime_col: Name of datetime column
        value_col: Name of value column to aggregate
        freq: Resampling frequency ('H', 'D', '15min', etc.)
        agg_func: Aggregation function ('mean', 'sum', 'max', 'min')
    
    Returns:
        Resampled DataFrame
    """
    
    try:
        df_copy = df.copy()
        df_copy[datetime_col] = pd.to_datetime(df_copy[datetime_col])
        df_copy = df_copy.set_index(datetime_col)
        
        if agg_func == 'mean':
            resampled = df_copy[value_col].resample(freq).mean()
        elif agg_func == 'sum':
            resampled = df_copy[value_col].resample(freq).sum()
        elif agg_func == 'max':
            resampled = df_copy[value_col].resample(freq).max()
        elif agg_func == 'min':
            resampled = df_copy[value_col].resample(freq).min()
        elif agg_func == 'count':
            resampled = df_copy[value_col].resample(freq).count()
        else:
            logger.error(f"Unknown aggregation function: {agg_func}")
            return df
        
        result = resampled.reset_index()
        result.columns = [datetime_col, value_col]
        
        logger.info(f"Resampled data from {len(df)} to {len(result)} records at {freq} frequency")
        return result
        
    except Exception as e:
        logger.error(f"Error resampling time series: {e}")
        return df

def create_summary_stats(df: pd.DataFrame, group_cols: List[str], 
                        value_cols: List[str]) -> pd.DataFrame:
    """
    Create summary statistics grouped by specified columns
    
    Args:
        df: DataFrame to summarize
        group_cols: Columns to group by
        value_cols: Columns to calculate statistics for
    
    Returns:
        DataFrame with summary statistics
    """
    
    try:
        summary_list = []
        
        for value_col in value_cols:
            if value_col not in df.columns:
                logger.warning(f"Column {value_col} not found in DataFrame")
                continue
            
            # Calculate statistics for each group
            grouped = df.groupby(group_cols)[value_col].agg([
                'count', 'mean', 'median', 'std', 'min', 'max',
                lambda x: x.quantile(0.25),  # Q1
                lambda x: x.quantile(0.75),  # Q3
                lambda x: x.quantile(0.95)   # 95th percentile
            ])
            
            # Rename columns
            grouped.columns = [
                f'{value_col}_count', f'{value_col}_mean', f'{value_col}_median',
                f'{value_col}_std', f'{value_col}_min', f'{value_col}_max',
                f'{value_col}_q25', f'{value_col}_q75', f'{value_col}_p95'
            ]
            
            summary_list.append(grouped)
        
        # Combine all summaries
        if summary_list:
            summary = pd.concat(summary_list, axis=1).reset_index()
            logger.info(f"Created summary statistics for {len(value_cols)} columns")
            return summary
        else:
            logger.warning("No valid columns found for summary statistics")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error creating summary statistics: {e}")
        return pd.DataFrame()

def export_to_multiple_formats(df: pd.DataFrame, base_filename: str, 
                              formats: List[str] = ['csv', 'parquet', 'json']):
    """
    Export DataFrame to multiple file formats
    
    Args:
        df: DataFrame to export
        base_filename: Base filename (without extension)
        formats: List of formats to export to
    """
    
    try:
        exported_files = []
        
        for fmt in formats:
            filename = f"{base_filename}.{fmt}"
            
            if fmt == 'csv':
                df.to_csv(filename, index=False)
            elif fmt == 'parquet':
                df.to_parquet(filename, index=False)
            elif fmt == 'json':
                df.to_json(filename, orient='records', date_format='iso')
            elif fmt == 'xlsx':
                df.to_excel(filename, index=False)
            elif fmt == 'feather':
                df.to_feather(filename)
            else:
                logger.warning(f"Unsupported format: {fmt}")
                continue
            
            exported_files.append(filename)
            logger.info(f"Exported to {filename}")
        
        logger.info(f"Exported DataFrame to {len(exported_files)} formats")
        return exported_files
        
    except Exception as e:
        logger.error(f"Error exporting to multiple formats: {e}")
        return []

def compare_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, 
                      key_columns: List[str] = None) -> Dict[str, Any]:
    """
    Compare two DataFrames and return summary of differences
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        key_columns: Columns to use as keys for comparison
    
    Returns:
        Dictionary with comparison results
    """
    
    try:
        comparison = {
            'shape_df1': df1.shape,
            'shape_df2': df2.shape,
            'columns_df1': set(df1.columns),
            'columns_df2': set(df2.columns),
            'common_columns': set(df1.columns) & set(df2.columns),
            'columns_only_df1': set(df1.columns) - set(df2.columns),
            'columns_only_df2': set(df2.columns) - set(df1.columns),
        }
        
        # Data type comparison for common columns
        dtype_differences = {}
        for col in comparison['common_columns']:
            if df1[col].dtype != df2[col].dtype:
                dtype_differences[col] = {
                    'df1_dtype': str(df1[col].dtype),
                    'df2_dtype': str(df2[col].dtype)
                }
        comparison['dtype_differences'] = dtype_differences
        
        # Value comparison if key columns provided
        if key_columns and all(col in comparison['common_columns'] for col in key_columns):
            # Merge on key columns to compare values
            merged = df1.merge(df2, on=key_columns, suffixes=('_df1', '_df2'), how='outer')
            
            value_differences = {}
            for col in comparison['common_columns']:
                if col not in key_columns:
                    col1 = f"{col}_df1"
                    col2 = f"{col}_df2"
                    if col1 in merged.columns and col2 in merged.columns:
                        differences = merged[merged[col1] != merged[col2]]
                        if len(differences) > 0:
                            value_differences[col] = len(differences)
            
            comparison['value_differences'] = value_differences
            comparison['records_only_df1'] = len(merged[merged[f"{key_columns[0]}_df2"].isna()])
            comparison['records_only_df2'] = len(merged[merged[f"{key_columns[0]}_df1"].isna()])
        
        logger.info("DataFrame comparison completed")
        return comparison
        
    except Exception as e:
        logger.error(f"Error comparing DataFrames: {e}")
        return {}

def profile_dataframe(df: pd.DataFrame, sample_size: int = 10000) -> Dict[str, Any]:
    """
    Create a profile of a DataFrame with various statistics
    
    Args:
        df: DataFrame to profile
        sample_size: Sample size for performance (None for full dataset)
    
    Returns:
        Dictionary with profile information
    """
    
    try:
        # Sample if necessary
        df_sample = df.sample(n=min(sample_size, len(df))) if sample_size else df
        
        profile = {
            'shape': df.shape,
            'memory_usage_mb': memory_usage_mb(df),
            'dtypes': dict(df.dtypes.astype(str)),
            'missing_values': dict(df.isnull().sum()),
            'missing_percentages': dict((df.isnull().sum() / len(df) * 100).round(2)),
            'duplicate_rows': df.duplicated().sum(),
            'numeric_columns': list(df.select_dtypes(include=[np.number]).columns),
            'categorical_columns': list(df.select_dtypes(include=['object', 'category']).columns),
            'datetime_columns': list(df.select_dtypes(include=['datetime64']).columns),
        }
        
        # Statistics for numeric columns
        numeric_stats = {}
        for col in profile['numeric_columns']:
            if col in df_sample.columns:
                numeric_stats[col] = calculate_percentiles(df_sample[col])
        profile['numeric_statistics'] = numeric_stats
        
        # Unique value counts for categorical columns
        categorical_stats = {}
        for col in profile['categorical_columns']:
            if col in df_sample.columns:
                categorical_stats[col] = {
                    'unique_count': df_sample[col].nunique(),
                    'most_frequent': df_sample[col].value_counts().head(5).to_dict()
                }
        profile['categorical_statistics'] = categorical_stats
        
        logger.info(f"Created profile for DataFrame with shape {df.shape}")
        return profile
        
    except Exception as e:
        logger.error(f"Error profiling DataFrame: {e}")
        return {}

def health_check_s3_connection(bucket: str) -> bool:
    """
    Check if S3 connection is working
    
    Args:
        bucket: S3 bucket name to test
    
    Returns:
        Boolean indicating if connection is healthy
    """
    
    try:
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=bucket)
        logger.info(f"S3 connection healthy for bucket: {bucket}")
        return True
        
    except Exception as e:
        logger.error(f"S3 connection failed for bucket {bucket}: {e}")
        return False

def health_check_database_connection(connection_string: str) -> bool:
    """
    Check if database connection is working
    
    Args:
        connection_string: Database connection string
    
    Returns:
        Boolean indicating if connection is healthy
    """
    
    try:
        # This would need to be implemented based on your specific database
        # For now, return True as placeholder
        logger.info("Database connection check not implemented")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def system_health_check(config: Dict) -> Dict[str, bool]:
    """
    Perform comprehensive system health check
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Dictionary with health check results
    """
    
    try:
        health_results = {
            'system_resources': True,
            's3_connection': False,
            'database_connection': False,
            'disk_space': True,
            'memory_available': True
        }
        
        # Check system resources
        try:
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            health_results['memory_available'] = memory.percent < 90
            health_results['disk_space'] = disk.percent < 90
            
            if not health_results['memory_available']:
                logger.warning(f"Low memory: {memory.percent}% used")
            if not health_results['disk_space']:
                logger.warning(f"Low disk space: {disk.percent}% used")
                
        except Exception as e:
            logger.error(f"Error checking system resources: {e}")
            health_results['system_resources'] = False
        
        # Check S3 connection
        if 'bucket' in config:
            health_results['s3_connection'] = health_check_s3_connection(config['bucket'])
        
        # Check database connection
        if 'database' in config:
            health_results['database_connection'] = health_check_database_connection(
                config.get('database', {}).get('connection_string', '')
            )
        
        healthy_checks = sum(health_results.values())
        total_checks = len(health_results)
        
        logger.info(f"System health check: {healthy_checks}/{total_checks} checks passed")
        
        return health_results
        
    except Exception as e:
        logger.error(f"Error performing system health check: {e}")
        return {'overall_health': False}

class DataQualityChecker:
    """
    Class for performing data quality checks
    """
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.issues = []
    
    def check_missing_values(self, threshold: float = 0.5) -> 'DataQualityChecker':
        """Check for columns with high missing value rates"""
        missing_pct = self.df.isnull().sum() / len(self.df)
        high_missing = missing_pct[missing_pct > threshold]
        
        if len(high_missing) > 0:
            self.issues.append({
                'type': 'high_missing_values',
                'details': high_missing.to_dict(),
                'severity': 'high' if high_missing.max() > 0.8 else 'medium'
            })
        
        return self
    
    def check_duplicates(self, subset: List[str] = None) -> 'DataQualityChecker':
        """Check for duplicate rows"""
        duplicates = self.df.duplicated(subset=subset).sum()
        
        if duplicates > 0:
            duplicate_pct = (duplicates / len(self.df)) * 100
            self.issues.append({
                'type': 'duplicate_rows',
                'details': {'count': duplicates, 'percentage': duplicate_pct},
                'severity': 'high' if duplicate_pct > 10 else 'medium'
            })
        
        return self
    
    def check_outliers(self, columns: List[str] = None, method: str = 'iqr') -> 'DataQualityChecker':
        """Check for outliers in numeric columns"""
        if columns is None:
            columns = self.df.select_dtypes(include=[np.number]).columns
        
        outlier_summary = {}
        for col in columns:
            if col in self.df.columns:
                outliers = detect_outliers(self.df[col], method=method)
                outlier_count = outliers.sum()
                if outlier_count > 0:
                    outlier_pct = (outlier_count / len(self.df)) * 100
                    outlier_summary[col] = {'count': outlier_count, 'percentage': outlier_pct}
        
        if outlier_summary:
            self.issues.append({
                'type': 'outliers',
                'details': outlier_summary,
                'severity': 'medium'
            })
        
        return self
    
    def check_data_types(self, expected_types: Dict[str, str] = None) -> 'DataQualityChecker':
        """Check if columns have expected data types"""
        if expected_types:
            type_issues = {}
            for col, expected_type in expected_types.items():
                if col in self.df.columns:
                    actual_type = str(self.df[col].dtype)
                    if expected_type not in actual_type:
                        type_issues[col] = {'expected': expected_type, 'actual': actual_type}
            
            if type_issues:
                self.issues.append({
                    'type': 'incorrect_data_types',
                    'details': type_issues,
                    'severity': 'medium'
                })
        
        return self
    
    def check_value_ranges(self, range_checks: Dict[str, Dict[str, float]] = None) -> 'DataQualityChecker':
        """Check if numeric values are within expected ranges"""
        if range_checks:
            range_issues = {}
            for col, ranges in range_checks.items():
                if col in self.df.columns:
                    min_val = ranges.get('min')
                    max_val = ranges.get('max')
                    
                    issues = []
                    if min_val is not None:
                        below_min = (self.df[col] < min_val).sum()
                        if below_min > 0:
                            issues.append(f"{below_min} values below {min_val}")
                    
                    if max_val is not None:
                        above_max = (self.df[col] > max_val).sum()
                        if above_max > 0:
                            issues.append(f"{above_max} values above {max_val}")
                    
                    if issues:
                        range_issues[col] = issues
            
            if range_issues:
                self.issues.append({
                    'type': 'value_range_violations',
                    'details': range_issues,
                    'severity': 'medium'
                })
        
        return self
    
    def get_report(self) -> Dict[str, Any]:
        """Get complete data quality report"""
        return {
            'timestamp': datetime.now().isoformat(),
            'dataframe_shape': self.df.shape,
            'total_issues': len(self.issues),
            'issues': self.issues,
            'overall_quality': 'good' if len(self.issues) == 0 else 'needs_attention'
        }

def run_data_quality_check(df: pd.DataFrame, config: Dict = None) -> Dict[str, Any]:
    """
    Run comprehensive data quality check on DataFrame
    
    Args:
        df: DataFrame to check
        config: Configuration for quality checks
    
    Returns:
        Data quality report
    """
    
    try:
        checker = DataQualityChecker(df)
        
        # Run standard checks
        checker.check_missing_values(threshold=config.get('missing_threshold', 0.5) if config else 0.5)
        checker.check_duplicates()
        
        # Run additional checks if config provided
        if config:
            if 'outlier_columns' in config:
                checker.check_outliers(config['outlier_columns'])
            
            if 'expected_types' in config:
                checker.check_data_types(config['expected_types'])
            
            if 'range_checks' in config:
                checker.check_value_ranges(config['range_checks'])
        
        report = checker.get_report()
        
        # Log summary
        if report['total_issues'] == 0:
            logger.info("Data quality check passed with no issues")
        else:
            logger.warning(f"Data quality check found {report['total_issues']} issues")
            
        return report
        
    except Exception as e:
        logger.error(f"Error running data quality check: {e}")
        return {'error': str(e), 'overall_quality': 'unknown'}

def create_data_lineage_entry(source: str, target: str, transformation: str, 
                             timestamp: datetime = None) -> Dict[str, Any]:
    """
    Create a data lineage entry for tracking data flow
    
    Args:
        source: Source of the data
        target: Target/destination of the data
        transformation: Description of transformation applied
        timestamp: Timestamp of the operation
    
    Returns:
        Data lineage entry dictionary
    """
    
    if timestamp is None:
        timestamp = datetime.now()
    
    return {
        'timestamp': timestamp.isoformat(),
        'source': source,
        'target': target,
        'transformation': transformation,
        'user': os.getenv('USER', 'unknown'),
        'hostname': socket.gethostname()
    }

def log_data_lineage(lineage_entry: Dict[str, Any], bucket: str = None, 
                    local_file: str = None):
    """
    Log data lineage entry to S3 or local file
    
    Args:
        lineage_entry: Data lineage entry
        bucket: S3 bucket for lineage storage
        local_file: Local file for lineage storage
    """
    
    try:
        lineage_json = json.dumps(lineage_entry, indent=2)
        
        if bucket:
            # Store in S3
            s3_key = f"lineage/{datetime.now().strftime('%Y/%m/%d')}/{uuid.uuid4()}.json"
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=lineage_json,
                ContentType='application/json'
            )
            logger.info(f"Data lineage logged to S3: {s3_key}")
        
        if local_file:
            # Append to local file
            with open(local_file, 'a') as f:
                f.write(lineage_json + '\n')
            logger.info(f"Data lineage logged to file: {local_file}")
            
    except Exception as e:
        logger.error(f"Error logging data lineage: {e}")

class ConfigManager:
    """
    Manager for configuration handling with validation and defaults
    """
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = {}
        self.defaults = {}
        self.load_config()
    
    def load_config(self):
        """Load configuration from file"""
        try:
            self.config = load_config_file(self.config_path)
            logger.info(f"Configuration loaded from {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            self.config = {}
    
    def set_defaults(self, defaults: Dict[str, Any]):
        """Set default configuration values"""
        self.defaults = defaults
        logger.info("Default configuration values set")
    
    def get(self, key: str, default: Any = None):
        """Get configuration value with fallback to defaults"""
        try:
            # Handle nested keys like 'database.host'
            if '.' in key:
                keys = key.split('.')
                value = self.config
                for k in keys:
                    if isinstance(value, dict) and k in value:
                        value = value[k]
                    else:
                        value = None
                        break
            else:
                value = self.config.get(key)
            
            # Fallback to defaults if not found
            if value is None:
                if '.' in key:
                    keys = key.split('.')
                    default_value = self.defaults
                    for k in keys:
                        if isinstance(default_value, dict) and k in default_value:
                            default_value = default_value[k]
                        else:
                            default_value = None
                            break
                    value = default_value
                else:
                    value = self.defaults.get(key)
            
            # Final fallback to provided default
            if value is None:
                value = default
            
            return value
            
        except Exception as e:
            logger.error(f"Error getting config value for {key}: {e}")
            return default
    
    def validate_required_keys(self, required_keys: List[str]) -> bool:
        """Validate that all required keys are present"""
        return validate_config(self.config, required_keys)
    
    def reload(self):
        """Reload configuration from file"""
        self.load_config()

def create_backup(source_path: Union[str, Path], backup_dir: Union[str, Path] = None):
    """
    Create backup of a file or directory
    
    Args:
        source_path: Path to backup
        backup_dir: Directory to store backup (default: source_dir/backups)
    """
    
    try:
        source_path = Path(source_path)
        
        if backup_dir is None:
            backup_dir = source_path.parent / 'backups'
        else:
            backup_dir = Path(backup_dir)
        
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"{source_path.name}_{timestamp}"
        backup_path = backup_dir / backup_name
        
        if source_path.is_file():
            shutil.copy2(source_path, backup_path)
        elif source_path.is_dir():
            shutil.copytree(source_path, backup_path)
        else:
            logger.error(f"Source path does not exist: {source_path}")
            return None
        
        logger.info(f"Backup created: {backup_path}")
        return backup_path
        
    except Exception as e:
        logger.error(f"Error creating backup: {e}")
        return None

def compress_file(file_path: Union[str, Path], compression: str = 'gzip') -> Path:
    """
    Compress a file using specified compression method
    
    Args:
        file_path: Path to file to compress
        compression: Compression method ('gzip', 'bz2', 'lzma')
    
    Returns:
        Path to compressed file
    """
    
    try:
        file_path = Path(file_path)
        
        if compression == 'gzip':
            import gzip
            compressed_path = file_path.with_suffix(file_path.suffix + '.gz')
            with open(file_path, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif compression == 'bz2':
            import bz2
            compressed_path = file_path.with_suffix(file_path.suffix + '.bz2')
            with open(file_path, 'rb') as f_in:
                with bz2.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif compression == 'lzma':
            import lzma
            compressed_path = file_path.with_suffix(file_path.suffix + '.xz')
            with open(file_path, 'rb') as f_in:
                with lzma.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        else:
            logger.error(f"Unsupported compression method: {compression}")
            return file_path
        
        # Get compression ratio
        original_size = file_path.stat().st_size
        compressed_size = compressed_path.stat().st_size
        ratio = (1 - compressed_size / original_size) * 100
        
        logger.info(f"File compressed with {compression}: {format_bytes(original_size)} -> "
                   f"{format_bytes(compressed_size)} ({ratio:.1f}% reduction)")
        
        return compressed_path
        
    except Exception as e:
        logger.error(f"Error compressing file: {e}")
        return Path(file_path)

def decompress_file(file_path: Union[str, Path]) -> Path:
    """
    Decompress a file based on its extension
    
    Args:
        file_path: Path to compressed file
    
    Returns:
        Path to decompressed file
    """
    
    try:
        file_path = Path(file_path)
        
        if file_path.suffix == '.gz':
            import gzip
            decompressed_path = file_path.with_suffix('')
            with gzip.open(file_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif file_path.suffix == '.bz2':
            import bz2
            decompressed_path = file_path.with_suffix('')
            with bz2.open(file_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        elif file_path.suffix == '.xz':
            import lzma
            decompressed_path = file_path.with_suffix('')
            with lzma.open(file_path, 'rb') as f_in:
                with open(decompressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
        else:
            logger.warning(f"Unknown compression format: {file_path.suffix}")
            return file_path
        
        logger.info(f"File decompressed: {file_path} -> {decompressed_path}")
        return decompressed_path
        
    except Exception as e:
        logger.error(f"Error decompressing file: {e}")
        return Path(file_path)

def archive_old_data(data_dir: Union[str, Path], archive_dir: Union[str, Path],
                    max_age_days: int = 90, compress: bool = True):
    """
    Archive old data files to separate directory
    
    Args:
        data_dir: Directory containing data files
        archive_dir: Directory to store archived files
        max_age_days: Age threshold for archiving
        compress: Whether to compress archived files
    """
    
    try:
        data_dir = Path(data_dir)
        archive_dir = Path(archive_dir)
        archive_dir.mkdir(parents=True, exist_ok=True)
        
        archived_count = 0
        total_size_saved = 0
        
        for file_path in data_dir.rglob('*'):
            if file_path.is_file():
                age = get_file_age_days(file_path)
                
                if age > max_age_days:
                    # Create archive subdirectory structure
                    relative_path = file_path.relative_to(data_dir)
                    archive_file_path = archive_dir / relative_path
                    archive_file_path.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Move file to archive
                    shutil.move(str(file_path), str(archive_file_path))
                    archived_count += 1
                    
                    # Compress if requested
                    if compress:
                        compressed_path = compress_file(archive_file_path)
                        if compressed_path != archive_file_path:
                            archive_file_path.unlink()  # Remove uncompressed file
                            total_size_saved += archive_file_path.stat().st_size
        
        logger.info(f"Archived {archived_count} old files")
        if compress:
            logger.info(f"Compression saved {format_bytes(total_size_saved)}")
            
    except Exception as e:
        logger.error(f"Error archiving old data: {e}")

def create_manifest_file(directory: Union[str, Path], manifest_path: Union[str, Path] = None):
    """
    Create manifest file with checksums for files in directory
    
    Args:
        directory: Directory to create manifest for
        manifest_path: Path for manifest file (default: directory/manifest.txt)
    """
    
    try:
        import hashlib
        
        directory = Path(directory)
        if manifest_path is None:
            manifest_path = directory / 'manifest.txt'
        else:
            manifest_path = Path(manifest_path)
        
        manifest_entries = []
        
        for file_path in directory.rglob('*'):
            if file_path.is_file() and file_path != manifest_path:
                # Calculate MD5 checksum
                md5_hash = hashlib.md5()
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        md5_hash.update(chunk)
                
                relative_path = file_path.relative_to(directory)
                file_size = file_path.stat().st_size
                checksum = md5_hash.hexdigest()
                
                manifest_entries.append(f"{checksum}  {file_size}  {relative_path}")
        
        # Write manifest file
        with open(manifest_path, 'w') as f:
            f.write('\n'.join(manifest_entries))
        
        logger.info(f"Created manifest file with {len(manifest_entries)} entries: {manifest_path}")
        
    except Exception as e:
        logger.error(f"Error creating manifest file: {e}")

def verify_manifest(directory: Union[str, Path], manifest_path: Union[str, Path] = None) -> bool:
    """
    Verify files against manifest
    
    Args:
        directory: Directory to verify
        manifest_path: Path to manifest file
    
    Returns:
        Boolean indicating if verification passed
    """
    
    try:
        import hashlib
        
        directory = Path(directory)
        if manifest_path is None:
            manifest_path = directory / 'manifest.txt'
        else:
            manifest_path = Path(manifest_path)
        
        if not manifest_path.exists():
            logger.error(f"Manifest file not found: {manifest_path}")
            return False
        
        verification_errors = []
        
        with open(manifest_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                parts = line.strip().split('  ', 2)
                if len(parts) != 3:
                    continue
                
                expected_checksum, expected_size, relative_path = parts
                file_path = directory / relative_path
                
                if not file_path.exists():
                    verification_errors.append(f"Missing file: {relative_path}")
                    continue
                
                # Check file size
                actual_size = file_path.stat().st_size
                if str(actual_size) != expected_size:
                    verification_errors.append(f"Size mismatch for {relative_path}: "
                                             f"expected {expected_size}, got {actual_size}")
                    continue
                
                # Check checksum
                md5_hash = hashlib.md5()
                with open(file_path, 'rb') as f:
                    for chunk in iter(lambda: f.read(4096), b""):
                        md5_hash.update(chunk)
                
                actual_checksum = md5_hash.hexdigest()
                if actual_checksum != expected_checksum:
                    verification_errors.append(f"Checksum mismatch for {relative_path}: "
                                             f"expected {expected_checksum}, got {actual_checksum}")
        
        if verification_errors:
            logger.error(f"Manifest verification failed with {len(verification_errors)} errors:")
            for error in verification_errors[:10]:  # Log first 10 errors
                logger.error(f"  {error}")
            if len(verification_errors) > 10:
                logger.error(f"  ... and {len(verification_errors) - 10} more errors")
            return False
        else:
            logger.info("Manifest verification passed")
            return True
            
    except Exception as e:
        logger.error(f"Error verifying manifest: {e}")
        return False

def create_data_dictionary(df: pd.DataFrame, output_path: Union[str, Path] = None) -> Dict[str, Any]:
    """
    Create data dictionary for a DataFrame
    
    Args:
        df: DataFrame to document
        output_path: Optional path to save dictionary as JSON
    
    Returns:
        Data dictionary as dictionary
    """
    
    try:
        data_dict = {
            'metadata': {
                'created_date': datetime.now().isoformat(),
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': memory_usage_mb(df)
            },
            'columns': {}
        }
        
        for col in df.columns:
            col_info = {
                'data_type': str(df[col].dtype),
                'non_null_count': df[col].count(),
                'null_count': df[col].isnull().sum(),
                'null_percentage': round((df[col].isnull().sum() / len(df)) * 100, 2),
                'unique_count': df[col].nunique(),
                'unique_percentage': round((df[col].nunique() / len(df)) * 100, 2)
            }
            
            # Add type-specific information
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                col_info.update({
                    'min_value': float(df[col].min()) if pd.notna(df[col].min()) else None,
                    'max_value': float(df[col].max()) if pd.notna(df[col].max()) else None,
                    'mean_value': float(df[col].mean()) if pd.notna(df[col].mean()) else None,
                    'median_value': float(df[col].median()) if pd.notna(df[col].median()) else None,
                    'std_dev': float(df[col].std()) if pd.notna(df[col].std()) else None
                })
            
            elif df[col].dtype == 'object':
                # String/categorical columns
                value_counts = df[col].value_counts().head(10)
                col_info.update({
                    'top_values': value_counts.to_dict(),
                    'avg_length': df[col].astype(str).str.len().mean() if len(df) > 0 else 0
                })
            
            elif 'datetime' in str(df[col].dtype):
                col_info.update({
                    'min_date': df[col].min().isoformat() if pd.notna(df[col].min()) else None,
                    'max_date': df[col].max().isoformat() if pd.notna(df[col].max()) else None,
                    'date_range_days': (df[col].max() - df[col].min()).days if pd.notna(df[col].min()) and pd.notna(df[col].max()) else None
                })
            
            data_dict['columns'][col] = col_info
        
        # Save to file if requested
        if output_path:
            output_path = Path(output_path)
            with open(output_path, 'w') as f:
                json.dump(data_dict, f, indent=2, default=str)
            logger.info(f"Data dictionary saved to {output_path}")
        
        logger.info(f"Created data dictionary for {len(df.columns)} columns")
        return data_dict
        
    except Exception as e:
        logger.error(f"Error creating data dictionary: {e}")
        return {}

def generate_sample_data(schema: Dict[str, str], num_rows: int = 1000) -> pd.DataFrame:
    """
    Generate sample data based on schema definition
    
    Args:
        schema: Dictionary mapping column names to data types
        num_rows: Number of rows to generate
    
    Returns:
        DataFrame with sample data
    """
    
    try:
        np.random.seed(42)  # For reproducibility
        data = {}
        
        for col_name, col_type in schema.items():
            if col_type == 'int':
                data[col_name] = np.random.randint(1, 1000, num_rows)
            elif col_type == 'float':
                data[col_name] = np.random.uniform(0, 100, num_rows)
            elif col_type == 'string':
                categories = [f'Category_{i}' for i in range(1, 11)]
                data[col_name] = np.random.choice(categories, num_rows)
            elif col_type == 'datetime':
                start_date = datetime(2020, 1, 1)
                end_date = datetime(2024, 1, 1)
                time_range = end_date - start_date
                random_days = np.random.randint(0, time_range.days, num_rows)
                data[col_name] = [start_date + timedelta(days=int(day)) for day in random_days]
            elif col_type == 'boolean':
                data[col_name] = np.random.choice([True, False], num_rows)
            else:
                logger.warning(f"Unknown column type: {col_type}, using string default")
                data[col_name] = [f'Value_{i}' for i in range(num_rows)]
        
        df = pd.DataFrame(data)
        logger.info(f"Generated sample data with {num_rows} rows and {len(schema)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Error generating sample data: {e}")
        return pd.DataFrame()

def anonymize_dataframe(df: pd.DataFrame, sensitive_columns: List[str], 
                       method: str = 'mask') -> pd.DataFrame:
    """
    Anonymize sensitive columns in DataFrame
    
    Args:
        df: DataFrame to anonymize
        sensitive_columns: List of column names to anonymize
        method: Anonymization method ('mask', 'hash', 'remove')
    
    Returns:
        Anonymized DataFrame
    """
    
    try:
        df_anon = df.copy()
        
        for col in sensitive_columns:
            if col not in df_anon.columns:
                logger.warning(f"Column {col} not found in DataFrame")
                continue
            
            if method == 'mask':
                # Replace with masked values
                df_anon[col] = df_anon[col].astype(str).apply(
                    lambda x: 'X' * min(len(str(x)), 8) if pd.notna(x) else x
                )
            
            elif method == 'hash':
                # Replace with hash values
                import hashlib
                df_anon[col] = df_anon[col].astype(str).apply(
                    lambda x: hashlib.md5(str(x).encode()).hexdigest()[:8] if pd.notna(x) else x
                )
            
            elif method == 'remove':
                # Remove column entirely
                df_anon = df_anon.drop(columns=[col])
            
            else:
                logger.error(f"Unknown anonymization method: {method}")
        
        logger.info(f"Anonymized {len(sensitive_columns)} columns using {method} method")
        return df_anon
        
    except Exception as e:
        logger.error(f"Error anonymizing DataFrame: {e}")
        return df

def create_test_data_pipeline():
    """
    Create test data for pipeline validation
    
    Returns:
        Dictionary containing test datasets
    """
    
    try:
        # Create test schemas
        signal_schema = {
            'SignalID': 'string',
            'Timeperiod': 'datetime',
            'CallPhase': 'int',
            'Detector': 'int',
            'vol': 'int',
            'Date': 'datetime'
        }
        
        corridor_schema = {
            'SignalID': 'string',
            'Corridor': 'string',
            'Zone_Group': 'string',
            'Zone': 'string',
            'Latitude': 'float',
            'Longitude': 'float'
        }
        
        # Generate test data
        test_data = {
            'signals': generate_sample_data(signal_schema, 10000),
            'corridors': generate_sample_data(corridor_schema, 100),
            'travel_times': generate_sample_data({
                'Corridor': 'string',
                'Hour': 'datetime',
                'tti': 'float',
                'speed_mph': 'float'
            }, 5000)
        }
        
        # Add realistic constraints
        test_data['signals']['vol'] = np.clip(test_data['signals']['vol'], 0, 2000)
        test_data['signals']['CallPhase'] = np.random.choice([2, 6], len(test_data['signals']))
        test_data['travel_times']['tti'] = np.clip(test_data['travel_times']['tti'], 0.8, 3.0)
        test_data['travel_times']['speed_mph'] = np.clip(test_data['travel_times']['speed_mph'], 5, 80)
        
        logger.info("Created test data pipeline with realistic constraints")
        return test_data
        
    except Exception as e:
        logger.error(f"Error creating test data pipeline: {e}")
        return {}

# Utility decorators
def timing_decorator(func):
    """Decorator to time function execution"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            logger.info(f"{func.__name__} completed in {format_duration(duration)}")
            return result
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"{func.__name__} failed after {format_duration(duration)}: {e}")
            raise
    return wrapper

def memory_profiler_decorator(func):
    """Decorator to profile memory usage"""
    def wrapper(*args, **kwargs):
        start_memory = psutil.Process().memory_info().rss / 1024**2
        try:
            result = func(*args, **kwargs)
            end_memory = psutil.Process().memory_info().rss / 1024**2
            memory_change = end_memory - start_memory
            logger.info(f"{func.__name__} memory change: {memory_change:+.1f} MB")
            return result
        except Exception as e:
            end_memory = psutil.Process().memory_info().rss / 1024**2
            memory_change = end_memory - start_memory
            logger.error(f"{func.__name__} failed with memory change: {memory_change:+.1f} MB")
            raise
    return wrapper

def exception_handler_decorator(default_return=None, log_traceback=True):
    """Decorator to handle exceptions gracefully"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if log_traceback:
                    logger.error(f"Exception in {func.__name__}: {e}", exc_info=True)
                else:
                    logger.error(f"Exception in {func.__name__}: {e}")
                return default_return
        return wrapper
    return decorator

# Export commonly used functions and constants
__all__ = [
    # Core utility functions
    'setup_logging', 'load_config_file', 'validate_config', 'memory_usage_mb',
    # 'keep_trying', 'parallel_apply', 'chunked_processing',
    'keep_trying', 'parallel_apply',
    
    # S3 utilities
    'list_s3_objects_by_date', 'ensure_directory_exists',
    
    # File utilities
    'get_file_age_days', 'clean_old_files', 'compress_file', 'decompress_file',
    'create_backup', 'archive_old_data',
    
    # Data utilities
    'filter_business_days', 'add_time_features', 'calculate_percentiles',
    'detect_outliers', 'rolling_statistics', 'lag_features', 'calculate_change_metrics',
    'resample_timeseries', 'create_summary_stats', 'export_to_multiple_formats',
    
    # Data quality
    'DataQualityChecker', 'run_data_quality_check', 'profile_dataframe',
    'compare_dataframes', 'create_data_dictionary', 'anonymize_dataframe',
    
    # System utilities
    'format_bytes', 'format_duration', 'system_health_check', 'PerformanceMonitor',
    'batch_process', 'create_progress_tracker', 'validate_date_range',
    
    # Configuration and management
    'ConfigManager', 'create_data_lineage_entry', 'log_data_lineage',
    'create_manifest_file', 'verify_manifest',
    
    # Test utilities
    'generate_sample_data', 'create_test_data_pipeline',
    
    # Constants
    'WEEKDAYS', 'SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT',
    'COLORS', 'get_color', 'is_business_day',
    
    # Decorators
    'timing_decorator', 'memory_profiler_decorator', 'exception_handler_decorator'
]

if __name__ == "__main__":
    # Example usage and testing
    print("Signal Operations Utilities Module")
    print("==================================")
    
    # Test configuration loading
    try:
        config = ConfigManager('config.yaml')
        print(f"✓ Configuration manager initialized")
    except:
        print("✗ Configuration manager failed (expected if config.yaml not found)")
    
        # Test data generation
    try:
        test_schema = {'id': 'int', 'name': 'string', 'value': 'float', 'date': 'datetime'}
        sample_df = generate_sample_data(test_schema, 100)
        print(f"✓ Generated sample data: {sample_df.shape}")
    except Exception as e:
        print(f"✗ Sample data generation failed: {e}")
    
    # Test data quality check
    try:
        if 'sample_df' in locals():
            quality_report = run_data_quality_check(sample_df)
            print(f"✓ Data quality check completed: {quality_report['overall_quality']}")
    except Exception as e:
        print(f"✗ Data quality check failed: {e}")
    
    # Test utility functions
    try:
        # Test date functions
        test_date = datetime(2024, 3, 15)  # Friday
        is_bday = is_business_day(test_date)
        print(f"✓ Business day check: {test_date.strftime('%A')} is business day: {is_bday}")
        
        # Test file size formatting
        test_sizes = [1024, 1048576, 1073741824]
        for size in test_sizes:
            formatted = format_bytes(size)
            print(f"✓ Format bytes: {size} -> {formatted}")
        
        # Test duration formatting
        test_durations = [65, 3661, 86461]
        for duration in test_durations:
            formatted = format_duration(duration)
            print(f"✓ Format duration: {duration}s -> {formatted}")
            
    except Exception as e:
        print(f"✗ Utility function tests failed: {e}")
    
    # Test performance monitoring
    try:
        @timing_decorator
        @memory_profiler_decorator
        def test_function():
            import time
            time.sleep(0.1)
            return [i**2 for i in range(1000)]
        
        result = test_function()
        print(f"✓ Performance decorators work: computed {len(result)} values")
        
    except Exception as e:
        print(f"✗ Performance decorator tests failed: {e}")
    
    print("\nUtilities module ready for use!")
