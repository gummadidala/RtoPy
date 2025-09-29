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
import duckdb
from parquet_lib import read_s3_parquet_pattern_duckdb

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
        with open("Monthly_Report_AWS.yaml", 'r') as file:
            aws_conf = yaml.safe_load(file)
        
        # Set environment variables
        os.environ['AWS_ACCESS_KEY_ID'] = aws_conf['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        os.environ['AWS_DEFAULT_REGION'] = aws_conf['AWS_DEFAULT_REGION']
        
        # Update Athena configuration
        conf['athena']['uid'] = aws_conf['AWS_ACCESS_KEY_ID']
        conf['athena']['pwd'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        
        return conf, aws_conf
        
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return {}, {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return {}, {}

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

# Enhanced version of your existing function
def process_single_date_counts(date_str: str, 
                             bucket: str, 
                             conf_athena: Dict[str, Any],
                             use_duckdb: bool = True):
    """
    Enhanced single date processing with DuckDB option
    """
    if use_duckdb:
        # Use DuckDB for faster processing
        try:
            pattern = f"atspm/date={date_str}/atspm_*_{date_str}.parquet"
            return read_s3_parquet_pattern_duckdb(
                bucket=bucket,
                pattern=pattern,
                columns=['SignalID', 'TimeStamp', 'EventCode', 'EventParam', 'CallPhase']
            )
        except Exception as e:
            logger.error(f"DuckDB processing failed for {date_str}, falling back to original method: {e}")
    
    # Fallback to original implementation
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
        logger.error(f"Error processing {date_str}: {e}")
        return None

def get_usable_cores_enhanced() -> int:
    """
    Enhanced core detection that considers DuckDB availability
    If DuckDB is available, we can use fewer cores for parallel processing
    since DuckDB handles parallelization internally
    """
    try:
        total_cores = multiprocessing.cpu_count()
        # With DuckDB, use fewer parallel workers since DuckDB parallelizes internally
        return max(1, total_cores // 4)
    except ImportError:
        # Fallback to original implementation
        return get_usable_cores()

def optimize_processing_strategy(data_size_estimate: int, 
                               date_count: int,
                               signal_count: int) -> Dict[str, Any]:
    """
    Determine optimal processing strategy based on data characteristics
    
    Args:
        data_size_estimate: Estimated data size in MB
        date_count: Number of dates to process
        signal_count: Number of signals to process
    
    Returns:
        Dictionary with processing recommendations
    """
    
    strategy = {
        'use_duckdb': False,
        'batch_size': 1,
        'parallel_workers': get_usable_cores(),
        'chunk_by': 'date',
        'reasoning': []
    }
    
    # Large dataset - favor DuckDB
    if data_size_estimate > 1000:  # > 1GB
        strategy['use_duckdb'] = True
        strategy['parallel_workers'] = max(1, strategy['parallel_workers'] // 2)
        strategy['reasoning'].append("Large dataset detected - using DuckDB for efficiency")
    
    # Many small files - favor batching
    if date_count > 30 and signal_count < 100:
        strategy['batch_size'] = min(7, date_count // 4)  # Weekly batches
        strategy['chunk_by'] = 'date'
        strategy['reasoning'].append("Many dates with few signals - batching by date")
    
    # Many signals, few dates - different strategy
    if signal_count > 500 and date_count < 10:
        strategy['batch_size'] = min(50, signal_count // 10)
        strategy['chunk_by'] = 'signal'
        strategy['reasoning'].append("Many signals with few dates - batching by signal")
    
    # Very large scale - definitely use DuckDB
    if date_count > 100 or signal_count > 1000:
        strategy['use_duckdb'] = True
        strategy['parallel_workers'] = 2
        strategy['reasoning'].append("Very large scale processing - using DuckDB with minimal parallelization")
    
    return strategy

def adaptive_batch_processing(date_range: List[str],
                            bucket: str,
                            signal_ids: Optional[List[int]] = None,
                            **kwargs) -> pd.DataFrame:
    """
    Adaptively choose processing strategy based on data characteristics
    
    Args:
        date_range: List of dates to process
        bucket: S3 bucket name
        signal_ids: Optional signal IDs
        **kwargs: Additional processing parameters
    
    Returns:
        Processed results DataFrame
    """
    try:
        # Estimate data characteristics
        estimated_size = estimate_data_size(bucket, date_range, signal_ids)
        strategy = optimize_processing_strategy(
            estimated_size, 
            len(date_range), 
            len(signal_ids) if signal_ids else 1000  # Assume many signals if not specified
        )
        
        logger.info(f"Processing strategy: {strategy}")
        
        if strategy['use_duckdb']:
            # Use DuckDB-based processing
            return batch_process_dates_duckdb(
                date_range=date_range,
                bucket=bucket,
                signal_ids=signal_ids,
                **kwargs
            )
        else:
            # Use traditional parallel processing
            if strategy['chunk_by'] == 'date':
                # Process in date chunks
                all_results = []
                batch_size = strategy['batch_size']
                
                for i in range(0, len(date_range), batch_size):
                    batch_dates = date_range[i:i + batch_size]
                    
                    batch_results = parallel_process_dates(
                        date_range=batch_dates,
                        process_function=process_single_date_counts,
                        max_workers=strategy['parallel_workers'],
                        bucket=bucket,
                        **kwargs
                    )
                    
                    valid_results = [r for r in batch_results if r is not None]
                    if valid_results:
                        all_results.extend(valid_results)
                
                return pd.concat(all_results, ignore_index=True) if all_results else pd.DataFrame()
            
            else:
                # Standard parallel processing
                return parallel_process_dates(
                    date_range=date_range,
                    process_function=process_single_date_counts,
                    max_workers=strategy['parallel_workers'],
                    bucket=bucket,
                    **kwargs
                )
    
    except Exception as e:
        logger.error(f"Error in adaptive batch processing: {e}")
        return pd.DataFrame()

def estimate_data_size(bucket: str, 
                      date_range: List[str], 
                      signal_ids: Optional[List[int]] = None) -> int:
    """
    Estimate data size in MB for processing strategy decisions
    
    Args:
        bucket: S3 bucket name
        date_range: List of dates
        signal_ids: Optional signal IDs
    
    Returns:
        Estimated size in MB
    """
    try:
        import boto3
        s3_client = boto3.client('s3')
        
        # Sample a few files to estimate average size
        sample_size = 0
        sample_count = 0
        max_samples = 10
        
        for date_str in date_range[:3]:  # Sample first 3 dates
            if sample_count >= max_samples:
                break
                
            prefix = f"atspm/date={date_str}/"
            
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=5  # Sample few files per date
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    sample_size += obj['Size']
                    sample_count += 1
                    if sample_count >= max_samples:
                        break
        
        if sample_count == 0:
            return 100  # Default estimate
        
        # Calculate average file size
        avg_file_size = sample_size / sample_count
        
        # Estimate total files
        if signal_ids:
            total_files = len(date_range) * len(signal_ids)
        else:
            # Estimate based on typical signal count
            total_files = len(date_range) * 200  # Assume 200 signals per day
        
        # Total size in MB
        total_size_mb = (total_files * avg_file_size) / (1024 * 1024)
        
        logger.info(f"Estimated data size: {total_size_mb:.1f} MB for {total_files} files")
        return int(total_size_mb)
        
    except Exception as e:
        logger.warning(f"Could not estimate data size: {e}. Using default estimate.")
        return 500  # Default estimate

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

def get_date_from_string(
    x,
    s3bucket=None,
    s3prefix=None,
    table_include_regex_pattern="_dy_",
    table_exclude_regex_pattern="_outstand|_report|_resolv|_task|_tpri|_tsou|_tsub|_ttyp|_kabco|_maint|_ops|_safety|_alert|_udc|_summ",
):
    if type(x) == str:
        re_da = re.compile(r"\d+(?= *days ago)")  # Added 'r' prefix for raw string
        if x == "today":
            x = datetime.today().strftime("%F")
        elif x == "yesterday":
            x = (datetime.today() - timedelta(days=1)).strftime("%F")
        elif re_da.search(x):
            d = int(re_da.search(x).group())
            x = (datetime.today() - timedelta(days=d)).strftime("%F")
        elif x == "first_missing":
            if s3bucket is not None and s3prefix is not None:
                s3 = boto3.resource("s3")
                all_dates = [
                    re.search(r"(?<=date\=)(\d+-\d+-\d+)", obj.key)  # Added 'r' prefix for raw string
                    for obj in s3.Bucket(s3bucket).objects.filter(Prefix=s3prefix)
                ]
                all_dates = [date_.group() for date_ in all_dates if date_ is not None]
                first_missing = datetime.strptime(max(all_dates), "%Y-%m-%d") + timedelta(days=1)
                first_missing = min(first_missing, datetime.today() - timedelta(days=1))
                x = first_missing.strftime("%F")
            else:
                raise Exception("Must include arguments for s3bucket and s3prefix")
    else:
        x = x.strftime("%F")
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

def get_signalids_from_s3(date: str, bucket: str) -> List[str]:
    """
    Get list of signal IDs from S3 for a specific date
    
    Args:
        date: Date string
        bucket: S3 bucket name
    
    Returns:
        List of signal IDs
    """
    
    try:
        s3_client = boto3.client('s3')
        
        # List objects for the date
        prefix = f"mark/counts_1hr/date={date}/"
        
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        signal_ids = set()
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # Extract signal ID from filename if possible
                # This would depend on your file naming convention
                filename = os.path.basename(key)
                if filename.startswith('signal_'):
                    signal_id = filename.split('_')[1].split('.')[0]
                    signal_ids.add(signal_id)
        
        return list(signal_ids)
        
    except Exception as e:
        logger.error(f"Error getting signal IDs from S3: {e}")
        return []

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
        print("latest_keyy:", latest_key)
        # Read the file
        if latest_key.endswith('.qs'):
            det_config = s3_read_qs(config['bucket'], latest_key)
        else:
            det_config = s3read_using(
                pd.read_feather,
                bucket=config['bucket'],
                object=latest_key
            )
        
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
        
        # Upload to S3
        s3_upload_parquet_date_split(
            signal_details,
            bucket=config['bucket'],
            prefix='signal_details',
            table_name='signal_details',
            conf_athena=config['athena']
        )
        
        logger.info(f"Wrote signal details for {date_str}: {len(signals_list)} signals")
        
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

def batch_process_dates_duckdb(date_range: List[str], 
                              bucket: str,
                              signal_ids: Optional[List[int]] = None,
                              aggregation_type: str = 'daily') -> pd.DataFrame:
    """
    Batch process dates using DuckDB instead of parallel individual processing
    
    Args:
        date_range: List of dates to process
        bucket: S3 bucket name
        signal_ids: Optional list of signal IDs
        aggregation_type: Type of aggregation ('daily', 'hourly', 'signal_summary')
    
    Returns:
        Processed results DataFrame
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        # Build file pattern for date range
        if len(date_range) == 1:
            pattern = f"atspm/date={date_range[0]}/atspm_*_{date_range[0]}.parquet"
        else:
            # Use pattern with multiple dates
            date_pattern = "{" + ",".join(date_range) + "}"
            pattern = f"atspm/date={date_pattern}/atspm_*.parquet"
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        # Add signal filter if specified
        signal_filter = ""
        if signal_ids:
            signal_list = ",".join(map(str, signal_ids))
            signal_filter = f"AND SignalID IN ({signal_list})"
        
        if aggregation_type == 'daily':
            query = f"""
            SELECT 
                SignalID,
                Date,
                CallPhase,
                COUNT(*) as total_events,
                COUNT(CASE WHEN EventCode IN (81, 82) THEN 1 END) as vehicle_events,
                COUNT(CASE WHEN EventCode IN (90, 91) THEN 1 END) as ped_events,
                COUNT(DISTINCT DATE_TRUNC('hour', TimeStamp)) as active_hours,
                MIN(TimeStamp) as first_event,
                MAX(TimeStamp) as last_event
            FROM read_parquet('{s3_pattern}')
            WHERE 1=1 {signal_filter}
            GROUP BY SignalID, Date, CallPhase
            ORDER BY SignalID, Date, CallPhase
            """
            
        elif aggregation_type == 'hourly':
            query = f"""
            SELECT 
                SignalID,
                Date,
                CallPhase,
                DATE_TRUNC('hour', TimeStamp) as Hour,
                COUNT(*) as total_events,
                COUNT(CASE WHEN EventCode IN (81, 82) THEN 1 END) as vehicle_events,
                COUNT(CASE WHEN EventCode IN (90, 91) THEN 1 END) as ped_events
            FROM read_parquet('{s3_pattern}')
            WHERE 1=1 {signal_filter}
            GROUP BY SignalID, Date, CallPhase, Hour
            ORDER BY SignalID, Date, CallPhase, Hour
            """
            
        elif aggregation_type == 'signal_summary':
            query = f"""
            SELECT 
                SignalID,
                COUNT(DISTINCT Date) as active_days,
                COUNT(*) as total_events,
                COUNT(CASE WHEN EventCode IN (81, 82) THEN 1 END) as total_vehicle_events,
                COUNT(CASE WHEN EventCode IN (90, 91) THEN 1 END) as total_ped_events,
                MIN(Date) as first_date,
                MAX(Date) as last_date,
                AVG(CASE WHEN EventCode IN (81, 82) THEN 1.0 ELSE 0.0 END) * 100 as vehicle_event_pct
            FROM read_parquet('{s3_pattern}')
            WHERE 1=1 {signal_filter}
            GROUP BY SignalID
            ORDER BY SignalID
            """
        else:
            raise ValueError(f"Unsupported aggregation type: {aggregation_type}")
        
        result = conn.execute(query).df()
        conn.close()
        
        logger.info(f"Successfully batch processed {len(date_range)} dates with {aggregation_type} aggregation")
        return result
        
    except Exception as e:
        logger.error(f"Error in batch date processing: {e}")
        # Fallback to original parallel processing
        return parallel_process_dates(date_range, process_single_date_counts)

def generate_report_from_s3_duckdb(bucket: str, 
                                  report_config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Generate comprehensive report directly from S3 using DuckDB
    
    Args:
        bucket: S3 bucket name
        report_config: Configuration for report generation
            {
                'date_range': ['2024-01-01', '2024-01-31'],
                'signal_ids': [1001, 1002, 1003],
                'include_uptime': True,
                'include_volume': True,
                'include_performance': True,
                'include_pedestrian': True
            }
    
    Returns:
        Dictionary with different report sections
    """
    try:
        date_range = report_config['date_range']
        signal_ids = report_config.get('signal_ids', [])
        
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        # Build file pattern
        if len(date_range) > 1:
            start_date, end_date = min(date_range), max(date_range)
            # For date ranges, we might need to be more specific about the pattern
            pattern = f"atspm/date=*/atspm_*.parquet"  # Will filter in WHERE clause
        else:
            pattern = f"atspm/date={date_range[0]}/atspm_*_{date_range[0]}.parquet"
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        # Build filters
        filters = []
        if len(date_range) > 1:
            start_date, end_date = min(date_range), max(date_range)
            filters.append(f"Date BETWEEN '{start_date}' AND '{end_date}'")
        
        if signal_ids:
            signal_list = ",".join(map(str, signal_ids))
            filters.append(f"SignalID IN ({signal_list})")
        
        where_clause = "WHERE " + " AND ".join(filters) if filters else ""
        
        report_sections = {}
        
        # Uptime Analysis
        if report_config.get('include_uptime', True):
            uptime_query = f"""
            WITH hourly_activity AS (
                SELECT 
                    SignalID,
                    Date,
                    DATE_TRUNC('hour', TimeStamp) as Hour,
                    COUNT(*) as event_count
                FROM read_parquet('{s3_pattern}')
                {where_clause}
                GROUP BY SignalID, Date, Hour
            ),
            daily_uptime AS (
                SELECT 
                    SignalID,
                    Date,
                    COUNT(*) as active_hours,
                    24 as expected_hours,
                    (COUNT(*)::float / 24) * 100 as uptime_pct
                FROM hourly_activity
                GROUP BY SignalID, Date
            )
            SELECT 
                SignalID,
                AVG(uptime_pct) as avg_uptime_pct,
                MIN(uptime_pct) as min_uptime_pct,
                MAX(uptime_pct) as max_uptime_pct,
                COUNT(*) as total_days,
                SUM(CASE WHEN uptime_pct >= 90 THEN 1 ELSE 0 END) as days_above_90pct
            FROM daily_uptime
            GROUP BY SignalID
            ORDER BY SignalID
            """
            report_sections['uptime'] = conn.execute(uptime_query).df()
        
        # Volume Analysis
        if report_config.get('include_volume', True):
            volume_query = f"""
            SELECT 
                SignalID,
                CallPhase,
                COUNT(CASE WHEN EventCode IN (81, 82) THEN 1 END) as total_volume,
                COUNT(CASE WHEN EventCode IN (81, 82) THEN 1 END) / COUNT(DISTINCT Date) as avg_daily_volume,
                EXTRACT(dow FROM Date) as day_of_week,
                AVG(CASE WHEN EventCode IN (81, 82) AND EXTRACT(hour FROM TimeStamp) BETWEEN 6 AND 9 THEN 1.0 ELSE 0 END) * COUNT(*) as am_peak_volume,
                AVG(CASE WHEN EventCode IN (81, 82) AND EXTRACT(hour FROM TimeStamp) BETWEEN 16 AND 19 THEN 1.0 ELSE 0 END) * COUNT(*) as pm_peak_volume
            FROM read_parquet('{s3_pattern}')
            {where_clause}
            GROUP BY SignalID, CallPhase, day_of_week
            ORDER BY SignalID, CallPhase, day_of_week
            """
            report_sections['volume'] = conn.execute(volume_query).df()
        
        # Performance Metrics
        if report_config.get('include_performance', True):
            performance_query = f"""
            WITH signal_performance AS (
                SELECT 
                    SignalID,
                    Date,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT CallPhase) as active_phases,
                    MAX(TimeStamp) - MIN(TimeStamp) as operation_span,
                    COUNT(CASE WHEN EventCode = 1 THEN 1 END) as phase_begin_events,
                    COUNT(CASE WHEN EventCode = 8 THEN 1 END) as phase_end_events
                FROM read_parquet('{s3_pattern}')
                {where_clause}
                GROUP BY SignalID, Date
            )
            SELECT 
                SignalID,
                AVG(total_events) as avg_daily_events,
                AVG(active_phases) as avg_active_phases,
                AVG(EXTRACT(epoch FROM operation_span) / 3600) as avg_operation_hours,
                AVG(phase_begin_events) as avg_phase_begins,
                AVG(phase_end_events) as avg_phase_ends,
                CASE WHEN AVG(phase_begin_events) > 0 
                     THEN AVG(phase_end_events) / AVG(phase_begin_events) 
                     ELSE 0 END as phase_completion_ratio
            FROM signal_performance
            GROUP BY SignalID
            ORDER BY SignalID
            """
            report_sections['performance'] = conn.execute(performance_query).df()
        
        # Pedestrian Analysis
        if report_config.get('include_pedestrian', True):
            pedestrian_query = f"""
            SELECT 
                SignalID,
                CallPhase,
                COUNT(CASE WHEN EventCode = 90 THEN 1 END) as ped_calls,
                COUNT(CASE WHEN EventCode = 91 THEN 1 END) as ped_begins,
                COUNT(CASE WHEN EventCode = 92 THEN 1 END) as ped_ends,
                EXTRACT(dow FROM Date) as day_of_week,
                EXTRACT(hour FROM TimeStamp) as hour_of_day,
                COUNT(*) as total_ped_events
            FROM read_parquet('{s3_pattern}')
            {where_clause}
            AND EventCode IN (90, 91, 92)
            GROUP BY SignalID, CallPhase, day_of_week, hour_of_day
            ORDER BY SignalID, CallPhase, day_of_week, hour_of_day
            """
            report_sections['pedestrian'] = conn.execute(pedestrian_query).df()
        
        conn.close()
        
        logger.info(f"Successfully generated comprehensive report with {len(report_sections)} sections")
        return report_sections
        
    except Exception as e:
        logger.error(f"Error generating report from S3: {e}")
        return {}

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
    AM_PEAK_HOURS, PM_PEAK_HOURS = get_peak_hours(conf)
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
    'send_notification', 'generate_processing_summary'
]
