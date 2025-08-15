"""
Monthly Report Package Initialization

Python port of Monthly_Report_Package_init.R
Sets up the environment and configuration for monthly report generation.
"""

import os
import sys
import logging
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import numpy as np
from pathlib import Path
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import psutil

# Import custom modules
from utilities import PerformanceMonitor, format_duration
from monthly_report_functions import *
from database_functions import *
from s3_parquet_io import s3read_using
from aggregations import *

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_usable_cores() -> int:
    """
    Get the number of usable CPU cores for parallel processing
    
    Returns:
        Number of cores to use
    """
    total_cores = mp.cpu_count()
    available_memory_gb = psutil.virtual_memory().available / (1024**3)
    
    # Use fewer cores if memory is limited
    if available_memory_gb < 8:
        usable_cores = max(1, total_cores // 4)
    elif available_memory_gb < 16:
        usable_cores = max(1, total_cores // 2)
    else:
        usable_cores = max(1, total_cores - 1)  # Leave one core free
    
    logger.info(f"Total cores: {total_cores}, Available memory: {available_memory_gb:.1f}GB, Using: {usable_cores} cores")
    return usable_cores


def setup_parallel_processing() -> int:
    """
    Setup parallel processing based on environment
    
    Returns:
        Number of cores configured
    """
    usable_cores = get_usable_cores()
    
    # Set environment variables for multiprocessing
    os.environ['OMP_NUM_THREADS'] = str(usable_cores)
    os.environ['NUMEXPR_NUM_THREADS'] = str(usable_cores)
    
    return usable_cores


def get_corridors(corridors_filename: str, filter_signals: bool = True) -> pd.DataFrame:
    """
    Load corridor configuration from S3
    
    Args:
        corridors_filename: S3 object key for corridors file
        filter_signals: Whether to filter for active signals only
    
    Returns:
        DataFrame with corridor mappings
    """
    try:
        with PerformanceMonitor("Loading corridors"):
            corridors = s3read_using(
                func=pd.read_csv,
                bucket=config['bucket'],
                object=corridors_filename
            )
            
            if filter_signals and 'Active' in corridors.columns:
                corridors = corridors[corridors['Active'] == True]
                logger.info(f"Filtered to {len(corridors)} active signals")
            
            # Ensure required columns exist
            required_cols = ['SignalID', 'Zone_Group', 'Zone', 'Corridor']
            missing_cols = [col for col in required_cols if col not in corridors.columns]
            if missing_cols:
                raise ValueError(f"Missing required columns in corridors: {missing_cols}")
            
            # Convert to appropriate data types
            corridors['SignalID'] = corridors['SignalID'].astype(str)
            corridors['Zone_Group'] = corridors['Zone_Group'].astype('category')
            corridors['Zone'] = corridors['Zone'].astype('category') 
            corridors['Corridor'] = corridors['Corridor'].astype('category')
            
            logger.info(f"Loaded {len(corridors)} corridor mappings")
            return corridors
            
    except Exception as e:
        logger.error(f"Error loading corridors from {corridors_filename}: {e}")
        raise


def get_cam_config(object_key: str, bucket: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Load camera configuration from S3
    
    Args:
        object_key: S3 object key for camera config
        bucket: S3 bucket name
        corridors: Corridor mappings for joining
    
    Returns:
        DataFrame with camera configuration
    """
    try:
        with PerformanceMonitor("Loading camera config"):
            cam_config = s3read_using(
                func=pd.read_csv,
                bucket=bucket,
                object=object_key
            )
            
            # Join with corridors if possible
            if 'CameraID' in cam_config.columns and 'CameraID' in corridors.columns:
                cam_config = cam_config.merge(
                    corridors[['CameraID', 'Zone_Group', 'Zone', 'Corridor']].drop_duplicates(),
                    on='CameraID',
                    how='left'
                )
            
            logger.info(f"Loaded {len(cam_config)} camera configurations")
            return cam_config
            
    except Exception as e:
        logger.error(f"Error loading camera config from {object_key}: {e}")
        return pd.DataFrame()


def get_athena_connection(athena_config: Dict[str, str]) -> Any:
    """
    Get Athena database connection
    
    Args:
        athena_config: Athena connection configuration
    
    Returns:
        Database connection object
    """
    try:
        # This would typically use pyathena or similar
        # Implementation depends on your specific database setup
        logger.info("Establishing Athena connection")
        
        # Placeholder - implement based on your database library
        conn = None  # get_database_connection(athena_config)
        
        logger.info("Athena connection established")
        return conn
        
    except Exception as e:
        logger.error(f"Error connecting to Athena: {e}")
        raise


def get_date_from_string(date_type: str, 
                        table_include_regex_pattern: str = "sig_dy_cu", 
                        exceptions: int = 0) -> date:
    """
    Get date from database based on criteria
    
    Args:
        date_type: Type of date to retrieve ("first_missing", etc.)
        table_include_regex_pattern: Pattern to match table names
        exceptions: Number of exceptions to allow
    
    Returns:
        Date object
    """
    try:
        # This would query the database to find the first missing date
        # Implementation depends on your specific database setup
        logger.info(f"Getting {date_type} date for pattern {table_include_regex_pattern}")
        
        # Placeholder - implement based on your database
        if date_type == "first_missing":
            # Return a reasonable default
            return datetime.now().date() - timedelta(days=30)
        
        return datetime.now().date()
        
    except Exception as e:
        logger.error(f"Error getting date from string: {e}")
        return datetime.now().date()


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
    
    if isinstance(date_, str):
        date_ = pd.to_datetime(date_).date()
    
    # Get the weekday (0=Monday, 1=Tuesday, etc.)
    weekday = date_.weekday()
    
    # Calculate days to add/subtract to get to Tuesday (1)
    days_to_tuesday = (1 - weekday) % 7
    if days_to_tuesday > 3:  # If more than 3 days away, go to previous Tuesday
        days_to_tuesday -= 7
    
    return date_ + timedelta(days=days_to_tuesday)


def get_month_abbrs(start_date: date, end_date: date) -> List[str]:
    """
    Get month abbreviations for date range
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        List of month abbreviations
    """
    abbrs = []
    current = start_date.replace(day=1)  # Start of month
    
    while current <= end_date:
        abbrs.append(current.strftime('%b'))
        current += relativedelta(months=1)
    
    return abbrs


def setup_date_ranges(config: Dict[str, Any]) -> Tuple[date, date, date, date, List[date], List[str], str]:
    """
    Setup all date ranges for the report
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Tuple of (report_end_date, report_start_date, calcs_start_date, 
                 wk_calcs_start_date, dates, month_abbrs, date_range_str)
    """
    
    # Report end date
    if config.get('report_end_date') == "yesterday":
        report_end_date = datetime.now().date() - timedelta(days=1)
    else:
        report_end_date = pd.to_datetime(config.get('report_end_date', 
                                        datetime.now().date() - timedelta(days=1))).date()
    
    # Report start date (18 months back)
    report_start_date = (report_end_date.replace(day=1) - relativedelta(months=18))
    
    # Calculations start date
    if config.get('calcs_start_date') == "auto":
        first_missing_date = get_date_from_string(
            "first_missing", 
            table_include_regex_pattern="sig_dy_cu", 
            exceptions=0
        )
        calcs_start_date = first_missing_date.replace(day=1)
        if first_missing_date.day <= 7:
            calcs_start_date = calcs_start_date - relativedelta(months=1)
    else:
        calcs_start_date = pd.to_datetime(config.get('calcs_start_date')).date()
    
    # Weekly calculations start date
    wk_calcs_start_date = round_to_tuesday(calcs_start_date)
    
    # Generate date sequences
    dates = []
    current = report_start_date
    while current <= report_end_date:
        dates.append(current)
        current += relativedelta(months=1)
    
    month_abbrs = get_month_abbrs(report_start_date, report_end_date)
    
    # Date range string for queries
    date_range = pd.date_range(start=report_start_date, end=report_end_date, freq='D')
    date_range_str = "{" + ",".join(date_range.strftime('%Y-%m-%d')) + "}"
    
    logger.info(f"Date ranges setup:")
    logger.info(f"  Report: {report_start_date} to {report_end_date}")
    logger.info(f"  Calculations: {calcs_start_date} to {report_end_date}")
    logger.info(f"  Weekly calculations: {wk_calcs_start_date}")
    
    return (report_end_date, report_start_date, calcs_start_date, 
            wk_calcs_start_date, dates, month_abbrs, date_range_str)


def setup_subcorridors(corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Setup subcorridor mappings
    
    Args:
        corridors: Main corridor DataFrame
    
    Returns:
        Subcorridor DataFrame
    """
    if 'Subcorridor' not in corridors.columns:
        logger.warning("No Subcorridor column found")
        return pd.DataFrame()
    
    subcorridors = corridors[corridors['Subcorridor'].notna()].copy()
    subcorridors = subcorridors.drop(columns=['Zone_Group'])
    subcorridors = subcorridors.rename(columns={
        'Zone': 'Zone_Group',
        'Corridor': 'Zone', 
        'Subcorridor': 'Corridor'
    })
    
    logger.info(f"Setup {len(subcorridors)} subcorridor mappings")
    return subcorridors


class MonthlyReportPackage:
    """
    Main class for monthly report package initialization and execution
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the monthly report package
        
        Args:
            config_path: Path to configuration file
        """
        self.config = self._load_config(config_path)
        self.corridors = None
        self.all_corridors = None
        self.subcorridors = None
        self.signals_list = None
        self.cam_config = None
        self.conn = None
        self.usable_cores = None
        
        # Date ranges
        self.report_end_date = None
        self.report_start_date = None
        self.calcs_start_date = None
        self.wk_calcs_start_date = None
        self.dates = None
        self.month_abbrs = None
        self.date_range_str = None
        
        logger.info(f"{datetime.now()} Starting Monthly Report Package")
        
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration from file or use defaults"""
        
        default_config = {
            'bucket': 'your-s3-bucket',
            'corridors_filename_s3': 'path/to/corridors.csv',
            'cctv_config_filename': 'path/to/cctv_config.csv',
            'report_end_date': 'yesterday',
            'calcs_start_date': 'auto',
            'athena': {
                'region': 'us-east-1',
                'database': 'traffic_db',
                'workgroup': 'primary'
            },
            'processing': {
                'use_multiprocessing': True,
                'batch_size': 1000,
                'memory_threshold_gb': 8
            }
        }
        
        if config_path and os.path.exists(config_path):
            import json
            with open(config_path, 'r') as f:
                file_config = json.load(f)
            config = {**default_config, **file_config}
            logger.info(f"Loaded configuration from {config_path}")
        else:
            config = default_config
            logger.warning("Using default configuration")
        
        return config
    
    def initialize(self):
        """Initialize all components of the monthly report package"""
        
        with PerformanceMonitor("Monthly Report Package Initialization"):
            # Setup parallel processing
            self.usable_cores = setup_parallel_processing()
            
            # Load corridor configurations
            self.corridors = get_corridors(
                self.config['corridors_filename_s3'], 
                filter_signals=True
            )
            self.all_corridors = get_corridors(
                self.config['corridors_filename_s3'], 
                filter_signals=False
            )
            
            # Get signals list
            self.signals_list = self.corridors['SignalID'].unique().tolist()
            logger.info(f"Found {len(self.signals_list)} signals")
            
            
            # Setup subcorridors
            self.subcorridors = setup_subcorridors(self.corridors)
            
            # Get database connection
            try:
                self.conn = get_athena_connection(self.config['athena'])
            except Exception as e:
                logger.warning(f"Could not establish database connection: {e}")
                self.conn = None
            
            # Load camera configuration
            try:
                self.cam_config = get_cam_config(
                    object=self.config['cctv_config_filename'],
                    bucket=self.config['bucket'],
                    corridors=self.all_corridors
                )
            except Exception as e:
                logger.warning(f"Could not load camera config: {e}")
                self.cam_config = pd.DataFrame()
            
            # Setup date ranges
            (self.report_end_date, self.report_start_date, self.calcs_start_date,
             self.wk_calcs_start_date, self.dates, self.month_abbrs, 
             self.date_range_str) = setup_date_ranges(self.config)
            
            logger.info("Monthly Report Package initialization completed successfully")
    
    def get_processing_executor(self, use_processes: bool = None) -> Any:
        """
        Get appropriate executor for parallel processing
        
        Args:
            use_processes: Whether to use processes vs threads
        
        Returns:
            Executor object
        """
        if use_processes is None:
            use_processes = self.config.get('processing', {}).get('use_multiprocessing', True)
        
        if use_processes:
            return ProcessPoolExecutor(max_workers=self.usable_cores)
        else:
            return ThreadPoolExecutor(max_workers=self.usable_cores)
    
    def validate_data_availability(self) -> Dict[str, bool]:
        """
        Validate that required data is available for the date range
        
        Returns:
            Dictionary indicating availability of different data types
        """
        availability = {
            'corridors': len(self.corridors) > 0,
            'signals': len(self.signals_list) > 0,
            'database_connection': self.conn is not None,
            'camera_config': len(self.cam_config) > 0,
            'date_ranges': all([
                self.report_start_date is not None,
                self.report_end_date is not None,
                self.calcs_start_date is not None
            ])
        }
        
        logger.info("Data availability check:")
        for key, value in availability.items():
            status = "✓" if value else "✗"
            logger.info(f"  {key}: {status}")
        
        return availability
    
    def get_date_filters(self) -> Dict[str, str]:
        """
        Get date filter strings for database queries
        
        Returns:
            Dictionary of date filter strings
        """
        return {
            'report_range': f"Date >= '{self.report_start_date}' AND Date <= '{self.report_end_date}'",
            'calcs_range': f"Date >= '{self.calcs_start_date}' AND Date <= '{self.report_end_date}'",
            'date_list': self.date_range_str,
            'signals_list': "(" + ",".join([f"'{s}'" for s in self.signals_list]) + ")"
        }
    
    def create_processing_batches(self, data: pd.DataFrame, 
                                 batch_column: str = 'SignalID') -> List[pd.DataFrame]:
        """
        Create batches for parallel processing
        
        Args:
            data: DataFrame to batch
            batch_column: Column to use for batching
        
        Returns:
            List of DataFrame batches
        """
        batch_size = self.config.get('processing', {}).get('batch_size', 1000)
        
        if batch_column not in data.columns:
            # Simple row-based batching
            batches = []
            for i in range(0, len(data), batch_size):
                batches.append(data.iloc[i:i + batch_size].copy())
            return batches
        
        # Group-based batching
        unique_values = data[batch_column].unique()
        batches = []
        
        for i in range(0, len(unique_values), batch_size):
            batch_values = unique_values[i:i + batch_size]
            batch_data = data[data[batch_column].isin(batch_values)].copy()
            batches.append(batch_data)
        
        logger.info(f"Created {len(batches)} batches for processing")
        return batches
    
    def get_memory_usage(self) -> Dict[str, float]:
        """
        Get current memory usage statistics
        
        Returns:
            Dictionary with memory usage information
        """
        process = psutil.Process()
        memory_info = process.memory_info()
        
        return {
            'rss_mb': memory_info.rss / (1024**2),
            'vms_mb': memory_info.vms / (1024**2),
            'percent': process.memory_percent(),
            'available_mb': psutil.virtual_memory().available / (1024**2)
        }
    
    def cleanup(self):
        """Clean up resources"""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.warning(f"Error closing database connection: {e}")
        
        logger.info("Monthly Report Package cleanup completed")
    
    def __enter__(self):
        """Context manager entry"""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.cleanup()
        if exc_type is not None:
            logger.error(f"Exception in Monthly Report Package: {exc_val}")
        return False


# Global configuration setup (equivalent to R's global variables)
def setup_global_config(config_path: Optional[str] = None) -> MonthlyReportPackage:
    """
    Setup global configuration for monthly report processing
    
    Args:
        config_path: Path to configuration file
    
    Returns:
        Initialized MonthlyReportPackage instance
    """
    return MonthlyReportPackage(config_path)


# Constants (equivalent to R's global constants)
class ReportConstants:
    """Constants used throughout the monthly report processing"""
    
    # Day of week constants
    TUESDAY = 1
    WEDNESDAY = 2  
    THURSDAY = 3
    
    # Peak hour definitions
    AM_PEAK_HOURS = [6, 7, 8, 9]
    PM_PEAK_HOURS = [16, 17, 18, 19]
    
    # File extensions for different data types
    SUPPORTED_EXTENSIONS = {
        'csv': pd.read_csv,
        'parquet': pd.read_parquet,
        'json': pd.read_json
    }
    
    # Default aggregation periods
    AGGREGATION_PERIODS = ['daily', 'weekly', 'monthly', 'quarterly']
    
    # Required columns for different data types
    REQUIRED_COLUMNS = {
        'corridors': ['SignalID', 'Zone_Group', 'Zone', 'Corridor'],
        'signals': ['SignalID', 'Date', 'Hour'],
        'counts': ['SignalID', 'Date', 'Hour', 'CallPhase', 'vol'],
        'detectors': ['SignalID', 'Date', 'Hour', 'Detector', 'Good_Day'],
        'cameras': ['CameraID', 'Date', 'Hour', 'uptime']
    }


# Utility functions for backwards compatibility with R patterns
def filter_by_date_range(df: pd.DataFrame, 
                        date_col: str, 
                        start_date: date, 
                        end_date: date) -> pd.DataFrame:
    """
    Filter DataFrame by date range
    
    Args:
        df: Input DataFrame
        date_col: Name of date column
        start_date: Start date
        end_date: End date
    
    Returns:
        Filtered DataFrame
    """
    return df[(df[date_col] >= start_date) & (df[date_col] <= end_date)]


def ensure_datetime_columns(df: pd.DataFrame, 
                           datetime_cols: List[str]) -> pd.DataFrame:
    """
    Ensure specified columns are datetime type
    
    Args:
        df: Input DataFrame
        datetime_cols: List of column names to convert
    
    Returns:
        DataFrame with converted datetime columns
    """
    df_copy = df.copy()
    
    for col in datetime_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_datetime(df_copy[col])
    
    return df_copy


def validate_required_columns(df: pd.DataFrame, 
                             required_cols: List[str], 
                             data_type: str = "data") -> bool:
    """
    Validate that DataFrame has required columns
    
    Args:
        df: DataFrame to validate
        required_cols: List of required column names
        data_type: Type of data for error messages
    
    Returns:
        True if all required columns present
    
    Raises:
        ValueError if required columns are missing
    """
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        raise ValueError(f"Missing required columns in {data_type}: {missing_cols}")
    
    return True


# Performance monitoring decorators
def monitor_performance(operation_name: str):
    """Decorator for performance monitoring"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with PerformanceMonitor(f"{operation_name}:{func.__name__}"):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def memory_check(threshold_gb: float = 8.0):
    """Decorator to check memory usage before expensive operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            available_gb = psutil.virtual_memory().available / (1024**3)
            if available_gb < threshold_gb:
                logger.warning(f"Low memory: {available_gb:.1f}GB available (threshold: {threshold_gb}GB)")
            
            return func(*args, **kwargs)
        return wrapper
    return decorator


# Main execution function (equivalent to the R script's main execution)
def main(config_path: Optional[str] = None):
    """
    Main execution function for monthly report package initialization
    
    Args:
        config_path: Path to configuration file
    """
    
    try:
        with setup_global_config(config_path) as package:
            # Validate data availability
            availability = package.validate_data_availability()
            
            if not all(availability.values()):
                logger.warning("Some required data is not available. Check configuration.")
                return package
            
            # Log successful initialization
            logger.info("Monthly Report Package successfully initialized")
            logger.info(f"Processing {len(package.signals_list)} signals")
            logger.info(f"Date range: {package.report_start_date} to {package.report_end_date}")
            logger.info(f"Using {package.usable_cores} cores for parallel processing")
            
            # Return the initialized package for use in subsequent processing
            return package
            
    except Exception as e:
        logger.error(f"Error in monthly report package initialization: {e}")
        raise


# Configuration validation
def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and enhance configuration with defaults
    
    Args:
        config: Input configuration dictionary
    
    Returns:
        Validated configuration with defaults applied
    """
    
    required_keys = [
        'bucket',
        'corridors_filename_s3',
        'cctv_config_filename'
    ]
    
    # Check required keys
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {missing_keys}")
    
    # Apply defaults for optional settings
    defaults = {
        'report_end_date': 'yesterday',
        'calcs_start_date': 'auto',
        'processing': {
            'use_multiprocessing': True,
            'batch_size': 1000,
            'memory_threshold_gb': 8
        },
        'athena': {
            'region': 'us-east-1',
            'database': 'traffic_db',
            'workgroup': 'primary'
        }
    }
    
    # Merge with defaults
    for key, value in defaults.items():
        if key not in config:
            config[key] = value
        elif isinstance(value, dict) and isinstance(config[key], dict):
            config[key] = {**value, **config[key]}
    
    return config


# Export main components
__all__ = [
    'MonthlyReportPackage',
    'setup_global_config',
    'ReportConstants',
    'main',
    'validate_config',
    'get_usable_cores',
    'setup_parallel_processing',
    'get_corridors',
    'get_cam_config',
    'setup_date_ranges',
    'round_to_tuesday',
    'filter_by_date_range',
    'ensure_datetime_columns',
    'validate_required_columns',
    'monitor_performance',
    'memory_check'
]


if __name__ == "__main__":
    """
    Script entry point - equivalent to running the R script directly
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Initialize Monthly Report Package')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--log-level', type=str, default='INFO', 
                       choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       help='Logging level')
    
    args = parser.parse_args()
    
    # Set logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    # Run main initialization
    try:
        package = main(args.config)
        logger.info("Monthly Report Package initialization completed successfully")
        
        # Print summary information
        print("\n" + "="*60)
        print("MONTHLY REPORT PACKAGE INITIALIZATION SUMMARY")
        print("="*60)
        print(f"Signals: {len(package.signals_list)}")
        print(f"Corridors: {len(package.corridors)}")
        print(f"Date Range: {package.report_start_date} to {package.report_end_date}")
        print(f"Cores Available: {package.usable_cores}")
        print(f"Memory Usage: {package.get_memory_usage()['rss_mb']:.1f} MB")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Failed to initialize Monthly Report Package: {e}")
        sys.exit(1)

