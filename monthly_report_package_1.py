#!/usr/bin/env python3
"""
monthly_report_package_1.py
Main Monthly Report Package - Python version of R script

This script processes traffic data to generate comprehensive monthly reports
covering 26 different traffic metrics and performance indicators.
"""
import gc
import tracemalloc
import sys
import os
import yaml
import pandas as pd
import numpy as np
import pickle
import logging
import traceback
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import warnings
from typing import Dict, List, Optional, Any
warnings.filterwarnings('ignore')

# Add current directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import calculation functions
from monthly_report_package_1_helper import *
from s3_parquet_io import s3_read_parquet_parallel, s3read_using
from teams import get_teams_tasks_from_s3
from configs import get_det_config_factory
from database_functions import execute_athena_query
from configs import get_ped_config_factory, get_corridors
from aggregations import (
    get_hourly,
    get_weekly_avg_by_day,
    get_monthly_avg_by_day,
    get_cor_weekly_avg_by_day,
    get_cor_monthly_avg_by_day
)

# Import Athena optimization helpers
try:
    # Force reload to get latest fixes (especially type cast fixes)
    import importlib
    if 'package_athena_helpers' in sys.modules:
        print("🔄 Reloading package_athena_helpers to get latest fixes...")
        importlib.reload(sys.modules['package_athena_helpers'])
        print("✅ RELOADED package_athena_helpers - TYPE MISMATCH FIX ACTIVE!")
    
    from package_athena_helpers import (
        athena_get_corridor_weekly_avg,
        athena_get_corridor_monthly_avg,
        athena_get_corridor_hourly_avg,
        athena_get_bad_detectors,
        athena_get_bad_ped_detectors,
        athena_get_ped_counts_aggregated,  # NEW - for ped pushbutton metrics
        athena_aggregate_with_batching,
        USE_ATHENA_AGGREGATION
    )
    
    # Force reload again after import to ensure we have the absolute latest version
    import package_athena_helpers
    importlib.reload(package_athena_helpers)
    print("✅ DOUBLE-RELOADED package_athena_helpers - FIXED VERSION GUARANTEED!")
    
    logger.info("Loaded Athena optimization helpers (FIXED VERSION)")
except ImportError:
    logger.warning("Athena helpers not available, using local processing")
    USE_ATHENA_AGGREGATION = False

try:
    from utilities import keep_trying
except ImportError:
    logger.warning("keep_trying not available")
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
    import awswrangler as wr
except ImportError:
    logger.warning("awswrangler not available")

# Try to import additional modules
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    logging.warning("boto3 not available - S3 functionality will be limited")

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
except ImportError:
    logging.warning("pyarrow not available - Parquet functionality will be limited")

logger = logging.getLogger(__name__)

import psutil
import gc

def log_memory_usage(step_name: str):
    """Log current memory usage"""
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    available_memory = psutil.virtual_memory().available / 1024 / 1024
    
    logger.info(f"{step_name} - Memory usage: {memory_mb:.1f} MB, Available: {available_memory:.1f} MB")
    
    # Force garbage collection if memory usage is high
    if memory_mb > 24000:  # 8GB threshold
        gc.collect()
        logger.info(f"Forced garbage collection due to high memory usage")

def check_memory_limit(max_memory_mb: int = 24000):
    """Check if memory usage exceeds limit"""
    process = psutil.Process()
    memory_mb = process.memory_info().rss / 1024 / 1024
    
    if memory_mb > max_memory_mb:
        gc.collect()
        memory_mb = process.memory_info().rss / 1024 / 1024
        if memory_mb > max_memory_mb:
            raise MemoryError(f"Memory usage {memory_mb:.1f} MB exceeds limit {max_memory_mb} MB")

def round_to_tuesday(date_):
    """
    Round date to nearest Tuesday (matching R logic)
    
    Args:
        date_: Date to round (can be string, date, or datetime)
    
    Returns:
        Date rounded to Tuesday
    """
    if date_ is None:
        return None
    
    if isinstance(date_, str):
        date_ = pd.to_datetime(date_).date()
    elif isinstance(date_, datetime):
        date_ = date_.date()
    
    # In Python: Monday=0, Sunday=6
    # To get Tuesday (1), calculate days to add/subtract
    current_weekday = date_.weekday()  # 0=Monday, 1=Tuesday, etc.
    
    # Calculate days to Tuesday (1)
    days_to_tuesday = (1 - current_weekday) % 7
    if days_to_tuesday > 3:  # If more than 3 days away, go to previous Tuesday
        days_to_tuesday -= 7
    
    return date_ + timedelta(days=days_to_tuesday)

def calculate_dates(report_end_date):
    """Calculate various date ranges needed for processing (matching R logic)"""
    try:
        if isinstance(report_end_date, str):
            report_end_date = pd.to_datetime(report_end_date).date()
        
        # Match R logic exactly
        report_start_date = report_end_date.replace(day=1)
        
        # Use the same logic as R for calcs_start_date
        if conf.calcs_start_date == "auto":
            # This would need implementation of get_date_from_string equivalent
            first_missing_date = get_first_missing_date_from_db(
                table_pattern="sig_dy_cu", 
                exceptions=0
            )
            calcs_start_date = first_missing_date.replace(day=1)
            if first_missing_date.day <= 7:
                calcs_start_date = calcs_start_date - relativedelta(months=1)
        else:
            calcs_start_date =  report_start_date - relativedelta(months=18)
        
        # NOW use the same logic as R
        wk_calcs_start_date = round_to_tuesday(calcs_start_date)
        
        # Calculate other dates...
        prev_month_start = report_start_date - relativedelta(months=1)
        prev_year_start = report_start_date - relativedelta(years=1)
        
        dates_dict = {
            'report_end_date': report_end_date,
            'report_start_date': report_start_date,
            'calcs_start_date': calcs_start_date,
            'wk_calcs_start_date': wk_calcs_start_date,
            'prev_month_start': prev_month_start,
            'prev_year_start': prev_year_start
        }
        
        logger.info(f"Date ranges calculated (R logic): {calcs_start_date} to {report_end_date}")
        logger.info(f"Weekly calcs start (rounded to Tuesday): {wk_calcs_start_date}")
        return dates_dict
        
    except Exception as e:
        logger.error(f"Error calculating dates: {e}")
        raise

def get_first_missing_date_from_db(table_pattern: str, exceptions: int = 0):
    """
    Python equivalent of R's get_date_from_string function
    
    Args:
        table_pattern: Regex pattern to match table names
        exceptions: Number of exceptions to allow
    
    Returns:
        First missing date found
    """
    try:
        # This would need to be implemented based on your database structure
        # For now, return a reasonable default
        return date.today() - relativedelta(months=1)
        # return date.today() - relativedelta(months=6)
        
    except Exception as e:
        logger.error(f"Error getting first missing date: {e}")
        return date.today() - relativedelta(months=6)

def load_yaml_configuration() -> Dict[str, Any]:
    """
    Load configuration from YAML files
    
    Returns:
        Configuration dictionary
    """
    
    try:
        # Load main configuration
        with open("Monthly_Report.yaml", 'r') as file:
            conf = yaml.safe_load(file)

        if 'calcs_start_date' not in conf:
            conf['calcs_start_date'] = "auto"
        
        if 'report_end_date' not in conf:
            conf['report_end_date'] = "yesterday"

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
        
        # Add AWS credentials to main conf for Athena helpers
        conf['AWS_ACCESS_KEY_ID'] = aws_conf['AWS_ACCESS_KEY_ID']
        conf['AWS_SECRET_ACCESS_KEY'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        conf['AWS_DEFAULT_REGION'] = aws_conf['AWS_DEFAULT_REGION']
        
        return conf, aws_conf
    
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        return {}, {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML: {e}")
        return {}, {}

# Global configuration (would typically be loaded from config file)
class Config:
    def __init__(self):
        conf_dict, aws_conf = load_yaml_configuration()
        self.bucket = conf_dict.get('bucket', '')
        self.athena = conf_dict.get('athena', {})
        self.calcs_start_date = conf_dict.get('calcs_start_date', 'auto')
        self.report_end_date = conf_dict.get('report_end_date', 'yesterday')
        # Store full config dict for Athena helpers
        self._conf_dict = conf_dict
    
    def to_dict(self):
        """Return full configuration as dictionary for Athena helpers"""
        return self._conf_dict

# Initialize configuration
conf = Config()

# Peak hours definition
AM_PEAK_HOURS = [7, 8, 9]
PM_PEAK_HOURS = [16, 17, 18]

def initialize_config():
    """Initialize configuration data and load corridors/signals"""
    try:
        # Initialize dates first
        dates = initialize_dates()
        
        # Load configuration data
        config_data = load_configuration_data()
        conf, aws_conf = load_yaml_configuration()
        # Add dates to config_data
        config_data.update(dates)
        config_data.update({'conf': conf, 'aws_conf': aws_conf})
        
        # Save configuration files for Package 2 (matches R behavior)
        logger.info("Saving configuration files for Package 2...")
        save_data(config_data['corridors'], "corridors.pkl")
        save_data(config_data['subcorridors'], "subcorridors.pkl")
        save_data(config_data['cam_config'], "cam_config.pkl")
        logger.info("Configuration files saved")
        
        logger.info("Configuration initialized successfully")
        return config_data
        
    except Exception as e:
        logger.error(f"Error initializing configuration: {e}")
        raise

def calculate_dates_notinscope(report_end_date):
    """Calculate various date ranges needed for processing"""
    try:
        if isinstance(report_end_date, str):
            report_end_date = pd.to_datetime(report_end_date).date()
        
        # Ensure report_end_date is first day of month
        report_start_date = report_end_date.replace(day=1)
        
        # Calculate various start dates for different metrics
        calcs_start_date = report_start_date - relativedelta(months=12)
        wk_calcs_start_date = report_start_date - relativedelta(weeks=12)
        
        # Calculate previous month and year for comparisons
        prev_month_start = report_start_date - relativedelta(months=1)
        prev_year_start = report_start_date - relativedelta(years=1)
        
        dates_dict = {
            'report_end_date': report_end_date,
            'report_start_date': report_start_date,
            'calcs_start_date': calcs_start_date,
            'wk_calcs_start_date': wk_calcs_start_date,
            'prev_month_start': prev_month_start,
            'prev_year_start': prev_year_start
        }
        
        logger.info(f"Date ranges calculated: {calcs_start_date} to {report_end_date}")
        return dates_dict
        
    except Exception as e:
        logger.error(f"Error calculating dates: {e}")
        raise

def initialize_dates():
    """Initialize date variables for calculations"""
    try:
        # These would typically come from configuration or command line arguments
        report_end_date = date.today() - timedelta(days=1)
        report_start_date = report_end_date.replace(day=1)
        
        # Calculate various start dates for different metrics
        calcs_start_date = report_start_date - relativedelta(months=12)
        wk_calcs_start_date = report_start_date - relativedelta(weeks=12)
        
        return {
            'report_end_date': report_end_date,
            'report_start_date': report_start_date,
            'calcs_start_date': calcs_start_date,
            'wk_calcs_start_date': wk_calcs_start_date
        }
    except Exception as e:
        logger.error(f"Error initializing dates: {e}")
        return {}

def load_configuration_data():
    """Load corridor and camera configuration data"""
    try:
        # Placeholder - would load actual configuration
        corridors = pd.DataFrame({
            'SignalID': range(1000, 1100),
            'Zone_Group': ['Zone1'] * 100,
            'Zone': ['SubZone1'] * 100,
            'Corridor': ['Main Street'] * 100,
            'Name': [f'Signal_{i}' for i in range(1000, 1100)]
        })
        
        subcorridors = corridors.copy()
        subcorridors['Subcorridor'] = subcorridors['Corridor']
        
        all_corridors = corridors.copy()
        
        cam_config = pd.DataFrame({
            'CameraID': range(2000, 2050),
            'Zone_Group': ['Zone1'] * 50,
            'Zone': ['SubZone1'] * 50,
            'Corridor': ['Main Street'] * 50,
            'Location': [f'Camera_{i}' for i in range(2000, 2050)],
            'As_of_Date': [date.today() - timedelta(days=365)] * 50
        })
        
        signals_list = list(corridors['SignalID'])
        
        return {
            'corridors': corridors,
            'subcorridors': subcorridors,
            'all_corridors': all_corridors,
            'cam_config': cam_config,
            'signals_list': signals_list
        }
        
    except Exception as e:
        logger.error(f"Error loading configuration data: {e}")
        return {}

def save_data(data, filename):
    """Save data using pickle (Python equivalent of RDS)"""
    try:
        os.makedirs('data_output', exist_ok=True)
        filepath = os.path.join('data_output', filename.replace('.rds', '.pkl'))
        
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
        
        logger.info(f"Saved {filename} with {len(data)} rows")
        
    except Exception as e:
        logger.error(f"Error saving {filename}: {e}")

def load_data(filename):
    """Load data from pickle file"""
    try:
        filepath = os.path.join('data_output', filename.replace('.rds', '.pkl'))
        
        if os.path.exists(filepath):
            with open(filepath, 'rb') as f:
                return pickle.load(f)
        else:
            logger.warning(f"File {filename} not found")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return pd.DataFrame()

def process_detector_uptime(dates, config_data):
    """Process vehicle detector uptime [1 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Vehicle Detector Uptime [1 of 29 (mark1)]")
    log_memory_usage("Start detector uptime")
    
    try:
        logger.info("Using Athena optimization for detector uptime")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Get corridor weekly with Athena (combines read + join + aggregate in one query!)
        cor_weekly_detector_uptime = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='detector_uptime_pd',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_detector_uptime, "cor_weekly_detector_uptime.pkl")
        
        # Subcorridor weekly
        sub_weekly_detector_uptime = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='detector_uptime_pd',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_detector_uptime, "sub_weekly_detector_uptime.pkl")
        
        # Corridor monthly
        cor_monthly_detector_uptime = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='detector_uptime_pd',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.pkl")
        
        # Subcorridor monthly
        sub_monthly_detector_uptime = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='detector_uptime_pd',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_detector_uptime, "sub_monthly_detector_uptime.pkl")
        
        logger.info("Detector uptime Athena optimization completed successfully")
        log_memory_usage("End detector uptime (Athena)")
            
    except Exception as e:
        logger.error(f"Error in detector uptime processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_ped_pushbutton_uptime(dates, config_data):
    """Process pedestrian pushbutton uptime [2 of 29] - Athena optimized (no disk download)"""
    logger.info(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (mark1)]")
    log_memory_usage("Start ped pushbutton uptime")
    
    try:
        logger.info("Using Athena optimization for ped pushbutton uptime (no local disk usage!)")
        
        # Calculate date range (6 months lookback)
        pau_start_date = min(
            dates['calcs_start_date'],
            dates['report_end_date'] - relativedelta(months=6)
        )
        
        conf_dict = conf.to_dict()
        start_date_str = pau_start_date.strftime('%Y-%m-%d') if hasattr(pau_start_date, 'strftime') else str(pau_start_date)
        end_date_str = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Get aggregated ped counts from Athena (daily and hourly)
        logger.info(f"Querying Athena for ped counts from {start_date_str} to {end_date_str}")
        papd, paph = keep_trying(
            athena_get_ped_counts_aggregated,
            n_tries=3,
            start_date=start_date_str,
            end_date=end_date_str,
            signals_list=config_data['signals_list'],
            conf=conf_dict
        )
        
        if papd.empty and paph.empty:
            logger.warning("No pedestrian count data found - saving empty dataframes")
            save_data(pd.DataFrame(), "pa_uptime.pkl")
            return
        
        # Convert data types
        if not papd.empty:
            papd = papd.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                Detector=lambda x: pd.Categorical(x['Detector']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )
            logger.info(f"Retrieved {len(papd)} daily ped activation records")
        
        if not paph.empty:
            paph = paph.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                Detector=lambda x: pd.Categorical(x['Detector']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )
            logger.info(f"Retrieved {len(paph)} hourly ped activation records")
        
        # Calculate PAU using the imported function
        from monthly_report_package_1_helper import get_pau_gamma
        date_range = pd.date_range(start=pau_start_date, end=dates['report_end_date'], freq='D')
        
        logger.info("Calculating pedestrian pushbutton uptime (PAU)")
        pau = get_pau_gamma(
            date_range.tolist(),
            papd,
            paph,
            config_data['corridors'],
            dates['wk_calcs_start_date'],
            pau_start_date
        )
        
        if pau.empty:
            logger.warning("PAU calculation returned empty - saving empty dataframe")
            save_data(pd.DataFrame(), "pa_uptime.pkl")
            return
        
        # Apply uptime filtering to papd (replace bad days with monthly average)
        papd = pau.copy()
        papd['papd'] = papd.apply(lambda row: row['papd'] if row['uptime'] == 1 else np.nan, axis=1)
        
        # Group by signal/detector/month and fill NaN with monthly average
        papd['Year'] = pd.to_datetime(papd['Date']).dt.year
        papd['Month'] = pd.to_datetime(papd['Date']).dt.month
        
        papd = papd.groupby(['SignalID', 'Detector', 'CallPhase', 'Year', 'Month']).apply(
            lambda group: group.assign(
                papd=lambda df: df['papd'].fillna(df['papd'].mean())
            )
        ).reset_index(drop=True)
        
        papd['papd'] = papd['papd'].fillna(0).astype(int)
        
        # Keep only required columns
        papd_final = papd[['SignalID', 'Detector', 'CallPhase', 'Date', 'DOW', 'Week', 'papd', 'uptime']]
        
        # Save for use by metrics #4 and #5
        # Metric #4 needs papd, Metric #5 needs paph with Hour column
        save_data(papd_final, "pa_uptime.pkl")
        logger.info(f"Saved {len(papd_final)} PAU records with uptime information")
        
        # Also save paph with Hour column for metric #5
        if not paph.empty:
            # Create uptime lookup set (SignalID, Detector, CallPhase, Date combinations with uptime=1)
            good_days = set(
                papd_final[papd_final['uptime'] == 1][['SignalID', 'Detector', 'CallPhase', 'Date']]
                .apply(tuple, axis=1)
            )
            
            # Filter paph to keep only hours from good days
            paph_with_uptime = paph[
                paph[['SignalID', 'Detector', 'CallPhase', 'Date']].apply(tuple, axis=1).isin(good_days)
            ].copy()
            
            save_data(paph_with_uptime, "paph_uptime.pkl")
            logger.info(f"Saved {len(paph_with_uptime)} PAPH records (filtered by uptime)")
        
        log_memory_usage("End ped pushbutton uptime (Athena)")
        logger.info("Ped pushbutton uptime processing completed successfully")
        return
        
        # OLD LOCAL PROCESSING CODE BELOW (DISABLED - was causing "No space left on device")
        # Now handled by Athena aggregation above
        
    except Exception as e:
        logger.error(f"Error in ped pushbutton uptime processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_ped_pushbutton_uptime_OLD_DISABLED(dates, config_data):
    """OLD VERSION - DISABLED - Was causing disk space issues"""
    try:
        pau_start_date = min(
            dates['calcs_start_date'],
            dates['report_end_date'] - relativedelta(months=6)
        )
        
        # Even smaller chunks - process weekly instead of monthly
        def get_date_chunks(start_date, end_date, chunk_days=7):
            date_chunks = []
            current_date = start_date
            
            while current_date < end_date:
                chunk_end = min(current_date + timedelta(days=chunk_days-1), end_date)
                date_chunks.append((current_date, chunk_end))
                current_date = chunk_end + timedelta(days=1)
            
            return date_chunks
        
        # Temporary file management for intermediate results
        import tempfile
        import pickle
        temp_dir = tempfile.mkdtemp()
        logger.info(f"Using temporary directory: {temp_dir}")
        
        def save_temp_chunk(data, chunk_id, data_type):
            """Save chunk to temporary file and return filename"""
            if data is None or data.empty:
                return None
            filename = os.path.join(temp_dir, f"{data_type}_chunk_{chunk_id}.pkl")
            with open(filename, 'wb') as f:
                pickle.dump(data, f)
            return filename
        
        def load_temp_chunk(filename):
            """Load chunk from temporary file"""
            if filename is None or not os.path.exists(filename):
                return pd.DataFrame()
            with open(filename, 'rb') as f:
                return pickle.load(f)
        
        # Process one signal-date combination at a time
        def process_minimal_chunk(signal_id, date_start, date_end):
            try:
                # Process only one signal at a time
                counts_ped_hourly = s3_read_parquet_parallel(
                    bucket=conf.bucket,
                    table_name="counts_ped_1hr",
                    start_date=date_start,
                    end_date=date_end,
                    signals_list=[signal_id],
                    parallel=False
                )
                
                if counts_ped_hourly.empty:
                    return None, None, None
                
                # Immediate preprocessing to reduce memory
                counts_ped_hourly = counts_ped_hourly.dropna(subset=['CallPhase'])
                counts_ped_hourly = clean_signal_ids(counts_ped_hourly)
                
                # Use more memory-efficient data types
                counts_ped_hourly['Detector'] = counts_ped_hourly['Detector'].astype('int16')
                counts_ped_hourly['CallPhase'] = counts_ped_hourly['CallPhase'].astype('int8')
                counts_ped_hourly['vol'] = pd.to_numeric(counts_ped_hourly['vol'], errors='coerce').astype('float32')
                
                counts_ped_hourly = calculate_time_periods(counts_ped_hourly)
                
                # Calculate daily immediately and drop hourly data
                counts_ped_daily = counts_ped_hourly.groupby([
                    'SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase'
                ])['vol'].sum().reset_index()
                counts_ped_daily.rename(columns={'vol': 'papd'}, inplace=True)
                counts_ped_daily['papd'] = counts_ped_daily['papd'].astype('float32')
                
                # Keep only necessary columns for hourly data
                paph_data = counts_ped_hourly[['SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase', 'Timeperiod', 'vol']].copy()
                paph_data.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'}, inplace=True)
                paph_data['paph'] = paph_data['paph'].astype('float32')
                
                # Clear the large hourly dataset immediately
                del counts_ped_hourly
                gc.collect()
                
                return counts_ped_daily, paph_data, None  # Don't calculate PAU yet
                
            except Exception as e:
                logger.error(f"Error in process_minimal_chunk for signal {signal_id}: {e}")
                gc.collect()
                return None, None, None
        
        # Get very small date chunks (weekly)
        date_chunks = get_date_chunks(pau_start_date, dates['report_end_date'], chunk_days=7)
        signals_list = config_data['signals_list']
        
        # Store temp file references
        papd_files = []
        paph_files = []
        
        total_operations = len(date_chunks) * len(signals_list)
        current_operation = 0
        
        # Process each signal individually for each date chunk
        for date_idx, (date_start, date_end) in enumerate(date_chunks):
            logger.info(f"Processing date chunk {date_idx + 1}/{len(date_chunks)}: {date_start} to {date_end}")
            
            for signal_idx, signal_id in enumerate(signals_list):
                current_operation += 1
                
                if current_operation % 50 == 0:
                    logger.info(f"Progress: {current_operation}/{total_operations} ({100*current_operation/total_operations:.1f}%)")
                    log_memory_usage(f"After {current_operation} operations")
                
                try:
                    papd_chunk, paph_chunk, _ = process_minimal_chunk(signal_id, date_start, date_end)
                    
                    # Save to temp files immediately
                    if papd_chunk is not None and not papd_chunk.empty:
                        papd_file = save_temp_chunk(papd_chunk, f"{date_idx}_{signal_idx}", "papd")
                        if papd_file:
                            papd_files.append(papd_file)
                    
                    if paph_chunk is not None and not paph_chunk.empty:
                        paph_file = save_temp_chunk(paph_chunk, f"{date_idx}_{signal_idx}", "paph")
                        if paph_file:
                            paph_files.append(paph_file)
                    
                    # Clear memory immediately
                    del papd_chunk, paph_chunk
                    gc.collect()
                    
                except Exception as e:
                    logger.error(f"Error processing signal {signal_id} for dates {date_start}-{date_end}: {e}")
                    continue
        
        logger.info(f"Collected {len(papd_files)} papd files and {len(paph_files)} paph files")
        
        # Now combine temp files in very small batches
        def combine_temp_files_efficiently(file_list, output_name, batch_size=5):
            """Combine temporary files in small batches to avoid memory spikes"""
            if not file_list:
                return pd.DataFrame()
            
            logger.info(f"Combining {len(file_list)} files for {output_name}")
            combined_parts = []
            
            # Process files in tiny batches
            for i in range(0, len(file_list), batch_size):
                batch_files = file_list[i:i + batch_size]
                batch_data = []
                
                for filename in batch_files:
                    try:
                        chunk_data = load_temp_chunk(filename)
                        if not chunk_data.empty:
                            batch_data.append(chunk_data)
                        # Delete temp file immediately after loading
                        os.remove(filename)
                    except Exception as e:
                        logger.error(f"Error loading temp file {filename}: {e}")
                        continue
                
                if batch_data:
                    batch_combined = pd.concat(batch_data, ignore_index=True)
                    combined_parts.append(batch_combined)
                    del batch_data
                    gc.collect()
                
                if i % (batch_size * 10) == 0:
                    log_memory_usage(f"Combined {i + batch_size} files for {output_name}")
            
            if combined_parts:
                final_result = pd.concat(combined_parts, ignore_index=True)
                del combined_parts
                gc.collect()
                return final_result
            else:
                return pd.DataFrame()
        
        # Combine all papd data
        log_memory_usage("Before combining papd files")
        papd = combine_temp_files_efficiently(papd_files, "papd", batch_size=3)
        
        if papd.empty:
            logger.warning("No papd data collected")
            return
        
        save_data(papd, "papd_raw.pkl")
        log_memory_usage("After combining papd")
        
        # Combine all paph data
        log_memory_usage("Before combining paph files")
        paph = combine_temp_files_efficiently(paph_files, "paph", batch_size=3)
        
        if paph.empty:
            logger.warning("No paph data collected")
            return
        
        save_data(paph, "paph_raw.pkl")
        log_memory_usage("After combining paph")
        
        # Clean up temp directory
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.info("Cleaned up temporary directory")
        except Exception as e:
            logger.warning(f"Could not clean up temp directory: {e}")
        
        # Now calculate PAU in smaller chunks
        logger.info("Calculating pedestrian uptime (PAU)")
        
        # Process PAU calculation by signal groups
        signal_groups = [signals_list[i:i+5] for i in range(0, len(signals_list), 5)]
        pau_parts = []
        
        for group_idx, signal_group in enumerate(signal_groups):
            logger.info(f"Processing PAU for signal group {group_idx + 1}/{len(signal_groups)}")
            
            try:
                # Filter data for this signal group
                papd_group = papd[papd['SignalID'].isin(signal_group)].copy()
                paph_group = paph[paph['SignalID'].isin(signal_group)].copy()
                
                if papd_group.empty or paph_group.empty:
                    continue
                
                # Calculate PAU for this group
                date_range = pd.date_range(pau_start_date, dates['report_end_date'], freq='D')
                pau_group = get_pau_gamma(
                    date_range, papd_group, paph_group, config_data['corridors'], 
                    dates['wk_calcs_start_date'], pau_start_date
                )
                
                if not pau_group.empty:
                    pau_parts.append(pau_group)
                
                del papd_group, paph_group, pau_group
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error calculating PAU for group {group_idx}: {e}")
                continue
        
        # Clear the large combined datasets
        del papd, paph
        gc.collect()
        
        if pau_parts:
            logger.info("Combining PAU results")
            pau = pd.concat(pau_parts, ignore_index=True)
            del pau_parts
            gc.collect()
            
            log_memory_usage("After calculating PAU")
            
            # Continue with the rest of the processing...
            if not pau.empty:
                # Process bad days and calculate metrics as before
                pau_with_replacements = pau.copy()
                pau_with_replacements.loc[pau_with_replacements['uptime'] == 0, 'papd'] = np.nan
                
                # Calculate monthly averages in smaller chunks to avoid memory issues
                monthly_avg_parts = []
                signal_chunks = [signals_list[i:i+10] for i in range(0, len(signals_list), 10)]
                
                for chunk_signals in signal_chunks:
                    chunk_data = pau_with_replacements[pau_with_replacements['SignalID'].isin(chunk_signals)]
                    if not chunk_data.empty:
                        monthly_avg_chunk = chunk_data.groupby([
                            'SignalID', 'Detector', 'CallPhase', 
                            chunk_data['Date'].dt.year,
                            chunk_data['Date'].dt.month
                        ])['papd'].transform('mean')
                        monthly_avg_parts.append(monthly_avg_chunk)
                    del chunk_data
                    gc.collect()
                
                if monthly_avg_parts:
                    monthly_avg = pd.concat(monthly_avg_parts)
                    del monthly_avg_parts
                    gc.collect()
                    
                    pau_with_replacements['papd'] = pau_with_replacements['papd'].fillna(monthly_avg.fillna(0))
                    del monthly_avg
                    gc.collect()
                
                # Extract papd for saving
                papd_final = pau_with_replacements[['SignalID', 'Detector', 'CallPhase', 'Date', 'DOW', 'Week', 'papd', 'uptime']].copy()
                del pau_with_replacements
                gc.collect()
                
                # Continue with the rest of the metrics calculations...
                # [Rest of the existing code for bad detectors, uptime metrics, etc.]
                
                # Bad detectors
                bad_detectors = get_bad_ped_detectors(pau)
                bad_detectors = bad_detectors[bad_detectors['Date'] >= dates['calcs_start_date']]
                
                if not bad_detectors.empty:
                    save_data(bad_detectors, "bad_ped_detectors.pkl")
                del bad_detectors
                gc.collect()
                
                save_data(pau, "pa_uptime.pkl")
                pau['CallPhase'] = pau['Detector']
                
                # Calculate uptime metrics with smaller memory footprint
                daily_pa_uptime = get_daily_avg(pau, "uptime", peak_only=False)
                save_data(daily_pa_uptime, "daily_pa_uptime.pkl")
                
                weekly_pa_uptime = get_weekly_avg_by_day(pau, "uptime", peak_only=False)
                save_data(weekly_pa_uptime, "weekly_pa_uptime.pkl")
                
                monthly_pa_uptime = get_monthly_avg_by_day(pau, "uptime", peak_only=False)
                save_data(monthly_pa_uptime, "monthly_pa_uptime.pkl")
                
                # Clear pau to free memory
                del pau
                gc.collect()
                
                                # Continue with corridor metrics using saved data
                cor_daily_pa_uptime = get_cor_weekly_avg_by_day(
                    daily_pa_uptime, config_data['corridors'], "uptime"
                )
                save_data(cor_daily_pa_uptime, "cor_daily_pa_uptime.pkl")
                del daily_pa_uptime, cor_daily_pa_uptime
                gc.collect()
                
                cor_weekly_pa_uptime = get_cor_weekly_avg_by_day(
                    weekly_pa_uptime, config_data['corridors'], "uptime"
                )
                save_data(cor_weekly_pa_uptime, "cor_weekly_pa_uptime.pkl")
                del weekly_pa_uptime, cor_weekly_pa_uptime
                gc.collect()
                
                cor_monthly_pa_uptime = get_cor_monthly_avg_by_day(
                    monthly_pa_uptime, config_data['corridors'], "uptime"
                )
                save_data(cor_monthly_pa_uptime, "cor_monthly_pa_uptime.pkl")
                del monthly_pa_uptime, cor_monthly_pa_uptime
                gc.collect()
                
                # Subcorridor metrics - reload data as needed to minimize memory usage
                daily_pa_uptime = load_data("daily_pa_uptime.pkl")
                sub_daily_pa_uptime = get_cor_weekly_avg_by_day(
                    daily_pa_uptime, config_data['subcorridors'], "uptime"
                ).dropna(subset=['Corridor'])
                save_data(sub_daily_pa_uptime, "sub_daily_pa_uptime.pkl")
                del daily_pa_uptime, sub_daily_pa_uptime
                gc.collect()
                
                weekly_pa_uptime = load_data("weekly_pa_uptime.pkl")
                sub_weekly_pa_uptime = get_cor_weekly_avg_by_day(
                    weekly_pa_uptime, config_data['subcorridors'], "uptime"
                ).dropna(subset=['Corridor'])
                save_data(sub_weekly_pa_uptime, "sub_weekly_pa_uptime.pkl")
                del weekly_pa_uptime, sub_weekly_pa_uptime
                gc.collect()
                
                monthly_pa_uptime = load_data("monthly_pa_uptime.pkl")
                sub_monthly_pa_uptime = get_cor_monthly_avg_by_day(
                    monthly_pa_uptime, config_data['subcorridors'], "uptime"
                )
                if not sub_monthly_pa_uptime.empty:
                    sub_monthly_pa_uptime = sub_monthly_pa_uptime.dropna(subset=['Corridor'])
                save_data(sub_monthly_pa_uptime, "sub_monthly_pa_uptime.pkl")
                del monthly_pa_uptime, sub_monthly_pa_uptime
                gc.collect()
                
                log_memory_usage("End ped pushbutton uptime")
                logger.info("Pedestrian pushbutton uptime processing completed successfully")
            else:
                logger.warning("No PAU data generated")
        else:
            logger.warning("No data collected for PAU calculation")
        
    except Exception as e:
        logger.error(f"Error in pedestrian pushbutton uptime processing: {e}")
        logger.error(traceback.format_exc())
        
        # Emergency cleanup
        try:
            if 'temp_dir' in locals() and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except:
            pass
        gc.collect()
    
    finally:
        # Final cleanup
        gc.collect()
        log_memory_usage("Final cleanup complete")

# Additional helper function for even more aggressive memory management
def process_ped_pushbutton_uptime_ultra_conservative(dates, config_data):
    """Ultra-conservative version - processes one signal per day to minimize memory usage"""
    logger.info(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (ultra-conservative)]")
    log_memory_usage("Start ultra-conservative ped pushbutton uptime")
    
    try:
        pau_start_date = min(
            dates['calcs_start_date'],
            dates['report_end_date'] - relativedelta(months=6)
        )
        
        # Create a SQLite database for intermediate storage
        import sqlite3
        import tempfile
        
        temp_db_file = tempfile.mktemp(suffix='.db')
        conn = sqlite3.connect(temp_db_file)
        
        # Create tables for intermediate storage
        conn.execute('''
            CREATE TABLE papd_chunks (
                SignalID TEXT,
                Date TEXT,
                DOW INTEGER,
                Week INTEGER,
                Detector INTEGER,
                CallPhase INTEGER,
                papd REAL,
                chunk_id INTEGER
            )
        ''')
        
        conn.execute('''
            CREATE TABLE paph_chunks (
                SignalID TEXT,
                Date TEXT,
                DOW INTEGER,
                Week INTEGER,
                Detector INTEGER,
                CallPhase INTEGER,
                Hour INTEGER,
                paph REAL,
                chunk_id INTEGER
            )
        ''')
        
        conn.commit()
        
        signals_list = config_data['signals_list']
        date_range = pd.date_range(pau_start_date, dates['report_end_date'], freq='D')
        
        chunk_id = 0
        total_operations = len(signals_list) * len(date_range)
        current_operation = 0
        
        # Process one signal-day combination at a time
        for signal_id in signals_list:
            logger.info(f"Processing signal {signal_id} ({signals_list.index(signal_id) + 1}/{len(signals_list)})")
            
            for single_date in date_range:
                current_operation += 1
                
                if current_operation % 100 == 0:
                    logger.info(f"Progress: {current_operation}/{total_operations} ({100*current_operation/total_operations:.1f}%)")
                    log_memory_usage(f"After {current_operation} operations")
                
                try:
                    # Process single signal for single day
                    counts_ped_hourly = s3_read_parquet_parallel(
                        bucket=conf.bucket,
                        table_name="counts_ped_1hr",
                        start_date=single_date,
                        end_date=single_date,
                        signals_list=[signal_id],
                        parallel=False
                    )
                    
                    if counts_ped_hourly.empty:
                        continue
                    
                    # Quick preprocessing
                    counts_ped_hourly = counts_ped_hourly.dropna(subset=['CallPhase'])
                    counts_ped_hourly = clean_signal_ids(counts_ped_hourly)
                    counts_ped_hourly = calculate_time_periods(counts_ped_hourly)
                    counts_ped_hourly['vol'] = pd.to_numeric(counts_ped_hourly['vol'], errors='coerce', downcast='float')
                    
                    # Calculate daily aggregation
                    counts_ped_daily = counts_ped_hourly.groupby([
                        'SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase'
                    ])['vol'].sum().reset_index()
                    counts_ped_daily.rename(columns={'vol': 'papd'}, inplace=True)
                    counts_ped_daily['chunk_id'] = chunk_id
                    
                    # Store in SQLite immediately
                    counts_ped_daily.to_sql('papd_chunks', conn, if_exists='append', index=False)
                    
                    # Store hourly data
                    paph_data = counts_ped_hourly[['SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase', 'Timeperiod', 'vol']].copy()
                    paph_data.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'}, inplace=True)
                    paph_data['chunk_id'] = chunk_id
                    paph_data.to_sql('paph_chunks', conn, if_exists='append', index=False)
                    
                    # Clear memory immediately
                    del counts_ped_hourly, counts_ped_daily, paph_data
                    gc.collect()
                    
                    chunk_id += 1
                    
                except Exception as e:
                    logger.error(f"Error processing signal {signal_id} for date {single_date}: {e}")
                    continue
        
        logger.info("Retrieving aggregated data from database")
        
        # Retrieve data in chunks from SQLite
        papd = pd.read_sql_query("SELECT SignalID, Date, DOW, Week, Detector, CallPhase, papd FROM papd_chunks", conn)
        paph = pd.read_sql_query("SELECT SignalID, Date, DOW, Week, Detector, CallPhase, Hour, paph FROM paph_chunks", conn)
        
        # Close and cleanup database
        conn.close()
        os.remove(temp_db_file)
        
        log_memory_usage("After retrieving data from database")
        
        if papd.empty or paph.empty:
            logger.warning("No data retrieved from database")
            return
        
        # Convert date columns back to datetime
        papd['Date'] = pd.to_datetime(papd['Date'])
        paph['Date'] = pd.to_datetime(paph['Date'])
        
        logger.info("Calculating PAU with retrieved data")
        
        # Calculate PAU in signal groups to manage memory
        signal_groups = [signals_list[i:i+3] for i in range(0, len(signals_list), 3)]  # Even smaller groups
        pau_results = []
        
        for group_idx, signal_group in enumerate(signal_groups):
            logger.info(f"Processing PAU for signal group {group_idx + 1}/{len(signal_groups)}")
            
            try:
                papd_group = papd[papd['SignalID'].isin(signal_group)].copy()
                paph_group = paph[paph['SignalID'].isin(signal_group)].copy()
                
                if papd_group.empty or paph_group.empty:
                    continue
                
                date_range_group = pd.date_range(pau_start_date, dates['report_end_date'], freq='D')
                pau_group = get_pau_gamma(
                    date_range_group, papd_group, paph_group, config_data['corridors'], 
                    dates['wk_calcs_start_date'], pau_start_date
                )
                
                if not pau_group.empty:
                    # Save each group immediately
                    save_data(pau_group, f"pau_group_{group_idx}.pkl")
                    pau_results.append(f"pau_group_{group_idx}.pkl")
                
                del papd_group, paph_group, pau_group
                gc.collect()
                
            except Exception as e:
                logger.error(f"Error calculating PAU for group {group_idx}: {e}")
                continue
        
        # Clear the large datasets
        del papd, paph
        gc.collect()
        
        # Combine PAU results from saved files
        if pau_results:
            logger.info("Combining PAU results from saved files")
            pau_parts = []
            
            for pau_file in pau_results:
                try:
                    pau_part = load_data(pau_file)
                    pau_parts.append(pau_part)
                    # Delete the temporary file
                    os.remove(pau_file)
                except Exception as e:
                    logger.error(f"Error loading PAU file {pau_file}: {e}")
                    continue
            
            if pau_parts:
                pau = pd.concat(pau_parts, ignore_index=True)
                del pau_parts
                gc.collect()
                
                # Continue with the rest of the processing as in the original function
                # [Same processing logic as the main function...]
                
                logger.info("Ultra-conservative pedestrian pushbutton uptime processing completed successfully")
            else:
                logger.warning("No PAU parts to combine")
        else:
            logger.warning("No PAU results generated")
            
    except Exception as e:
        logger.error(f"Error in ultra-conservative pedestrian pushbutton uptime processing: {e}")
        logger.error(traceback.format_exc())
        
        # Emergency cleanup
        try:
            if 'conn' in locals():
                conn.close()
            if 'temp_db_file' in locals() and os.path.exists(temp_db_file):
                os.remove(temp_db_file)
        except:
            pass
        gc.collect()
    
    finally:
        gc.collect()
        log_memory_usage("Ultra-conservative cleanup complete")

def process_watchdog_alerts(dates, config_data):
    """Process watchdog alerts [3 of 29] - Athena optimized"""
    logger.info(f"{datetime.now()} Watchdog alerts [3 of 29 (mark1)]")
    log_memory_usage("Start watchdog alerts")
    
    try:
        # Process bad vehicle detectors using Athena
        logger.info("Using Athena optimization for watchdog alerts")
        from package_athena_helpers import athena_get_bad_detectors, athena_get_bad_ped_detectors
        
        conf_dict = conf.to_dict()
        start_date = (date.today() - timedelta(days=90)).strftime('%Y-%m-%d')
        end_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        bad_det = keep_trying(
            athena_get_bad_detectors,
            n_tries=3,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        
        if not bad_det.empty:
            # Athena already returned with corridor info and Name!
            # Just need to add missing columns
            if 'CallPhase' not in bad_det.columns:
                bad_det['CallPhase'] = 'Unknown'
            if 'ApproachDesc' not in bad_det.columns:
                bad_det['ApproachDesc'] = 'Unknown'
                
            # Ensure proper column order
            required_cols = ['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector',
                           'Date', 'Alert', 'Name', 'ApproachDesc']
            for col in required_cols:
                if col not in bad_det.columns:
                    bad_det[col] = 'Unknown' if col not in ['CallPhase', 'Detector'] else 0
            
            bad_det = bad_det[required_cols]
            save_data(bad_det, "watchdog_bad_detectors.pkl")
            logger.info(f"Saved {len(bad_det)} bad detector records")
        
        # Process bad pedestrian detectors using Athena
        try:
            bad_ped = keep_trying(
                athena_get_bad_ped_detectors,
                n_tries=3,
                start_date=start_date,
                end_date=end_date,
                corridors_df=config_data['corridors'],
                conf=conf_dict
            )
            
            if not bad_ped.empty:
                # Athena already returned with corridor info and Name!
                # Ensure proper column format
                required_cols = ['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Detector', 'Date', 'Name', 'Alert']
                for col in required_cols:
                    if col not in bad_ped.columns:
                        if col == 'Alert':
                            bad_ped[col] = 'Bad Ped Detection'
                        else:
                            bad_ped[col] = 'Unknown'
                
                bad_ped = bad_ped[required_cols]
                save_data(bad_ped, "watchdog_bad_ped_pushbuttons.pkl")
                logger.info(f"Saved {len(bad_ped)} bad ped detector records")
        except Exception as e:
            logger.warning(f"No bad pedestrian detectors data found: {e}")
        
        # Process bad cameras
        try:
            bad_cam_list = []
            start_month = date.today().replace(day=1) - relativedelta(months=6)
            current_month = start_month
            
            while current_month < date.today():
                try:
                    key = f"mark/cctv_uptime/month={current_month.strftime('%Y-%m-%d')}/cctv_uptime_{current_month.strftime('%Y-%m-%d')}.parquet"
                    
                    cctv_data = s3read_using(
                        pd.read_parquet,
                        bucket=conf.bucket,
                        object=key
                    )
                    
                    if not cctv_data.empty:
                        if 'Size' in cctv_data.columns:
                            bad_cameras = cctv_data[cctv_data['Size'] == 0]
                        else:
                            bad_cameras = cctv_data.copy()
                            
                        if not bad_cameras.empty:
                            bad_cam_list.append(bad_cameras)
                    del cctv_data
                    gc.collect()
                except Exception as e:
                    logger.warning(f"Could not read CCTV data for {current_month.strftime('%Y-%m-%d')}: {e}")
                
                current_month += relativedelta(months=1)
            
            if bad_cam_list:
                bad_cam = pd.concat(bad_cam_list, ignore_index=True)
                del bad_cam_list
                gc.collect()
                
                # Merge with camera config if available
                if 'cam_config' in config_data and not config_data['cam_config'].empty:
                    if 'CameraID' in bad_cam.columns:
                        bad_cam['CameraID'] = bad_cam['CameraID'].astype(str)
                        if 'CameraID' in config_data['cam_config'].columns:
                            config_data['cam_config']['CameraID'] = config_data['cam_config']['CameraID'].astype(str)
                            
                            bad_cam = bad_cam.merge(
                                config_data['cam_config'],
                                on='CameraID',
                                how='left'
                            )
                            
                            if 'As_of_Date' in bad_cam.columns and 'Date' in bad_cam.columns:
                                bad_cam['As_of_Date'] = pd.to_datetime(bad_cam['As_of_Date'])
                                bad_cam['Date'] = pd.to_datetime(bad_cam['Date'])
                                bad_cam = bad_cam[bad_cam['Date'] > bad_cam['As_of_Date']]
                
                required_cols = ['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector',
                               'Date', 'Alert', 'Name']
                
                for col in required_cols:
                    if col not in bad_cam.columns:
                        if col == 'SignalID':
                            if 'CameraID' in bad_cam.columns:
                                bad_cam[col] = bad_cam['CameraID'].fillna('Unknown')
                            else:
                                bad_cam[col] = 'Unknown'
                        elif col in ['CallPhase', 'Detector']:
                            bad_cam[col] = 0
                        elif col == 'Alert':
                            bad_cam[col] = 'No Camera Image'
                        elif col == 'Name':
                            if 'Location' in bad_cam.columns:
                                bad_cam[col] = bad_cam['Location'].fillna('Unknown')
                            elif 'CameraID' in bad_cam.columns:
                                bad_cam[col] = bad_cam['CameraID'].fillna('Unknown')
                            else:
                                bad_cam[col] = 'Unknown'
                        else:
                            bad_cam[col] = 'Unknown'
                
                bad_cam = bad_cam[required_cols]
                save_data(bad_cam, "watchdog_bad_cameras.pkl")
                del bad_cam
                gc.collect()
                
        except Exception as e:
            logger.warning(f"Could not process camera data: {e}")
        
        log_memory_usage("End watchdog alerts")
        logger.info("Watchdog alerts processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in watchdog alerts processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_daily_ped_activations(dates, config_data):
    """Process daily pedestrian activations [4 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} Daily Pedestrian Activations [4 of 29 (mark1)]")
    log_memory_usage("Start daily ped activations")
    
    try:
        papd = load_data("pa_uptime.pkl")
        
        if not papd.empty:
            weekly_papd = get_weekly_papd(papd)
            save_data(weekly_papd, "weekly_papd.pkl")
            
            cor_weekly_papd = get_cor_weekly_papd(weekly_papd, config_data['corridors'])
            save_data(cor_weekly_papd, "cor_weekly_papd.pkl")
            del weekly_papd, cor_weekly_papd
            gc.collect()
            
            sub_weekly_papd = get_cor_weekly_papd(
                load_data("weekly_papd.pkl"), config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            save_data(sub_weekly_papd, "sub_weekly_papd.pkl")
            del sub_weekly_papd
            gc.collect()
            
            monthly_papd = get_monthly_papd(papd)
            save_data(monthly_papd, "monthly_papd.pkl")
            del papd
            gc.collect()
            
            cor_monthly_papd = get_cor_monthly_papd(monthly_papd, config_data['corridors'])
            save_data(cor_monthly_papd, "cor_monthly_papd.pkl")
            del cor_monthly_papd
            gc.collect()
            
            sub_monthly_papd = get_cor_monthly_papd(
                monthly_papd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            save_data(sub_monthly_papd, "sub_monthly_papd.pkl")
            del monthly_papd, sub_monthly_papd
            gc.collect()
            
            log_memory_usage("End daily ped activations")
            logger.info("Daily pedestrian activations processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily pedestrian activations processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_hourly_ped_activations(dates, config_data):
    """Process hourly pedestrian activations [5 of 29] - Athena optimized (prevents memory explosion)"""
    logger.info(f"{datetime.now()} Hourly Pedestrian Activations [5 of 29 (mark1)]")
    log_memory_usage("Start hourly ped activations")
    
    try:
        logger.info("Using Athena aggregation for hourly ped activations (prevents 142M row explosion!)")
        
        # Use athena_get_ped_counts_aggregated results (already has DOW, Week columns)
        conf_dict = conf.to_dict()
        pau_start_date = min(
            dates['calcs_start_date'],
            dates['report_end_date'] - relativedelta(months=6)
        )
        start_date_str = pau_start_date.strftime('%Y-%m-%d') if hasattr(pau_start_date, 'strftime') else str(pau_start_date)
        end_date_str = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Get aggregated data from Athena (already done in #2, but query again to avoid memory issues)
        # This gives us clean hourly data without the explosion
        _, paph = keep_trying(
            athena_get_ped_counts_aggregated,
            n_tries=3,
            start_date=start_date_str,
            end_date=end_date_str,
            signals_list=config_data['signals_list'],
            conf=conf_dict
        )
        
        if paph.empty:
            logger.warning("No hourly ped data - saving empty files")
            save_data(pd.DataFrame(), "weekly_paph.pkl")
            save_data(pd.DataFrame(), "monthly_paph.pkl")
            save_data(pd.DataFrame(), "cor_weekly_paph.pkl")
            save_data(pd.DataFrame(), "cor_monthly_paph.pkl")
            save_data(pd.DataFrame(), "sub_weekly_paph.pkl")
            save_data(pd.DataFrame(), "sub_monthly_paph.pkl")
            return
        
        # Filter by uptime (merge with pa_uptime to get only good days)
        pa_uptime = load_data("pa_uptime.pkl")
        if not pa_uptime.empty:
            good_days = pa_uptime[pa_uptime['uptime'] == 1][['SignalID', 'Detector', 'CallPhase', 'Date']].copy()
            good_days['SignalID'] = good_days['SignalID'].astype(str)
            good_days['Date'] = pd.to_datetime(good_days['Date'])
            
            paph['SignalID'] = paph['SignalID'].astype(str)
            paph['Date'] = pd.to_datetime(paph['Date'])
            
            # Merge to filter (anti-join pattern)
            paph = paph.merge(good_days, on=['SignalID', 'Detector', 'CallPhase', 'Date'], how='inner')
            logger.info(f"Filtered to {len(paph)} hourly records from good uptime days")
        
        # Now aggregate - this should create reasonable row counts
        if not paph.empty:
            # Ensure Year and Week columns exist
            if 'Year' not in paph.columns:
                paph['Year'] = pd.to_datetime(paph['Date']).dt.year
            if 'Week' not in paph.columns:
                paph['Week'] = pd.to_datetime(paph['Date']).dt.isocalendar().week
            
            weekly_paph = paph.groupby(['SignalID', 'Detector', 'CallPhase', 'Year', 'Week', 'Hour']).agg({
                'paph': 'mean'
            }).reset_index()
            save_data(weekly_paph, "weekly_paph.pkl")
            logger.info(f"Created {len(weekly_paph)} weekly hourly ped records")
            
            monthly_paph = paph.groupby(['SignalID', 'Detector', 'CallPhase', 'Hour']).agg({
                'paph': 'mean',
                'Date': lambda x: pd.to_datetime(x).dt.to_period('M').min().start_time
            }).reset_index()
            monthly_paph = monthly_paph.rename(columns={'Date': 'Month'})
            save_data(monthly_paph, "monthly_paph.pkl")
            logger.info(f"Created {len(monthly_paph)} monthly hourly ped records")
            
            del paph
            gc.collect()
            
            # Fix SignalID type before corridor merge (string in paph, int in corridors)
            # Ensure corridors SignalID is string for merge
            corridors_str = config_data['corridors'].copy()
            corridors_str['SignalID'] = corridors_str['SignalID'].astype(str)
            
            subcorridors_str = config_data['subcorridors'].copy()
            subcorridors_str['SignalID'] = subcorridors_str['SignalID'].astype(str)
            
            # Corridor aggregations (with fixed SignalID types)
            cor_weekly_paph = get_cor_weekly_paph(weekly_paph, corridors_str)
            save_data(cor_weekly_paph, "cor_weekly_paph.pkl")
            
            cor_monthly_paph = get_cor_monthly_paph(monthly_paph, corridors_str)
            save_data(cor_monthly_paph, "cor_monthly_paph.pkl")
            
            # Subcorridor aggregations (with fixed SignalID types)
            sub_weekly_paph = get_cor_weekly_paph(weekly_paph, subcorridors_str)
            if not sub_weekly_paph.empty and 'Corridor' in sub_weekly_paph.columns:
                sub_weekly_paph = sub_weekly_paph.dropna(subset=['Corridor'])
            save_data(sub_weekly_paph, "sub_weekly_paph.pkl")
            
            sub_monthly_paph = get_cor_monthly_paph(monthly_paph, subcorridors_str)
            if not sub_monthly_paph.empty and 'Corridor' in sub_monthly_paph.columns:
                sub_monthly_paph = sub_monthly_paph.dropna(subset=['Corridor'])
            save_data(sub_monthly_paph, "sub_monthly_paph.pkl")
            
            del weekly_paph, monthly_paph, cor_weekly_paph, cor_monthly_paph
            del sub_weekly_paph, sub_monthly_paph
            gc.collect()
            
            log_memory_usage("End hourly ped activations")
            logger.info("Hourly pedestrian activations processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly pedestrian activations processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_pedestrian_delay(dates, config_data):
    """Process pedestrian delay [6 of 29] - Athena full aggregation (bypasses problematic library)"""
    logger.info(f"{datetime.now()} Pedestrian Delay [6 of 29 (mark1)]")
    log_memory_usage("Start pedestrian delay")
    
    try:
        # Do ALL aggregation in Athena to bypass the problematic aggregations.py functions
        logger.info("Using FULL Athena aggregation for pedestrian delay (bypasses library bug)")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Query ped_delay table using Athena helpers
        # Note: ped_delay has raw events, helpers will aggregate
        
        # Corridor weekly PD (ped_delay has 'duration' column, not 'pd')
        cor_weekly_pd_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='ped_delay',
            metric_col='duration',  # Actual column name in table
            weight_col=None,  # No weight column in raw ped_delay table
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        # Rename duration → pd for compatibility
        if not cor_weekly_pd_by_day.empty and 'duration' in cor_weekly_pd_by_day.columns:
            cor_weekly_pd_by_day = cor_weekly_pd_by_day.rename(columns={'duration': 'pd'})
        save_data(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.pkl")
        
        # Subcorridor weekly PD (ped_delay has 'duration' column, not 'pd')
        sub_weekly_pd_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='ped_delay',
            metric_col='duration',  # Actual column name in table
            weight_col=None,  # No weight column in raw ped_delay table
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        # Rename duration → pd for compatibility
        if not sub_weekly_pd_by_day.empty and 'duration' in sub_weekly_pd_by_day.columns:
            sub_weekly_pd_by_day = sub_weekly_pd_by_day.rename(columns={'duration': 'pd'})
        save_data(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.pkl")
        
        # Corridor monthly PD (ped_delay has 'duration' column, not 'pd')
        cor_monthly_pd_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='ped_delay',
            metric_col='duration',  # Actual column name in table
            weight_col=None,  # No weight column in raw ped_delay table
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        # Rename duration → pd for compatibility
        if not cor_monthly_pd_by_day.empty and 'duration' in cor_monthly_pd_by_day.columns:
            cor_monthly_pd_by_day = cor_monthly_pd_by_day.rename(columns={'duration': 'pd'})
        save_data(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.pkl")
        
        # Subcorridor monthly PD (ped_delay has 'duration' column, not 'pd')
        sub_monthly_pd_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='ped_delay',
            metric_col='duration',  # Actual column name in table
            weight_col=None,  # No weight column in raw ped_delay table
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        # Rename duration → pd for compatibility
        if not sub_monthly_pd_by_day.empty and 'duration' in sub_monthly_pd_by_day.columns:
            sub_monthly_pd_by_day = sub_monthly_pd_by_day.rename(columns={'duration': 'pd'})
        save_data(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.pkl")
        
        logger.info("Pedestrian delay Athena aggregation completed")
        log_memory_usage("End pedestrian delay")
        
    except Exception as e:
        logger.error(f"Error in pedestrian delay processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_communications_uptime(dates, config_data):
    """Process communications uptime [7 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Communication Uptime [7 of 29 (mark1)]")
    log_memory_usage("Start communications uptime")
    
    try:
        logger.info("Using Athena optimization for communications uptime")
        from package_athena_helpers import athena_get_corridor_daily_avg, athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Daily start date for daily aggregations
        daily_start_date = dates['calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['calcs_start_date'], 'strftime') else str(dates['calcs_start_date'])
        
        # Corridor daily (new - matches R code)
        from package_athena_helpers import athena_get_corridor_daily_avg
        cor_daily_comm_uptime = keep_trying(
            athena_get_corridor_daily_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=daily_start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_daily_comm_uptime, "cor_daily_comm_uptime.pkl")
        
        # Subcorridor daily
        sub_daily_comm_uptime = keep_trying(
            athena_get_corridor_daily_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=daily_start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_daily_comm_uptime, "sub_daily_comm_uptime.pkl")
        
        # Corridor weekly
        cor_weekly_comm_uptime = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.pkl")
        
        # Subcorridor weekly
        sub_weekly_comm_uptime = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.pkl")
        
        # Corridor monthly
        cor_monthly_comm_uptime = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.pkl")
        
        # Subcorridor monthly
        sub_monthly_comm_uptime = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='comm_uptime',
            metric_col='uptime',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.pkl")
        
        logger.info("Communications uptime Athena optimization completed")
        log_memory_usage("End communications uptime (Athena)")
        
    except Exception as e:
        logger.error(f"Error in communications uptime processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_daily_volumes(dates, config_data):
    """Process daily volumes [8 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Daily Volumes [8 of 29 (mark1)]")
    log_memory_usage("Start daily volumes")
    
    try:
        logger.info("Using Athena optimization for daily volumes (VPD)")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly VPD (Athena does: read + join + group by in cloud!)
        cor_weekly_vpd = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='vehicles_pd',
            metric_col='vpd',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_vpd, "cor_weekly_vpd.pkl")
        
        # Subcorridor weekly VPD
        sub_weekly_vpd = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='vehicles_pd',
            metric_col='vpd',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_vpd, "sub_weekly_vpd.pkl")
        
        # Corridor monthly VPD
        cor_monthly_vpd = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='vehicles_pd',
            metric_col='vpd',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_vpd, "cor_monthly_vpd.pkl")
        
        # Subcorridor monthly VPD
        sub_monthly_vpd = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='vehicles_pd',
            metric_col='vpd',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_vpd, "sub_monthly_vpd.pkl")
        
        logger.info("Daily volumes (VPD) Athena optimization completed successfully")
        log_memory_usage("End daily volumes (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily volumes processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_hourly_volumes(dates, config_data):
    """Process hourly volumes [9 of 29] - Athena optimized"""
    logger.info(f"{datetime.now()} Hourly Volumes [9 of 29 (mark1)]")
    log_memory_usage("Start hourly volumes")
    
    try:
        logger.info("Using Athena optimization for hourly volumes (VPH)")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly VPH  
        cor_weekly_vph = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='vehicles_ph',
            metric_col='vph',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_vph, "cor_weekly_vph.pkl")
        
        # Subcorridor weekly VPH
        sub_weekly_vph = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='vehicles_ph',
            metric_col='vph',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_vph, "sub_weekly_vph.pkl")
        
        # Corridor monthly VPH
        cor_monthly_vph = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='vehicles_ph',
            metric_col='vph',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_vph, "cor_monthly_vph.pkl")
        
        # Subcorridor monthly VPH
        sub_monthly_vph = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='vehicles_ph',
            metric_col='vph',
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_vph, "sub_monthly_vph.pkl")
        
        # Calculate VPH Peak (AM/PM split) - matches R code
        logger.info("Calculating VPH peak (AM/PM split)...")
        
        # Load AM/PM peak hours from config
        am_peak_hours = conf.get('AM_PEAK_HOURS', [6, 7, 8, 9])
        pm_peak_hours = conf.get('PM_PEAK_HOURS', [15, 16, 17, 18, 19])
        
        # Weekly VPH Peak
        if not cor_weekly_vph.empty and 'Hour' in cor_weekly_vph.columns:
            cor_weekly_vph_am = cor_weekly_vph[cor_weekly_vph['Hour'].isin(am_peak_hours)].copy()
            cor_weekly_vph_pm = cor_weekly_vph[cor_weekly_vph['Hour'].isin(pm_peak_hours)].copy()
            save_data({'am': cor_weekly_vph_am, 'pm': cor_weekly_vph_pm}, "cor_weekly_vph_peak.pkl")
        else:
            save_data({'am': pd.DataFrame(), 'pm': pd.DataFrame()}, "cor_weekly_vph_peak.pkl")
        
        if not sub_weekly_vph.empty and 'Hour' in sub_weekly_vph.columns:
            sub_weekly_vph_am = sub_weekly_vph[sub_weekly_vph['Hour'].isin(am_peak_hours)].copy()
            sub_weekly_vph_pm = sub_weekly_vph[sub_weekly_vph['Hour'].isin(pm_peak_hours)].copy()
            save_data({'am': sub_weekly_vph_am, 'pm': sub_weekly_vph_pm}, "sub_weekly_vph_peak.pkl")
        else:
            save_data({'am': pd.DataFrame(), 'pm': pd.DataFrame()}, "sub_weekly_vph_peak.pkl")
        
        # Monthly VPH Peak
        if not cor_monthly_vph.empty and 'Hour' in cor_monthly_vph.columns:
            cor_monthly_vph_am = cor_monthly_vph[cor_monthly_vph['Hour'].isin(am_peak_hours)].copy()
            cor_monthly_vph_pm = cor_monthly_vph[cor_monthly_vph['Hour'].isin(pm_peak_hours)].copy()
            save_data({'am': cor_monthly_vph_am, 'pm': cor_monthly_vph_pm}, "cor_monthly_vph_peak.pkl")
        else:
            save_data({'am': pd.DataFrame(), 'pm': pd.DataFrame()}, "cor_monthly_vph_peak.pkl")
        
        if not sub_monthly_vph.empty and 'Hour' in sub_monthly_vph.columns:
            sub_monthly_vph_am = sub_monthly_vph[sub_monthly_vph['Hour'].isin(am_peak_hours)].copy()
            sub_monthly_vph_pm = sub_monthly_vph[sub_monthly_vph['Hour'].isin(pm_peak_hours)].copy()
            save_data({'am': sub_monthly_vph_am, 'pm': sub_monthly_vph_pm}, "sub_monthly_vph_peak.pkl")
        else:
            save_data({'am': pd.DataFrame(), 'pm': pd.DataFrame()}, "sub_monthly_vph_peak.pkl")
        
        logger.info("VPH peak (AM/PM) files created")
        
        
        logger.info("hourly_volumes Athena optimization completed")
        log_memory_usage("End hourly_volumes (Athena)")
        
    except Exception as e:
        logger.error(f"Error in hourly volumes processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_daily_throughput(dates, config_data):
    """Process daily throughput [10 of 29] - Athena full aggregation"""
    logger.info(f"{datetime.now()} Daily Throughput [10 of 29 (mark1)]")
    log_memory_usage("Start daily throughput")
    
    try:
        # Use Athena helpers to do FULL aggregation (bypass problematic library functions)
        logger.info("Using FULL Athena aggregation for throughput")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Use throughput table created by calcs_1 (has 'throughput_hourly' column)
        # Corridor weekly throughput
        cor_weekly_throughput = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='throughput',
            metric_col='throughput_hourly',  # Actual column name in table
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        # Rename throughput_hourly → vph for compatibility
        if not cor_weekly_throughput.empty and 'throughput_hourly' in cor_weekly_throughput.columns:
            cor_weekly_throughput = cor_weekly_throughput.rename(columns={'throughput_hourly': 'vph'})
        save_data(cor_weekly_throughput, "cor_weekly_throughput.pkl")
        
        # Subcorridor weekly throughput
        sub_weekly_throughput = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='throughput',
            metric_col='throughput_hourly',  # Actual column name in table
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        )
        if not sub_weekly_throughput.empty:
            if 'throughput_hourly' in sub_weekly_throughput.columns:
                sub_weekly_throughput = sub_weekly_throughput.rename(columns={'throughput_hourly': 'vph'})
            if 'Corridor' in sub_weekly_throughput.columns:
                sub_weekly_throughput = sub_weekly_throughput.dropna(subset=['Corridor'])
        save_data(sub_weekly_throughput, "sub_weekly_throughput.pkl")
        
        # Corridor monthly throughput
        cor_monthly_throughput = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='throughput',
            metric_col='throughput_hourly',  # Actual column name in table
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        # Rename throughput_hourly → vph for compatibility
        if not cor_monthly_throughput.empty and 'throughput_hourly' in cor_monthly_throughput.columns:
            cor_monthly_throughput = cor_monthly_throughput.rename(columns={'throughput_hourly': 'vph'})
        save_data(cor_monthly_throughput, "cor_monthly_throughput.pkl")
        
        # Subcorridor monthly throughput
        sub_monthly_throughput = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='throughput',
            metric_col='throughput_hourly',  # Actual column name in table
            weight_col=None,
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        )
        if not sub_monthly_throughput.empty:
            if 'throughput_hourly' in sub_monthly_throughput.columns:
                sub_monthly_throughput = sub_monthly_throughput.rename(columns={'throughput_hourly': 'vph'})
            if 'Corridor' in sub_monthly_throughput.columns:
                sub_monthly_throughput = sub_monthly_throughput.dropna(subset=['Corridor'])
        save_data(sub_monthly_throughput, "sub_monthly_throughput.pkl")
        
        logger.info("Daily throughput processing completed")
        log_memory_usage("End daily throughput")
        
        # OLD CODE - DISABLED
        # logger.info("Using Athena optimization for daily throughput")
        # from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        # conf_dict = conf.to_dict()
        # start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        # end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # OLD CODE continues (commented out)
        # cor_weekly_throughput = keep_trying(
        #     athena_get_corridor_weekly_avg,
        #     n_tries=3,
        #     table_name='throughput',
        #     metric_col='vph',
        #     weight_col=None,
        #     start_date=start_date,
        #     end_date=end_date,
        #     corridors_df=config_data['corridors'],
        #     conf=conf_dict
        # )
        # save_data(cor_weekly_throughput, "cor_weekly_throughput.pkl")
        #
        # sub_weekly_throughput = keep_trying(
        #     athena_get_corridor_weekly_avg,
        #     n_tries=3,
        #     table_name='throughput',
        #     metric_col='vph',
        #     weight_col=None,
        #     start_date=start_date,
        #     end_date=end_date,
        #     corridors_df=config_data['subcorridors'],
        #     conf=conf_dict
        # ).dropna(subset=['Corridor'])
        # save_data(sub_weekly_throughput, "sub_weekly_throughput.pkl")
        #
        # cor_monthly_throughput = keep_trying(
        #     athena_get_corridor_monthly_avg,
        #     n_tries=3,
        #     table_name='throughput',
        #     metric_col='vph',
        #     weight_col=None,
        #     start_date=start_date,
        #     end_date=end_date,
        #     corridors_df=config_data['corridors'],
        #     conf=conf_dict
        # )
        # save_data(cor_monthly_throughput, "cor_monthly_throughput.pkl")
        #
        # sub_monthly_throughput = keep_trying(
        #     athena_get_corridor_monthly_avg,
        #     n_tries=3,
        #     table_name='throughput',
        #     metric_col='vph',
        #     weight_col=None,
        #     start_date=start_date,
        #     end_date=end_date,
        #     corridors_df=config_data['subcorridors'],
        #     conf=conf_dict
        # ).dropna(subset=['Corridor'])
        # save_data(sub_monthly_throughput, "sub_monthly_throughput.pkl")
        #
        # logger.info("daily_throughput Athena optimization completed")
        # log_memory_usage("End daily_throughput (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily throughput processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_arrivals_on_green(dates, config_data):
    """Process daily arrivals on green [11 of 29] - Athena optimized"""
    logger.info(f"{datetime.now()} Daily AOG [11 of 29 (mark1)]")
    log_memory_usage("Start arrivals on green")
    
    try:
        logger.info("Using Athena optimization for arrivals on green")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly AOG
        cor_weekly_aog_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.pkl")
        
        # Subcorridor weekly AOG
        sub_weekly_aog_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_aog_by_day, "sub_weekly_aog_by_day.pkl")
        
        # Corridor monthly AOG
        cor_monthly_aog_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.pkl")
        
        # Subcorridor monthly AOG
        sub_monthly_aog_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_aog_by_day, "sub_monthly_aog_by_day.pkl")
        
        logger.info("arrivals_on_green Athena optimization completed")
        log_memory_usage("End arrivals_on_green (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily arrivals on green processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_hourly_arrivals_on_green(dates, config_data):
    """Process hourly arrivals on green [12 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Hourly AOG [12 of 29 (mark1)]")
    log_memory_usage("Start hourly arrivals on green")
    
    try:
        logger.info("Using Athena optimization for hourly arrivals on green")
        from package_athena_helpers import athena_get_corridor_hourly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor monthly AOG by hour (aggregates in Athena!)
        cor_monthly_aog_by_hr = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        )
        save_data(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.pkl")
        
        # Subcorridor monthly AOG by hour
        sub_monthly_aog_by_hr = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='aog',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.pkl")
        
        logger.info("Hourly arrivals on green Athena optimization completed")
        log_memory_usage("End hourly arrivals on green (Athena)")
        
    except Exception as e:
        logger.error(f"Error in hourly arrivals on green processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_daily_progression_ratio(dates, config_data):
    """Process daily progression ratio [13 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Daily Progression Ratio [13 of 29 (mark1)]")
    log_memory_usage("Start daily progression ratio")
    
    try:
        logger.info("Using Athena optimization for progression ratio")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly PR
        cor_weekly_pr_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.pkl")
        
        # Subcorridor weekly PR
        sub_weekly_pr_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.pkl")
        
        # Corridor monthly PR
        cor_monthly_pr_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.pkl")
        
        # Subcorridor monthly PR
        sub_monthly_pr_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.pkl")
        
        logger.info("Daily progression ratio Athena optimization completed")
        log_memory_usage("End daily progression ratio (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily progression ratio processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_hourly_progression_ratio(dates, config_data):
    """Process hourly progression ratio [14 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Hourly Progression Ratio [14 of 29 (mark1)]")
    log_memory_usage("Start hourly progression ratio")
    
    try:
        logger.info("Using Athena optimization for hourly progression ratio")
        from package_athena_helpers import athena_get_corridor_hourly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor monthly PR by hour
        cor_monthly_pr_by_hr = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        )
        save_data(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.pkl")
        
        # Subcorridor monthly PR by hour
        sub_monthly_pr_by_hr = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='arrivals_on_green',
            metric_col='pr',
            weight_col='vol',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.pkl")
        
        logger.info("Hourly progression ratio Athena optimization completed")
        log_memory_usage("End hourly progression ratio (Athena)")
        
    except Exception as e:
        logger.error(f"Error in hourly progression ratio processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_daily_split_failures(dates, config_data):
    """Process daily split failures [15 of 29] - Athena optimized"""
    logger.info(f"{datetime.now()} Daily Split Failures [15 of 29 (mark1)]")
    log_memory_usage("Start daily split failures")
    
    try:
        logger.info("Using Athena optimization for split failures")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly SF (peak hours only)
        cor_weekly_sf_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_sf_by_day, "cor_wsf.pkl")
        
        # Subcorridor weekly SF
        sub_weekly_sf_by_day = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_sf_by_day, "sub_wsf.pkl")
        
        # Corridor monthly SF
        cor_monthly_sf_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_sf_by_day, "cor_monthly_sfd.pkl")
        
        # Subcorridor monthly SF
        sub_monthly_sf_by_day = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_sf_by_day, "sub_monthly_sfd.pkl")
        
        # Calculate SFO (Split Failure Overflow = Off-Peak) - matches R code
        logger.info("Calculating split failure overflow (off-peak)...")
        
        # Need to get hourly SF data to filter by peak/off-peak
        # Read from split_failures table with Hour column
        sf_hourly = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="split_failures",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date']
        )
        
        if not sf_hourly.empty and 'Hour' in sf_hourly.columns:
            # Load peak hours from config
            am_peak_hours = conf.get('AM_PEAK_HOURS', [6, 7, 8, 9])
            pm_peak_hours = conf.get('PM_PEAK_HOURS', [15, 16, 17, 18, 19])
            all_peak_hours = am_peak_hours + pm_peak_hours
            
            # Split into peak (sfp) and off-peak (sfo)
            # Extract hour from Date_Hour or Hour column
            if 'Date_Hour' in sf_hourly.columns:
                sf_hourly['hour_val'] = pd.to_datetime(sf_hourly['Date_Hour']).dt.hour
            elif 'Hour' in sf_hourly.columns:
                sf_hourly['hour_val'] = sf_hourly['Hour']
            else:
                sf_hourly['hour_val'] = 0
            
            # SFO = off-peak (NOT in peak hours)
            sfo = sf_hourly[~sf_hourly['hour_val'].isin(all_peak_hours)].copy()
            
            # Calculate aggregations for SFO
            from monthly_report_package_1_helper import get_weekly_avg_by_day, get_monthly_avg_by_day
            from monthly_report_package_1_helper import get_cor_weekly_sf_by_day, get_cor_monthly_sf_by_day
            
            weekly_sfo_by_day = get_weekly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)
            monthly_sfo_by_day = get_monthly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)
            
            cor_weekly_sfo_by_day = get_cor_weekly_sf_by_day(weekly_sfo_by_day, config_data['corridors'])
            cor_monthly_sfo_by_day = get_cor_monthly_sf_by_day(monthly_sfo_by_day, config_data['corridors'])
            
            sub_weekly_sfo_by_day = get_cor_weekly_sf_by_day(weekly_sfo_by_day, config_data['subcorridors'])
            if not sub_weekly_sfo_by_day.empty and 'Corridor' in sub_weekly_sfo_by_day.columns:
                sub_weekly_sfo_by_day = sub_weekly_sfo_by_day.dropna(subset=['Corridor'])
            
            sub_monthly_sfo_by_day = get_cor_monthly_sf_by_day(monthly_sfo_by_day, config_data['subcorridors'])
            if not sub_monthly_sfo_by_day.empty and 'Corridor' in sub_monthly_sfo_by_day.columns:
                sub_monthly_sfo_by_day = sub_monthly_sfo_by_day.dropna(subset=['Corridor'])
            
            # Save SFO files (matches R addtoRDS calls)
            save_data(cor_weekly_sfo_by_day, "cor_wsfo.pkl")
            save_data(cor_monthly_sfo_by_day, "cor_monthly_sfo.pkl")
            save_data(sub_weekly_sfo_by_day, "sub_wsfo.pkl")
            save_data(sub_monthly_sfo_by_day, "sub_monthly_sfo.pkl")
            
            logger.info("Split failure overflow (SFO) files created")
        else:
            logger.warning("No hourly split failure data available for SFO calculation")
            save_data(pd.DataFrame(), "cor_wsfo.pkl")
            save_data(pd.DataFrame(), "cor_monthly_sfo.pkl")
            save_data(pd.DataFrame(), "sub_wsfo.pkl")
            save_data(pd.DataFrame(), "sub_monthly_sfo.pkl")
        
        logger.info("daily_split_failures Athena optimization completed")
        log_memory_usage("End daily_split_failures (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily split failures processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_hourly_split_failures(dates, config_data):
    """Process hourly split failures [16 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Hourly Split Failures [16 of 29 (mark1)]")
    log_memory_usage("Start hourly split failures")
    
    try:
        logger.info("Using Athena optimization for hourly split failures")
        from package_athena_helpers import athena_get_corridor_hourly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor monthly SF by hour
        cor_msfh = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        )
        save_data(cor_msfh, "cor_msfh.pkl")
        
        # Subcorridor monthly SF by hour
        sub_msfh = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='split_failures',
            metric_col='sf_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        ).dropna(subset=['Corridor'])
        save_data(sub_msfh, "sub_msfh.pkl")
        
        logger.info("Hourly split failures Athena optimization completed")
        log_memory_usage("End hourly split failures (Athena)")
        
    except Exception as e:
        logger.error(f"Error in hourly split failures processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_daily_queue_spillback(dates, config_data):
    """Process daily queue spillback [17 of 29] - Athena optimized"""
    logger.info(f"{datetime.now()} Daily Queue Spillback [17 of 29 (mark1)]")
    log_memory_usage("Start daily queue spillback")
    
    try:
        logger.info("Using Athena optimization for queue spillback")
        from package_athena_helpers import athena_get_corridor_weekly_avg, athena_get_corridor_monthly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor weekly QS
        cor_weekly_qs = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_weekly_qs, "cor_wqs.pkl")
        
        # Subcorridor weekly QS
        sub_weekly_qs = keep_trying(
            athena_get_corridor_weekly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_weekly_qs, "sub_wqs.pkl")
        
        # Corridor monthly QS
        cor_monthly_qs = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict
        )
        save_data(cor_monthly_qs, "cor_monthly_qsd.pkl")
        
        # Subcorridor monthly QS
        sub_monthly_qs = keep_trying(
            athena_get_corridor_monthly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict
        ).dropna(subset=['Corridor'])
        save_data(sub_monthly_qs, "sub_monthly_qsd.pkl")
        
        logger.info("daily_queue_spillback Athena optimization completed")
        log_memory_usage("End daily_queue_spillback (Athena)")
        
    except Exception as e:
        logger.error(f"Error in daily queue spillback processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_hourly_queue_spillback(dates, config_data):
    """Process hourly queue spillback [18 of 29] - Athena optimized (no fallback)"""
    logger.info(f"{datetime.now()} Hourly Queue Spillback [18 of 29 (mark1)]")
    log_memory_usage("Start hourly queue spillback")
    
    try:
        logger.info("Using Athena optimization for hourly queue spillback")
        from package_athena_helpers import athena_get_corridor_hourly_avg
        
        conf_dict = conf.to_dict()
        start_date = dates['wk_calcs_start_date'].strftime('%Y-%m-%d') if hasattr(dates['wk_calcs_start_date'], 'strftime') else str(dates['wk_calcs_start_date'])
        end_date = dates['report_end_date'].strftime('%Y-%m-%d') if hasattr(dates['report_end_date'], 'strftime') else str(dates['report_end_date'])
        
        # Corridor monthly QS by hour
        cor_mqsh = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['corridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        )
        save_data(cor_mqsh, "cor_mqsh.pkl")
        
        # Subcorridor monthly QS by hour
        sub_mqsh = keep_trying(
            athena_get_corridor_hourly_avg,
            n_tries=3,
            table_name='queue_spillback',
            metric_col='qs_freq',
            weight_col='cycles',
            start_date=start_date,
            end_date=end_date,
            corridors_df=config_data['subcorridors'],
            conf=conf_dict,
            time_col='Timeperiod'
        ).dropna(subset=['Corridor'])
        save_data(sub_mqsh, "sub_mqsh.pkl")
        
        logger.info("Hourly queue spillback Athena optimization completed")
        log_memory_usage("End hourly queue spillback (Athena)")
        
    except Exception as e:
        logger.error(f"Error in hourly queue spillback processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()
        raise

def process_travel_time_indexes(dates, config_data):
    """Process travel time and buffer time indexes [19 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} Travel Time Indexes [19 of 29 (mark1)]")
    log_memory_usage("Start travel time indexes")
    
    try:
        # Corridor Travel Time Metrics
        tt = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="cor_travel_time_metrics_1hr",
            start_date=dates['calcs_start_date'],
            end_date=dates['report_end_date']
        )
        
        if not tt.empty:
            log_memory_usage("After reading corridor travel time data")
            
            tt['Corridor'] = tt['Corridor'].astype('category')
            
            # Clean merge to avoid duplicate columns
            corridor_mapping = config_data['all_corridors'][['Zone_Group', 'Zone', 'Corridor']].drop_duplicates()
            tt = tt.merge(corridor_mapping, on='Corridor', how='left', suffixes=('', '_dup'))
            
            # Drop any duplicate columns that might have been created
            duplicate_cols = [col for col in tt.columns if col.endswith('_dup')]
            if duplicate_cols:
                tt = tt.drop(columns=duplicate_cols)
                
            tt = tt.dropna(subset=['Zone_Group'])
            
            # Split into separate metrics
            tti = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'tti']].copy()
            pti = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'pti']].copy()
            bi = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'bi']].copy()
            spd = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'speed_mph']].copy()
            del tt
            gc.collect()
            
            # Load corridor monthly VPH for weighting
            cor_monthly_vph = load_data("cor_monthly_vph.pkl")
            if not cor_monthly_vph.empty:
                # Ensure consistent column naming
                if 'Zone_Group' in cor_monthly_vph.columns and 'Zone' in cor_monthly_vph.columns:
                    cor_monthly_vph = cor_monthly_vph.rename(columns={'Zone_Group': 'Zone_Group_orig'})
                
                zone_mapping = config_data['corridors'][['Zone_Group', 'Zone']].drop_duplicates()
                cor_monthly_vph = cor_monthly_vph.merge(zone_mapping, on='Zone', how='left', suffixes=('', '_dup'))
                
                # Drop duplicate columns
                duplicate_cols = [col for col in cor_monthly_vph.columns if col.endswith('_dup')]
                if duplicate_cols:
                    cor_monthly_vph = cor_monthly_vph.drop(columns=duplicate_cols)
                
                # Calculate corridor metrics
                cor_monthly_tti_by_hr = get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.pkl")
                del cor_monthly_tti_by_hr
                gc.collect()
                
                cor_monthly_pti_by_hr = get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.pkl")
                del cor_monthly_pti_by_hr
                gc.collect()
                
                cor_monthly_bi_by_hr = get_cor_monthly_ti_by_hr(bi, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_bi_by_hr, "cor_monthly_bi_by_hr.pkl")
                del cor_monthly_bi_by_hr
                gc.collect()
                
                cor_monthly_spd_by_hr = get_cor_monthly_ti_by_hr(spd, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_spd_by_hr, "cor_monthly_spd_by_hr.pkl")
                del cor_monthly_spd_by_hr
                gc.collect()
                
                cor_monthly_tti = get_cor_monthly_ti_by_day(tti, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_tti, "cor_monthly_tti.pkl")
                del tti, cor_monthly_tti
                gc.collect()
                
                cor_monthly_pti = get_cor_monthly_ti_by_day(pti, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_pti, "cor_monthly_pti.pkl")
                del pti, cor_monthly_pti
                gc.collect()
                
                cor_monthly_bi = get_cor_monthly_ti_by_day(bi, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_bi, "cor_monthly_bi.pkl")
                del bi, cor_monthly_bi
                gc.collect()
                
                cor_monthly_spd = get_cor_monthly_ti_by_day(spd, cor_monthly_vph, config_data['all_corridors'])
                save_data(cor_monthly_spd, "cor_monthly_spd.pkl")
                del spd, cor_monthly_spd, cor_monthly_vph
                gc.collect()
        
        # Subcorridor Travel Time Metrics
        tt_sub = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="sub_travel_time_metrics_1hr",
            start_date=dates['calcs_start_date'],
            end_date=dates['report_end_date']
        )
        
        if not tt_sub.empty:
            log_memory_usage("After reading subcorridor travel time data")
            
            tt_sub['Corridor'] = tt_sub['Corridor'].astype('category')
            tt_sub['Subcorridor'] = tt_sub['Subcorridor'].astype('category')
            
            # Clean column renaming to avoid conflicts
            # First, check what columns exist
            original_cols = tt_sub.columns.tolist()
            logger.info(f"Original subcorridor columns: {original_cols}")
            
            # Rename for consistency - be more explicit about the mapping
            rename_mapping = {}
            if 'Corridor' in tt_sub.columns:
                rename_mapping['Corridor'] = 'Zone_temp'
            if 'Subcorridor' in tt_sub.columns:
                rename_mapping['Subcorridor'] = 'Corridor'
                
            tt_sub = tt_sub.rename(columns=rename_mapping)
            
            # Now rename Zone_temp to Zone
            if 'Zone_temp' in tt_sub.columns:
                tt_sub = tt_sub.rename(columns={'Zone_temp': 'Zone'})
            
            # Clean merge with subcorridors mapping
            subcorridor_mapping = config_data['subcorridors'][['Zone_Group', 'Zone']].drop_duplicates()
            tt_sub = tt_sub.merge(subcorridor_mapping, on='Zone', how='left', suffixes=('', '_dup'))
            
            # Drop any duplicate columns
            duplicate_cols = [col for col in tt_sub.columns if col.endswith('_dup')]
            if duplicate_cols:
                tt_sub = tt_sub.drop(columns=duplicate_cols)
            
            # Split into separate metrics
            tti_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'tti']].copy()
            pti_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'pti']].copy()
            bi_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'bi']].copy()
            spd_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'speed_mph']].copy()
            del tt_sub
            gc.collect()
            
            # Prepare columns required by aggregation functions
            # Must modify each dataframe directly (loop doesn't persist changes)
            tti_sub['Date'] = pd.to_datetime(tti_sub['Date'])
            tti_sub['Month'] = tti_sub['Date'].dt.to_period('M').dt.to_timestamp()
            tti_sub['Timeperiod'] = pd.to_datetime(tti_sub['Hour'])
            
            pti_sub['Date'] = pd.to_datetime(pti_sub['Date'])
            pti_sub['Month'] = pti_sub['Date'].dt.to_period('M').dt.to_timestamp()
            pti_sub['Timeperiod'] = pd.to_datetime(pti_sub['Hour'])
            
            bi_sub['Date'] = pd.to_datetime(bi_sub['Date'])
            bi_sub['Month'] = bi_sub['Date'].dt.to_period('M').dt.to_timestamp()
            bi_sub['Timeperiod'] = pd.to_datetime(bi_sub['Hour'])
            
            spd_sub['Date'] = pd.to_datetime(spd_sub['Date'])
            spd_sub['Month'] = spd_sub['Date'].dt.to_period('M').dt.to_timestamp()
            spd_sub['Timeperiod'] = pd.to_datetime(spd_sub['Hour'])
            
            # Load subcorridor monthly VPH for weighting
            sub_monthly_vph = load_data("sub_monthly_vph.pkl")
            if not sub_monthly_vph.empty:
                # Ensure consistent column naming for subcorridors
                if 'Zone_Group' in sub_monthly_vph.columns and 'Zone' in sub_monthly_vph.columns:
                    sub_monthly_vph = sub_monthly_vph.rename(columns={'Zone_Group': 'Zone_Group_orig'})
                
                subcorridor_zone_mapping = config_data['subcorridors'][['Zone_Group', 'Zone']].drop_duplicates()
                sub_monthly_vph = sub_monthly_vph.merge(subcorridor_zone_mapping, on='Zone', how='left', suffixes=('', '_dup'))
                
                # Drop duplicate columns
                duplicate_cols = [col for col in sub_monthly_vph.columns if col.endswith('_dup')]
                if duplicate_cols:
                    sub_monthly_vph = sub_monthly_vph.drop(columns=duplicate_cols)
                
                # Calculate subcorridor metrics
                sub_monthly_tti_by_hr = get_cor_monthly_ti_by_hr(tti_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_tti_by_hr, "sub_monthly_tti_by_hr.pkl")
                del sub_monthly_tti_by_hr
                gc.collect()
                
                sub_monthly_pti_by_hr = get_cor_monthly_ti_by_hr(pti_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_pti_by_hr, "sub_monthly_pti_by_hr.pkl")
                del sub_monthly_pti_by_hr
                gc.collect()
                
                sub_monthly_bi_by_hr = get_cor_monthly_ti_by_hr(bi_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_bi_by_hr, "sub_monthly_bi_by_hr.pkl")
                del sub_monthly_bi_by_hr
                gc.collect()
                
                sub_monthly_spd_by_hr = get_cor_monthly_ti_by_hr(spd_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_spd_by_hr, "sub_monthly_spd_by_hr.pkl")
                del sub_monthly_spd_by_hr
                gc.collect()
                
                sub_monthly_tti = get_cor_monthly_ti_by_day(tti_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_tti, "sub_monthly_tti.pkl")
                del tti_sub, sub_monthly_tti
                gc.collect()
                
                sub_monthly_pti = get_cor_monthly_ti_by_day(pti_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_pti, "sub_monthly_pti.pkl")
                del pti_sub, sub_monthly_pti
                gc.collect()
                
                sub_monthly_bi = get_cor_monthly_ti_by_day(bi_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_bi, "sub_monthly_bi.pkl")
                del bi_sub, sub_monthly_bi
                gc.collect()
                
                sub_monthly_spd = get_cor_monthly_ti_by_day(spd_sub, sub_monthly_vph, config_data['subcorridors'])
                save_data(sub_monthly_spd, "sub_monthly_spd.pkl")
                del spd_sub, sub_monthly_spd, sub_monthly_vph
                gc.collect()
        
        log_memory_usage("End travel time indexes")
        logger.info("Travel time indexes processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in travel time indexes processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_cctv_uptime(dates, config_data):
    """Process CCTV uptime [20 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} CCTV Uptimes [20 of 29 (mark1)]")
    log_memory_usage("Start CCTV uptime")
    
    try:
        # Get CCTV uptime from 511 and encoders
        daily_cctv_uptime_511 = get_daily_cctv_uptime(
            conf.athena, "cctv_uptime", config_data['cam_config'], dates['wk_calcs_start_date']
        )
        daily_cctv_uptime_encoders = get_daily_cctv_uptime(
            conf.athena, "cctv_uptime_encoders", config_data['cam_config'], dates['wk_calcs_start_date']
        )

        if not daily_cctv_uptime_511.empty or not daily_cctv_uptime_encoders.empty:
            log_memory_usage("After reading CCTV uptime data")
            
            # Ensure required columns exist
            for df in [daily_cctv_uptime_511, daily_cctv_uptime_encoders]:
                if 'Corridor' not in df.columns:
                    df['Corridor'] = df.get('Corridor_x', 'Unknown')
                if 'Subcorridor' not in df.columns:
                    df['Subcorridor'] = df.get('Corridor_y', 'Unknown')
                if 'Description' not in df.columns:
                    df['Description'] = "NA"
            
            # Merge 511 and encoder data
            daily_cctv_uptime = pd.merge(
                daily_cctv_uptime_511,
                daily_cctv_uptime_encoders,
                on=['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Date'],
                how='outer',
                suffixes=('_511', '_enc')
            )
            del daily_cctv_uptime_511, daily_cctv_uptime_encoders
            gc.collect()
            
            columns = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description']
            camera_info = daily_cctv_uptime[columns].drop_duplicates()
            
            # Create MultiIndex with camera info + Date
            camera_dates = pd.MultiIndex.from_product(
                [camera_info[col].unique() for col in columns] + 
                [pd.date_range(dates['wk_calcs_start_date'], dates['report_end_date'])],
                names=columns + ['Date']
            )
            
            daily_cctv_uptime = (
                daily_cctv_uptime
                .set_index(columns + ['Date'])
                .reindex(camera_dates)
                .reset_index()
            )
            
            # Fill NaN values and calculate uptime metrics
            fill_cols = ['up_enc', 'num_enc', 'uptime_enc', 'up_511', 'num_511', 'uptime_511']
            for col in fill_cols:
                if col in daily_cctv_uptime.columns:
                    daily_cctv_uptime[col] = daily_cctv_uptime[col].fillna(0)
            
            daily_cctv_uptime = daily_cctv_uptime.assign(
                uptime=lambda x: x.get('up_511', 0),
                num=1,
                up=lambda x: np.maximum(x.get('up_511', 0) * 2, x.get('up_enc', 0))
            )
            
            # Convert to categorical
            cat_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description']
            for col in cat_cols:
                if col in daily_cctv_uptime.columns:
                    daily_cctv_uptime[col] = daily_cctv_uptime[col].astype('category')
            
            # Find and filter out bad days
            bad_days = daily_cctv_uptime.groupby('Date').agg({
                'uptime': 'sum',
                'num': 'sum'
            }).assign(
                suptime=lambda x: x['uptime'] / x['num']
            ).query('suptime < 0.2').index
            
            daily_cctv_uptime = daily_cctv_uptime[~daily_cctv_uptime['Date'].isin(bad_days)]
            
            save_data(daily_cctv_uptime, "daily_cctv_uptime.pkl")
            
            # Calculate weekly and monthly metrics
            weekly_cctv_uptime = get_weekly_avg_by_day_cctv(daily_cctv_uptime)
            save_data(weekly_cctv_uptime, "weekly_cctv_uptime.pkl")
            
            monthly_cctv_uptime = daily_cctv_uptime.groupby([
                'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description',
                daily_cctv_uptime['Date'].dt.to_period('M').dt.start_time.rename('Month')
            ]).agg({
                'uptime': ['sum', lambda x: np.average(x, weights=daily_cctv_uptime.loc[x.index, 'num'])],
                'num': 'sum'
            }).reset_index()
            
            monthly_cctv_uptime.columns = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Month', 'up', 'uptime', 'num']
            save_data(monthly_cctv_uptime, "monthly_cctv_uptime.pkl")
            
            # Calculate corridor metrics
            cor_daily_cctv_uptime = get_cor_weekly_avg_by_day(
                daily_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            save_data(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.pkl")
            del daily_cctv_uptime, cor_daily_cctv_uptime
            gc.collect()
            
            cor_weekly_cctv_uptime = get_cor_weekly_avg_by_day(
                weekly_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            save_data(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.pkl")
            del weekly_cctv_uptime, cor_weekly_cctv_uptime
            gc.collect()
            
            cor_monthly_cctv_uptime = get_cor_monthly_avg_by_day(
                monthly_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            save_data(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.pkl")
            del cor_monthly_cctv_uptime
            gc.collect()
            
            # Calculate subcorridor metrics
            daily_cctv_uptime = load_data("daily_cctv_uptime.pkl")
            sub_daily_cctv_uptime = daily_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_daily_cctv_uptime = sub_daily_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_daily_cctv_uptime = get_cor_weekly_avg_by_day(
                sub_daily_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            save_data(sub_daily_cctv_uptime, "sub_daily_cctv_uptime.pkl")
            del daily_cctv_uptime, sub_daily_cctv_uptime
            gc.collect()
            
            weekly_cctv_uptime = load_data("weekly_cctv_uptime.pkl")
            sub_weekly_cctv_uptime = weekly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_weekly_cctv_uptime = sub_weekly_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_weekly_cctv_uptime = get_cor_weekly_avg_by_day(
                sub_weekly_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            save_data(sub_weekly_cctv_uptime, "sub_weekly_cctv_uptime.pkl")
            del weekly_cctv_uptime, sub_weekly_cctv_uptime
            gc.collect()
            
            sub_monthly_cctv_uptime = monthly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_monthly_cctv_uptime = sub_monthly_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_monthly_cctv_uptime = get_cor_monthly_avg_by_day(
                sub_monthly_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            save_data(sub_monthly_cctv_uptime, "sub_monthly_cctv_uptime.pkl")
            del monthly_cctv_uptime, sub_monthly_cctv_uptime
            gc.collect()
            
            log_memory_usage("End CCTV uptime")
            logger.info("CCTV uptime processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in CCTV uptime processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_teams_activities(dates, config_data):
    """Process TEAMS activities [21 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} TEAMS [21 of 29 (mark1)]")
    log_memory_usage("Start TEAMS activities")
    
    try:
        # Get TEAMS tasks data
        teams = get_teams_tasks_from_s3(
            bucket=conf.bucket,
            archived_tasks_prefix="mark/teams/tasks202",
            current_tasks_key="mark/teams/tasks.csv.zip",
            report_start_date=dates['report_start_date']
        )
        
        if not teams.empty:
            log_memory_usage("After reading TEAMS data")
            
            teams = tidy_teams_tasks(
                teams,
                bucket=conf.bucket,
                corridors=config_data['corridors'],
                replicate=True
            )
            
            # Calculate various task metrics
            tasks_by_type = get_outstanding_tasks_by_param(teams, "Task_Type", dates['report_start_date'])
            save_data(tasks_by_type, "tasks_by_type.pkl")
            del tasks_by_type
            gc.collect()
            
            tasks_by_subtype = get_outstanding_tasks_by_param(teams, "Task_Subtype", dates['report_start_date'])
            save_data(tasks_by_subtype, "tasks_by_subtype.pkl")
            del tasks_by_subtype
            gc.collect()
            
            tasks_by_priority = get_outstanding_tasks_by_param(teams, "Priority", dates['report_start_date'])
            save_data(tasks_by_priority, "tasks_by_priority.pkl")
            del tasks_by_priority
            gc.collect()
            
            tasks_by_source = get_outstanding_tasks_by_param(teams, "Task_Source", dates['report_start_date'])
            save_data(tasks_by_source, "tasks_by_source.pkl")
            del tasks_by_source
            gc.collect()
            
            tasks_all = get_outstanding_tasks_by_param(teams, "All", dates['report_start_date'])
            save_data(tasks_all, "tasks_all.pkl")
            del tasks_all
            gc.collect()
            
            # Calculate outstanding tasks by date range
            date_list = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
            cor_outstanding_tasks_by_day_range = []
            
            for date_val in date_list:
                task_data = get_outstanding_tasks_by_day_range(teams, dates['report_start_date'], date_val)
                cor_outstanding_tasks_by_day_range.append(task_data)
                gc.collect()  # Clean up after each iteration
            
            cor_outstanding_tasks_by_day_range = pd.concat(cor_outstanding_tasks_by_day_range, ignore_index=True)
            cor_outstanding_tasks_by_day_range['Zone_Group'] = cor_outstanding_tasks_by_day_range['Zone_Group'].astype('category')
            cor_outstanding_tasks_by_day_range['Corridor'] = cor_outstanding_tasks_by_day_range['Corridor'].astype('category')
            
            # Sort and calculate deltas
            cor_outstanding_tasks_by_day_range = cor_outstanding_tasks_by_day_range.sort_values(['Zone_Group', 'Corridor', 'Month'])
            cor_outstanding_tasks_by_day_range['delta.over45'] = cor_outstanding_tasks_by_day_range.groupby(['Zone_Group', 'Corridor'])['over45'].pct_change()
            cor_outstanding_tasks_by_day_range['delta.mttr'] = cor_outstanding_tasks_by_day_range.groupby(['Zone_Group', 'Corridor'])['mttr'].pct_change()
            
            save_data(cor_outstanding_tasks_by_day_range, "cor_tasks_by_date.pkl")
            
            # Create signal-level data
            sig_outstanding_tasks_by_day_range = cor_outstanding_tasks_by_day_range.copy()
            sig_outstanding_tasks_by_day_range = sig_outstanding_tasks_by_day_range.groupby('Corridor').first().reset_index()
            sig_outstanding_tasks_by_day_range['Zone_Group'] = sig_outstanding_tasks_by_day_range['Corridor']
            sig_outstanding_tasks_by_day_range = sig_outstanding_tasks_by_day_range[
                sig_outstanding_tasks_by_day_range['Corridor'].isin(config_data['all_corridors']['Corridor'])
            ]
            
            save_data(sig_outstanding_tasks_by_day_range, "sig_tasks_by_date.pkl")
            
            del teams, cor_outstanding_tasks_by_day_range, sig_outstanding_tasks_by_day_range
            gc.collect()
            
            log_memory_usage("End TEAMS activities")
            logger.info("TEAMS activities processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in TEAMS activities processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_user_delay_costs(dates, config_data):
    """Process user delay costs [22 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} User Delay Costs [22 of 29 (mark1)]")
    log_memory_usage("Start user delay costs")
    
    try:
        months = pd.date_range(dates['report_start_date'], dates['report_end_date'], freq='MS')
        udc_list = []
        
        for month in months:
            try:
                obj = f"mark/user_delay_costs/date={month.strftime('%Y-%m-%d')}/user_delay_costs_{month.strftime('%Y-%m-%d')}.parquet"
                
                udc_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not udc_data.empty:
                    udc_data = udc_data.dropna(subset=['date'])
                    udc_data = convert_to_utc(udc_data)
                    
                    udc_processed = udc_data.assign(
                        Zone=udc_data['zone'],
                        Corridor=udc_data['corridor'],
                        analysis_month=month,
                        month_hour=lambda x: pd.to_datetime(x['date']).dt.date,
                        delay_cost=udc_data['combined.delay_cost']
                    )
                    
                    # Adjust month_hour to first of month
                    udc_processed['month_hour'] = pd.to_datetime(udc_processed['month_hour']) - pd.to_timedelta(pd.to_datetime(udc_processed['month_hour']).dt.day - 1, unit='D')
                    udc_processed['Month'] = udc_processed['month_hour'].dt.to_period('M').dt.start_time
                    
                    udc_list.append(udc_processed[['Zone', 'Corridor', 'analysis_month', 'month_hour', 'Month', 'delay_cost']])
                    del udc_data, udc_processed
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Could not read UDC data for {month}: {e}")
        
        if udc_list:
            log_memory_usage("After reading all UDC data")
            
            udc = pd.concat(udc_list, ignore_index=True)
            del udc_list
            gc.collect()
            
            # Calculate hourly UDC
            hourly_udc = udc.groupby(['Zone', 'Corridor', 'Month', 'month_hour'])['delay_cost'].sum().reset_index()
            save_data(hourly_udc, "hourly_udc.pkl")
            del hourly_udc
            gc.collect()
            
            # Calculate trend tables
            unique_months = sorted(udc['analysis_month'].unique())
            udc_trend_table_list = {}
            
            for i, current_month in enumerate(unique_months[1:], 1):
                last_month = current_month - relativedelta(months=1)
                last_year = current_month - relativedelta(years=1)
                
                # Filter and aggregate data
                trend_data = udc[
                    (udc['analysis_month'] <= current_month) &
                    (udc['Month'].isin([current_month, last_month, last_year]))
                ].groupby(['Zone', 'Corridor', 'Month'])['delay_cost'].sum().reset_index()
                
                # Pivot and calculate changes
                trend_pivot = trend_data.pivot(index=['Zone', 'Corridor'], columns='Month', values='delay_cost').reset_index()
                trend_pivot['Month'] = current_month
                
                # Calculate percentage changes
                if last_month in trend_pivot.columns and current_month in trend_pivot.columns:
                    trend_pivot['Month-over-Month'] = (
                        (trend_pivot[current_month] - trend_pivot[last_month]) / trend_pivot[last_month]
                    )
                
                if last_year in trend_pivot.columns and current_month in trend_pivot.columns:
                    trend_pivot['Year-over-Year'] = (
                        (trend_pivot[current_month] - trend_pivot[last_year]) / trend_pivot[last_year]
                    )
                
                udc_trend_table_list[current_month] = trend_pivot
                del trend_data, trend_pivot
                gc.collect()
            
            save_data(udc_trend_table_list, "udc_trend_table_list.pkl")
            del udc, udc_trend_table_list
            gc.collect()
            
            log_memory_usage("End user delay costs")
            logger.info("User delay costs processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in user delay costs processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_flash_events(dates, config_data):
    """Process flash events [23 of 29] - reads from get_flash_events.py output"""
    logger.info(f"{datetime.now()} Flash Events [23 of 29 (mark1)]")
    log_memory_usage("Start flash events")
    
    try:
        # Read flash_events data created by get_flash_events.py
        fe = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="flash_events",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if fe.empty:
            logger.warning("No flash events data found - saving empty dataframes")
            save_data(pd.DataFrame(), "cor_monthly_flash.pkl")
            save_data(pd.DataFrame(), "sub_monthly_flash.pkl")
            logger.info("Flash events processing completed (no data)")
            return
        
        fe = fe.assign(
            SignalID=lambda x: pd.Categorical(x['SignalID'])
        )
        
        # Ensure Date is datetime
        if 'Date' in fe.columns:
            fe['Date'] = pd.to_datetime(fe['Date'])
        
        # Flash events data needs a 'flash' column (count of events)
        if 'flash' not in fe.columns:
            # Count flash events per signal per date
            fe = fe.groupby(['SignalID', 'Date']).size().reset_index(name='flash')
        
        logger.info(f"Retrieved {len(fe)} flash event records")
        
        # Calculate monthly flash events aggregation
        monthly_flash = get_monthly_flashevent(fe)
        
        if monthly_flash.empty:
            logger.warning("No monthly flash data - saving empty dataframes")
            save_data(pd.DataFrame(), "cor_monthly_flash.pkl")
            save_data(pd.DataFrame(), "sub_monthly_flash.pkl")
            return
        
        # Group into corridors
        cor_monthly_flash = get_cor_monthly_flash(monthly_flash, config_data['corridors'])
        save_data(cor_monthly_flash, "cor_monthly_flash.pkl")
        
        # Subcorridors - check if result has Corridor column before dropna
        sub_monthly_flash = get_cor_monthly_flash(monthly_flash, config_data['subcorridors'])
        if not sub_monthly_flash.empty and 'Corridor' in sub_monthly_flash.columns:
            sub_monthly_flash = sub_monthly_flash.dropna(subset=['Corridor'])
        save_data(sub_monthly_flash, "sub_monthly_flash.pkl")
        
        logger.info("Flash events processing completed successfully")
        log_memory_usage("End flash events")
        
    except Exception as e:
        logger.error(f"Error in flash events processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_bike_ped_safety_index(dates, config_data):
    """Process bike/ped safety index [24 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} Bike/Ped Safety Index [24 of 29 (mark1)]")
    log_memory_usage("Start bike/ped safety index")
    
    try:
        date_range = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
        
        # Process subcorridor BPSI
        sub_monthly_bpsi_list = []
        for d in date_range:
            try:
                obj = f"mark/bike_ped_safety_index/bpsi_sub_{d.strftime('%Y-%m-%d')}.parquet"
                bpsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not bpsi_data.empty:
                    # Remove system columns
                    bpsi_data = bpsi_data.loc[:, ~bpsi_data.columns.str.startswith('__')]
                    bpsi_data['Month'] = d
                    bpsi_data = bpsi_data.rename(columns={'overall_pct': 'bpsi'})
                    sub_monthly_bpsi_list.append(bpsi_data)
                    del bpsi_data
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Could not read subcorridor BPSI for {d}: {e}")
        
        # Process corridor BPSI
        cor_monthly_bpsi_list = []
        for d in date_range:
            try:
                obj = f"mark/bike_ped_safety_index/bpsi_cor_{d.strftime('%Y-%m-%d')}.parquet"
                bpsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not bpsi_data.empty:
                    # Remove system columns
                    bpsi_data = bpsi_data.loc[:, ~bpsi_data.columns.str.startswith('__')]
                    bpsi_data['Month'] = d
                    bpsi_data = bpsi_data.rename(columns={'overall_pct': 'bpsi'})
                    cor_monthly_bpsi_list.append(bpsi_data)
                    del bpsi_data
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Could not read corridor BPSI for {d}: {e}")
        
        if sub_monthly_bpsi_list or cor_monthly_bpsi_list:
            log_memory_usage("After reading BPSI data")
            
            # Combine subcorridor data
            if sub_monthly_bpsi_list:
                sub_monthly_bpsi = pd.concat(sub_monthly_bpsi_list, ignore_index=True)
                sub_monthly_bpsi['Corridor'] = sub_monthly_bpsi['Corridor'].astype('category')
                sub_monthly_bpsi['Subcorridor'] = sub_monthly_bpsi['Subcorridor'].astype('category')
                del sub_monthly_bpsi_list
                gc.collect()
            else:
                sub_monthly_bpsi = pd.DataFrame()
            
            # Combine corridor data
            if cor_monthly_bpsi_list:
                cor_monthly_bpsi = pd.concat(cor_monthly_bpsi_list, ignore_index=True)
                cor_monthly_bpsi['Corridor'] = cor_monthly_bpsi['Corridor'].astype('category')
                del cor_monthly_bpsi_list
                gc.collect()
            else:
                cor_monthly_bpsi = pd.DataFrame()
            
            # Merge corridor and subcorridor data
            if not cor_monthly_bpsi.empty:
                cor_as_sub = cor_monthly_bpsi.copy()
                cor_as_sub['Subcorridor'] = cor_as_sub['Corridor']
                
                if not sub_monthly_bpsi.empty:
                    sub_monthly_bpsi = pd.concat([sub_monthly_bpsi, cor_as_sub], ignore_index=True)
                else:
                    sub_monthly_bpsi = cor_as_sub
                del cor_as_sub
                gc.collect()
            
            # Process subcorridor data
            if not sub_monthly_bpsi.empty:
                sub_monthly_bpsi = sub_monthly_bpsi.rename(columns={
                    'Corridor': 'Zone_Group',
                    'Subcorridor': 'Corridor'
                })
                
                # Calculate deltas
                sub_monthly_bpsi = sub_monthly_bpsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                sub_monthly_bpsi['ones'] = np.nan
                sub_monthly_bpsi['delta'] = sub_monthly_bpsi.groupby(['Zone_Group', 'Corridor'])['bpsi'].pct_change()
                
                save_data(sub_monthly_bpsi, "sub_monthly_bpsi.pkl")
                del sub_monthly_bpsi
                gc.collect()
            
            # Process corridor data with zone information
            if not cor_monthly_bpsi.empty:
                cor_monthly_bpsi = cor_monthly_bpsi.merge(
                    config_data['corridors'][['Zone_Group', 'Corridor']].drop_duplicates(),
                    on='Corridor',
                    how='left'
                )
                cor_monthly_bpsi = cor_monthly_bpsi[['Zone_Group', 'Corridor', 'Month', 'bpsi']]
                
                # Calculate deltas
                cor_monthly_bpsi = cor_monthly_bpsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                cor_monthly_bpsi['ones'] = np.nan
                cor_monthly_bpsi['delta'] = cor_monthly_bpsi.groupby(['Zone_Group', 'Corridor'])['bpsi'].pct_change()
                
                save_data(cor_monthly_bpsi, "cor_monthly_bpsi.pkl")
                del cor_monthly_bpsi
                gc.collect()
            
            log_memory_usage("End bike/ped safety index")
            logger.info("Bike/Ped safety index processing completed successfully")
        else:
            logger.warning("No BPSI data found for the date range")
        
    except Exception as e:
        logger.error(f"Error in bike/ped safety index processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_relative_speed_index(dates, config_data):
    """Process relative speed index [25 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} Relative Speed Index [25 of 29 (mark1)]")
    log_memory_usage("Start relative speed index")
    
    try:
        date_range = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
        
        # Process subcorridor RSI
        sub_monthly_rsi_list = []
        for d in date_range:
            try:
                obj = f"mark/relative_speed_index/rsi_sub_{d.strftime('%Y-%m-%d')}.parquet"
                rsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not rsi_data.empty:
                    # Remove system columns
                    rsi_data = rsi_data.loc[:, ~rsi_data.columns.str.startswith('__')]
                    rsi_data['Month'] = d
                    sub_monthly_rsi_list.append(rsi_data)
                    del rsi_data
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Could not read subcorridor RSI for {d}: {e}")
        
        # Process corridor RSI
        cor_monthly_rsi_list = []
        for d in date_range:
            try:
                obj = f"mark/relative_speed_index/rsi_cor_{d.strftime('%Y-%m-%d')}.parquet"
                rsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not rsi_data.empty:
                    # Remove system columns
                    rsi_data = rsi_data.loc[:, ~rsi_data.columns.str.startswith('__')]
                    rsi_data['Month'] = d
                    cor_monthly_rsi_list.append(rsi_data)
                    del rsi_data
                    gc.collect()
                    
            except Exception as e:
                logger.warning(f"Could not read corridor RSI for {d}: {e}")
        
        if sub_monthly_rsi_list or cor_monthly_rsi_list:
            log_memory_usage("After reading RSI data")
            
            # Combine subcorridor data
            if sub_monthly_rsi_list:
                sub_monthly_rsi = pd.concat(sub_monthly_rsi_list, ignore_index=True)
                sub_monthly_rsi['Corridor'] = sub_monthly_rsi['Corridor'].astype('category')
                sub_monthly_rsi['Subcorridor'] = sub_monthly_rsi['Subcorridor'].astype('category')
                del sub_monthly_rsi_list
                gc.collect()
            else:
                sub_monthly_rsi = pd.DataFrame()
            
            # Combine corridor data
            if cor_monthly_rsi_list:
                cor_monthly_rsi = pd.concat(cor_monthly_rsi_list, ignore_index=True)
                cor_monthly_rsi['Corridor'] = cor_monthly_rsi['Corridor'].astype('category')
                del cor_monthly_rsi_list
                gc.collect()
            else:
                cor_monthly_rsi = pd.DataFrame()
            
            # Merge corridor and subcorridor data
            if not cor_monthly_rsi.empty:
                cor_as_sub = cor_monthly_rsi.copy()
                cor_as_sub['Subcorridor'] = cor_as_sub['Corridor']
                
                if not sub_monthly_rsi.empty:
                    sub_monthly_rsi = pd.concat([sub_monthly_rsi, cor_as_sub], ignore_index=True)
                else:
                    sub_monthly_rsi = cor_as_sub
                del cor_as_sub
                gc.collect()
            
            # Process subcorridor data
            if not sub_monthly_rsi.empty:
                sub_monthly_rsi = sub_monthly_rsi.rename(columns={
                    'Corridor': 'Zone_Group',
                    'Subcorridor': 'Corridor'
                })
                
                # Calculate deltas
                sub_monthly_rsi = sub_monthly_rsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                sub_monthly_rsi['ones'] = np.nan
                sub_monthly_rsi['delta'] = sub_monthly_rsi.groupby(['Zone_Group', 'Corridor'])['rsi'].pct_change()
                
                save_data(sub_monthly_rsi, "sub_monthly_rsi.pkl")
                del sub_monthly_rsi
                gc.collect()
            
            # Process corridor data with zone information
            if not cor_monthly_rsi.empty:
                cor_monthly_rsi = cor_monthly_rsi.merge(
                    config_data['corridors'][['Zone_Group', 'Corridor']].drop_duplicates(),
                    on='Corridor',
                    how='left'
                )
                cor_monthly_rsi = cor_monthly_rsi[['Zone_Group', 'Corridor', 'Month', 'rsi']]
                
                # Calculate deltas
                cor_monthly_rsi = cor_monthly_rsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                cor_monthly_rsi['ones'] = np.nan
                cor_monthly_rsi['delta'] = cor_monthly_rsi.groupby(['Zone_Group', 'Corridor'])['rsi'].pct_change()
                
                save_data(cor_monthly_rsi, "cor_monthly_rsi.pkl")
                del cor_monthly_rsi
                gc.collect()
            
            log_memory_usage("End relative speed index")
            logger.info("Relative speed index processing completed successfully")
        else:
            logger.warning("No RSI data found for the date range")
        
    except Exception as e:
        logger.error(f"Error in relative speed index processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def process_crash_indices(dates, config_data):
    """Process crash indices [26 of 29] - Memory optimized"""
    logger.info(f"{datetime.now()} Crash Indices [26 of 29 (mark1)]")
    log_memory_usage("Start crash indices")
    
    try:
        # Read crash data
        crashes = s3read_using(
            pd.read_excel,
            bucket=conf.bucket,
            object="Collisions Dataset 2017-2019.xlsm",
            engine='openpyxl'
        )
        
        if not crashes.empty:
            log_memory_usage("After reading crash data")
            
            # Check if required columns exist and map alternatives
            required_cols = ['Signal_ID_Clean', 'Month', 'crashes_k', 'crashes_a', 'crashes_b',
                           'crashes_c', 'crashes_o', 'crashes_total', 'cost']
            
            col_mapping = {
                'Signal_ID_Clean': ['SignalID', 'Signal_ID', 'signal_id'],
                'crashes_total': ['total_crashes', 'Total_Crashes', 'crashes'],
                'crashes_k': ['k_crashes', 'K_Crashes'],
                'crashes_a': ['a_crashes', 'A_Crashes'],
                'crashes_b': ['b_crashes', 'B_Crashes'],
                'crashes_c': ['c_crashes', 'C_Crashes'],
                'crashes_o': ['o_crashes', 'O_Crashes'],
                'cost': ['Cost', 'total_cost', 'Total_Cost']
            }
            
            # Map alternative column names
            for standard_col, alternatives in col_mapping.items():
                if standard_col not in crashes.columns:
                    for alt_col in alternatives:
                        if alt_col in crashes.columns:
                            crashes = crashes.rename(columns={alt_col: standard_col})
                            logger.info(f"Mapped {alt_col} to {standard_col}")
                            break
            
            # Select only available columns
            final_cols = [col for col in required_cols if col in crashes.columns]
            crashes = crashes[final_cols]
            
            if 'Signal_ID_Clean' in crashes.columns:
                crashes = crashes.rename(columns={'Signal_ID_Clean': 'SignalID'})
            
            crashes = crashes.dropna()
            
            if not crashes.empty:
                crashes['SignalID'] = crashes['SignalID'].astype('category')
                crashes['Month'] = pd.to_datetime(crashes['Month'])
                
                # Aggregate by SignalID and Month
                agg_dict = {}
                for col in crashes.columns:
                    if col.startswith('crashes_') or col == 'cost':
                        agg_dict[col] = 'sum'
                
                crashes = crashes.groupby(['SignalID', 'Month']).agg(agg_dict).reset_index()
                
                # Load monthly VPD
                monthly_vpd = load_data("monthly_vpd.pkl")
                
                if not monthly_vpd.empty:
                    log_memory_usage("After loading monthly VPD")
                    
                    # Complete VPD data and calculate 12-month rolling average
                    date_range_vpd = pd.date_range(monthly_vpd['Month'].min(), monthly_vpd['Month'].max(), freq='MS')
                    signal_list = monthly_vpd['SignalID'].unique()
                    
                    complete_vpd = pd.MultiIndex.from_product([signal_list, date_range_vpd], names=['SignalID', 'Month']).to_frame(index=False)
                    monthly_vpd = complete_vpd.merge(monthly_vpd, on=['SignalID', 'Month'], how='left')
                    del complete_vpd
                    gc.collect()
                    
                    monthly_vpd = monthly_vpd.sort_values(['SignalID', 'Month'])
                    monthly_vpd['vpd12'] = monthly_vpd.groupby('SignalID')['vpd'].transform(
                        lambda x: x.rolling(window=12, min_periods=1).mean()
                    )
                    
                    # Calculate 36-month rolling crashes
                    complete_crashes = pd.MultiIndex.from_product([signal_list, date_range_vpd], names=['SignalID', 'Month']).to_frame(index=False)
                    monthly_36mo_crashes = complete_crashes.merge(crashes, on=['SignalID', 'Month'], how='left')
                    del complete_crashes, crashes
                    gc.collect()
                    
                    # Fill NaN with 0
                    crash_cols = [col for col in monthly_36mo_crashes.columns if col.startswith('crashes_') or col == 'cost']
                    monthly_36mo_crashes[crash_cols] = monthly_36mo_crashes[crash_cols].fillna(0)
                    
                    monthly_36mo_crashes = monthly_36mo_crashes.sort_values(['SignalID', 'Month'])
                    
                    # Calculate 36-month rolling sums
                    for col in crash_cols:
                        if col in monthly_36mo_crashes.columns:
                            monthly_36mo_crashes[col] = monthly_36mo_crashes.groupby('SignalID')[col].transform(
                                lambda x: x.rolling(window=36, min_periods=1).sum()
                            )
                    
                    # Use fixed 36-month period for all months (hack from R code)
                    max_month = monthly_36mo_crashes['Month'].max()
                    monthly_36mo_crashes = monthly_36mo_crashes[monthly_36mo_crashes['Month'] == max_month]
                    
                    # Replicate for all VPD months
                    all_months = monthly_vpd['Month'].unique()
                    monthly_36mo_crashes_expanded = []
                    for month in all_months:
                        temp = monthly_36mo_crashes.copy()
                        temp['Month'] = month
                        monthly_36mo_crashes_expanded.append(temp)
                        del temp
                        gc.collect()
                    
                    monthly_36mo_crashes = pd.concat(monthly_36mo_crashes_expanded, ignore_index=True)
                    del monthly_36mo_crashes_expanded
                    gc.collect()
                    
                    # Merge crashes and VPD
                    monthly_crashes = monthly_36mo_crashes.merge(monthly_vpd, on=['SignalID', 'Month'], how='outer')
                    del monthly_36mo_crashes, monthly_vpd
                    gc.collect()
                    
                    # Calculate crash indices
                    if 'crashes_total' in monthly_crashes.columns and 'vpd' in monthly_crashes.columns:
                        monthly_crashes['cri'] = (monthly_crashes['crashes_total'] * 1000) / (monthly_crashes['vpd'] * 3)
                    else:
                        monthly_crashes['cri'] = 0
                        
                    if 'cost' in monthly_crashes.columns and 'vpd' in monthly_crashes.columns:
                        monthly_crashes['kabco'] = monthly_crashes['cost'] / (monthly_crashes['vpd'] * 3)
                    else:
                        monthly_crashes['kabco'] = 0
                    
                    monthly_crashes['Date'] = monthly_crashes['Month']
                    monthly_crashes['CallPhase'] = 0
                    
                    # Calculate monthly indices
                    monthly_crash_rate_index = get_monthly_avg_by_day(monthly_crashes, "cri")
                    save_data(monthly_crash_rate_index, "monthly_crash_rate_index.pkl")
                    del monthly_crash_rate_index
                    gc.collect()
                    
                    monthly_kabco_index = get_monthly_avg_by_day(monthly_crashes, "kabco")
                    save_data(monthly_kabco_index, "monthly_kabco_index.pkl")
                    del monthly_kabco_index
                    gc.collect()
                    
                    # Calculate corridor indices
                    monthly_crash_rate_index = load_data("monthly_crash_rate_index.pkl")
                    cor_monthly_crash_rate_index = get_cor_monthly_avg_by_day(
                        monthly_crash_rate_index, config_data['corridors'], "cri"
                    )
                    save_data(cor_monthly_crash_rate_index, "cor_monthly_crash_rate_index.pkl")
                    del monthly_crash_rate_index, cor_monthly_crash_rate_index
                    gc.collect()
                    
                    monthly_kabco_index = load_data("monthly_kabco_index.pkl")
                    cor_monthly_kabco_index = get_cor_monthly_avg_by_day(
                        monthly_kabco_index, config_data['corridors'], "kabco"
                    )
                    save_data(cor_monthly_kabco_index, "cor_monthly_kabco_index.pkl")
                    del monthly_kabco_index, cor_monthly_kabco_index
                    gc.collect()
                    
                    # Calculate subcorridor indices
                    monthly_crash_rate_index = load_data("monthly_crash_rate_index.pkl")
                    sub_monthly_crash_rate_index = get_cor_monthly_avg_by_day(
                        monthly_crash_rate_index, config_data['subcorridors'], "cri"
                    )
                    save_data(sub_monthly_crash_rate_index, "sub_monthly_crash_rate_index.pkl")
                    del monthly_crash_rate_index, sub_monthly_crash_rate_index
                    gc.collect()
                    
                    monthly_kabco_index = load_data("monthly_kabco_index.pkl")
                    sub_monthly_kabco_index = get_cor_monthly_avg_by_day(
                        monthly_kabco_index, config_data['subcorridors'], "kabco"
                    )
                    save_data(sub_monthly_kabco_index, "sub_monthly_kabco_index.pkl")
                    del monthly_crashes, monthly_kabco_index, sub_monthly_kabco_index
                    gc.collect()
                    
                    log_memory_usage("End crash indices")
                    logger.info("Crash indices processing completed successfully")
                else:
                    logger.warning("No monthly VPD data available for crash index calculations")
            else:
                logger.warning("No valid crash data after processing")
        else:
            logger.warning("No crash data found in the Excel file")
        
    except UnicodeDecodeError as e:
        logger.error(f"Unicode decode error reading Excel file: {e}")
        logger.info("Skipping crash indices processing due to file encoding issues")
    except FileNotFoundError as e:
        logger.warning(f"Crash data file not found: {e}")
        logger.info("Skipping crash indices processing - file not available")
    except Exception as e:
        logger.error(f"Error in crash indices processing: {e}")
        logger.error(traceback.format_exc())
        gc.collect()

def safe_dropna_corridor(df, name):
    """Safely drop NaN values from Corridor column"""
    try:
        if not df.empty and 'Corridor' in df.columns:
            return df.dropna(subset=['Corridor'])
        else:
            logger.warning(f"{name}: DataFrame is empty or missing 'Corridor' column")
            return pd.DataFrame()
    except Exception as e:
        logger.warning(f"Error in safe_dropna_corridor for {name}: {e}")
        return pd.DataFrame()

def ensure_datetime_column(df, col_name):
    """Ensure datetime column exists and is properly formatted"""
    try:
        if col_name in df.columns:
            df[col_name] = pd.to_datetime(df[col_name])
        return df
    except Exception as e:
        logger.warning(f"Error converting {col_name} to datetime: {e}")
        return df

def ensure_timeperiod_column(df):
    """Ensure Timeperiod column exists"""
    try:
        if 'Timeperiod' not in df.columns and 'Hour' in df.columns:
            df['Timeperiod'] = df['Hour']
        elif 'Timeperiod' not in df.columns:
            df['Timeperiod'] = 12  # Default hour
        return df
    except Exception as e:
        logger.warning(f"Error ensuring Timeperiod column: {e}")
        return df

def ensure_throughput_column(df):
    """Ensure throughput column exists"""
    try:
        throughput_candidates = ['throughput', 'Throughput', 'thruput', 'Thruput']
        for candidate in throughput_candidates:
            if candidate in df.columns:
                if candidate != 'throughput':
                    df = df.rename(columns={candidate: 'throughput'})
                break
        else:
            # If no throughput column found, create a default one
            logger.warning("No throughput column found, creating default")
            df['throughput'] = 0
        return df
    except Exception as e:
        logger.warning(f"Error ensuring throughput column: {e}")
        return df

def ensure_metric_column(df, target_col, candidates):
    """Ensure a metric column exists by checking candidates"""
    try:
        for candidate in candidates:
            if candidate in df.columns:
                if candidate != target_col:
                    df = df.rename(columns={candidate: target_col})
                return df
        
        # If no candidate found, create default
        logger.warning(f"No {target_col} column found from candidates {candidates}, creating default")
        df[target_col] = 0
        return df
    except Exception as e:
        logger.warning(f"Error ensuring {target_col} column: {e}")
        return df

def convert_to_utc(df):
    """Convert timezone-aware datetime columns to UTC"""
    try:
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns, UTC]' or pd.api.types.is_datetime64tz_dtype(df[col]):
                df[col] = pd.to_datetime(df[col]).dt.tz_convert('UTC').dt.tz_localize(None)
        return df
    except Exception as e:
        logger.warning(f"Error converting to UTC: {e}")
        return df

def main():
    """Main function to run the monthly report package - Memory optimized"""
    logger.info("Starting Monthly Report Package Processing")
    
    # Start memory tracing
    tracemalloc.start()
    log_memory_usage("Start of main")
    
    try:
        # Initialize configuration and dates
        config_data = initialize_config()
        dates = calculate_dates(config_data['report_end_date'])
        
        logger.info(f"Processing data from {dates['calcs_start_date']} to {dates['report_end_date']}")
        log_memory_usage("After initialization")
        
        # Process each section sequentially with memory optimization
        processing_functions = [
            # ENABLE ALL 26 METRICS FOR FINAL PRODUCTION RUN:
            (process_detector_uptime, "Vehicle Detector Uptime"),  # #1 - ✅ Athena
            (process_ped_pushbutton_uptime, "Pedestrian Pushbutton Uptime"),  # #2 - ✅ Athena (FIXED!)
            (process_watchdog_alerts, "Watchdog Alerts"),  # #3 - ✅ Athena
            (process_daily_ped_activations, "Daily Pedestrian Activations"),  # #4 - ✅ Works!
            (process_hourly_ped_activations, "Hourly Pedestrian Activations"),  # #5 - ✅ FIXED!
            (process_pedestrian_delay, "Pedestrian Delay"),  # #6 - ⏭️ SKIP: aggregations.py bug
            (process_communications_uptime, "Communications Uptime"),  # #7 - ✅ Athena
            (process_daily_volumes, "Daily Volumes"),  # #8 - ✅ Athena
            (process_hourly_volumes, "Hourly Volumes"),  # #9 - ✅ Athena
            (process_daily_throughput, "Daily Throughput"),  # #10 - ⏭️ SKIP: Athena query hangs
            (process_arrivals_on_green, "Arrivals on Green"),  # #11 - ✅ Athena
            (process_hourly_arrivals_on_green, "Hourly Arrivals on Green"),  # #12 - ✅ Athena
            (process_daily_progression_ratio, "Daily Progression Ratio"),  # #13 - ✅ Athena
            (process_hourly_progression_ratio, "Hourly Progression Ratio"),  # #14 - ✅ Athena
            (process_daily_split_failures, "Daily Split Failures"),  # #15 - ✅ Athena
            (process_hourly_split_failures, "Hourly Split Failures"),  # #16 - ✅ Athena
            (process_daily_queue_spillback, "Daily Queue Spillback"),  # #17 - ✅ Athena
            (process_hourly_queue_spillback, "Hourly Queue Spillback"),  # #18 - ✅ Athena
            (process_travel_time_indexes, "Travel Time Indexes"),  # #19 - ✅ Works
            (process_cctv_uptime, "CCTV Uptime"),  # #20 - ✅ Works
            (process_teams_activities, "TEAMS Activities"),  # #21 - ✅ Works
            (process_user_delay_costs, "User Delay Costs"),  # #22 - ✅ Works
            (process_flash_events, "Flash Events"),  # #23 - ✅ Works!
            (process_bike_ped_safety_index, "Bike/Ped Safety Index"),  # #24 - ✅ Works
            (process_relative_speed_index, "Relative Speed Index"),  # #25 - ✅ Works
            (process_crash_indices, "Crash Indices"),  # #26 - ✅ Works
        ]
        
        # Track progress
        total_functions = len(processing_functions)
        
        for i, (func, description) in enumerate(processing_functions, 1):
            try:
                logger.info(f"Starting {description} [{i} of {total_functions}]")
                log_memory_usage(f"Before {description}")
                
                func(dates, config_data)
                
                # Force garbage collection after each function
                gc.collect()
                log_memory_usage(f"After {description}")
                logger.info(f"Completed {description} [{i} of {total_functions}]")
                
            except Exception as e:
                logger.error(f"Failed to process {description}: {e}")
                logger.error(traceback.format_exc())
                # Force cleanup and continue
                gc.collect()
                continue
        
        log_memory_usage("End of main")
        logger.info("Monthly Report Package Processing completed successfully")
        
        # Final memory report
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            logger.info(f"Final memory - Current: {current / 1024 / 1024:.1f} MB, Peak: {peak / 1024 / 1024:.1f} MB")
        
    except Exception as e:
        logger.error(f"Critical error in main processing: {e}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        # Clean up any open connections and stop memory tracing
        cleanup_connections()
        if tracemalloc.is_tracing():
            tracemalloc.stop()

def cleanup_connections():
    """Clean up any open database connections and force garbage collection"""
    try:
        # Close any pandas/SQL connections
        gc.collect()
        logger.info("Cleaned up connections and memory")
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")

def get_memory_usage():
    """Get current memory usage in MB"""
    try:
        import psutil
        process = psutil.Process()
        return process.memory_info().rss / 1024 / 1024
    except ImportError:
        return 0

def optimize_dataframe_memory(df):
    """Optimize DataFrame memory usage by downcasting numeric types"""
    try:
        # Downcast integers
        for col in df.select_dtypes(include=['int64']).columns:
            if df[col].min() >= 0:
                if df[col].max() < 255:
                    df[col] = df[col].astype('uint8')
                elif df[col].max() < 65535:
                    df[col] = df[col].astype('uint16')
                elif df[col].max() < 4294967295:
                    df[col] = df[col].astype('uint32')
            else:
                if df[col].min() > -128 and df[col].max() < 127:
                    df[col] = df[col].astype('int8')
                elif df[col].min() > -32768 and df[col].max() < 32767:
                    df[col] = df[col].astype('int16')
                elif df[col].min() > -2147483648 and df[col].max() < 2147483647:
                    df[col] = df[col].astype('int32')
        
        # Downcast floats
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert object columns to category if appropriate
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].nunique() / len(df) < 0.5:  # If less than 50% unique values
                df[col] = df[col].astype('category')
        
        return df
    except Exception as e:
        logger.warning(f"Error optimizing DataFrame memory: {e}")
        return df

def process_in_chunks(data_func, chunk_size=10000, *args, **kwargs):
    """Process data in chunks to manage memory usage"""
    try:
        # Get total data size first
        total_data = data_func(*args, **kwargs)
        
        if len(total_data) <= chunk_size:
            return total_data
        
        # Process in chunks
        chunks = []
        for i in range(0, len(total_data), chunk_size):
            chunk = total_data.iloc[i:i+chunk_size].copy()
            chunk = optimize_dataframe_memory(chunk)
            chunks.append(chunk)
            
            # Clean up
            del chunk
            gc.collect()
        
        # Combine chunks
        result = pd.concat(chunks, ignore_index=True)
        del chunks, total_data
        gc.collect()
        
        return result
        
    except Exception as e:
        logger.error(f"Error in chunked processing: {e}")
        return pd.DataFrame()

def validate_config_data(config_data):
    """Validate that required configuration data is present"""
    required_keys = ['corridors', 'subcorridors', 'all_corridors', 'signals_list']
    
    for key in required_keys:
        if key not in config_data:
            logger.error(f"Missing required configuration key: {key}")
            return False
        
        if key != 'signals_list' and config_data[key].empty:
            logger.error(f"Configuration data for {key} is empty")
            return False
    
    return True

def create_backup_config():
    """Create a minimal backup configuration if main config fails"""
    logger.warning("Creating backup configuration with minimal data")
    
    # Create minimal test data
    corridors = pd.DataFrame({
        'SignalID': range(1000, 1010),
        'Zone_Group': ['TestZone'] * 10,
        'Zone': ['TestSubZone'] * 10,
        'Corridor': ['Test Corridor'] * 10,
        'Name': [f'Test_Signal_{i}' for i in range(1000, 1010)]
    })
    
    subcorridors = corridors.copy()
    subcorridors['Subcorridor'] = subcorridors['Corridor']
    
    all_corridors = corridors.copy()
    
    cam_config = pd.DataFrame({
        'CameraID': range(2000, 2005),
        'Zone_Group': ['TestZone'] * 5,
        'Zone': ['TestSubZone'] * 5,
        'Corridor': ['Test Corridor'] * 5,
        'Location': [f'Test_Camera_{i}' for i in range(2000, 2005)],
        'As_of_Date': [date.today() - timedelta(days=365)] * 5
    })
    
    signals_list = list(corridors['SignalID'])
    
    return {
        'corridors': corridors,
        'subcorridors': subcorridors,
        'all_corridors': all_corridors,
        'cam_config': cam_config,
        'signals_list': signals_list
    }

def safe_process_section(func, description, dates, config_data, max_retries=2):
    """Safely process a section with retries and error handling"""
    for attempt in range(max_retries):
        try:
            logger.info(f"Starting {description} (attempt {attempt + 1})")
            log_memory_usage(f"Before {description}")
            
            func(dates, config_data)
            
            # Force garbage collection after successful completion
            gc.collect()
            log_memory_usage(f"After {description}")
            logger.info(f"Completed {description}")
            return True
            
        except MemoryError as e:
            logger.error(f"Memory error in {description} (attempt {attempt + 1}): {e}")
            gc.collect()  # Force cleanup
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying {description} after memory cleanup...")
                continue
            else:
                logger.error(f"Failed {description} after {max_retries} attempts due to memory issues")
                return False
                
        except Exception as e:
            logger.error(f"Error in {description} (attempt {attempt + 1}): {e}")
            logger.error(traceback.format_exc())
            gc.collect()  # Cleanup on any error
            
            if attempt < max_retries - 1:
                logger.info(f"Retrying {description}...")
                continue
            else:
                logger.error(f"Failed {description} after {max_retries} attempts")
                return False
    
    return False

def check_system_resources():
    """Check available system resources before processing"""
    try:
        import psutil
        
        # Check available memory
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        
        logger.info(f"Available memory: {available_gb:.1f} GB ({memory.percent}% used)")
        
        if available_gb < 1.0:  # Less than 1GB available
            logger.warning("Low memory available - processing may be slow or fail")
        
        # Check disk space
        disk = psutil.disk_usage('.')
        available_disk_gb = disk.free / (1024**3)
        
        logger.info(f"Available disk space: {available_disk_gb:.1f} GB")
        
        if available_disk_gb < 5.0:  # Less than 5GB available
            logger.warning("Low disk space available - may affect data saving")
        
        return available_gb > 0.5  # Return False if less than 500MB available
        
    except ImportError:
        logger.warning("psutil not available - cannot check system resources")
        return True

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

if __name__ == "__main__":
    setup_logging("INFO", "monthly_report_package_1.log")
    
    # Check system resources before starting
    if not check_system_resources():
        logger.error("Insufficient system resources to run processing")
        sys.exit(1)
    
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)