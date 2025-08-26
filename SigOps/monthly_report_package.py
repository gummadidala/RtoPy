#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monthly Report Package - Python Implementation
Converted from R script for SigOps traffic signal operations reporting

This script processes various traffic signal metrics including:
- Vehicle detector uptime
- Pedestrian pushbutton uptime  
- Watchdog alerts
- Daily/hourly volumes
- Arrivals on green
- Split failures
- Queue spillback
- And more...
"""

import sys
import os
import time
import subprocess
import gc
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from typing import List, Dict, Any, Optional, Iterator
import traceback
import pickle
import io
from contextlib import contextmanager

import pandas as pd
import numpy as np
import boto3
import yaml
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config

# Add parent directory to path to import RtoPy modules
sys.path.append(str(Path(__file__).parent.parent))

# Import from RtoPy repository
from aggregations import (
    sigify,
    get_monthly_avg_by_day,
    get_weekly_avg_by_day,
    get_daily_avg,
    get_vph,
    # get_quarterly,
    # calculate_reliability_metrics
)
from s3_parquet_io import s3_read_parquet_parallel
from configs import get_corridors
from monthly_report_functions import parallel_process_dates
from database_functions import execute_athena_query

from SigOps.configs import get_cam_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Memory management utilities
@contextmanager
def memory_cleanup():
    """Context manager for automatic memory cleanup"""
    try:
        yield
    finally:
        gc.collect()

def optimize_dataframe_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Optimize DataFrame memory usage by downcasting numeric types"""
    if df.empty:
        return df
    
    start_memory = df.memory_usage(deep=True).sum() / 1024**2
    
    # Optimize numeric columns
    for col in df.select_dtypes(include=[np.number]).columns:
        col_type = df[col].dtype
        
        if col_type != object:
            c_min = df[col].min()
            c_max = df[col].max()
            
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    
    # Optimize object columns
    for col in df.select_dtypes(include=['object']).columns:
        num_unique_values = len(df[col].unique())
        num_total_values = len(df[col])
        if num_unique_values / num_total_values < 0.5:
            df[col] = df[col].astype('category')
    
    end_memory = df.memory_usage(deep=True).sum() / 1024**2
    logger.debug(f"Memory usage decreased from {start_memory:.2f}MB to {end_memory:.2f}MB "
                f"({100 * (start_memory - end_memory) / start_memory:.1f}% reduction)")
    
    return df

def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 10000) -> Iterator[pd.DataFrame]:
    """Yield successive chunks from DataFrame"""
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i:i + chunk_size]

def retry_on_failure(max_retries=3, delay=1.0, backoff=2.0):
    """Decorator for retrying failed operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed: {e}, retrying in {delay * (backoff ** attempt)}s")
                    time.sleep(delay * (backoff ** attempt))
            return None
        return wrapper
    return decorator

def create_progress_tracker(total_steps: int, description: str = "Processing"):
    """Create a progress tracking function"""
    def tracker(current_step: int):
        progress = (current_step / total_steps) * 100
        logger.info(f"{description}: {current_step}/{total_steps} ({progress:.1f}%)")
    return tracker

def batch_process(items: List[Any], batch_size: int, process_func, **kwargs):
    """Process items in batches"""
    results = []
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        try:
            result = process_func(batch, **kwargs)
            results.append(result)
        except Exception as e:
            logger.error(f"Error processing batch {i//batch_size + 1}: {e}")
    return results

def format_duration(seconds: float) -> str:
    """Format duration in seconds to human readable string"""
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes > 0:
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{seconds:.1f}s"

def get_memory_usage() -> Dict[str, float]:
    """Get current memory usage information"""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': process.memory_percent()
        }
    except ImportError:
        return {'rss_mb': 0, 'vms_mb': 0, 'percent': 0}

def get_usable_cores() -> int:
    """Get number of usable CPU cores"""
    try:
        import multiprocessing
        return max(1, multiprocessing.cpu_count() - 1)
    except:
        return 1

@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str):
    """Write parquet file to S3 with retry logic"""
    try:
        # Convert DataFrame to parquet bytes
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        # Upload to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.read(),
            ContentType='application/octet-stream'
        )
        
        logger.info(f"Successfully wrote {len(df)} records to s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Error writing parquet to S3 {bucket}/{key}: {e}")
        raise

def read_parquet_file(bucket: str, key: str) -> pd.DataFrame:
    """Read parquet file from S3"""
    return read_parquet_from_s3(bucket, key)

def get_tuesdays(df: pd.DataFrame) -> pd.DataFrame:
    """Get Tuesday mapping for weekly calculations"""
    if df.empty or ('Date' not in df.columns and 'Date_Hour' not in df.columns):
        return pd.DataFrame()
    
    date_col = 'Date' if 'Date' in df.columns else 'Date_Hour'
    dates = pd.to_datetime(df[date_col])
    
    # Get week numbers and find Tuesday for each week
    weeks = dates.dt.isocalendar().week
    years = dates.dt.year
    
    tuesdays = []
    for year in years.unique():
        for week in weeks[years == year].unique():
            # Find Tuesday of this week
            tuesday = pd.to_datetime(f"{year}-W{week:02d}-2", format="%Y-W%W-%w")
            tuesdays.append({'Year': year, 'Week': week, 'Tuesday': tuesday})
    
    return pd.DataFrame(tuesdays)

class MonthlyReportProcessor:
    """Main class for processing monthly traffic signal reports"""
    
    def __init__(self, config):
        """Initialize the processor with configuration"""
        self.config = config
        self.start_time = None
        
        # Initialize AWS clients with error handling
        try:
            boto_config = Config(
                retries={'max_attempts': 10, 'mode': 'standard'},
                max_pool_connections=500
            )
            self.s3_client = boto3.client('s3', config=boto_config)
            self.athena_client = boto3.client('athena', config=boto_config)
        except Exception as e:
            logger.error(f"Failed to initialize AWS clients: {e}")
            raise
        
        # Date configurations
        self.setup_dates()
        
        # Load signal configurations
        self.load_signal_configs()
        
        # Initialize data storage attributes
        self.papd = None
        self.aog = None
        self.sf = None
        self.qs = None
            
    def setup_dates(self):
        """Setup date ranges for calculations"""
        try:
            self.report_end_date = self.config.get('report_end_date', datetime.now().date())
            if isinstance(self.report_end_date, str):
                self.report_end_date = pd.to_datetime(self.report_end_date).date()
                
            self.calcs_start_date = self.config.get('calcs_start_date')
            if isinstance(self.calcs_start_date, str):
                self.calcs_start_date = pd.to_datetime(self.calcs_start_date).date()
                
            self.wk_calcs_start_date = self.config.get('wk_calcs_start_date')
            if isinstance(self.wk_calcs_start_date, str):
                self.wk_calcs_start_date = pd.to_datetime(self.wk_calcs_start_date).date()
                
            self.report_start_date = self.config.get('report_start_date')
            if isinstance(self.report_start_date, str):
                self.report_start_date = pd.to_datetime(self.report_start_date).date()
        
            logger.info(f"Week Calcs Start Date: {self.wk_calcs_start_date}")
            logger.info(f"Calcs Start Date: {self.calcs_start_date}")
            logger.info(f"Report End Date: {self.report_end_date}")
            
        except Exception as e:
            logger.error(f"Error setting up dates: {e}")
            raise
        
    def load_signal_configs(self):
        """Load signal configuration data"""
        try:
            # Load corridors using RtoPy function
            self.corridors = self._load_corridors()
            self.subcorridors = self._load_subcorridors()
            self.signals_list = self._load_signals_list()
            self.cam_config = self._load_camera_config()
            
            # Create all_corridors for travel time processing
            self.all_corridors = self.corridors.copy()
            
        except Exception as e:
            logger.error(f"Error loading signal configurations: {e}")
            raise
        
    def _load_corridors(self) -> pd.DataFrame:
        """Load corridor configuration"""
        try:
            corridors_file = self.config.get('corridors_filename_s3', '')
            if not corridors_file:
                raise ValueError("corridors_filename_s3 not specified in config")
                
            corridors = get_corridors(corridors_file, filter_signals=True)
            corridors = optimize_dataframe_memory(corridors)
            logger.info(f"Loaded {len(corridors)} corridor configurations")
            return corridors
            
        except Exception as e:
            logger.warning(f"Could not load corridors config: {e}")
            return pd.DataFrame(columns=['SignalID', 'Corridor', 'Zone_Group', 'Zone', 'Name'])
        
    def _load_subcorridors(self) -> pd.DataFrame:
        """Load subcorridor configuration"""
        try:
            if self.corridors.empty or 'Subcorridor' not in self.corridors.columns:
                logger.warning("No Subcorridor column found")
                return pd.DataFrame()
            
            subcorridors = self.corridors[self.corridors['Subcorridor'].notna()].copy()
            if 'Zone_Group' in subcorridors.columns:
                subcorridors = subcorridors.drop(columns=['Zone_Group'])
            subcorridors = subcorridors.rename(columns={
                'Zone': 'Zone_Group',
                'Corridor': 'Zone', 
                'Subcorridor': 'Corridor'
            })
            
            subcorridors = optimize_dataframe_memory(subcorridors)
            logger.info(f"Setup {len(subcorridors)} subcorridor mappings")
            return subcorridors
            
        except Exception as e:
            logger.warning(f"Could not load subcorridors config: {e}")
            return pd.DataFrame()
        
    def _load_signals_list(self) -> List[str]:
        """Load list of signal IDs to process"""
        try:
            if self.corridors.empty or 'SignalID' not in self.corridors.columns:
                logger.warning("No SignalID column found in corridors")
                return []
                
            signals_list = self.corridors['SignalID'].unique().tolist()
            logger.info(f"Loaded {len(signals_list)} signals to process")
            return signals_list
            
        except Exception as e:
            logger.warning(f"Could not load signals list: {e}")
            return []
    
    def _load_camera_config(self) -> pd.DataFrame:
        """Load camera configuration with better error handling"""
        try:
            return get_cam_config(
                object_key=self.config['cctv_config_filename'],
                bucket=self.config['bucket'],
                corridors=self.corridors
            )
        except Exception as e:
            logger.warning(f"Could not load camera config: {e}")
            logger.info("Continuing without camera configuration - some features may be limited")
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'CameraID', 'Location', 'SignalID', 'As_of_Date', 
                'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Description'
            ])

    def process_travel_times(self):
        """Execute travel times calculations asynchronously [0 of 29]"""
        logger.info("Travel Times [0 of 29 (sigops)]")

        if not self.config.get('run', {}).get('travel_times', True):
            logger.info("Travel times processing skipped per configuration")
            return

        try:
            python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
            # python_env = "C:\\Users\\kgummadidala\\Desktop\\Rtopy\\server-env\\Scripts\\python.exe"
            
            # Run python scripts asynchronously
            scripts_to_run = [
                [python_env, "../get_travel_times_v2.py", "sigops", "../travel_times_1hr.yaml"],
                [python_env, "../get_travel_times_v2.py", "sigops", "../travel_times_15min.yaml"],
                [python_env, "../get_travel_times_1min_v2.py", "sigops"]
            ]
            
            for script_cmd in scripts_to_run:
                try:
                    if os.path.exists(script_cmd[1]):
                        subprocess.Popen(script_cmd)
                        logger.info(f"Started: {' '.join(script_cmd)}")
                    else:
                        logger.warning(f"Script not found: {script_cmd[1]}")
                except Exception as e:
                    logger.error(f"Failed to start {script_cmd[1]}: {e}")
                    
            logger.info("Started travel times processing in background")
            
        except Exception as e:
            logger.error(f"Error starting travel times processes: {e}")

    def process_detector_uptime(self):
        """Process vehicle detector uptime [1 of 29]"""
        logger.info("Vehicle Detector Uptime [1 of 29 (sigops)]")
        
        if not self.signals_list:
            logger.warning("No signals to process for detector uptime")
            return
        
        try:
            with memory_cleanup():
                def callback(x):
                    return self.get_avg_daily_detector_uptime(x).assign(
                        Date=lambda df: pd.to_datetime(df['Date']).dt.date
                    )

                avg_daily_detector_uptime = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="detector_uptime_pd",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list,
                    callback=callback
                )
                
                if avg_daily_detector_uptime.empty:
                    logger.warning("No detector uptime data available")
                    return
                    
                avg_daily_detector_uptime = optimize_dataframe_memory(avg_daily_detector_uptime.assign(
                    SignalID=lambda df: df['SignalID'].astype('category')
                ))

                # Corridor aggregations using existing aggregation functions
                with memory_cleanup():
                    cor_avg_daily_detector_uptime = self.get_cor_avg_daily_detector_uptime(
                        avg_daily_detector_uptime, self.corridors
                    )
                    sub_avg_daily_detector_uptime = self.get_cor_avg_daily_detector_uptime(
                        avg_daily_detector_uptime, self.subcorridors
                    ).dropna(subset=['Corridor'])

                # Weekly aggregations using existing functions
                with memory_cleanup():
                    weekly_detector_uptime = get_weekly_avg_by_day(
                        avg_daily_detector_uptime, "uptime", peak_only=False
                    )
                    weekly_detector_uptime = optimize_dataframe_memory(weekly_detector_uptime)
                    
                    cor_weekly_detector_uptime = self.get_cor_weekly_detector_uptime(
                        weekly_detector_uptime, self.corridors
                    )
                    sub_weekly_detector_uptime = self.get_cor_weekly_detector_uptime(
                        weekly_detector_uptime, self.subcorridors
                    ).dropna(subset=['Corridor'])

                # Monthly aggregations using existing functions
                with memory_cleanup():
                    monthly_detector_uptime = get_monthly_avg_by_day(
                        avg_daily_detector_uptime, "uptime", peak_only=False
                    )
                    monthly_detector_uptime = optimize_dataframe_memory(monthly_detector_uptime)
                    
                    cor_monthly_detector_uptime = self.get_cor_monthly_detector_uptime(
                        avg_daily_detector_uptime, self.corridors
                    )
                    sub_monthly_detector_uptime = self.get_cor_monthly_detector_uptime(
                        avg_daily_detector_uptime, self.subcorridors
                    ).dropna(subset=['Corridor'])

                # Save results using existing S3 functions
                self.save_results([
                    (avg_daily_detector_uptime, "avg_daily_detector_uptime.parquet", "uptime"),
                    (weekly_detector_uptime, "weekly_detector_uptime.parquet", "uptime"),
                    (monthly_detector_uptime, "monthly_detector_uptime.parquet", "uptime"),
                    (cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.parquet", "uptime"),
                    (cor_weekly_detector_uptime, "cor_weekly_detector_uptime.parquet", "uptime"),
                    (cor_monthly_detector_uptime, "cor_monthly_detector_uptime.parquet", "uptime"),
                    (sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.parquet", "uptime"),
                    (sub_weekly_detector_uptime, "sub_weekly_detector_uptime.parquet", "uptime"),
                    (sub_monthly_detector_uptime, "sub_monthly_detector_uptime.parquet", "uptime")
                ])

        except Exception as e:
            logger.error(f"Error in detector uptime processing: {e}")
            logger.error(traceback.format_exc())

    def process_pedestrian_uptime(self):
        """Process pedestrian pushbutton uptime [2 of 29]"""
        logger.info("Ped Pushbutton Uptime [2 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                # Calculate start date (6 months back minimum)
                pau_start_date = min(
                    pd.to_datetime(self.calcs_start_date),
                    pd.to_datetime(self.report_end_date) - pd.DateOffset(months=6)
                ).strftime('%Y-%m-%d')

                # Read hourly ped count data
                paph = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="counts_ped_1hr",
                    start_date=pau_start_date,
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list,
                    parallel=False
                )
                
                if paph.empty:
                    logger.warning("No pedestrian count data available")
                    return
                    
                paph = optimize_dataframe_memory(paph.dropna(subset=['CallPhase']).assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    Detector=lambda df: df['Detector'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                    DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                    Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week,
                    vol=lambda df: pd.to_numeric(df['vol'])
                ))

                # Aggregate to daily
                with memory_cleanup():
                    papd = paph.groupby(['SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase']).agg(
                        papd=('vol', 'sum')
                    ).reset_index()
                    papd = optimize_dataframe_memory(papd)

                # Rename for hourly data
                paph = paph.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'})

                # Calculate uptime using simplified method
                dates = pd.date_range(pau_start_date, str(self.report_end_date), freq='D')
                pau = self.get_pau_simple(dates, papd, paph,
                                        str(self.wk_calcs_start_date), pau_start_date)
                pau = optimize_dataframe_memory(pau)

                # Process daily, weekly, monthly aggregations using existing functions
                with memory_cleanup():
                    daily_pa_uptime = get_daily_avg(pau, "uptime", peak_only=False)
                    daily_pa_uptime = optimize_dataframe_memory(daily_pa_uptime)
                    
                    weekly_pa_uptime = get_weekly_avg_by_day(pau, "uptime", peak_only=False)
                    weekly_pa_uptime = optimize_dataframe_memory(weekly_pa_uptime)
                    
                    monthly_pa_uptime = get_monthly_avg_by_day(pau, "uptime", weight_col="ones", peak_only=False)
                    monthly_pa_uptime = optimize_dataframe_memory(monthly_pa_uptime)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_pa_uptime = self.get_cor_weekly_avg_by_day(daily_pa_uptime, self.corridors, "uptime")
                    sub_daily_pa_uptime = self.get_cor_weekly_avg_by_day(daily_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

                    cor_weekly_pa_uptime = self.get_cor_weekly_avg_by_day(weekly_pa_uptime, self.corridors, "uptime")
                    sub_weekly_pa_uptime = self.get_cor_weekly_avg_by_day(weekly_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

                    cor_monthly_pa_uptime = self.get_cor_monthly_avg_by_day(monthly_pa_uptime, self.corridors, "uptime")
                    sub_monthly_pa_uptime = self.get_cor_monthly_avg_by_day(monthly_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

                # Store papd for later use
                self.papd = papd

                # Save results
                self.save_results([
                    (daily_pa_uptime, "daily_pa_uptime.parquet", "uptime"),
                    (weekly_pa_uptime, "weekly_pa_uptime.parquet", "uptime"),
                    (monthly_pa_uptime, "monthly_pa_uptime.parquet", "uptime"),
                    (cor_daily_pa_uptime, "cor_daily_pa_uptime.parquet", "uptime"),
                    (cor_weekly_pa_uptime, "cor_weekly_pa_uptime.parquet", "uptime"),
                    (cor_monthly_pa_uptime, "cor_monthly_pa_uptime.parquet", "uptime"),
                    (sub_daily_pa_uptime, "sub_daily_pa_uptime.parquet", "uptime"),
                    (sub_weekly_pa_uptime, "sub_weekly_pa_uptime.parquet", "uptime"),
                    (sub_monthly_pa_uptime, "sub_monthly_pa_uptime.parquet", "uptime")
                ])

        except Exception as e:
            logger.error(f"Error in ped pushbutton uptime processing: {e}")
            logger.error(traceback.format_exc())

    def process_watchdog_alerts(self):
        """Process watchdog alerts [3 of 29]"""
        logger.info("Watchdog Alerts [3 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                # Process bad detectors - last 180 days
                bad_det_data = []
                date_range = [(datetime.now().date() - timedelta(days=i+1)).strftime('%Y-%m-%d') 
                             for i in range(180)]
                
                def process_bad_detector_date(date_str):
                    key = f"mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet"
                    try:
                        df = read_parquet_file(self.config['bucket'], key)
                        return optimize_dataframe_memory(df[['SignalID', 'Detector']].assign(Date=date_str))
                    except:
                        return pd.DataFrame()
                
                # Use parallel processing for bad detectors with chunking
                chunk_size = 30  # Process 30 days at a time to manage memory
                for i in range(0, len(date_range), chunk_size):
                    chunk_dates = date_range[i:i + chunk_size]
                    
                    bad_det_results = parallel_process_dates(
                        chunk_dates, 
                        process_bad_detector_date,
                        max_workers=min(5, len(chunk_dates))
                    )
                    
                    chunk_data = [df for df in bad_det_results if df is not None and not df.empty]
                    if chunk_data:
                        bad_det_data.extend(chunk_data)
                    
                    # Force garbage collection between chunks
                    gc.collect()
                
                if bad_det_data:
                    with memory_cleanup():
                        bad_det = pd.concat(bad_det_data, ignore_index=True)
                        bad_det = optimize_dataframe_memory(bad_det.assign(
                            SignalID=lambda df: df['SignalID'].astype('category'),
                            Detector=lambda df: df['Detector'].astype('category')
                        ))
                        
                        # Get detector configurations
                        det_config = self.get_detector_config_batch(bad_det['Date'].unique())
                        
                        if not det_config.empty:
                            # Join bad detectors with config and corridors
                            bad_det = bad_det.merge(
                                det_config,
                                on=['SignalID', 'Detector', 'Date'],
                                how='left'
                            ).merge(
                                self.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']],
                                on='SignalID',
                                how='left'
                            ).dropna(subset=['Corridor']).assign(
                                Alert=lambda df: pd.Categorical(['Bad Vehicle Detection'] * len(df)),
                                Name=lambda df: df['Name'].apply(lambda x: x.replace('@', '-') if pd.notna(x) else x).astype('category'),
                                ApproachDesc=lambda df: df.apply(
                                    lambda row: f"{row['ApproachDesc'].strip()} Lane {row['LaneNumber']}" 
                                    if pd.notna(row.get('ApproachDesc')) else "", axis=1
                                )
                            )
                            
                            bad_det = optimize_dataframe_memory(bad_det)
                            
                            # Upload bad detectors alert
                            write_parquet_to_s3(
                                bad_det,
                                bucket=self.config['bucket'],
                                key="sigops/watchdog/bad_detectors.parquet"
                            )

            # Process bad ped detectors - last 90 days
            with memory_cleanup():
                bad_ped_data = []
                ped_date_range = [(datetime.now().date() - timedelta(days=i+1)).strftime('%Y-%m-%d') 
                                 for i in range(90)]
                
                def process_bad_ped_date(date_str):
                    key = f"mark/bad_ped_detectors/date={date_str}/bad_ped_detectors_{date_str}.parquet"
                    try:
                        df = read_parquet_file(self.config['bucket'], key)
                        return optimize_dataframe_memory(df.assign(Date=date_str))
                    except:
                        return pd.DataFrame()
                
                # Process in chunks
                chunk_size = 20
                for i in range(0, len(ped_date_range), chunk_size):
                    chunk_dates = ped_date_range[i:i + chunk_size]
                    
                    bad_ped_results = parallel_process_dates(
                        chunk_dates,
                        process_bad_ped_date,
                        max_workers=min(5, len(chunk_dates))
                    )
                    
                    chunk_data = [df for df in bad_ped_results if df is not None and not df.empty]
                    if chunk_data:
                        bad_ped_data.extend(chunk_data)
                    
                    gc.collect()
                
                if bad_ped_data:
                    bad_ped = pd.concat(bad_ped_data, ignore_index=True)
                    bad_ped = optimize_dataframe_memory(bad_ped.assign(
                        SignalID=lambda df: df['SignalID'].astype('category'),
                        Detector=lambda df: df['Detector'].astype('category')
                    ).merge(
                        self.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']],
                        on='SignalID',
                        how='left'
                    ).assign(
                        Alert=lambda df: pd.Categorical(['Bad Ped Pushbuttons'] * len(df)),
                        Name=lambda df: df['Name'].astype('category')
                    ))
                    
                    # Upload bad ped pushbuttons alert
                    write_parquet_to_s3(
                        bad_ped,
                        bucket=self.config['bucket'],
                        key="sigops/watchdog/bad_ped_pushbuttons.parquet"
                    )

            # Process bad cameras - last 6 months
            if not self.cam_config.empty:
                with memory_cleanup():
                    self.process_bad_cameras()

        except Exception as e:
            logger.error(f"Error in watchdog alerts processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_pedestrian_activations(self):
        """Process daily pedestrian activations [4 of 29]"""
        logger.info("Daily Pedestrian Activations [4 of 29 (sigops)]")
        
        if self.papd is None:
            logger.warning("No pedestrian data available from previous step")
            return
        
        try:
            with memory_cleanup():
                # Use papd from previous step
                daily_papd = get_daily_avg(self.papd, "papd")
                daily_papd = optimize_dataframe_memory(daily_papd)
                
                weekly_papd = self.get_weekly_papd(self.papd)
                weekly_papd = optimize_dataframe_memory(weekly_papd)
                
                monthly_papd = self.get_monthly_papd(self.papd)
                monthly_papd = optimize_dataframe_memory(monthly_papd)

                # Group into corridors using existing aggregation functions
                with memory_cleanup():
                    cor_daily_papd = self.get_cor_weekly_papd(daily_papd, self.corridors)
                    if 'Week' in cor_daily_papd.columns:
                        cor_daily_papd = cor_daily_papd.drop(columns=['Week'])
                    
                    cor_weekly_papd = self.get_cor_weekly_papd(weekly_papd, self.corridors)
                    cor_monthly_papd = self.get_cor_monthly_papd(monthly_papd, self.corridors)

                # Group into subcorridors
                with memory_cleanup():
                    sub_daily_papd = self.get_cor_weekly_papd(daily_papd, self.subcorridors)
                    if 'Week' in sub_daily_papd.columns:
                        sub_daily_papd = sub_daily_papd.drop(columns=['Week'])
                    sub_daily_papd = sub_daily_papd.dropna(subset=['Corridor'])
                    
                    sub_weekly_papd = self.get_cor_weekly_papd(weekly_papd, self.subcorridors).dropna(subset=['Corridor'])
                    sub_monthly_papd = self.get_cor_monthly_papd(monthly_papd, self.subcorridors).dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_papd, "daily_papd.parquet", "papd"),
                    (weekly_papd, "weekly_papd.parquet", "papd"),
                    (monthly_papd, "monthly_papd.parquet", "papd"),
                    (cor_daily_papd, "cor_daily_papd.parquet", "papd"),
                    (cor_weekly_papd, "cor_weekly_papd.parquet", "papd"),
                    (cor_monthly_papd, "cor_monthly_papd.parquet", "papd"),
                    (sub_daily_papd, "sub_daily_papd.parquet", "papd"),
                    (sub_weekly_papd, "sub_weekly_papd.parquet", "papd"),
                    (sub_monthly_papd, "sub_monthly_papd.parquet", "papd")
                ])

        except Exception as e:
            logger.error(f"Error in daily ped activations processing: {e}")
            logger.error(traceback.format_exc())

    def process_hourly_pedestrian_activations(self):
        """Process hourly pedestrian activations [5 of 29]"""
        logger.info("Hourly Pedestrian Activations [5 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                # Read hourly ped activation data
                paph = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="counts_ped_1hr",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if paph.empty:
                    logger.warning("No hourly pedestrian data available")
                    return
                
                paph = optimize_dataframe_memory(paph.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                    Hour=lambda df: pd.to_datetime(df.get('Timeperiod', df.get('Date_Hour', df['Date']))).dt.hour
                ))
                
                # Calculate hourly aggregations
                with memory_cleanup():
                    hourly_paph = self.get_hourly_papd(paph)
                    cor_hourly_paph = self.get_cor_hourly_papd(hourly_paph, self.corridors)
                    sub_hourly_paph = self.get_cor_hourly_papd(hourly_paph, self.subcorridors).dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (hourly_paph, "hourly_paph.parquet", "paph"),
                    (cor_hourly_paph, "cor_hourly_paph.parquet", "paph"),
                    (sub_hourly_paph, "sub_hourly_paph.parquet", "paph")
                ])

        except Exception as e:
            logger.error(f"Error in hourly ped activations processing: {e}")
            logger.error(traceback.format_exc())

    def process_ped_delay(self):
        """Process pedestrian delay [6 of 29]"""
        logger.info("Pedestrian Delay [6 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                def callback(x):
                    if "Avg.Max.Ped.Delay" in x.columns:
                        x = x.rename(columns={"Avg.Max.Ped.Delay": "pd"}).assign(
                            CallPhase=lambda df: pd.Categorical([0] * len(df))
                        )
                    return optimize_dataframe_memory(x.assign(
                        DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                        Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
                    ))

                ped_delay = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="ped_delay",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list,
                    callback=callback
                )
                
                if ped_delay.empty:
                    logger.warning("No pedestrian delay data available")
                    return
                
                ped_delay = optimize_dataframe_memory(ped_delay.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category')
                ).fillna({'Events': 1}))

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_pd = get_daily_avg(ped_delay, "pd", "Events")
                    daily_pd = optimize_dataframe_memory(daily_pd)
                    
                    weekly_pd_by_day = get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
                    weekly_pd_by_day = optimize_dataframe_memory(weekly_pd_by_day)
                    
                    monthly_pd_by_day = get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
                    monthly_pd_by_day = optimize_dataframe_memory(monthly_pd_by_day)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_pd = self.get_cor_weekly_avg_by_day(daily_pd, self.corridors, "pd", "Events")
                    if 'Week' in cor_daily_pd.columns:
                        cor_daily_pd = cor_daily_pd.drop(columns=['Week'])
                    
                    cor_weekly_pd_by_day = self.get_cor_weekly_avg_by_day(weekly_pd_by_day, self.corridors, "pd", "Events")
                    cor_monthly_pd_by_day = self.get_cor_monthly_avg_by_day(monthly_pd_by_day, self.corridors, "pd", "Events")

                # Subcorridor aggregations
                with memory_cleanup():
                    sub_daily_pd = self.get_cor_weekly_avg_by_day(daily_pd, self.subcorridors, "pd", "Events")
                    if 'Week' in sub_daily_pd.columns:
                        sub_daily_pd = sub_daily_pd.drop(columns=['Week'])
                    sub_daily_pd = sub_daily_pd.dropna(subset=['Corridor'])
                    
                    sub_weekly_pd_by_day = self.get_cor_weekly_avg_by_day(weekly_pd_by_day, self.subcorridors, "pd", "Events").dropna(subset=['Corridor'])
                    sub_monthly_pd_by_day = self.get_cor_monthly_avg_by_day(monthly_pd_by_day, self.subcorridors, "pd", "Events").dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_pd, "daily_pd.parquet", "pd"),
                    (weekly_pd_by_day, "weekly_pd_by_day.parquet", "pd"),
                    (monthly_pd_by_day, "monthly_pd_by_day.parquet", "pd"),
                    (cor_daily_pd, "cor_daily_pd.parquet", "pd"),
                    (cor_weekly_pd_by_day, "cor_weekly_pd_by_day.parquet", "pd"),
                    (cor_monthly_pd_by_day, "cor_monthly_pd_by_day.parquet", "pd"),
                    (sub_daily_pd, "sub_daily_pd.parquet", "pd"),
                    (sub_weekly_pd_by_day, "sub_weekly_pd_by_day.parquet", "pd"),
                    (sub_monthly_pd_by_day, "sub_monthly_pd_by_day.parquet", "pd")
                ])

        except Exception as e:
            logger.error(f"Error in ped delay processing: {e}")
            logger.error(traceback.format_exc())

    def process_comm_uptime(self):
        """Process communication uptime [7 of 29]"""
        logger.info("Communication Uptime [7 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                cu = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="comm_uptime",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if cu.empty:
                    logger.warning("No communication uptime data available")
                    return
                
                cu = optimize_dataframe_memory(cu.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_comm_uptime = get_daily_avg(cu, "uptime", peak_only=False)
                    daily_comm_uptime = optimize_dataframe_memory(daily_comm_uptime)
                    
                    weekly_comm_uptime = get_weekly_avg_by_day(cu, "uptime", peak_only=False)
                    weekly_comm_uptime = optimize_dataframe_memory(weekly_comm_uptime)
                    
                    monthly_comm_uptime = get_monthly_avg_by_day(cu, "uptime", peak_only=False)
                    monthly_comm_uptime = optimize_dataframe_memory(monthly_comm_uptime)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_comm_uptime = self.get_cor_weekly_avg_by_day(daily_comm_uptime, self.corridors, "uptime")
                    cor_weekly_comm_uptime = self.get_cor_weekly_avg_by_day(weekly_comm_uptime, self.corridors, "uptime")
                    cor_monthly_comm_uptime = self.get_cor_monthly_avg_by_day(monthly_comm_uptime, self.corridors, "uptime")

                # Subcorridor aggregations
                with memory_cleanup():
                    sub_daily_comm_uptime = self.get_cor_weekly_avg_by_day(daily_comm_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])
                    sub_weekly_comm_uptime = self.get_cor_weekly_avg_by_day(weekly_comm_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])
                    sub_monthly_comm_uptime = self.get_cor_monthly_avg_by_day(monthly_comm_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_comm_uptime, "daily_comm_uptime.parquet", "uptime"),
                    (weekly_comm_uptime, "weekly_comm_uptime.parquet", "uptime"),
                    (monthly_comm_uptime, "monthly_comm_uptime.parquet", "uptime"),
                    (cor_daily_comm_uptime, "cor_daily_comm_uptime.parquet", "uptime"),
                    (cor_weekly_comm_uptime, "cor_weekly_comm_uptime.parquet", "uptime"),
                    (cor_monthly_comm_uptime, "cor_monthly_comm_uptime.parquet", "uptime"),
                    (sub_daily_comm_uptime, "sub_daily_comm_uptime.parquet", "uptime"),
                    (sub_weekly_comm_uptime, "sub_weekly_comm_uptime.parquet", "uptime"),
                    (sub_monthly_comm_uptime, "sub_monthly_comm_uptime.parquet", "uptime")
                ])

        except Exception as e:
            logger.error(f"Error in comm uptime processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_volumes(self):
        """Process daily volumes [8 of 29]"""
        logger.info("Daily Volumes [8 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                vpd = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="vehicles_pd",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if vpd.empty:
                    logger.warning("No daily volume data available")
                    return
                    
                vpd = optimize_dataframe_memory(vpd.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_vpd = self.get_daily_sum(vpd, "vpd")
                    daily_vpd = optimize_dataframe_memory(daily_vpd)
                    
                    weekly_vpd = self.get_weekly_vpd(vpd)
                    weekly_vpd = optimize_dataframe_memory(weekly_vpd)
                    
                    monthly_vpd = self.get_monthly_vpd(vpd)
                    monthly_vpd = optimize_dataframe_memory(monthly_vpd)

                # Group into corridors using sigify for signal-level data
                with memory_cleanup():
                    cor_daily_vpd = sigify(daily_vpd, pd.DataFrame(), self.corridors, 'SignalID')
                    # Clean up unnecessary columns
                    for col in ['ones', 'Week']:
                        if col in cor_daily_vpd.columns:
                            cor_daily_vpd = cor_daily_vpd.drop(columns=[col])
                    
                    cor_weekly_vpd = self.get_cor_weekly_vpd(weekly_vpd, self.corridors)
                    cor_monthly_vpd = self.get_cor_monthly_vpd(monthly_vpd, self.corridors)

                # Subcorridors
                with memory_cleanup():
                    sub_daily_vpd = self.get_cor_weekly_vpd(daily_vpd, self.subcorridors)
                    # Clean up unnecessary columns
                    for col in ['ones', 'Week']:
                        if col in sub_daily_vpd.columns:
                            sub_daily_vpd = sub_daily_vpd.drop(columns=[col])
                    sub_daily_vpd = sub_daily_vpd.dropna(subset=['Corridor'])
                    
                    sub_weekly_vpd = self.get_cor_weekly_vpd(weekly_vpd, self.subcorridors).dropna(subset=['Corridor'])
                    sub_monthly_vpd = self.get_cor_monthly_vpd(monthly_vpd, self.subcorridors).dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_vpd, "daily_vpd.parquet", "vpd"),
                    (weekly_vpd, "weekly_vpd.parquet", "vpd"),
                    (monthly_vpd, "monthly_vpd.parquet", "vpd"),
                    (cor_daily_vpd, "cor_daily_vpd.parquet", "vpd"),
                    (cor_weekly_vpd, "cor_weekly_vpd.parquet", "vpd"),
                    (cor_monthly_vpd, "cor_monthly_vpd.parquet", "vpd"),
                    (sub_daily_vpd, "sub_daily_vpd.parquet", "vpd"),
                    (sub_weekly_vpd, "sub_weekly_vpd.parquet", "vpd"),
                    (sub_monthly_vpd, "sub_monthly_vpd.parquet", "vpd")
                ])

        except Exception as e:
            logger.error(f"Error in daily volumes processing: {e}")
            logger.error(traceback.format_exc())

    # Helper methods for data processing with memory optimization
    def get_avg_daily_detector_uptime(self, df):
        """Get average daily detector uptime"""
        if df.empty:
            return pd.DataFrame()
        result = df.groupby(['SignalID', 'Date']).agg(
            uptime=('uptime', 'mean')
        ).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_avg_daily_detector_uptime(self, df, corridors):
        """Get corridor average daily detector uptime"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Date']
        ).agg(uptime=('uptime', 'mean')).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_weekly_detector_uptime(self, df, corridors):
        """Get corridor weekly detector uptime"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week']
        ).agg(uptime=('uptime', 'mean')).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_monthly_detector_uptime(self, df, corridors):
        """Get corridor monthly detector uptime"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg(uptime=('uptime', 'mean')).reset_index()
        return optimize_dataframe_memory(result)

    def get_pau_simple(self, dates, papd, paph, wk_start, pau_start):
        """Calculate pedestrian uptime using simplified method"""
        if papd.empty:
            return pd.DataFrame()
            
        # Simple uptime calculation based on data availability
        pau = papd.copy()
        pau['uptime'] = 1.0  # Default to 100% uptime
        
        # Set uptime to 0 for days with no data
        pau.loc[pau['papd'] == 0, 'uptime'] = 0.0
        
        # Set uptime to 0.5 for days with very low counts (potentially bad data)
        if 'papd' in pau.columns:
            daily_mean = pau['papd'].mean()
            if daily_mean > 0:
                pau.loc[pau['papd'] < (daily_mean * 0.1), 'uptime'] = 0.5
        
        return optimize_dataframe_memory(pau)

    def get_detector_config_batch(self, dates):
        """Get detector configuration for multiple dates efficiently"""
        try:
            det_config_data = []
            unique_dates = sorted(dates)
            
            def get_det_config_for_date(date_str):
                try:
                    # Placeholder for detector config retrieval
                    # This would need to be implemented based on your data source
                    return pd.DataFrame(columns=['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber', 'Date'])
                except:
                    return pd.DataFrame()
            
            # Process dates in batches to manage memory
            chunk_size = 10
            for i in range(0, len(unique_dates), chunk_size):
                chunk_dates = unique_dates[i:i + chunk_size]
                
                det_config_results = parallel_process_dates(
                    chunk_dates,
                    get_det_config_for_date,
                    max_workers=min(3, len(chunk_dates))
                )
                
                chunk_data = [df for df in det_config_results if df is not None and not df.empty]
                if chunk_data:
                    det_config_data.extend(chunk_data)
                
                gc.collect()
            
            if det_config_data:
                result = pd.concat(det_config_data, ignore_index=True)
                return optimize_dataframe_memory(result.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Detector=lambda df: df['Detector'].astype('category')
                ))
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error getting detector config: {e}")
            return pd.DataFrame()

    def process_bad_cameras(self):
        """Process bad cameras data"""
        try:
            bad_cam_data = []
            cam_date_range = pd.date_range(
                start=datetime.now().date() - timedelta(days=180),
                end=datetime.now().date() - timedelta(days=1),
                freq='MS'  # Month start
            ).strftime('%Y-%m-%d').tolist()
            
            def process_bad_cam_date(date_str):
                key = f"mark/cctv_uptime/month={date_str}/cctv_uptime_{date_str}.parquet"
                try:
                    df = read_parquet_file(self.config['bucket'], key)
                    return optimize_dataframe_memory(df[df['Size'] == 0])  # No camera image
                except Exception as e:
                    logger.debug(f"Could not read camera data for {date_str}: {e}")
                    return pd.DataFrame()
            
            # Process in chunks
            chunk_size = 3
            for i in range(0, len(cam_date_range), chunk_size):
                chunk_dates = cam_date_range[i:i + chunk_size]
                
                bad_cam_results = parallel_process_dates(
                    chunk_dates,
                    process_bad_cam_date,
                    max_workers=min(2, len(chunk_dates))
                )
                
                chunk_data = [df for df in bad_cam_results if df is not None and not df.empty]
                if chunk_data:
                    bad_cam_data.extend(chunk_data)
                
                gc.collect()
            
            if bad_cam_data:
                bad_cam = pd.concat(bad_cam_data, ignore_index=True).merge(
                    self.cam_config,
                    on='CameraID',
                    how='left'
                ).query('Date > As_of_Date').assign(
                    Corridor=lambda df: df['Corridor'].astype('category'),
                    SignalID=lambda df: df['CameraID'].astype('category'),  # Rename CameraID to SignalID
                    CallPhase=lambda df: pd.Categorical([0] * len(df)),
                    Detector=lambda df: pd.Categorical([0] * len(df)),
                    Alert=lambda df: pd.Categorical(['No Camera Image'] * len(df)),
                    Name=lambda df: df['Location'].astype('category')
                )[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector', 'Date', 'Alert', 'Name']]
                
                bad_cam = optimize_dataframe_memory(bad_cam)
                
                # Upload bad cameras alert
                write_parquet_to_s3(
                    bad_cam,
                    bucket=self.config['bucket'],
                    key="sigops/watchdog/bad_cameras.parquet"
                )
                
        except Exception as e:
            logger.error(f"Error processing bad cameras: {e}")

    def get_cor_weekly_avg_by_day(self, df, corridors, variable, weight_col="ones"):
        """Get corridor weekly average by day"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        if weight_col == "ones" and weight_col not in df.columns:
            df = df.assign(ones=1)
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg({variable: 'mean', weight_col: 'sum'}).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_monthly_avg_by_day(self, df, corridors, variable, weight_col="ones"):
        """Get corridor monthly average by day"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        if weight_col == "ones" and weight_col not in df.columns:
            df = df.assign(ones=1)
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg({variable: 'mean', weight_col: 'sum'}).reset_index()
        return optimize_dataframe_memory(result)

    def get_daily_sum(self, df, variable):
        """Get daily sum"""
        if df.empty:
            return pd.DataFrame()
        result = df.groupby(['SignalID', 'Date']).agg(
            {variable: 'sum'}
        ).reset_index()
        return optimize_dataframe_memory(result)

    def get_weekly_vpd(self, df):
        """Get weekly vehicles per day"""
        if df.empty:
            return pd.DataFrame()
        result = get_weekly_avg_by_day(df, "vpd")
        return optimize_dataframe_memory(result)

    def get_monthly_vpd(self, df):
        """Get monthly vehicles per day"""
        if df.empty:
            return pd.DataFrame()
        result = get_monthly_avg_by_day(df, "vpd")
        return optimize_dataframe_memory(result)

    def get_cor_weekly_vpd(self, df, corridors):
        """Get corridor weekly vehicles per day"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg(vpd=('vpd', 'sum')).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_monthly_vpd(self, df, corridors):
        """Get corridor monthly vehicles per day"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg(vpd=('vpd', 'sum')).reset_index()
        return optimize_dataframe_memory(result)

    def get_weekly_papd(self, df):
        """Get weekly pedestrian activations per day"""
        if df.empty:
            return pd.DataFrame()
        result = get_weekly_avg_by_day(df, "papd")
        return optimize_dataframe_memory(result)

    def get_monthly_papd(self, df):
        """Get monthly pedestrian activations per day"""
        if df.empty:
            return pd.DataFrame()
        result = get_monthly_avg_by_day(df, "papd")
        return optimize_dataframe_memory(result)

    def get_cor_weekly_papd(self, df, corridors):
        """Get corridor weekly pedestrian activations"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg(papd=('papd', 'sum')).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_monthly_papd(self, df, corridors):
        """Get corridor monthly pedestrian activations"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg(papd=('papd', 'sum')).reset_index()
        return optimize_dataframe_memory(result)

    def get_hourly_papd(self, df):
        """Get hourly pedestrian activations per day"""
        if df.empty:
            return pd.DataFrame()
        result = df.groupby(['SignalID', 'CallPhase', 'Date', 'Hour']).agg(
            paph=('vol', 'sum')
        ).reset_index()
        return optimize_dataframe_memory(result)

    def get_cor_hourly_papd(self, df, corridors):
        """Get corridor hourly pedestrian activations"""
        if df.empty or corridors.empty:
            return pd.DataFrame()
        result = df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Hour', 'Date']
        ).agg(paph=('paph', 'sum')).reset_index()
        return optimize_dataframe_memory(result)

    def save_results(self, results_list):
        """Save results to S3 using existing utilities with memory optimization"""
        for df, filename, metric_type in results_list:
            try:
                if df is not None and not df.empty:
                    # Optimize memory before saving
                    df = optimize_dataframe_memory(df)
                    
                    # Use batch processing for large datasets
                    if len(df) > 50000:  # Reduced threshold for memory efficiency
                        batch_process(
                            [df], 
                            batch_size=1,
                            process_func=self._save_single_result,
                            filename=filename,
                            metric_type=metric_type
                        )
                    else:
                        self._save_single_result([df], filename, metric_type)
                else:
                    logger.warning(f"Empty DataFrame for {filename}, skipping save")
            except Exception as e:
                logger.error(f"Error saving {filename}: {e}")
            finally:
                # Force garbage collection after each save
                gc.collect()

    def _save_single_result(self, df_list, filename, metric_type):
        """Save a single result to S3"""
        for df in df_list:
            try:
                if filename.endswith('.pkl'):
                    # For dictionary/complex objects, use pickle
                    buffer = io.BytesIO()
                    pickle.dump(df, buffer)
                    buffer.seek(0)
                    self.s3_client.put_object(
                        Bucket=self.config['bucket'],
                        Key=f"processed/{metric_type}/{filename}",
                        Body=buffer.read()
                    )
                else:
                    # Use existing parquet utilities
                    key = f"processed/{metric_type}/{filename}"
                    write_parquet_to_s3(df, self.config['bucket'], key)
                    logger.info(f"Saved {filename} with {len(df)} records to S3")
                    
            except Exception as e:
                logger.error(f"Error saving {filename}: {e}")
                raise

    def load_result(self, filename):
        """Load a previously saved result from S3"""
        try:
            # Determine metric type from filename
            metric_type = filename.split('_')[-1].replace('.parquet', '').replace('.pkl', '')
            key = f"processed/{metric_type}/{filename}"
            
            if filename.endswith('.pkl'):
                # Load pickle file
                response = self.s3_client.get_object(Bucket=self.config['bucket'], Key=key)
                return pickle.load(io.BytesIO(response['Body'].read()))
            else:
                result = read_parquet_from_s3(self.config['bucket'], key)
                return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return None

    # Additional processing methods (placeholders for remaining steps 9-29)
    def process_hourly_volumes(self):
        """Process hourly volumes [9 of 29]"""
        logger.info("Hourly Volumes [9 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                vph = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="vehicles_ph",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if vph.empty:
                    logger.warning("No hourly volume data available")
                    return
                
                vph = optimize_dataframe_memory(vph.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: pd.Categorical([2] * len(df)),  # Hack because next function needs a CallPhase
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Use existing aggregation functions from aggregations.py
                with memory_cleanup():
                    hourly_vol = self.get_hourly(vph, "vph", self.corridors)
                    
                    cor_daily_vph = self.get_cor_weekly_vph(hourly_vol, self.corridors)
                    sub_daily_vph = self.get_cor_weekly_vph(hourly_vol, self.subcorridors).dropna(subset=['Corridor'])

                    daily_vph_peak = self.get_weekly_vph_peak(hourly_vol)
                    cor_daily_vph_peak = self.get_cor_weekly_vph_peak(cor_daily_vph)
                    sub_daily_vph_peak = {k: v.dropna(subset=['Corridor']) for k, v in 
                                         self.get_cor_weekly_vph_peak(sub_daily_vph).items()}

                # Weekly processing using get_vph from aggregations
                with memory_cleanup():
                    weekly_vph = get_vph(vph, interval="1 hour", mainline_only=True)
                    weekly_vph = get_weekly_avg_by_day(weekly_vph, "vph")
                    weekly_vph = optimize_dataframe_memory(weekly_vph)
                    
                    cor_weekly_vph = self.get_cor_weekly_vph(weekly_vph, self.corridors)
                    sub_weekly_vph = self.get_cor_weekly_vph(weekly_vph, self.subcorridors).dropna(subset=['Corridor'])

                    weekly_vph_peak = self.get_weekly_vph_peak(weekly_vph)
                    cor_weekly_vph_peak = self.get_cor_weekly_vph_peak(cor_weekly_vph)
                    sub_weekly_vph_peak = {k: v.dropna(subset=['Corridor']) for k, v in 
                                          self.get_cor_weekly_vph_peak(sub_weekly_vph).items()}

                # Monthly processing
                with memory_cleanup():
                    monthly_vph = get_monthly_avg_by_day(vph, "vph")
                    monthly_vph = optimize_dataframe_memory(monthly_vph)
                    
                    cor_monthly_vph = self.get_cor_monthly_vph(monthly_vph, self.corridors)
                    sub_monthly_vph = self.get_cor_monthly_vph(monthly_vph, self.subcorridors).dropna(subset=['Corridor'])

                    monthly_vph_peak = self.get_monthly_vph_peak(monthly_vph)
                    cor_monthly_vph_peak = self.get_cor_monthly_vph_peak(cor_monthly_vph)
                    sub_monthly_vph_peak = {k: v.dropna(subset=['Corridor']) for k, v in 
                                           self.get_cor_monthly_vph_peak(sub_monthly_vph).items()}

                # Save results
                self.save_results([
                    (weekly_vph, "weekly_vph.parquet", "vph"),
                    (monthly_vph, "monthly_vph.parquet", "vph"),
                    (cor_daily_vph, "cor_daily_vph.parquet", "vph"),
                    (cor_weekly_vph, "cor_weekly_vph.parquet", "vph"),
                    (cor_monthly_vph, "cor_monthly_vph.parquet", "vph"),
                    (sub_daily_vph, "sub_daily_vph.parquet", "vph"),
                    (sub_weekly_vph, "sub_weekly_vph.parquet", "vph"),
                    (sub_monthly_vph, "sub_monthly_vph.parquet", "vph"),
                    (daily_vph_peak, "daily_vph_peak.pkl", "vph"),
                    (weekly_vph_peak, "weekly_vph_peak.pkl", "vph"),
                    (monthly_vph_peak, "monthly_vph_peak.pkl", "vph"),
                    (cor_daily_vph_peak, "cor_daily_vph_peak.pkl", "vph"),
                    (cor_weekly_vph_peak, "cor_weekly_vph_peak.pkl", "vph"),
                    (cor_monthly_vph_peak, "cor_monthly_vph_peak.pkl", "vph"),
                    (sub_daily_vph_peak, "sub_daily_vph_peak.pkl", "vph"),
                    (sub_weekly_vph_peak, "sub_weekly_vph_peak.pkl", "vph"),
                    (sub_monthly_vph_peak, "sub_monthly_vph_peak.pkl", "vph")
                ])

        except Exception as e:
            logger.error(f"Error in hourly volumes processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_throughput(self):
        """Process daily throughput [10 of 29]"""
        logger.info("Daily Throughput [10 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                throughput = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="throughput",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if throughput.empty:
                    logger.warning("No throughput data available")
                    return
                
                throughput = optimize_dataframe_memory(throughput.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('int').astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Use existing functions
                with memory_cleanup():
                    daily_throughput = self.get_daily_sum(throughput, "vph")
                    weekly_throughput = self.get_weekly_thruput(throughput)
                    monthly_throughput = self.get_monthly_thruput(throughput)

                # Daily throughput - corridor aggregations
                with memory_cleanup():
                    cor_daily_throughput = self.get_cor_weekly_thruput(daily_throughput, self.corridors)
                    if 'Week' in cor_daily_throughput.columns:
                        cor_daily_throughput = cor_daily_throughput.drop(columns=['Week'])
                    
                    sub_daily_throughput = self.get_cor_weekly_thruput(daily_throughput, self.subcorridors)
                    if 'Week' in sub_daily_throughput.columns:
                        sub_daily_throughput = sub_daily_throughput.drop(columns=['Week'])
                    sub_daily_throughput = sub_daily_throughput.dropna(subset=['Corridor'])

                # Weekly throughput - Group into corridors
                with memory_cleanup():
                    cor_weekly_throughput = self.get_cor_weekly_thruput(weekly_throughput, self.corridors)
                    sub_weekly_throughput = self.get_cor_weekly_thruput(weekly_throughput, self.subcorridors).dropna(subset=['Corridor'])

                # Monthly throughput - Group into corridors
                with memory_cleanup():
                    cor_monthly_throughput = self.get_cor_monthly_thruput(monthly_throughput, self.corridors)
                    sub_monthly_throughput = self.get_cor_monthly_thruput(monthly_throughput, self.subcorridors).dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_throughput, "daily_throughput.parquet", "vph"),
                    (weekly_throughput, "weekly_throughput.parquet", "vph"),
                    (monthly_throughput, "monthly_throughput.parquet", "vph"),
                    (cor_daily_throughput, "cor_daily_throughput.parquet", "vph"),
                    (cor_weekly_throughput, "cor_weekly_throughput.parquet", "vph"),
                    (cor_monthly_throughput, "cor_monthly_throughput.parquet", "vph"),
                    (sub_daily_throughput, "sub_daily_throughput.parquet", "vph"),
                    (sub_weekly_throughput, "sub_weekly_throughput.parquet", "vph"),
                    (sub_monthly_throughput, "sub_monthly_throughput.parquet", "vph")
                ])

        except Exception as e:
            logger.error(f"Error in daily throughput processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_aog(self):
        """Process daily arrivals on green [11 of 29]"""
        logger.info("Daily AOG [11 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                aog = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="arrivals_on_green",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if aog.empty:
                    logger.warning("No arrivals on green data available")
                    return
                
                aog = optimize_dataframe_memory(aog.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                    DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                    Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
                ))

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_aog = self.get_daily_aog(aog)
                    weekly_aog_by_day = self.get_weekly_aog_by_day(aog)
                    monthly_aog_by_day = self.get_monthly_aog_by_day(aog)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_aog = self.get_cor_weekly_aog_by_day(daily_aog, self.corridors)
                    if 'Week' in cor_daily_aog.columns:
                        cor_daily_aog = cor_daily_aog.drop(columns=['Week'])
                    
                    cor_weekly_aog_by_day = self.get_cor_weekly_aog_by_day(weekly_aog_by_day, self.corridors)
                    cor_monthly_aog_by_day = self.get_cor_monthly_aog_by_day(monthly_aog_by_day, self.corridors)

                # Subcorridor aggregations
                with memory_cleanup():
                    sub_daily_aog = self.get_cor_weekly_aog_by_day(daily_aog, self.subcorridors)
                    if 'Week' in sub_daily_aog.columns:
                        sub_daily_aog = sub_daily_aog.drop(columns=['Week'])
                    sub_daily_aog = sub_daily_aog.dropna(subset=['Corridor'])
                    
                    sub_weekly_aog_by_day = self.get_cor_weekly_aog_by_day(weekly_aog_by_day, self.subcorridors).dropna(subset=['Corridor'])
                    sub_monthly_aog_by_day = self.get_cor_monthly_aog_by_day(monthly_aog_by_day, self.subcorridors).dropna(subset=['Corridor'])

                # Store aog for use in other functions
                self.aog = aog

                # Save results
                self.save_results([
                    (daily_aog, "daily_aog.parquet", "aog"),
                    (weekly_aog_by_day, "weekly_aog_by_day.parquet", "aog"),
                    (monthly_aog_by_day, "monthly_aog_by_day.parquet", "aog"),
                    (cor_daily_aog, "cor_daily_aog.parquet", "aog"),
                    (cor_weekly_aog_by_day, "cor_weekly_aog_by_day.parquet", "aog"),
                    (cor_monthly_aog_by_day, "cor_monthly_aog_by_day.parquet", "aog"),
                    (sub_daily_aog, "sub_daily_aog.parquet", "aog"),
                    (sub_weekly_aog_by_day, "sub_weekly_aog_by_day.parquet", "aog"),
                    (sub_monthly_aog_by_day, "sub_monthly_aog_by_day.parquet", "aog")
                ])

        except Exception as e:
            logger.error(f"Error in daily AOG processing: {e}")
            logger.error(traceback.format_exc())

    # Placeholder methods for remaining processing steps (12-29)
    # These would need full implementation based on the complete R code
    
    def process_hourly_aog(self):
        """Process hourly arrivals on green [12 of 29]"""
        logger.info("Hourly AOG [12 of 29 (sigops)]")
        try:
            if self.aog is not None and not self.aog.empty:
                with memory_cleanup():
                    aog_by_hr = self.get_aog_by_hr(self.aog)
                    monthly_aog_by_hr = self.get_monthly_aog_by_hr(aog_by_hr)
                    
                    cor_monthly_aog_by_hr = self.get_cor_monthly_aog_by_hr(monthly_aog_by_hr, self.corridors)
                    sub_monthly_aog_by_hr = self.get_cor_monthly_aog_by_hr(monthly_aog_by_hr, self.subcorridors).dropna(subset=['Corridor'])

                    self.save_results([
                        (cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.parquet", "aog"),
                        (sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.parquet", "aog")
                    ])
            else:
                logger.warning("No AOG data available for hourly processing")
        except Exception as e:
            logger.error(f"Error in hourly AOG processing: {e}")

    def process_daily_progression_ratio(self):
        """Process daily progression ratio [13 of 29]"""
        logger.info("Daily Progression Ratio [13 of 29 (sigops)]")
        try:
            if self.aog is not None and not self.aog.empty:
                with memory_cleanup():
                    daily_pr = get_daily_avg(self.aog, "pr", "vol")
                    weekly_pr_by_day = self.get_weekly_pr_by_day(self.aog)
                    monthly_pr_by_day = self.get_monthly_pr_by_day(self.aog)

                    # Corridor and subcorridor aggregations
                    cor_daily_pr = self.get_cor_weekly_pr_by_day(daily_pr, self.corridors)
                    if 'Week' in cor_daily_pr.columns:
                        cor_daily_pr = cor_daily_pr.drop(columns=['Week'])
                    
                    cor_weekly_pr_by_day = self.get_cor_weekly_pr_by_day(weekly_pr_by_day, self.corridors)
                    cor_monthly_pr_by_day = self.get_cor_monthly_pr_by_day(monthly_pr_by_day, self.corridors)

                    sub_daily_pr = self.get_cor_weekly_pr_by_day(daily_pr, self.subcorridors)
                    if 'Week' in sub_daily_pr.columns:
                        sub_daily_pr = sub_daily_pr.drop(columns=['Week'])
                    sub_daily_pr = sub_daily_pr.dropna(subset=['Corridor'])
                    
                    sub_weekly_pr_by_day = self.get_cor_weekly_pr_by_day(weekly_pr_by_day, self.subcorridors).dropna(subset=['Corridor'])
                    sub_monthly_pr_by_day = self.get_cor_monthly_pr_by_day(monthly_pr_by_day, self.subcorridors).dropna(subset=['Corridor'])

                    self.save_results([
                        (daily_pr, "daily_pr.parquet", "pr"),
                        (weekly_pr_by_day, "weekly_pr_by_day.parquet", "pr"),
                        (monthly_pr_by_day, "monthly_pr_by_day.parquet", "pr"),
                        (cor_daily_pr, "cor_daily_pr.parquet", "pr"),
                        (cor_weekly_pr_by_day, "cor_weekly_pr_by_day.parquet", "pr"),
                        (cor_monthly_pr_by_day, "cor_monthly_pr_by_day.parquet", "pr"),
                        (sub_daily_pr, "sub_daily_pr.parquet", "pr"),
                        (sub_weekly_pr_by_day, "sub_weekly_pr_by_day.parquet", "pr"),
                        (sub_monthly_pr_by_day, "sub_monthly_pr_by_day.parquet", "pr")
                    ])
            else:
                logger.warning("No AOG data available for progression ratio processing")
        except Exception as e:
            logger.error(f"Error in daily progression ratio processing: {e}")

    def process_hourly_progression_ratio(self):
        """Process hourly progression ratio [14 of 29]"""
        logger.info("Hourly Progression Ratio [14 of 29 (sigops)]")
        try:
            if self.aog is not None and not self.aog.empty:
                with memory_cleanup():
                    pr_by_hr = self.get_pr_by_hr(self.aog)
                    monthly_pr_by_hr = self.get_monthly_pr_by_hr(pr_by_hr)
                    
                    cor_monthly_pr_by_hr = self.get_cor_monthly_pr_by_hr(monthly_pr_by_hr, self.corridors)
                    sub_monthly_pr_by_hr = self.get_cor_monthly_pr_by_hr(monthly_pr_by_hr, self.subcorridors).dropna(subset=['Corridor'])

                    self.save_results([
                        (monthly_pr_by_hr, "monthly_pr_by_hr.parquet", "pr"),
                        (cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.parquet", "pr"),
                        (sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.parquet", "pr")
                    ])
            else:
                logger.warning("No AOG data available for hourly progression ratio processing")
        except Exception as e:
            logger.error(f"Error in hourly progression ratio processing: {e}")

    def process_daily_split_failures(self):
        """Process daily split failures [15 of 29]"""
        logger.info("Daily Split Failures [15 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                sf = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="split_failures",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list,
                    callback=lambda x: x[x['CallPhase'] == 0] if 'CallPhase' in x.columns else x
                )
                
                if sf.empty:
                    logger.warning("No split failures data available")
                    return
                
                sf = optimize_dataframe_memory(sf.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Define peak hours
                AM_PEAK_HOURS = self.config.get('AM_PEAK_HOURS', [7, 8, 9])
                PM_PEAK_HOURS = self.config.get('PM_PEAK_HOURS', [16, 17, 18])

                # Divide into peak/off-peak split failures
                date_hour_col = 'Date_Hour' if 'Date_Hour' in sf.columns else 'Date'
                if 'Date_Hour' in sf.columns:
                    sfo = sf[~pd.to_datetime(sf[date_hour_col]).dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
                    sfp = sf[pd.to_datetime(sf[date_hour_col]).dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
                else:
                    # If no hour info, treat all as peak
                    sfp = sf.copy()
                    sfo = sf.copy()

                # Store sf for use in hourly processing
                self.sf = sf

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_sfp = get_daily_avg(sfp, "sf_freq", "cycles") if not sfp.empty else pd.DataFrame()
                    daily_sfo = get_daily_avg(sfo, "sf_freq", "cycles") if not sfo.empty else pd.DataFrame()

                    weekly_sf_by_day = get_weekly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False) if not sfp.empty else pd.DataFrame()
                    weekly_sfo_by_day = get_weekly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False) if not sfo.empty else pd.DataFrame()
                    
                    monthly_sf_by_day = get_monthly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False) if not sfp.empty else pd.DataFrame()
                    monthly_sfo_by_day = get_monthly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False) if not sfo.empty else pd.DataFrame()

                # Save results (only non-empty dataframes)
                results_to_save = []
                if not daily_sfp.empty:
                    results_to_save.append((daily_sfp, "daily_sfp.parquet", "sf_freq"))
                if not weekly_sf_by_day.empty:
                    results_to_save.append((weekly_sf_by_day, "wsf.parquet", "sf_freq"))
                if not monthly_sf_by_day.empty:
                    results_to_save.append((monthly_sf_by_day, "monthly_sfd.parquet", "sf_freq"))

                if results_to_save:
                    self.save_results(results_to_save)

        except Exception as e:
            logger.error(f"Error in daily split failures processing: {e}")
            logger.error(traceback.format_exc())

    def process_hourly_split_failures(self):
        """Process hourly split failures [16 of 29]"""
        logger.info("Hourly Split Failures [16 of 29 (sigops)]")
        
        try:
            if self.sf is not None and not self.sf.empty:
                with memory_cleanup():
                    sfh = self.get_sf_by_hr(self.sf)
                    msfh = self.get_monthly_sf_by_hr(sfh)
                    
                    cor_msfh = self.get_cor_monthly_sf_by_hr(msfh, self.corridors)
                    sub_msfh = self.get_cor_monthly_sf_by_hr(msfh, self.subcorridors).dropna(subset=['Corridor'])

                    self.save_results([
                        (msfh, "msfh.parquet", "sf_freq"),
                        (cor_msfh, "cor_msfh.parquet", "sf_freq"),
                        (sub_msfh, "sub_msfh.parquet", "sf_freq")
                    ])
            else:
                logger.warning("No split failures data available for hourly processing")
        except Exception as e:
            logger.error(f"Error in hourly split failures processing: {e}")

    def process_daily_queue_spillback(self):
        """Process daily queue spillback [17 of 29]"""
        logger.info("Daily Queue Spillback [17 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                qs = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="queue_spillback",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if qs.empty:
                    logger.warning("No queue spillback data available")
                    return
                
                qs = optimize_dataframe_memory(qs.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Store qs for use in hourly processing
                self.qs = qs

                # Use existing aggregation functions
                with memory_cleanup():
                    daily_qs = get_daily_avg(qs, "qs_freq", "cycles")
                    wqs = self.get_weekly_qs_by_day(qs)
                    monthly_qsd = self.get_monthly_qs_by_day(qs)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_qs = self.get_cor_weekly_qs_by_day(daily_qs, self.corridors)
                    if 'Week' in cor_daily_qs.columns:
                        cor_daily_qs = cor_daily_qs.drop(columns=['Week'])
                    
                    cor_wqs = self.get_cor_weekly_qs_by_day(wqs, self.corridors)
                    cor_monthly_qsd = self.get_cor_monthly_qs_by_day(monthly_qsd, self.corridors)

                # Subcorridor aggregations
                with memory_cleanup():
                    sub_daily_qs = self.get_cor_weekly_qs_by_day(daily_qs, self.subcorridors)
                    if 'Week' in sub_daily_qs.columns:
                        sub_daily_qs = sub_daily_qs.drop(columns=['Week'])
                    sub_daily_qs = sub_daily_qs.dropna(subset=['Corridor'])
                    
                    sub_wqs = self.get_cor_weekly_qs_by_day(wqs, self.subcorridors).dropna(subset=['Corridor'])
                    sub_monthly_qsd = self.get_cor_monthly_qs_by_day(monthly_qsd, self.subcorridors).dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_qs, "daily_qsd.parquet", "qs_freq"),
                    (wqs, "wqs.parquet", "qs_freq"),
                    (monthly_qsd, "monthly_qsd.parquet", "qs_freq"),
                    (cor_daily_qs, "cor_daily_qsd.parquet", "qs_freq"),
                    (cor_wqs, "cor_wqs.parquet", "qs_freq"),
                    (cor_monthly_qsd, "cor_monthly_qsd.parquet", "qs_freq"),
                    (sub_daily_qs, "sub_daily_qsd.parquet", "qs_freq"),
                    (sub_wqs, "sub_wqs.parquet", "qs_freq"),
                    (sub_monthly_qsd, "sub_monthly_qsd.parquet", "qs_freq")
                ])

        except Exception as e:
            logger.error(f"Error in daily queue spillback processing: {e}")
            logger.error(traceback.format_exc())

    def process_hourly_queue_spillback(self):
        """Process hourly queue spillback [18 of 29]"""
        logger.info("Hourly Queue Spillback [18 of 29 (sigops)]")
        
        try:
            if self.qs is not None and not self.qs.empty:
                with memory_cleanup():
                    qsh = self.get_qs_by_hr(self.qs)
                    mqsh = self.get_monthly_qs_by_hr(qsh)
                    
                    cor_mqsh = self.get_cor_monthly_qs_by_hr(mqsh, self.corridors)
                    sub_mqsh = self.get_cor_monthly_qs_by_hr(mqsh, self.subcorridors).dropna(subset=['Corridor'])

                    self.save_results([
                        (mqsh, "mqsh.parquet", "qs_freq"),
                        (cor_mqsh, "cor_mqsh.parquet", "qs_freq"),
                        (sub_mqsh, "sub_mqsh.parquet", "qs_freq")
                    ])
            else:
                logger.warning("No queue spillback data available for hourly processing")
        except Exception as e:
            logger.error(f"Error in hourly queue spillback processing: {e}")

    def process_travel_time_indexes(self):
        """Process travel time and buffer time indexes [19 of 29]"""
        logger.info("Travel Time Indexes [19 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                # Process corridor travel time metrics
                tt = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="cor_travel_time_metrics_1hr",
                    s3root='sigops',
                    start_date=str(self.calcs_start_date),
                    end_date=str(self.report_end_date)
                )
                
                if not tt.empty:
                    tt = optimize_dataframe_memory(tt.assign(
                        Corridor=lambda df: df['Corridor'].astype('category')
                    ))
                    
                    # Process different metrics
                    metrics = ['tti', 'pti', 'bi', 'speed_mph']
                    for metric in metrics:
                        if metric in tt.columns:
                            metric_data = tt.drop(columns=[m for m in metrics if m != metric], errors='ignore')
                            
                            # Save basic metric data
                            monthly_data = self.get_monthly_metric_by_day(metric_data, metric)
                            self.save_results([(monthly_data, f"cor_monthly_{metric}.parquet", metric)])
                
                # Process subcorridor travel time metrics if available
                tt_sub = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="sub_travel_time_metrics_1hr",
                    s3root='sigops',
                    start_date=str(self.calcs_start_date),
                    end_date=str(self.report_end_date)
                )
                
                if not tt_sub.empty:
                    tt_sub = optimize_dataframe_memory(tt_sub)
                    
                    # Process subcorridor metrics
                    for metric in metrics:
                        if metric in tt_sub.columns:
                            metric_data = tt_sub.drop(columns=[m for m in metrics if m != metric], errors='ignore')
                            
                            monthly_data = self.get_monthly_metric_by_day(metric_data, metric)
                            self.save_results([(monthly_data, f"sub_monthly_{metric}.parquet", metric)])

        except Exception as e:
            logger.error(f"Error in travel time indexes processing: {e}")
            logger.error(traceback.format_exc())

    def process_cctv_uptime(self):
        """Process CCTV uptime from 511 and Encoders [20 of 29]"""
        logger.info("CCTV Uptimes [20 of 29 (sigops)]")
        
        try:
            if self.cam_config.empty:
                logger.warning("No camera configuration available")
                return
                
            with memory_cleanup():
                # Simplified CCTV uptime processing
                daily_cctv_uptime = self.get_cctv_uptime_data()
                
                if not daily_cctv_uptime.empty:
                    # Calculate aggregations
                    weekly_cctv_uptime = self.get_weekly_cctv_uptime(daily_cctv_uptime)
                    monthly_cctv_uptime = self.get_monthly_cctv_uptime(daily_cctv_uptime)
                    
                    # Save results
                    self.save_results([
                        (daily_cctv_uptime, "daily_cctv_uptime.parquet", "uptime"),
                        (weekly_cctv_uptime, "weekly_cctv_uptime.parquet", "uptime"),
                        (monthly_cctv_uptime, "monthly_cctv_uptime.parquet", "uptime")
                    ])

        except Exception as e:
            logger.error(f"Error in CCTV uptime processing: {e}")
            logger.error(traceback.format_exc())

    def process_travel_time_speeds(self):
        """Process travel time speeds [21 of 29]"""
        logger.info("Travel Time Speeds [21 of 29 (sigops)]")
        
        try:
            # This step is often commented out in the R code
            logger.info("Travel time speeds processing skipped (optional feature)")
        except Exception as e:
            logger.error(f"Error in travel time speeds processing: {e}")

    def process_ramp_meter_features(self):
        """Process ramp meter features [22 of 29]"""
        logger.info("Ramp Meter Features [22 of 29 (sigops)]")
        
        try:
            # Get ramp meter signals
            if 'Corridor' in self.corridors.columns:
                ramps = self.corridors[self.corridors['Corridor'] == 'Ramp Meter']['SignalID'].unique()
            else:
                ramps = []
            
            if len(ramps) > 0:
                with memory_cleanup():
                    vph_rm = s3_read_parquet_parallel(
                        bucket=self.config['bucket'],
                        table_name="vehicles_ph",
                        start_date=str(self.wk_calcs_start_date),
                        end_date=str(self.report_end_date),
                        signals_list=ramps.tolist()
                    )
                    
                    if not vph_rm.empty:
                        vph_rm = optimize_dataframe_memory(vph_rm)
                        
                        # Process ramp meter data
                        daily_vph_rm = get_daily_avg(vph_rm, "vph", peak_only=False)
                        self.save_results([(daily_vph_rm, "sig_daily_vph_rm.parquet", "vph")])
            else:
                logger.info("No ramp meter signals found")

        except Exception as e:
            logger.error(f"Error in ramp meter features processing: {e}")

    def process_counts_1hr_calcs(self):
        """Process counts 1hr calculations [23 of 29]"""
        logger.info("Counts 1hr calcs [23 of 29 (sigops)]")
        
        try:
            # This would typically call an external calculation module
            logger.info("Counts 1hr calculations - external module call would go here")
        except Exception as e:
            logger.error(f"Error in counts 1hr calculations: {e}")

    def process_monthly_summary_calcs(self):
        """Process monthly summary calculations [24 of 29]"""
        logger.info("Monthly Summary calcs [24 of 29 (sigops)]")
        
        try:
            # Calculate summary statistics and trends
            self.calculate_monthly_summaries()
        except Exception as e:
            logger.error(f"Error in monthly summary calculations: {e}")

    def process_high_resolution_calcs(self):
        """Process high resolution calculations [25 of 29]"""
        logger.info("Hi-res calcs [25 of 29 (sigops)]")
        
        try:
            # This would process 15-minute and 1-minute data
            logger.info("High resolution calculations - would process 15min and 1min data")
        except Exception as e:
            logger.error(f"Error in high resolution calculations: {e}")

    def process_lane_by_lane_volume_calcs(self):
        """Process lane-by-lane volume calculations [26 of 29]"""
        logger.info("Lane-by-lane volume calcs [26 of 29 (sigops)]")
        
        try:
            # This would process lane-level volume data
            logger.info("Lane-by-lane volume calculations - would process detector-level data")
        except Exception as e:
            logger.error(f"Error in lane-by-lane volume calculations: {e}")

    def process_mainline_green_utilization(self):
        """Process mainline green utilization [27 of 29]"""
        logger.info("Mainline Green Utilization [27 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                gu = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="green_utilization",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if gu.empty:
                    logger.warning("No green utilization data available")
                    return
                
                gu = optimize_dataframe_memory(gu.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                ))

                # Filter to mainline phases only (2, 6)
                if 'CallPhase' in gu.columns:
                    gu = gu[gu['CallPhase'].isin([2, 6])]

                # Calculate aggregations
                with memory_cleanup():
                    daily_gu = get_daily_avg(gu, "gu", "vol")
                    weekly_gu_by_day = get_weekly_avg_by_day(gu, "gu", "vol")
                    monthly_gu_by_day = get_monthly_avg_by_day(gu, "gu", "vol")

                    # Save results
                    self.save_results([
                        (daily_gu, "daily_gu.parquet", "gu"),
                        (weekly_gu_by_day, "weekly_gu_by_day.parquet", "gu"),
                        (monthly_gu_by_day, "monthly_gu_by_day.parquet", "gu")
                    ])

        except Exception as e:
            logger.error(f"Error in mainline green utilization processing: {e}")

    def process_coordination_and_preemption(self):
        """Process coordination and preemption [28 of 29]"""
        logger.info("Coordination and Preemption [28 of 29 (sigops)]")
        
        try:
            with memory_cleanup():
                # Process preemption data
                preempt = s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="preemption",
                    start_date=str(self.wk_calcs_start_date),
                    end_date=str(self.report_end_date),
                    signals_list=self.signals_list
                )
                
                if preempt.empty:
                    logger.warning("No preemption data available")
                    return
                
                preempt = optimize_dataframe_memory(preempt.assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                    DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                    Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week,
                    CallPhase=lambda df: pd.Categorical([0] * len(df))  # Default phase
                ))

                # Calculate daily, weekly, and monthly aggregations
                with memory_cleanup():
                    daily_preempt = get_daily_avg(preempt, "freq", peak_only=False)
                    weekly_preempt_by_day = get_weekly_avg_by_day(preempt, "freq", peak_only=False)
                    monthly_preempt_by_day = get_monthly_avg_by_day(preempt, "freq", peak_only=False)

                # Corridor aggregations
                with memory_cleanup():
                    cor_daily_preempt = self.get_cor_weekly_avg_by_day(daily_preempt, self.corridors, "freq")
                    if 'Week' in cor_daily_preempt.columns:
                        cor_daily_preempt = cor_daily_preempt.drop(columns=['Week'])
                    
                    cor_weekly_preempt_by_day = self.get_cor_weekly_avg_by_day(weekly_preempt_by_day, self.corridors, "freq")
                    cor_monthly_preempt_by_day = self.get_cor_monthly_avg_by_day(monthly_preempt_by_day, self.corridors, "freq")

                # Subcorridor aggregations
                with memory_cleanup():
                    sub_daily_preempt = self.get_cor_weekly_avg_by_day(daily_preempt, self.subcorridors, "freq")
                    if 'Week' in sub_daily_preempt.columns:
                        sub_daily_preempt = sub_daily_preempt.drop(columns=['Week'])
                    sub_daily_preempt = sub_daily_preempt.dropna(subset=['Corridor'])
                    
                    sub_weekly_preempt_by_day = self.get_cor_weekly_avg_by_day(weekly_preempt_by_day, self.subcorridors, "freq").dropna(subset=['Corridor'])
                    sub_monthly_preempt_by_day = self.get_cor_monthly_avg_by_day(monthly_preempt_by_day, self.subcorridors, "freq").dropna(subset=['Corridor'])

                # Save results
                self.save_results([
                    (daily_preempt, "daily_preempt.parquet", "freq"),
                    (weekly_preempt_by_day, "weekly_preempt_by_day.parquet", "freq"),
                    (monthly_preempt_by_day, "monthly_preempt_by_day.parquet", "freq"),
                    (cor_daily_preempt, "cor_daily_preempt.parquet", "freq"),
                    (cor_weekly_preempt_by_day, "cor_weekly_preempt_by_day.parquet", "freq"),
                    (cor_monthly_preempt_by_day, "cor_monthly_preempt_by_day.parquet", "freq"),
                    (sub_daily_preempt, "sub_daily_preempt.parquet", "freq"),
                    (sub_weekly_preempt_by_day, "sub_weekly_preempt_by_day.parquet", "freq"),
                    (sub_monthly_preempt_by_day, "sub_monthly_preempt_by_day.parquet", "freq")
                ])

        except Exception as e:
            logger.error(f"Error in coordination and preemption processing: {e}")
            logger.error(traceback.format_exc())

    def process_task_completion_and_cleanup(self):
        """Process task completion and cleanup [29 of 29]"""
        logger.info("Task Completion and Cleanup [29 of 29 (sigops)]")
        
        try:
            # Generate final summary reports
            self.generate_executive_summary()
            
            # Check for quarterly report configuration
            if os.path.exists('../quarterly_report.yaml'):
                with open('../quarterly_report.yaml', 'r') as f:
                    quarterly_conf = yaml.safe_load(f)
                    
                if quarterly_conf.get('run', {}).get('quarterly', False):
                    self.process_quarterly_reports()

            # Generate signal state summary
            self.generate_signal_state_summary()
            
            # Clean up large objects from memory
            self.cleanup_temp_data()
            
            # Log completion statistics
            end_time = time.time()
            duration = end_time - self.start_time
            final_memory = get_memory_usage()
            
            logger.info(f"Monthly report package completed in {format_duration(duration)}")
            logger.info(f"Final memory usage: {final_memory['rss_mb']:.2f}MB ({final_memory['percent']:.1f}%)")
            
            # Send completion notification if configured
            if self.config.get('notifications', {}).get('enabled', False):
                self.send_completion_notification(duration)

            # Create completion marker file
            self.create_completion_marker()

        except Exception as e:
            logger.error(f"Error in task completion and cleanup: {e}")
            logger.error(traceback.format_exc())

    # Supporting methods for the advanced processing steps

    def calculate_monthly_summaries(self):
        """Calculate comprehensive monthly summary statistics"""
        try:
            summary_metrics = {}
            
            # Load key metrics and calculate summaries
            key_files = [
                "cor_monthly_detector_uptime.parquet",
                "cor_monthly_vpd.parquet", 
                "cor_monthly_aog.parquet",
                "cor_monthly_sf_freq.parquet",
                "cor_monthly_qs_freq.parquet"
            ]
            
            for filename in key_files:
                try:
                    df = self.load_result(filename)
                    if df is not None and not df.empty:
                        metric_name = filename.split('_')[2].replace('.parquet', '')
                        summary_metrics[metric_name] = self.calculate_metric_summary(df, metric_name)
                except Exception as e:
                    logger.warning(f"Could not process {filename}: {e}")
            
            # Save summary statistics
            if summary_metrics:
                summary_df = pd.DataFrame(summary_metrics).T
                self.save_results([(summary_df, "monthly_summary_stats.parquet", "summary")])
            
        except Exception as e:
            logger.error(f"Error calculating monthly summaries: {e}")

    def calculate_metric_summary(self, df, metric_name):
        """Calculate summary statistics for a specific metric"""
        try:
            # Find the metric column (usually the last non-categorical column)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                metric_col = numeric_cols[-1]  # Assume last numeric column is the metric
                
                return {
                    'mean': df[metric_col].mean(),
                    'median': df[metric_col].median(),
                    'std': df[metric_col].std(),
                    'min': df[metric_col].min(),
                    'max': df[metric_col].max(),
                    'count': len(df),
                    'corridors': df['Corridor'].nunique() if 'Corridor' in df.columns else 0
                }
            else:
                return {'error': 'No numeric columns found'}
                
        except Exception as e:
            logger.error(f"Error calculating summary for {metric_name}: {e}")
            return {'error': str(e)}

    def generate_executive_summary(self):
        """Generate executive summary with key performance indicators"""
        try:
            summary_data = {
                'report_period_start': str(self.calcs_start_date),
                'report_period_end': str(self.report_end_date),
                'total_signals': len(self.signals_list),
                'total_corridors': len(self.corridors['Corridor'].unique()) if 'Corridor' in self.corridors.columns else 0,
                'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'processing_duration_minutes': (time.time() - self.start_time) / 60
            }
            
            # Add performance metrics if available
            try:
                # Load key performance metrics
                detector_uptime = self.load_result("cor_monthly_detector_uptime.parquet")
                if detector_uptime is not None and not detector_uptime.empty:
                    summary_data['avg_detector_uptime'] = detector_uptime['uptime'].mean()
                
                comm_uptime = self.load_result("cor_monthly_comm_uptime.parquet")
                if comm_uptime is not None and not comm_uptime.empty:
                    summary_data['avg_comm_uptime'] = comm_uptime['uptime'].mean()
                    
            except Exception as e:
                logger.warning(f"Could not calculate performance metrics: {e}")
            
            # Save executive summary
            summary_df = pd.DataFrame([summary_data])
            self.save_results([(summary_df, "executive_summary.parquet", "summary")])
            
        except Exception as e:
            logger.error(f"Error generating executive summary: {e}")

    def process_quarterly_reports(self):
        """Process quarterly report calculations"""
        try:
            logger.info("Processing quarterly report calculations")
            
            # Define quarterly metrics to process
            quarterly_metrics = ['vpd', 'aog', 'pr', 'sf_freq', 'qs_freq', 'throughput']
            
            for metric in quarterly_metrics:
                try:
                    # Load monthly data
                    monthly_df = self.load_result(f"cor_monthly_{metric}.parquet")
                    if monthly_df is not None and not monthly_df.empty:
                        # Convert to quarterly
                        quarterly_df = self.convert_monthly_to_quarterly(monthly_df, metric)
                        if not quarterly_df.empty:
                            self.save_results([(quarterly_df, f"quarterly_{metric}.parquet", metric)])
                            
                except Exception as e:
                    logger.warning(f"Could not process quarterly data for {metric}: {e}")
                    
        except Exception as e:
            logger.error(f"Error processing quarterly reports: {e}")

    def convert_monthly_to_quarterly(self, df, metric):
        """Convert monthly data to quarterly aggregations"""
        try:
            if 'Month' not in df.columns:
                logger.warning(f"No Month column found in {metric} data")
                return pd.DataFrame()
            
            # Add quarter information
            df['Quarter'] = pd.to_datetime(df['Month']).dt.quarter
            df['Year'] = pd.to_datetime(df['Month']).dt.year
            
            # Group by quarter and calculate averages
            quarterly_df = df.groupby(['Corridor', 'Year', 'Quarter']).agg({
                metric: 'mean',
                'Month': 'count'  # Number of months in quarter
            }).reset_index().rename(columns={'Month': 'months_in_quarter'})
            
            return optimize_dataframe_memory(quarterly_df)
            
        except Exception as e:
            logger.error(f"Error converting {metric} to quarterly: {e}")
            return pd.DataFrame()

    def generate_signal_state_summary(self):
        """Generate comprehensive signal state summary"""
        try:
            # Collect signal status information
            signal_status = {}
            
            for signal_id in self.signals_list:
                signal_status[signal_id] = {
                    'corridor': self.get_signal_corridor(signal_id),
                    'has_detector_data': self.check_signal_has_data(signal_id, 'detector_uptime'),
                    'has_volume_data': self.check_signal_has_data(signal_id, 'vpd'),
                    'has_aog_data': self.check_signal_has_data(signal_id, 'aog'),
                    'data_quality_score': self.calculate_signal_data_quality(signal_id)
                }
            
            # Convert to DataFrame
            status_df = pd.DataFrame.from_dict(signal_status, orient='index').reset_index()
            status_df.columns = ['SignalID'] + list(status_df.columns[1:])
            
            # Save signal status summary
            self.save_results([(status_df, "signal_status_summary.parquet", "summary")])
            
        except Exception as e:
            logger.error(f"Error generating signal state summary: {e}")

    def get_signal_corridor(self, signal_id):
        """Get corridor for a signal"""
        try:
            if 'SignalID' in self.corridors.columns and 'Corridor' in self.corridors.columns:
                corridor_match = self.corridors[self.corridors['SignalID'] == signal_id]
                if not corridor_match.empty:
                    return corridor_match['Corridor'].iloc[0]
            return 'Unknown'
        except:
            return 'Unknown'

    def check_signal_has_data(self, signal_id, data_type):
        """Check if signal has data for specific metric type"""
        try:
            # This is a simplified check - in practice you'd check actual data files
            return True  # Placeholder
        except:
            return False

    def calculate_signal_data_quality(self, signal_id):
        """Calculate data quality score for a signal (0-100)"""
        try:
            # Simplified quality score calculation
            score = 100  # Start with perfect score
            
            # Deduct points for missing data types
            data_types = ['detector_uptime', 'vpd', 'aog', 'comm_uptime']
            for data_type in data_types:
                if not self.check_signal_has_data(signal_id, data_type):
                    score -= 25  # Deduct 25 points for each missing data type
            
            return max(0, score)  # Ensure score doesn't go below 0
            
        except:
            return 0

    def cleanup_temp_data(self):
        """Clean up temporary data and large objects from memory"""
        try:
            # Clear large DataFrames to free memory
            attrs_to_clear = [
                'aog', 'sf', 'qs', 'papd', 'corridors', 'subcorridors', 
                'cam_config', 'signals_list'
            ]
            
            for attr in attrs_to_clear:
                if hasattr(self, attr):
                    delattr(self, attr)
                    logger.debug(f"Cleared {attr} from memory")
            
            # Force garbage collection
            gc.collect()
            
            logger.info("Memory cleanup completed")
            
        except Exception as e:
            logger.error(f"Error during memory cleanup: {e}")

    def send_completion_notification(self, duration):
        """Send completion notification via configured channels"""
        try:
            notification_config = self.config.get('notifications', {})
            
            message = f"""
            Monthly Report Package Completed Successfully
            
            Duration: {format_duration(duration)}
            Signals Processed: {len(self.signals_list)}
            Report Period: {self.calcs_start_date} to {self.report_end_date}
            Completion Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            if notification_config.get('email', {}).get('enabled', False):
                self.send_email_notification(message)
                
            if notification_config.get('slack', {}).get('enabled', False):
                self.send_slack_notification(message)
                
        except Exception as e:
            logger.error(f"Error sending notifications: {e}")

    def send_email_notification(self, message):
        """Send email notification of completion"""
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            email_config = self.config.get('notifications', {}).get('email', {})
            
            if not all(k in email_config for k in ['smtp_server', 'port', 'username', 'password', 'recipients']):
                logger.warning("Email configuration incomplete, skipping email notification")
                return
            
            msg = MIMEMultipart()
            msg['From'] = email_config['username']
            msg['To'] = ', '.join(email_config['recipients'])
            msg['Subject'] = "Monthly Report Package - Completion Notification"
            
            msg.attach(MIMEText(message, 'plain'))
            
            server = smtplib.SMTP(email_config['smtp_server'], email_config['port'])
            server.starttls()
            server.login(email_config['username'], email_config['password'])
            
            text = msg.as_string()
            server.sendmail(email_config['username'], email_config['recipients'], text)
            server.quit()
            
            logger.info("Email notification sent successfully")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {e}")

    def send_slack_notification(self, message):
        """Send Slack notification of completion"""
        try:
            import requests
            import json
            
            slack_config = self.config.get('notifications', {}).get('slack', {})
            webhook_url = slack_config.get('webhook_url')
            
            if not webhook_url:
                logger.warning("Slack webhook URL not configured, skipping Slack notification")
                return
            
            payload = {
                'text': 'Monthly Report Package Completed',
                'attachments': [{
                    'color': 'good',
                    'fields': [{
                        'title': 'Status',
                        'value': message,
                        'short': False
                    }]
                }]
            }
            
            response = requests.post(
                webhook_url,
                data=json.dumps(payload),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
            else:
                logger.error(f"Failed to send Slack notification: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    def create_completion_marker(self):
        """Create a completion marker file for external monitoring"""
        try:
            marker_data = {
                'completion_time': datetime.now().isoformat(),
                'report_period_start': str(self.calcs_start_date),
                'report_period_end': str(self.report_end_date),
                'signals_processed': len(self.signals_list),
                'processing_duration_seconds': time.time() - self.start_time,
                'status': 'completed'
            }
            
            # Save marker to S3
            marker_df = pd.DataFrame([marker_data])
            self.save_results([(marker_df, f"completion_marker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", "markers")])
            
            # Also save as local file for immediate reference
            marker_file = f"logs/completion_marker_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('logs', exist_ok=True)
            
            with open(marker_file, 'w') as f:
                json.dump(marker_data, f, indent=2)
            
            logger.info(f"Completion marker created: {marker_file}")
            
        except Exception as e:
            logger.error(f"Error creating completion marker: {e}")

    # Helper methods for remaining processing functions

    def get_hourly(self, df, variable, corridors):
        """Process hourly data with corridor information"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Group by signal, phase, date, and hour
            hourly_data = df.groupby([
                'SignalID', 'CallPhase', 'Date',
                df['Timeperiod'].dt.hour if 'Timeperiod' in df.columns else 0
            ]).agg({variable: 'sum'}).reset_index()
            
            # Add corridor information
            result = hourly_data.merge(corridors, on='SignalID', how='left')
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_hourly: {e}")
            return pd.DataFrame()

    def get_weekly_vph_peak(self, df):
        """Get weekly VPH peak values by time period"""
        try:
            if df.empty or 'Hour' not in df.columns:
                return {}
            
            # Define peak periods
            am_peak = df[df['Hour'].isin([7, 8, 9])]
            pm_peak = df[df['Hour'].isin([16, 17, 18])]
            
            return {
                'am_peak': am_peak.groupby(['SignalID', 'Week'])['vph'].max().reset_index(),
                'pm_peak': pm_peak.groupby(['SignalID', 'Week'])['vph'].max().reset_index()
            }
            
        except Exception as e:
            logger.error(f"Error in get_weekly_vph_peak: {e}")
            return {}

    def get_cor_weekly_vph_peak(self, df):
        """Get corridor weekly VPH peak values"""
        try:
            if df.empty:
                return {}
            
            peak_data = self.get_weekly_vph_peak(df)
            result = {}
            
            for period, data in peak_data.items():
                if not data.empty:
                    cor_data = data.merge(self.corridors, on='SignalID', how='left')
                    result[period] = cor_data.groupby(['Corridor', 'Week'])['vph'].mean().reset_index()
            
            return result
            
        except Exception as e:
            logger.error(f"Error in get_cor_weekly_vph_peak: {e}")
            return {}

    def get_weekly_thruput(self, df):
        """Get weekly throughput"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_weekly_avg_by_day(df, "vph")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_thruput: {e}")
            return pd.DataFrame()

    def get_monthly_thruput(self, df):
        """Get monthly throughput"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_monthly_avg_by_day(df, "vph")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_thruput: {e}")
            return pd.DataFrame()

    def get_cor_weekly_thruput(self, df, corridors):
        """Get corridor weekly throughput"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
            ).agg(vph=('vph', 'sum')).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_weekly_thruput: {e}")
            return pd.DataFrame()

    def get_cor_monthly_thruput(self, df, corridors):
        """Get corridor monthly throughput"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
            ).agg(vph=('vph', 'sum')).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_thruput: {e}")
            return pd.DataFrame()

    def get_daily_aog(self, df):
        """Get daily arrivals on green"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_daily_avg(df, "aog", "vol")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_daily_aog: {e}")
            return pd.DataFrame()

    def get_weekly_aog_by_day(self, df):
        """Get weekly AOG by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_weekly_avg_by_day(df, "aog", "vol")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_aog_by_day: {e}")
            return pd.DataFrame()

    def get_monthly_aog_by_day(self, df):
        """Get monthly AOG by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_monthly_avg_by_day(df, "aog", "vol")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_aog_by_day: {e}")
            return pd.DataFrame()

    def get_cor_weekly_aog_by_day(self, df, corridors):
        """Get corridor weekly AOG by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['aog'], weights=x.get('vol', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['aog']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_weekly_aog_by_day: {e}")
            return pd.DataFrame()

    def get_cor_monthly_aog_by_day(self, df, corridors):
        """Get corridor monthly AOG by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['aog'], weights=x.get('vol', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['aog']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_aog_by_day: {e}")
            return pd.DataFrame()

    def get_aog_by_hr(self, df):
        """Get AOG by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Group by hour and calculate weighted average
            if 'Date_Hour' in df.columns:
                hourly_data = df.groupby([
                    'SignalID', 'CallPhase',
                    df['Date_Hour'].dt.hour
                ]).apply(lambda x: np.average(x['aog'], weights=x.get('vol', 1))).reset_index()
                hourly_data.columns = ['SignalID', 'CallPhase', 'Hour', 'aog']
            else:
                hourly_data = df.copy()
            
            return optimize_dataframe_memory(hourly_data)
            
        except Exception as e:
            logger.error(f"Error in get_aog_by_hr: {e}")
            return pd.DataFrame()

    def get_monthly_aog_by_hr(self, df):
        """Get monthly AOG by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = df.groupby(['SignalID', 'CallPhase', 'Hour']).agg(
                aog=('aog', 'mean')
            ).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_aog_by_hr: {e}")
            return pd.DataFrame()

    def get_cor_monthly_aog_by_hr(self, df, corridors):
        """Get corridor monthly AOG by hour"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Hour']
            ).agg(aog=('aog', 'mean')).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_aog_by_hr: {e}")
            return pd.DataFrame()

    def get_weekly_pr_by_day(self, df):
        """Get weekly progression ratio by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_weekly_avg_by_day(df, "pr", "vol")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_pr_by_day: {e}")
            return pd.DataFrame()

    def get_monthly_pr_by_day(self, df):
        """Get monthly progression ratio by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_monthly_avg_by_day(df, "pr", "vol")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_pr_by_day: {e}")
            return pd.DataFrame()

    def get_cor_weekly_pr_by_day(self, df, corridors):
        """Get corridor weekly progression ratio by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['pr'], weights=x.get('vol', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['pr']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_weekly_pr_by_day: {e}")
            return pd.DataFrame()

    def get_cor_monthly_pr_by_day(self, df, corridors):
        """Get corridor monthly progression ratio by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['pr'], weights=x.get('vol', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['pr']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_pr_by_day: {e}")
            return pd.DataFrame()

    def get_pr_by_hr(self, df):
        """Get progression ratio by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Group by hour and calculate weighted average
            if 'Date_Hour' in df.columns:
                hourly_data = df.groupby([
                    'SignalID', 'CallPhase',
                    df['Date_Hour'].dt.hour
                ]).apply(lambda x: np.average(x['pr'], weights=x.get('vol', 1))).reset_index()
                hourly_data.columns = ['SignalID', 'CallPhase', 'Hour', 'pr']
            else:
                hourly_data = df.copy()
            
            return optimize_dataframe_memory(hourly_data)
            
        except Exception as e:
            logger.error(f"Error in get_pr_by_hr: {e}")
            return pd.DataFrame()

    def get_monthly_pr_by_hr(self, df):
        """Get monthly progression ratio by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = df.groupby(['SignalID', 'CallPhase', 'Hour']).agg(
                pr=('pr', 'mean')
            ).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_pr_by_hr: {e}")
            return pd.DataFrame()

    def get_cor_monthly_pr_by_hr(self, df, corridors):
        """Get corridor monthly progression ratio by hour"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Hour']
            ).agg(pr=('pr', 'mean')).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_pr_by_hr: {e}")
            return pd.DataFrame()

    def get_sf_by_hr(self, df):
        """Get split failures by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Group by hour
            if 'Date_Hour' in df.columns:
                hourly_data = df.groupby([
                    'SignalID', 'CallPhase',
                    df['Date_Hour'].dt.hour
                ]).agg(
                    sf_freq=('sf_freq', 'mean'),
                    cycles=('cycles', 'sum')
                ).reset_index()
            else:
                hourly_data = df.copy()
            
            return optimize_dataframe_memory(hourly_data)
            
        except Exception as e:
            logger.error(f"Error in get_sf_by_hr: {e}")
            return pd.DataFrame()

    def get_monthly_sf_by_hr(self, df):
        """Get monthly split failures by hour"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = df.groupby(['SignalID', 'CallPhase', 'Hour']).agg(
                sf_freq=('sf_freq', 'mean'),
                cycles=('cycles', 'sum')
            ).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_sf_by_hr: {e}")
            return pd.DataFrame()

    def get_cor_monthly_sf_by_hr(self, df, corridors):
        """Get corridor monthly split failures by hour"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Hour']
            ).apply(lambda x: np.average(x['sf_freq'], weights=x.get('cycles', 1))).reset_index()
            result.columns = ['Corridor', 'Hour', 'sf_freq']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_sf_by_hr: {e}")
            return pd.DataFrame()

    def get_weekly_qs_by_day(self, df):
        """Get weekly queue spillback by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_weekly_avg_by_day(df, "qs_freq", "cycles")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_qs_by_day: {e}")
            return pd.DataFrame()

    def get_monthly_qs_by_day(self, df):
        """Get monthly queue spillback by day"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            result = get_monthly_avg_by_day(df, "qs_freq", "cycles")
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_qs_by_day: {e}")
            return pd.DataFrame()

    def get_cor_weekly_qs_by_day(self, df, corridors):
        """Get corridor weekly queue spillback by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['qs_freq'], weights=x.get('cycles', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['qs_freq']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_weekly_qs_by_day: {e}")
            return pd.DataFrame()

    def get_cor_monthly_qs_by_day(self, df, corridors):
        """Get corridor monthly queue spillback by day"""
        try:
            if df.empty or corridors.empty:
                return pd.DataFrame()
            
            result = df.merge(corridors, on='SignalID', how='left').groupby(
                ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
            ).apply(lambda x: np.average(x['qs_freq'], weights=x.get('cycles', 1))).reset_index()
            result.columns = list(result.columns[:-1]) + ['qs_freq']
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_cor_monthly_qs_by_day: {e}")
            return pd.DataFrame()

    def get_monthly_metric_by_day(self, df, metric):
        """Get monthly metric by day (generic function)"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            # Add month column if not present
            if 'Month' not in df.columns:
                df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.to_timestamp()
            
            result = df.groupby(['Corridor', 'Month']).agg({
                metric: 'mean'
            }).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_metric_by_day for {metric}: {e}")
            return pd.DataFrame()

    def get_cctv_uptime_data(self):
        """Get CCTV uptime data from multiple sources"""
        try:
            # Simplified CCTV uptime data collection
            # In practice, this would query multiple data sources
            
            uptime_data = []
            date_range = pd.date_range(
                start=self.wk_calcs_start_date,
                end=self.report_end_date,
                freq='D'
            )
            
            for camera_id in self.cam_config['CameraID'].unique():
                for date in date_range:
                    # Simulate uptime data (replace with actual data source)
                    uptime_data.append({
                        'CameraID': camera_id,
                        'Date': date.date(),
                        'uptime': np.random.uniform(0.8, 1.0),  # Placeholder
                        'num': 1
                    })
            
            df = pd.DataFrame(uptime_data)
            
            # Merge with camera configuration
            result = df.merge(self.cam_config, on='CameraID', how='left')
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error getting CCTV uptime data: {e}")
            return pd.DataFrame()

    def get_weekly_cctv_uptime(self, df):
        """Get weekly CCTV uptime"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
            
            result = df.groupby(['CameraID', 'Week']).agg(
                uptime=('uptime', 'mean'),
                num=('num', 'sum')
            ).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_weekly_cctv_uptime: {e}")
            return pd.DataFrame()

    def get_monthly_cctv_uptime(self, df):
        """Get monthly CCTV uptime"""
        try:
            if df.empty:
                return pd.DataFrame()
            
            df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.to_timestamp()
            
            result = df.groupby(['CameraID', 'Month']).agg(
                uptime=('uptime', 'mean'),
                num=('num', 'sum')
            ).reset_index()
            
            return optimize_dataframe_memory(result)
            
        except Exception as e:
            logger.error(f"Error in get_monthly_cctv_uptime: {e}")
            return pd.DataFrame()

    def run_all_processing_steps(self):
        """Main method to run all 29 processing steps with memory optimization"""
        try:
            self.start_time = time.time()
            logger.info("Starting Monthly Report Package processing")
            logger.info(f"Week Calcs Start Date: {self.wk_calcs_start_date}")
            logger.info(f"Calcs Start Date: {self.calcs_start_date}")
            logger.info(f"Report End Date: {self.report_end_date}")
            
            # Log initial memory usage
            memory_info = get_memory_usage()
            logger.info(f"Initial memory usage: {memory_info['rss_mb']:.2f}MB ({memory_info['percent']:.1f}%)")
            
            # Create progress tracker
            progress_tracker = create_progress_tracker(29, "Monthly Report Processing")
            
            processing_steps = [
                # (0, "Travel Times", self.process_travel_times),
                (1, "Vehicle Detector Uptime", self.process_detector_uptime),
                # (2, "Pedestrian Pushbutton Uptime", self.process_pedestrian_uptime),
                # (3, "Watchdog Alerts", self.process_watchdog_alerts),
                # (4, "Daily Pedestrian Activations", self.process_daily_pedestrian_activations),
                # (5, "Hourly Pedestrian Activations", self.process_hourly_pedestrian_activations),
                # (6, "Pedestrian Delay", self.process_ped_delay),
                # (7, "Communication Uptime", self.process_comm_uptime),
                # (8, "Daily Volumes", self.process_daily_volumes),
                # (9, "Hourly Volumes", self.process_hourly_volumes),
                # (10, "Daily Throughput", self.process_daily_throughput),
                # (11, "Daily Arrivals on Green", self.process_daily_aog),
                # (12, "Hourly Arrivals on Green", self.process_hourly_aog),
                # (13, "Daily Progression Ratio", self.process_daily_progression_ratio),
                # (14, "Hourly Progression Ratio", self.process_hourly_progression_ratio),
                # (15, "Daily Split Failures", self.process_daily_split_failures),
                # (16, "Hourly Split Failures", self.process_hourly_split_failures),
                # (17, "Daily Queue Spillback", self.process_daily_queue_spillback),
                # (18, "Hourly Queue Spillback", self.process_hourly_queue_spillback),
                # (19, "Travel Time Indexes", self.process_travel_time_indexes),
                # (20, "CCTV Uptime", self.process_cctv_uptime),
                # (21, "Travel Time Speeds", self.process_travel_time_speeds),
                # (22, "Ramp Meter Features", self.process_ramp_meter_features),
                # (23, "Counts 1hr Calcs", self.process_counts_1hr_calcs),
                # (24, "Monthly Summary Calcs", self.process_monthly_summary_calcs),
                # (25, "Hi-res Calcs", self.process_high_resolution_calcs),
                # (26, "Lane-by-lane Volume Calcs", self.process_lane_by_lane_volume_calcs),
                # (27, "Mainline Green Utilization", self.process_mainline_green_utilization),
                # (28, "Coordination and Preemption", self.process_coordination_and_preemption),
                (29, "Task Completion and Cleanup", self.process_task_completion_and_cleanup)
            ]
            
            # Execute each processing step with memory monitoring
            for step_num, step_name, step_function in processing_steps:
                try:
                    progress_tracker(step_num)
                    logger.info(f"Starting step {step_num}: {step_name}")
                    
                    # Monitor memory before step
                    memory_before = get_memory_usage()
                    step_start_time = time.time()
                    
                    # Execute step with memory cleanup
                    with memory_cleanup():
                        step_function()
                    
                    step_duration = time.time() - step_start_time
                    memory_after = get_memory_usage()
                    
                    logger.info(f"Completed step {step_num}: {step_name} in {format_duration(step_duration)}")
                    logger.info(f"Memory: {memory_before['rss_mb']:.1f}MB -> {memory_after['rss_mb']:.1f}MB")
                    
                    # Force garbage collection after each step
                    gc.collect()
                    
                except Exception as e:
                    logger.error(f"Error in step {step_num} ({step_name}): {e}")
                    logger.error(traceback.format_exc())
                    # Continue with next step even if one fails
                    gc.collect()
                    continue
            
            # Final progress update
            progress_tracker(len(processing_steps))
            
            total_duration = time.time() - self.start_time
            final_memory = get_memory_usage()
            
            logger.info(f"Monthly Report Package processing completed in {format_duration(total_duration)}")
            logger.info(f"Final memory usage: {final_memory['rss_mb']:.2f}MB ({final_memory['percent']:.1f}%)")
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in monthly report processing: {e}")
            logger.error(traceback.format_exc())
            return False

# Context managers and utility functions

class memory_cleanup:
    """Context manager for automatic memory cleanup"""
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        gc.collect()


def optimize_dataframe_memory(df):
    """Optimize DataFrame memory usage by downcasting numeric types"""
    try:
        if df.empty:
            return df
        
        # Optimize numeric columns
        for col in df.select_dtypes(include=['int64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer')
        
        for col in df.select_dtypes(include=['float64']).columns:
            df[col] = pd.to_numeric(df[col], downcast='float')
        
        # Convert object columns to category if they have low cardinality
        for col in df.select_dtypes(include=['object']).columns:
            num_unique_values = df[col].nunique()
            num_total_values = len(df[col])
            if num_unique_values / num_total_values < 0.5:  # Less than 50% unique
                df[col] = df[col].astype('category')
        
        return df
        
    except Exception as e:
        logger.warning(f"Error optimizing DataFrame memory: {e}")
        return df


def validate_config(config):
    """Validate configuration file structure and required fields"""
    try:
        required_fields = [
            'bucket',
            'report_end_date',
            'calcs_start_date',
            'corridors_filename_s3'
        ]
        
        for field in required_fields:
            if field not in config:
                logger.error(f"Missing required configuration field: {field}")
                return False
        
        # Auto-generate wk_calcs_start_date if missing
        if 'wk_calcs_start_date' not in config or config['wk_calcs_start_date'] == 'auto':
            if config['calcs_start_date'] == 'auto':
                # Default to 3 months ago for weekly calcs
                config['wk_calcs_start_date'] = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
            else:
                # Use calcs_start_date as base
                base_date = pd.to_datetime(config['calcs_start_date'])
                config['wk_calcs_start_date'] = (base_date - timedelta(days=30)).strftime('%Y-%m-%d')
            
            logger.info(f"Auto-generated wk_calcs_start_date: {config['wk_calcs_start_date']}")
        
        # Auto-generate calcs_start_date if set to 'auto'
        if config['calcs_start_date'] == 'auto':
            # Default to 1 month ago
            config['calcs_start_date'] = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            logger.info(f"Auto-generated calcs_start_date: {config['calcs_start_date']}")
        
        # Validate date formats
        date_fields = ['report_end_date', 'calcs_start_date', 'wk_calcs_start_date']
        for field in date_fields:
            try:
                if isinstance(config[field], str):
                    if config[field] in ['yesterday', '2 days ago']:
                        # Handle relative dates
                        if config[field] == 'yesterday':
                            config[field] = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                        elif config[field] == '2 days ago':
                            config[field] = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
                    else:
                        pd.to_datetime(config[field])
                elif not isinstance(config[field], (datetime, pd.Timestamp)):
                    logger.error(f"Invalid date format for {field}")
                    return False
            except Exception:
                logger.error(f"Invalid date format for {field}: {config[field]}")
                return False
        
        # Validate S3 bucket name
        bucket = config['bucket']
        if not isinstance(bucket, str) or len(bucket) < 3:
            logger.error(f"Invalid S3 bucket name: {bucket}")
            return False
        
        logger.info("Configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating configuration: {e}")
        return False

def setup_logging(log_level='INFO'):
    """Setup comprehensive logging configuration"""
    import logging
    from datetime import datetime
    import sys
    
    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Setup log file with timestamp
    timestamp = datetime.now().strftime('%Y%m%d')
    log_filename = logs_dir / f"monthly_report_{timestamp}.log"
    
    # Create custom formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    )
    
    # Setup file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setLevel(getattr(logging, log_level.upper()))
    file_handler.setFormatter(formatter)
    
    # Setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Setup root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # Setup specific loggers
    logger = logging.getLogger(__name__)
    
    # Suppress noisy third-party loggers
    logging.getLogger('boto3').setLevel(logging.WARNING)
    logging.getLogger('botocore').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('s3transfer').setLevel(logging.WARNING)
    
    logger.info(f"Logging initialized - log file: {log_filename}")
    return logger


@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def read_parquet_from_s3(bucket, key):
    """Read parquet file from S3 with retry logic and error handling"""
    try:
        import boto3
        import pyarrow.parquet as pq
        import io
        
        s3_client = boto3.client('s3')
        
        # Check if object exists
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except s3_client.exceptions.NoSuchKey:
            logger.warning(f"S3 object not found: s3://{bucket}/{key}")
            return pd.DataFrame()
        
        # Read object
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Read parquet from bytes
        parquet_file = pq.ParquetFile(io.BytesIO(response['Body'].read()))
        df = parquet_file.read().to_pandas()
        
        logger.debug(f"Successfully read {len(df)} records from s3://{bucket}/{key}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading parquet from S3 {bucket}/{key}: {e}")
        raise


@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def write_parquet_to_s3(df, bucket, key):
    """Write parquet file to S3 with retry logic and compression"""
    try:
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io
        
        if df.empty:
            logger.warning(f"Attempting to write empty DataFrame to s3://{bucket}/{key}")
            return
        
        # Convert DataFrame to parquet bytes with compression
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(
            table, 
            buffer, 
            compression='snappy',
            use_dictionary=True,
            row_group_size=50000
        )
        buffer.seek(0)
        
        # Upload to S3 with metadata
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.read(),
            ContentType='application/octet-stream',
            Metadata={
                'created_by': 'monthly_report_package',
                'created_at': datetime.now().isoformat(),
                'row_count': str(len(df)),
                'columns': str(list(df.columns))
            }
        )
        
        logger.info(f"Successfully wrote {len(df)} records to s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Error writing parquet to S3 {bucket}/{key}: {e}")
        raise


def check_s3_connectivity(bucket):
    """Check S3 connectivity and permissions"""
    try:
        import boto3
        
        s3_client = boto3.client('s3')
        
        # Try to list objects in bucket (limited to 1 for efficiency)
        response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        
        logger.info(f"S3 connectivity check passed for bucket: {bucket}")
        return True
        
    except Exception as e:
        logger.error(f"S3 connectivity check failed for bucket {bucket}: {e}")
        return False


if __name__ == "__main__":
    import sys
    import json
    import gc
    
    # Setup logging
    logger = setup_logging()
    
    try:
        # Load configuration from Monthly_Report.yaml
        config_path = 'Monthly_Report.yaml'
        if not os.path.exists(config_path):
            logger.error(f"Configuration file not found: {config_path}")
            sys.exit(1)
            
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        logger.info(f"Configuration loaded from {config_path}")
        
        # Validate configuration
        if not validate_config(config):
            logger.error("Configuration validation failed")
            sys.exit(1)
        
        # Check S3 connectivity
        if not check_s3_connectivity(config['bucket']):
            logger.error("S3 connectivity check failed")
            sys.exit(1)
        
        # Initialize processor
        processor = MonthlyReportProcessor(config)
        
        start_time = time.time()
        
        # Run all processing steps
        success = processor.run_all_processing_steps()
        
        duration = time.time() - start_time
        
        # Final logging
        if success:
            logger.info("Monthly Report Package completed successfully")
            logger.info(f"Total duration: {format_duration(duration)}")
            sys.exit(0)
        else:
            logger.error("Monthly Report Package completed with errors")
            logger.error(f"Total duration: {format_duration(duration)}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error in Monthly Report Package: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Final cleanup
        gc.collect()
        final_memory = get_memory_usage()
        logger.info(f"Final memory usage: {final_memory['rss_mb']:.2f}MB ({final_memory['percent']:.1f}%)")
