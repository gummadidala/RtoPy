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
        conf, aws_conf = load_yaml_configuration()
        self.bucket = conf.get('bucket', '')
        self.athena = conf.get('athena', {})
        self.calcs_start_date = conf.get('calcs_start_date', 'auto')
        self.report_end_date = conf.get('report_end_date', 'yesterday')

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
    """Process vehicle detector uptime [1 of 29]"""
    logger.info(f"{datetime.now()} Vehicle Detector Uptime [1 of 29 (mark1)]")
    try:
        def callback(x):
            return get_avg_daily_detector_uptime(x)
        
        # Read detector uptime data
        avg_daily_detector_uptime = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="detector_uptime_pd",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list'],
            callback=callback
        )
        if not avg_daily_detector_uptime.empty:
            avg_daily_detector_uptime['SignalID'] = avg_daily_detector_uptime['SignalID'].astype('category')
            
            # Calculate corridor and subcorridor metrics
            cor_avg_daily_detector_uptime = get_cor_avg_daily_detector_uptime(
                avg_daily_detector_uptime, config_data['corridors']
            )
            
            sub_avg_daily_detector_uptime = get_cor_avg_daily_detector_uptime(
                avg_daily_detector_uptime, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Calculate weekly metrics
            weekly_detector_uptime = get_weekly_detector_uptime(avg_daily_detector_uptime)
            cor_weekly_detector_uptime = get_cor_weekly_detector_uptime(
                weekly_detector_uptime, config_data['corridors']
            )
            sub_weekly_detector_uptime = get_cor_weekly_detector_uptime(
                weekly_detector_uptime, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Calculate monthly metrics
            monthly_detector_uptime = get_monthly_detector_uptime(avg_daily_detector_uptime)
            cor_monthly_detector_uptime = get_cor_monthly_detector_uptime(
                avg_daily_detector_uptime, config_data['corridors']
            )
            sub_monthly_detector_uptime = get_cor_monthly_detector_uptime(
                avg_daily_detector_uptime, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save all results
            save_data(avg_daily_detector_uptime, "avg_daily_detector_uptime.pkl")
            save_data(weekly_detector_uptime, "weekly_detector_uptime.pkl")
            save_data(monthly_detector_uptime, "monthly_detector_uptime.pkl")
            save_data(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.pkl")
            save_data(cor_weekly_detector_uptime, "cor_weekly_detector_uptime.pkl")
            save_data(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.pkl")
            save_data(sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.pkl")
            save_data(sub_weekly_detector_uptime, "sub_weekly_detector_uptime.pkl")
            save_data(sub_monthly_detector_uptime, "sub_monthly_detector_uptime.pkl")
            
            logger.info("Detector uptime processing completed successfully")
            # Cleanup memory
            del avg_daily_detector_uptime
            del cor_avg_daily_detector_uptime
            del sub_avg_daily_detector_uptime
            del weekly_detector_uptime
            del cor_weekly_detector_uptime
            del sub_weekly_detector_uptime
            del monthly_detector_uptime
            del cor_monthly_detector_uptime
            del sub_monthly_detector_uptime
            gc.collect()
    except Exception as e:
        logger.error(f"Error in detector uptime processing: {e}")
        logger.error(traceback.format_exc())

def process_ped_pushbutton_uptime(dates, config_data):
    """Process pedestrian pushbutton uptime [2 of 29]"""
    logger.info(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (mark1)]")
    
    try:
        # Calculate PAU start date (minimum of calcs_start_date or 6 months before report end)
        pau_start_date = min(
            dates['calcs_start_date'],
            dates['report_end_date'] - relativedelta(months=6)
        )
        
        # Read pedestrian count data
        counts_ped_hourly = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="counts_ped_1hr",
            start_date=pau_start_date,
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list'],
            parallel=False
        )
        
        if not counts_ped_hourly.empty:
            # Filter out non-programmed pedestrian pushbuttons
            counts_ped_hourly = counts_ped_hourly.dropna(subset=['CallPhase'])
            counts_ped_hourly = clean_signal_ids(counts_ped_hourly)
            counts_ped_hourly['Detector'] = counts_ped_hourly['Detector'].astype('category')
            counts_ped_hourly['CallPhase'] = counts_ped_hourly['CallPhase'].astype('category')
            counts_ped_hourly = calculate_time_periods(counts_ped_hourly)
            counts_ped_hourly['vol'] = pd.to_numeric(counts_ped_hourly['vol'], errors='coerce')
            
            # Calculate daily pedestrian activations
            counts_ped_daily = counts_ped_hourly.groupby([
                'SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase'
            ])['vol'].sum().reset_index()
            counts_ped_daily.rename(columns={'vol': 'papd'}, inplace=True)
            
            papd = counts_ped_daily.copy()
            paph = counts_ped_hourly.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'})
            
            # Calculate pedestrian uptime using gamma distribution
            date_range = pd.date_range(pau_start_date, dates['report_end_date'], freq='D')
            pau = get_pau_gamma(
                date_range, papd, paph, config_data['corridors'], 
                dates['wk_calcs_start_date'], pau_start_date
            )
            
            if not pau.empty:
                # Remove and replace papd for bad days
                pau_with_replacements = pau.copy()
                pau_with_replacements.loc[pau_with_replacements['uptime'] == 0, 'papd'] = np.nan
                
                # Replace with monthly averages
                monthly_avg = pau_with_replacements.groupby([
                    'SignalID', 'Detector', 'CallPhase', 
                    pau_with_replacements['Date'].dt.year,
                    pau_with_replacements['Date'].dt.month
                ])['papd'].transform('mean')
                
                pau_with_replacements['papd'] = pau_with_replacements['papd'].fillna(monthly_avg.fillna(0))
                papd = pau_with_replacements[['SignalID', 'Detector', 'CallPhase', 'Date', 'DOW', 'Week', 'papd', 'uptime']]
                
                # Identify bad detectors
                bad_detectors = get_bad_ped_detectors(pau)
                bad_detectors = bad_detectors[bad_detectors['Date'] >= dates['calcs_start_date']]
                
                if not bad_detectors.empty:
                    save_data(bad_detectors, "bad_ped_detectors.pkl")
                    del bad_detectors  # Cleanup memory
                
                # Calculate uptime metrics (hack to make aggregation functions work)
                save_data(pau, "pa_uptime.pkl")
                pau['CallPhase'] = pau['Detector']  # Hack for compatibility
                
                # Calculate daily, weekly, and monthly uptime
                daily_pa_uptime = get_daily_avg(pau, "uptime", peak_only=False)
                weekly_pa_uptime = get_weekly_avg_by_day(pau, "uptime", peak_only=False)
                monthly_pa_uptime = get_monthly_avg_by_day(pau, "uptime", peak_only=False)
                
                # Calculate corridor metrics
                cor_daily_pa_uptime = get_cor_weekly_avg_by_day(
                    daily_pa_uptime, config_data['corridors'], "uptime"
                )
                sub_daily_pa_uptime = get_cor_weekly_avg_by_day(
                    daily_pa_uptime, config_data['subcorridors'], "uptime"
                ).dropna(subset=['Corridor'])
                
                cor_weekly_pa_uptime = get_cor_weekly_avg_by_day(
                    weekly_pa_uptime, config_data['corridors'], "uptime"
                )
                sub_weekly_pa_uptime = get_cor_weekly_avg_by_day(
                    weekly_pa_uptime, config_data['subcorridors'], "uptime"
                ).dropna(subset=['Corridor'])
                
                cor_monthly_pa_uptime = get_cor_monthly_avg_by_day(
                    monthly_pa_uptime, config_data['corridors'], "uptime"
                )
                sub_monthly_pa_uptime = get_cor_monthly_avg_by_day(
                    monthly_pa_uptime, config_data['subcorridors'], "uptime"
                ).dropna(subset=['Corridor'])
                
                # Save all results
                save_data(daily_pa_uptime, "daily_pa_uptime.pkl")
                save_data(cor_daily_pa_uptime, "cor_daily_pa_uptime.pkl")
                save_data(sub_daily_pa_uptime, "sub_daily_pa_uptime.pkl")
                save_data(weekly_pa_uptime, "weekly_pa_uptime.pkl")
                save_data(cor_weekly_pa_uptime, "cor_weekly_pa_uptime.pkl")
                save_data(sub_weekly_pa_uptime, "sub_weekly_pa_uptime.pkl")
                save_data(monthly_pa_uptime, "monthly_pa_uptime.pkl")
                save_data(cor_monthly_pa_uptime, "cor_monthly_pa_uptime.pkl")
                save_data(sub_monthly_pa_uptime, "sub_monthly_pa_uptime.pkl")
                # Cleanup memory
                del daily_pa_uptime
                del cor_daily_pa_uptime
                del sub_daily_pa_uptime
                del weekly_pa_uptime
                del cor_weekly_pa_uptime
                del sub_weekly_pa_uptime
                del monthly_pa_uptime
                del cor_monthly_pa_uptime
                del sub_monthly_pa_uptime
                del pau
                gc.collect()
                logger.info("Pedestrian pushbutton uptime processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in pedestrian pushbutton uptime processing: {e}")
        logger.error(traceback.format_exc())

def process_watchdog_alerts(dates, config_data):
    """Process watchdog alerts [3 of 29]"""
    logger.info(f"{datetime.now()} Watchdog alerts [3 of 29 (mark1)]")
    
    try:
        # Process bad vehicle detectors
        bad_det = s3_read_parquet_parallel(
            table_name="bad_detectors",
            start_date=date.today() - timedelta(days=90),
            end_date=date.today() - timedelta(days=1),
            bucket=conf.bucket
        )
        
        if not bad_det.empty:
            bad_det = clean_signal_ids(bad_det)
            bad_det['Detector'] = bad_det['Detector'].astype('category')
            
            # Get detector configuration with fallback
            det_config_list = []
            unique_dates = sorted(bad_det['Date'].unique())
            
            for date_val in unique_dates:
                try:
                    # Get detector config factory function
                    get_det_config = get_det_config_factory(conf.bucket, 'atspm_det_config_good')
                    config = get_det_config(date_val)
                    
                    if not config.empty:
                        config['Date'] = date_val
                        det_config_list.append(config)
                    else:
                        logger.warning(f"No detector config found for {date_val}")
                        
                except Exception as e:
                    logger.warning(f"Error loading detector config for {date_val}: {e}")
            
            # Process detector data if config available
            if det_config_list:
                det_config = pd.concat(det_config_list, ignore_index=True)
                det_config = clean_signal_ids(det_config)
                det_config['CallPhase'] = det_config['CallPhase'].astype('category')
                det_config['Detector'] = det_config['Detector'].astype('category')
                
                # Join with detector configuration
                bad_det = bad_det.merge(
                    det_config[['SignalID', 'Detector', 'Date', 'CallPhase', 'LaneType', 'MovementType']],
                    on=['SignalID', 'Detector', 'Date'],
                    how='left'
                )
            
            # Add missing columns with defaults
            for col in ['CallPhase', 'LaneType', 'MovementType']:
                if col not in bad_det.columns:
                    bad_det[col] = 'Unknown'
            
            # Join with corridor information
            bad_det = bad_det.merge(
                config_data['corridors'][['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Name']],
                on='SignalID',
                how='left'
            ).dropna(subset=['Corridor'])
            
            # Format the output
            bad_det = bad_det.assign(
                Alert='Bad Vehicle Detection',
                Name=lambda x: x['Name'].str.replace('@', '-', regex=False),
                ApproachDesc=lambda x: x['LaneType'].fillna('Unknown').astype(str)
            )[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector', 
               'Date', 'Alert', 'Name', 'ApproachDesc']]
            
            save_data(bad_det, "watchdog_bad_detectors.pkl")
            del bad_det  # Cleanup memory
        # Process bad pedestrian detectors
        try:
            bad_ped = s3_read_parquet_parallel(
                table_name="bad_ped_detectors",
                start_date=date.today() - timedelta(days=90),
                end_date=date.today() - timedelta(days=1),
                bucket=conf.bucket
            )
            
            if not bad_ped.empty:
                bad_ped = clean_signal_ids(bad_ped)
                bad_ped['Detector'] = bad_ped['Detector'].astype('category')
                
                bad_ped = bad_ped.merge(
                    config_data['corridors'][['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Name']],
                    on='SignalID',
                    how='left'
                )[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Detector', 'Date', 'Name']].assign(
                    Alert='Bad Ped Detection'
                )
                
                save_data(bad_ped, "watchdog_bad_ped_pushbuttons.pkl")
                del bad_ped  # Cleanup memory
        except Exception as e:
            logger.warning(f"No bad pedestrian detectors data found: {e}")
        
        # Process bad cameras - FIXED
        try:
            bad_cam_list = []
            start_month = date.today().replace(day=1) - relativedelta(months=6)
            current_month = start_month
            
            while current_month < date.today():
                try:
                    key = f"mark/cctv_uptime/month={current_month.strftime('%Y-%m-%d')}/cctv_uptime_{current_month.strftime('%Y-%m-%d')}.parquet"
                    
                    # FIXED: Pass the function object, not string
                    cctv_data = s3read_using(
                        pd.read_parquet,  # Function object
                        bucket=conf.bucket, 
                        object=key
                    )
                    
                    if not cctv_data.empty:
                        bad_cameras = cctv_data[cctv_data.get('Size', 0) == 0]
                        if not bad_cameras.empty:
                            bad_cam_list.append(bad_cameras)
                    del cctv_data  # Cleanup memory
                except Exception as e:
                    logger.warning(f"Could not read CCTV data for {current_month.strftime('%Y-%m-%d')}: {e}")
                
                current_month += relativedelta(months=1)
            
            if bad_cam_list:
                bad_cam = pd.concat(bad_cam_list, ignore_index=True)
                
                # Merge with camera config if available
                if 'cam_config' in config_data and not config_data['cam_config'].empty:
                    bad_cam = bad_cam.merge(
                        config_data['cam_config'], 
                        on='CameraID', 
                        how='left'
                    )
                    if 'As_of_Date' in bad_cam.columns and 'Date' in bad_cam.columns:
                        bad_cam = bad_cam[bad_cam['Date'] > bad_cam['As_of_Date']]
                
                # Add required columns with defaults
                required_cols = ['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector', 
                               'Date', 'Alert', 'Name']
                
                for col in required_cols:
                    if col not in bad_cam.columns:
                        if col == 'SignalID':
                            bad_cam[col] = bad_cam.get('CameraID', 'Unknown')
                        elif col in ['CallPhase', 'Detector']:
                            bad_cam[col] = 0
                        elif col == 'Alert':
                            bad_cam[col] = 'No Camera Image'
                        elif col == 'Name':
                            bad_cam[col] = bad_cam.get('Location', bad_cam.get('CameraID', 'Unknown'))
                        else:
                            bad_cam[col] = 'Unknown'
                
                bad_cam = bad_cam[required_cols]
                save_data(bad_cam, "watchdog_bad_cameras.pkl")
                del bad_cam  # Cleanup memory
                
        except Exception as e:
            logger.warning(f"Could not process camera data: {e}")
        
        logger.info("Watchdog alerts processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in watchdog alerts processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_ped_activations(dates, config_data):
    """Process daily pedestrian activations [4 of 29]"""
    logger.info(f"{datetime.now()} Daily Pedestrian Activations [4 of 29 (mark1)]")
    
    try:
        # Load PAPD data (should be available from previous processing)
        papd = load_data("pa_uptime.pkl")
        
        if not papd.empty:
            # Calculate weekly PAPD
            weekly_papd = get_weekly_papd(papd)
            
            # Group into corridors
            cor_weekly_papd = get_cor_weekly_papd(weekly_papd, config_data['corridors'])
            
            # Group into subcorridors
            sub_weekly_papd = get_cor_weekly_papd(
                weekly_papd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Monthly volumes for bar charts and % change
            monthly_papd = get_monthly_papd(papd)
            
            # Group into corridors
            cor_monthly_papd = get_cor_monthly_papd(monthly_papd, config_data['corridors'])
            
            # Group into subcorridors
            sub_monthly_papd = get_cor_monthly_papd(
                monthly_papd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_papd, "weekly_papd.pkl")
            save_data(monthly_papd, "monthly_papd.pkl")
            save_data(cor_weekly_papd, "cor_weekly_papd.pkl")
            save_data(cor_monthly_papd, "cor_monthly_papd.pkl")
            save_data(sub_weekly_papd, "sub_weekly_papd.pkl")
            save_data(sub_monthly_papd, "sub_monthly_papd.pkl")
            
            # Cleanup memory
            del weekly_papd
            del monthly_papd
            del cor_weekly_papd
            del cor_monthly_papd
            del sub_weekly_papd
            del sub_monthly_papd
            gc.collect()

            logger.info("Daily pedestrian activations processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily pedestrian activations processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_ped_activations(dates, config_data):
    """Process hourly pedestrian activations [5 of 29]"""
    logger.info(f"{datetime.now()} Hourly Pedestrian Activations [5 of 29 (mark1)]")
    
    try:
        # Load PAPH data (should be available from previous processing)
        paph = load_data("pa_uptime.pkl")  # This contains hourly data
        
        if not paph.empty:
            weekly_paph = get_weekly_paph(paph)
            monthly_paph = get_monthly_paph(paph)
            
            # Group into corridors
            cor_weekly_paph = get_cor_weekly_paph(weekly_paph, config_data['corridors'])
            sub_weekly_paph = get_cor_weekly_paph(
                weekly_paph, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Hourly volumes by Corridor
            cor_monthly_paph = get_cor_monthly_paph(monthly_paph, config_data['corridors'])
            sub_monthly_paph = get_cor_monthly_paph(
                monthly_paph, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_paph, "weekly_paph.pkl")
            save_data(monthly_paph, "monthly_paph.pkl")
            save_data(cor_weekly_paph, "cor_weekly_paph.pkl")
            save_data(cor_monthly_paph, "cor_monthly_paph.pkl")
            save_data(sub_weekly_paph, "sub_weekly_paph.pkl")
            save_data(sub_monthly_paph, "sub_monthly_paph.pkl")

            # Cleanup memory
            del weekly_paph
            del monthly_paph
            del cor_weekly_paph
            del cor_monthly_paph
            del sub_weekly_paph
            del sub_monthly_paph
            gc.collect()
            
            logger.info("Hourly pedestrian activations processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly pedestrian activations processing: {e}")
        logger.error(traceback.format_exc())

def process_pedestrian_delay(dates, config_data):
    """Process pedestrian delay [6 of 29]"""
    logger.info(f"{datetime.now()} Pedestrian Delay [6 of 29 (mark1)]")
    
    try:
        def callback(x):
            if "Avg.Max.Ped.Delay" in x.columns:
                x = x.rename(columns={"Avg.Max.Ped.Delay": "pd"})
                x['CallPhase'] = 0
            x = calculate_time_periods(x)
            return x
        
        ped_delay = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="ped_delay",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list'],
            callback=callback
        )
        
        if not ped_delay.empty:
            ped_delay = clean_signal_ids(ped_delay)
            ped_delay['CallPhase'] = ped_delay['CallPhase'].astype('category')
            ped_delay['Events'] = ped_delay.get('Events', 1).fillna(1)
            
            daily_pd = get_daily_avg(ped_delay, "pd", "Events")
            weekly_pd_by_day = get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
            monthly_pd_by_day = get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
            
            cor_weekly_pd_by_day = get_cor_weekly_avg_by_day(
                weekly_pd_by_day, config_data['corridors'], "pd", "Events"
            )
            cor_monthly_pd_by_day = get_cor_monthly_avg_by_day(
                monthly_pd_by_day, config_data['corridors'], "pd", "Events"
            )
            
            sub_weekly_pd_by_day = get_cor_weekly_avg_by_day(
                weekly_pd_by_day, config_data['subcorridors'], "pd", "Events"
            ).dropna(subset=['Corridor'])
            sub_monthly_pd_by_day = get_cor_monthly_avg_by_day(
                monthly_pd_by_day, config_data['subcorridors'], "pd", "Events"
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_pd_by_day, "weekly_pd_by_day.pkl")
            save_data(monthly_pd_by_day, "monthly_pd_by_day.pkl")
            save_data(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.pkl")
            save_data(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.pkl")
            save_data(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.pkl")
            save_data(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.pkl")

            # Cleanup memory
            del weekly_pd_by_day
            del monthly_pd_by_day
            del cor_weekly_pd_by_day
            del cor_monthly_pd_by_day
            del sub_weekly_pd_by_day
            del sub_monthly_pd_by_day
            gc.collect()
            
            logger.info("Pedestrian delay processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in pedestrian delay processing: {e}")
        logger.error(traceback.format_exc())

def process_communications_uptime(dates, config_data):
    """Process communications uptime [7 of 29]"""
    logger.info(f"{datetime.now()} Communication Uptime [7 of 29 (mark1)]")
    
    try:
        cu = s3_read_parquet_parallel(
            bucket=conf.bucket,
                        table_name="comm_uptime",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not cu.empty:
            cu = clean_signal_ids(cu)
            cu['CallPhase'] = cu['CallPhase'].astype('category')
            cu = ensure_datetime_column(cu, 'Date')
            
            daily_comm_uptime = get_daily_avg(cu, "uptime", peak_only=False)
            cor_daily_comm_uptime = get_cor_weekly_avg_by_day(
                daily_comm_uptime, config_data['corridors'], "uptime"
            )
            sub_daily_comm_uptime = get_cor_weekly_avg_by_day(
                daily_comm_uptime, config_data['subcorridors'], "uptime"
            ).dropna(subset=['Corridor'])
            
            weekly_comm_uptime = get_weekly_avg_by_day(cu, "uptime", peak_only=False)
            cor_weekly_comm_uptime = get_cor_weekly_avg_by_day(
                weekly_comm_uptime, config_data['corridors'], "uptime"
            )
            sub_weekly_comm_uptime = get_cor_weekly_avg_by_day(
                weekly_comm_uptime, config_data['subcorridors'], "uptime"
            ).dropna(subset=['Corridor'])
            
            monthly_comm_uptime = get_monthly_avg_by_day(cu, "uptime", peak_only=False)
            cor_monthly_comm_uptime = get_cor_monthly_avg_by_day(
                monthly_comm_uptime, config_data['corridors'], "uptime"
            )
            sub_monthly_comm_uptime = get_cor_monthly_avg_by_day(
                monthly_comm_uptime, config_data['subcorridors'], "uptime"
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(daily_comm_uptime, "daily_comm_uptime.pkl")
            save_data(cor_daily_comm_uptime, "cor_daily_comm_uptime.pkl")
            save_data(sub_daily_comm_uptime, "sub_daily_comm_uptime.pkl")
            save_data(weekly_comm_uptime, "weekly_comm_uptime.pkl")
            save_data(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.pkl")
            save_data(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.pkl")
            save_data(monthly_comm_uptime, "monthly_comm_uptime.pkl")
            save_data(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.pkl")
            save_data(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.pkl")

            # Cleanup memory
            del daily_comm_uptime
            del cor_daily_comm_uptime
            del sub_daily_comm_uptime
            del weekly_comm_uptime
            del cor_weekly_comm_uptime
            del sub_weekly_comm_uptime
            del monthly_comm_uptime
            del cor_monthly_comm_uptime
            del sub_monthly_comm_uptime
            gc.collect()
            
            logger.info("Communications uptime processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in communications uptime processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_volumes(dates, config_data):
    """Process daily volumes [8 of 29]"""
    logger.info(f"{datetime.now()} Daily Volumes [8 of 29 (mark1)]")
    
    try:
        vpd = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="vehicles_pd",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not vpd.empty:
            vpd = clean_signal_ids(vpd)
            vpd['CallPhase'] = vpd['CallPhase'].astype('category')
            vpd = ensure_datetime_column(vpd, 'Date')
            
            weekly_vpd = get_weekly_vpd(vpd)
            
            # Group into corridors
            cor_weekly_vpd = get_cor_weekly_vpd(weekly_vpd, config_data['corridors'])
            # Subcorridors
            sub_weekly_vpd = get_cor_weekly_vpd(
                weekly_vpd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Monthly volumes for bar charts and % change
            monthly_vpd = get_monthly_vpd(vpd)
            
            # Group into corridors
            cor_monthly_vpd = get_cor_monthly_vpd(monthly_vpd, config_data['corridors'])
            
            # Subcorridors
            sub_monthly_vpd = get_cor_monthly_vpd(
                monthly_vpd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_vpd, "weekly_vpd.pkl")
            save_data(monthly_vpd, "monthly_vpd.pkl")
            save_data(cor_weekly_vpd, "cor_weekly_vpd.pkl")
            save_data(cor_monthly_vpd, "cor_monthly_vpd.pkl")
            save_data(sub_weekly_vpd, "sub_weekly_vpd.pkl")
            save_data(sub_monthly_vpd, "sub_monthly_vpd.pkl")

            # Cleanup memory
            del weekly_vpd
            del monthly_vpd
            del cor_weekly_vpd
            del cor_monthly_vpd
            del sub_weekly_vpd
            del sub_monthly_vpd
            gc.collect()
            
            logger.info("Daily volumes processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily volumes processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_volumes(dates, config_data):
    """Process hourly volumes [9 of 29]"""
    logger.info(f"{datetime.now()} Hourly Volumes [9 of 29 (mark1)]")
    
    try:
        vph = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="vehicles_ph",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not vph.empty:
            vph = clean_signal_ids(vph)
            vph['CallPhase'] = 2  # Hack because next function needs a CallPhase
            vph = ensure_datetime_column(vph, 'Date')
            vph = ensure_timeperiod_column(vph)  # Ensure Timeperiod column exists
            
            weekly_vph = get_weekly_vph(vph)
            if not weekly_vph.empty:
                weekly_vph_peak = get_weekly_vph_peak(weekly_vph)
                
                # Group into corridors
                cor_weekly_vph = get_cor_weekly_vph(weekly_vph, config_data['corridors'])
                if not cor_weekly_vph.empty:
                    cor_weekly_vph_peak = get_cor_weekly_vph_peak(cor_weekly_vph)
                else:
                    cor_weekly_vph_peak = pd.DataFrame()
                
                # Group into Subcorridors
                sub_weekly_vph = get_cor_weekly_vph(
                    weekly_vph, config_data['subcorridors']
                )
                if not sub_weekly_vph.empty and 'Corridor' in sub_weekly_vph.columns:
                    sub_weekly_vph = sub_weekly_vph.dropna(subset=['Corridor'])
                    sub_weekly_vph_peak = get_cor_weekly_vph_peak(sub_weekly_vph)
                else:
                    sub_weekly_vph_peak = pd.DataFrame()
            else:
                weekly_vph_peak = pd.DataFrame()
                cor_weekly_vph = pd.DataFrame()
                cor_weekly_vph_peak = pd.DataFrame()
                sub_weekly_vph = pd.DataFrame()
                sub_weekly_vph_peak = pd.DataFrame()
            
            monthly_vph = get_monthly_vph(vph)
            if not monthly_vph.empty:
                monthly_vph_peak = get_monthly_vph_peak(monthly_vph)
                
                # Hourly volumes by Corridor
                cor_monthly_vph = get_cor_monthly_vph(monthly_vph, config_data['corridors'])
                if not cor_monthly_vph.empty:
                    cor_monthly_vph_peak = get_cor_monthly_vph_peak(cor_monthly_vph)
                else:
                    cor_monthly_vph_peak = pd.DataFrame()
                
                # Hourly volumes by Subcorridor
                sub_monthly_vph = get_cor_monthly_vph(
                    monthly_vph, config_data['subcorridors']
                )
                if not sub_monthly_vph.empty and 'Corridor' in sub_monthly_vph.columns:
                    sub_monthly_vph = sub_monthly_vph.dropna(subset=['Corridor'])
                    sub_monthly_vph_peak = get_cor_monthly_vph_peak(sub_monthly_vph)
                else:
                    sub_monthly_vph_peak = pd.DataFrame()
            else:
                monthly_vph_peak = pd.DataFrame()
                cor_monthly_vph = pd.DataFrame()
                cor_monthly_vph_peak = pd.DataFrame()
                sub_monthly_vph = pd.DataFrame()
                sub_monthly_vph_peak = pd.DataFrame()
            
            # Save results
            save_data(weekly_vph, "weekly_vph.pkl")
            save_data(monthly_vph, "monthly_vph.pkl")
            save_data(cor_weekly_vph, "cor_weekly_vph.pkl")
            save_data(cor_monthly_vph, "cor_monthly_vph.pkl")
            save_data(sub_weekly_vph, "sub_weekly_vph.pkl")
            save_data(sub_monthly_vph, "sub_monthly_vph.pkl")
            save_data(weekly_vph_peak, "weekly_vph_peak.pkl")
            save_data(monthly_vph_peak, "monthly_vph_peak.pkl")
            save_data(cor_weekly_vph_peak, "cor_weekly_vph_peak.pkl")
            save_data(cor_monthly_vph_peak, "cor_monthly_vph_peak.pkl")
            save_data(sub_weekly_vph_peak, "sub_weekly_vph_peak.pkl")
            save_data(sub_monthly_vph_peak, "sub_monthly_vph_peak.pkl")

            # Cleanup memory
            del weekly_vph
            del monthly_vph
            del cor_weekly_vph
            del cor_monthly_vph
            del sub_weekly_vph
            del sub_monthly_vph
            del weekly_vph_peak
            del monthly_vph_peak
            del cor_weekly_vph_peak
            del cor_monthly_vph_peak
            del sub_weekly_vph_peak
            del sub_monthly_vph_peak
            gc.collect()
            
            logger.info("Hourly volumes processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly volumes processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_throughput(dates, config_data):
    """Process daily throughput [10 of 29]"""
    logger.info(f"{datetime.now()} Daily Throughput [10 of 29 (mark1)]")
    
    try:
        throughput = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="throughput",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not throughput.empty:
            throughput = clean_signal_ids(throughput)
            throughput['CallPhase'] = throughput['CallPhase'].astype(int).astype('category')
            throughput = ensure_datetime_column(throughput, 'Date')
            throughput = ensure_throughput_column(throughput)
            
            weekly_throughput = get_weekly_thruput(throughput)
            monthly_throughput = get_monthly_thruput(throughput)
            
            # Weekly throughput - Group into corridors
            cor_weekly_throughput = get_cor_weekly_thruput(weekly_throughput, config_data['corridors'])
            sub_weekly_throughput = safe_dropna_corridor(
                get_cor_weekly_thruput(weekly_throughput, config_data['subcorridors']),
                "sub_weekly_throughput"
            )
            
            # Monthly throughput - Group into corridors
            cor_monthly_throughput = get_cor_monthly_thruput(monthly_throughput, config_data['corridors'])
            sub_monthly_throughput = safe_dropna_corridor(
                get_cor_monthly_thruput(monthly_throughput, config_data['subcorridors']),
                "sub_monthly_throughput"
            )
            
            # Save results
            save_data(weekly_throughput, "weekly_throughput.pkl")
            save_data(monthly_throughput, "monthly_throughput.pkl")
            save_data(cor_weekly_throughput, "cor_weekly_throughput.pkl")
            save_data(cor_monthly_throughput, "cor_monthly_throughput.pkl")
            save_data(sub_weekly_throughput, "sub_weekly_throughput.pkl")
            save_data(sub_monthly_throughput, "sub_monthly_throughput.pkl")

            # Cleanup memory
            del weekly_throughput
            del monthly_throughput
            del cor_weekly_throughput
            del cor_monthly_throughput
            del sub_weekly_throughput
            del sub_monthly_throughput
            gc.collect()
            
            logger.info("Daily throughput processing completed successfully")
        else:
            logger.warning("No throughput data found")
        
    except Exception as e:
        logger.error(f"Error in daily throughput processing: {e}")
        logger.error(traceback.format_exc())

def process_arrivals_on_green(dates, config_data):
    """Process daily arrivals on green [11 of 29]"""
    logger.info(f"{datetime.now()} Daily AOG [11 of 29 (mark1)]")
    
    try:
        aog = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="arrivals_on_green",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not aog.empty:
            aog = clean_signal_ids(aog)
            aog['CallPhase'] = aog['CallPhase'].astype('category')
            aog = calculate_time_periods(aog, 'Date')
            
            daily_aog = get_daily_aog(aog)
            weekly_aog_by_day = get_weekly_aog_by_day(aog)
            monthly_aog_by_day = get_monthly_aog_by_day(aog)
            
            cor_weekly_aog_by_day = get_cor_weekly_aog_by_day(weekly_aog_by_day, config_data['corridors'])
            cor_monthly_aog_by_day = get_cor_monthly_aog_by_day(monthly_aog_by_day, config_data['corridors'])
            
            sub_weekly_aog_by_day = get_cor_weekly_aog_by_day(
                weekly_aog_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            sub_monthly_aog_by_day = get_cor_monthly_aog_by_day(
                monthly_aog_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_aog_by_day, "weekly_aog_by_day.pkl")
            save_data(monthly_aog_by_day, "monthly_aog_by_day.pkl")
            save_data(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.pkl")
            save_data(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.pkl")
            save_data(sub_weekly_aog_by_day, "sub_weekly_aog_by_day.pkl")
            save_data(sub_monthly_aog_by_day, "sub_monthly_aog_by_day.pkl")

            # Cleanup memory
            del weekly_aog_by_day
            del monthly_aog_by_day
            del cor_weekly_aog_by_day
            del cor_monthly_aog_by_day
            del sub_weekly_aog_by_day
            del sub_monthly_aog_by_day
            gc.collect()
            
            # Store aog for use in progression ratio calculations
            save_data(aog, "aog_data.pkl")
            
            logger.info("Daily arrivals on green processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily arrivals on green processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_arrivals_on_green(dates, config_data):
    """Process hourly arrivals on green [12 of 29]"""
    logger.info(f"{datetime.now()} Hourly AOG [12 of 29 (mark1)]")
    
    try:
        # Load AOG data from previous processing
        aog = load_data("aog_data.pkl")
        
        if not aog.empty:
            aog_by_hr = get_aog_by_hr(aog)
            monthly_aog_by_hr = get_monthly_aog_by_hr(aog_by_hr)
            
                        # Hourly volumes by Corridor
            cor_monthly_aog_by_hr = get_cor_monthly_aog_by_hr(monthly_aog_by_hr, config_data['corridors'])
            sub_monthly_aog_by_hr = get_cor_monthly_aog_by_hr(
                monthly_aog_by_hr, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(monthly_aog_by_hr, "monthly_aog_by_hr.pkl")
            save_data(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.pkl")
            save_data(sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.pkl")

            # Cleanup memory
            del monthly_aog_by_hr
            del cor_monthly_aog_by_hr
            del sub_monthly_aog_by_hr
            gc.collect()
            
            logger.info("Hourly arrivals on green processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly arrivals on green processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_progression_ratio(dates, config_data):
    """Process daily progression ratio [13 of 29]"""
    logger.info(f"{datetime.now()} Daily Progression Ratio [13 of 29 (mark1)]")
    
    try:
        # Load AOG data from previous processing
        aog = load_data("aog_data.pkl")
        
        if not aog.empty:
            weekly_pr_by_day = get_weekly_pr_by_day(aog)
            monthly_pr_by_day = get_monthly_pr_by_day(aog)
            
            cor_weekly_pr_by_day = get_cor_weekly_pr_by_day(weekly_pr_by_day, config_data['corridors'])
            cor_monthly_pr_by_day = get_cor_monthly_pr_by_day(monthly_pr_by_day, config_data['corridors'])
            
            sub_weekly_pr_by_day = get_cor_weekly_pr_by_day(
                weekly_pr_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            sub_monthly_pr_by_day = get_cor_monthly_pr_by_day(
                monthly_pr_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_pr_by_day, "weekly_pr_by_day.pkl")
            save_data(monthly_pr_by_day, "monthly_pr_by_day.pkl")
            save_data(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.pkl")
            save_data(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.pkl")
            save_data(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.pkl")
            save_data(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.pkl")

            # Cleanup memory
            del weekly_pr_by_day
            del monthly_pr_by_day
            del cor_weekly_pr_by_day
            del cor_monthly_pr_by_day
            del sub_weekly_pr_by_day
            del sub_monthly_pr_by_day
            gc.collect()
            
            logger.info("Daily progression ratio processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily progression ratio processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_progression_ratio(dates, config_data):
    """Process hourly progression ratio [14 of 29]"""
    logger.info(f"{datetime.now()} Hourly Progression Ratio [14 of 29 (mark1)]")
    
    try:
        # Load AOG data from previous processing
        aog = load_data("aog_data.pkl")
        
        if not aog.empty:
            pr_by_hr = get_pr_by_hr(aog)
            monthly_pr_by_hr = get_monthly_pr_by_hr(pr_by_hr)
            
            # Hourly volumes by Corridor
            cor_monthly_pr_by_hr = get_cor_monthly_pr_by_hr(monthly_pr_by_hr, config_data['corridors'])
            sub_monthly_pr_by_hr = get_cor_monthly_pr_by_hr(
                monthly_pr_by_hr, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(monthly_pr_by_hr, "monthly_pr_by_hr.pkl")
            save_data(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.pkl")
            save_data(sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.pkl")

            # Cleanup memory
            del monthly_pr_by_hr
            del cor_monthly_pr_by_hr
            del sub_monthly_pr_by_hr
            gc.collect()
            
            logger.info("Hourly progression ratio processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly progression ratio processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_split_failures(dates, config_data):
    """Process daily split failures [15 of 29]"""
    logger.info(f"{datetime.now()} Daily Split Failures [15 of 29 (mark1)]")
    
    try:
        def filter_callback(x):
            return x[x['CallPhase'] == 0]
        
        sf = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="split_failures",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list'],
            callback=filter_callback
        )
        
        if not sf.empty:
            sf = clean_signal_ids(sf)
            sf['CallPhase'] = sf['CallPhase'].astype('category')
            sf = ensure_datetime_column(sf, 'Date')
            
            # Add hour column for peak/off-peak separation
            if 'Date_Hour' in sf.columns:
                sf['Hour'] = pd.to_datetime(sf['Date_Hour']).dt.hour
            else:
                sf['Hour'] = 12  # Default if no hour info
            
            # Divide into peak/off-peak split failures
            sfo = sf[~sf['Hour'].isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]  # Off-peak
            sfp = sf[sf['Hour'].isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]   # Peak
            
            # Calculate weekly metrics
            weekly_sf_by_day = get_weekly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False)
            weekly_sfo_by_day = get_weekly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)
            
            cor_weekly_sf_by_day = get_cor_weekly_sf_by_day(weekly_sf_by_day, config_data['corridors'])
            cor_weekly_sfo_by_day = get_cor_weekly_sf_by_day(weekly_sfo_by_day, config_data['corridors'])
            
            sub_weekly_sf_by_day = get_cor_weekly_sf_by_day(
                weekly_sf_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            sub_weekly_sfo_by_day = get_cor_weekly_sf_by_day(
                weekly_sfo_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Calculate monthly metrics
            monthly_sf_by_day = get_monthly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False)
            monthly_sfo_by_day = get_monthly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)
            
            cor_monthly_sf_by_day = get_cor_monthly_sf_by_day(monthly_sf_by_day, config_data['corridors'])
            cor_monthly_sfo_by_day = get_cor_monthly_sf_by_day(monthly_sfo_by_day, config_data['corridors'])
            
            sub_monthly_sf_by_day = get_cor_monthly_sf_by_day(
                monthly_sf_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            sub_monthly_sfo_by_day = get_cor_monthly_sf_by_day(
                monthly_sfo_by_day, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(weekly_sf_by_day, "wsf.pkl")
            save_data(monthly_sf_by_day, "monthly_sfd.pkl")
            save_data(cor_weekly_sf_by_day, "cor_wsf.pkl")
            save_data(cor_monthly_sf_by_day, "cor_monthly_sfd.pkl")
            save_data(sub_weekly_sf_by_day, "sub_wsf.pkl")
            save_data(sub_monthly_sf_by_day, "sub_monthly_sfd.pkl")
            save_data(weekly_sfo_by_day, "wsfo.pkl")
            save_data(monthly_sfo_by_day, "monthly_sfo.pkl")
            save_data(cor_weekly_sfo_by_day, "cor_wsfo.pkl")
            save_data(cor_monthly_sfo_by_day, "cor_monthly_sfo.pkl")
            save_data(sub_weekly_sfo_by_day, "sub_wsfo.pkl")
            save_data(sub_monthly_sfo_by_day, "sub_monthly_sfo.pkl")

            # Cleanup memory
            del weekly_sf_by_day
            del monthly_sf_by_day
            del cor_weekly_sf_by_day
            del cor_monthly_sf_by_day
            del sub_weekly_sf_by_day
            del sub_monthly_sf_by_day
            del weekly_sfo_by_day
            del monthly_sfo_by_day
            del cor_weekly_sfo_by_day
            del cor_monthly_sfo_by_day
            del sub_weekly_sfo_by_day
            del sub_monthly_sfo_by_day
            gc.collect()
            
            # Store sf for hourly processing
            save_data(sf, "sf_data.pkl")
            
            logger.info("Daily split failures processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily split failures processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_split_failures(dates, config_data):
    """Process hourly split failures [16 of 29]"""
    logger.info(f"{datetime.now()} Hourly Split Failures [16 of 29 (mark1)]")
    
    try:
        # Load SF data from previous processing
        sf = load_data("sf_data.pkl")
        
        if not sf.empty:
            sfh = get_sf_by_hr(sf)
            msfh = get_monthly_sf_by_hr(sfh)
            
            # Hourly volumes by Corridor
            cor_msfh = get_cor_monthly_sf_by_hr(msfh, config_data['corridors'])
            sub_msfh = get_cor_monthly_sf_by_hr(
                msfh, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(msfh, "msfh.pkl")
            save_data(cor_msfh, "cor_msfh.pkl")
            save_data(sub_msfh, "sub_msfh.pkl")

            # Cleanup memory
            del msfh
            del cor_msfh
            del sub_msfh
            gc.collect()
            
            logger.info("Hourly split failures processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly split failures processing: {e}")
        logger.error(traceback.format_exc())

def process_daily_queue_spillback(dates, config_data):
    """Process daily queue spillback [17 of 29]"""
    logger.info(f"{datetime.now()} Daily Queue Spillback [17 of 29 (mark1)]")
    
    try:
        qs = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="queue_spillback",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not qs.empty:
            qs = clean_signal_ids(qs)
            qs['CallPhase'] = qs['CallPhase'].astype('category')
            qs = ensure_datetime_column(qs, 'Date')
            
            wqs = get_weekly_qs_by_day(qs)
            cor_wqs = get_cor_weekly_qs_by_day(wqs, config_data['corridors'])
            sub_wqs = get_cor_weekly_qs_by_day(
                wqs, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            monthly_qsd = get_monthly_qs_by_day(qs)
            cor_monthly_qsd = get_cor_monthly_qs_by_day(monthly_qsd, config_data['corridors'])
            sub_monthly_qsd = get_cor_monthly_qs_by_day(
                monthly_qsd, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(wqs, "wqs.pkl")
            save_data(monthly_qsd, "monthly_qsd.pkl")
            save_data(cor_wqs, "cor_wqs.pkl")
            save_data(cor_monthly_qsd, "cor_monthly_qsd.pkl")
            save_data(sub_wqs, "sub_wqs.pkl")
            save_data(sub_monthly_qsd, "sub_monthly_qsd.pkl")

            # Cleanup memory
            del wqs
            del monthly_qsd
            del cor_wqs
            del cor_monthly_qsd
            del sub_wqs
            del sub_monthly_qsd
            gc.collect()
            
            # Store qs for hourly processing
            save_data(qs, "qs_data.pkl")
            
            logger.info("Daily queue spillback processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in daily queue spillback processing: {e}")
        logger.error(traceback.format_exc())

def process_hourly_queue_spillback(dates, config_data):
    """Process hourly queue spillback [18 of 29]"""
    logger.info(f"{datetime.now()} Hourly Queue Spillback [18 of 29 (mark1)]")
    
    try:
        # Load QS data from previous processing
        qs = load_data("qs_data.pkl")
        
        if not qs.empty:
            qsh = get_qs_by_hr(qs)
            mqsh = get_monthly_qs_by_hr(qsh)
            
            # Hourly volumes by Corridor
            cor_mqsh = get_cor_monthly_qs_by_hr(mqsh, config_data['corridors'])
            sub_mqsh = get_cor_monthly_qs_by_hr(
                mqsh, config_data['subcorridors']
            ).dropna(subset=['Corridor'])
            
            # Save results
            save_data(mqsh, "mqsh.pkl")
            save_data(cor_mqsh, "cor_mqsh.pkl")
            save_data(sub_mqsh, "sub_mqsh.pkl")

            # Cleanup memory
            del mqsh
            del cor_mqsh
            del sub_mqsh
            gc.collect()
            
            logger.info("Hourly queue spillback processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hourly queue spillback processing: {e}")
        logger.error(traceback.format_exc())

def process_travel_time_indexes(dates, config_data):
    """Process travel time and buffer time indexes [19 of 29]"""
    logger.info(f"{datetime.now()} Travel Time Indexes [19 of 29 (mark1)]")
    try:
        # ------- Corridor Travel Time Metrics ------- #
        tt = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="cor_travel_time_metrics_1hr",
            start_date=dates['calcs_start_date'],
            end_date=dates['report_end_date']
        )
        
        if not tt.empty:
            tt['Corridor'] = tt['Corridor'].astype('category')
            tt = tt.merge(
                config_data['all_corridors'][['Zone_Group', 'Zone', 'Corridor']].drop_duplicates(),
                on='Corridor',
                how='left'
            ).dropna(subset=['Zone_Group'])
            
            # Split into separate metrics
            tti = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'tti']].copy()
            pti = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'pti']].copy()
            bi = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'bi']].copy()
            spd = tt[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'speed_mph']].copy()
            
            # Load corridor monthly VPH for weighting
            cor_monthly_vph = load_data("cor_monthly_vph.pkl")
            if not cor_monthly_vph.empty:
                cor_monthly_vph = cor_monthly_vph.rename(columns={'Zone_Group': 'Zone'})
                cor_monthly_vph = cor_monthly_vph.merge(
                    config_data['corridors'][['Zone_Group', 'Zone']].drop_duplicates(),
                    on='Zone',
                    how='left'
                )
                
                # Calculate corridor metrics
                cor_monthly_tti_by_hr = get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_pti_by_hr = get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_bi_by_hr = get_cor_monthly_ti_by_hr(bi, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_spd_by_hr = get_cor_monthly_ti_by_hr(spd, cor_monthly_vph, config_data['all_corridors'])
                
                cor_monthly_tti = get_cor_monthly_ti_by_day(tti, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_pti = get_cor_monthly_ti_by_day(pti, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_bi = get_cor_monthly_ti_by_day(bi, cor_monthly_vph, config_data['all_corridors'])
                cor_monthly_spd = get_cor_monthly_ti_by_day(spd, cor_monthly_vph, config_data['all_corridors'])
                
                # Save corridor results
                save_data(cor_monthly_tti, "cor_monthly_tti.pkl")
                save_data(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.pkl")
                save_data(cor_monthly_pti, "cor_monthly_pti.pkl")
                save_data(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.pkl")
                save_data(cor_monthly_bi, "cor_monthly_bi.pkl")
                save_data(cor_monthly_bi_by_hr, "cor_monthly_bi_by_hr.pkl")
                save_data(cor_monthly_spd, "cor_monthly_spd.pkl")
                save_data(cor_monthly_spd_by_hr, "cor_monthly_spd_by_hr.pkl")

                # Cleanup memory
                del cor_monthly_tti
                del cor_monthly_tti_by_hr
                del cor_monthly_pti
                del cor_monthly_pti_by_hr
                del cor_monthly_bi
                del cor_monthly_bi_by_hr
                del cor_monthly_spd
                del cor_monthly_spd_by_hr
                gc.collect()
        
        # ------- Subcorridor Travel Time Metrics ------- #
        tt_sub = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="sub_travel_time_metrics_1hr",
            start_date=dates['calcs_start_date'],
            end_date=dates['report_end_date']
        )
        
        if not tt_sub.empty:
            tt_sub['Corridor'] = tt_sub['Corridor'].astype('category')
            tt_sub['Subcorridor'] = tt_sub['Subcorridor'].astype('category')
            
            # Rename for consistency
            tt_sub = tt_sub.rename(columns={'Corridor': 'Zone', 'Subcorridor': 'Corridor'})
            tt_sub = tt_sub.merge(
                config_data['subcorridors'][['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )
            
            # Split into separate metrics
            tti_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'tti']].copy()
            pti_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'pti']].copy()
            bi_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'bi']].copy()
            spd_sub = tt_sub[['Zone_Group', 'Zone', 'Corridor', 'Date', 'Hour', 'speed_mph']].copy()
            
            # Load subcorridor monthly VPH for weighting
            sub_monthly_vph = load_data("sub_monthly_vph.pkl")
            if not sub_monthly_vph.empty:
                sub_monthly_vph = sub_monthly_vph.rename(columns={'Zone_Group': 'Zone'})
                sub_monthly_vph = sub_monthly_vph.merge(
                    config_data['subcorridors'][['Zone_Group', 'Zone']].drop_duplicates(),
                    on='Zone',
                    how='left'
                )
                
                # Calculate subcorridor metrics
                sub_monthly_tti_by_hr = get_cor_monthly_ti_by_hr(tti_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_pti_by_hr = get_cor_monthly_ti_by_hr(pti_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_bi_by_hr = get_cor_monthly_ti_by_hr(bi_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_spd_by_hr = get_cor_monthly_ti_by_hr(spd_sub, sub_monthly_vph, config_data['subcorridors'])
                
                sub_monthly_tti = get_cor_monthly_ti_by_day(tti_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_pti = get_cor_monthly_ti_by_day(pti_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_bi = get_cor_monthly_ti_by_day(bi_sub, sub_monthly_vph, config_data['subcorridors'])
                sub_monthly_spd = get_cor_monthly_ti_by_day(spd_sub, sub_monthly_vph, config_data['subcorridors'])
                
                # Save subcorridor results
                save_data(sub_monthly_tti, "sub_monthly_tti.pkl")
                save_data(sub_monthly_tti_by_hr, "sub_monthly_tti_by_hr.pkl")
                save_data(sub_monthly_pti, "sub_monthly_pti.pkl")
                save_data(sub_monthly_pti_by_hr, "sub_monthly_pti_by_hr.pkl")
                save_data(sub_monthly_bi, "sub_monthly_bi.pkl")
                save_data(sub_monthly_bi_by_hr, "sub_monthly_bi_by_hr.pkl")
                save_data(sub_monthly_spd, "sub_monthly_spd.pkl")
                save_data(sub_monthly_spd_by_hr, "sub_monthly_spd_by_hr.pkl")

                # Cleanup memory
                del sub_monthly_tti
                del sub_monthly_tti_by_hr
                del sub_monthly_pti
                del sub_monthly_pti_by_hr
                del sub_monthly_bi
                del sub_monthly_bi_by_hr
                del sub_monthly_spd
                del sub_monthly_spd_by_hr
                gc.collect()
        
        logger.info("Travel time indexes processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in travel time indexes processing: {e}")
        logger.error(traceback.format_exc())

def process_cctv_uptime(dates, config_data):
    """Process CCTV uptime [20 of 29]"""
    logger.info(f"{datetime.now()} CCTV Uptimes [20 of 29 (mark1)]")
    
    try:
        # Get CCTV uptime from 511 and encoders
        daily_cctv_uptime_511 = get_daily_cctv_uptime(
            conf.athena, "cctv_uptime", config_data['cam_config'], dates['wk_calcs_start_date']
        )
        daily_cctv_uptime_encoders = get_daily_cctv_uptime(
            conf.athena, "cctv_uptime_encoders", config_data['cam_config'], dates['wk_calcs_start_date']
        )

        if 'Corridor' not in daily_cctv_uptime_511.columns or 'Corridor' not in daily_cctv_uptime_encoders.columns:
            daily_cctv_uptime_511['Corridor'] = daily_cctv_uptime_511['Corridor_x']
            daily_cctv_uptime_encoders['Corridor'] = daily_cctv_uptime_encoders['Corridor_x']

        if 'Subcorridor' not in daily_cctv_uptime_511.columns or 'Subcorridor' not in daily_cctv_uptime_encoders.columns:
            daily_cctv_uptime_511['Subcorridor'] = daily_cctv_uptime_511['Corridor_y']
            daily_cctv_uptime_encoders['Subcorridor'] = daily_cctv_uptime_encoders['Corridor_y']

        if 'Description' not in daily_cctv_uptime_511.columns or 'Description' not in daily_cctv_uptime_encoders.columns:
            daily_cctv_uptime_511['Description'] = "NA"
            daily_cctv_uptime_encoders['Description'] = "NA"

        if not daily_cctv_uptime_511.empty or not daily_cctv_uptime_encoders.empty:
            # Merge 511 and encoder data
            daily_cctv_uptime = pd.merge(
                daily_cctv_uptime_511,
                daily_cctv_uptime_encoders,
                on=['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Date'],
                how='outer',
                suffixes=('_511', '_enc')
            )
            columns = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description']

            camera_info = (
                daily_cctv_uptime[columns]
                .drop_duplicates()
            )

            # Create MultiIndex with 7 levels: the 6 camera info fields + Date
            camera_dates = pd.MultiIndex.from_product(
                [
                    camera_info[col].unique() for col in columns
                ] + [pd.date_range(dates['wk_calcs_start_date'], dates['report_end_date'])],
                names=columns + ['Date']
            )

            # Now set index to the same 7 columns
            daily_cctv_uptime = (
                daily_cctv_uptime
                .set_index(columns + ['Date'])
                .reindex(camera_dates)
                .reset_index()
            )

            # Fill NaN values
            fill_cols = ['up_enc', 'num_enc', 'uptime_enc', 'up_511', 'num_511', 'uptime_511']
            for col in fill_cols:
                if col in daily_cctv_uptime.columns:
                    daily_cctv_uptime[col] = daily_cctv_uptime[col].fillna(0)
            
            # Calculate uptime metrics
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
            
            # Find bad days (systemic issues)
            bad_days = daily_cctv_uptime.groupby('Date').agg({
                'uptime': 'sum',
                'num': 'sum'
            }).assign(
                suptime=lambda x: x['uptime'] / x['num']
            ).query('suptime < 0.2').index
            
            # Filter out bad days
            daily_cctv_uptime = daily_cctv_uptime[~daily_cctv_uptime['Date'].isin(bad_days)]
            
            # Calculate weekly and monthly metrics
            weekly_cctv_uptime = get_weekly_avg_by_day_cctv(daily_cctv_uptime)
            monthly_cctv_uptime = daily_cctv_uptime.groupby([
                'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description',
                daily_cctv_uptime['Date'].dt.to_period('M').dt.start_time.rename('Month')
            ]).agg({
                'uptime': ['sum', lambda x: np.average(x, weights=daily_cctv_uptime.loc[x.index, 'num'])],
                'num': 'sum'
            }).reset_index()
            
            monthly_cctv_uptime.columns = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Month', 'up', 'uptime', 'num']
            
            # Calculate corridor metrics
            cor_daily_cctv_uptime = get_cor_weekly_avg_by_day(
                daily_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            cor_weekly_cctv_uptime = get_cor_weekly_avg_by_day(
                weekly_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            cor_monthly_cctv_uptime = get_cor_monthly_avg_by_day(
                monthly_cctv_uptime, config_data['all_corridors'], "uptime", "num"
            )
            
            # Calculate subcorridor metrics
            sub_daily_cctv_uptime = daily_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_daily_cctv_uptime = sub_daily_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_daily_cctv_uptime = get_cor_weekly_avg_by_day(
                sub_daily_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            
            sub_weekly_cctv_uptime = weekly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_weekly_cctv_uptime = sub_weekly_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_weekly_cctv_uptime = get_cor_weekly_avg_by_day(
                sub_weekly_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            
            sub_monthly_cctv_uptime = monthly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor'])
            sub_monthly_cctv_uptime = sub_monthly_cctv_uptime.rename(columns={
                'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'
            })
            sub_monthly_cctv_uptime = get_cor_monthly_avg_by_day(
                sub_monthly_cctv_uptime, config_data['subcorridors'], "uptime", "num"
            )
            
            # Save results
            save_data(daily_cctv_uptime, "daily_cctv_uptime.pkl")
            save_data(weekly_cctv_uptime, "weekly_cctv_uptime.pkl")
            save_data(monthly_cctv_uptime, "monthly_cctv_uptime.pkl")
            save_data(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.pkl")
            save_data(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.pkl")
            save_data(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.pkl")
            save_data(sub_daily_cctv_uptime, "sub_daily_cctv_uptime.pkl")
            save_data(sub_weekly_cctv_uptime, "sub_weekly_cctv_uptime.pkl")
            save_data(sub_monthly_cctv_uptime, "sub_monthly_cctv_uptime.pkl")

            # Cleanup memory
            del daily_cctv_uptime
            del weekly_cctv_uptime
            del monthly_cctv_uptime
            del cor_daily_cctv_uptime
            del cor_weekly_cctv_uptime
            del cor_monthly_cctv_uptime
            del sub_daily_cctv_uptime
            del sub_weekly_cctv_uptime
            del sub_monthly_cctv_uptime
            gc.collect()
            
            logger.info("CCTV uptime processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in CCTV uptime processing: {e}")
        logger.error(traceback.format_exc())

def process_teams_activities(dates, config_data):
    """Process TEAMS activities [21 of 29]"""
    logger.info(f"{datetime.now()} TEAMS [21 of 29 (mark1)]")
    
    try:
        # Get TEAMS tasks data
        teams = get_teams_tasks_from_s3(
            bucket=conf.bucket,
            archived_tasks_prefix="mark/teams/tasks202",
            current_tasks_key="mark/teams/tasks.csv.zip",
            report_start_date=dates['report_start_date']
        )
        
        if not teams.empty:
            teams = tidy_teams_tasks(
                teams,
                bucket=conf.bucket,
                corridors=config_data['corridors'],
                replicate=True
            )
            
            # Calculate various task metrics
            tasks_by_type = get_outstanding_tasks_by_param(teams, "Task_Type", dates['report_start_date'])
            tasks_by_subtype = get_outstanding_tasks_by_param(teams, "Task_Subtype", dates['report_start_date'])
            tasks_by_priority = get_outstanding_tasks_by_param(teams, "Priority", dates['report_start_date'])
            tasks_by_source = get_outstanding_tasks_by_param(teams, "Task_Source", dates['report_start_date'])
            tasks_all = get_outstanding_tasks_by_param(teams, "All", dates['report_start_date'])
            
            # Calculate outstanding tasks by date range
            date_list = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
            cor_outstanding_tasks_by_day_range = []
            
            for date_val in date_list:
                task_data = get_outstanding_tasks_by_day_range(teams, dates['report_start_date'], date_val)
                cor_outstanding_tasks_by_day_range.append(task_data)
            
            cor_outstanding_tasks_by_day_range = pd.concat(cor_outstanding_tasks_by_day_range, ignore_index=True)
            cor_outstanding_tasks_by_day_range['Zone_Group'] = cor_outstanding_tasks_by_day_range['Zone_Group'].astype('category')
            cor_outstanding_tasks_by_day_range['Corridor'] = cor_outstanding_tasks_by_day_range['Corridor'].astype('category')
            
            # Sort and calculate deltas
            cor_outstanding_tasks_by_day_range = cor_outstanding_tasks_by_day_range.sort_values(['Zone_Group', 'Corridor', 'Month'])
            cor_outstanding_tasks_by_day_range['delta.over45'] = cor_outstanding_tasks_by_day_range.groupby(['Zone_Group', 'Corridor'])['over45'].pct_change()
            cor_outstanding_tasks_by_day_range['delta.mttr'] = cor_outstanding_tasks_by_day_range.groupby(['Zone_Group', 'Corridor'])['mttr'].pct_change()
            
            # Create signal-level data
            sig_outstanding_tasks_by_day_range = cor_outstanding_tasks_by_day_range.copy()
            sig_outstanding_tasks_by_day_range = sig_outstanding_tasks_by_day_range.groupby('Corridor').first().reset_index()
            sig_outstanding_tasks_by_day_range['Zone_Group'] = sig_outstanding_tasks_by_day_range['Corridor']
            sig_outstanding_tasks_by_day_range = sig_outstanding_tasks_by_day_range[
                sig_outstanding_tasks_by_day_range['Corridor'].isin(config_data['all_corridors']['Corridor'])
            ]
            
            # Save results
            save_data(tasks_by_type, "tasks_by_type.pkl")
            save_data(tasks_by_subtype, "tasks_by_subtype.pkl")
            save_data(tasks_by_priority, "tasks_by_priority.pkl")
            save_data(tasks_by_source, "tasks_by_source.pkl")
            save_data(tasks_all, "tasks_all.pkl")
            save_data(cor_outstanding_tasks_by_day_range, "cor_tasks_by_date.pkl")
            save_data(sig_outstanding_tasks_by_day_range, "sig_tasks_by_date.pkl")

            # Cleanup memory
            del teams
            del tasks_by_type
            del tasks_by_subtype
            del tasks_by_priority
            del tasks_by_source
            del tasks_all
            del cor_outstanding_tasks_by_day_range
            del sig_outstanding_tasks_by_day_range
            gc.collect()
            
            logger.info("TEAMS activities processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in TEAMS activities processing: {e}")
        logger.error(traceback.format_exc())

def process_user_delay_costs(dates, config_data):
    """Process user delay costs [22 of 29]"""
    logger.info(f"{datetime.now()} User Delay Costs [22 of 29 (mark1)]")
    
    try:
        months = pd.date_range(dates['report_start_date'], dates['report_end_date'], freq='MS')
        udc_list = []
        
        for month in months:
            try:
                obj = f"mark/user_delay_costs/date={month.strftime('%Y-%m-%d')}/user_delay_costs_{month.strftime('%Y-%m-%d')}.parquet"
                
                # Check if object exists
                try:
                    udc_data = s3read_using('read_parquet', bucket=conf.bucket, object=obj)
                    
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
                        
                except Exception as e:
                    logger.warning(f"Could not read UDC data for {month}: {e}")
                    
            except Exception as e:
                logger.warning(f"Error processing UDC for {month}: {e}")
        
        if udc_list:
            udc = pd.concat(udc_list, ignore_index=True)
            
            # Calculate hourly UDC
            hourly_udc = udc.groupby(['Zone', 'Corridor', 'Month', 'month_hour'])['delay_cost'].sum().reset_index()
            
            # Calculate trend tables
            unique_months = sorted(udc['analysis_month'].unique())
            udc_trend_table_list = {}
            
            for i, current_month in enumerate(unique_months[1:], 1):
                last_month = current_month - relativedelta(months=1)
                last_year = current_month - relativedelta(years=1)
                
                current_month_str = current_month.strftime("%B %Y")
                last_month_str = last_month.strftime("%B %Y")
                last_year_str = last_year.strftime("%B %Y")
                
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
            
            # Save results
            save_data(hourly_udc, "hourly_udc.pkl")
            save_data(udc_trend_table_list, "udc_trend_table_list.pkl")

            # Cleanup memory
            del udc
            del hourly_udc
            del udc_trend_table_list
            gc.collect()
            
            logger.info("User delay costs processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error in user delay costs processing: {e}")
        logger.error(traceback.format_exc())

def process_flash_events(dates, config_data):
    """Process flash events [23 of 29]"""
    logger.info(f"{datetime.now()} Flash Events [23 of 29 (mark1)]")
    
    try:
        fe = s3_read_parquet_parallel(
            bucket=conf.bucket,
            table_name="flash_events",
            start_date=dates['wk_calcs_start_date'],
            end_date=dates['report_end_date'],
            signals_list=config_data['signals_list']
        )
        
        if not fe.empty:
            fe = clean_signal_ids(fe)
            fe = ensure_datetime_column(fe, 'Date')
            
            # Define flash event column candidates
            flash_candidates = [
                'Flash', 'flash_events', 'Flash_Events', 'FlashEvents', 
                'flash_event', 'Flash_Event', 'events', 'Events',
                'flash_count', 'Flash_Count'
            ]
            fe = ensure_metric_column(fe, 'flash', flash_candidates)
            
            # Monthly flash events for bar charts and % change
            monthly_flash = get_monthly_flashevent(fe)
            
            if not monthly_flash.empty:
                # Group into corridors
                cor_monthly_flash = get_cor_monthly_flash(monthly_flash, config_data['corridors'])
                
                # Subcorridors
                sub_monthly_flash = safe_dropna_corridor(
                    get_cor_monthly_flash(monthly_flash, config_data['subcorridors']),
                    "sub_monthly_flash"
                )
            else:
                cor_monthly_flash = pd.DataFrame()
                sub_monthly_flash = pd.DataFrame()
            
            # Save results
            save_data(monthly_flash, "monthly_flash.pkl")
            save_data(cor_monthly_flash, "cor_monthly_flash.pkl")
            save_data(sub_monthly_flash, "sub_monthly_flash.pkl")

            # Cleanup memory
            del fe
            del monthly_flash
            del cor_monthly_flash
            del sub_monthly_flash
            gc.collect()
            
            logger.info("Flash events processing completed successfully")
        else:
            logger.warning("No flash events data found")
        
    except Exception as e:
        logger.error(f"Error in flash events processing: {e}")
        logger.error(traceback.format_exc())

def process_bike_ped_safety_index(dates, config_data):
    """Process bike/ped safety index [24 of 29]"""
    logger.info(f"{datetime.now()} Bike/Ped Safety Index [24 of 29 (mark1)]")
    
    try:
        date_range = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
        
        # Process subcorridor BPSI
        sub_monthly_bpsi_list = []
        for d in date_range:
            try:
                obj = f"mark/bike_ped_safety_index/bpsi_sub_{d.strftime('%Y-%m-%d')}.parquet"
                # FIX: Pass the function object, not string
                bpsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not bpsi_data.empty:
                    # Remove system columns
                    bpsi_data = bpsi_data.loc[:, ~bpsi_data.columns.str.startswith('__')]
                    bpsi_data['Month'] = d
                    bpsi_data = bpsi_data.rename(columns={'overall_pct': 'bpsi'})
                    sub_monthly_bpsi_list.append(bpsi_data)
                    
            except Exception as e:
                logger.warning(f"Could not read subcorridor BPSI for {d}: {e}")
        
        # Process corridor BPSI
        cor_monthly_bpsi_list = []
        for d in date_range:
            try:
                obj = f"mark/bike_ped_safety_index/bpsi_cor_{d.strftime('%Y-%m-%d')}.parquet"
                # FIX: Pass the function object, not string
                bpsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not bpsi_data.empty:
                    # Remove system columns
                    bpsi_data = bpsi_data.loc[:, ~bpsi_data.columns.str.startswith('__')]
                    bpsi_data['Month'] = d
                    bpsi_data = bpsi_data.rename(columns={'overall_pct': 'bpsi'})
                    cor_monthly_bpsi_list.append(bpsi_data)
                    
            except Exception as e:
                logger.warning(f"Could not read corridor BPSI for {d}: {e}")
        
        if sub_monthly_bpsi_list or cor_monthly_bpsi_list:
            # Combine subcorridor data
            if sub_monthly_bpsi_list:
                sub_monthly_bpsi = pd.concat(sub_monthly_bpsi_list, ignore_index=True)
                sub_monthly_bpsi['Corridor'] = sub_monthly_bpsi['Corridor'].astype('category')
                sub_monthly_bpsi['Subcorridor'] = sub_monthly_bpsi['Subcorridor'].astype('category')
            else:
                sub_monthly_bpsi = pd.DataFrame()
            
            # Combine corridor data
            if cor_monthly_bpsi_list:
                cor_monthly_bpsi = pd.concat(cor_monthly_bpsi_list, ignore_index=True)
                cor_monthly_bpsi['Corridor'] = cor_monthly_bpsi['Corridor'].astype('category')
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
            
            # Rename columns for consistency
            if not sub_monthly_bpsi.empty:
                sub_monthly_bpsi = sub_monthly_bpsi.rename(columns={
                    'Corridor': 'Zone_Group',
                    'Subcorridor': 'Corridor'
                })
                
                # Calculate deltas
                sub_monthly_bpsi = sub_monthly_bpsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                sub_monthly_bpsi['ones'] = np.nan
                sub_monthly_bpsi['delta'] = sub_monthly_bpsi.groupby(['Zone_Group', 'Corridor'])['bpsi'].pct_change()
            
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
            
            # Save results
            if not cor_monthly_bpsi.empty:
                save_data(cor_monthly_bpsi, "cor_monthly_bpsi.pkl")
            if not sub_monthly_bpsi.empty:
                save_data(sub_monthly_bpsi, "sub_monthly_bpsi.pkl")

            # Cleanup memory
            del cor_monthly_bpsi
            del sub_monthly_bpsi
            gc.collect()
            
            logger.info("Bike/Ped safety index processing completed successfully")
        else:
            logger.warning("No BPSI data found for the date range")
        
    except Exception as e:
        logger.error(f"Error in bike/ped safety index processing: {e}")
        logger.error(traceback.format_exc())

def process_relative_speed_index(dates, config_data):
    """Process relative speed index [25 of 29]"""
    logger.info(f"{datetime.now()} Relative Speed Index [25 of 29 (mark1)]")
    
    try:
        date_range = pd.date_range(dates['calcs_start_date'], dates['report_end_date'], freq='MS')
        
        # Process subcorridor RSI
        sub_monthly_rsi_list = []
        for d in date_range:
            try:
                obj = f"mark/relative_speed_index/rsi_sub_{d.strftime('%Y-%m-%d')}.parquet"
                # FIX: Pass the function object, not string
                rsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not rsi_data.empty:
                    # Remove system columns
                    rsi_data = rsi_data.loc[:, ~rsi_data.columns.str.startswith('__')]
                    rsi_data['Month'] = d
                    sub_monthly_rsi_list.append(rsi_data)
                    
            except Exception as e:
                logger.warning(f"Could not read subcorridor RSI for {d}: {e}")
        
        # Process corridor RSI
        cor_monthly_rsi_list = []
        for d in date_range:
            try:
                obj = f"mark/relative_speed_index/rsi_cor_{d.strftime('%Y-%m-%d')}.parquet"
                # FIX: Pass the function object, not string
                rsi_data = s3read_using(pd.read_parquet, bucket=conf.bucket, object=obj)
                
                if not rsi_data.empty:
                    # Remove system columns
                    rsi_data = rsi_data.loc[:, ~rsi_data.columns.str.startswith('__')]
                    rsi_data['Month'] = d
                    cor_monthly_rsi_list.append(rsi_data)
                    
            except Exception as e:
                logger.warning(f"Could not read corridor RSI for {d}: {e}")
        
        if sub_monthly_rsi_list or cor_monthly_rsi_list:
            # Combine subcorridor data
            if sub_monthly_rsi_list:
                sub_monthly_rsi = pd.concat(sub_monthly_rsi_list, ignore_index=True)
                sub_monthly_rsi['Corridor'] = sub_monthly_rsi['Corridor'].astype('category')
                sub_monthly_rsi['Subcorridor'] = sub_monthly_rsi['Subcorridor'].astype('category')
            else:
                sub_monthly_rsi = pd.DataFrame()
            
            # Combine corridor data
            if cor_monthly_rsi_list:
                cor_monthly_rsi = pd.concat(cor_monthly_rsi_list, ignore_index=True)
                cor_monthly_rsi['Corridor'] = cor_monthly_rsi['Corridor'].astype('category')
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
            
            # Rename columns for consistency
            if not sub_monthly_rsi.empty:
                sub_monthly_rsi = sub_monthly_rsi.rename(columns={
                    'Corridor': 'Zone_Group',
                    'Subcorridor': 'Corridor'
                })
                
                # Calculate deltas
                sub_monthly_rsi = sub_monthly_rsi.sort_values(['Zone_Group', 'Corridor', 'Month'])
                sub_monthly_rsi['ones'] = np.nan
                sub_monthly_rsi['delta'] = sub_monthly_rsi.groupby(['Zone_Group', 'Corridor'])['rsi'].pct_change()
            
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
            
            # Save results
            if not cor_monthly_rsi.empty:
                save_data(cor_monthly_rsi, "cor_monthly_rsi.pkl")
            if not sub_monthly_rsi.empty:
                save_data(sub_monthly_rsi, "sub_monthly_rsi.pkl")
            
            # Cleanup memory
            del cor_monthly_rsi
            del sub_monthly_rsi
            gc.collect()
            
            logger.info("Relative speed index processing completed successfully")
        else:
            logger.warning("No RSI data found for the date range")
        
    except Exception as e:
        logger.error(f"Error in relative speed index processing: {e}")
        logger.error(traceback.format_exc())

def process_crash_indices(dates, config_data):
    """Process crash indices [26 of 29]"""
    logger.info(f"{datetime.now()} Crash Indices [26 of 29 (mark1)]")
    
    try:
        # Read crash data - FIX: Pass function object, not string
        crashes = s3read_using(
            pd.read_excel,  # Function object, not string
            bucket=conf.bucket,
            object="Collisions Dataset 2017-2019.xlsm",
            engine='openpyxl'  # Add engine for Excel files
        )
        
        if not crashes.empty:
            # Check if required columns exist
            required_cols = ['Signal_ID_Clean', 'Month', 'crashes_k', 'crashes_a', 'crashes_b',
                           'crashes_c', 'crashes_o', 'crashes_total', 'cost']
            
            # Check which columns actually exist
            available_cols = [col for col in required_cols if col in crashes.columns]
            missing_cols = [col for col in required_cols if col not in crashes.columns]
            
            if missing_cols:
                logger.warning(f"Missing columns in crash data: {missing_cols}")
                logger.info(f"Available columns: {list(crashes.columns)}")
                
                # Try alternative column names
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
                    # Complete VPD data and calculate 12-month rolling average
                    date_range_vpd = pd.date_range(monthly_vpd['Month'].min(), monthly_vpd['Month'].max(), freq='MS')
                    signal_list = monthly_vpd['SignalID'].unique()
                    
                    complete_vpd = pd.MultiIndex.from_product([signal_list, date_range_vpd], names=['SignalID', 'Month']).to_frame(index=False)
                    monthly_vpd = complete_vpd.merge(monthly_vpd, on=['SignalID', 'Month'], how='left')
                    
                    monthly_vpd = monthly_vpd.sort_values(['SignalID', 'Month'])
                    monthly_vpd['vpd12'] = monthly_vpd.groupby('SignalID')['vpd'].transform(
                        lambda x: x.rolling(window=12, min_periods=1).mean()
                    )
                    
                    # Calculate 36-month rolling crashes
                    complete_crashes = pd.MultiIndex.from_product([signal_list, date_range_vpd], names=['SignalID', 'Month']).to_frame(index=False)
                    monthly_36mo_crashes = complete_crashes.merge(crashes, on=['SignalID', 'Month'], how='left')
                    
                    # Fill NaN with 0
                    crash_cols = [col for col in crashes.columns if col.startswith('crashes_') or col == 'cost']
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
                    
                    monthly_36mo_crashes = pd.concat(monthly_36mo_crashes_expanded, ignore_index=True)
                    
                    # Merge crashes and VPD
                    monthly_crashes = monthly_36mo_crashes.merge(monthly_vpd, on=['SignalID', 'Month'], how='outer')
                    
                    # Calculate crash indices (only if required columns exist)
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
                    monthly_kabco_index = get_monthly_avg_by_day(monthly_crashes, "kabco")
                    
                    # Calculate corridor indices
                    cor_monthly_crash_rate_index = get_cor_monthly_avg_by_day(
                        monthly_crash_rate_index, config_data['corridors'], "cri"
                    )
                    cor_monthly_kabco_index = get_cor_monthly_avg_by_day(
                        monthly_kabco_index, config_data['corridors'], "kabco"
                    )
                    
                    # Calculate subcorridor indices
                    sub_monthly_crash_rate_index = get_cor_monthly_avg_by_day(
                        monthly_crash_rate_index, config_data['subcorridors'], "cri"
                    )
                    sub_monthly_kabco_index = get_cor_monthly_avg_by_day(
                        monthly_kabco_index, config_data['subcorridors'], "kabco"
                    )
                    
                    # Save results
                    save_data(monthly_crash_rate_index, "monthly_crash_rate_index.pkl")
                    save_data(monthly_kabco_index, "monthly_kabco_index.pkl")
                    save_data(cor_monthly_crash_rate_index, "cor_monthly_crash_rate_index.pkl")
                    save_data(cor_monthly_kabco_index, "cor_monthly_kabco_index.pkl")
                    save_data(sub_monthly_crash_rate_index, "sub_monthly_crash_rate_index.pkl")
                    save_data(sub_monthly_kabco_index, "sub_monthly_kabco_index.pkl")

                    # Cleanup memory
                    del monthly_crashes
                    del monthly_crash_rate_index
                    del monthly_kabco_index
                    del cor_monthly_crash_rate_index
                    del cor_monthly_kabco_index
                    del sub_monthly_crash_rate_index
                    del sub_monthly_kabco_index
                    gc.collect()
                    
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

def main():
    """Main function to run the monthly report package"""
    logger.info("Starting Monthly Report Package Processing")
    
    try:
        # Initialize configuration and dates
        config_data = initialize_config()
        dates = calculate_dates(config_data['report_end_date'])
        
        logger.info(f"Processing data from {dates['calcs_start_date']} to {dates['report_end_date']}")
        
        # Process each section sequentially
        processing_functions = [
            # (process_detector_uptime, "Vehicle Detector Uptime"),
            # (process_ped_pushbutton_uptime, "Pedestrian Pushbutton Uptime"),
            (process_watchdog_alerts, "Watchdog Alerts"),
            # (process_daily_ped_activations, "Daily Pedestrian Activations"),
            # (process_hourly_ped_activations, "Hourly Pedestrian Activations"),
            # (process_pedestrian_delay, "Pedestrian Delay"),
            # (process_communications_uptime, "Communications Uptime"),
            # (process_daily_volumes, "Daily Volumes"),
            # (process_hourly_volumes, "Hourly Volumes"),
            # (process_daily_throughput, "Daily Throughput"),
            # (process_arrivals_on_green, "Arrivals on Green"),
            # (process_hourly_arrivals_on_green, "Hourly Arrivals on Green"),
            # (process_daily_progression_ratio, "Daily Progression Ratio"),
            # (process_hourly_progression_ratio, "Hourly Progression Ratio"),
            # (process_daily_split_failures, "Daily Split Failures"),
            # (process_hourly_split_failures, "Hourly Split Failures"),
            # (process_daily_queue_spillback, "Daily Queue Spillback"),
            # (process_hourly_queue_spillback, "Hourly Queue Spillback"),
            # (process_travel_time_indexes, "Travel Time Indexes"),
            # (process_cctv_uptime, "CCTV Uptime"),
            # (process_teams_activities, "TEAMS Activities"),
            # (process_user_delay_costs, "User Delay Costs"),
            # (process_flash_events, "Flash Events"),
            # (process_bike_ped_safety_index, "Bike/Ped Safety Index"),
            # (process_relative_speed_index, "Relative Speed Index"),
            # (process_crash_indices, "Crash Indices")
        ]
        
        # Track progress
        total_functions = len(processing_functions)
        
        for i, (func, description) in enumerate(processing_functions, 1):
            try:
                logger.info(f"Starting {description} [{i} of {total_functions}]")
                func(dates, config_data)
                logger.info(f"Completed {description} [{i} of {total_functions}]")
                
            except Exception as e:
                logger.error(f"Failed to process {description}: {e}")
                logger.error(traceback.format_exc())
                # Continue with next function rather than stopping
                continue
        
        logger.info("Monthly Report Package Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Critical error in main processing: {e}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        # Clean up any open connections
        cleanup_connections()

def cleanup_connections():
    """Clean up any open database connections"""
    try:
        # Close any pandas/SQL connections
        import gc
        gc.collect()
        logger.info("Cleaned up connections and memory")
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")

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
    main()






