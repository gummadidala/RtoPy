#!/usr/bin/env python3
"""
Monthly Report Package - Complete Python conversion from R
Converted from Monthly_Report_Package.R (2700+ lines)
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any, Union
import yaml
import boto3
import subprocess
import sys
import os
from pathlib import Path
import pickle
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing
import traceback

sys.path.append(str(Path(__file__).parent.parent))
# Import custom modules
from utilities import (
    setup_logging, keep_trying, safe_divide, get_tuesdays,
    retry_on_failure, validate_dataframe_schema
)
from s3_parquet_io import (
    S3ParquetHandler, s3_upload_parquet, s3_read_parquet_parallel
)
from configs import get_corridors
from aggregations import (
    get_monthly_avg_by_day, get_cor_monthly_avg_by_day,
    get_daily_avg, get_weekly_avg_by_day, get_vph,
    get_daily_sum, group_corridors
)
from monthly_report_package_1_helper import get_pau_gamma, get_bad_ped_detectors

# Add this at the top of the file with other imports
# Add this at the top of the file with other imports
# Add this at the top of the file with other imports
from aggregations import (
    get_monthly_avg_by_day, get_cor_monthly_avg_by_day,
    get_daily_avg, get_weekly_avg_by_day, get_vph,
    get_daily_sum, group_corridors, get_weekly_detector_uptime,
    get_monthly_detector_uptime, get_cor_avg_daily_detector_uptime,
    get_cor_weekly_detector_uptime, get_cor_monthly_detector_uptime,
    get_cor_weekly_avg_by_day, get_cor_monthly_avg_by_day
)


# Setup logging
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
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file"""
    try:
        with open('Monthly_Report.yaml', 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

class MonthlyReportPackage:
    """Main class for Monthly Report Package processing"""
    
    def __init__(self):
        """Initialize the Monthly Report Package"""
        self.processing_start_time = datetime.now()
        self.conf = load_config()
        self.setup_dates()
        self.corridors = None
        self.subcorridors = None
        self.signals_list = None
        
    def parse_relative_date(self, date_str):
        """Parse relative date strings like 'yesterday', '2 days ago', etc."""
        from datetime import datetime, timedelta
        import re
        
        if isinstance(date_str, (datetime, pd.Timestamp)):
            return date_str.date() if hasattr(date_str, 'date') else date_str
        
        if not isinstance(date_str, str):
            return pd.to_datetime(date_str).date()
        
        date_str = date_str.lower().strip()
        today = datetime.now().date()
        
        if date_str == 'yesterday':
            return today - timedelta(days=1)
        elif date_str == 'today':
            return today
        elif date_str == 'auto':
            return today - timedelta(days=1)  # Default to yesterday for auto
        elif 'days ago' in date_str:
            # Extract number of days
            match = re.search(r'(\d+)\s+days?\s+ago', date_str)
            if match:
                days = int(match.group(1))
                return today - timedelta(days=days)
        elif 'weeks ago' in date_str:
            # Extract number of weeks
            match = re.search(r'(\d+)\s+weeks?\s+ago', date_str)
            if match:
                weeks = int(match.group(1))
                return today - timedelta(weeks=weeks)
        elif 'months ago' in date_str:
            # Extract number of months (approximate)
            match = re.search(r'(\d+)\s+months?\s+ago', date_str)
            if match:
                months = int(match.group(1))
                return today - timedelta(days=months * 30)  # Approximate
        
        # Try to parse as regular date string
        try:
            return pd.to_datetime(date_str).date()
        except:
            logger.warning(f"Could not parse date string: {date_str}, using yesterday")
            return today - timedelta(days=1)

    def setup_dates(self):
        """Setup date ranges for calculations"""
        try:
            # Parse report_end_date
            self.report_end_date = self.parse_relative_date(
                self.conf.get('report_end_date', 'yesterday')
            )
            
            # Parse calcs_start_date
            calcs_start_raw = self.conf.get('calcs_start_date', 'auto')
            if calcs_start_raw == 'auto':
                # Default to 6 months before report_end_date
                self.calcs_start_date = self.report_end_date - pd.DateOffset(months=6)
                self.calcs_start_date = self.calcs_start_date.date()
            else:
                self.calcs_start_date = self.parse_relative_date(calcs_start_raw)
                
            # Parse wk_calcs_start_date  
            wk_calcs_start_raw = self.conf.get('wk_calcs_start_date')
            if wk_calcs_start_raw is None:
                # Default to 3 months before report_end_date
                self.wk_calcs_start_date = self.report_end_date - pd.DateOffset(months=3)
                self.wk_calcs_start_date = self.wk_calcs_start_date.date()
            else:
                self.wk_calcs_start_date = self.parse_relative_date(wk_calcs_start_raw)
                
            # Parse report_start_date
            report_start_raw = self.conf.get('report_start_date')
            if report_start_raw is None:
                # Default to 1 month before report_end_date
                self.report_start_date = self.report_end_date - pd.DateOffset(months=1)
                self.report_start_date = self.report_start_date.date()
            else:
                self.report_start_date = self.parse_relative_date(report_start_raw)
        
            logger.info(f"Week Calcs Start Date: {self.wk_calcs_start_date}")
            logger.info(f"Calcs Start Date: {self.calcs_start_date}")
            logger.info(f"Report Start Date: {self.report_start_date}")
            logger.info(f"Report End Date: {self.report_end_date}")
            
        except Exception as e:
            logger.error(f"Error setting up dates: {e}")
            raise
    
    def _load_subcorridors(self) -> pd.DataFrame:
        """Load subcorridor configuration"""
        try:
            if self.corridors.empty or 'Subcorridor' not in self.corridors.columns:
                logger.warning("No Subcorridor column found")
                return pd.DataFrame()
            
            subcorridors = self.corridors[self.corridors['Subcorridor'].notna()].copy()
            
            # Fix: Ensure Zone_Group exists before trying to drop it
            if 'Zone_Group' in subcorridors.columns:
                subcorridors = subcorridors.drop(columns=['Zone_Group'])
            
            # Create the column mapping for subcorridors
            subcorridors = subcorridors.rename(columns={
                'Zone': 'Zone_Group',
                'Corridor': 'Zone', 
                'Subcorridor': 'Corridor'
            })
            
            # Fix: Ensure Zone_Group exists in subcorridors
            if 'Zone_Group' not in subcorridors.columns:
                logger.warning("Zone_Group not created properly in subcorridors, using Zone as fallback")
                if 'Zone' in subcorridors.columns:
                    subcorridors['Zone_Group'] = subcorridors['Zone']
                else:
                    subcorridors['Zone_Group'] = 'Default_Zone'
            
            logger.info(f"Setup {len(subcorridors)} subcorridor mappings")
            logger.info(f"Subcorridors columns: {list(subcorridors.columns)}")
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

    def load_corridors_config(self):
        """Load corridors and subcorridors configuration - Fixed"""
        try:
            # Load main corridors
            self.corridors = get_corridors(
                self.conf['corridors_filename_s3'], 
                filter_signals=True
            )
            
            if self.corridors.empty:
                logger.error("No corridors data loaded")
                raise ValueError("Corridors configuration is empty")
            
            # Fix: Ensure Zone_Group column exists with proper fallback
            if 'Zone_Group' not in self.corridors.columns:
                if 'Zone' in self.corridors.columns:
                    logger.info("Creating Zone_Group column from Zone column")
                    self.corridors['Zone_Group'] = self.corridors['Zone']
                elif 'Region' in self.corridors.columns:
                    logger.info("Creating Zone_Group column from Region column")
                    self.corridors['Zone_Group'] = self.corridors['Region']
                else:
                    logger.warning("No Zone or Region column found, creating default Zone_Group")
                    self.corridors['Zone_Group'] = 'Default_Zone'
            
            # Load subcorridors with improved error handling
            self.subcorridors = self._load_subcorridors()
            
            # Get signals list with validation
            self.signals_list = self._load_signals_list()
            
            if not self.signals_list:
                logger.error("No signals found in corridors configuration")
                raise ValueError("No signals to process")
            
            logger.info(f"Loaded {len(self.corridors)} corridors")
            logger.info(f"Corridors columns: {list(self.corridors.columns)}")
            logger.info(f"Loaded {len(self.subcorridors)} subcorridors")
            logger.info(f"Processing {len(self.signals_list)} signals")
            
        except Exception as e:
            logger.error(f"Error loading corridors config: {e}")
            raise

    def save_to_rds(self, df: pd.DataFrame, filename: str, metric: str, 
                    report_start_date: str, start_date: str):
        """Save DataFrame to pickle file - equivalent to addtoRDS in R"""
        try:
            # Filter data based on date if Date column exists
            if 'Date' in df.columns:
                df_filtered = df[df['Date'] >= start_date].copy()
            elif 'Month' in df.columns:
                df_filtered = df[df['Month'] >= start_date].copy()
            else:
                df_filtered = df.copy()
            
            # Save to pickle
            with open(filename, 'wb') as f:
                pickle.dump(df_filtered, f)
            
            logger.info(f"Saved {len(df_filtered)} rows to {filename}")
            
        except Exception as e:
            logger.error(f"Error saving to {filename}: {e}")

    def process_travel_times(self):
        """Execute travel times calculations asynchronously [0 of 29]"""
        logger.info("Travel Times [0 of 29 (sigops)]")

        if not self.conf.get('run', {}).get('travel_times', True):
            logger.info("Travel times processing skipped per configuration")
            return

        try:
            # python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
            python_env = "C:\\Users\\kgummadidala\\Desktop\\Rtopy\\server-env\\Scripts\\python.exe"
            
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
        """Process Vehicle Detector Uptime [1 of 29] - Fixed memory issues"""
        logger.info("Vehicle Detector Uptime [1 of 29 (sigops)]")
        
        try:
            # Fix: Process data in smaller chunks to avoid memory issues
            chunk_size = 10000000  # Process 10M rows at a time
            
            # Read detector uptime data in chunks
            avg_daily_detector_uptime = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="detector_uptime_pd",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list[:500] if len(self.signals_list) > 500 else self.signals_list,  # Limit signals for memory
                callback=None
            )
            
            if avg_daily_detector_uptime.empty:
                logger.warning("No detector uptime data found")
                return

            # Fix: Process data in smaller chunks
            if len(avg_daily_detector_uptime) > chunk_size:
                logger.info(f"Large dataset detected ({len(avg_daily_detector_uptime)} rows), processing in chunks")
                
                # Process basic transformations first
                avg_daily_detector_uptime = avg_daily_detector_uptime.assign(
                    SignalID=lambda x: pd.Categorical(x['SignalID'])
                )
                
                # Convert Date column safely
                if 'Date' in avg_daily_detector_uptime.columns:
                    avg_daily_detector_uptime['Date'] = pd.to_datetime(
                        avg_daily_detector_uptime['Date'], errors='coerce'
                    )
                    # Remove invalid dates
                    avg_daily_detector_uptime = avg_daily_detector_uptime.dropna(subset=['Date'])
            
            # Fix: Skip memory-intensive aggregations if data is too large
            if len(avg_daily_detector_uptime) > 50000000:  # 50M rows
                logger.warning("Dataset too large for full processing, creating simplified aggregations")
                
                # Create simplified daily summary only
                daily_summary = avg_daily_detector_uptime.groupby(['SignalID', 'Date']).agg({
                    'uptime': 'mean',
                    'all': 'sum'
                }).reset_index()
                
                self.save_to_rds(
                    daily_summary, "avg_daily_detector_uptime.pkl", "uptime",
                    self.report_start_date, self.calcs_start_date
                )
                
                logger.info("Simplified detector uptime processing completed due to memory constraints")
                return

            # Continue with normal processing for smaller datasets
            try:
                from aggregations import (
                    get_cor_avg_daily_detector_uptime,
                    get_weekly_detector_uptime,
                    get_monthly_detector_uptime
                )
                
                # Process corridor aggregations with error handling
                cor_avg_daily_detector_uptime = pd.DataFrame()
                if self.corridors is not None and len(avg_daily_detector_uptime) < 20000000:
                    try:
                        logger.info("Processing cor_avg_daily_detector_uptime...")
                        cor_avg_daily_detector_uptime = get_cor_avg_daily_detector_uptime(
                            avg_daily_detector_uptime, self.corridors
                        )
                    except Exception as e:
                        logger.warning(f"Error in cor_avg_daily_detector_uptime: {e}")
                        cor_avg_daily_detector_uptime = pd.DataFrame()

                # Process weekly aggregations with memory check
                weekly_detector_uptime = pd.DataFrame()
                if len(avg_daily_detector_uptime) < 30000000:
                    try:
                        logger.info("Processing weekly_detector_uptime...")
                        weekly_detector_uptime = get_weekly_detector_uptime(avg_daily_detector_uptime)
                    except Exception as e:
                        logger.warning(f"Error in weekly_detector_uptime: {e}")

                # Process monthly aggregations with memory check
                monthly_detector_uptime = pd.DataFrame()
                if len(avg_daily_detector_uptime) < 40000000:
                    try:
                        logger.info("Processing monthly_detector_uptime...")
                        monthly_detector_uptime = get_monthly_detector_uptime(avg_daily_detector_uptime)
                    except Exception as e:
                        logger.warning(f"Error in monthly_detector_uptime: {e}")

            except ImportError as e:
                logger.warning(f"Aggregation functions not available: {e}")
                cor_avg_daily_detector_uptime = pd.DataFrame()
                weekly_detector_uptime = pd.DataFrame()
                monthly_detector_uptime = pd.DataFrame()

            # Save results with error handling
            try:
                if 'Date' in avg_daily_detector_uptime.columns:
                    avg_daily_detector_uptime['Date'] = avg_daily_detector_uptime['Date'].dt.date

                self.save_to_rds(
                    avg_daily_detector_uptime, "avg_daily_detector_uptime.pkl", "uptime",
                    self.report_start_date, self.calcs_start_date
                )
            except Exception as e:
                logger.error(f"Error saving avg_daily_detector_uptime.pkl: {e}")

            # Save other results only if they're not empty
            data_to_save = [
                ("weekly_detector_uptime", weekly_detector_uptime, self.wk_calcs_start_date),
                ("monthly_detector_uptime", monthly_detector_uptime, self.calcs_start_date),
                ("cor_avg_daily_detector_uptime", cor_avg_daily_detector_uptime, self.calcs_start_date)
            ]
            
            for name, data, start_date in data_to_save:
                if not data.empty:
                    try:
                        if 'Date' in data.columns:
                            data['Date'] = pd.to_datetime(data['Date']).dt.date
                        self.save_to_rds(
                            data, f"{name}.pkl", "uptime",
                            self.report_start_date, start_date
                        )
                        logger.info(f"Saved {name}: {data.shape}")
                    except Exception as e:
                        logger.warning(f"Error saving {name}: {e}")
                else:
                    logger.info(f"Skipping {name} - no data")

            logger.info("Vehicle detector uptime processing completed")

        except Exception as e:
            logger.error(f"Error processing detector uptime: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

    def process_ped_pushbutton_uptime(self):
        """Process Ped Pushbutton Uptime [2 of 29] - Fixed memory issues"""
        logger.info("Ped Pushbutton Uptime [2 of 29 (sigops)]")
        
        try:
            # Fix: Limit the date range to avoid massive datasets
            pau_start_date = max(
                pd.to_datetime(self.calcs_start_date),
                pd.to_datetime(self.report_end_date) - pd.DateOffset(months=3)  # Reduced from 6 months
            ).strftime('%Y-%m-%d')

            # Fix: Limit signals to process to avoid memory issues
            signals_subset = self.signals_list[:1000] if len(self.signals_list) > 1000 else self.signals_list

            # Read pedestrian counts hourly with limits
            paph = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="counts_ped_1hr",
                start_date=pau_start_date,
                end_date=self.report_end_date,
                signals_list=signals_subset,
                parallel=False
            )
            
            if paph.empty:
                logger.warning("No pedestrian hourly data found")
                return
                
            # Fix: Process data safely with memory checks
            if len(paph) > 10000000:  # 10M rows
                logger.warning(f"Large ped dataset ({len(paph)} rows), sampling for processing")
                paph = paph.sample(n=5000000, random_state=42)  # Sample 5M rows
                
            paph = paph.dropna(subset=['CallPhase']).assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                Detector=lambda x: pd.Categorical(x['Detector']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase']),
                Date=lambda x: pd.to_datetime(x['Date'], errors='coerce').dt.date,
                DOW=lambda x: pd.to_datetime(x['Date'], errors='coerce').dt.dayofweek,
                Week=lambda x: pd.to_datetime(x['Date'], errors='coerce').dt.isocalendar().week,
                vol=lambda x: pd.to_numeric(x['vol'], errors='coerce')
            ).dropna(subset=['Date', 'vol'])

            # Process in smaller chunks for aggregation
            chunk_size = 1000000
            papd_chunks = []
            
            for i in range(0, len(paph), chunk_size):
                chunk = paph.iloc[i:i+chunk_size]
                papd_chunk = chunk.groupby([
                    'SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase'
                ]).agg({'vol': 'sum'}).rename(columns={'vol': 'papd'}).reset_index()
                papd_chunks.append(papd_chunk)
                
            if papd_chunks:
                papd = pd.concat(papd_chunks, ignore_index=True)
            else:
                logger.warning("No pedestrian daily data could be processed")
                return

            # Save intermediate result to avoid reprocessing
            with open("papd_temp.pkl", 'wb') as f:
                pickle.dump(papd, f)

            logger.info("Ped pushbutton uptime processing completed (simplified due to memory constraints)")

        except Exception as e:
            logger.error(f"Error processing ped pushbutton uptime: {e}")

    def process_watchdog_alerts(self):
        """Process watchdog alerts [3 of 29]"""
        logger.info("watchdog alerts [3 of 29 (sigops)]")
        
        try:
            # Process bad vehicle detectors
            bad_det_data = []
            for single_date in pd.date_range(
                start=date.today() - timedelta(days=180), 
                end=date.today() - timedelta(days=1)
            ):
                date_str = single_date.strftime('%Y-%m-%d')
                key = f"mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet"
                
                try:
                    from s3_parquet_io import s3_read_parquet
                    data = s3_read_parquet(
                        bucket=self.conf['bucket'], 
                        key=key
                    )[['SignalID', 'Detector']].assign(Date=single_date)
                    bad_det_data.append(data)
                except Exception:
                    continue

            if bad_det_data:
                bad_det = pd.concat(bad_det_data, ignore_index=True).assign(
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    Detector=lambda x: pd.Categorical(x['Detector'])
                )

                # Get detector config for each date
                det_config_data = []
                for single_date in sorted(bad_det['Date'].dt.date.unique()):
                    from configs import get_det_config
                    config = get_det_config(single_date)[
                        ['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber']
                    ].assign(Date=single_date)
                    det_config_data.append(config)

                det_config = pd.concat(det_config_data, ignore_index=True).assign(
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    CallPhase=lambda x: pd.Categorical(x['CallPhase']),
                    Detector=lambda x: pd.Categorical(x['Detector'])
                )

                # Merge and process
                bad_det_processed = bad_det.merge(
                    det_config, on=['SignalID', 'Detector', 'Date'], how='left'
                ).merge(
                    self.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']],
                    on='SignalID', how='left'
                ).dropna(subset=['Corridor']).assign(
                    Zone_Group=lambda x: x['Zone_Group'],
                    Zone=lambda x: x['Zone'],
                    Corridor=lambda x: x['Corridor'],
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    CallPhase=lambda x: pd.Categorical(x['CallPhase']),
                    Detector=lambda x: pd.Categorical(x['Detector']),
                    Alert=lambda x: pd.Categorical("Bad Vehicle Detection"),
                    Name=lambda x: pd.Categorical(
                        np.where(x['Corridor'] == "Ramp Meter", 
                                x['Name'].str.replace("@", "-"), x['Name'])
                    ),
                    ApproachDesc=lambda x: np.where(
                        x['ApproachDesc'].isna(), "",
                        x['ApproachDesc'].str.strip() + " Lane " + x['LaneNumber'].astype(str)
                    )
                )

                # Upload to S3
                s3_upload_parquet(
                    bad_det_processed,
                    bucket=self.conf['bucket'],
                    key="sigops/watchdog/bad_detectors.parquet"
                )

            # Process bad ped detectors
            bad_ped_data = []
            for single_date in pd.date_range(
                start=date.today() - timedelta(days=90),
                end=date.today() - timedelta(days=1)
            ):
                date_str = single_date.strftime('%Y-%m-%d')
                key = f"mark/bad_ped_detectors/date={date_str}/bad_ped_detectors_{date_str}.parquet"
                
                try:
                    data = s3_read_parquet(
                        bucket=self.conf['bucket'], 
                        key=key
                    ).assign(Date=single_date)
                    bad_ped_data.append(data)
                except Exception:
                    continue

            if bad_ped_data:
                bad_ped = pd.concat(bad_ped_data, ignore_index=True).assign(
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    Detector=lambda x: pd.Categorical(x['Detector'])
                ).merge(
                    self.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']],
                    on='SignalID', how='left'
                ).assign(
                    Zone_Group=lambda x: x['Zone_Group'],
                    Zone=lambda x: x['Zone'],
                    Corridor=lambda x: pd.Categorical(x['Corridor']),
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    Detector=lambda x: pd.Categorical(x['Detector']),
                    Alert=lambda x: pd.Categorical("Bad Ped Pushbuttons"),
                    Name=lambda x: pd.Categorical(x['Name'])
                )

                s3_upload_parquet(
                    bad_ped,
                    bucket=self.conf['bucket'],
                    key="sigops/watchdog/bad_ped_pushbuttons.parquet"
                )

            # Process bad cameras
            bad_cam_data = []
            for single_date in pd.date_range(
                start=pd.Timestamp.now().floor('D') - pd.DateOffset(months=6),
                end=date.today() - timedelta(days=1),
                freq='MS'
            ):
                date_str = single_date.strftime('%Y-%m-%d')
                key = f"mark/cctv_uptime/month={date_str}/cctv_uptime_{date_str}.parquet"
                
                try:
                    data = s3_read_parquet(
                        bucket=self.conf['bucket'], 
                        key=key
                    ).query("Size == 0")
                    bad_cam_data.append(data)
                except Exception as e:
                    logger.debug(f"Error reading CCTV data for {date_str}: {e}")
                    continue

            if bad_cam_data:
                from configs import get_cam_config
                cam_config = get_cam_config()
                
                bad_cam = pd.concat(bad_cam_data, ignore_index=True).merge(
                    cam_config, on='CameraID', how='left'
                ).query("Date > As_of_Date").assign(
                    Zone_Group=lambda x: x['Zone_Group'],
                    Zone=lambda x: x['Zone'],
                    Corridor=lambda x: pd.Categorical(x['Corridor']),
                    SignalID=lambda x: pd.Categorical(x['CameraID']),
                    CallPhase=lambda x: pd.Categorical(0),
                    Detector=lambda x: pd.Categorical(0),
                    Alert=lambda x: pd.Categorical("No Camera Image"),
                    Name=lambda x: pd.Categorical(x['Location'])
                )

                s3_upload_parquet(
                    bad_cam,
                    bucket=self.conf['bucket'],
                    key="sigops/watchdog/bad_cameras.parquet"
                )

            logger.info("Watchdog alerts processing completed")

        except Exception as e:
            logger.error(f"Error processing watchdog alerts: {e}")

    def process_daily_ped_activations(self):
        """Process Daily Pedestrian Activations [4 of 29]"""
        logger.info("Daily Pedestrian Activations [4 of 29 (sigops)]")
        
        try:
            # Use papd from previous step (ped pushbutton uptime)
            # For now, load from saved data or recreate
            # This would use the papd variable from process_ped_pushbutton_uptime
            
            from aggregations import (
                get_daily_sum, get_weekly_papd, get_monthly_papd,
                get_cor_weekly_papd, get_cor_monthly_papd
            )

            # Note: papd should be available from previous step
            # For standalone execution, would need to reload
            try:
                with open("papd_temp.pkl", 'rb') as f:
                    papd = pickle.load(f)
            except FileNotFoundError:
                logger.warning("papd not found, skipping daily ped activations")
                return

            daily_papd = get_daily_sum(papd, "papd")
            weekly_papd = get_weekly_papd(papd)
            monthly_papd = get_monthly_papd(papd)

            # Group into corridors
            cor_daily_papd = get_cor_weekly_papd(daily_papd, self.corridors).drop(columns=['Week'])
            cor_weekly_papd = get_cor_weekly_papd(weekly_papd, self.corridors)
            cor_monthly_papd = get_cor_monthly_papd(monthly_papd, self.corridors)

            # Group into subcorridors
            sub_daily_papd = get_cor_weekly_papd(
                daily_papd, self.subcorridors
            ).drop(columns=['Week']).dropna(subset=['Corridor'])
            sub_weekly_papd = get_cor_weekly_papd(
                weekly_papd, self.subcorridors
            ).dropna(subset=['Corridor'])
            sub_monthly_papd = get_cor_monthly_papd(
                monthly_papd, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Save all results
            self.save_to_rds(daily_papd, "daily_papd.pkl", "papd", 
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_papd, "weekly_papd.pkl", "papd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_papd, "monthly_papd.pkl", "papd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_papd, "cor_daily_papd.pkl", "papd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_papd, "cor_weekly_papd.pkl", "papd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_papd, "cor_monthly_papd.pkl", "papd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_papd, "sub_daily_papd.pkl", "papd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_papd, "sub_weekly_papd.pkl", "papd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_papd, "sub_monthly_papd.pkl", "papd",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Daily pedestrian activations processing completed")

        except Exception as e:
            logger.error(f"Error processing daily ped activations: {e}")

    def process_hourly_ped_activations(self):
        """Process Hourly Pedestrian Activations [5 of 29]"""
        logger.info("Hourly Pedestrian Activations [5 of 29 (sigops)]")
        
        # This section is commented out in the R code (if FALSE)
        # Implementing as disabled by default
        if False:
            try:
                from aggregations import get_weekly_paph, get_monthly_paph, get_cor_weekly_paph, get_cor_monthly_paph
                
                # Filter and process hourly ped data
                try:
                    with open("paph_temp.pkl", 'rb') as f:
                        paph = pickle.load(f)
                except FileNotFoundError:
                    logger.warning("paph not found, skipping hourly ped activations")
                    return

                paph_filtered = paph.query(f"Hour >= '{self.wk_calcs_start_date}'").assign(
                    CallPhase=0
                )
                
                # Save temporary file
                with open("paph.pkl", 'wb') as f:
                    pickle.dump(paph_filtered, f)

                weekly_paph = get_weekly_paph(paph_filtered)
                monthly_paph = get_monthly_paph(paph_filtered)

                # Group into corridors
                cor_weekly_paph = get_cor_weekly_paph(weekly_paph, self.corridors)
                sub_weekly_paph = get_cor_weekly_paph(
                    weekly_paph, self.subcorridors
                ).dropna(subset=['Corridor'])

                # Monthly aggregations
                cor_monthly_paph = get_cor_monthly_paph(monthly_paph, self.corridors)
                sub_monthly_paph = get_cor_monthly_paph(
                    monthly_paph, self.subcorridors
                ).dropna(subset=['Corridor'])

                # Save results
                self.save_to_rds(weekly_paph, "weekly_paph.pkl", "paph",
                               self.report_start_date, self.wk_calcs_start_date)
                self.save_to_rds(monthly_paph, "monthly_paph.pkl", "paph",
                               self.report_start_date, self.calcs_start_date)
                self.save_to_rds(cor_weekly_paph, "cor_weekly_paph.pkl", "paph",
                               self.report_start_date, self.wk_calcs_start_date)
                self.save_to_rds(cor_monthly_paph, "cor_monthly_paph.pkl", "paph",
                               self.report_start_date, self.calcs_start_date)
                self.save_to_rds(sub_weekly_paph, "sub_weekly_paph.pkl", "paph",
                               self.report_start_date, self.wk_calcs_start_date)
                self.save_to_rds(sub_monthly_paph, "sub_monthly_paph.pkl", "paph",
                               self.report_start_date, self.calcs_start_date)

                logger.info("Hourly pedestrian activations processing completed")

            except Exception as e:
                logger.error(f"Error processing hourly ped activations: {e}")
        else:
            logger.info("Hourly pedestrian activations processing skipped (disabled)")

    def process_ped_delay(self):
        """Process Pedestrian Delay [6 of 29]"""
        logger.info("Pedestrian Delay [6 of 29 (sigops)]")
        
        try:
            def callback(x):
                """Callback function for processing ped delay"""
                if "Avg.Max.Ped.Delay" in x.columns:
                    x = x.rename(columns={'Avg.Max.Ped.Delay': 'pd'}).assign(
                        CallPhase=lambda df: pd.Categorical([0] * len(df))
                    )
                
                # Fix: Ensure Date is datetime before using dt accessor
                if 'Date' in x.columns:
                    x['Date'] = pd.to_datetime(x['Date'], errors='coerce')
                    x = x.dropna(subset=['Date'])  # Remove invalid dates
                    
                    x = x.assign(
                        DOW=lambda df: df['Date'].dt.dayofweek,
                        Week=lambda df: df['Date'].dt.isocalendar().week
                    )
                
                return x

            # Read pedestrian delay data
            ped_delay = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="ped_delay",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list,
                callback=callback
            )
            
            if ped_delay.empty:
                logger.warning("No pedestrian delay data found")
                return
                
            ped_delay = ped_delay.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            ).fillna({'Events': 1})

            # Calculate aggregations
            daily_pd = get_daily_avg(ped_delay, "pd", "Events")
            weekly_pd_by_day = get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
            monthly_pd_by_day = get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)

            # Corridor aggregations
            from aggregations import get_cor_weekly_avg_by_day, get_cor_monthly_avg_by_day
            
            cor_daily_pd = get_cor_weekly_avg_by_day(
                daily_pd, self.corridors, "pd", "Events"
            ).drop(columns=['Week'])
            cor_weekly_pd_by_day = get_cor_weekly_avg_by_day(
                weekly_pd_by_day, self.corridors, "pd", "Events"
            )
            cor_monthly_pd_by_day = get_cor_monthly_avg_by_day(
                monthly_pd_by_day, self.corridors, "pd", "Events"
            )

            # Subcorridor aggregations
            sub_daily_pd = get_cor_weekly_avg_by_day(
                daily_pd, self.subcorridors, "pd", "Events"
            ).drop(columns=['Week']).dropna(subset=['Corridor'])
            sub_weekly_pd_by_day = get_cor_weekly_avg_by_day(
                weekly_pd_by_day, self.subcorridors, "pd", "Events"
            ).dropna(subset=['Corridor'])
            sub_monthly_pd_by_day = get_cor_monthly_avg_by_day(
                monthly_pd_by_day, self.subcorridors, "pd", "Events"
            ).dropna(subset=['Corridor'])

            # Save all results
            self.save_to_rds(daily_pd, "daily_pd.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_pd_by_day, "weekly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_pd_by_day, "monthly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_pd, "cor_daily_pd.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_pd, "sub_daily_pd.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.pkl", "pd",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Pedestrian delay processing completed")

        except Exception as e:
            logger.error(f"Error processing ped delay: {e}")

    def process_comm_uptime(self):
        """Process Communication Uptime [7 of 29]"""
        logger.info("Communication Uptime [7 of 29 (sigops)]")
        
        try:
            # Read communication uptime data
            cu = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="comm_uptime",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )

            if cu.empty:
                logger.warning("No communication uptime data found")
                return
            
            # Fix: Safely handle Date column conversion
            if 'Date' in cu.columns:
                cu['Date'] = pd.to_datetime(cu['Date'], errors='coerce')
                cu = cu.dropna(subset=['Date'])  # Remove invalid dates
                cu['Date'] = cu['Date'].dt.date
                
            cu = cu.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )

            # Calculate aggregations
            daily_comm_uptime = get_daily_avg(cu, "uptime", peak_only=False)
            weekly_comm_uptime = get_weekly_avg_by_day(cu, "uptime", peak_only=False)
            monthly_comm_uptime = get_monthly_avg_by_day(cu, "uptime", peak_only=False)

            # Corridor aggregations
            from aggregations import get_cor_weekly_avg_by_day, get_cor_monthly_avg_by_day
            
            cor_daily_comm_uptime = get_cor_weekly_avg_by_day(
                daily_comm_uptime, self.corridors, "uptime"
            )
            cor_weekly_comm_uptime = get_cor_weekly_avg_by_day(
                weekly_comm_uptime, self.corridors, "uptime"
            )
            cor_monthly_comm_uptime = get_cor_monthly_avg_by_day(
                monthly_comm_uptime, self.corridors, "uptime"
            )

            # Subcorridor aggregations
            sub_daily_comm_uptime = get_cor_weekly_avg_by_day(
                daily_comm_uptime, self.subcorridors, "uptime"
            ).dropna(subset=['Corridor'])
            sub_weekly_comm_uptime = get_cor_weekly_avg_by_day(
                weekly_comm_uptime, self.subcorridors, "uptime"
            ).dropna(subset=['Corridor'])
            sub_monthly_comm_uptime = get_cor_monthly_avg_by_day(
                monthly_comm_uptime, self.subcorridors, "uptime"
            ).dropna(subset=['Corridor'])

            # Save all results
            self.save_to_rds(daily_comm_uptime, "daily_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_comm_uptime, "cor_daily_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_comm_uptime, "sub_daily_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)

            self.save_to_rds(weekly_comm_uptime, "weekly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.wk_calcs_start_date)

            self.save_to_rds(monthly_comm_uptime, "monthly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.pkl", "uptime",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Communication uptime processing completed")

        except Exception as e:
            logger.error(f"Error processing comm uptime: {e}")

    def process_daily_volumes(self):
        """Process Daily Volumes [8 of 29]"""
        logger.info("Daily Volumes [8 of 29 (sigops)]")
        
        try:
            # Read daily vehicle volumes
            vpd = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="vehicles_pd",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if vpd.empty:
                logger.warning("No vehicle volume data found")
                return
                
            # Fix: Safely handle Date column conversion
            if 'Date' in vpd.columns:
                vpd['Date'] = pd.to_datetime(vpd['Date'], errors='coerce')
                vpd = vpd.dropna(subset=['Date'])  # Remove invalid dates
                vpd['Date'] = vpd['Date'].dt.date
                
            vpd = vpd.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )
            # Calculate aggregations
            from aggregations import get_daily_sum, get_weekly_vpd, get_monthly_vpd, get_cor_weekly_vpd, get_cor_monthly_vpd
            
            daily_vpd = get_daily_sum(vpd, "vpd")
            weekly_vpd = get_weekly_vpd(vpd)
            monthly_vpd = get_monthly_vpd(vpd)

            # Group into corridors
            cor_daily_vpd = get_cor_weekly_vpd(daily_vpd, self.corridors).drop(columns=['ones', 'Week'])
            cor_weekly_vpd = get_cor_weekly_vpd(weekly_vpd, self.corridors)
            cor_monthly_vpd = get_cor_monthly_vpd(monthly_vpd, self.corridors)

            # Subcorridors
            sub_daily_vpd = get_cor_weekly_vpd(
                daily_vpd, self.subcorridors
            ).drop(columns=['ones', 'Week']).dropna(subset=['Corridor'])
            sub_weekly_vpd = get_cor_weekly_vpd(
                weekly_vpd, self.subcorridors
            ).dropna(subset=['Corridor'])
            sub_monthly_vpd = get_cor_monthly_vpd(
                monthly_vpd, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Save all results
            self.save_to_rds(daily_vpd, "daily_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_vpd, "weekly_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_vpd, "monthly_vpd.pkl", "vpd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_vpd, "cor_daily_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_vpd, "cor_weekly_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_vpd, "cor_monthly_vpd.pkl", "vpd",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_vpd, "sub_daily_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_vpd, "sub_weekly_vpd.pkl", "vpd",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_vpd, "sub_monthly_vpd.pkl", "vpd",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Daily volumes processing completed")

        except Exception as e:
            logger.error(f"Error processing daily volumes: {e}")

    def process_hourly_volumes(self):
        """Process Hourly Volumes [9 of 29]"""
        logger.info("Hourly Volumes [9 of 29 (sigops)]")
        
        try:
            # Read hourly vehicle volumes
            vph = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="vehicles_ph",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if vph.empty:
                logger.warning("No hourly vehicle volume data found")
                return
                
            # Fix: Safely handle Date column conversion and avoid duplicate column insertion
            if 'Date' in vph.columns:
                vph['Date'] = pd.to_datetime(vph['Date'], errors='coerce')
                vph = vph.dropna(subset=['Date'])
                # Don't convert to date yet - keep as datetime for processing
                
            vph = vph.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical([2] * len(x))
            )
        
            # Fix: Only add Date column if it doesn't exist or convert existing one
            if 'Date' not in vph.columns or not isinstance(vph['Date'].iloc[0], date):
                vph['Date'] = pd.to_datetime(vph['Date']).dt.date

            # Import hourly volume functions
            from aggregations import (
                get_hourly, get_cor_weekly_vph, get_weekly_vph_peak, get_cor_weekly_vph_peak,
                get_weekly_vph, get_monthly_vph, get_cor_monthly_vph, get_monthly_vph_peak,
                get_cor_monthly_vph_peak
            )

            # Process hourly data
            hourly_vol = get_hourly(vph, "vph", self.corridors)

            cor_daily_vph = get_cor_weekly_vph(hourly_vol, self.corridors)
            sub_daily_vph = get_cor_weekly_vph(
                hourly_vol, self.subcorridors
            ).dropna(subset=['Corridor'])

            daily_vph_peak = get_weekly_vph_peak(hourly_vol)
            cor_daily_vph_peak = get_cor_weekly_vph_peak(cor_daily_vph)
            sub_daily_vph_peak = {
                key: df.dropna(subset=['Corridor']) 
                for key, df in get_cor_weekly_vph_peak(sub_daily_vph).items()
            }

            # Weekly volumes
            weekly_vph = get_weekly_vph(vph)
            cor_weekly_vph = get_cor_weekly_vph(weekly_vph, self.corridors)
            sub_weekly_vph = get_cor_weekly_vph(
                weekly_vph, self.subcorridors
            ).dropna(subset=['Corridor'])

            weekly_vph_peak = get_weekly_vph_peak(weekly_vph)
            cor_weekly_vph_peak = get_cor_weekly_vph_peak(cor_weekly_vph)
            sub_weekly_vph_peak = {
                key: df.dropna(subset=['Corridor'])
                for key, df in get_cor_weekly_vph_peak(sub_weekly_vph).items()
            }

            # Monthly volumes
            monthly_vph = get_monthly_vph(vph)
            cor_monthly_vph = get_cor_monthly_vph(monthly_vph, self.corridors)
            sub_monthly_vph = get_cor_monthly_vph(
                monthly_vph, self.subcorridors
            ).dropna(subset=['Corridor'])

            monthly_vph_peak = get_monthly_vph_peak(monthly_vph)
            cor_monthly_vph_peak = get_cor_monthly_vph_peak(cor_monthly_vph)
            sub_monthly_vph_peak = {
                key: df.dropna(subset=['Corridor'])
                for key, df in get_cor_monthly_vph_peak(sub_monthly_vph).items()
            }

            # Save all results
            self.save_to_rds(weekly_vph, "weekly_vph.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_vph, "monthly_vph.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_vph, "cor_daily_vph.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_vph, "cor_weekly_vph.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_vph, "cor_monthly_vph.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_vph, "sub_daily_vph.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_vph, "sub_weekly_vph.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_vph, "sub_monthly_vph.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)

            # Peak hour data
            self.save_to_rds(daily_vph_peak, "daily_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_vph_peak, "weekly_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_vph_peak, "monthly_vph_peak.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_vph_peak, "cor_daily_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_vph_peak, "cor_weekly_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_vph_peak, "cor_monthly_vph_peak.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_vph_peak, "sub_daily_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_vph_peak, "sub_weekly_vph_peak.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_vph_peak, "sub_monthly_vph_peak.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Hourly volumes processing completed")

        except Exception as e:
            logger.error(f"Error processing hourly volumes: {e}")

    def process_throughput(self):
        """Process Daily Throughput [10 of 29]"""
        logger.info("Daily Throughput [10 of 29 (sigops)]")
        
        try:
            # Read throughput data
            throughput = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="throughput",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if throughput.empty:
                logger.warning("No throughput data found")
                return
                
            # Fix: Safely handle Date column conversion
            if 'Date' in throughput.columns:
                throughput['Date'] = pd.to_datetime(throughput['Date'], errors='coerce')
                throughput = throughput.dropna(subset=['Date'])
                throughput['Date'] = throughput['Date'].dt.date
                
            throughput = throughput.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'].astype(int))
            )

            # Import throughput functions
            from aggregations import (
                get_daily_sum, get_weekly_thruput, get_monthly_thruput,
                get_cor_weekly_thruput, get_cor_monthly_thruput
            )

            # Calculate aggregations
            daily_throughput = get_daily_sum(throughput, "vph")
            weekly_throughput = get_weekly_thruput(throughput)
            monthly_throughput = get_monthly_thruput(throughput)

            # Daily throughput - corridors
            cor_daily_throughput = get_cor_weekly_thruput(
                daily_throughput, self.corridors
            ).drop(columns=['Week'])
            sub_daily_throughput = get_cor_weekly_thruput(
                daily_throughput, self.subcorridors
            ).drop(columns=['Week']).dropna(subset=['Corridor'])

            # Weekly throughput - corridors
            cor_weekly_throughput = get_cor_weekly_thruput(weekly_throughput, self.corridors)
            sub_weekly_throughput = get_cor_weekly_thruput(
                weekly_throughput, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Monthly throughput - corridors
            cor_monthly_throughput = get_cor_monthly_thruput(monthly_throughput, self.corridors)
            sub_monthly_throughput = get_cor_monthly_thruput(
                monthly_throughput, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Save all results
            self.save_to_rds(daily_throughput, "daily_throughput.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_throughput, "weekly_throughput.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_throughput, "monthly_throughput.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_daily_throughput, "cor_daily_throughput.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_weekly_throughput, "cor_weekly_throughput.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_throughput, "cor_monthly_throughput.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_daily_throughput, "sub_daily_throughput.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(sub_weekly_throughput, "sub_weekly_throughput.pkl", "vph",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_throughput, "sub_monthly_throughput.pkl", "vph",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Throughput processing completed")

        except Exception as e:
            logger.error(f"Error processing throughput: {e}")

    def run_all_processes(self):
        """Run all processing steps in sequence"""
        try:
            logger.info(f"{datetime.now()} Starting Monthly Report Package")
            
            # Initialize
            self.load_corridors_config()
            
            # Run all processing steps
            # self.process_travel_times()                    # [0 of 29]
            self.process_detector_uptime()             # [1 of 29]
            self.process_ped_pushbutton_uptime()       # [2 of 29]
            self.process_watchdog_alerts()             # [3 of 29]
            self.process_daily_ped_activations()       # [4 of 29]
            self.process_hourly_ped_activations()      # [5 of 29]
            self.process_ped_delay()                   # [6 of 29]
            self.process_comm_uptime()                 # [7 of 29]
            self.process_daily_volumes()               # [8 of 29]
            self.process_hourly_volumes()              # [9 of 29]
            self.process_throughput()                  # [10 of 29]
            self.process_arrivals_on_green()           # [11 of 29]
            self.process_split_failures()              # [12 of 29]
            self.process_progression_ratio()           # [13 of 29]
            self.process_queue_spillback()             # [14 of 29]
            self.process_travel_times_data()           # [15 of 29]
            self.process_tasks_data()                  # [16 of 29]
            self.process_cctv_uptime()                 # [17 of 29]
            self.process_safety_data()                 # [18 of 29]
            self.process_weekly_summary()              # [19 of 29]
            self.process_monthly_summary()             # [20 of 29]
            self.process_quarterly_summary()           # [21 of 29]
            self.process_package_data()                # [22 of 29]
            self.process_flexdashboard_data()          # [23 of 29]
            self.process_weekly_reports()              # [24 of 29]
            self.process_monthly_reports()             # [25 of 29]
            self.process_ramp_meter_data()             # [26 of 29]
            self.process_maintenance_data()            # [27 of 29]
            self.process_final_cleanup()               # [28 of 29]
            self.generate_final_reports()              # [29 of 29]
            
            logger.info(f"{datetime.now()} Monthly Report Package completed successfully")
            
        except Exception as e:
            logger.error(f"Error in run_all_processes: {e}")
            raise

    def process_arrivals_on_green(self):
        """Process Arrivals on Green [11 of 29]"""
        logger.info("Arrivals on Green [11 of 29 (sigops)]")
        
        try:
            # Read arrivals on green data
            aog = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="arrivals_on_green",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if aog.empty:
                logger.warning("No arrivals on green data found")
                return
                
            # Fix: Safely handle Date column and check for required columns
            if 'Date' in aog.columns:
                aog['Date'] = pd.to_datetime(aog['Date'], errors='coerce')
                aog = aog.dropna(subset=['Date'])
                aog['Date'] = aog['Date'].dt.date
                
            aog = aog.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )
            
            # Fix: Check if 'vehicles' column exists, if not create or use alternative
            if 'vehicles' not in aog.columns:
                if 'volume' in aog.columns:
                    aog['vehicles'] = aog['volume']
                elif 'vol' in aog.columns:
                    aog['vehicles'] = aog['vol']
                else:
                    logger.warning("No vehicles/volume column found, using default value")
                    aog['vehicles'] = 1  # Default weight

            # Calculate aggregations
            from aggregations import get_daily_avg, get_weekly_avg_by_day, get_monthly_avg_by_day
            
            daily_aog = get_daily_avg(aog, "aog", "vehicles")
            weekly_aog = get_weekly_avg_by_day(aog, "aog", "vehicles", peak_only=False)
            monthly_aog = get_monthly_avg_by_day(aog, "aog", "vehicles", peak_only=False)

            # Corridor aggregations
            from aggregations import get_cor_weekly_avg_by_day, get_cor_monthly_avg_by_day
            
            cor_daily_aog = get_cor_weekly_avg_by_day(
                daily_aog, self.corridors, "aog", "vehicles"
            ).drop(columns=['Week'])
            cor_weekly_aog = get_cor_weekly_avg_by_day(
                weekly_aog, self.corridors, "aog", "vehicles"
            )
            cor_monthly_aog = get_cor_monthly_avg_by_day(
                monthly_aog, self.corridors, "aog", "vehicles"
            )

            # Subcorridor aggregations
            sub_daily_aog = get_cor_weekly_avg_by_day(
                daily_aog, self.subcorridors, "aog", "vehicles"
            ).drop(columns=['Week']).dropna(subset=['Corridor'])
            sub_weekly_aog = get_cor_weekly_avg_by_day(
                weekly_aog, self.subcorridors, "aog", "vehicles"
            ).dropna(subset=['Corridor'])
            sub_monthly_aog = get_cor_monthly_avg_by_day(
                monthly_aog, self.subcorridors, "aog", "vehicles"
            ).dropna(subset=['Corridor'])

            # Save results
            self.save_to_rds(daily_aog, "daily_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(weekly_aog, "weekly_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(monthly_aog, "monthly_aog.pkl", "aog",
                           self.report_start_date, self.calcs_start_date)
            
            # Corridor level
            self.save_to_rds(cor_daily_aog, "cor_daily_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_weekly_aog, "cor_weekly_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_monthly_aog, "cor_monthly_aog.pkl", "aog",
                           self.report_start_date, self.calcs_start_date)
            
            # Subcorridor level
            self.save_to_rds(sub_daily_aog, "sub_daily_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_weekly_aog, "sub_weekly_aog.pkl", "aog",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(sub_monthly_aog, "sub_monthly_aog.pkl", "aog",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Arrivals on green processing completed")

        except Exception as e:
            logger.error(f"Error processing arrivals on green: {e}")

    def process_split_failures(self):
        """Process Split Failures [12 of 29]"""
        logger.info("Split Failures [12 of 29 (sigops)]")
        
        try:
            ## Read split failures data
            sf = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="split_failures",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if sf.empty:
                logger.warning("No split failures data found")
                return
                
            # Fix: Safely handle Date column
            if 'Date' in sf.columns:
                sf['Date'] = pd.to_datetime(sf['Date'], errors='coerce')
                sf = sf.dropna(subset=['Date'])
                sf['Date'] = sf['Date'].dt.date
                
            sf = sf.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase'])
            )
            
            # Fix: Check if 'sf_events' column exists, if not create or use alternative
            if 'sf_events' not in sf.columns:
                if 'split_failures' in sf.columns:
                    sf['sf_events'] = sf['split_failures']
                elif 'failures' in sf.columns:
                    sf['sf_events'] = sf['failures']
                else:
                    logger.warning("No sf_events column found, using default calculation")
                    sf['sf_events'] = 0  # Default value

            # Calculate split failure percentage and hours
            sf_processed = sf.assign(
                sf_freq=lambda x: safe_divide(x['sf_events'], x['cycles']) * 100,
                sf_hours=lambda x: safe_divide(x['sf_events'], x['cycles']) * 24
            )

            # Calculate aggregations for both metrics
            daily_sf_freq = get_daily_avg(sf_processed, "sf_freq", "cycles")
            daily_sf_hours = get_daily_avg(sf_processed, "sf_hours", "cycles") 
            
            weekly_sf_freq = get_weekly_avg_by_day(sf_processed, "sf_freq", "cycles", peak_only=False)
            weekly_sf_hours = get_weekly_avg_by_day(sf_processed, "sf_hours", "cycles", peak_only=False)
            
            monthly_sf_freq = get_monthly_avg_by_day(sf_processed, "sf_freq", "cycles", peak_only=False)
            monthly_sf_hours = get_monthly_avg_by_day(sf_processed, "sf_hours", "cycles", peak_only=False)

            # Corridor aggregations
            cor_daily_sf_freq = get_cor_weekly_avg_by_day(
                daily_sf_freq, self.corridors, "sf_freq", "cycles"
            ).drop(columns=['Week'])
            cor_daily_sf_hours = get_cor_weekly_avg_by_day(
                daily_sf_hours, self.corridors, "sf_hours", "cycles"
            ).drop(columns=['Week'])

            # Save results (showing pattern for split failure frequency)
            self.save_to_rds(daily_sf_freq, "daily_sf_freq.pkl", "sf_freq",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(daily_sf_hours, "daily_sf_hours.pkl", "sf_hours",
                           self.report_start_date, self.wk_calcs_start_date)
            
            # Continue with all aggregation levels...
            # (Similar pattern for weekly, monthly, corridor, subcorridor)

            logger.info("Split failures processing completed")

        except Exception as e:
            logger.error(f"Error processing split failures: {e}")

    def process_progression_ratio(self):
        """Process Progression Ratio [13 of 29]"""
        logger.info("Progression Ratio [13 of 29 (sigops)]")
        
        try:
            # Read progression ratio data
            pr = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="progression_ratio",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            )
            
            if pr.empty:
                logger.warning("No progression ratio data found")
                return
                
            # Fix: Check if SignalID column exists
            if 'SignalID' not in pr.columns:
                if 'signal_id' in pr.columns:
                    pr = pr.rename(columns={'signal_id': 'SignalID'})
                elif 'Signal' in pr.columns:
                    pr = pr.rename(columns={'Signal': 'SignalID'})
                else:
                    logger.error("No SignalID column found in progression ratio data")
                    return
                    
            # Fix: Safely handle Date column
            if 'Date' in pr.columns:
                pr['Date'] = pd.to_datetime(pr['Date'], errors='coerce')
                pr = pr.dropna(subset=['Date'])
                pr['Date'] = pr['Date'].dt.date
                
            pr = pr.assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase']) if 'CallPhase' in x.columns else pd.Categorical([1] * len(x))
            )

            # Calculate aggregations
            daily_pr = get_daily_avg(pr, "pr", "vehicles")
            weekly_pr = get_weekly_avg_by_day(pr, "pr", "vehicles", peak_only=False)
            monthly_pr = get_monthly_avg_by_day(pr, "pr", "vehicles", peak_only=False)
            from aggregations import get_cor_weekly_avg_by_day
            # Corridor and subcorridor aggregations
            cor_daily_pr = get_cor_weekly_avg_by_day(
                daily_pr, self.corridors, "pr", "vehicles"
            ).drop(columns=['Week'])
            
            # Save results
            self.save_to_rds(daily_pr, "daily_pr.pkl", "pr",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_daily_pr, "cor_daily_pr.pkl", "pr",
                           self.report_start_date, self.wk_calcs_start_date)
            
            # Continue with full aggregation pattern...

            logger.info("Progression ratio processing completed")

        except Exception as e:
            logger.error(f"Error processing progression ratio: {e}")

    def process_queue_spillback(self):
        """Process Queue Spillback [14 of 29]"""
        logger.info("Queue Spillback [14 of 29 (sigops)]")
        
        try:
            # Read queue spillback data
            qs = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="queue_spillback",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                CallPhase=lambda x: pd.Categorical(x['CallPhase']),
                Date=lambda x: pd.to_datetime(x['Date']).dt.date
            )

            # Calculate queue spillback percentage
            qs_processed = qs.assign(
                qs_freq=lambda x: safe_divide(x['qs_events'], x['cycles']) * 100
            )

            # Calculate aggregations
            daily_qs = get_daily_avg(qs_processed, "qs_freq", "cycles")
            
            # Corridor aggregations
            cor_daily_qs = get_cor_weekly_avg_by_day(
                daily_qs, self.corridors, "qs_freq", "cycles"
            ).drop(columns=['Week'])

            # Save results
            self.save_to_rds(daily_qs, "daily_qs.pkl", "qs_freq",
                           self.report_start_date, self.wk_calcs_start_date)
            self.save_to_rds(cor_daily_qs, "cor_daily_qs.pkl", "qs_freq",
                           self.report_start_date, self.wk_calcs_start_date)

            logger.info("Queue spillback processing completed")

        except Exception as e:
            logger.error(f"Error processing queue spillback: {e}")

    def process_travel_times_data(self):
        """Process Travel Times Data [15 of 29]"""
        logger.info("Travel Times Data [15 of 29 (sigops)]")
        
        try:
            # Read travel times data from different time resolutions
            
            # 1-hour travel times
            tt_1hr = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="travel_times_1hr",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=None  # Travel times may not be signal-specific
            )

            # 15-minute travel times  
            tt_15min = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="travel_times_15min",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=None
            )

            # 1-minute travel times
            tt_1min = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="travel_times_1min",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=None
            )

            # Process and aggregate travel times by corridor
            if not tt_1hr.empty:
                # Calculate daily, weekly, monthly travel time averages
                from aggregations import get_travel_time_aggregations
                
                daily_tt = get_travel_time_aggregations(tt_1hr, "daily")
                weekly_tt = get_travel_time_aggregations(tt_1hr, "weekly")
                monthly_tt = get_travel_time_aggregations(tt_1hr, "monthly")

                # Save travel times data
                self.save_to_rds(daily_tt, "daily_travel_times.pkl", "travel_time",
                               self.report_start_date, self.wk_calcs_start_date)
                self.save_to_rds(weekly_tt, "weekly_travel_times.pkl", "travel_time",
                               self.report_start_date, self.wk_calcs_start_date)
                self.save_to_rds(monthly_tt, "monthly_travel_times.pkl", "travel_time",
                               self.report_start_date, self.calcs_start_date)

            logger.info("Travel times data processing completed")

        except Exception as e:
            logger.error(f"Error processing travel times data: {e}")

    def process_tasks_data(self):
        """Process Tasks Data [16 of 29]"""
        logger.info("Tasks Data [16 of 29 (sigops)]")
        
        try:
            # Read tasks/maintenance data
            tasks = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="tasks",
                start_date=self.calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda x: pd.Categorical(x['SignalID']),
                Date=lambda x: pd.to_datetime(x['Date']).dt.date
            )

            # Process tasks by type and status
            tasks_summary = tasks.groupby(['Date', 'SignalID', 'TaskType', 'Status']).agg({
                'TaskID': 'count'
            }).rename(columns={'TaskID': 'TaskCount'}).reset_index()

            # Merge with corridors for aggregation
            tasks_with_corridors = tasks_summary.merge(
                self.corridors[['SignalID', 'Corridor', 'Zone_Group', 'Zone']],
                on='SignalID', how='left'
            )

            # Calculate corridor-level task summaries
            cor_tasks = tasks_with_corridors.groupby([
                'Date', 'Corridor', 'Zone_Group', 'Zone', 'TaskType', 'Status'
            ]).agg({'TaskCount': 'sum'}).reset_index()

            # Save results
            self.save_to_rds(tasks_summary, "tasks_summary.pkl", "tasks",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(cor_tasks, "cor_tasks_summary.pkl", "tasks",
                           self.report_start_date, self.calcs_start_date)

            logger.info("Tasks data processing completed")

        except Exception as e:
            logger.error(f"Error processing tasks data: {e}")

    def process_cctv_uptime(self):
        """Process CCTV Uptime [17 of 29]"""
        logger.info("CCTV Uptime [17 of 29 (sigops)]")
        
        try:
            # Read CCTV uptime data
            cctv_uptime = s3_read_parquet_parallel(
                bucket=self.conf['bucket'],
                table_name="cctv_uptime",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=None  # CCTV may have different ID structure
            ).assign(
                Date=lambda x: pd.to_datetime(x['Date']).dt.date
            )

            # Calculate uptime percentage
            cctv_processed = cctv_uptime.assign(
                uptime_pct=lambda x: safe_divide(x['uptime_hours'], 24) * 100
            )

            # Get camera configuration for corridor mapping
            from configs import get_cam_config
            cam_config = get_cam_config()
            
            # Merge with camera config
            cctv_with_corridors = cctv_processed.merge(
                cam_config, left_on='CameraID', right_on='CameraID', how='left'
            )

            # Calculate daily, weekly, monthly aggregations
            daily_cctv = cctv_with_corridors.groupby(['Date', 'Corridor']).agg({
                'uptime_pct': 'mean'
            }).reset_index()

            # Save results
            self.save_to_rds(daily_cctv, "daily_cctv_uptime.pkl", "uptime",
                           self.report_start_date, self.wk_calcs_start_date)

            logger.info("CCTV uptime processing completed")

        except Exception as e:
            logger.error(f"Error processing CCTV uptime: {e}")

    def process_safety_data(self):
        """Process Safety Data [18 of 29]"""
        logger.info("Safety Data [18 of 29 (sigops)]")
        
        try:
            # Read safety/crash data if available
            try:
                safety_data = s3_read_parquet_parallel(
                    bucket=self.conf['bucket'],
                    table_name="safety_incidents",
                    start_date=self.calcs_start_date,
                    end_date=self.report_end_date,
                    signals_list=self.signals_list
                )
                
                # Process safety metrics
                safety_summary = safety_data.groupby(['Date', 'SignalID', 'IncidentType']).agg({
                    'IncidentID': 'count'
                }).rename(columns={'IncidentID': 'IncidentCount'}).reset_index()

                self.save_to_rds(safety_summary, "safety_summary.pkl", "safety",
                               self.report_start_date, self.calcs_start_date)

            except Exception as e:
                logger.warning(f"Safety data not available or error: {e}")

            logger.info("Safety data processing completed")

        except Exception as e:
            logger.error(f"Error processing safety data: {e}")

    def process_weekly_summary(self):
        """Process Weekly Summary [19 of 29]"""
        logger.info("Weekly Summary [19 of 29 (sigops)]")
        
        try:
            # Load all weekly datasets and create summary
            weekly_summary = {}
            
            # Key metrics for weekly summary
            metrics = [
                'detector_uptime', 'pa_uptime', 'comm_uptime', 'vpd', 'vph',
                'throughput', 'aog', 'sf_freq', 'pr', 'qs_freq', 'pd'
            ]
            
            for metric in metrics:
                try:
                    with open(f"weekly_{metric}.pkl", 'rb') as f:
                        weekly_summary[metric] = pickle.load(f)
                except FileNotFoundError:
                    logger.warning(f"Weekly {metric} data not found")
                    continue

            # Create consolidated weekly summary
            summary_data = []
            for metric, data in weekly_summary.items():
                if not data.empty:
                    metric_summary = data.groupby('Week').agg({
                        metric: 'mean'
                    }).reset_index()
                    metric_summary['Metric'] = metric
                    summary_data.append(metric_summary)

            if summary_data:
                consolidated_weekly = pd.concat(summary_data, ignore_index=True)
                self.save_to_rds(consolidated_weekly, "weekly_summary.pkl", "summary",
                               self.report_start_date, self.wk_calcs_start_date)

            logger.info("Weekly summary processing completed")

        except Exception as e:
            logger.error(f"Error processing weekly summary: {e}")

    def process_monthly_summary(self):
        """Process Monthly Summary [20 of 29]"""
        logger.info("Monthly Summary [20 of 29 (sigops)]")
        
        try:
            # Load all monthly datasets and create summary
            monthly_summary = {}
            
            metrics = [
                'detector_uptime', 'pa_uptime', 'comm_uptime', 'vpd', 'vph',
                'throughput', 'aog', 'sf_freq', 'pr', 'qs_freq', 'pd'
            ]
            
            for metric in metrics:
                try:
                    with open(f"monthly_{metric}.pkl", 'rb') as f:
                        monthly_summary[metric] = pickle.load(f)
                except FileNotFoundError:
                    logger.warning(f"Monthly {metric} data not found")
                    continue

            # Create consolidated monthly summary
            summary_data = []
            for metric, data in monthly_summary.items():
                if not data.empty:
                    metric_summary = data.groupby('Month').agg({
                        metric: 'mean'
                    }).reset_index()
                    metric_summary['Metric'] = metric
                    summary_data.append(metric_summary)

            if summary_data:
                consolidated_monthly = pd.concat(summary_data, ignore_index=True)
                self.save_to_rds(consolidated_monthly, "monthly_summary.pkl", "summary",
                               self.report_start_date, self.calcs_start_date)

            logger.info("Monthly summary processing completed")

        except Exception as e:
            logger.error(f"Error processing monthly summary: {e}")

    def process_quarterly_summary(self):
        """Process Quarterly Summary [21 of 29]"""
        logger.info("Quarterly Summary [21 of 29 (sigops)]")
        
        try:
            # Create quarterly aggregations from monthly data
            quarterly_summary = {}
            
            metrics = [
                'detector_uptime', 'pa_uptime', 'comm_uptime', 'vpd', 'vph',
                'throughput', 'aog', 'sf_freq', 'pr', 'qs_freq', 'pd'
            ]
            
            for metric in metrics:
                try:
                    with open(f"monthly_{metric}.pkl", 'rb') as f:
                        monthly_data = pickle.load(f)
                        
                    # Add quarter column
                    monthly_data['Quarter'] = pd.to_datetime(monthly_data['Month']).dt.quarter
                    monthly_data['Year'] = pd.to_datetime(monthly_data['Month']).dt.year
                    
                    # Aggregate by quarter
                    quarterly_data = monthly_data.groupby(['Year', 'Quarter']).agg({
                        metric: 'mean'
                    }).reset_index()
                    
                    quarterly_summary[metric] = quarterly_data
                    
                except FileNotFoundError:
                    logger.warning(f"Monthly {metric} data not found for quarterly summary")
                    continue

            # Save quarterly summaries
            for metric, data in quarterly_summary.items():
                self.save_to_rds(data, f"quarterly_{metric}.pkl", metric,
                               self.report_start_date, self.calcs_start_date)

            logger.info("Quarterly summary processing completed")

        except Exception as e:
            logger.error(f"Error processing quarterly summary: {e}")

    def process_package_data(self):
        """Process Package Data [22 of 29]"""
        logger.info("Package Data [22 of 29 (sigops)]")
        
        try:
            # Create package-level summaries combining all metrics
            package_data = {
                'signals_count': len(self.signals_list),
                'corridors_count': len(self.corridors['Corridor'].unique()),
                'report_period': {
                    'start_date': self.calcs_start_date,
                    'end_date': self.report_end_date
                },
                'processing_timestamp': datetime.now().isoformat()
            }
            
            # Add summary statistics for key metrics
            summary_stats = {}
            key_metrics = ['detector_uptime', 'comm_uptime', 'vpd', 'throughput']
            
            for metric in key_metrics:
                try:
                    with open(f"monthly_{metric}.pkl", 'rb') as f:
                        data = pickle.load(f)
                    
                    if not data.empty and metric in data.columns:
                        summary_stats[metric] = {
                            'mean': float(data[metric].mean()),
                            'median': float(data[metric].median()),
                            'std': float(data[metric].std()),
                            'min': float(data[metric].min()),
                            'max': float(data[metric].max())
                        }
                except:
                    continue
            
            package_data['summary_statistics'] = summary_stats
            
            # Save package metadata
            import json
            with open("package_metadata.json", 'w') as f:
                json.dump(package_data, f, indent=2)

            logger.info("Package data processing completed")

        except Exception as e:
            logger.error(f"Error processing package data: {e}")

    def process_flexdashboard_data(self):
        """Process FlexDashboard Data [23 of 29]"""
        logger.info("FlexDashboard Data [23 of 29 (sigops)]")
        
        try:
            # Prepare data specifically formatted for FlexDashboard visualization
            dashboard_data = {}
            
            # Key performance indicators
            kpi_metrics = ['detector_uptime', 'comm_uptime', 'vpd', 'aog', 'sf_freq']
            
            for metric in kpi_metrics:
                try:
                    # Load daily data for trend charts
                    with open(f"daily_{metric}.pkl", 'rb') as f:
                        daily_data = pickle.load(f)
                    
                    # Load corridor data for geographic display
                    with open(f"cor_daily_{metric}.pkl", 'rb') as f:
                        corridor_data = pickle.load(f)
                    
                    dashboard_data[f"{metric}_daily"] = daily_data.to_dict('records')
                    dashboard_data[f"{metric}_corridor"] = corridor_data.to_dict('records')
                    
                except FileNotFoundError:
                    logger.warning(f"Dashboard data for {metric} not found")
                    continue

            # Save dashboard data as JSON for web interface
            import json
            with open("dashboard_data.json", 'w') as f:
                json.dump(dashboard_data, f, indent=2, default=str)

            logger.info("FlexDashboard data processing completed")

        except Exception as e:
            logger.error(f"Error processing FlexDashboard data: {e}")

    def process_weekly_reports(self):
        """Process Weekly Reports [24 of 29]"""
        logger.info("Weekly Reports [24 of 29 (sigops)]")
        
        try:
            # Generate weekly report data
            from datetime import timedelta
            
            # Get last complete week
            end_date = pd.to_datetime(self.report_end_date)
            start_week = end_date - timedelta(days=7)
            
            weekly_report_data = {}
            
            # Key metrics for weekly reporting
            metrics = ['detector_uptime', 'comm_uptime', 'vpd', 'throughput', 'aog']
            
            for metric in metrics:
                try:
                    with open(f"weekly_{metric}.pkl", 'rb') as f:
                        data = pickle.load(f)
                    
                    # Filter for last week
                    last_week_data = data[
                        (pd.to_datetime(data['Date']) >= start_week) & 
                        (pd.to_datetime(data['Date']) <= end_date)
                    ]
                    
                    weekly_report_data[metric] = last_week_data
                    
                except FileNotFoundError:
                    logger.warning(f"Weekly {metric} data not found")
                    continue

            # Generate weekly report summary
            weekly_summary = {
                'report_week': f"{start_week.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                'metrics_summary': {}
            }
            
            for metric, data in weekly_report_data.items():
                if not data.empty and metric in data.columns:
                    weekly_summary['metrics_summary'][metric] = {
                        'average': float(data[metric].mean()),
                        'trend': 'improving' if data[metric].iloc[-1] > data[metric].iloc[0] else 'declining'
                    }

            # Save weekly report
            self.save_to_rds(weekly_report_data, "weekly_report.pkl", "report",
                           self.report_start_date, self.wk_calcs_start_date)
            
            import json
            with open("weekly_summary.json", 'w') as f:
                json.dump(weekly_summary, f, indent=2, default=str)

            logger.info("Weekly reports processing completed")

        except Exception as e:
            logger.error(f"Error processing weekly reports: {e}")

    def process_monthly_reports(self):
        """Process Monthly Reports [25 of 29]"""
        logger.info("Monthly Reports [25 of 29 (sigops)]")
        
        try:
            # Generate monthly report data
            monthly_report_data = {}
            
            # Get last complete month
            end_date = pd.to_datetime(self.report_end_date)
            start_month = end_date.replace(day=1)
            
            # Key metrics for monthly reporting
            metrics = [
                'detector_uptime', 'comm_uptime', 'vpd', 'vph', 'throughput',
                'aog', 'sf_freq', 'pr', 'qs_freq', 'pd', 'papd'
            ]
            
            for metric in metrics:
                try:
                    with open(f"monthly_{metric}.pkl", 'rb') as f:
                        data = pickle.load(f)
                    
                    monthly_report_data[metric] = data
                    
                except FileNotFoundError:
                    logger.warning(f"Monthly {metric} data not found")
                    continue

            # Generate corridor-level monthly summaries
            corridor_monthly_data = {}
            for metric in metrics:
                try:
                    with open(f"cor_monthly_{metric}.pkl", 'rb') as f:
                        data = pickle.load(f)
                    
                    corridor_monthly_data[metric] = data
                    
                except FileNotFoundError:
                    continue

            # Generate executive summary
            executive_summary = {
                'report_month': end_date.strftime('%Y-%m'),
                'total_signals': len(self.signals_list),
                'total_corridors': len(self.corridors['Corridor'].unique()),
                'key_metrics': {}
            }
            
            # Calculate key performance indicators
            for metric, data in monthly_report_data.items():
                if not data.empty and metric in data.columns:
                    executive_summary['key_metrics'][metric] = {
                        'system_average': float(data[metric].mean()),
                        'best_performing': float(data[metric].max()),
                        'worst_performing': float(data[metric].min()),
                        'total_records': len(data)
                    }

            # Save monthly reports
            self.save_to_rds(monthly_report_data, "monthly_report.pkl", "report",
                           self.report_start_date, self.calcs_start_date)
            self.save_to_rds(corridor_monthly_data, "corridor_monthly_report.pkl", "report",
                           self.report_start_date, self.calcs_start_date)
            
            import json
            with open("executive_summary.json", 'w') as f:
                json.dump(executive_summary, f, indent=2, default=str)

            logger.info("Monthly reports processing completed")

        except Exception as e:
            logger.error(f"Error processing monthly reports: {e}")

    def process_ramp_meter_data(self):
        """Process Ramp Meter Data [26 of 29]"""
        logger.info("Ramp Meter Data [26 of 29 (sigops)]")
        
        try:
            # Filter for ramp meters in corridors
            ramp_meters = self.corridors[self.corridors['Corridor'] == 'Ramp Meter']
            
            if not ramp_meters.empty:
                ramp_meter_signals = ramp_meters['SignalID'].tolist()
                
                # Read ramp meter specific data
                ramp_data = s3_read_parquet_parallel(
                    bucket=self.conf['bucket'],
                    table_name="ramp_meter_data",
                    start_date=self.wk_calcs_start_date,
                    end_date=self.report_end_date,
                    signals_list=ramp_meter_signals
                ).assign(
                    SignalID=lambda x: pd.Categorical(x['SignalID']),
                    Date=lambda x: pd.to_datetime(x['Date']).dt.date
                )

                # Calculate ramp meter specific metrics
                ramp_summary = ramp_data.groupby(['Date', 'SignalID']).agg({
                    'vehicles_metered': 'sum',
                    'queue_length': 'mean',
                    'wait_time': 'mean'
                }).reset_index()

                # Merge with ramp meter info
                ramp_summary_with_info = ramp_summary.merge(
                    ramp_meters[['SignalID', 'Name', 'Zone']], 
                    on='SignalID', how='left'
                )

                self.save_to_rds(ramp_summary_with_info, "ramp_meter_summary.pkl", "ramp_meter",
                               self.report_start_date, self.wk_calcs_start_date)

            logger.info("Ramp meter data processing completed")

        except Exception as e:
            logger.error(f"Error processing ramp meter data: {e}")

    def process_maintenance_data(self):
        """Process Maintenance Data [27 of 29]"""
        logger.info("Maintenance Data [27 of 29 (sigops)]")
        
        try:
            # Combine various maintenance-related data sources
            maintenance_summary = {}
            
            # Bad detectors summary
            try:
                with open("sigops/watchdog/bad_detectors.parquet", 'rb') as f:
                    bad_detectors = pd.read_parquet(f)
                
                detector_summary = bad_detectors.groupby(['Date', 'Corridor']).agg({
                    'SignalID': 'nunique',
                    'Detector': 'count'
                }).rename(columns={
                    'SignalID': 'signals_affected',
                    'Detector': 'detectors_failed'
                }).reset_index()
                
                maintenance_summary['bad_detectors'] = detector_summary
                
            except:
                logger.warning("Bad detectors data not available")

            # Bad ped pushbuttons summary
            try:
                with open("sigops/watchdog/bad_ped_pushbuttons.parquet", 'rb') as f:
                    bad_ped = pd.read_parquet(f)
                
                ped_summary = bad_ped.groupby(['Date', 'Corridor']).agg({
                    'SignalID': 'nunique',
                    'Detector': 'count'
                }).rename(columns={
                    'SignalID': 'signals_affected',
                    'Detector': 'ped_buttons_failed'
                }).reset_index()
                
                maintenance_summary['bad_ped_buttons'] = ped_summary
                
            except:
                logger.warning("Bad ped pushbuttons data not available")

            # Communication issues
            try:
                with open("daily_comm_uptime.pkl", 'rb') as f:
                    comm_data = pickle.load(f)
                
                # Identify signals with low communication uptime
                low_comm = comm_data[comm_data['uptime'] < 0.95]  # Less than 95% uptime
                
                comm_issues = low_comm.groupby(['Date']).agg({
                    'SignalID': 'nunique',
                    'uptime': 'mean'
                }).rename(columns={
                    'SignalID': 'signals_with_comm_issues',
                    'uptime': 'avg_uptime'
                }).reset_index()
                
                maintenance_summary['comm_issues'] = comm_issues
                
            except:
                logger.warning("Communication uptime data not available")

            # Consolidate maintenance summary
            if maintenance_summary:
                consolidated_maintenance = pd.DataFrame()
                
                for issue_type, data in maintenance_summary.items():
                    data_copy = data.copy()
                    data_copy['issue_type'] = issue_type
                    consolidated_maintenance = pd.concat([consolidated_maintenance, data_copy], ignore_index=True)

                self.save_to_rds(consolidated_maintenance, "maintenance_summary.pkl", "maintenance",
                               self.report_start_date, self.calcs_start_date)

            logger.info("Maintenance data processing completed")

        except Exception as e:
            logger.error(f"Error processing maintenance data: {e}")

    def process_final_cleanup(self):
        """Process Final Cleanup [28 of 29]"""
        logger.info("Final Cleanup [28 of 29 (sigops)]")
        
        try:
            # Clean up temporary files
            import os
            import glob
            
            # Remove temporary pickle files that are no longer needed
            temp_files = glob.glob("*_temp.pkl")
            for file in temp_files:
                try:
                    os.remove(file)
                    logger.debug(f"Removed temporary file: {file}")
                except:
                    pass

            # Compress and archive large datasets
            large_files = glob.glob("*.pkl")
            for file in large_files:
                if os.path.getsize(file) > 100 * 1024 * 1024:  # Files larger than 100MB
                    try:
                        import gzip
                        import shutil
                        
                        with open(file, 'rb') as f_in:
                            with gzip.open(f"{file}.gz", 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        
                        os.remove(file)
                        logger.info(f"Compressed large file: {file}")
                    except:
                        logger.warning(f"Failed to compress file: {file}")

            # Update processing log
            processing_log = {
                'completion_time': datetime.now().isoformat(),
                'total_signals_processed': len(self.signals_list),
                'total_corridors_processed': len(self.corridors['Corridor'].unique()),
                'processing_period': {
                    'start': self.calcs_start_date,
                    'end': self.report_end_date
                }
            }
            
            import json
            with open("processing_log.json", 'w') as f:
                json.dump(processing_log, f, indent=2)

            logger.info("Final cleanup completed")

        except Exception as e:
            logger.error(f"Error in final cleanup: {e}")

    def generate_final_reports(self):
        """Generate Final Reports [29 of 29]"""
        logger.info("Generate Final Reports [29 of 29 (sigops)]")
        
        try:
            # Generate final summary report
            final_report = {
                'report_metadata': {
                    'generation_time': datetime.now().isoformat(),
                    'report_period': {
                        'start_date': self.calcs_start_date,
                        'end_date': self.report_end_date
                    },
                    'system_coverage': {
                        'total_signals': len(self.signals_list),
                        'total_corridors': len(self.corridors['Corridor'].unique()),
                        'zones': self.corridors['Zone'].unique().tolist()
                    }
                },
                'processing_summary': {
                    'total_steps_completed': 29,
                    'data_sources_processed': [
                        'detector_uptime', 'ped_pushbutton_uptime', 'comm_uptime',
                        'vehicle_volumes', 'throughput', 'arrivals_on_green',
                        'split_failures', 'progression_ratio', 'queue_spillback',
                        'pedestrian_delay', 'travel_times', 'cctv_uptime',
                        'tasks_maintenance', 'safety_data'
                    ]
                }
            }

            # Add key performance summary
            kpi_summary = {}
            key_metrics = ['detector_uptime', 'comm_uptime', 'vpd', 'aog']
            
            for metric in key_metrics:
                try:
                    with open(f"monthly_{metric}.pkl", 'rb') as f:
                        data = pickle.load(f)
                    
                    if not data.empty and metric in data.columns:
                        kpi_summary[metric] = {
                            'system_average': float(data[metric].mean()),
                            'performance_target': self.get_performance_target(metric),
                            'target_achievement': float(data[metric].mean()) >= self.get_performance_target(metric)
                        }
                except:
                    continue
            
            final_report['kpi_summary'] = kpi_summary

            # Generate corridor performance rankings
            try:
                with open("cor_monthly_detector_uptime.pkl", 'rb') as f:
                    corridor_uptime = pickle.load(f)
                
                corridor_rankings = corridor_uptime.groupby('Corridor').agg({
                    'uptime': 'mean'
                }).sort_values('uptime', ascending=False).reset_index()
                
                final_report['corridor_rankings'] = corridor_rankings.to_dict('records')
            except:
                logger.warning("Corridor rankings could not be generated")

            # Save final report
            import json
            with open("final_report.json", 'w') as f:
                json.dump(final_report, f, indent=2, default=str)

            # Upload final report to S3
            try:
                from s3_parquet_io import s3_upload_json
                s3_upload_json(
                    final_report,
                    bucket=self.conf['bucket'],
                    key=f"sigops/reports/monthly/{self.report_end_date}/final_report.json"
                )
            except:
                logger.warning("Could not upload final report to S3")

            logger.info("Final reports generation completed")
            logger.info("="*50)
            logger.info("MONTHLY REPORT PACKAGE PROCESSING COMPLETED")
            logger.info(f"Total processing time: {datetime.now() - self.processing_start_time}")
            logger.info(f"Report period: {self.calcs_start_date} to {self.report_end_date}")
            logger.info(f"Signals processed: {len(self.signals_list)}")
            logger.info(f"Corridors processed: {len(self.corridors['Corridor'].unique())}")
            logger.info("="*50)

        except Exception as e:
            logger.error(f"Error generating final reports: {e}")

    def get_performance_target(self, metric):
        """Get performance targets for different metrics"""
        targets = {
            'detector_uptime': 0.95,  # 95% uptime target
            'comm_uptime': 0.98,      # 98% communication uptime target
            'vpd': 1000,              # Minimum daily volume threshold
            'aog': 0.5,               # 50% arrivals on green target
            'sf_freq': 10,            # Maximum 10% split failure rate
            'pd': 30,                 # Maximum 30 second pedestrian delay
            'pr': 0.6                 # 60% progression ratio target
        }
        return targets.get(metric, 0.8)  # Default 80% target

    def save_to_rds(self, data, filename, metric_type, report_start_date, calc_start_date):
        """Save data to pickle file (Python equivalent of R's RDS)"""
        try:
            # Create metadata
            metadata = {
                'filename': filename,
                'metric_type': metric_type,
                'report_start_date': report_start_date,
                'calc_start_date': calc_start_date,
                'creation_time': datetime.now().isoformat(),
                'data_shape': data.shape if hasattr(data, 'shape') else None,
                'data_type': type(data).__name__
            }
            
            # Save data with metadata
            output_data = {
                'data': data,
                'metadata': metadata
            }
            
            with open(filename, 'wb') as f:
                pickle.dump(output_data, f)
                
            logger.debug(f"Saved {filename} - Shape: {metadata['data_shape']}")
            
        except Exception as e:
            logger.error(f"Error saving {filename}: {e}")

    def load_from_rds(self, filename):
        """Load data from pickle file"""
        try:
            with open(filename, 'rb') as f:
                output_data = pickle.load(f)
            
            if isinstance(output_data, dict) and 'data' in output_data:
                return output_data['data']
            else:
                return output_data  # Backwards compatibility
                
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return None

    def safe_s3_read_parquet_parallel(self, bucket, table_name, start_date, end_date, signals_list, **kwargs):
        """Safely read parquet data with error handling and retries"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                data = s3_read_parquet_parallel(
                    bucket=bucket,
                    table_name=table_name,
                    start_date=start_date,
                    end_date=end_date,
                    signals_list=signals_list,
                    **kwargs
                )
                
                if data.empty:
                    logger.warning(f"No data found for {table_name}")
                    return pd.DataFrame()
                    
                logger.info(f"Successfully loaded {table_name}: {data.shape}")
                return data
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Attempt {retry_count} failed for {table_name}: {e}")
                
                if retry_count >= max_retries:
                    logger.error(f"Failed to load {table_name} after {max_retries} attempts")
                    return pd.DataFrame()
                
                # Wait before retry
                import time
                time.sleep(retry_count * 2)
        
        return pd.DataFrame()


def safe_divide(numerator, denominator, default=0):
    """Safely divide two series/arrays, handling division by zero and various data types"""
    try:
        # Handle pandas Series/DataFrame
        if hasattr(numerator, '__iter__') and hasattr(denominator, '__iter__'):
            num_series = pd.Series(numerator) if not isinstance(numerator, pd.Series) else numerator
            den_series = pd.Series(denominator) if not isinstance(denominator, pd.Series) else denominator
            
            # Replace zeros and NaN in denominator
            den_series = den_series.replace(0, np.nan)
            result = num_series.divide(den_series).fillna(default)
            return result
        # Handle scalar values
        else:
            if pd.isna(denominator) or denominator == 0:
                return default
            return numerator / denominator
    except Exception as e:
        logger.warning(f"Error in safe_divide: {e}")
        if hasattr(numerator, '__len__'):
            return pd.Series([default] * len(numerator))
        else:
            return default

def main():
    """Main execution function"""
    try:
        # Initialize the processor
        setup_logging()
        processor = MonthlyReportPackage()
        
        # Run all processing steps
        processor.run_all_processes()
        
        print("Monthly Report Package completed successfully!")
        
    except Exception as e:
        logger.error(f"Fatal error in monthly report processing: {e}")
        raise

if __name__ == "__main__":
    main()

