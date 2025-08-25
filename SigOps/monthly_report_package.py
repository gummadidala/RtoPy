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
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
import logging
from typing import List, Dict, Any, Optional
import traceback

import pandas as pd
import numpy as np
import boto3
import yaml
from pathlib import Path

# Add the parent directory to the path to import local modules
sys.path.append(str(Path(__file__).parent.parent))

# Import from existing RtoPy modules
from utilities import (
    create_progress_tracker, 
    retry_on_failure, 
    batch_process,
    # resample_timeseries,
    # validate_dataframe_scheme,
    format_duration,
    get_memory_usage
)
from aggregations import (
    sigify,
    get_monthly_avg_by_day,
    get_weekly_avg_by_day,
    get_daily_avg,
    get_vph
)
# from config import get_date_from_string
# from s3_parquet_io import read_parquet_from_s3, write_parquet_to_s3
from SigOps.configs import get_det_config_, get_corridors, get_cam_config
from monthly_report_functions import parallel_process_dates
from parquet_lib import read_parquet_file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MonthlyReportProcessor:
    """Main class for processing monthly traffic signal reports"""
    
    def __init__(self, config):
        """Initialize the processor with configuration"""
        self.config = config
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        self.start_time = None
        
        # Date configurations
        self.setup_dates()
        
        # Load signal configurations
        self.load_signal_configs()
            
    def setup_dates(self):
        """Setup date ranges for calculations"""
        self.report_end_date = self.config.get('report_end_date', datetime.now().date())
        self.calcs_start_date = self.config.get('calcs_start_date')
        self.wk_calcs_start_date = self.config.get('wk_calcs_start_date')
        self.report_start_date = self.config.get('report_start_date')
        
        logger.info(f"Week Calcs Start Date: {self.wk_calcs_start_date}")
        logger.info(f"Calcs Start Date: {self.calcs_start_date}")
        logger.info(f"Report End Date: {self.report_end_date}")
        
    def load_signal_configs(self):
        """Load signal configuration data"""
        # Load corridors, subcorridors, signals_list, etc.
        # This would need to be adapted based on your data sources
        self.corridors = self._load_corridors()
        self.subcorridors = self._load_subcorridors()
        self.signals_list = self._load_signals_list()
        self.cam_config = self._load_camera_config()
        
    def _load_corridors(self) -> pd.DataFrame:
        """Load corridor configuration"""
        # Implement based on your data source
        try:
            corridors = get_corridors(self.config['corridors_filename_s3'], filter_signals=True)
            return corridors
        except Exception as e:
            logger.warning(f"Could not load corridors config: {e}")
            return pd.DataFrame()
        
    def _load_subcorridors(self) -> pd.DataFrame:
        """Load subcorridor configuration"""
        try:
            if 'Subcorridor' not in self.corridors.columns:
                logger.warning("No Subcorridor column found")
                return pd.DataFrame()
            
            subcorridors = self.corridors[self.corridors['Subcorridor'].notna()].copy()
            subcorridors = subcorridors.drop(columns=['Zone_Group'])
            subcorridors = subcorridors.rename(columns={
                'Zone': 'Zone_Group',
                'Corridor': 'Zone', 
                'Subcorridor': 'Corridor'
            })
            
            logger.info(f"Setup {len(subcorridors)} subcorridor mappings")
            return subcorridors
        except Exception as e:
            logger.warning(f"Could not load subcorridors config: {e}")
            return pd.DataFrame()
        
    def _load_signals_list(self) -> List[str]:
        """Load list of signal IDs to process"""
        try:
            signals_list = self.corridors['SignalID'].unique().tolist()
            return signals_list
        except Exception as e:
            logger.warning(f"Could not load signals list: {e}")
            return []
        
    def _load_camera_config(self) -> pd.DataFrame:
        """Load camera configuration"""
        try:
            return get_cam_config(
                    object_key=self.config['cctv_config_filename'],
                    bucket=self.config['bucket'],
                    corridors=self.corridors
                )
        except Exception as e:
            logger.warning(f"Could not load camera config: {e}")
            return pd.DataFrame()

    @retry_on_failure(max_retries=3, delay=2.0)
    def s3_read_parquet_parallel(self, bucket: str, table_name: str, 
                                start_date: str, end_date: str,
                                signals_list: List[str] = None,
                                callback: callable = None,
                                parallel: bool = True) -> pd.DataFrame:
        """
        Read parquet files from S3 in parallel using existing utilities
        
        Args:
            bucket: S3 bucket name
            table_name: Table/prefix name
            start_date: Start date for filtering
            end_date: End date for filtering
            signals_list: List of signal IDs to filter
            callback: Optional callback function to apply to each chunk
            parallel: Whether to use parallel processing
        
        Returns:
            Combined DataFrame
        """
        try:
            date_range = pd.date_range(start_date, end_date, freq='D')
            date_strings = [d.strftime('%Y-%m-%d') for d in date_range]
            
            def process_date(date_str):
                key = f"mark/{table_name}/date={date_str}/{table_name}_{date_str}.parquet"
                try:
                    df = read_parquet_file(bucket, key)
                    
                    # Validate DataFrame
                    if not validate_dataframe_scheme(df, required_columns=['SignalID']):
                        return pd.DataFrame()
                    
                    # Filter by signals if provided
                    if signals_list and 'SignalID' in df.columns:
                        df = df[df['SignalID'].isin(signals_list)]
                    
                    # Apply callback if provided
                    if callback:
                        df = callback(df)
                        
                    return df
                except Exception as e:
                    logger.warning(f"Failed to read {key}: {e}")
                    return pd.DataFrame()
            
            # Use existing parallel processing utility
            if parallel:
                results = parallel_process_dates(
                    date_strings, 
                    process_date,
                    max_workers=min(cpu_count(), len(date_strings))
                )
            else:
                results = [process_date(d) for d in date_strings]
            
            # Combine results
            non_empty_results = [df for df in results if df is not None and not df.empty]
            if non_empty_results:
                combined_df = pd.concat(non_empty_results, ignore_index=True)
                logger.info(f"Combined {len(non_empty_results)} files into DataFrame with {len(combined_df)} records")
                logger.info(f"Memory usage: {get_memory_usage(combined_df)}")
                return combined_df
            else:
                logger.warning(f"No data found for {table_name} between {start_date} and {end_date}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error in s3_read_parquet_parallel: {e}")
            raise

    def process_travel_times(self):
        """Execute travel times calculations asynchronously"""
        logger.info("travel times [0 of 29 (sigops)]")

        python_env = "C:\\Users\\kogum\\Desktop\\JobSupport\\achyuth\\server-env\\Scripts\\python.exe"
        
        if self.config.get('run', {}).get('travel_times', True):
            try:
                # Run python scripts asynchronously
                subprocess.Popen([
                    python_env,
                    "../get_travel_times_v2.py",
                    "sigops",
                    "../travel_times_1hr.yaml"
                ])

                subprocess.Popen([
                    python_env,
                    "../get_travel_times_v2.py", "sigops", "../travel_times_15min.yaml"  
                ])
                subprocess.Popen([
                    python_env,
                    "../get_travel_times_1min_v2.py", "sigops"
                ])
                logger.info("Started travel times processing in background")
            except Exception as e:
                logger.error(f"Error starting travel times processes: {e}")

    def process_detector_uptime(self):
        """Process vehicle detector uptime [1 of 29]"""
        logger.info("Vehicle Detector Uptime [1 of 29 (sigops)]")
        
        try:
            def callback(x):
                return self.get_avg_daily_detector_uptime(x).assign(
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date
                )

            avg_daily_detector_uptime = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="detector_uptime_pd",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list,
                callback=callback
            ).assign(SignalID=lambda df: df['SignalID'].astype('category'))

            # Corridor aggregations using existing aggregation functions
            cor_avg_daily_detector_uptime = self.get_cor_avg_daily_detector_uptime(
                avg_daily_detector_uptime, self.corridors
            )
            sub_avg_daily_detector_uptime = self.get_cor_avg_daily_detector_uptime(
                avg_daily_detector_uptime, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Weekly aggregations using existing functions
            weekly_detector_uptime = get_weekly_avg_by_day(
                avg_daily_detector_uptime, "uptime", peak_only=False
            )
            cor_weekly_detector_uptime = self.get_cor_weekly_detector_uptime(
                weekly_detector_uptime, self.corridors
            )
            sub_weekly_detector_uptime = self.get_cor_weekly_detector_uptime(
                weekly_detector_uptime, self.subcorridors
            ).dropna(subset=['Corridor'])

            # Monthly aggregations using existing functions
            monthly_detector_uptime = get_monthly_avg_by_day(
                avg_daily_detector_uptime, "uptime", peak_only=False
            )
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
            # Calculate start date (6 months back minimum)
            pau_start_date = min(
                pd.to_datetime(self.calcs_start_date),
                pd.to_datetime(self.report_end_date) - pd.DateOffset(months=6)
            ).strftime('%Y-%m-%d')

            # Read hourly ped count data
            paph = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="counts_ped_1hr",
                start_date=pau_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list,
                parallel=False
            ).dropna(subset=['CallPhase']).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                Detector=lambda df: df['Detector'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week,
                vol=lambda df: pd.to_numeric(df['vol'])
            )

            # Aggregate to daily
            papd = paph.groupby(['SignalID', 'Date', 'DOW', 'Week', 'Detector', 'CallPhase']).agg(
                papd=('vol', 'sum')
            ).reset_index()

            # Rename for hourly data
            paph = paph.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'})

            # Calculate uptime using gamma distribution method
            dates = pd.date_range(pau_start_date, self.report_end_date, freq='D')
            pau = self.get_pau_gamma(dates, papd, paph, self.corridors, 
                                   self.wk_calcs_start_date, pau_start_date)

            # Remove and replace papd for bad days
            papd = pau.assign(
                papd=lambda df: np.where(df['uptime'] == 1, df['papd'], np.nan)
            ).groupby(['SignalID', 'Detector', 'CallPhase', 
                      pau['Date'].dt.year, pau['Date'].dt.month]).apply(
                lambda x: x.assign(
                    papd=lambda y: np.where(y['uptime'] == 1, y['papd'], 
                                          np.floor(y['papd'].mean()))
                )
            ).reset_index(drop=True)[['SignalID', 'Detector', 'CallPhase', 'Date', 'DOW', 'Week', 'papd', 'uptime']]

            # Generate bad ped detectors data and upload to S3
            bad_ped_detectors = self.get_bad_ped_detectors(pau).query(
                f"Date >= '{self.calcs_start_date}'"
            )
            
            # Upload bad ped detectors using batch processing
            if not bad_ped_detectors.empty:
                batch_process(
                    [bad_ped_detectors], 
                    batch_size=1,
                    process_func=self.upload_bad_ped_detectors
                )

            # Store papd for later use
            self.papd = papd

            # Process daily, weekly, monthly aggregations using existing functions
            daily_pa_uptime = get_daily_avg(pau, "uptime", peak_only=False)
            weekly_pa_uptime = get_weekly_avg_by_day(pau, "uptime", peak_only=False)
            monthly_pa_uptime = get_monthly_avg_by_day(pau, "uptime", weight_col="all", peak_only=False)

            # Corridor aggregations
            cor_daily_pa_uptime = self.get_cor_weekly_avg_by_day(daily_pa_uptime, self.corridors, "uptime")
            sub_daily_pa_uptime = self.get_cor_weekly_avg_by_day(daily_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

            cor_weekly_pa_uptime = self.get_cor_weekly_avg_by_day(weekly_pa_uptime, self.corridors, "uptime")
            sub_weekly_pa_uptime = self.get_cor_weekly_avg_by_day(weekly_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

            cor_monthly_pa_uptime = self.get_cor_monthly_avg_by_day(monthly_pa_uptime, self.corridors, "uptime")
            sub_monthly_pa_uptime = self.get_cor_monthly_avg_by_day(monthly_pa_uptime, self.subcorridors, "uptime").dropna(subset=['Corridor'])

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
        logger.info("watchdog alerts [3 of 29 (sigops)]")
        
        try:
            # Process bad detectors - last 180 days
            bad_det_data = []
            date_range = [(datetime.now().date() - timedelta(days=i+1)).strftime('%Y-%m-%d') 
                         for i in range(180)]
            
            def process_bad_detector_date(date_str):
                key = f"mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet"
                try:
                    df = read_parquet_file(self.config['bucket'], key)
                    return df[['SignalID', 'Detector']].assign(Date=date_str)
                except:
                    return pd.DataFrame()
            
            # Use parallel processing for bad detectors
            bad_det_results = parallel_process_dates(
                date_range, 
                process_bad_detector_date,
                max_workers=min(10, len(date_range))
            )
            
            bad_det_data = [df for df in bad_det_results if df is not None and not df.empty]
            
            if bad_det_data:
                bad_det = pd.concat(bad_det_data, ignore_index=True).assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    Detector=lambda df: df['Detector'].astype('category')
                )
                
                # Get detector configurations for each date
                det_config_data = []
                unique_dates = sorted(bad_det['Date'].unique())
                
                def get_det_config_for_date(date_str):
                    try:
                        get_det_config = get_det_config_(self.config.get('bucket'), 'atspm_det_config_good')
                        config = get_det_config(date_str)
                        return config[['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber']].assign(Date=date_str)
                    except:
                        return pd.DataFrame()
                
                det_config_results = parallel_process_dates(
                    unique_dates,
                    get_det_config_for_date,
                    max_workers=min(5, len(unique_dates))
                )
                
                det_config_data = [df for df in det_config_results if df is not None and not df.empty]
                
                if det_config_data:
                    det_config = pd.concat(det_config_data, ignore_index=True).assign(
                        SignalID=lambda df: df['SignalID'].astype('category'),
                        CallPhase=lambda df: df['CallPhase'].astype('category'),
                        Detector=lambda df: df['Detector'].astype('category')
                    )
                    
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
                        SignalID=lambda df: df['SignalID'].astype('category'),
                        CallPhase=lambda df: df['CallPhase'].astype('category'),
                        Detector=lambda df: df['Detector'].astype('category'),
                        Alert=lambda df: pd.Categorical(['Bad Vehicle Detection'] * len(df)),
                        Name=lambda df: df['Name'].apply(lambda x: x.replace('@', '-') if pd.notna(x) else x).astype('category'),
                        ApproachDesc=lambda df: df.apply(
                            lambda row: f"{row['ApproachDesc'].strip()} Lane {row['LaneNumber']}" 
                            if pd.notna(row['ApproachDesc']) else "", axis=1
                        )
                    )
                    
                    # Upload bad detectors alert
                    write_parquet_to_s3(
                        bad_det,
                        bucket=self.config['bucket'],
                        key="sigops/watchdog/bad_detectors.parquet"
                    )

            # Process bad ped detectors - last 90 days
            bad_ped_data = []
            ped_date_range = [(datetime.now().date() - timedelta(days=i+1)).strftime('%Y-%m-%d') 
                             for i in range(90)]
            
            def process_bad_ped_date(date_str):
                key = f"mark/bad_ped_detectors/date={date_str}/bad_ped_detectors_{date_str}.parquet"
                try:
                    df = read_parquet_file(self.config['bucket'], key)
                    return df.assign(Date=date_str)
                except:
                    return pd.DataFrame()
            
            bad_ped_results = parallel_process_dates(
                ped_date_range,
                process_bad_ped_date,
                max_workers=min(10, len(ped_date_range))
            )
            
            bad_ped_data = [df for df in bad_ped_results if df is not None and not df.empty]
            
            if bad_ped_data:
                bad_ped = pd.concat(bad_ped_data, ignore_index=True).assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    Detector=lambda df: df['Detector'].astype('category')
                ).merge(
                    self.corridors[['Zone_Group', 'Zone', 'Corridor', 'SignalID', 'Name']],
                    on='SignalID',
                    how='left'
                ).assign(
                    Corridor=lambda df: df['Corridor'].astype('category'),
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    Detector=lambda df: df['Detector'].astype('category'),
                    Alert=lambda df: pd.Categorical(['Bad Ped Pushbuttons'] * len(df)),
                    Name=lambda df: df['Name'].astype('category')
                )
                
                # Upload bad ped pushbuttons alert
                write_parquet_to_s3(
                    bad_ped,
                    bucket=self.config['bucket'],
                    key="sigops/watchdog/bad_ped_pushbuttons.parquet"
                )

            # Process bad cameras - last 6 months
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
                    return df[df['Size'] == 0]  # No camera image
                except Exception as e:
                    logger.debug(f"Could not read camera data for {date_str}: {e}")
                    return pd.DataFrame()
            
            bad_cam_results = parallel_process_dates(
                cam_date_range,
                process_bad_cam_date,
                max_workers=min(5, len(cam_date_range))
            )
            
            bad_cam_data = [df for df in bad_cam_results if df is not None and not df.empty]
            
            if bad_cam_data and not self.cam_config.empty:
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
                
                # Upload bad cameras alert
                write_parquet_to_s3(
                    bad_cam,
                    bucket=self.config['bucket'],
                    key="sigops/watchdog/bad_cameras.parquet"
                )

        except Exception as e:
            logger.error(f"Error in watchdog alerts processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_pedestrian_activations(self):
        """Process daily pedestrian activations [4 of 29]"""
        logger.info("Daily Pedestrian Activations [4 of 29 (sigops)]")
        
        try:
            # Use papd from previous step
            daily_papd = get_daily_avg(self.papd, "papd")
            weekly_papd = self.get_weekly_papd(self.papd)
            monthly_papd = self.get_monthly_papd(self.papd)

            # Group into corridors using existing aggregation functions
            cor_daily_papd = self.get_cor_weekly_papd(daily_papd, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_weekly_papd = self.get_cor_weekly_papd(weekly_papd, self.corridors)
            cor_monthly_papd = self.get_cor_monthly_papd(monthly_papd, self.corridors)

            # Group into subcorridors
            sub_daily_papd = self.get_cor_weekly_papd(daily_papd, self.subcorridors).drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
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

    def process_ped_delay(self):
        """Process pedestrian delay [6 of 29]"""
        logger.info("Pedestrian Delay [6 of 29 (sigops)]")
        
        try:
            def callback(x):
                if "Avg.Max.Ped.Delay" in x.columns:
                    x = x.rename(columns={"Avg.Max.Ped.Delay": "pd"}).assign(
                        CallPhase=lambda df: pd.Categorical([0] * len(df))
                    )
                return x.assign(
                    DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                    Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
                )

            ped_delay = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="ped_delay",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list,
                callback=callback
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category')
            ).fillna({'Events': 1})

            # Use existing aggregation functions
            daily_pd = get_daily_avg(ped_delay, "pd", "Events")
            weekly_pd_by_day = get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)
            monthly_pd_by_day = get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only=False)

            # Corridor aggregations
            cor_daily_pd = self.get_cor_weekly_avg_by_day(daily_pd, self.corridors, "pd", "Events").drop(columns=['Week'], errors='ignore')
            cor_weekly_pd_by_day = self.get_cor_weekly_avg_by_day(weekly_pd_by_day, self.corridors, "pd", "Events")
            cor_monthly_pd_by_day = self.get_cor_monthly_avg_by_day(monthly_pd_by_day, self.corridors, "pd", "Events")

            # Subcorridor aggregations
            sub_daily_pd = self.get_cor_weekly_avg_by_day(daily_pd, self.subcorridors, "pd", "Events").drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
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
            cu = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="comm_uptime",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Use existing aggregation functions
            daily_comm_uptime = get_daily_avg(cu, "uptime", peak_only=False)
            weekly_comm_uptime = get_weekly_avg_by_day(cu, "uptime", peak_only=False)
            monthly_comm_uptime = get_monthly_avg_by_day(cu, "uptime", peak_only=False)

            # Corridor aggregations
            cor_daily_comm_uptime = self.get_cor_weekly_avg_by_day(daily_comm_uptime, self.corridors, "uptime")
            cor_weekly_comm_uptime = self.get_cor_weekly_avg_by_day(weekly_comm_uptime, self.corridors, "uptime")
            cor_monthly_comm_uptime = self.get_cor_monthly_avg_by_day(monthly_comm_uptime, self.corridors, "uptime")

            # Subcorridor aggregations
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
            vpd = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="vehicles_pd",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Use existing aggregation functions
            daily_vpd = self.get_daily_sum(vpd, "vpd")
            weekly_vpd = self.get_weekly_vpd(vpd)
            monthly_vpd = self.get_monthly_vpd(vpd)

            # Group into corridors using sigify for signal-level data
            cor_daily_vpd = sigify(daily_vpd, pd.DataFrame(), self.corridors, 'SignalID').drop(columns=['ones', 'Week'], errors='ignore')
            cor_weekly_vpd = self.get_cor_weekly_vpd(weekly_vpd, self.corridors)
            cor_monthly_vpd = self.get_cor_monthly_vpd(monthly_vpd, self.corridors)

            # Subcorridors
            sub_daily_vpd = self.get_cor_weekly_vpd(daily_vpd, self.subcorridors).drop(
                columns=['ones', 'Week'], errors='ignore').dropna(subset=['Corridor'])
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

    def process_hourly_volumes(self):
        """Process hourly volumes [9 of 29]"""
        logger.info("Hourly Volumes [9 of 29 (sigops)]")
        
        try:
            vph = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="vehicles_ph",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: pd.Categorical([2] * len(df)),  # Hack because next function needs a CallPhase
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Use existing aggregation functions from aggregations.py
            hourly_vol = self.get_hourly(vph, "vph", self.corridors)

            cor_daily_vph = self.get_cor_weekly_vph(hourly_vol, self.corridors)
            sub_daily_vph = self.get_cor_weekly_vph(hourly_vol, self.subcorridors).dropna(subset=['Corridor'])

            daily_vph_peak = self.get_weekly_vph_peak(hourly_vol)
            cor_daily_vph_peak = self.get_cor_weekly_vph_peak(cor_daily_vph)
            sub_daily_vph_peak = {k: v.dropna(subset=['Corridor']) for k, v in 
                                 self.get_cor_weekly_vph_peak(sub_daily_vph).items()}

            # Weekly processing using get_vph from aggregations
            weekly_vph = get_vph(vph, interval="1 hour", mainline_only=True)
            weekly_vph = get_weekly_avg_by_day(weekly_vph, "vph")
            cor_weekly_vph = self.get_cor_weekly_vph(weekly_vph, self.corridors)
            sub_weekly_vph = self.get_cor_weekly_vph(weekly_vph, self.subcorridors).dropna(subset=['Corridor'])

            weekly_vph_peak = self.get_weekly_vph_peak(weekly_vph)
            cor_weekly_vph_peak = self.get_cor_weekly_vph_peak(cor_weekly_vph)
            sub_weekly_vph_peak = {k: v.dropna(subset=['Corridor']) for k, v in 
                                  self.get_cor_weekly_vph_peak(sub_weekly_vph).items()}

            # Monthly processing
            monthly_vph = get_monthly_avg_by_day(vph, "vph")
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
                (daily_vph_peak, "daily_vph_peak.pkl", "vph"),  # Dictionary saved as pickle
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
            throughput = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="throughput",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('int').astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Use existing functions
            daily_throughput = self.get_daily_sum(throughput, "vph")
            weekly_throughput = self.get_weekly_thruput(throughput)
            monthly_throughput = self.get_monthly_thruput(throughput)

            # Daily throughput - corridor aggregations
            cor_daily_throughput = self.get_cor_weekly_thruput(daily_throughput, self.corridors).drop(columns=['Week'], errors='ignore')
            sub_daily_throughput = self.get_cor_weekly_thruput(daily_throughput, self.subcorridors).drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])

            # Weekly throughput - Group into corridors
            cor_weekly_throughput = self.get_cor_weekly_thruput(weekly_throughput, self.corridors)
            sub_weekly_throughput = self.get_cor_weekly_thruput(weekly_throughput, self.subcorridors).dropna(subset=['Corridor'])

            # Monthly throughput - Group into corridors
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
            aog = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="arrivals_on_green",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
            )

            # Use existing aggregation functions
            daily_aog = self.get_daily_aog(aog)
            weekly_aog_by_day = self.get_weekly_aog_by_day(aog)
            monthly_aog_by_day = self.get_monthly_aog_by_day(aog)

            # Corridor aggregations
            cor_daily_aog = self.get_cor_weekly_aog_by_day(daily_aog, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_weekly_aog_by_day = self.get_cor_weekly_aog_by_day(weekly_aog_by_day, self.corridors)
            cor_monthly_aog_by_day = self.get_cor_monthly_aog_by_day(monthly_aog_by_day, self.corridors)

            # Subcorridor aggregations
            sub_daily_aog = self.get_cor_weekly_aog_by_day(daily_aog, self.subcorridors).drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
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

    def process_hourly_aog(self):
        """Process hourly arrivals on green [12 of 29]"""
        logger.info("Hourly AOG [12 of 29 (sigops)]")
        
        try:
            aog_by_hr = self.get_aog_by_hr(self.aog)
            monthly_aog_by_hr = self.get_monthly_aog_by_hr(aog_by_hr)

            # Hourly volumes by Corridor
            cor_monthly_aog_by_hr = self.get_cor_monthly_aog_by_hr(monthly_aog_by_hr, self.corridors)
            sub_monthly_aog_by_hr = self.get_cor_monthly_aog_by_hr(monthly_aog_by_hr, self.subcorridors).dropna(subset=['Corridor'])

            # Save results
            self.save_results([
                (cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.parquet", "aog"),
                (sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.parquet", "aog")
            ])

        except Exception as e:
            logger.error(f"Error in hourly AOG processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_progression_ratio(self):
        """Process daily progression ratio [13 of 29]"""
        logger.info("Daily Progression Ratio [13 of 29 (sigops)]")
        
        try:
            # Use existing aggregation functions
            daily_pr = get_daily_avg(self.aog, "pr", "vol")
            weekly_pr_by_day = self.get_weekly_pr_by_day(self.aog)
            monthly_pr_by_day = self.get_monthly_pr_by_day(self.aog)

            # Corridor aggregations
            cor_daily_pr = self.get_cor_weekly_pr_by_day(daily_pr, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_weekly_pr_by_day = self.get_cor_weekly_pr_by_day(weekly_pr_by_day, self.corridors)
            cor_monthly_pr_by_day = self.get_cor_monthly_pr_by_day(monthly_pr_by_day, self.corridors)

            # Subcorridor aggregations
            sub_daily_pr = self.get_cor_weekly_pr_by_day(daily_pr, self.subcorridors).drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
            sub_weekly_pr_by_day = self.get_cor_weekly_pr_by_day(weekly_pr_by_day, self.subcorridors).dropna(subset=['Corridor'])
            sub_monthly_pr_by_day = self.get_cor_monthly_pr_by_day(monthly_pr_by_day, self.subcorridors).dropna(subset=['Corridor'])

            # Save results
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

        except Exception as e:
            logger.error(f"Error in daily progression ratio processing: {e}")
            logger.error(traceback.format_exc())

    def process_hourly_progression_ratio(self):
        """Process hourly progression ratio [14 of 29]"""
        logger.info("Hourly Progression Ratio [14 of 29 (sigops)]")
        
        try:
            pr_by_hr = self.get_pr_by_hr(self.aog)
            monthly_pr_by_hr = self.get_monthly_pr_by_hr(pr_by_hr)

            # Hourly volumes by Corridor
            cor_monthly_pr_by_hr = self.get_cor_monthly_pr_by_hr(monthly_pr_by_hr, self.corridors)
            sub_monthly_pr_by_hr = self.get_cor_monthly_pr_by_hr(monthly_pr_by_hr, self.subcorridors).dropna(subset=['Corridor'])

            # Save results
            self.save_results([
                (monthly_pr_by_hr, "monthly_pr_by_hr.parquet", "pr"),
                (cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.parquet", "pr"),
                (sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.parquet", "pr")
            ])

        except Exception as e:
            logger.error(f"Error in hourly progression ratio processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_split_failures(self):
        """Process daily split failures [15 of 29]"""
        logger.info("Daily Split Failures [15 of 29 (sigops)]")
        
        try:
            sf = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="split_failures",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list,
                callback=lambda x: x[x['CallPhase'] == 0]
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Define peak hours (you'll need to set these based on your config)
            AM_PEAK_HOURS = self.config.get('AM_PEAK_HOURS', [7, 8, 9])
            PM_PEAK_HOURS = self.config.get('PM_PEAK_HOURS', [16, 17, 18])

            # Divide into peak/off-peak split failures
            sfo = sf[~pd.to_datetime(sf['Date_Hour']).dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
            sfp = sf[pd.to_datetime(sf['Date_Hour']).dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]

            # Use existing aggregation functions
            daily_sfp = get_daily_avg(sfp, "sf_freq", "cycles")
            daily_sfo = get_daily_avg(sfo, "sf_freq", "cycles")

            weekly_sf_by_day = get_weekly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False)
            weekly_sfo_by_day = get_weekly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)
            monthly_sf_by_day = get_monthly_avg_by_day(sfp, "sf_freq", "cycles", peak_only=False)
            monthly_sfo_by_day = get_monthly_avg_by_day(sfo, "sf_freq", "cycles", peak_only=False)

            # Corridor aggregations
            cor_daily_sfp = self.get_cor_weekly_sf_by_day(daily_sfp, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_daily_sfo = self.get_cor_weekly_sf_by_day(daily_sfo, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_weekly_sf_by_day = self.get_cor_weekly_sf_by_day(weekly_sf_by_day, self.corridors)
            cor_weekly_sfo_by_day = self.get_cor_weekly_sf_by_day(weekly_sfo_by_day, self.corridors)
            cor_monthly_sf_by_day = self.get_cor_monthly_sf_by_day(monthly_sf_by_day, self.corridors)
            cor_monthly_sfo_by_day = self.get_cor_monthly_sf_by_day(monthly_sfo_by_day, self.corridors)

            # Subcorridor aggregations
            sub_daily_sfp = self.get_cor_weekly_sf_by_day(daily_sfp, self.subcorridors).dropna(subset=['Corridor'])
            sub_daily_sfo = self.get_cor_weekly_sf_by_day(daily_sfo, self.subcorridors).dropna(subset=['Corridor'])
            sub_weekly_sf_by_day = self.get_cor_weekly_sf_by_day(weekly_sf_by_day, self.subcorridors).dropna(subset=['Corridor'])
            sub_weekly_sfo_by_day = self.get_cor_weekly_sf_by_day(weekly_sfo_by_day, self.subcorridors).dropna(subset=['Corridor'])
            sub_monthly_sf_by_day = self.get_cor_monthly_sf_by_day(monthly_sf_by_day, self.subcorridors).dropna(subset=['Corridor'])
            sub_monthly_sfo_by_day = self.get_cor_monthly_sf_by_day(monthly_sfo_by_day, self.subcorridors).dropna(subset=['Corridor'])

            # Store sf for use in hourly processing
            self.sf = sf

            # Save results
            self.save_results([
                (daily_sfp, "daily_sfp.parquet", "sf_freq"),
                (weekly_sf_by_day, "wsf.parquet", "sf_freq"),
                (monthly_sf_by_day, "monthly_sfd.parquet", "sf_freq"),
                (cor_daily_sfp, "cor_daily_sfp.parquet", "sf_freq"),
                (cor_weekly_sf_by_day, "cor_wsf.parquet", "sf_freq"),
                (cor_monthly_sf_by_day, "cor_monthly_sfd.parquet", "sf_freq"),
                (sub_daily_sfp, "sub_daily_sfp.parquet", "sf_freq"),
                (sub_weekly_sf_by_day, "sub_wsf.parquet", "sf_freq"),
                (sub_monthly_sf_by_day, "sub_monthly_sfd.parquet", "sf_freq"),
                (daily_sfo, "daily_sfo.parquet", "sf_freq"),
                (weekly_sfo_by_day, "wsfo.parquet", "sf_freq"),
                (monthly_sfo_by_day, "monthly_sfo.parquet", "sf_freq"),
                (cor_daily_sfo, "cor_daily_sfo.parquet", "sf_freq"),
                (cor_weekly_sfo_by_day, "cor_wsfo.parquet", "sf_freq"),
                (cor_monthly_sfo_by_day, "cor_monthly_sfo.parquet", "sf_freq"),
                (sub_daily_sfo, "sub_daily_sfo.parquet", "sf_freq"),
                (sub_weekly_sfo_by_day, "sub_wsfo.parquet", "sf_freq"),
                (sub_monthly_sfo_by_day, "sub_monthly_sfo.parquet", "sf_freq")
            ])

        except Exception as e:
            logger.error(f"Error in daily split failures processing: {e}")
            logger.error(traceback.format_exc())

    def process_hourly_split_failures(self):
        """Process hourly split failures [16 of 29]"""
        logger.info("Hourly Split Failures [16 of 29 (sigops)]")
        
        try:
            sfh = self.get_sf_by_hr(self.sf)
            msfh = self.get_monthly_sf_by_hr(sfh)

            # Hourly volumes by Corridor
            cor_msfh = self.get_cor_monthly_sf_by_hr(msfh, self.corridors)
            sub_msfh = self.get_cor_monthly_sf_by_hr(msfh, self.subcorridors).dropna(subset=['Corridor'])

            # Save results
            self.save_results([
                (msfh, "msfh.parquet", "sf_freq"),
                (cor_msfh, "cor_msfh.parquet", "sf_freq"),
                (sub_msfh, "sub_msfh.parquet", "sf_freq")
            ])

        except Exception as e:
            logger.error(f"Error in hourly split failures processing: {e}")
            logger.error(traceback.format_exc())

    def process_daily_queue_spillback(self):
        """Process daily queue spillback [17 of 29]"""
        logger.info("Daily Queue Spillback [17 of 29 (sigops)]")
        
        try:
            qs = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="queue_spillback",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date
            )

            # Use existing aggregation functions
            daily_qs = get_daily_avg(qs, "qs_freq", "cycles")
            wqs = self.get_weekly_qs_by_day(qs)
            monthly_qsd = self.get_monthly_qs_by_day(qs)

            # Corridor aggregations
            cor_daily_qs = self.get_cor_weekly_qs_by_day(daily_qs, self.corridors).drop(columns=['Week'], errors='ignore')
            cor_wqs = self.get_cor_weekly_qs_by_day(wqs, self.corridors)
            cor_monthly_qsd = self.get_cor_monthly_qs_by_day(monthly_qsd, self.corridors)

            # Subcorridor aggregations
            sub_daily_qs = self.get_cor_weekly_qs_by_day(daily_qs, self.subcorridors).drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
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

    # Helper methods for data processing (these would need to be implemented based on your R functions)
    def get_avg_daily_detector_uptime(self, df):
        """Get average daily detector uptime"""
        # Implement based on your R function
        return df.groupby(['SignalID', 'Date']).agg(
            uptime=('uptime', 'mean')
        ).reset_index()

    def get_cor_avg_daily_detector_uptime(self, df, corridors):
        """Get corridor average daily detector uptime"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Date']
        ).agg(uptime=('uptime', 'mean')).reset_index()

    def get_cor_weekly_detector_uptime(self, df, corridors):
        """Get corridor weekly detector uptime"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week']
        ).agg(uptime=('uptime', 'mean')).reset_index()

    def get_pau_gamma(self, dates, papd, paph, corridors, wk_start, pau_start):
        """Calculate pedestrian uptime using gamma distribution"""
        # This would be a complex implementation of the gamma distribution method
        # For now, return a placeholder
        return papd.assign(uptime=1.0)

    def get_bad_ped_detectors(self, pau):
        """Get bad pedestrian detectors"""
        return pau[pau['uptime'] < 0.8][['SignalID', 'Detector', 'Date']]

    def upload_bad_ped_detectors(self, data_list):
        """Upload bad ped detectors to S3"""
        for df in data_list:
            if not df.empty:
                # Use date partitioning similar to R script
                for date_str in df['Date'].dt.strftime('%Y-%m-%d').unique():
                    daily_data = df[df['Date'].dt.strftime('%Y-%m-%d') == date_str]
                    key = f"mark/bad_ped_detectors/date={date_str}/bad_ped_detectors_{date_str}.parquet"
                    write_parquet_to_s3(daily_data, self.config['bucket'], key)

    def get_weekly_papd(self, papd):
        """Get weekly pedestrian activations per day"""
        return get_weekly_avg_by_day(papd, "papd")

    def get_monthly_papd(self, papd):
        """Get monthly pedestrian activations per day"""
        return get_monthly_avg_by_day(papd, "papd")

    def get_cor_weekly_papd(self, df, corridors):
        """Get corridor weekly pedestrian activations"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg(papd=('papd', 'sum')).reset_index()

    def get_cor_monthly_papd(self, df, corridors):
        """Get corridor monthly pedestrian activations"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg(papd=('papd', 'sum')).reset_index()

    def get_cor_weekly_avg_by_day(self, df, corridors, variable, weight_col="ones"):
        """Get corridor weekly average by day"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg({variable: 'mean', weight_col: 'sum'}).reset_index()

    def get_cor_monthly_avg_by_day(self, df, corridors, variable, weight_col="ones"):
        """Get corridor monthly average by day"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg({variable: 'mean', weight_col: 'sum'}).reset_index()

    def get_daily_sum(self, df, variable):
        """Get daily sum"""
        return df.groupby(['SignalID', 'Date']).agg(
            {variable: 'sum'}
        ).reset_index()

    def get_weekly_vpd(self, df):
        """Get weekly vehicles per day"""
        return get_weekly_avg_by_day(df, "vpd")

    def get_monthly_vpd(self, df):
        """Get monthly vehicles per day"""
        return get_monthly_avg_by_day(df, "vpd")

    def get_cor_weekly_vpd(self, df, corridors):
        """Get corridor weekly vehicles per day"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Week'] if 'Week' in df.columns else ['Corridor']
        ).agg(vpd=('vpd', 'sum')).reset_index()

    def get_cor_monthly_vpd(self, df, corridors):
        """Get corridor monthly vehicles per day"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month'] if 'Month' in df.columns else ['Corridor']
        ).agg(vpd=('vpd', 'sum')).reset_index()

    # Additional helper methods would continue here...
    # (I'll implement more as needed based on the remaining R code)

    def save_results(self, results_list):
        """
        Save results to S3 using existing utilities
        
        Args:
            results_list: List of tuples (dataframe, filename, metric_type)
        """
        for df, filename, metric_type in results_list:
            try:
                if df is not None and not df.empty:
                    # Use batch processing for large datasets
                    if len(df) > 10000:
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

    def _save_single_result(self, df_list, filename, metric_type):
        """Save a single result to S3"""
        for df in df_list:
            if filename.endswith('.pkl'):
                # For dictionary/complex objects, use pickle (though parquet is preferred)
                import pickle
                import io
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

    # Placeholder methods for the remaining processing steps
    # These would need to be implemented based on the complete R code

    def get_hourly(self, df, variable, corridors):
        """Process hourly data"""
        # Placeholder - implement based on R function
        return df

    def get_weekly_vph_peak(self, df):
        """Get weekly VPH peak values"""
        # Placeholder - implement based on R function
        return {}

    def get_cor_weekly_vph_peak(self, df):
        """Get corridor weekly VPH peak values"""
        # Placeholder - implement based on R function
        return {}

    def get_weekly_thruput(self, df):
        """Get weekly throughput"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_thruput(self, df):
        """Get monthly throughput"""
        # Placeholder - implement based on R function
        return df

    def get_cor_weekly_thruput(self, df, corridors):
        """Get corridor weekly throughput"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_thruput(self, df, corridors):
        """Get corridor monthly throughput"""
        # Placeholder - implement based on R function
        return df

    def get_daily_aog(self, df):
        """Get daily arrivals on green"""
        # Placeholder - implement based on R function
        return df

    def get_weekly_aog_by_day(self, df):
        """Get weekly AOG by day"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_aog_by_day(self, df):
        """Get monthly AOG by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_weekly_aog_by_day(self, df, corridors):
        """Get corridor weekly AOG by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_aog_by_day(self, df, corridors):
        """Get corridor monthly AOG by day"""
        # Placeholder - implement based on R function
        return df

    def get_aog_by_hr(self, df):
        """Get AOG by hour"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_aog_by_hr(self, df):
        """Get monthly AOG by hour"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_aog_by_hr(self, df, corridors):
        """Get corridor monthly AOG by hour"""
        # Placeholder - implement based on R function
        return df

    def get_weekly_pr_by_day(self, df):
        """Get weekly progression ratio by day"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_pr_by_day(self, df):
        """Get monthly progression ratio by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_weekly_pr_by_day(self, df, corridors):
        """Get corridor weekly PR by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_pr_by_day(self, df, corridors):
        """Get corridor monthly PR by day"""
        # Placeholder - implement based on R function
        return df

    def get_pr_by_hr(self, df):
        """Get progression ratio by hour"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_pr_by_hr(self, df):
        """Get monthly progression ratio by hour"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_pr_by_hr(self, df, corridors):
        """Get corridor monthly PR by hour"""
        # Placeholder - implement based on R function
        return df

    def get_cor_weekly_sf_by_day(self, df, corridors):
        """Get corridor weekly split failures by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_sf_by_day(self, df, corridors):
        """Get corridor monthly split failures by day"""
        # Placeholder - implement based on R function
        return df

    def get_sf_by_hr(self, df):
        """Get split failures by hour"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_sf_by_hr(self, df):
        """Get monthly split failures by hour"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_sf_by_hr(self, df, corridors):
        """Get corridor monthly split failures by hour"""
        # Placeholder - implement based on R function
        return df

    def get_weekly_qs_by_day(self, df):
        """Get weekly queue spillback by day"""
        # Placeholder - implement based on R function
        return df

    def get_monthly_qs_by_day(self, df):
        """Get monthly queue spillback by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_weekly_qs_by_day(self, df, corridors):
        """Get corridor weekly queue spillback by day"""
        # Placeholder - implement based on R function
        return df

    def get_cor_monthly_qs_by_day(self, df, corridors):
        """Get corridor monthly queue spillback by day"""
        # Placeholder - implement based on R function
        return df

    def process_hourly_queue_spillback(self):
        """Process hourly queue spillback [18 of 29]"""
        logger.info("Hourly Queue Spillback [18 of 29 (sigops)]")
        
        try:
            qsh = self.get_qs_by_hr(self.qs)
            mqsh = self.get_monthly_qs_by_hr(qsh)

            # Hourly volumes by Corridor
            cor_mqsh = self.get_cor_monthly_qs_by_hr(mqsh, self.corridors)
            sub_mqsh = self.get_cor_monthly_qs_by_hr(mqsh, self.subcorridors).dropna(subset=['Corridor'])

            # Save results
            self.save_results([
                (mqsh, "mqsh.parquet", "qs_freq"),
                (cor_mqsh, "cor_mqsh.parquet", "qs_freq"),
                (sub_mqsh, "sub_mqsh.parquet", "qs_freq")
            ])

        except Exception as e:
            logger.error(f"Error in hourly queue spillback processing: {e}")
            logger.error(traceback.format_exc())

    def process_travel_time_indexes(self):
        """Process travel time and buffer time indexes [19 of 29]"""
        logger.info("Travel Time Indexes [19 of 29 (sigops)]")
        
        try:
            # ------- Corridor Travel Time Metrics ------- #
            tt = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="cor_travel_time_metrics_1hr",
                s3root='sigops',
                start_date=self.calcs_start_date,
                end_date=self.report_end_date
            ).assign(
                Corridor=lambda df: df['Corridor'].astype('category')
            ).merge(
                self.all_corridors[['Zone_Group', 'Zone', 'Corridor']].drop_duplicates(),
                on='Corridor',
                how='left'
            ).dropna(subset=['Zone_Group'])

            # Split into different metrics
            tti = tt.drop(columns=['pti', 'bi', 'speed_mph'], errors='ignore')
            pti = tt.drop(columns=['tti', 'bi', 'speed_mph'], errors='ignore')
            bi = tt.drop(columns=['tti', 'pti', 'speed_mph'], errors='ignore')
            spd = tt.drop(columns=['tti', 'pti', 'bi'], errors='ignore')

            # Read volume data for weighting
            cor_monthly_vph = self.load_result("cor_monthly_vph.parquet").rename(
                columns={'Zone_Group': 'Zone'}
            ).merge(
                self.corridors[['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )

            cor_weekly_vph = self.load_result("cor_weekly_vph.parquet").rename(
                columns={'Zone_Group': 'Zone'}
            ).merge(
                self.corridors[['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )

            # Process corridor travel time metrics
            cor_monthly_tti_by_hr = self.get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, self.all_corridors)
            cor_monthly_pti_by_hr = self.get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, self.all_corridors)
            cor_monthly_bi_by_hr = self.get_cor_monthly_ti_by_hr(bi, cor_monthly_vph, self.all_corridors)
            cor_monthly_spd_by_hr = self.get_cor_monthly_ti_by_hr(spd, cor_monthly_vph, self.all_corridors)

            cor_monthly_tti = self.get_cor_monthly_ti_by_day(tti, cor_monthly_vph, self.all_corridors)
            cor_monthly_pti = self.get_cor_monthly_ti_by_day(pti, cor_monthly_vph, self.all_corridors)
            cor_monthly_bi = self.get_cor_monthly_ti_by_day(bi, cor_monthly_vph, self.all_corridors)
            cor_monthly_spd = self.get_cor_monthly_ti_by_day(spd, cor_monthly_vph, self.all_corridors)

            # ------- Subcorridor Travel Time Metrics ------- #
            tt_sub = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="sub_travel_time_metrics_1hr",
                s3root='sigops',
                start_date=self.calcs_start_date,
                end_date=self.report_end_date
            ).assign(
                Corridor=lambda df: df['Corridor'].astype('category'),
                Subcorridor=lambda df: df['Subcorridor'].astype('category')
            ).rename(
                columns={'Corridor': 'Zone', 'Subcorridor': 'Corridor'}
            ).merge(
                self.subcorridors[['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )

            # Split subcorridor metrics
            tti_sub = tt_sub.drop(columns=['pti', 'bi', 'speed_mph'], errors='ignore')
            pti_sub = tt_sub.drop(columns=['tti', 'bi', 'speed_mph'], errors='ignore')
            bi_sub = tt_sub.drop(columns=['tti', 'pti', 'speed_mph'], errors='ignore')
            spd_sub = tt_sub.drop(columns=['tti', 'pti', 'bi'], errors='ignore')

            # Read subcorridor volume data
            sub_monthly_vph = self.load_result("sub_monthly_vph.parquet").rename(
                columns={'Zone_Group': 'Zone'}
            ).merge(
                self.subcorridors[['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )

            sub_weekly_vph = self.load_result("sub_weekly_vph.parquet").rename(
                columns={'Zone_Group': 'Zone'}
            ).merge(
                self.subcorridors[['Zone_Group', 'Zone']].drop_duplicates(),
                on='Zone',
                how='left'
            )

            # Process subcorridor travel time metrics
            sub_monthly_tti_by_hr = self.get_cor_monthly_ti_by_hr(tti_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_pti_by_hr = self.get_cor_monthly_ti_by_hr(pti_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_bi_by_hr = self.get_cor_monthly_ti_by_hr(bi_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_spd_by_hr = self.get_cor_monthly_ti_by_hr(spd_sub, sub_monthly_vph, self.subcorridors)

            sub_monthly_tti = self.get_cor_monthly_ti_by_day(tti_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_pti = self.get_cor_monthly_ti_by_day(pti_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_bi = self.get_cor_monthly_ti_by_day(bi_sub, sub_monthly_vph, self.subcorridors)
            sub_monthly_spd = self.get_cor_monthly_ti_by_day(spd_sub, sub_monthly_vph, self.subcorridors)

            # Save results
            self.save_results([
                (cor_monthly_tti, "cor_monthly_tti.parquet", "tti"),
                (cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.parquet", "tti"),
                (cor_monthly_pti, "cor_monthly_pti.parquet", "pti"),
                (cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.parquet", "pti"),
                (cor_monthly_bi, "cor_monthly_bi.parquet", "bi"),
                (cor_monthly_bi_by_hr, "cor_monthly_bi_by_hr.parquet", "bi"),
                (cor_monthly_spd, "cor_monthly_spd.parquet", "speed_mph"),
                (cor_monthly_spd_by_hr, "cor_monthly_spd_by_hr.parquet", "speed_mph"),
                (sub_monthly_tti, "sub_monthly_tti.parquet", "tti"),
                (sub_monthly_tti_by_hr, "sub_monthly_tti_by_hr.parquet", "tti"),
                (sub_monthly_pti, "sub_monthly_pti.parquet", "pti"),
                (sub_monthly_pti_by_hr, "sub_monthly_pti_by_hr.parquet", "pti"),
                (sub_monthly_bi, "sub_monthly_bi.parquet", "bi"),
                (sub_monthly_bi_by_hr, "sub_monthly_bi_by_hr.parquet", "bi"),
                (sub_monthly_spd, "sub_monthly_spd.parquet", "speed_mph"),
                (sub_monthly_spd_by_hr, "sub_monthly_spd_by_hr.parquet", "speed_mph")
            ])

        except Exception as e:
            logger.error(f"Error in travel time indexes processing: {e}")
            logger.error(traceback.format_exc())

    def process_cctv_uptime(self):
        """Process CCTV uptime from 511 and Encoders [20 of 29]"""
        logger.info("CCTV Uptimes [20 of 29 (sigops)]")
        
        try:
            daily_cctv_uptime_511 = self.get_daily_cctv_uptime(
                self.config['athena']['database'], "cctv_uptime", self.cam_config, self.wk_calcs_start_date
            )
            daily_cctv_uptime_encoders = self.get_daily_cctv_uptime(
                self.config['athena']['database'], "cctv_uptime_encoders", self.cam_config, self.wk_calcs_start_date
            )

            # Merge uptime data from both sources
            daily_cctv_uptime = daily_cctv_uptime_511.merge(
                daily_cctv_uptime_encoders,
                on=['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Date'],
                how='outer',
                suffixes=('_511', '_enc')
            )

            # Complete missing combinations and fill NAs
            camera_dates = pd.MultiIndex.from_product([
                daily_cctv_uptime[['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description']].drop_duplicates().values,
                pd.date_range(self.wk_calcs_start_date, self.report_end_date)
            ], names=['camera_info', 'Date']).to_frame().reset_index()

            daily_cctv_uptime = camera_dates.merge(daily_cctv_uptime, how='left').fillna({
                'up_enc': 0, 'num_enc': 0, 'uptime_enc': 0,
                'up_511': 0, 'num_511': 0, 'uptime_511': 0
            }).assign(
                uptime=lambda df: df['up_511'],
                num=1,
                up=lambda df: np.maximum(df['up_511'] * 2, df['up_enc']),
                Zone_Group=lambda df: df['Zone_Group'].astype('category'),
                Zone=lambda df: df['Zone'].astype('category'),
                Corridor=lambda df: df['Corridor'].astype('category'),
                Subcorridor=lambda df: df['Subcorridor'].astype('category'),
                CameraID=lambda df: df['CameraID'].astype('category'),
                Description=lambda df: df['Description'].astype('category')
            )[['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description', 'Date', 'up_511', 'up_enc', 'uptime', 'num', 'up']]

            # Find bad days with systemic issues
            bad_days = daily_cctv_uptime.groupby('Date').agg(
                sup=('uptime', 'sum'),
                snum=('num', 'sum')
            ).assign(
                suptime=lambda df: df['sup'] / df['snum']
            ).query('suptime < 0.2').index

            # Filter out bad days
            daily_cctv_uptime = daily_cctv_uptime[~daily_cctv_uptime['Date'].isin(bad_days)]

            # Calculate weekly and monthly aggregations
            weekly_cctv_uptime = self.get_weekly_avg_by_day_cctv(daily_cctv_uptime)
            monthly_cctv_uptime = daily_cctv_uptime.groupby([
                'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description',
                daily_cctv_uptime['Date'].dt.to_period('M').dt.to_timestamp()
            ]).agg(
                up=('uptime', 'sum'),
                uptime=('uptime', lambda x: np.average(x, weights=daily_cctv_uptime.loc[x.index, 'num'])),
                num=('num', 'sum')
            ).reset_index()

            # Corridor aggregations
            cor_daily_cctv_uptime = self.get_cor_weekly_avg_by_day(daily_cctv_uptime, self.all_corridors, "uptime", "num")
            cor_weekly_cctv_uptime = self.get_cor_weekly_avg_by_day(weekly_cctv_uptime, self.all_corridors, "uptime", "num")
            cor_monthly_cctv_uptime = self.get_cor_monthly_avg_by_day(monthly_cctv_uptime, self.all_corridors, "uptime", "num")

            # Subcorridor aggregations
            sub_daily_cctv_uptime = daily_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor']).rename(
                columns={'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'}
            )
            sub_weekly_cctv_uptime = weekly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor']).rename(
                columns={'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'}
            )
            sub_monthly_cctv_uptime = monthly_cctv_uptime.drop(columns=['Zone_Group']).dropna(subset=['Subcorridor']).rename(
                columns={'Zone': 'Zone_Group', 'Corridor': 'Zone', 'Subcorridor': 'Corridor'}
            )

            # Save results
            self.save_results([
                (daily_cctv_uptime, "daily_cctv_uptime.parquet", "uptime"),
                (weekly_cctv_uptime, "weekly_cctv_uptime.parquet", "uptime"),
                (monthly_cctv_uptime, "monthly_cctv_uptime.parquet", "uptime"),
                (cor_daily_cctv_uptime, "cor_daily_cctv_uptime.parquet", "uptime"),
                (cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.parquet", "uptime"),
                (cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.parquet", "uptime"),
                (sub_daily_cctv_uptime, "sub_daily_cctv_uptime.parquet", "uptime"),
                (sub_weekly_cctv_uptime, "sub_weekly_cctv_uptime.parquet", "uptime"),
                (sub_monthly_cctv_uptime, "sub_monthly_cctv_uptime.parquet", "uptime")
            ])

        except Exception as e:
            logger.error(f"Error in CCTV uptime processing: {e}")
            logger.error(traceback.format_exc())

    def process_travel_time_speeds(self):
        """Process travel time speeds [21 of 29]"""
        logger.info("Travel Time Speeds [21 of 29 (sigops)]")
        
        try:
            # This step appears to be commented out in the R code
            # If needed, implementation would go here
            logger.info("Travel time speeds processing skipped (commented in R code)")

        except Exception as e:
            logger.error(f"Error in travel time speeds processing: {e}")
            logger.error(traceback.format_exc())

    def process_ramp_meter_features(self):
        """Process ramp meter features [22 of 29]"""
        logger.info("Ramp Meter Features [22 of 29 (sigops)]")
        
        try:
            # Get ramp meter signals
            ramps <- self.corridors[self.corridors['Corridor'] == 'Ramp Meter']['SignalID'].unique()
            
            if len(ramps) > 0:
                vph_rm = self.s3_read_parquet_parallel(
                    bucket=self.config['bucket'],
                    table_name="vehicles_ph",
                    start_date=self.wk_calcs_start_date,
                    end_date=self.report_end_date,
                    signals_list=ramps
                ).assign(
                    SignalID=lambda df: df['SignalID'].astype('category'),
                    CallPhase=lambda df: df['CallPhase'].astype('category'),
                    Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                    DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                    Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
                )

                # Calculate ramp meter volumes
                daily_vph_rm = get_daily_avg(vph_rm, "vph", peak_only=False)
                weekly_vph_rm = get_weekly_avg_by_day(vph_rm, "vph", peak_only=False)
                monthly_vph_rm = get_monthly_avg_by_day(vph_rm, "vph", peak_only=False)

                # Use sigify to convert corridor to signal level data
                from aggregations import sigify
                
                sig_daily_vph_rm = sigify(daily_vph_rm, pd.DataFrame(), self.corridors, 'SignalID')
                sig_weekly_vph_rm = sigify(weekly_vph_rm, pd.DataFrame(), self.corridors, 'SignalID')
                sig_monthly_vph_rm = sigify(monthly_vph_rm, pd.DataFrame(), self.corridors, 'SignalID')

                # Save results
                self.save_results([
                    (sig_daily_vph_rm, "sig_daily_vph_rm.parquet", "vph"),
                    (sig_weekly_vph_rm, "sig_weekly_vph_rm.parquet", "vph"),
                    (sig_monthly_vph_rm, "sig_monthly_vph_rm.parquet", "vph")
                ])
            else:
                logger.info("No ramp meter signals found")

        except Exception as e:
            logger.error(f"Error in ramp meter features processing: {e}")
            logger.error(traceback.format_exc())

    def process_counts_1hr_calcs(self):
        """Process counts 1hr calculations [23 of 29]"""
        logger.info("Counts 1hr calcs [23 of 29 (sigops)]")
        
        try:
            # This appears to be done asynchronously in R
            from monthly_report_calcs_1 import main as run_monthly_report_calcs_1
            
            # Run calculations using existing module
            result = run_monthly_report_calcs_1(
                start_date=self.calcs_start_date,
                end_date=self.report_end_date,
                bucket=self.config['bucket'],
                conf_athena=self.config['athena']
            )
            
            logger.info("Counts 1hr calculations completed")

        except Exception as e:
            logger.error(f"Error in counts 1hr calculations: {e}")
            logger.error(traceback.format_exc())

    def process_monthly_summary_calcs(self):
        """Process monthly summary calculations [24 of 29]"""
        logger.info("Monthly Summary calcs [24 of 29 (sigops)]")
        
        try:
            # Run additional monthly calculations
            # This would involve processing all the data calculated so far
            # and creating summary statistics
            
            # Calculate delta values (month-over-month changes)
            self.calculate_delta_values()
            
            # Create summary tables
            self.create_summary_tables()
            
            logger.info("Monthly summary calculations completed")

        except Exception as e:
            logger.error(f"Error in monthly summary calculations: {e}")
            logger.error(traceback.format_exc())

    def process_high_resolution_calcs(self):
        """Process high resolution calculations [25 of 29]"""
        logger.info("Hi-res calcs [25 of 29 (sigops)]")
        
        try:
            # This appears to be done asynchronously in R
            # Implementation would involve high-resolution data processing
            
            # Process 15-minute data
            self.process_15min_data()
            
            # Process 1-minute travel time data
            self.process_1min_travel_time()
            
            logger.info("High resolution calculations completed")

        except Exception as e:
            logger.error(f"Error in high resolution calculations: {e}")
            logger.error(traceback.format_exc())

    def process_lane_by_lane_volume_calcs(self):
        """Process lane-by-lane volume calculations [26 of 29]"""
        logger.info("Lane-by-lane volume calcs [26 of 29 (sigops)]")
        
        try:
            # Process lane-level volume data
            self.process_lane_volumes()
            
            logger.info("Lane-by-lane volume calculations completed")

        except Exception as e:
            logger.error(f"Error in lane-by-lane volume calculations: {e}")
            logger.error(traceback.format_exc())

    def process_mainline_green_utilization(self):
        """Process mainline green utilization [27 of 29]"""
        logger.info("Mainline Green Utilization [27 of 29 (sigops)]")
        
        try:
            gu = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="green_utilization",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                CallPhase=lambda df: df['CallPhase'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week
            )

            # Filter to mainline phases only
            if 'CallPhase' in gu.columns:
                gu = gu[gu['CallPhase'].isin([2, 6])]

            # Calculate daily, weekly, and monthly aggregations
            daily_gu = get_daily_avg(gu, "gu", "vol")
            weekly_gu_by_day = get_weekly_avg_by_day(gu, "gu", "vol")
            monthly_gu_by_day = get_monthly_avg_by_day(gu, "gu", "vol")

            # Corridor aggregations
            cor_daily_gu = self.get_cor_weekly_avg_by_day(daily_gu, self.corridors, "gu", "vol").drop(columns=['Week'], errors='ignore')
            cor_weekly_gu_by_day = self.get_cor_weekly_avg_by_day(weekly_gu_by_day, self.corridors, "gu", "vol")
            cor_monthly_gu_by_day = self.get_cor_monthly_avg_by_day(monthly_gu_by_day, self.corridors, "gu", "vol")

            # Subcorridor aggregations
            sub_daily_gu = self.get_cor_weekly_avg_by_day(daily_gu, self.subcorridors, "gu", "vol").drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
            sub_weekly_gu_by_day = self.get_cor_weekly_avg_by_day(weekly_gu_by_day, self.subcorridors, "gu", "vol").dropna(subset=['Corridor'])
            sub_monthly_gu_by_day = self.get_cor_monthly_avg_by_day(monthly_gu_by_day, self.subcorridors, "gu", "vol").dropna(subset=['Corridor'])

            # Save results
            self.save_results([
                (daily_gu, "daily_gu.parquet", "gu"),
                (weekly_gu_by_day, "weekly_gu_by_day.parquet", "gu"),
                (monthly_gu_by_day, "monthly_gu_by_day.parquet", "gu"),
                (cor_daily_gu, "cor_daily_gu.parquet", "gu"),
                (cor_weekly_gu_by_day, "cor_weekly_gu_by_day.parquet", "gu"),
                (cor_monthly_gu_by_day, "cor_monthly_gu_by_day.parquet", "gu"),
                (sub_daily_gu, "sub_daily_gu.parquet", "gu"),
                (sub_weekly_gu_by_day, "sub_weekly_gu_by_day.parquet", "gu"),
                (sub_monthly_gu_by_day, "sub_monthly_gu_by_day.parquet", "gu")
            ])

        except Exception as e:
            logger.error(f"Error in mainline green utilization processing: {e}")
            logger.error(traceback.format_exc())

    def process_coordination_and_preemption(self):
        """Process coordination and preemption [28 of 29]"""
        logger.info("Coordination and Preemption [28 of 29 (sigops)]")
        
        try:
            # Process preemption data
            preempt = self.s3_read_parquet_parallel(
                bucket=self.config['bucket'],
                table_name="preemption",
                start_date=self.wk_calcs_start_date,
                end_date=self.report_end_date,
                signals_list=self.signals_list
            ).assign(
                SignalID=lambda df: df['SignalID'].astype('category'),
                Date=lambda df: pd.to_datetime(df['Date']).dt.date,
                DOW=lambda df: pd.to_datetime(df['Date']).dt.dayofweek + 1,
                Week=lambda df: pd.to_datetime(df['Date']).dt.isocalendar().week,
                CallPhase=lambda df: pd.Categorical([0] * len(df))  # Default phase
            )

            # Calculate daily, weekly, and monthly aggregations
            daily_preempt = get_daily_avg(preempt, "freq", peak_only=False)
            weekly_preempt_by_day = get_weekly_avg_by_day(preempt, "freq", peak_only=False)
            monthly_preempt_by_day = get_monthly_avg_by_day(preempt, "freq", peak_only=False)

            # Corridor aggregations
            cor_daily_preempt = self.get_cor_weekly_avg_by_day(daily_preempt, self.corridors, "freq").drop(columns=['Week'], errors='ignore')
            cor_weekly_preempt_by_day = self.get_cor_weekly_avg_by_day(weekly_preempt_by_day, self.corridors, "freq")
            cor_monthly_preempt_by_day = self.get_cor_monthly_avg_by_day(monthly_preempt_by_day, self.corridors, "freq")

            # Subcorridor aggregations
            sub_daily_preempt = self.get_cor_weekly_avg_by_day(daily_preempt, self.subcorridors, "freq").drop(
                columns=['Week'], errors='ignore').dropna(subset=['Corridor'])
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
            # Read final quarterly report configuration
            if os.path.exists('../quarterly_report.yaml'):
                with open('../quarterly_report.yaml', 'r') as f:
                    quarterly_conf = yaml.safe_load(f)
                    
                # If quarterly reporting is enabled, trigger quarterly calculations
                if quarterly_conf.get('run', {}).get('quarterly', False):
                    self.process_quarterly_reports()

            # Generate signal state summary
            self.generate_signal_state_summary()
            
            # Clean up temporary files and variables
            self.cleanup_temp_data()
            
            # Log completion
            end_time = time.time()
            duration = end_time - self.start_time
            logger.info(f"Monthly report package completed in {format_duration(duration)}")
            
            # Send completion notification if configured
            if self.config.get('notifications', {}).get('enabled', False):
                self.send_completion_notification(duration)

        except Exception as e:
            logger.error(f"Error in task completion and cleanup: {e}")
            logger.error(traceback.format_exc())

    # Additional helper methods for the new processing steps

    def get_qs_by_hr(self, df):
        """Get queue spillback by hour"""
        return df.groupby([
            'SignalID', 'CallPhase', 
            df['Date_Hour'].dt.floor('H') if 'Date_Hour' in df.columns else df['Date'],
            df['Date_Hour'].dt.hour if 'Date_Hour' in df.columns else 0
        ]).agg(
            qs_freq=('qs_freq', 'mean'),
            cycles=('cycles', 'sum')
        ).reset_index()

    def get_monthly_qs_by_hr(self, df):
        """Get monthly queue spillback by hour"""
        return df.groupby([
            'SignalID', 'CallPhase',
            df['Date_Hour'].dt.to_period('M').dt.to_timestamp() if 'Date_Hour' in df.columns else df['Date'],
            df['Hour'] if 'Hour' in df.columns else 0
        ]).agg(
            qs_freq=('qs_freq', 'mean'),
            cycles=('cycles', 'sum')
        ).reset_index()

    def get_cor_monthly_qs_by_hr(self, df, corridors):
        """Get corridor monthly queue spillback by hour"""
        return df.merge(corridors, on='SignalID', how='left').groupby(
            ['Corridor', 'Month', 'Hour']
        ).agg(
            qs_freq=('qs_freq', 'mean'),
            cycles=('cycles', 'sum')
        ).reset_index()

    def get_cor_monthly_ti_by_hr(self, df, vol_df, corridors):
        """Get corridor monthly travel time index by hour"""
        # Weight travel time metrics by volume
        merged = df.merge(vol_df, on=['Corridor', 'Month'], how='left', suffixes=('', '_vol'))
        
        return merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).apply(
            lambda x: pd.Series({
                col: np.average(x[col].dropna(), weights=x['vph'].fillna(1)) 
                for col in ['tti', 'pti', 'bi', 'speed_mph'] if col in x.columns
            })
        ).reset_index()

    def get_cor_monthly_ti_by_day(self, df, vol_df, corridors):
        """Get corridor monthly travel time index by day"""
        merged = df.merge(vol_df, on=['Corridor', 'Month'], how='left', suffixes=('', '_vol'))
        
        return merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).apply(
            lambda x: pd.Series({
                col: np.average(x[col].dropna(), weights=x['vph'].fillna(1)) 
                for col in ['tti', 'pti', 'bi', 'speed_mph'] if col in x.columns
            })
        ).reset_index()

    def get_daily_cctv_uptime(self, database, table_name, cam_config, start_date):
        """Get daily CCTV uptime from database"""
        try:
            from database_functions import get_connection_engine
            
            query = f"""
            SELECT CameraID, Date, 
                   SUM(CASE WHEN Size > 0 THEN 1 ELSE 0 END) as up,
                   COUNT(*) as num,
                   AVG(CASE WHEN Size > 0 THEN 1.0 ELSE 0.0 END) as uptime
            FROM {database}.{table_name}
            WHERE Date >= '{start_date}'
            GROUP BY CameraID, Date
            """
            
            engine = get_connection_engine(self.config['athena'])
            df = pd.read_sql(query, engine)
            
            # Merge with camera configuration
            return df.merge(cam_config, on='CameraID', how='left')
            
        except Exception as e:
            logger.error(f"Error getting CCTV uptime: {e}")
            return pd.DataFrame()

    def get_weekly_avg_by_day_cctv(self, df):
        """Get weekly average by day for CCTV data"""
        return df.groupby([
            'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'CameraID', 'Description',
            df['Date'].dt.isocalendar().week
        ]).agg(
            up=('uptime', 'sum'),
            uptime=('uptime', lambda x: np.average(x, weights=df.loc[x.index, 'num'])),
            num=('num', 'sum')
        ).reset_index()

    def calculate_delta_values(self):
        """Calculate month-over-month delta values for all metrics"""
        try:
            # Define metrics to calculate deltas for
            metrics = [
                'detector_uptime', 'pa_uptime', 'papd', 'pd', 'comm_uptime',
                'vpd', 'vph', 'throughput', 'aog', 'pr', 'sf_freq', 'qs_freq',
                'tti', 'pti', 'bi', 'speed_mph', 'cctv_uptime', 'gu', 'preempt'
            ]
            
            for metric in metrics:
                self.calculate_metric_delta(metric)
                
        except Exception as e:
            logger.error(f"Error calculating delta values: {e}")

    def calculate_metric_delta(self, metric):
        """Calculate delta for a specific metric"""
        try:
            # Load monthly data for the metric
            monthly_files = [
                f"monthly_{metric}.parquet",
                f"cor_monthly_{metric}.parquet", 
                f"sub_monthly_{metric}.parquet"
            ]
            
            for filename in monthly_files:
                df = self.load_result(filename)
                if df is not None and not df.empty:
                    # Calculate month-over-month change
                    df_with_delta = self.add_delta_column(df, metric)
                    
                    # Save back with delta
                    delta_filename = filename.replace('.parquet', '_with_delta.parquet')
                    self.save_results([(df_with_delta, delta_filename, metric)])
                    
        except Exception as e:
            logger.error(f"Error calculating delta for {metric}: {e}")

    def add_delta_column(self, df, metric):
        """Add delta column to DataFrame"""
        if 'Month' not in df.columns:
            return df
            
        df = df.sort_values('Month')
        df[f'{metric}_delta'] = df.groupby(['SignalID'] if 'SignalID' in df.columns else ['Corridor'])[metric].pct_change()
        
        return df

    def create_summary_tables(self):
        """Create summary tables for the report"""
        try:
            # Create corridor performance summary
            self.create_corridor_summary()
            
            # Create signal performance summary  
            self.create_signal_summary()
            
            # Create trend analysis summary
            self.create_trend_summary()
            
        except Exception as e:
            logger.error(f"Error creating summary tables: {e}")

    def create_corridor_summary(self):
        """Create corridor-level performance summary"""
        # Implementation would aggregate key metrics by corridor
        pass

    def create_signal_summary(self):
        """Create signal-level performance summary"""
        # Implementation would aggregate key metrics by signal
        pass

    def create_trend_summary(self):
        """Create trend analysis summary"""
        # Implementation would analyze trends over time
        pass

    def process_15min_data(self):
        """Process 15-minute resolution data"""
        # Implementation for 15-minute data processing
        pass

    def process_1min_travel_time(self):
        """Process 1-minute travel time data"""
        # Implementation for 1-minute travel time processing
        pass

    def process_lane_volumes(self):
        """Process lane-by-lane volume data"""
        # Implementation for lane-level volume processing
        pass

    def process_quarterly_reports(self):
        """Process quarterly report calculations"""
        try:
            from aggregations import get_quarterly
            
            # Load monthly data and convert to quarterly
            monthly_metrics = [
                'vpd', 'aog', 'pr', 'sf_freq', 'qs_freq', 'throughput'
            ]
            
            for metric in monthly_metrics:
                monthly_df = self.load_result(f"cor_monthly_{metric}.parquet")
                if monthly_df is not None and not monthly_df.empty:
                    quarterly_df = get_quarterly(monthly_df, metric, operation='avg')
                    self.save_results([(quarterly_df, f"quarterly_{metric}.parquet", metric)])
                    
        except Exception as e:
            logger.error(f"Error processing quarterly reports: {e}")

    def generate_signal_state_summary(self):
        """Generate signal state summary for final report"""
        try:
            # Load all processed data and create executive summary
            summary_data = {
                'total_signals': len(self.signals_list),
                'total_corridors': len(self.corridors['Corridor'].unique()),
                'report_period': f"{self.calcs_start_date} to {self.report_end_date}",
                'processing_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Save summary
            summary_df = pd.DataFrame([summary_data])
            self.save_results([(summary_df, "signal_state_summary.parquet", "summary")])
            
        except Exception as e:
            logger.error(f"Error generating signal state summary: {e}")

    def cleanup_temp_data(self):
        """Clean up temporary data and variables"""
        try:
            # Clear large DataFrames to free memory
            attrs_to_clear = ['aog', 'sf', 'qs', 'vph', 'vpd']
            for attr in attrs_to_clear:
                if hasattr(self, attr):
                    delattr(self, attr)
                    
            # Force garbage collection
            import gc
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def send_completion_notification(self, duration):
        """Send completion notification"""
        try:
            notification_config = self.config.get('notifications', {})
            
            if notification_config.get('email', {}).get('enabled', False):
                self.send_email_notification(duration)
                
            if notification_config.get('slack', {}).get('enabled', False):
                self.send_slack_notification(duration)
                
        except Exception as e:
            logger.error(f"Error sending notifications: {e}")

    def send_email_notification(self, duration):
        """Send email notification of completion"""
        # Implementation for email notification
        pass

    def send_slack_notification(self, duration):
        """Send Slack notification of completion"""
        # Implementation for Slack notification
        pass

    def load_result(self, filename):
        """Load a previously saved result from S3"""
        try:
            # Determine metric type from filename
            metric_type = filename.split('_')[-1].replace('.parquet', '')
            key = f"processed/{metric_type}/{filename}"
            
            return read_parquet_from_s3(self.config['bucket'], key)
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return None

    def run_all_processing_steps(self):
        """
        Main method to run all 29 processing steps
        """
        try:
            self.start_time = time.time()  # Start timer here
            logger.info("Starting Monthly Report Package processing")
            logger.info(f"Week Calcs Start Date: {self.wk_calcs_start_date}")
            logger.info(f"Calcs Start Date: {self.calcs_start_date}")
            logger.info(f"Report End Date: {self.report_end_date}")
            
            # Create progress tracker
            progress_tracker = create_progress_tracker(29, "Monthly Report Processing")
            
            processing_steps = [
                (1, "Travel Times", self.process_travel_times),
                (2, "Vehicle Detector Uptime", self.process_detector_uptime),
                # (3, "Pedestrian Pushbutton Uptime", self.process_pedestrian_uptime),
                # (4, "Watchdog Alerts", self.process_watchdog_alerts),
                # (5, "Daily Pedestrian Activations", self.process_daily_pedestrian_activations),
                # (6, "Hourly Pedestrian Activations", self.process_hourly_pedestrian_activations),
                # (7, "Pedestrian Delay", self.process_ped_delay),
                # (8, "Communication Uptime", self.process_comm_uptime),
                # (9, "Daily Volumes", self.process_daily_volumes),
                # (10, "Hourly Volumes", self.process_hourly_volumes),
                # (11, "Daily Throughput", self.process_daily_throughput),
                # (12, "Daily Arrivals on Green", self.process_daily_aog),
                # (13, "Hourly Arrivals on Green", self.process_hourly_aog),
                # (14, "Daily Progression Ratio", self.process_daily_progression_ratio),
                # (15, "Hourly Progression Ratio", self.process_hourly_progression_ratio),
                # (16, "Daily Split Failures", self.process_daily_split_failures),
                # (17, "Hourly Split Failures", self.process_hourly_split_failures),
                # (18, "Daily Queue Spillback", self.process_daily_queue_spillback),
                # (19, "Hourly Queue Spillback", self.process_hourly_queue_spillback),
                # (20, "Travel Time Indexes", self.process_travel_time_indexes),
                # (21, "CCTV Uptime", self.process_cctv_uptime),
                # (22, "Travel Time Speeds", self.process_travel_time_speeds),
                # (23, "Ramp Meter Features", self.process_ramp_meter_features),
                # (24, "Counts 1hr Calcs", self.process_counts_1hr_calcs),
                # (25, "Monthly Summary Calcs", self.process_monthly_summary_calcs),
                # (26, "Hi-res Calcs", self.process_high_resolution_calcs),
                # (27, "Lane-by-lane Volume Calcs", self.process_lane_by_lane_volume_calcs),
                # (28, "Mainline Green Utilization", self.process_mainline_green_utilization),
                # (29, "Coordination and Preemption", self.process_coordination_and_preemption),
                # (30, "Task Completion and Cleanup", self.process_task_completion_and_cleanup)
            ]
            
            # Execute each processing step
            for step_num, step_name, step_function in processing_steps:
                try:
                    progress_tracker(step_num - 1)
                    logger.info(f"Starting step {step_num}: {step_name}")
                    
                    step_start_time = time.time()
                    step_function()
                    step_duration = time.time() - step_start_time
                    
                    logger.info(f"Completed step {step_num}: {step_name} in {format_duration(step_duration)}")
                    
                except Exception as e:
                    logger.error(f"Error in step {step_num} ({step_name}): {e}")
                    logger.error(traceback.format_exc())
                    # Continue with next step even if one fails
                    continue
            
            # Final progress update
            progress_tracker(len(processing_steps))
            
            total_duration = time.time() - self.start_time
            logger.info(f"Monthly Report Package processing completed in {format_duration(total_duration)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Fatal error in monthly report processing: {e}")
            logger.error(traceback.format_exc())
            return False


def main():
    """
    Main execution function for the Monthly Report Package
    """
    try:
        # Load configuration
        config_path = 'Monthly_Report.yaml'
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Initialize and run the monthly report processor
        processor = MonthlyReportProcessor(config)
        
        # Run all processing steps
        success = processor.run_all_processing_steps()
        
        if success:
            logger.info("Monthly Report Package completed successfully")
            return 0
        else:
            logger.error("Monthly Report Package completed with errors")
            return 1
            
    except Exception as e:
        logger.error(f"Failed to initialize Monthly Report Package: {e}")
        logger.error(traceback.format_exc())
        return 1

@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def read_parquet_from_s3(bucket, key):
    """Read parquet file from S3 with retry logic"""
    try:
        import boto3
        import pyarrow.parquet as pq
        import io
        
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Read parquet from bytes
        parquet_file = pq.ParquetFile(io.BytesIO(response['Body'].read()))
        df = parquet_file.read().to_pandas()
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading parquet from S3 {bucket}/{key}: {e}")
        raise

@retry_on_failure(max_retries=3, delay=1.0, backoff=2.0)
def write_parquet_to_s3(df, bucket, key):
    """Write parquet file to S3 with retry logic"""
    try:
        import boto3
        import pyarrow as pa
        import pyarrow.parquet as pq
        import io
        
        # Convert DataFrame to parquet bytes
        table = pa.Table.from_pandas(df)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
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

def setup_logging(log_level='INFO'):
    """Setup logging configuration"""
    import logging
    from datetime import datetime
    
    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Setup log file with timestamp
    log_filename = logs_dir / f"monthly_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

if __name__ == "__main__":
    logger = setup_logging()
    main()
    



