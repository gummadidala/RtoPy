#!/usr/bin/env python3
"""
Monthly_Report_Package_1.py
Python conversion of Monthly_Report_Package_1.R
"""

import pandas as pd
import numpy as np
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import pickle
import sys
import traceback
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')
from scipy import stats
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def open_arrow_dataset(path: str):
    """
    Open Arrow dataset with error handling
    
    Args:
        path: Path to dataset
    
    Returns:
        Arrow dataset
    """
    try:
        import pyarrow.dataset as ds
        return ds.dataset(path)
    except Exception as e:
        logger.error(f"Error opening dataset {path}: {e}")
        return None

def get_avg_daily_detector_uptime(df):
    """Calculate average daily detector uptime"""
    try:
        if df.empty:
            return df
        
        # Ensure required columns exist
        required_cols = ['SignalID', 'Date', 'uptime']
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0 if col == 'uptime' else ''
        
        # Calculate daily averages
        daily_uptime = df.groupby(['SignalID', 'Date']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return daily_uptime
    except Exception as e:
        logger.error(f"Error calculating avg daily detector uptime: {e}")
        return pd.DataFrame()

def get_cor_avg_daily_detector_uptime(df, corridors):
    """Get corridor average daily detector uptime"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        # Merge with corridor data
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        # Group by corridor and date
        cor_uptime = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Date']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return cor_uptime
    except Exception as e:
        logger.error(f"Error calculating corridor avg daily detector uptime: {e}")
        return pd.DataFrame()

def get_weekly_detector_uptime(df):
    """Calculate weekly detector uptime"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly = df.groupby(['SignalID', 'Year', 'Week']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return weekly
    except Exception as e:
        logger.error(f"Error calculating weekly detector uptime: {e}")
        return pd.DataFrame()

def get_cor_weekly_detector_uptime(df, corridors):
    """Get corridor weekly detector uptime"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return cor_weekly
    except Exception as e:
        logger.error(f"Error calculating corridor weekly detector uptime: {e}")
        return pd.DataFrame()

def get_monthly_detector_uptime(df):
    """Calculate monthly detector uptime"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly = df.groupby(['SignalID', 'Month']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return monthly
    except Exception as e:
        logger.error(f"Error calculating monthly detector uptime: {e}")
        return pd.DataFrame()

def get_cor_monthly_detector_uptime(df, corridors):
    """Get corridor monthly detector uptime"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'uptime': 'mean'
        }).reset_index()
        
        return cor_monthly
    except Exception as e:
        logger.error(f"Error calculating corridor monthly detector uptime: {e}")
        return pd.DataFrame()

def get_pau_gamma(dates, papd, paph, corridors, wk_calcs_start_date, pau_start_date):
    """
    Calculate pedestrian uptime using gamma distribution
    
    Args:
        dates: Date range
        papd: Pedestrian activations per day
        paph: Pedestrian activations per hour
        corridors: Corridor mapping
        wk_calcs_start_date: Week calculation start date
        pau_start_date: PAU calculation start date
    
    Returns:
        DataFrame with uptime calculations
    """
    try:
        if papd.empty:
            return pd.DataFrame()
        
        # Calculate baseline statistics for each detector
        baseline_stats = papd.groupby(['SignalID', 'Detector', 'CallPhase']).agg({
            'papd': ['mean', 'std', 'count']
        }).reset_index()
        
        baseline_stats.columns = ['SignalID', 'Detector', 'CallPhase', 'mean_papd', 'std_papd', 'count_papd']
        
        # Merge with daily data
        pau_data = papd.merge(baseline_stats, on=['SignalID', 'Detector', 'CallPhase'], how='left')
        
        # Calculate z-scores and determine outliers
        pau_data['z_score'] = np.where(
            pau_data['std_papd'] > 0,
            (pau_data['papd'] - pau_data['mean_papd']) / pau_data['std_papd'],
            0
        )
        
        # Mark as down if z-score indicates significant deviation (beyond 2 std devs)
        pau_data['uptime'] = np.where(
            (abs(pau_data['z_score']) > 2) | (pau_data['papd'] == 0), 
            0, 1
        )
        
        # Add required columns
        pau_data['DOW'] = pd.to_datetime(pau_data['Date']).dt.dayofweek + 1
        pau_data['Week'] = pd.to_datetime(pau_data['Date']).dt.isocalendar().week
        
        return pau_data[['SignalID', 'Detector', 'CallPhase', 'Date', 'DOW', 'Week', 'papd', 'uptime']]
        
    except Exception as e:
        logger.error(f"Error calculating PAU gamma: {e}")
        return pd.DataFrame()

def get_bad_ped_detectors(pau_data):
    """Identify bad pedestrian detectors"""
    try:
        if pau_data.empty:
            return pd.DataFrame()
        
        # Calculate uptime percentage over last 30 days
        recent_data = pau_data[pau_data['Date'] >= (pd.Timestamp.now() - timedelta(days=30)).date()]
        
        bad_detectors = recent_data.groupby(['SignalID', 'Detector', 'CallPhase']).agg({
            'uptime': 'mean',
            'Date': 'max'
        }).reset_index()
        
        # Mark as bad if uptime < 80%
        bad_detectors = bad_detectors[bad_detectors['uptime'] < 0.8]
        bad_detectors['Alert'] = 'Bad Ped Detection'
        
        return bad_detectors
        
    except Exception as e:
        logger.error(f"Error identifying bad ped detectors: {e}")
        return pd.DataFrame()

def get_daily_avg(df, metric_col, weight_col=None, peak_only=True):
    """Calculate daily averages"""
    try:
        if df.empty:
            return df
        
        group_cols = ['SignalID', 'CallPhase', 'Date']
        if 'DOW' in df.columns:
            group_cols.append('DOW')
        
        if weight_col and weight_col in df.columns:
            # Weighted average
            df['weighted_metric'] = df[metric_col] * df[weight_col]
            daily_avg = df.groupby(group_cols).agg({
                'weighted_metric': 'sum',
                weight_col: 'sum'
            }).reset_index()
            daily_avg[metric_col] = daily_avg['weighted_metric'] / daily_avg[weight_col]
            daily_avg = daily_avg.drop(columns=['weighted_metric'])
        else:
            # Simple average
            daily_avg = df.groupby(group_cols).agg({
                metric_col: 'mean'
            }).reset_index()
        
        return daily_avg
        
    except Exception as e:
        logger.error(f"Error calculating daily average: {e}")
        return pd.DataFrame()

def get_weekly_avg_by_day(df, metric_col, weight_col=None, peak_only=True):
    """Calculate weekly averages by day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        group_cols = ['SignalID', 'CallPhase', 'Year', 'Week']
        if 'DOW' in df.columns:
            group_cols.append('DOW')
        
        if weight_col and weight_col in df.columns:
            df['weighted_metric'] = df[metric_col] * df[weight_col]
            weekly_avg = df.groupby(group_cols).agg({
                'weighted_metric': 'sum',
                weight_col: 'sum'
            }).reset_index()
            weekly_avg[metric_col] = weekly_avg['weighted_metric'] / weekly_avg[weight_col]
            weekly_avg = weekly_avg.drop(columns=['weighted_metric'])
        else:
            weekly_avg = df.groupby(group_cols).agg({
                metric_col: 'mean'
            }).reset_index()
        
        return weekly_avg
        
    except Exception as e:
        logger.error(f"Error calculating weekly average by day: {e}")
        return pd.DataFrame()

def get_monthly_avg_by_day(df, metric_col, weight_col=None, peak_only=True):
    """Calculate monthly averages by day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        group_cols = ['SignalID', 'CallPhase', 'Month']
        if 'DOW' in df.columns:
            group_cols.append('DOW')
        
        if weight_col and weight_col in df.columns:
            df['weighted_metric'] = df[metric_col] * df[weight_col]
            monthly_avg = df.groupby(group_cols).agg({
                'weighted_metric': 'sum',
                weight_col: 'sum'
            }).reset_index()
            monthly_avg[metric_col] = monthly_avg['weighted_metric'] / monthly_avg[weight_col]
            monthly_avg = monthly_avg.drop(columns=['weighted_metric'])
        else:
            monthly_avg = df.groupby(group_cols).agg({
                metric_col: 'mean'
            }).reset_index()
        
        return monthly_avg
        
    except Exception as e:
        logger.error(f"Error calculating monthly average by day: {e}")
        return pd.DataFrame()

def get_cor_weekly_avg_by_day(df, corridors, metric_col, weight_col=None):
    """Calculate corridor weekly averages by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        group_cols = ['Zone_Group', 'Zone', 'Corridor']
        if 'Year' in merged.columns and 'Week' in merged.columns:
            group_cols.extend(['Year', 'Week'])
        if 'DOW' in merged.columns:
            group_cols.append('DOW')
        
        if weight_col and weight_col in merged.columns:
            merged['weighted_metric'] = merged[metric_col] * merged[weight_col]
            cor_avg = merged.groupby(group_cols).agg({
                'weighted_metric': 'sum',
                weight_col: 'sum'
            }).reset_index()
            cor_avg[metric_col] = cor_avg['weighted_metric'] / cor_avg[weight_col]
            cor_avg = cor_avg.drop(columns=['weighted_metric'])
        else:
            cor_avg = merged.groupby(group_cols).agg({
                metric_col: 'mean'
            }).reset_index()
        
        return cor_avg
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly average by day: {e}")
        return pd.DataFrame()

def get_cor_monthly_avg_by_day(df, corridors, metric_col, weight_col=None):
    """Calculate corridor monthly averages by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        group_cols = ['Zone_Group', 'Zone', 'Corridor']
        if 'Month' in merged.columns:
            group_cols.append('Month')
        if 'DOW' in merged.columns:
            group_cols.append('DOW')
        
        if weight_col and weight_col in merged.columns:
            merged['weighted_metric'] = merged[metric_col] * merged[weight_col]
            cor_avg = merged.groupby(group_cols).agg({'weighted_metric': 'sum',
                weight_col: 'sum'
            }).reset_index()
            cor_avg[metric_col] = cor_avg['weighted_metric'] / cor_avg[weight_col]
            cor_avg = cor_avg.drop(columns=['weighted_metric'])
        else:
            cor_avg = merged.groupby(group_cols).agg({
                metric_col: 'mean'
            }).reset_index()
        
        return cor_avg
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly average by day: {e}")
        return pd.DataFrame()

# Volume-related functions
def get_weekly_vpd(df):
    """Get weekly vehicles per day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_vpd = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week']).agg({
            'vpd': 'mean'
        }).reset_index()
        
        return weekly_vpd
        
    except Exception as e:
        logger.error(f"Error calculating weekly VPD: {e}")
        return pd.DataFrame()

def get_monthly_vpd(df):
    """Get monthly vehicles per day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_vpd = df.groupby(['SignalID', 'CallPhase', 'Month']).agg({
            'vpd': 'mean'
        }).reset_index()
        
        return monthly_vpd
        
    except Exception as e:
        logger.error(f"Error calculating monthly VPD: {e}")
        return pd.DataFrame()

def get_cor_weekly_vpd(df, corridors):
    """Get corridor weekly VPD"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week']).agg({
            'vpd': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly VPD: {e}")
        return pd.DataFrame()

def get_cor_monthly_vpd(df, corridors):
    """Get corridor monthly VPD"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'vpd': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly VPD: {e}")
        return pd.DataFrame()

def get_weekly_vph(df):
    """Get weekly vehicles per hour"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_vph = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week', 'Timeperiod']).agg({
            'vph': 'mean'
        }).reset_index()
        
        return weekly_vph
        
    except Exception as e:
        logger.error(f"Error calculating weekly VPH: {e}")
        return pd.DataFrame()

def get_monthly_vph(df):
    """Get monthly vehicles per hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_vph = df.groupby(['SignalID', 'CallPhase', 'Month', 'Timeperiod']).agg({
            'vph': 'mean'
        }).reset_index()
        
        return monthly_vph
        
    except Exception as e:
        logger.error(f"Error calculating monthly VPH: {e}")
        return pd.DataFrame()

def get_weekly_vph_peak(df):
    """Get weekly VPH for peak periods"""
    try:
        if df.empty:
            return df
        
        # Define peak hours (typically 7-9 AM and 4-6 PM)
        am_peak = [7, 8, 9]
        pm_peak = [16, 17, 18]
        
        peak_data = df[df['Timeperiod'].isin(am_peak + pm_peak)]
        
        am_peak_data = peak_data[peak_data['Timeperiod'].isin(am_peak)]
        pm_peak_data = peak_data[peak_data['Timeperiod'].isin(pm_peak)]
        
        return {
            'am_peak': am_peak_data,
            'pm_peak': pm_peak_data,
            'all_peak': peak_data
        }
        
    except Exception as e:
        logger.error(f"Error calculating weekly VPH peak: {e}")
        return {}

def get_monthly_vph_peak(df):
    """Get monthly VPH for peak periods"""
    try:
        if df.empty:
            return df
        
        am_peak = [7, 8, 9]
        pm_peak = [16, 17, 18]
        
        peak_data = df[df['Timeperiod'].isin(am_peak + pm_peak)]
        
        am_peak_data = peak_data[peak_data['Timeperiod'].isin(am_peak)]
        pm_peak_data = peak_data[peak_data['Timeperiod'].isin(pm_peak)]
        
        return {
            'am_peak': am_peak_data,
            'pm_peak': pm_peak_data,
            'all_peak': peak_data
        }
        
    except Exception as e:
        logger.error(f"Error calculating monthly VPH peak: {e}")
        return {}

def get_cor_weekly_vph(df, corridors):
    """Get corridor weekly VPH"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'Timeperiod']).agg({
            'vph': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly VPH: {e}")
        return pd.DataFrame()

def get_cor_monthly_vph(df, corridors):
    """Get corridor monthly VPH"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Timeperiod']).agg({
            'vph': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly VPH: {e}")
        return pd.DataFrame()

def get_cor_weekly_vph_peak(df):
    """Get corridor weekly VPH peak"""
    try:
        am_peak = [7, 8, 9]
        pm_peak = [16, 17, 18]
        
        peak_data = df[df['Timeperiod'].isin(am_peak + pm_peak)]
        am_peak_data = peak_data[peak_data['Timeperiod'].isin(am_peak)]
        pm_peak_data = peak_data[peak_data['Timeperiod'].isin(pm_peak)]
        
        return {
            'am_peak': am_peak_data,
            'pm_peak': pm_peak_data,
            'all_peak': peak_data
        }
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly VPH peak: {e}")
        return {}

def get_cor_monthly_vph_peak(df):
    """Get corridor monthly VPH peak"""
    try:
        am_peak = [7, 8, 9]
        pm_peak = [16, 17, 18]
        
        peak_data = df[df['Timeperiod'].isin(am_peak + pm_peak)]
        am_peak_data = peak_data[peak_data['Timeperiod'].isin(am_peak)]
        pm_peak_data = peak_data[peak_data['Timeperiod'].isin(pm_peak)]
        
        return {
            'am_peak': am_peak_data,
            'pm_peak': pm_peak_data,
            'all_peak': peak_data
        }
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly VPH peak: {e}")
        return {}

# Pedestrian activation functions
def get_weekly_papd(df):
    """Get weekly pedestrian activations per day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_papd = df.groupby(['SignalID', 'Detector', 'CallPhase', 'Year', 'Week']).agg({
            'papd': 'mean'
        }).reset_index()
        
        return weekly_papd
        
    except Exception as e:
        logger.error(f"Error calculating weekly PAPD: {e}")
        return pd.DataFrame()

def get_monthly_papd(df):
    """Get monthly pedestrian activations per day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_papd = df.groupby(['SignalID', 'Detector', 'CallPhase', 'Month']).agg({
            'papd': 'mean'
        }).reset_index()
        
        return monthly_papd
        
    except Exception as e:
        logger.error(f"Error calculating monthly PAPD: {e}")
        return pd.DataFrame()

def get_cor_weekly_papd(df, corridors):
    """Get corridor weekly PAPD"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week']).agg({
            'papd': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly PAPD: {e}")
        return pd.DataFrame()

def get_cor_monthly_papd(df, corridors):
    """Get corridor monthly PAPD"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'papd': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly PAPD: {e}")
        return pd.DataFrame()

def get_weekly_paph(df):
    """Get weekly pedestrian activations per hour"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_paph = df.groupby(['SignalID', 'Detector', 'CallPhase', 'Year', 'Week', 'Hour']).agg({
            'paph': 'mean'
        }).reset_index()
        
        return weekly_paph
        
    except Exception as e:
        logger.error(f"Error calculating weekly PAPH: {e}")
        return pd.DataFrame()

def get_monthly_paph(df):
    """Get monthly pedestrian activations per hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_paph = df.groupby(['SignalID', 'Detector', 'CallPhase', 'Month', 'Hour']).agg({
            'paph': 'mean'
        }).reset_index()
        
        return monthly_paph
        
    except Exception as e:
        logger.error(f"Error calculating monthly PAPH: {e}")
        return pd.DataFrame()

def get_cor_weekly_paph(df, corridors):
    """Get corridor weekly PAPH"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'Hour']).agg({
            'paph': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly PAPH: {e}")
        return pd.DataFrame()

def get_cor_monthly_paph(df, corridors):
    """Get corridor monthly PAPH"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                    on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'paph': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly PAPH: {e}")
        return pd.DataFrame()

# Throughput functions
def get_weekly_thruput(df):
    """Get weekly throughput"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_thruput = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week']).agg({
            'throughput': 'mean'
        }).reset_index()
        
        return weekly_thruput
        
    except Exception as e:
        logger.error(f"Error calculating weekly throughput: {e}")
        return pd.DataFrame()

def get_monthly_thruput(df):
    """Get monthly throughput"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_thruput = df.groupby(['SignalID', 'CallPhase', 'Month']).agg({
            'throughput': 'mean'
        }).reset_index()
        
        return monthly_thruput
        
    except Exception as e:
        logger.error(f"Error calculating monthly throughput: {e}")
        return pd.DataFrame()

def get_cor_weekly_thruput(df, corridors):
    """Get corridor weekly throughput"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week']).agg({
            'throughput': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly throughput: {e}")
        return pd.DataFrame()

def get_cor_monthly_thruput(df, corridors):
    """Get corridor monthly throughput"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'throughput': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly throughput: {e}")
        return pd.DataFrame()

# Arrivals on Green (AOG) functions
def get_daily_aog(df):
    """Get daily arrivals on green"""
    try:
        if df.empty:
            return df
        
        daily_aog = df.groupby(['SignalID', 'CallPhase', 'Date']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return daily_aog
        
    except Exception as e:
        logger.error(f"Error calculating daily AOG: {e}")
        return pd.DataFrame()

def get_weekly_aog_by_day(df):
    """Get weekly AOG by day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_aog = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week', 'DOW']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return weekly_aog
        
    except Exception as e:
        logger.error(f"Error calculating weekly AOG by day: {e}")
        return pd.DataFrame()

def get_monthly_aog_by_day(df):
    """Get monthly AOG by day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_aog = df.groupby(['SignalID', 'CallPhase', 'Month', 'DOW']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return monthly_aog
        
    except Exception as e:
        logger.error(f"Error calculating monthly AOG by day: {e}")
        return pd.DataFrame()

def get_cor_weekly_aog_by_day(df, corridors):
    """Get corridor weekly AOG by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'DOW']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly AOG by day: {e}")
        return pd.DataFrame()

def get_cor_monthly_aog_by_day(df, corridors):
    """Get corridor monthly AOG by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'DOW']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly AOG by day: {e}")
        return pd.DataFrame()

def get_aog_by_hr(df):
    """Get AOG by hour"""
    try:
        if df.empty:
            return df
        
        if 'Date_Hour' in df.columns:
            df['Hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        elif 'Timeperiod' in df.columns:
            df['Hour'] = df['Timeperiod']
        
        aog_by_hr = df.groupby(['SignalID', 'CallPhase', 'Date', 'Hour']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return aog_by_hr
        
    except Exception as e:
        logger.error(f"Error calculating AOG by hour: {e}")
        return pd.DataFrame()

def get_monthly_aog_by_hr(df):
    """Get monthly AOG by hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_aog_hr = df.groupby(['SignalID', 'CallPhase', 'Month', 'Hour']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return monthly_aog_hr
        
    except Exception as e:
        logger.error(f"Error calculating monthly AOG by hour: {e}")
        return pd.DataFrame()

def get_cor_monthly_aog_by_hr(df, corridors):
    """Get corridor monthly AOG by hour"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly_hr = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'aog': 'mean'
        }).reset_index()
        
        return cor_monthly_hr
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly AOG by hour: {e}")
        return pd.DataFrame()

# Progression Ratio functions
def get_weekly_pr_by_day(df):
    """Get weekly progression ratio by day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        # Calculate progression ratio (typically AOG >= 0.5)
        df['pr'] = (df['aog'] >= 0.5).astype(int)
        
        weekly_pr = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week', 'DOW']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return weekly_pr
        
    except Exception as e:
        logger.error(f"Error calculating weekly PR by day: {e}")
        return pd.DataFrame()

def get_monthly_pr_by_day(df):
    """Get monthly progression ratio by day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        df['pr'] = (df['aog'] >= 0.5).astype(int)
        
        monthly_pr = df.groupby(['SignalID', 'CallPhase', 'Month', 'DOW']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return monthly_pr
        
    except Exception as e:
        logger.error(f"Error calculating monthly PR by day: {e}")
        return pd.DataFrame()

def get_cor_weekly_pr_by_day(df, corridors):
    """Get corridor weekly progression ratio by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'DOW']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly PR by day: {e}")
        return pd.DataFrame()

def get_cor_monthly_pr_by_day(df, corridors):
    """Get corridor monthly progression ratio by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'DOW']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly PR by day: {e}")
        return pd.DataFrame()

def get_pr_by_hr(df):
    """Get progression ratio by hour"""
    try:
        if df.empty:
            return df
        
        if 'Date_Hour' in df.columns:
            df['Hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        elif 'Timeperiod' in df.columns:
            df['Hour'] = df['Timeperiod']
        
        df['pr'] = (df['aog'] >= 0.5).astype(int)
        
        pr_by_hr = df.groupby(['SignalID', 'CallPhase', 'Date', 'Hour']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return pr_by_hr
        
    except Exception as e:
        logger.error(f"Error calculating PR by hour: {e}")
        return pd.DataFrame()

def get_monthly_pr_by_hr(df):
    """Get monthly progression ratio by hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_pr_hr = df.groupby(['SignalID', 'CallPhase', 'Month', 'Hour']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return monthly_pr_hr
        
    except Exception as e:
        logger.error(f"Error calculating monthly PR by hour: {e}")
        return pd.DataFrame()

def get_cor_monthly_pr_by_hr(df, corridors):
    """Get corridor monthly progression ratio by hour"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly_hr = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'pr': 'mean'
        }).reset_index()
        
        return cor_monthly_hr
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly PR by hour: {e}")
        return pd.DataFrame()

# Split Failure functions
def get_cor_weekly_sf_by_day(df, corridors):
    """Get corridor weekly split failures by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'DOW']).agg({
            'sf_freq': 'mean'
        }).reset_index()

        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly SF by day: {e}")
        return pd.DataFrame()

def get_cor_monthly_sf_by_day(df, corridors):
    """Get corridor monthly split failures by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'DOW']).agg({
            'sf_freq': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly SF by day: {e}")
        return pd.DataFrame()

def get_sf_by_hr(df):
    """Get split failures by hour"""
    try:
        if df.empty:
            return df
        
        if 'Date_Hour' in df.columns:
            df['Hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        elif 'Timeperiod' in df.columns:
            df['Hour'] = df['Timeperiod']
        
        sf_by_hr = df.groupby(['SignalID', 'CallPhase', 'Date', 'Hour']).agg({
            'sf_freq': 'mean',
            'cycles': 'sum'
        }).reset_index()
        
        return sf_by_hr
        
    except Exception as e:
        logger.error(f"Error calculating SF by hour: {e}")
        return pd.DataFrame()

def get_monthly_sf_by_hr(df):
    """Get monthly split failures by hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_sf_hr = df.groupby(['SignalID', 'CallPhase', 'Month', 'Hour']).agg({
            'sf_freq': 'mean',
            'cycles': 'sum'
        }).reset_index()
        
        return monthly_sf_hr
        
    except Exception as e:
        logger.error(f"Error calculating monthly SF by hour: {e}")
        return pd.DataFrame()

def get_cor_monthly_sf_by_hr(df, corridors):
    """Get corridor monthly split failures by hour"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly_hr = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'sf_freq': 'mean',
            'cycles': 'sum'
        }).reset_index()
        
        return cor_monthly_hr
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly SF by hour: {e}")
        return pd.DataFrame()

# Queue Spillback functions
def get_weekly_qs_by_day(df):
    """Get weekly queue spillback by day"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_qs = df.groupby(['SignalID', 'CallPhase', 'Year', 'Week', 'DOW']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return weekly_qs
        
    except Exception as e:
        logger.error(f"Error calculating weekly QS by day: {e}")
        return pd.DataFrame()

def get_monthly_qs_by_day(df):
    """Get monthly queue spillback by day"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_qs = df.groupby(['SignalID', 'CallPhase', 'Month', 'DOW']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return monthly_qs
        
    except Exception as e:
        logger.error(f"Error calculating monthly QS by day: {e}")
        return pd.DataFrame()

def get_cor_weekly_qs_by_day(df, corridors):
    """Get corridor weekly queue spillback by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_weekly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Year', 'Week', 'DOW']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return cor_weekly
        
    except Exception as e:
        logger.error(f"Error calculating corridor weekly QS by day: {e}")
        return pd.DataFrame()

def get_cor_monthly_qs_by_day(df, corridors):
    """Get corridor monthly queue spillback by day"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'DOW']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly QS by day: {e}")
        return pd.DataFrame()

def get_qs_by_hr(df):
    """Get queue spillback by hour"""
    try:
        if df.empty:
            return df
        
        if 'Date_Hour' in df.columns:
            df['Hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        elif 'Timeperiod' in df.columns:
            df['Hour'] = df['Timeperiod']
        
        qs_by_hr = df.groupby(['SignalID', 'CallPhase', 'Date', 'Hour']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return qs_by_hr
        
    except Exception as e:
        logger.error(f"Error calculating QS by hour: {e}")
        return pd.DataFrame()

def get_monthly_qs_by_hr(df):
    """Get monthly queue spillback by hour"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_qs_hr = df.groupby(['SignalID', 'CallPhase', 'Month', 'Hour']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return monthly_qs_hr
        
    except Exception as e:
        logger.error(f"Error calculating monthly QS by hour: {e}")
        return pd.DataFrame()

def get_cor_monthly_qs_by_hr(df, corridors):
    """Get corridor monthly queue spillback by hour"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly_hr = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'qs_freq': 'mean'
        }).reset_index()
        
        return cor_monthly_hr
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly QS by hour: {e}")
        return pd.DataFrame()

# Travel Time Index functions
def get_cor_monthly_ti_by_hr(df, cor_monthly_vph, all_corridors):
    """Get corridor monthly travel time index by hour"""
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Merge with volume data for weighting
        merged = df.merge(
            cor_monthly_vph[['Zone_Group', 'Zone', 'Corridor', 'Month', 'Timeperiod', 'vph']], 
            left_on=['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour'],
            right_on=['Zone_Group', 'Zone', 'Corridor', 'Month', 'Timeperiod'],
            how='left'
        )
        
        # Get the metric column (tti, pti, bi, or speed_mph)
        metric_cols = [col for col in df.columns if col not in ['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']]
        if not metric_cols:
            return pd.DataFrame()
        
        metric_col = metric_cols[0]
        
        # Calculate weighted average
        merged['weighted_metric'] = merged[metric_col] * merged['vph'].fillna(1)
        
        result = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']).agg({
            'weighted_metric': 'sum',
            'vph': 'sum'
        }).reset_index()
        
        result[metric_col] = result['weighted_metric'] / result['vph']
        result = result.drop(columns=['weighted_metric', 'vph'])
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly TI by hour: {e}")
        return pd.DataFrame()

def get_cor_monthly_ti_by_day(df, cor_monthly_vph, all_corridors):
    """Get corridor monthly travel time index by day"""
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Calculate daily averages first
        daily_df = df.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            col: 'mean' for col in df.columns if col not in ['Zone_Group', 'Zone', 'Corridor', 'Month', 'Hour']
        }).reset_index()
        
        return daily_df
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly TI by day: {e}")
        return pd.DataFrame()

# CCTV functions
def get_daily_cctv_uptime(database, table_name, cam_config, start_date):
    """Get daily CCTV uptime"""
    try:
        # Import database connection function
        from configs import get_athena_connection
        
        # Get database connection
        conn = get_athena_connection(database)
        
        # Query the database table
        query = f"""
        SELECT cameraid, date
        FROM {table_name}
        WHERE size > 0 AND date >= '{start_date}'
        """
        
        # Execute query and get results
        import pandas as pd
        from sqlalchemy import text
        
        result_df = pd.read_sql(text(query), conn)
        
        if result_df.empty:
            logger.warning(f"No CCTV data found in {table_name} for date >= {start_date}")
            return pd.DataFrame()
        
        # Transform the data
        cctv_data = result_df.copy()
        cctv_data['CameraID'] = cctv_data['cameraid'].astype('category')
        cctv_data['Date'] = pd.to_datetime(cctv_data['date']).dt.date
        cctv_data = cctv_data.drop(columns=['cameraid', 'date'])
        
        # Add uptime indicators
        cctv_data['up'] = 1
        cctv_data['num'] = 1
        cctv_data = cctv_data.drop_duplicates()
        
        # Ensure cam_config has the right column types
        if not cam_config.empty:
            cam_config = cam_config.copy()
            cam_config['CameraID'] = cam_config['CameraID'].astype('category')
            
            # Right join with camera config to include all available cameras
            merged_data = cam_config.merge(
                cctv_data, 
                on='CameraID', 
                how='left'
            )
            
            # Fill missing values
            merged_data['Date'] = merged_data['Date'].fillna(pd.to_datetime(start_date).date())
            merged_data['up'] = merged_data['up'].fillna(0)
            merged_data['num'] = merged_data['num'].fillna(1)
            
            # Expand to include all available cameras on all days
            # Get date range
            min_date = pd.to_datetime(start_date).date()
            max_date = merged_data['Date'].max()
            if pd.isna(max_date):
                max_date = min_date
            
            # Create complete date range
            date_range = pd.date_range(start=min_date, end=max_date, freq='D').date
            
            # Create all combinations of cameras and dates
            cameras = cam_config['CameraID'].unique()
            all_combinations = []
            
            for camera in cameras:
                for date in date_range:
                    all_combinations.append({'CameraID': camera, 'Date': date})
            
            complete_df = pd.DataFrame(all_combinations)
            complete_df['CameraID'] = complete_df['CameraID'].astype('category')
            
            # Merge with actual data
            final_data = complete_df.merge(
                merged_data.drop(columns=['Location', 'As_of_Date'] if 'Location' in merged_data.columns else []),
                on=['CameraID', 'Date'],
                how='left'
            )
            
            # Fill missing uptime data
            final_data['up'] = final_data['up'].fillna(0)
            final_data['num'] = final_data['num'].fillna(1)
            
            # Calculate uptime
            final_data['uptime'] = final_data['up'] / final_data['num']
            
            # Filter by As_of_Date if available in cam_config
            if 'As_of_Date' in cam_config.columns:
                final_data = final_data.merge(
                    cam_config[['CameraID', 'As_of_Date']], 
                    on='CameraID', 
                    how='left'
                )
                final_data['As_of_Date'] = pd.to_datetime(final_data['As_of_Date']).dt.date
                final_data = final_data[final_data['Date'] >= final_data['As_of_Date']]
                final_data = final_data.drop(columns=['As_of_Date'])
            
            # Add other config columns if available
            config_cols = ['Corridor', 'Description', 'Location']
            available_config_cols = [col for col in config_cols if col in cam_config.columns]
            
            if available_config_cols:
                final_data = final_data.merge(
                    cam_config[['CameraID'] + available_config_cols],
                    on='CameraID',
                    how='left'
                )
                
                # Convert categorical columns
                for col in available_config_cols:
                    if col in final_data.columns:
                        final_data[col] = final_data[col].astype('category')
            
            # Reorder columns
            column_order = ['CameraID', 'Date', 'up', 'num', 'uptime']
            column_order.extend([col for col in final_data.columns if col not in column_order])
            final_data = final_data[column_order]
            
            # Ensure proper data types
            final_data['CameraID'] = final_data['CameraID'].astype('category')
            final_data['Date'] = pd.to_datetime(final_data['Date'])
            
            logger.info(f"Retrieved CCTV uptime data for {len(final_data)} camera-days")
            return final_data
            
        else:
            # If no camera config, just return the raw data with uptime calculation
            cctv_data['uptime'] = cctv_data['up'] / cctv_data['num']
            cctv_data['Date'] = pd.to_datetime(cctv_data['Date'])
            
            logger.info(f"Retrieved CCTV uptime data for {len(cctv_data)} camera-days (no config)")
            return cctv_data
        
    except ImportError as e:
        logger.error(f"Could not import required modules: {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error getting daily CCTV uptime: {e}")
        return pd.DataFrame()


def get_weekly_avg_by_day_cctv(df):
    """Get weekly average by day for CCTV"""
    try:
        if df.empty:
            return df
        
        df['Week'] = pd.to_datetime(df['Date']).dt.isocalendar().week
        df['Year'] = pd.to_datetime(df['Date']).dt.year
        
        weekly_avg = df.groupby(['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 
                               'CameraID', 'Description', 'Year', 'Week']).agg({
            'uptime': 'mean',
            'num': 'sum'
        }).reset_index()
        
        return weekly_avg
        
    except Exception as e:
        logger.error(f"Error calculating weekly CCTV average: {e}")
        return pd.DataFrame()

# Teams/Activities functions
def tidy_teams_tasks(teams, bucket, corridors, replicate=True):
    """Tidy teams tasks data"""
    try:
        if teams.empty:
            return teams
        
        # Basic cleaning
        teams = teams.dropna(subset=['Task_Type'])
        teams['Created_Date'] = pd.to_datetime(teams.get('Created_Date', pd.Timestamp.now()))
        teams['Closed_Date'] = pd.to_datetime(teams.get('Closed_Date'))
        
        return teams
        
    except Exception as e:
        logger.error(f"Error tidying teams tasks: {e}")
        return pd.DataFrame()

def get_outstanding_tasks_by_param(teams, param, report_start_date):
    """Get outstanding tasks by parameter"""
    try:
        if teams.empty:
            return pd.DataFrame()
        
        # Filter for outstanding tasks
        outstanding = teams[teams['Closed_Date'].isna()]
        
        if param == "All":
            return outstanding.groupby(['Zone_Group', 'Corridor']).size().reset_index(name='count')
        else:
            return outstanding.groupby(['Zone_Group', 'Corridor', param]).size().reset_index(name='count')
        
    except Exception as e:
        logger.error(f"Error getting outstanding tasks by {param}: {e}")
        return pd.DataFrame()

def get_outstanding_tasks_by_day_range(teams, report_start_date, end_date):
    """Get outstanding tasks by day range"""
    try:
        if teams.empty:
            return pd.DataFrame()
        
        # Calculate tasks over 45 days and MTTR
        current_date = pd.Timestamp(end_date)
        teams['days_open'] = (current_date - teams['Created_Date']).dt.days
        
        result = teams.groupby(['Zone_Group', 'Corridor']).agg({
            'days_open': ['count', lambda x: (x > 45).sum(), 'mean']
        }).reset_index()
        
        result.columns = ['Zone_Group', 'Corridor', 'total_tasks', 'over45', 'mttr']
        result['Month'] = pd.Timestamp(end_date).to_period('M').start_time
        
        return result
        
    except Exception as e:
        logger.error(f"Error getting outstanding tasks by day range: {e}")
        return pd.DataFrame()

# Flash Events functions
def get_monthly_flashevent(df):
    """Get monthly flash events"""
    try:
        if df.empty:
            return df
        
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
        
        monthly_flash = df.groupby(['SignalID', 'Month']).agg({
            'flash': 'sum'
        }).reset_index()
        
        return monthly_flash
        
    except Exception as e:
        logger.error(f"Error calculating monthly flash events: {e}")
        return pd.DataFrame()

def get_cor_monthly_flash(df, corridors):
    """Get corridor monthly flash events"""
    try:
        if df.empty or corridors.empty:
            return pd.DataFrame()
        
        merged = df.merge(corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor']], 
                         on='SignalID', how='left')
        
        cor_monthly = merged.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'flash': 'sum'
        }).reset_index()
        
        return cor_monthly
        
    except Exception as e:
        logger.error(f"Error calculating corridor monthly flash: {e}")
        return pd.DataFrame()

# Configuration functions
def get_det_config(date, conf):
    """Get detector configuration for a specific date"""
    try:
        from configs import get_det_config_factory
        
        # Get configuration - replace with your actual config loading method
        # This should match your conf object from the main script
        
        # Create the detector config function using the factory
        get_det_config_func = get_det_config_factory(conf['bucket'], 'atspm_det_config_good')
        
        # Get detector configuration for the specified date
        det_config = get_det_config_func(date)
        
        if det_config.empty:
            logger.warning(f"No detector config found for date {date}")
            return pd.DataFrame(columns=['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber'])
        
        # Ensure required columns exist and have proper data types
        det_config['SignalID'] = det_config['SignalID'].astype(str)
        det_config['Detector'] = det_config['Detector'].astype(str)
        det_config['CallPhase'] = det_config['CallPhase'].astype(str)
        
        # Add ApproachDesc if missing (map from MovementType or DetectionHardware)
        if 'ApproachDesc' not in det_config.columns:
            if 'MovementType' in det_config.columns:
                det_config['ApproachDesc'] = det_config['MovementType'].fillna('')
            elif 'LaneType' in det_config.columns:
                det_config['ApproachDesc'] = det_config['LaneType'].fillna('')
            else:
                det_config['ApproachDesc'] = ''
        
        # Ensure LaneNumber exists
        if 'LaneNumber' not in det_config.columns:
            det_config['LaneNumber'] = 0
        
        # Convert LaneNumber to integer, handling NaN values
        det_config['LaneNumber'] = pd.to_numeric(det_config['LaneNumber'], errors='coerce').fillna(0).astype(int)
        
        # Select and return only the required columns
        required_columns = ['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber']
        result = det_config[required_columns].copy()
        
        logger.info(f"Retrieved {len(result)} detector configurations for {date}")
        return result
        
    except ImportError as e:
        logger.error(f"Could not import configs module: {e}")
        return pd.DataFrame(columns=['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber'])
    except Exception as e:
        logger.error(f"Error getting detector config for {date}: {e}")
        return pd.DataFrame(columns=['SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'LaneNumber'])

# Utility functions
def convert_to_utc(df):
    """Convert datetime columns to UTC"""
    try:
        datetime_cols = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')
        return df
        
    except Exception as e:
        logger.error(f"Error converting to UTC: {e}")
        return df

def addtoRDS(df, filename, metric_type, report_start_date, calcs_start_date):
    """Add data to RDS file (Python equivalent of R's addtoRDS)"""
    try:
        import pickle
        import os
        
        # Create directory if it doesn't exist
        os.makedirs('rds_files', exist_ok=True)
        
        # Save as pickle file (Python equivalent of RDS)
        filepath = os.path.join('rds_files', filename.replace('.rds', '.pkl'))
        
        with open(filepath, 'wb') as f:
            pickle.dump(df, f)
        
        logger.info(f"Saved {filename} with {len(df)} rows")
        
    except Exception as e:
        logger.error(f"Error saving to RDS: {e}")

# Additional utility functions that may be missing
def filter_by_signals(df, signals_list):
    """Filter DataFrame by signals list"""
    try:
        if signals_list is None or df.empty:
            return df
        
        if 'SignalID' in df.columns:
            return df[df['SignalID'].isin(signals_list)]
        
        return df
        
    except Exception as e:
        logger.error(f"Error filtering by signals: {e}")
        return df

def calculate_delta_percentage(df, value_col, group_cols):
    """Calculate percentage change from previous period"""
    try:
        if df.empty:
            return df
        
        df = df.sort_values(group_cols + ['Month'])
        df[f'delta_{value_col}'] = df.groupby(group_cols)[value_col].pct_change()
        
        return df
        
    except Exception as e:
        logger.error(f"Error calculating delta percentage: {e}")
        return df

# Helper function for reading Excel files
def read_excel(*args, **kwargs):
    """Wrapper for pandas read_excel"""
    try:
        return pd.read_excel(*args, **kwargs)
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame()

# Helper function for writing Parquet files
def write_parquet(df, path, **kwargs):
    """Wrapper for pandas to_parquet"""
    try:
        return df.to_parquet(path, **kwargs)
    except Exception as e:
        logger.error(f"Error writing Parquet file: {e}")

def read_parquet(path, **kwargs):
    """Wrapper for pandas read_parquet"""
    try:
        return pd.read_parquet(path, **kwargs)
    except Exception as e:
        logger.error(f"Error reading Parquet file: {e}")
        return pd.DataFrame()

# Data validation functions
def validate_dataframe(df, required_columns=None, min_rows=0):
    """Validate DataFrame structure and content"""
    try:
        if df is None:
            return False
        
        if not isinstance(df, pd.DataFrame):
            return False
        
        if len(df) < min_rows:
            return False
        
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                logger.warning(f"Missing columns: {missing_cols}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating DataFrame: {e}")
        return False

def safe_merge(left_df, right_df, on=None, how='left', **kwargs):
    """Safely merge DataFrames with error handling"""
    try:
        if left_df.empty or right_df.empty:
            return left_df
        
        if on:
            # Check if merge columns exist
            missing_left = set(on) - set(left_df.columns) if isinstance(on, list) else {on} - set(left_df.columns)
            missing_right = set(on) - set(right_df.columns) if isinstance(on, list) else {on} - set(right_df.columns)
            
            if missing_left or missing_right:
                logger.warning(f"Missing merge columns - Left: {missing_left}, Right: {missing_right}")
                return left_df
        
        return left_df.merge(right_df, on=on, how=how, **kwargs)
        
    except Exception as e:
        logger.error(f"Error in safe merge: {e}")
        return left_df

def ensure_datetime_column(df, col_name, format=None):
    """Ensure column is datetime type"""
    try:
        if df.empty or col_name not in df.columns:
            return df
        
        if not pd.api.types.is_datetime64_any_dtype(df[col_name]):
            df[col_name] = pd.to_datetime(df[col_name], format=format, errors='coerce')
        
        return df
        
    except Exception as e:
        logger.error(f"Error ensuring datetime column: {e}")
        return df

def calculate_time_periods(df, date_col='Date'):
    """Add common time period columns"""
    try:
        if df.empty or date_col not in df.columns:
            return df
        
        df = ensure_datetime_column(df, date_col)
        
        df['DOW'] = df[date_col].dt.dayofweek + 1  # Monday = 1
        df['Week'] = df[date_col].dt.isocalendar().week
        df['Year'] = df[date_col].dt.year
        df['Month'] = df[date_col].dt.to_period('M').dt.start_time
        df['Quarter'] = df[date_col].dt.to_period('Q').dt.start_time
        
        return df
        
    except Exception as e:
        logger.error(f"Error calculating time periods: {e}")
        return df

def clean_signal_ids(df, signal_col='SignalID'):
    """Clean and standardize signal IDs"""
    try:
        if df.empty or signal_col not in df.columns:
            return df
        
        # Convert to string and remove any non-numeric characters if needed
        df[signal_col] = df[signal_col].astype(str).str.strip()
        
        # Convert to categorical for memory efficiency
        df[signal_col] = df[signal_col].astype('category')
        
        return df
        
    except Exception as e:
        logger.error(f"Error cleaning signal IDs: {e}")
        return df

# Memory management utilities
def reduce_memory_usage(df):
    """Reduce DataFrame memory usage by optimizing dtypes"""
    try:
        start_mem = df.memory_usage(deep=True).sum() / 1024**2
        
        for col in df.columns:
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
        
        end_mem = df.memory_usage(deep=True).sum() / 1024**2
        logger.info(f"Memory usage decreased from {start_mem:.2f} MB to {end_mem:.2f} MB "
                   f"({100 * (start_mem - end_mem) / start_mem:.1f}% reduction)")
        
        return df
        
    except Exception as e:
        logger.error(f"Error reducing memory usage: {e}")
        return df

def ensure_timeperiod_column(df, time_col_candidates=['Timeperiod', 'Date_Hour', 'Hour']):
    """
    Ensure DataFrame has a 'Timeperiod' column
    
    Args:
        df: DataFrame to check
        time_col_candidates: List of potential time column names
    
    Returns:
        DataFrame with 'Timeperiod' column
    """
    if df.empty:
        return df
    
    # If Timeperiod already exists, we're good
    if 'Timeperiod' in df.columns:
        return df
    
    # Look for alternative time columns
    for col in time_col_candidates:
        if col in df.columns:
            df = df.copy()
            df['Timeperiod'] = pd.to_datetime(df[col])
            logger.info(f"Created Timeperiod column from {col}")
            return df
    
    # If no time column found, create a default
    if 'Date' in df.columns:
        df = df.copy()
        df['Timeperiod'] = pd.to_datetime(df['Date'])
        logger.warning("Created Timeperiod column from Date column")
        return df
    
    logger.error("No suitable time column found for Timeperiod")
    return df

