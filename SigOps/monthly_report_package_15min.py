#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monthly_Report_Package_15min.py -- For 15-min Data

Converted from R to Python
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import time
import logging
from multiprocessing import Pool, cpu_count
import boto3
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
from pathlib import Path
import yaml, re
import traceback

# Add the parent directory to the path to import local modules
sys.path.append(str(Path(__file__).parent.parent))

# Import local modules
# from monthly_report_package_init import *
from SigOps.aggregations import sigify
# from database_functions import *
# from utilities import *
# from aggregations import *
from configs import get_corridors
from s3_parquet_io import s3_read_parquet_parallel
from SigOps.athena_helpers import s3_read_parquet_parallel_athena

# Setup logging
logger = logging.getLogger(__name__)

def get_usable_cores():
    """Get number of usable CPU cores"""
    return max(1, cpu_count() - 1)

def get_period_sum(df, value_col, period_col):
    """Get sum by period"""
    try:
        return df.groupby(['SignalID', 'CallPhase', period_col])[value_col].sum().reset_index()
    except Exception as e:
        logger.error(f"Error in get_period_sum: {e}")
        return pd.DataFrame()

def get_period_avg(df, value_col, period_col, weight_col=None):
    """Get average by period"""
    try:
        if weight_col:
            # Weighted average
            df['weighted_value'] = df[value_col] * df[weight_col]
            result = df.groupby(['SignalID', 'CallPhase', period_col]).agg({
                'weighted_value': 'sum',
                weight_col: 'sum'
            }).reset_index()
            result[value_col] = result['weighted_value'] / result[weight_col]
            return result[['SignalID', 'CallPhase', period_col, value_col]]
        else:
            return df.groupby(['SignalID', 'CallPhase', period_col])[value_col].mean().reset_index()
    except Exception as e:
        logger.error(f"Error in get_period_avg: {e}")
        return pd.DataFrame()

def get_cor_monthly_avg_by_period(df, corridors, value_col, period_col):
    """Get corridor monthly average by period"""
    try:
        # Merge with corridors
        df_with_corridors = df.merge(corridors[['SignalID', 'Corridor']], on='SignalID', how='left')
        
        # Add time components
        df_with_corridors['Date'] = pd.to_datetime(df_with_corridors[period_col]).dt.date
        df_with_corridors['Hour'] = pd.to_datetime(df_with_corridors[period_col]).dt.hour
        df_with_corridors['Minute'] = pd.to_datetime(df_with_corridors[period_col]).dt.minute
        
        # Group by corridor and time period
        result = df_with_corridors.groupby(['Corridor', 'Hour', 'Minute'])[value_col].mean().reset_index()
        return result
        
    except Exception as e:
        logger.error(f"Error in get_cor_monthly_avg_by_period: {e}")
        return pd.DataFrame()

def addtoRDS(df, filename, value_col, rds_start_date, calcs_start_date):
    """Add data to RDS file (equivalent to R's addtoRDS)"""
    try:
        # Filter data from calcs_start_date
        if 'Date' in df.columns:
            df_filtered = df[pd.to_datetime(df['Date']) >= pd.to_datetime(calcs_start_date)]
        elif 'Timeperiod' in df.columns:
            df_filtered = df[pd.to_datetime(df['Timeperiod']) >= pd.to_datetime(calcs_start_date)]
        else:
            df_filtered = df
        
        # Save as pickle (Python equivalent of RDS)
        df_filtered.to_pickle(filename)
        logger.info(f"Saved {len(df_filtered)} records to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving to {filename}: {e}")

def parse_relative_date(date_string):
    """
    Parse relative date strings like 'yesterday', '2 days ago', etc.
    
    Args:
        date_string: String representation of a date (could be relative or absolute)
    
    Returns:
        datetime object
    """
    if isinstance(date_string, (datetime, date)):
        return date_string
    
    if not isinstance(date_string, str):
        return datetime.now()
    
    date_string = date_string.strip().lower()
    now = datetime.now()
    
    # Handle relative date strings
    if date_string == 'today':
        return now.date()
    elif date_string == 'yesterday':
        return (now - timedelta(days=1)).date()
    elif date_string == 'tomorrow':
        return (now + timedelta(days=1)).date()
    elif 'ago' in date_string:
        # Parse patterns like "2 days ago", "1 week ago", "3 months ago"
        match = re.match(r'(\d+)\s+(day|days|week|weeks|month|months|year|years)\s+ago', date_string)
        if match:
            number = int(match.group(1))
            unit = match.group(2)
            
            if unit.startswith('day'):
                return (now - timedelta(days=number)).date()
            elif unit.startswith('week'):
                return (now - timedelta(weeks=number)).date()
            elif unit.startswith('month'):
                return (now - relativedelta(months=number)).date()
            elif unit.startswith('year'):
                return (now - relativedelta(years=number)).date()
    elif 'from now' in date_string or 'later' in date_string:
        # Parse patterns like "2 days from now", "1 week later"
        match = re.match(r'(\d+)\s+(day|days|week|weeks|month|months|year|years)\s+(from now|later)', date_string)
        if match:
            number = int(match.group(1))
            unit = match.group(2)
            
            if unit.startswith('day'):
                return (now + timedelta(days=number)).date()
            elif unit.startswith('week'):
                return (now + timedelta(weeks=number)).date()
            elif unit.startswith('month'):
                return (now + relativedelta(months=number)).date()
            elif unit.startswith('year'):
                return (now + relativedelta(years=number)).date()
    
    # If not a relative date, try to parse as regular date
    try:
        parsed_date = pd.to_datetime(date_string)
        return parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date
    except Exception as e:
        logger.warning(f"Could not parse date string '{date_string}': {e}. Using current date.")
        return now.date()

def normalize_time_column(df, column):
    if column in df.columns:
        df[column] = pd.to_datetime(df[column]).dt.tz_localize(None)
    return df

def make_hashable(df, columns):
    """
    Ensure specified columns in a DataFrame are hashable (no lists, sets, etc.).
    Converts lists to tuples, leaves scalars as is.
    """
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: tuple(x) if isinstance(x, list) else x
            )
    return df


def main():
    """Main function for 15-min package processing"""
    
    print(f"{datetime.now()} Starting 15min Package")
    
    # Initialize from config
    conf = load_config()
    corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=True)
    signals_list = corridors['SignalID'].unique().tolist()
    
    # For hourly counts (no monthly or weekly), go back to first missing day in the database
    calcs_start_date = get_date_from_string(
        conf['start_date'], table_include_regex_pattern="sig_qhr_", exceptions=0
    )
    
    report_end_date_str = conf.get('report_end_date', datetime.now())
    report_end_date = parse_relative_date(report_end_date_str)
    
    # Ensure report_end_date is a date object
    if isinstance(report_end_date, datetime):
        report_end_date = report_end_date.date()
    
    # Limit report end date
    report_end_date = min(report_end_date, calcs_start_date + timedelta(days=7))
    
    print(f"{datetime.now()} 15min Package Start Date: {calcs_start_date}")
    print(f"{datetime.now()} Report End Date: {report_end_date}")
    
    # Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
    rds_start_date = pd.to_datetime(calcs_start_date) - timedelta(days=1)
    
    # Only keep 6 months of data for 15min aggregations
    report_start_date = (pd.to_datetime(report_end_date).replace(day=1) - 
                        relativedelta(months=6))
    
    # DETECTOR UPTIME
    print(f"{datetime.now()} Vehicle Detector Uptime [1 of 29 (sigops 15min)]")
    
    # DAILY PEDESTRIAN PUSHBUTTON UPTIME
    print(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (sigops 15min)]")
    
    # WATCHDOG
    print(f"{datetime.now()} watchdog alerts [3 of 29 (sigops 15min)]")
    
    # DAILY PEDESTRIAN ACTIVATIONS
    print(f"{datetime.now()} Daily Pedestrian Activations [4 of 29 (sigops 15min)]")
    
    # 15-MINUTE PEDESTRIAN ACTIVATIONS
    print(f"{datetime.now()} 15-Minute Pedestrian Activations [5 of 29 (sigops 15min)]")
    
    try:
        paph = s3_read_parquet_parallel_athena(
            table_name="counts_ped_15min",
            start_date=rds_start_date,
            end_date=report_end_date,
            signals_list=signals_list,
            conf=conf,
            parallel=False
        )
        
        if not paph.empty:
            paph = paph.dropna(subset=['CallPhase'])  # Added to exclude non-programmed ped pushbuttons
            paph['SignalID'] = paph['SignalID'].astype('category')
            paph['Detector'] = paph['Detector'].astype('category')
            paph['CallPhase'] = paph['CallPhase'].astype('category')
            paph['Date'] = pd.to_datetime(paph['Date']).dt.date
            paph['DOW'] = pd.to_datetime(paph['Date']).dt.dayofweek + 1
            paph['Week'] = pd.to_datetime(paph['Date']).dt.isocalendar().week
            paph['vol'] = pd.to_numeric(paph['vol'])
            
            bad_ped_detectors = s3_read_parquet_parallel_athena(
                table_name="bad_ped_detectors",
                start_date=rds_start_date,
                end_date=report_end_date,
                signals_list=signals_list,
                conf=conf,
                parallel=False
            )
            
            if not bad_ped_detectors.empty:
                bad_ped_detectors['SignalID'] = bad_ped_detectors['SignalID'].astype('category')
                bad_ped_detectors['Detector'] = bad_ped_detectors['Detector'].astype('category')
                
                # Filter out bad days
                paph = paph[['SignalID', 'Timeperiod', 'CallPhase', 'Detector', 'vol']]
                paph = paph.merge(bad_ped_detectors, on=['SignalID', 'Detector'], how='left', indicator=True)
                paph = paph[paph['_merge'] == 'left_only'].drop('_merge', axis=1)
            
            # Create all timeperiods
            min_time = pd.to_datetime(paph['Timeperiod']).min().floor('D')
            max_time = pd.to_datetime(paph['Timeperiod']).max().floor('D') + timedelta(days=1) - timedelta(minutes=15)
            all_timeperiods = pd.date_range(min_time, max_time, freq='15min')
            
            # Expand to include all timeperiods
            # Ensure columns are hashable (convert lists to strings)
            for col in ['SignalID', 'Detector', 'CallPhase']:
                if col in paph.columns:
                    paph[col] = paph[col].apply(lambda x: str(x) if isinstance(x, list) else x)

            signal_detector_combos = (
                paph[['SignalID', 'Detector', 'CallPhase']]
                .drop_duplicates()
                .apply(tuple, axis=1)  # convert each row into a tuple
            )

            expanded = pd.MultiIndex.from_product([
                signal_detector_combos,
                all_timeperiods
            ], names=['combo', 'Timeperiod']).to_frame(index=False)

            
            expanded[['SignalID', 'Detector', 'CallPhase']] = pd.DataFrame(
                expanded['combo'].tolist(), index=expanded.index
            )
            expanded = expanded.drop('combo', axis=1)
            
            paph = expanded.merge(paph, on=['SignalID', 'Detector', 'CallPhase', 'Timeperiod'], how='left')
            paph['vol'] = paph['vol'].fillna(0)
            pa_15min = get_period_sum(paph, "vol", "Timeperiod")
            cor_15min_pa = get_cor_monthly_avg_by_period(pa_15min, corridors, "vol", "Timeperiod")
            pa_15min = sigify(pa_15min, cor_15min_pa, corridors)
            # Ensure period column exists
            if 'Timeperiod' not in pa_15min.columns:
                pa_15min['Timeperiod'] = pd.NaT

            # Ensure delta column exists
            if 'delta' not in pa_15min.columns:
                pa_15min['delta'] = 0
            pa_15min = pa_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'vol', 'delta']]
            
            addtoRDS(pa_15min, "pa_15min.pkl", "vol", rds_start_date, calcs_start_date)
            addtoRDS(cor_15min_pa, "cor_15min_pa.pkl", "vol", rds_start_date, calcs_start_date)
            
            del paph, bad_ped_detectors, pa_15min, cor_15min_pa
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e, traceback.format_exc())
    
    # GET PEDESTRIAN DELAY
    print(f"{datetime.now()} Pedestrian Delay [6 of 29 (sigops 15min)]")
    
    # GET COMMUNICATIONS UPTIME
    print(f"{datetime.now()} Communication Uptime [7 of 29 (sigops 15min)]")
    
    # DAILY VOLUMES
    print(f"{datetime.now()} Daily Volumes [8 of 29 (sigops 15min)]")
    
    # 15-MINUTE VOLUMES
    print(f"{datetime.now()} 15-Minute Volumes [9 of 29 (sigops 15min)]")
    
    try:
        vph = s3_read_parquet_parallel_athena(
            table_name="vehicles_15min",
            start_date=rds_start_date,
            end_date=report_end_date,
            signals_list=signals_list,
            conf=conf
        )
        
        if not vph.empty:
            vph['SignalID'] = vph['SignalID'].astype('category')
            vph['CallPhase'] = pd.Categorical([2] * len(vph))  # Hack because next function needs a CallPhase
            vph['Date'] = pd.to_datetime(vph['Date']).dt.date
            vph = vph.rename(columns={'vph': 'vol'})
            
            vol_15min = get_period_sum(vph, "vol", "Timeperiod")
            cor_15min_vol = get_cor_monthly_avg_by_period(vol_15min, corridors, "vol", "Timeperiod")
            
            vol_15min = sigify(vol_15min, cor_15min_vol, corridors)
            # Ensure period column exists
            if 'Timeperiod' not in vol_15min.columns:
                vol_15min['Timeperiod'] = pd.NaT

            # Ensure delta column exists
            if 'delta' not in vol_15min.columns:
                vol_15min['delta'] = 0

            vol_15min = vol_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'vol', 'delta']]
            
            addtoRDS(vol_15min, "vol_15min.pkl", "vol", rds_start_date, calcs_start_date)
            addtoRDS(cor_15min_vol, "cor_15min_vol.pkl", "vol", rds_start_date, calcs_start_date)
            
            del vph, vol_15min, cor_15min_vol
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)
    
    # DAILY THROUGHPUT
    print(f"{datetime.now()} Daily Throughput [10 of 29 (sigops 15min)]")
    
    # DAILY ARRIVALS ON GREEN
    print(f"{datetime.now()} Daily AOG [11 of 29 (sigops 15min)]")
    
    # 15-MINUTE ARRIVALS ON GREEN
    print(f"{datetime.now()} 15-Minute AOG [12 of 29 (sigops 15min)]")
    
    try:
        aog = s3_read_parquet_parallel_athena(
            table_name="arrivals_on_green_15min",
            start_date=rds_start_date,
            end_date=report_end_date,
            signals_list=signals_list,
            conf=conf
        )
        
        if not aog.empty:
            aog['SignalID'] = aog['SignalID'].astype('category')
            aog['CallPhase'] = aog['CallPhase'].astype('category')
            aog['Date'] = pd.to_datetime(aog['Date']).dt.date
            aog['DOW'] = pd.to_datetime(aog['Date']).dt.dayofweek + 1
            aog['Week'] = pd.to_datetime(aog['Date']).dt.isocalendar().week
            
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            aog = aog.rename(columns={'Date_Period': 'Timeperiod'})
            aog = aog[['SignalID', 'CallPhase', 'Timeperiod', 'aog', 'pr', 'vol', 'Date']]
            
            # Create complete timeperiod range
            timeperiod_range = pd.date_range(
                start=pd.to_datetime(rds_start_date),
                end=pd.to_datetime(report_end_date) - timedelta(minutes=15),
                freq='15min'
            )
            
            # Ensure hashable values
            for col in ['SignalID', 'Date']:
                if col in aog.columns:
                    aog[col] = aog[col].apply(lambda x: str(x) if isinstance(x, list) else x)

            signal_date_combos = (
                aog[['SignalID', 'Date']]
                .drop_duplicates()
                .apply(tuple, axis=1)  # Convert each row to a tuple
            )

            expanded_aog = pd.MultiIndex.from_product([
                signal_date_combos,
                timeperiod_range
            ], names=['combo', 'Timeperiod']).to_frame(index=False)

            # Split tuple back into separate columns
            expanded_aog[['SignalID', 'Date']] = pd.DataFrame(
                expanded_aog['combo'].tolist(), index=expanded_aog.index
            )
            expanded_aog = expanded_aog.drop('combo', axis=1)
            expanded_aog = normalize_time_column(expanded_aog, 'Timeperiod')
            aog = normalize_time_column(aog, 'Timeperiod')
            aog = expanded_aog.merge(aog, on=['SignalID', 'Date', 'Timeperiod'], how='left')
            
            aog_15min = get_period_avg(aog, "aog", "Timeperiod", "vol")
            cor_15min_aog = get_cor_monthly_avg_by_period(aog_15min, corridors, "aog", "Timeperiod")
            
            aog_15min = sigify(aog_15min, cor_15min_aog, corridors)
            # Ensure period column exists
            if 'Timeperiod' not in aog_15min.columns:
                aog_15min['Timeperiod'] = pd.NaT

            # Ensure delta column exists
            if 'delta' not in aog_15min.columns:
                aog_15min['delta'] = 0

            aog_15min = aog_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'aog', 'delta']]
            
            addtoRDS(aog_15min, "aog_15min.pkl", "aog", rds_start_date, calcs_start_date)
            addtoRDS(cor_15min_aog, "cor_15min_aog.pkl", "aog", rds_start_date, calcs_start_date)
            
            del aog_15min, cor_15min_aog
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e, traceback.format_exc())
    
    # DAILY PROGRESSION RATIO
    print(f"{datetime.now()} Daily Progression Ratio [13 of 29 (sigops 15min)]")
    
    # 15-MINUTE PROGRESSION RATIO
    print(f"{datetime.now()} 15-Minute Progression Ratio [14 of 29 (sigops 15min)]")
    
    try:
        # Use the aog data from previous step
        if 'aog' in locals():
            # Create complete timeperiod range
            timeperiod_range = pd.date_range(
                start=pd.to_datetime(rds_start_date),
                end=pd.to_datetime(report_end_date) - timedelta(minutes=15),
                freq='15min'
            )
            
            # Complete the data for PR
            # Ensure hashable values
            aog = make_hashable(aog, ['SignalID', 'Date'])

            signal_date_combos = (
                aog[['SignalID', 'Date']]
                .drop_duplicates()
                .apply(tuple, axis=1)  # each row is now a tuple
            )

            expanded_pr = pd.MultiIndex.from_product([
                signal_date_combos,
                timeperiod_range
            ], names=['combo', 'Timeperiod']).to_frame(index=False)

            expanded_pr[['SignalID', 'Date']] = pd.DataFrame(
                expanded_pr['combo'].tolist(), index=expanded_pr.index
            )
            expanded_pr = expanded_pr.drop('combo', axis=1)

            
            aog_for_pr = expanded_pr.merge(aog, on=['SignalID', 'Date', 'Timeperiod'], how='left')
            
            pr_15min = get_period_avg(aog_for_pr, "pr", "Timeperiod", "vol")
            cor_15min_pr = get_cor_monthly_avg_by_period(pr_15min, corridors, "pr", "Timeperiod")
            
            pr_15min = sigify(pr_15min, cor_15min_pr, corridors)
            # Ensure period column exists
            if 'Timeperiod' not in pr_15min.columns:
                pr_15min['Timeperiod'] = pd.NaT

            # Ensure delta column exists
            if 'delta' not in pr_15min.columns:
                pr_15min['delta'] = 0
            pr_15min = pr_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'pr', 'delta']]
            
            addtoRDS(pr_15min, "pr_15min.pkl", "pr", rds_start_date, calcs_start_date)
            addtoRDS(cor_15min_pr, "cor_15min_pr.pkl", "pr", rds_start_date, calcs_start_date)
            
            del aog, pr_15min, cor_15min_pr
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e, traceback.format_exc())
    
    # DAILY SPLIT FAILURES
    print(f"{datetime.now()} Daily Split Failures [15 of 29 (sigops 15min)]")
    
    # 15-MINUTE SPLIT FAILURES
    print(f"{datetime.now()} 15-Minute Split Failures [16 of 29 (sigops 15min)]")
    
    try:
        sf = s3_read_parquet_parallel_athena(
            table_name="split_failures_15min",
            start_date=rds_start_date,
            end_date=report_end_date,
            signals_list=signals_list,
            conf=conf,
            callback=lambda x: x[x['CallPhase'] == 0]
        )
        
        if not sf.empty:
            sf['SignalID'] = sf['SignalID'].astype('category')
            sf['CallPhase'] = sf['CallPhase'].astype('category')
            sf['Date'] = pd.to_datetime(sf['Date']).dt.date
            
            sf = sf.rename(columns={'Date_Hour': 'Timeperiod'})
            sf = sf[['SignalID', 'CallPhase', 'Timeperiod', 'sf_freq', 'Date']]
            
            # Create complete timeperiod range
            timeperiod_range = pd.date_range(
                start=pd.to_datetime(rds_start_date),
                end=pd.to_datetime(report_end_date) - timedelta(minutes=15),
                freq='15min'
            )
            
            # Complete the data
            signal_date_phase_combos = sf[['SignalID', 'Date', 'CallPhase']].drop_duplicates()
            expanded_sf = pd.MultiIndex.from_product([
                signal_date_phase_combos.values.tolist(),
                timeperiod_range
            ], names=['combo', 'Timeperiod']).to_frame(index=False)
            
            expanded_sf[['SignalID', 'Date', 'CallPhase']] = pd.DataFrame(
                expanded_sf['combo'].tolist(), index=expanded_sf.index
            )
            expanded_sf = expanded_sf.drop('combo', axis=1)
            
            sf = expanded_sf.merge(sf, on=['SignalID', 'Date', 'CallPhase', 'Timeperiod'], how='left')
            sf['sf_freq'] = sf['sf_freq'].fillna(0)
            
            sf_15min = get_period_avg(sf, "sf_freq", "Timeperiod")
            cor_15min_sf = get_cor_monthly_avg_by_period(sf_15min, corridors, "sf_freq", "Timeperiod")
            
            sf_15min = sigify(sf_15min, cor_15min_sf, corridors)
            sf_15min = sf_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'sf_freq', 'delta']]
            
            addtoRDS(sf_15min, "sf_15min.pkl", "sf_freq", rds_start_date, rds_start_date)
            addtoRDS(cor_15min_sf, "cor_15min_sf.pkl", "sf_freq", rds_start_date, calcs_start_date)
            
            del sf, sf_15min, cor_15min_sf
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)
    
    # DAILY QUEUE SPILLBACK
    print(f"{datetime.now()} Daily Queue Spillback [17 of 29 (sigops 15min)]")
    
    # 15-MINUTE QUEUE SPILLBACK
    print(f"{datetime.now()} 15-Minute Queue Spillback [18 of 29 (sigops 15min)]")
    
    try:
        qs = s3_read_parquet_parallel_athena(
            table_name="queue_spillback_15min",
            start_date=rds_start_date,
            end_date=report_end_date,
            signals_list=signals_list,
            conf=conf
        )
        
        if not qs.empty:
            qs['SignalID'] = qs['SignalID'].astype('category')
            qs['CallPhase'] = qs['CallPhase'].astype('category')
            qs['Date'] = pd.to_datetime(qs['Date']).dt.date
            
            qs = qs.rename(columns={'Date_Hour': 'Timeperiod'})
            qs = qs[['SignalID', 'CallPhase', 'Timeperiod', 'qs_freq', 'Date']]
            
            # Create complete timeperiod range
            timeperiod_range = pd.date_range(
                start=pd.to_datetime(rds_start_date),
                end=pd.to_datetime(report_end_date) - timedelta(minutes=15),
                freq='15min'
            )
            
            # Complete the data
            signal_date_phase_combos = qs[['SignalID', 'Date', 'CallPhase']].drop_duplicates()
            expanded_qs = pd.MultiIndex.from_product([
                signal_date_phase_combos.values.tolist(),
                timeperiod_range
            ], names=['combo', 'Timeperiod']).to_frame(index=False)
            
            expanded_qs[['SignalID', 'Date', 'CallPhase']] = pd.DataFrame(
                expanded_qs['combo'].tolist(), index=expanded_qs.index
            )
            expanded_qs = expanded_qs.drop('combo', axis=1)
            
            qs = expanded_qs.merge(qs, on=['SignalID', 'Date', 'CallPhase', 'Timeperiod'], how='left')
            qs['qs_freq'] = qs['qs_freq'].fillna(0)
            
            qs_15min = get_period_avg(qs, "qs_freq", "Timeperiod")
            cor_15min_qs = get_cor_monthly_avg_by_period(qs_15min, corridors, "qs_freq", "Timeperiod")
            
            qs_15min = sigify(qs_15min, cor_15min_qs, corridors)
            qs_15min = qs_15min[['Zone_Group', 'Corridor', 'Timeperiod', 'qs_freq', 'delta']]
            
            addtoRDS(qs_15min, "qs_15min.pkl", "qs_freq", rds_start_date, rds_start_date)
            addtoRDS(cor_15min_qs, "cor_15min_qs.pkl", "qs_freq", rds_start_date, calcs_start_date)
            
            del qs, qs_15min, cor_15min_qs
            
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)
    
    # TRAVEL TIME AND BUFFER TIME INDEXES
    print(f"{datetime.now()} Travel Time Indexes [19 of 29 (sigops 15min)]")
    
    # CCTV UPTIME From 511 and Encoders
    print(f"{datetime.now()} CCTV Uptimes [20 of 29 (sigops 15min)]")
    
    # ACTIVITIES
    print(f"{datetime.now()} TEAMS [21 of 29 (sigops 15min)]")
    
    # USER DELAY COSTS
    print(f"{datetime.now()} User Delay Costs [22 of 29 (sigops 15min)]")
    
    # Flash Events
    print(f"{datetime.now()} Flash Events [23 of 29 (sigops 15min)]")
    
    # BIKE/PED SAFETY INDEX
    print(f"{datetime.now()} Bike/Ped Safety Index [24 of 29 (sigops 15min)]")
    
    # RELATIVE SPEED INDEX
    print(f"{datetime.now()} Relative Speed Index [25 of 29 (sigops 15min)]")
    
    # CRASH INDICES
    print(f"{datetime.now()} Crash Indices [26 of 29 (sigops 15min)]")
    
    # Package up for Flexdashboard
    print(f"{datetime.now()} Package for Monthly Report [27 of 29 (sigops 15min)]")
    
    try:
        sig = {
            'qhr': {}
        }
        
        # Load all the saved data files
        try:
            sig['qhr']['vph'] = pd.read_pickle("vol_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['vph'] = pd.DataFrame()
            
        try:
            sig['qhr']['paph'] = pd.read_pickle("pa_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['paph'] = pd.DataFrame()
            
        try:
            sig['qhr']['aogh'] = pd.read_pickle("aog_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['aogh'] = pd.DataFrame()
            
        try:
            sig['qhr']['prh'] = pd.read_pickle("pr_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['prh'] = pd.DataFrame()
            
        try:
            sig['qhr']['sfh'] = pd.read_pickle("sf_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['sfh'] = pd.DataFrame()
            
        try:
            sig['qhr']['qsh'] = pd.read_pickle("qs_15min.pkl")
        except FileNotFoundError:
            sig['qhr']['qsh'] = pd.DataFrame()
            
        logger.info("Successfully packaged 15min data for monthly report")
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)
        logger.error(f"Error packaging data for monthly report: {e}")
    
    # Upload to AWS
    print(f"{datetime.now()} Upload to AWS [28 of 29 (sigops 15min)]")
    
    try:
        # Upload processed files to S3
        s3_client = boto3.client('s3')
        bucket = conf['bucket']
        
        files_to_upload = [
            "vol_15min.pkl",
            "pa_15min.pkl", 
            "aog_15min.pkl",
            "pr_15min.pkl",
            "sf_15min.pkl",
            "qs_15min.pkl",
            "cor_15min_pa.pkl",
            "cor_15min_vol.pkl",
            "cor_15min_aog.pkl",
            "cor_15min_pr.pkl",
            "cor_15min_sf.pkl",
            "cor_15min_qs.pkl"
        ]
        
        for filename in files_to_upload:
            if os.path.exists(filename):
                try:
                    s3_key = f"monthly_reports/15min/{filename}"
                    s3_client.upload_file(filename, bucket, s3_key)
                    logger.info(f"Uploaded {filename} to s3://{bucket}/{s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {filename}: {e}")
        
        logger.info("Completed AWS upload")
        
    except Exception as e:
        print("ENCOUNTERED AN ERROR:")
        print(e)
        logger.error(f"Error uploading to AWS: {e}")
    
    # Write to Database
    # print(f"{datetime.now()} Write to Database [29 of 29 (sigops 15min)]")
    
    # try:
    #     from write_sigops_to_db import append_to_database
    #     from database_functions import get_aurora_connection
        
    #     # Update Aurora Nightly
    #     aurora_conn = keep_trying(func=get_aurora_connection, n_tries=5)
        
    #     try:
    #         append_to_database(
    #             aurora_conn, 
    #             sig, 
    #             "sig", 
    #             calcs_start_date, 
    #             report_start_date=report_start_date, 
    #             report_end_date=None
    #         )
    #         logger.info("Successfully wrote data to Aurora database")
            
    #     except Exception as e:
    #         logger.error(f"Error writing to database: {e}")
            
    #     finally:
    #         if aurora_conn:
    #             logger.info("Closed Aurora database connection")
        
    # except Exception as e:
    #     print("ENCOUNTERED AN ERROR:")
    #     print(e)
    #     logger.error(f"Error in database operations: {e}")
    
    print(f"{datetime.now()} Completed 15min Package Processing")

def keep_trying(func, n_tries=3, **kwargs):
    """
    Keep trying a function until it succeeds or max tries reached
    
    Args:
        func: Function to call
        n_tries: Maximum number of attempts
        **kwargs: Arguments to pass to function
    
    Returns:
        Result of function call
    """
    for attempt in range(n_tries):
        try:
            return func(**kwargs)
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == n_tries - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff

def load_config():
    """Load configuration from YAML file with AWS credentials"""
    try:
        from SigOps.config_loader import load_merged_config
        conf = load_merged_config()
        return conf
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        # Fallback to old method
        try:
            config_path = "Monthly_Report.yaml"
            with open(config_path, 'r') as file:
                conf = yaml.safe_load(file)
            return conf
        except:
            raise

def get_date_from_string(date_string: str, table_include_regex_pattern: str = "", 
                        exceptions: int = 0) -> date:
    """
    Get date from string with table pattern matching.
    Supports relative strings like '2 days ago', 'yesterday', etc.
    
    Args:
        date_string: Date string to parse
        table_include_regex_pattern: Regex pattern for table inclusion
        exceptions: Number of exceptions to allow
    
    Returns:
        Date object
    """
    try:
        if date_string and date_string.lower() != 'auto':
            # Use our custom relative-date parser
            parsed = parse_relative_date(date_string)
            return parsed if isinstance(parsed, date) else parsed.date()
        
        # Auto mode (fallback) → default to 7 days ago
        return date.today() - timedelta(days=7)

    except Exception as e:
        logger.error(f"Error parsing date from string '{date_string}': {e}")
        return date.today() - timedelta(days=7)

if __name__ == "__main__":
    # Setup logging
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_filename = os.path.join(log_dir, f'monthly_15min_{datetime.now().strftime("%Y-%m-%d")}.log')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Run main processing
    try:
        main()
        print(f"{datetime.now()} 15min Package completed successfully")
    except Exception as e:
        logger.error(f"Fatal error in main processing: {e}")
        print(f"{datetime.now()} 15min Package failed with error: {e}")
        sys.exit(1)


