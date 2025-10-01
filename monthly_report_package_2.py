#!/usr/bin/env python3
"""
Monthly Report Package 2 - Python conversion of Monthly_Report_Package_2.R
Packages data for FlexDashboard reporting
"""

import os
import sys
import pandas as pd
import numpy as np
import pickle
import boto3
import logging
from datetime import datetime
import traceback
import gc
from pathlib import Path
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DuckDBManager:
    """Manages DuckDB connections and operations for improved performance"""
    
    def __init__(self, memory_limit='4GB', threads=None):
        self.conn = duckdb.connect(':memory:')
        
        # Configure DuckDB for optimal performance
        self.conn.execute(f"SET memory_limit = '{memory_limit}'")
        if threads:
            self.conn.execute(f"SET threads = {threads}")
        self.conn.execute("SET enable_progress_bar = true")
        self.conn.execute("SET enable_optimizer = true")
        
        logger.info(f"DuckDB initialized with memory_limit={memory_limit}")
    
    def register_dataframe(self, df, table_name):
        """Register a pandas DataFrame as a DuckDB table"""
        if not df.empty:
            self.conn.register(table_name, df)
    
    def execute_query(self, query):
        """Execute a query and return results as pandas DataFrame"""
        return self.conn.execute(query).df()
    
    def close(self):
        self.conn.close()

# Initialize global DuckDB instance
db = DuckDBManager()

# Replace the load_rds_data function
def load_rds_data(filename):
    """Load RDS file data with DuckDB optimization"""
    try:
        pkl_filename = Path(filename).with_suffix('.pkl')
        parquet_filename = Path(filename).with_suffix('.parquet')
        file_path = Path('data_output') / pkl_filename.name
        parquet_path = Path('data_output') / parquet_filename.name

        # Try to load from parquet first (fastest)
        if parquet_path.exists():
            query = f"SELECT * FROM read_parquet('{parquet_path}')"
            return db.execute_query(query)
        
        # Load from pickle and cache as parquet
        elif file_path.exists():
            with open(file_path, 'rb') as f:
                data = pickle.load(f)
                
            # Convert to parquet for faster future loads
            if isinstance(data, pd.DataFrame) and not data.empty:
                try:
                    data.to_parquet(parquet_path, index=False)
                    logger.debug(f"Cached {parquet_filename} for faster future loads")
                except Exception as e:
                    logger.warning(f"Could not cache parquet file: {e}")
                    
            return data
        else:
            logger.warning(f"File not found: {file_path.resolve()}")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return pd.DataFrame()

def get_quarterly(monthly_data, metric_col, weight_col=None, operation="sum"):
    """Convert monthly data to quarterly data"""
    try:
        if monthly_data.empty:
            return pd.DataFrame()
        
        # Ensure Month column is datetime
        if 'Month' in monthly_data.columns:
            monthly_data['Month'] = pd.to_datetime(monthly_data['Month'])
            monthly_data['Quarter'] = monthly_data['Month'].dt.to_period('Q')
        else:
            return pd.DataFrame()
        
        # Group by Quarter and aggregate
        group_cols = ['Zone_Group', 'Corridor', 'Quarter']
        group_cols = [col for col in group_cols if col in monthly_data.columns]
        
        if operation == "sum":
            quarterly = monthly_data.groupby(group_cols)[metric_col].sum().reset_index()
        elif operation == "latest":
            quarterly = monthly_data.sort_values('Month').groupby(group_cols)[metric_col].last().reset_index()
        else:  # mean
            if weight_col and weight_col in monthly_data.columns:
                quarterly = monthly_data.groupby(group_cols).apply(
                    lambda x: np.average(x[metric_col], weights=x[weight_col])
                ).reset_index()
            else:
                quarterly = monthly_data.groupby(group_cols)[metric_col].mean().reset_index()
        
        return quarterly
        
    except Exception as e:
        logger.error(f"Error in get_quarterly: {e}")
        return pd.DataFrame()

def get_quarterly_optimized(monthly_data, metric_col, weight_col=None, operation="sum"):
    """Convert monthly data to quarterly data using DuckDB for performance"""
    try:
        if monthly_data.empty:
            return pd.DataFrame()
        
        # Use DuckDB for large datasets
        if len(monthly_data) > 10000:
            table_name = f"monthly_data_{abs(hash(str(monthly_data.columns.tolist())))}"
            db.register_dataframe(monthly_data, table_name)
            
            group_cols = ['Zone_Group', 'Corridor']
            group_cols = [col for col in group_cols if col in monthly_data.columns]
            group_str = ', '.join(group_cols)
            
            if operation == "sum":
                query = f"""
                    SELECT {group_str}, 
                           DATE_TRUNC('quarter', Month) as Quarter,
                           SUM({metric_col}) as {metric_col}
                    FROM {table_name}
                    WHERE {metric_col} IS NOT NULL
                    GROUP BY {group_str}, DATE_TRUNC('quarter', Month)
                    ORDER BY Quarter
                """
            elif operation == "latest":
                query = f"""
                    SELECT {group_str}, 
                           DATE_TRUNC('quarter', Month) as Quarter,
                           LAST({metric_col} ORDER BY Month) as {metric_col}
                    FROM {table_name}
                    WHERE {metric_col} IS NOT NULL
                    GROUP BY {group_str}, DATE_TRUNC('quarter', Month)
                    ORDER BY Quarter
                """
            else:  # mean
                if weight_col and weight_col in monthly_data.columns:
                    query = f"""
                        SELECT {group_str}, 
                               DATE_TRUNC('quarter', Month) as Quarter,
                               SUM({metric_col} * {weight_col}) / SUM({weight_col}) as {metric_col}
                        FROM {table_name}
                        WHERE {metric_col} IS NOT NULL AND {weight_col} IS NOT NULL
                        GROUP BY {group_str}, DATE_TRUNC('quarter', Month)
                        ORDER BY Quarter
                    """
                else:
                    query = f"""
                        SELECT {group_str}, 
                               DATE_TRUNC('quarter', Month) as Quarter,
                               AVG({metric_col}) as {metric_col}
                        FROM {table_name}
                        WHERE {metric_col} IS NOT NULL
                        GROUP BY {group_str}, DATE_TRUNC('quarter', Month)
                        ORDER BY Quarter
                    """
            
            return db.execute_query(query)
        else:
            # Use original pandas method for smaller datasets
            return get_quarterly(monthly_data, metric_col, weight_col, operation)
        
    except Exception as e:
        logger.error(f"Error in get_quarterly_optimized: {e}")
        return get_quarterly(monthly_data, metric_col, weight_col, operation)

def get_corridor_summary_data(cor_data):
    """Generate corridor summary data"""
    try:
        summary_data = {}
        
        # Extract key metrics for summary
        if 'mo' in cor_data and 'vpd' in cor_data['mo']:
            vpd_data = cor_data['mo']['vpd']
            if not vpd_data.empty:
                summary_data['avg_vpd'] = vpd_data.groupby('Corridor')['vpd'].mean()
        
        if 'mo' in cor_data and 'du' in cor_data['mo']:
            uptime_data = cor_data['mo']['du']
            if not uptime_data.empty:
                summary_data['avg_uptime'] = uptime_data.groupby('Corridor')['uptime'].mean()
        
        return summary_data
        
    except Exception as e:
        logger.error(f"Error getting corridor summary data: {e}")
        return {}

def sigify(signal_data, corridor_data, config_data, identifier='SignalID'):
    """Convert signal-level data to corridor-level by joining with configuration"""
    try:
        if signal_data.empty or corridor_data.empty:
            return pd.DataFrame()
        
        # Merge signal data with corridor configuration
        if identifier in signal_data.columns:
            merged = signal_data.merge(
                config_data[[identifier, 'Zone_Group', 'Corridor', 'Description']], 
                on=identifier, 
                how='left'
            )
            return merged
        else:
            return signal_data
            
    except Exception as e:
        logger.error(f"Error in sigify: {e}")
        return pd.DataFrame()

def sigify_optimized(signal_data, corridor_data, config_data, identifier='SignalID'):
    """Convert signal-level data to corridor-level using DuckDB joins"""
    try:
        if signal_data.empty or config_data.empty:
            return pd.DataFrame()
        
        # Use DuckDB for large datasets
        if len(signal_data) > 5000:
            db.register_dataframe(signal_data, 'signal_data')
            db.register_dataframe(config_data, 'config_data')
            
            if identifier in signal_data.columns:
                query = f"""
                    SELECT s.*, c.Zone_Group, c.Corridor, c.Description
                    FROM signal_data s
                    LEFT JOIN config_data c ON s.{identifier} = c.{identifier}
                """
                return db.execute_query(query)
            else:
                return signal_data
        else:
            # Use original pandas method for smaller datasets
            return sigify(signal_data, corridor_data, config_data, identifier)
            
    except Exception as e:
        logger.error(f"Error in sigify_optimized: {e}")
        return sigify(signal_data, corridor_data, config_data, identifier)

def load_data_parallel(file_list, max_workers=4):
    """Load multiple RDS files in parallel for improved performance"""
    data_dict = {}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(load_rds_data, filename): filename 
            for filename in file_list
        }
        
        for future in as_completed(future_to_file):
            filename = future_to_file[future]
            try:
                data = future.result()
                key = Path(filename).stem
                data_dict[key] = data
            except Exception as e:
                logger.error(f"Failed to load {filename}: {e}")
                data_dict[Path(filename).stem] = pd.DataFrame()
    
    return data_dict

def main():
    """Main processing function with DuckDB optimizations"""
    
    print(f"{datetime.now()} Package for Monthly Report [27 of 29 (mark1)] - DuckDB Optimized")
    
    try:
        # Initialize data structures
        cor = {'dy': {}, 'wk': {}, 'mo': {}, 'qu': {}}
        sub = {'dy': {}, 'wk': {}, 'mo': {}, 'qu': {}}
        sig = {'dy': {}, 'wk': {}, 'mo': {}, 'qu': {}}
        
        # Load configuration data in parallel
        config_files = ["corridors.rds", "cam_config.rds"]
        config_data = load_data_parallel(config_files)
        corridors = config_data.get('corridors', pd.DataFrame())
        cam_config = config_data.get('cam_config', pd.DataFrame())
        
        print("Building corridor (cor) data structure with DuckDB optimization...")
        
        # Load daily data in parallel
        daily_files = [
            "cor_avg_daily_detector_uptime.rds",
            "cor_daily_comm_uptime.rds", 
            "cor_daily_pa_uptime.rds",
            "cor_daily_cctv_uptime.rds"
        ]
        daily_data = load_data_parallel(daily_files)
        
        cor['dy']['du'] = daily_data.get('cor_avg_daily_detector_uptime', pd.DataFrame())
        cor['dy']['cu'] = daily_data.get('cor_daily_comm_uptime', pd.DataFrame())
        cor['dy']['pau'] = daily_data.get('cor_daily_pa_uptime', pd.DataFrame())
        cor['dy']['cctv'] = daily_data.get('cor_daily_cctv_uptime', pd.DataFrame())

        # Load weekly data in parallel
        weekly_files = [
            "cor_weekly_vpd.rds",
            "cor_weekly_papd.rds",
            "cor_weekly_pd_by_day.rds",
            "cor_weekly_throughput.rds",
            "cor_weekly_detector_uptime.rds",
            "cor_weekly_comm_uptime.rds",
            "cor_weekly_pa_uptime.rds",
            "cor_weekly_cctv_uptime.rds"
        ]
        weekly_data = load_data_parallel(weekly_files)
        
        cor['wk']['vpd'] = weekly_data.get('cor_weekly_vpd', pd.DataFrame())
        cor['wk']['papd'] = weekly_data.get('cor_weekly_papd', pd.DataFrame())
        cor['wk']['pd'] = weekly_data.get('cor_weekly_pd_by_day', pd.DataFrame())
        cor['wk']['tp'] = weekly_data.get('cor_weekly_throughput', pd.DataFrame())
        cor['wk']['du'] = weekly_data.get('cor_weekly_detector_uptime', pd.DataFrame())
        cor['wk']['cu'] = weekly_data.get('cor_weekly_comm_uptime', pd.DataFrame())
        cor['wk']['pau'] = weekly_data.get('cor_weekly_pa_uptime', pd.DataFrame())
        cor['wk']['cctv'] = weekly_data.get('cor_weekly_cctv_uptime', pd.DataFrame())
        
        # ======= COR MONTHLY DATA =======
        cor['mo']['vpd'] = load_rds_data("cor_monthly_vpd.rds")
        
        # Monthly peak hour data
        monthly_vph_peak = load_rds_data("cor_monthly_vph_peak.rds")
        if isinstance(monthly_vph_peak, dict):
            cor['mo']['vphpa'] = monthly_vph_peak.get('am', pd.DataFrame())
            cor['mo']['vphpp'] = monthly_vph_peak.get('pm', pd.DataFrame())
        
        cor['mo']['papd'] = load_rds_data("cor_monthly_papd.rds")
        cor['mo']['pd'] = load_rds_data("cor_monthly_pd_by_day.rds")
        cor['mo']['tp'] = load_rds_data("cor_monthly_throughput.rds")
        cor['mo']['aogd'] = load_rds_data("cor_monthly_aog_by_day.rds")
        cor['mo']['aogh'] = load_rds_data("cor_monthly_aog_by_hr.rds")
        cor['mo']['prd'] = load_rds_data("cor_monthly_pr_by_day.rds")
        cor['mo']['prh'] = load_rds_data("cor_monthly_pr_by_hr.rds")
        cor['mo']['qsd'] = load_rds_data("cor_monthly_qsd.rds")
        cor['mo']['qsh'] = load_rds_data("cor_mqsh.rds")
        cor['mo']['sfd'] = load_rds_data("cor_monthly_sfd.rds")
        cor['mo']['sfh'] = load_rds_data("cor_msfh.rds")
        cor['mo']['sfo'] = load_rds_data("cor_monthly_sfo.rds")
        cor['mo']['tti'] = load_rds_data("cor_monthly_tti.rds")
        cor['mo']['ttih'] = load_rds_data("cor_monthly_tti_by_hr.rds")
        cor['mo']['pti'] = load_rds_data("cor_monthly_pti.rds")
        cor['mo']['ptih'] = load_rds_data("cor_monthly_pti_by_hr.rds")
        cor['mo']['bi'] = load_rds_data("cor_monthly_bi.rds")
        cor['mo']['bih'] = load_rds_data("cor_monthly_bi_by_hr.rds")
        cor['mo']['spd'] = load_rds_data("cor_monthly_spd.rds")
        cor['mo']['spdh'] = load_rds_data("cor_monthly_spd_by_hr.rds")
        cor['mo']['du'] = load_rds_data("cor_monthly_detector_uptime.rds")
        cor['mo']['cu'] = load_rds_data("cor_monthly_comm_uptime.rds")
        cor['mo']['pau'] = load_rds_data("cor_monthly_pa_uptime.rds")
        cor['mo']['cctv'] = load_rds_data("cor_monthly_cctv_uptime.rds")
        
        # Monthly task data
        tasks_all_monthly = load_rds_data("tasks_all.rds")
        if isinstance(tasks_all_monthly, dict) and 'cor_monthly' in tasks_all_monthly:
            tasks_monthly = tasks_all_monthly['cor_monthly']
            
            cor['mo']['ttyp'] = load_rds_data("tasks_by_type.rds").get('cor_monthly', pd.DataFrame())
            cor['mo']['tsub'] = load_rds_data("tasks_by_subtype.rds").get('cor_monthly', pd.DataFrame())
            cor['mo']['tpri'] = load_rds_data("tasks_by_priority.rds").get('cor_monthly', pd.DataFrame())
            cor['mo']['tsou'] = load_rds_data("tasks_by_source.rds").get('cor_monthly', pd.DataFrame())
            cor['mo']['tasks'] = tasks_monthly
            
            # Extract specific metrics with deltas
            if not tasks_monthly.empty:
                cor['mo']['reported'] = tasks_monthly[['Zone_Group', 'Corridor', 'Month', 'Reported']].copy()
                if 'delta.rep' in tasks_monthly.columns:
                    cor['mo']['reported']['delta'] = tasks_monthly['delta.rep']
                else:
                    cor['mo']['reported']['delta'] = np.nan
                
                cor['mo']['resolved'] = tasks_monthly[['Zone_Group', 'Corridor', 'Month', 'Resolved']].copy()
                if 'delta.res' in tasks_monthly.columns:
                    cor['mo']['resolved']['delta'] = tasks_monthly['delta.res']
                else:
                    cor['mo']['resolved']['delta'] = np.nan
                
                cor['mo']['outstanding'] = tasks_monthly[['Zone_Group', 'Corridor', 'Month', 'Outstanding']].copy()
                if 'delta.out' in tasks_monthly.columns:
                    cor['mo']['outstanding']['delta'] = tasks_monthly['delta.out']
                else:
                    cor['mo']['outstanding']['delta'] = np.nan
        
        # Task performance metrics
        tasks_by_date = load_rds_data("cor_tasks_by_date.rds")
        if not tasks_by_date.empty:
            cor['mo']['over45'] = tasks_by_date[['Zone_Group', 'Corridor', 'Month', 'over45']].copy()
            if 'delta.over45' in tasks_by_date.columns:
                cor['mo']['over45']['delta'] = tasks_by_date['delta.over45']
            else:
                cor['mo']['over45']['delta'] = np.nan
                
            cor['mo']['mttr'] = tasks_by_date[['Zone_Group', 'Corridor', 'Month', 'mttr']].copy()
            if 'delta.mttr' in tasks_by_date.columns:
                cor['mo']['mttr']['delta'] = tasks_by_date['delta.mttr']
            else:
                cor['mo']['mttr']['delta'] = np.nan
        
        # Safety and economic indices
        cor['mo']['flash'] = load_rds_data("cor_monthly_flash.rds")
        cor['mo']['bpsi'] = load_rds_data("cor_monthly_bpsi.rds")
        cor['mo']['rsi'] = load_rds_data("cor_monthly_rsi.rds")
        cor['mo']['cri'] = load_rds_data("cor_monthly_crash_rate_index.rds")
        cor['mo']['kabco'] = load_rds_data("cor_monthly_kabco_index.rds")
        
        # User delay costs
        hourly_udc = load_rds_data("hourly_udc.rds")
        if not hourly_udc.empty:
            # Aggregate to daily
            daily_udc = hourly_udc.groupby(['Zone', 'Corridor', hourly_udc['Month'].dt.date])['delay_cost'].sum().reset_index()
            daily_udc.columns = ['Zone_Group', 'Corridor', 'Date', 'delay_cost']
            cor['dy']['udc'] = daily_udc
            
            # Monthly aggregation  
            monthly_udc = hourly_udc.groupby(['Zone', 'Corridor', 'Month'])['delay_cost'].sum().reset_index()
            monthly_udc.columns = ['Zone_Group', 'Corridor', 'Month', 'delay_cost']
            cor['mo']['udc'] = monthly_udc
        
        # Hourly data
        cor['mo']['vphh'] = load_rds_data("cor_monthly_vph.rds")
        cor['mo']['paph'] = load_rds_data("cor_monthly_paph.rds")
        
        # ======= COR QUARTERLY DATA =======
        print("Generating quarterly corridor data...")
        
        # Generate quarterly data from monthly data
        cor['qu']['vpd'] = get_quarterly(cor['mo']['vpd'], 'vpd', operation="sum")
        cor['qu']['vphpa'] = get_quarterly(cor['mo']['vphpa'], 'vph', operation="sum")
        cor['qu']['vphpp'] = get_quarterly(cor['mo']['vphpp'], 'vph', operation="sum")
        cor['qu']['papd'] = get_quarterly(cor['mo']['papd'], 'papd', operation="sum")
        cor['qu']['pd'] = get_quarterly(cor['mo']['pd'], 'pd', operation="mean")
        cor['qu']['tp'] = get_quarterly(cor['mo']['tp'], 'throughput', operation="mean")
        cor['qu']['aogd'] = get_quarterly(cor['mo']['aogd'], 'aog', operation="mean")
        cor['qu']['prd'] = get_quarterly(cor['mo']['prd'], 'pr', operation="mean")
        cor['qu']['qsd'] = get_quarterly(cor['mo']['qsd'], 'qs_freq', operation="mean")
        cor['qu']['sfd'] = get_quarterly(cor['mo']['sfd'], 'sf_freq', operation="mean")
        cor['qu']['sfo'] = get_quarterly(cor['mo']['sfo'], 'sf_freq', operation="mean")
        cor['qu']['tti'] = get_quarterly(cor['mo']['tti'], 'tti', operation="mean")
        cor['qu']['pti'] = get_quarterly(cor['mo']['pti'], 'pti', operation="mean")
        cor['qu']['bi'] = get_quarterly(cor['mo']['bi'], 'bi', operation="mean")
        cor['qu']['spd'] = get_quarterly(cor['mo']['spd'], 'speed_mph', operation="mean")
        cor['qu']['du'] = get_quarterly(cor['mo']['du'], 'uptime', operation="mean")
        cor['qu']['cu'] = get_quarterly(cor['mo']['cu'], 'uptime', operation="mean")
        cor['qu']['pau'] = get_quarterly(cor['mo']['pau'], 'uptime', operation="mean")
        cor['qu']['cctv'] = get_quarterly(cor['mo']['cctv'], 'uptime', operation="mean")
        cor['qu']['flash'] = get_quarterly(cor['mo']['flash'], 'flash', operation="sum")
        cor['qu']['bpsi'] = get_quarterly(cor['mo']['bpsi'], 'bpsi', operation="latest")
        cor['qu']['rsi'] = get_quarterly(cor['mo']['rsi'], 'rsi', operation="latest")
        cor['qu']['cri'] = get_quarterly(cor['mo']['cri'], 'cri', operation="latest")
        cor['qu']['kabco'] = get_quarterly(cor['mo']['kabco'], 'kabco', operation="latest")
        
        # ======= SUBCORRIDOR (SUB) DATA STRUCTURE =======
        print("Building subcorridor (sub) data structure...")
        
        # ======= SUB DAILY DATA =======
        sub['dy']['du'] = load_rds_data("sub_avg_daily_detector_uptime.rds")
        sub['dy']['cu'] = load_rds_data("sub_daily_comm_uptime.rds")
        sub['dy']['pau'] = load_rds_data("sub_daily_pa_uptime.rds")
        sub['dy']['cctv'] = load_rds_data("sub_daily_cctv_uptime.rds")
        
        # ======= SUB WEEKLY DATA =======
        sub['wk']['vpd'] = load_rds_data("sub_weekly_vpd.rds")
        
        # Weekly peak hour data
        sub_weekly_vph_peak = load_rds_data("sub_weekly_vph_peak.rds")
        if isinstance(sub_weekly_vph_peak, dict):
            sub['wk']['vphpa'] = sub_weekly_vph_peak.get('am', pd.DataFrame())
            sub['wk']['vphpp'] = sub_weekly_vph_peak.get('pm', pd.DataFrame())
        
        sub['wk']['papd'] = load_rds_data("sub_weekly_papd.rds")
        sub['wk']['pd'] = load_rds_data("sub_weekly_pd_by_day.rds")
        sub['wk']['tp'] = load_rds_data("sub_weekly_throughput.rds")
        sub['wk']['aogd'] = load_rds_data("sub_weekly_aog_by_day.rds")
        sub['wk']['prd'] = load_rds_data("sub_weekly_pr_by_day.rds")
        sub['wk']['qsd'] = load_rds_data("sub_wqs.rds")
        sub['wk']['sfd'] = load_rds_data("sub_wsf.rds")
        sub['wk']['sfo'] = load_rds_data("sub_wsfo.rds")
        sub['wk']['du'] = load_rds_data("sub_weekly_detector_uptime.rds")
        sub['wk']['cu'] = load_rds_data("sub_weekly_comm_uptime.rds")
        sub['wk']['pau'] = load_rds_data("sub_weekly_pa_uptime.rds")
        sub['wk']['cctv'] = load_rds_data("sub_weekly_cctv_uptime.rds")
        
        # ======= SUB MONTHLY DATA =======
        sub['mo']['vpd'] = load_rds_data("sub_monthly_vpd.rds")
        
        # Monthly peak hour data
        sub_monthly_vph_peak = load_rds_data("sub_monthly_vph_peak.rds")
        if isinstance(sub_monthly_vph_peak, dict):
            sub['mo']['vphpa'] = sub_monthly_vph_peak.get('am', pd.DataFrame())
            sub['mo']['vphpp'] = sub_monthly_vph_peak.get('pm', pd.DataFrame())
        
        sub['mo']['papd'] = load_rds_data("sub_monthly_papd.rds")
        sub['mo']['pd'] = load_rds_data("sub_monthly_pd_by_day.rds")
        sub['mo']['tp'] = load_rds_data("sub_monthly_throughput.rds")
        sub['mo']['aogd'] = load_rds_data("sub_monthly_aog_by_day.rds")
        sub['mo']['aogh'] = load_rds_data("sub_monthly_aog_by_hr.rds")
        sub['mo']['prd'] = load_rds_data("sub_monthly_pr_by_day.rds")
        sub['mo']['prh'] = load_rds_data("sub_monthly_pr_by_hr.rds")
        sub['mo']['qsd'] = load_rds_data("sub_monthly_qsd.rds")
        sub['mo']['qsh'] = load_rds_data("sub_mqsh.rds")
        sub['mo']['sfd'] = load_rds_data("sub_monthly_sfd.rds")
        sub['mo']['sfh'] = load_rds_data("sub_msfh.rds")
        sub['mo']['sfo'] = load_rds_data("sub_monthly_sfo.rds")
        sub['mo']['tti'] = load_rds_data("sub_monthly_tti.rds")
        sub['mo']['ttih'] = load_rds_data("sub_monthly_tti_by_hr.rds")
        sub['mo']['pti'] = load_rds_data("sub_monthly_pti.rds")
        sub['mo']['ptih'] = load_rds_data("sub_monthly_pti_by_hr.rds")
        sub['mo']['bi'] = load_rds_data("sub_monthly_bi.rds")
        sub['mo']['bih'] = load_rds_data("sub_monthly_bi_by_hr.rds")
        sub['mo']['spd'] = load_rds_data("sub_monthly_spd.rds")
        sub['mo']['spdh'] = load_rds_data("sub_monthly_spd_by_hr.rds")
        sub['mo']['du'] = load_rds_data("sub_monthly_detector_uptime.rds")
        sub['mo']['cu'] = load_rds_data("sub_monthly_comm_uptime.rds")
        sub['mo']['pau'] = load_rds_data("sub_monthly_pa_uptime.rds")
        sub['mo']['cctv'] = load_rds_data("sub_monthly_cctv_uptime.rds")
        sub['mo']['flash'] = load_rds_data("sub_monthly_flash.rds")
        sub['mo']['bpsi'] = load_rds_data("sub_monthly_bpsi.rds")
        sub['mo']['rsi'] = load_rds_data("sub_monthly_rsi.rds")
        sub['mo']['cri'] = load_rds_data("sub_monthly_crash_rate_index.rds")
        sub['mo']['kabco'] = load_rds_data("sub_monthly_kabco_index.rds")
        
        # Hourly subcorridor data
        sub['mo']['vphh'] = load_rds_data("sub_monthly_vph.rds")
        sub['mo']['paph'] = load_rds_data("sub_monthly_paph.rds")
        
        # ======= SUB QUARTERLY DATA =======
        print("Generating quarterly subcorridor data...")
        
        # Generate quarterly data from monthly data
        sub['qu']['vpd'] = get_quarterly(sub['mo']['vpd'], 'vpd', operation="sum")
        sub['qu']['vphpa'] = get_quarterly(sub['mo']['vphpa'], 'vph', operation="sum")
        sub['qu']['vphpp'] = get_quarterly(sub['mo']['vphpp'], 'vph', operation="sum")
        sub['qu']['papd'] = get_quarterly(sub['mo']['papd'], 'papd', operation="sum")
        sub['qu']['pd'] = get_quarterly(sub['mo']['pd'], 'pd', operation="mean")
        sub['qu']['tp'] = get_quarterly(sub['mo']['tp'], 'throughput', operation="mean")
        sub['qu']['aogd'] = get_quarterly(sub['mo']['aogd'], 'aog', operation="mean")
        sub['qu']['prd'] = get_quarterly(sub['mo']['prd'], 'pr', operation="mean")
        sub['qu']['qsd'] = get_quarterly(sub['mo']['qsd'], 'qs_freq', operation="mean")
        sub['qu']['sfd'] = get_quarterly(sub['mo']['sfd'], 'sf_freq', operation="mean")
        sub['qu']['sfo'] = get_quarterly(sub['mo']['sfo'], 'sf_freq', operation="mean")
        sub['qu']['tti'] = get_quarterly(sub['mo']['tti'], 'tti', operation="mean")
        sub['qu']['pti'] = get_quarterly(sub['mo']['pti'], 'pti', operation="mean")
        sub['qu']['bi'] = get_quarterly(sub['mo']['bi'], 'bi', operation="mean")
        sub['qu']['spd'] = get_quarterly(sub['mo']['spd'], 'speed_mph', operation="mean")
        sub['qu']['du'] = get_quarterly(sub['mo']['du'], 'uptime', operation="mean")
        sub['qu']['cu'] = get_quarterly(sub['mo']['cu'], 'uptime', operation="mean")
        sub['qu']['pau'] = get_quarterly(sub['mo']['pau'], 'uptime', operation="mean")
        sub['qu']['cctv'] = get_quarterly(sub['mo']['cctv'], 'uptime', operation="mean")
        sub['qu']['flash'] = get_quarterly(sub['mo']['flash'], 'flash', operation="sum")
        sub['qu']['bpsi'] = get_quarterly(sub['mo']['bpsi'], 'bpsi', operation="latest")
        sub['qu']['rsi'] = get_quarterly(sub['mo']['rsi'], 'rsi', operation="latest")
        sub['qu']['cri'] = get_quarterly(sub['mo']['cri'], 'cri', operation="latest")
        sub['qu']['kabco'] = get_quarterly(sub['mo']['kabco'], 'kabco', operation="latest")
        
        # ======= SIGNAL (SIG) DATA STRUCTURE =======
        print("Building signal (sig) data structure...")
        
        # Load signal-level data
        sig_tasks_by_date = load_rds_data("sig_tasks_by_date.rds")
        
        # ======= SIG MONTHLY DATA =======
        sig['mo']['over45'] = sig_tasks_by_date[['Zone_Group', 'Month', 'over45']].copy() if not sig_tasks_by_date.empty else pd.DataFrame()
        if not sig_tasks_by_date.empty and 'delta.over45' in sig_tasks_by_date.columns:
            sig['mo']['over45']['delta'] = sig_tasks_by_date['delta.over45']
        elif not sig['mo']['over45'].empty:
            sig['mo']['over45']['delta'] = np
        if not sig_tasks_by_date.empty and 'delta.over45' in sig_tasks_by_date.columns:
            sig['mo']['over45']['delta'] = sig_tasks_by_date['delta.over45']
        elif not sig['mo']['over45'].empty:
            sig['mo']['over45']['delta'] = np.nan
            
        sig['mo']['mttr'] = sig_tasks_by_date[['Zone_Group', 'Month', 'mttr']].copy() if not sig_tasks_by_date.empty else pd.DataFrame()
        if not sig_tasks_by_date.empty and 'delta.mttr' in sig_tasks_by_date.columns:
            sig['mo']['mttr']['delta'] = sig_tasks_by_date['delta.mttr']
        elif not sig['mo']['mttr'].empty:
            sig['mo']['mttr']['delta'] = np.nan
        
        # Load signal-level aggregated data
        sig['mo']['vpd'] = load_rds_data("monthly_vpd.rds")
        sig['mo']['du'] = load_rds_data("monthly_detector_uptime.rds")
        sig['mo']['cu'] = load_rds_data("monthly_comm_uptime.rds")
        sig['mo']['pau'] = load_rds_data("monthly_pa_uptime.rds")
        sig['mo']['cri'] = load_rds_data("monthly_crash_rate_index.rds")
        sig['mo']['kabco'] = load_rds_data("monthly_kabco_index.rds")
        sig['mo']['flash'] = load_rds_data("monthly_flash.rds")
        
        # Convert signal data to include corridor information using sigify
        if not corridors.empty:
            for key in ['vpd', 'du', 'cu', 'pau', 'cri', 'kabco', 'flash']:
                if not sig['mo'][key].empty:
                    sig['mo'][key] = sigify(sig['mo'][key], corridors, corridors, 'SignalID')
        
        # ======= HEALTH METRICS =======
        print("Calculating health metrics...")
        
        # Calculate health score for corridors
        health_data = calculate_health_metrics(cor, sub, sig)
        
        # ======= PACKAGE DATA FOR EXPORT =======
        print("Packaging data for export...")
        
        # Create package structure
        package = {
            'cor': cor,
            'sub': sub, 
            'sig': sig,
            'health': health_data,
            'config': {
                'corridors': corridors,
                'cam_config': cam_config
            },
            'metadata': {
                'generated_date': datetime.now(),
                'data_period': f"{cor['mo']['vpd']['Month'].min() if not cor['mo']['vpd'].empty else 'N/A'} to {cor['mo']['vpd']['Month'].max() if not cor['mo']['vpd'].empty else 'N/A'}"
            }
        }
        
        # Save packaged data
        print("Saving packaged data...")
        with open('data_output/monthly_report_package.pkl', 'wb') as f:
            pickle.dump(package, f)
        
        # Upload to AWS S3 if configured
        try:
            upload_to_s3(package)
        except Exception as e:
            logger.warning(f"Could not upload to S3: {e}")
        
        # Update database tables
        try:
            update_database_tables(package)
        except Exception as e:
            logger.warning(f"Could not update database: {e}")
        
        print(f"{datetime.now()} Package for Monthly Report completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        logger.error(traceback.format_exc())
        raise
    
    finally:
        # Cleanup
        db.close()
        gc.collect()

def calculate_health_metrics(cor, sub, sig):
    """Calculate health metrics for corridors and subcorridors"""
    try:
        health_data = {}
        
        # Corridor health metrics
        if cor['mo']['du'] and not cor['mo']['du'].empty:
            # Calculate overall system health
            latest_month = cor['mo']['du']['Month'].max()
            latest_data = cor['mo']['du'][cor['mo']['du']['Month'] == latest_month]
            
            corridor_health = []
            for _, row in latest_data.iterrows():
                health_score = 0
                weight_sum = 0
                
                # Detector uptime (20% weight)
                if 'uptime' in row and pd.notna(row['uptime']):
                    health_score += row['uptime'] * 0.2
                    weight_sum += 0.2
                
                # Communication uptime (15% weight)
                comm_data = cor['mo']['cu']
                if not comm_data.empty:
                    comm_row = comm_data[
                        (comm_data['Corridor'] == row['Corridor']) & 
                        (comm_data['Month'] == latest_month)
                    ]
                    if not comm_row.empty and 'uptime' in comm_row.columns:
                        health_score += comm_row['uptime'].iloc[0] * 0.15
                        weight_sum += 0.15
                
                # Travel time index (15% weight) - lower is better
                tti_data = cor['mo']['tti']
                if not tti_data.empty:
                    tti_row = tti_data[
                        (tti_data['Corridor'] == row['Corridor']) & 
                        (tti_data['Month'] == latest_month)
                    ]
                    if not tti_row.empty and 'tti' in tti_row.columns:
                        # Convert TTI to health score (TTI of 1.0 = 100%, TTI of 2.0 = 0%)
                        tti_health = max(0, min(100, (2.0 - tti_row['tti'].iloc[0]) * 100))
                        health_score += tti_health * 0.15
                        weight_sum += 0.15
                
                # Progression ratio (10% weight)
                pr_data = cor['mo']['prd']
                if not pr_data.empty:
                    pr_row = pr_data[
                        (pr_data['Corridor'] == row['Corridor']) & 
                        (pr_data['Month'] == latest_month)
                    ]
                    if not pr_row.empty and 'pr' in pr_row.columns:
                        health_score += pr_row['pr'].iloc[0] * 100 * 0.10
                        weight_sum += 0.10
                
                # Calculate final health score
                if weight_sum > 0:
                    final_health = health_score / weight_sum
                else:
                    final_health = 0
                
                corridor_health.append({
                    'Zone_Group': row['Zone_Group'],
                    'Corridor': row['Corridor'],
                    'Month': latest_month,
                    'health_score': final_health,
                    'health_grade': get_health_grade(final_health)
                })
            
            health_data['corridor_health'] = pd.DataFrame(corridor_health)
        
        # System-wide health summary
        if 'corridor_health' in health_data:
            system_health = {
                'overall_health_score': health_data['corridor_health']['health_score'].mean(),
                'corridors_excellent': len(health_data['corridor_health'][health_data['corridor_health']['health_score'] >= 90]),
                'corridors_good': len(health_data['corridor_health'][(health_data['corridor_health']['health_score'] >= 70) & (health_data['corridor_health']['health_score'] < 90)]),
                'corridors_fair': len(health_data['corridor_health'][(health_data['corridor_health']['health_score'] >= 50) & (health_data['corridor_health']['health_score'] < 70)]),
                'corridors_poor': len(health_data['corridor_health'][health_data['corridor_health']['health_score'] < 50]),
                'total_corridors': len(health_data['corridor_health'])
            }
            health_data['system_summary'] = system_health
        
        return health_data
        
    except Exception as e:
        logger.error(f"Error calculating health metrics: {e}")
        return {}

def get_health_grade(score):
    """Convert health score to letter grade"""
    if score >= 90:
        return 'A'
    elif score >= 80:
        return 'B' 
    elif score >= 70:
        return 'C'
    elif score >= 60:
        return 'D'
    else:
        return 'F'

def upload_to_s3(package_data):
    """Upload packaged data to S3"""
    try:
        # Load AWS configuration
        import yaml
        with open("Monthly_Report_AWS.yaml", 'r') as file:
            aws_conf = yaml.safe_load(file)
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_conf['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=aws_conf['AWS_SECRET_ACCESS_KEY'],
            region_name=aws_conf['AWS_DEFAULT_REGION']
        )
        
        # Upload main package
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"monthly_report_packages/monthly_report_package_{timestamp}.pkl"
        
        # Save to temporary file for upload
        temp_file = f"/tmp/monthly_report_package_{timestamp}.pkl"
        with open(temp_file, 'wb') as f:
            pickle.dump(package_data, f)
        
        # Upload to S3
        bucket_name = aws_conf.get('bucket', 'atspm-data')
        s3_client.upload_file(temp_file, bucket_name, s3_key)
        
        # Upload individual components
        for component_name, component_data in package_data.items():
            if component_name in ['cor', 'sub', 'sig']:
                component_key = f"monthly_report_packages/{component_name}_{timestamp}.pkl"
                component_file = f"/tmp/{component_name}_{timestamp}.pkl"
                
                with open(component_file, 'wb') as f:
                    pickle.dump(component_data, f)
                
                s3_client.upload_file(component_file, bucket_name, component_key)
                
                # Clean up temp file
                os.remove(component_file)
        
        # Clean up main temp file
        os.remove(temp_file)
        
        logger.info(f"Successfully uploaded package to S3: {s3_key}")
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise

def update_database_tables(package_data):
    """Update database tables with summary metrics"""
    try:
        # This would connect to your database and update summary tables
        # Implementation depends on your specific database setup
        
        logger.info("Database update functionality not implemented - skipping")
        
        # Example structure for database updates:
        # 1. Update corridor_performance_summary table
        # 2. Update system_health_metrics table  
        # 3. Update monthly_report_status table
        
    except Exception as e:
        logger.error(f"Error updating database: {e}")
        raise

def validate_package_completeness(package_data):
    """Validate that the package contains expected data"""
    try:
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check main structure
        required_keys = ['cor', 'sub', 'sig', 'health', 'config', 'metadata']
        for key in required_keys:
            if key not in package_data:
                validation_results['errors'].append(f"Missing required section: {key}")
                validation_results['valid'] = False
        
        # Check corridor data completeness
        if 'cor' in package_data:
            cor_data = package_data['cor']
            for time_period in ['dy', 'wk', 'mo', 'qu']:
                if time_period not in cor_data:
                    validation_results['warnings'].append(f"Missing corridor {time_period} data")
                else:
                    # Check for key metrics
                    key_metrics = ['vpd', 'du', 'cu']
                    for metric in key_metrics:
                        if metric not in cor_data[time_period] or cor_data[time_period][metric].empty:
                            validation_results['warnings'].append(f"Missing or empty corridor {time_period}.{metric}")
        
        # Check subcorridor data
        if 'sub' in package_data:
            sub_data = package_data['sub']
            if not sub_data['mo']['vpd'].empty:
                validation_results['warnings'].append("Subcorridor data appears complete")
            else:
                validation_results['warnings'].append("Limited subcorridor data available")
        
        # Log validation results
        if validation_results['errors']:
            logger.error(f"Package validation errors: {validation_results['errors']}")
        if validation_results['warnings']:
            logger.warning(f"Package validation warnings: {validation_results['warnings']}")
        
        if validation_results['valid']:
            logger.info("Package validation completed successfully")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating package: {e}")
        return {'valid': False, 'errors': [str(e)], 'warnings': []}

def generate_package_summary(package_data):
    """Generate summary statistics for the package"""
    try:
        summary = {
            'generation_time': datetime.now().isoformat(),
            'data_size_mb': 0,
            'corridor_count': 0,
            'subcorridor_count': 0,
            'signal_count': 0,
            'time_periods': [],
            'metrics_included': []
        }
        
        # Calculate data size (approximate)
        import sys
        summary['data_size_mb'] = sys.getsizeof(pickle.dumps(package_data)) / (1024 * 1024)
        
        # Count entities
        if 'cor' in package_data and 'mo' in package_data['cor'] and not package_data['cor']['mo']['vpd'].empty:
            summary['corridor_count'] = package_data['cor']['mo']['vpd']['Corridor'].nunique()
        
        if 'sub' in package_data and 'mo' in package_data['sub'] and not package_data['sub']['mo']['vpd'].empty:
            summary['subcorridor_count'] = package_data['sub']['mo']['vpd']['Corridor'].nunique()
        
        if 'sig' in package_data and 'mo' in package_data['sig'] and not package_data['sig']['mo']['vpd'].empty:
            summary['signal_count'] = package_data['sig']['mo']['vpd']['SignalID'].nunique()
        
        # List time periods
        if 'cor' in package_data:
            summary['time_periods'] = list(package_data['cor'].keys())
        
        # List metrics
        if 'cor' in package_data and 'mo' in package_data['cor']:
            summary['metrics_included'] = list(package_data['cor']['mo'].keys())
        
        # Save summary
        with open('data_output/package_summary.json', 'w') as f:
            import json
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Package summary: {summary['corridor_count']} corridors, "
                   f"{summary['subcorridor_count']} subcorridors, "
                   f"{summary['signal_count']} signals, "
                   f"{summary['data_size_mb']:.1f} MB")
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating package summary: {e}")
        return {}

def cleanup_temporary_files():
    """Clean up temporary files and free memory"""
    try:
        # Clean up temporary files
        temp_dir = "/tmp"
        if os.path.exists(temp_dir):
            temp_files = [f for f in os.listdir(temp_dir) if f.startswith('monthly_report_')]
            for temp_file in temp_files:
                try:
                    os.remove(os.path.join(temp_dir, temp_file))
                except:
                    pass  # Ignore cleanup errors
        
        # Force garbage collection
        gc.collect()
        
        logger.info("Temporary file cleanup completed")
        
    except Exception as e:
        logger.warning(f"Error during cleanup: {e}")

def create_data_quality_report(package_data):
    """Create a data quality assessment report"""
    try:
        quality_report = {
            'assessment_date': datetime.now().isoformat(),
            'overall_score': 0,
            'corridor_quality': {},
            'metric_completeness': {},
            'data_issues': []
        }
        
        total_score = 0
        metric_count = 0
        
        # Assess corridor data quality
        if 'cor' in package_data and 'mo' in package_data['cor']:
            cor_monthly = package_data['cor']['mo']
            
            for metric_name, metric_data in cor_monthly.items():
                if isinstance(metric_data, pd.DataFrame) and not metric_data.empty:
                    # Calculate completeness score
                    total_records = len(metric_data)
                    non_null_records = metric_data.count().sum()
                    total_possible = total_records * len(metric_data.columns)
                    
                    if total_possible > 0:
                        completeness_score = (non_null_records / total_possible) * 100
                        quality_report['metric_completeness'][metric_name] = completeness_score
                        total_score += completeness_score
                        metric_count += 1
                        
                        # Identify data issues
                        if completeness_score < 80:
                            quality_report['data_issues'].append(
                                f"Low completeness for {metric_name}: {completeness_score:.1f}%"
                            )
                else:
                    quality_report['metric_completeness'][metric_name] = 0
                    quality_report['data_issues'].append(f"No data available for {metric_name}")
        
        # Calculate overall score
        if metric_count > 0:
            quality_report['overall_score'] = total_score / metric_count
        
        # Assess individual corridor quality
        if 'health' in package_data and 'corridor_health' in package_data['health']:
            corridor_health = package_data['health']['corridor_health']
            for _, corridor in corridor_health.iterrows():
                quality_report['corridor_quality'][corridor['Corridor']] = {
                    'health_score': corridor['health_score'],
                    'health_grade': corridor['health_grade']
                }
        
        # Save quality report
        with open('data_output/data_quality_report.json', 'w') as f:
            import json
            json.dump(quality_report, f, indent=2, default=str)
        
        logger.info(f"Data quality assessment completed. Overall score: {quality_report['overall_score']:.1f}%")
        
        return quality_report
        
    except Exception as e:
        logger.error(f"Error creating data quality report: {e}")
        return {}

def export_summary_tables(package_data):
    """Export key summary tables to CSV for external use"""
    try:
        output_dir = 'data_output/summary_tables'
        os.makedirs(output_dir, exist_ok=True)
        
        # Export corridor monthly summaries
        if 'cor' in package_data and 'mo' in package_data['cor']:
            cor_monthly = package_data['cor']['mo']
            
            # Key performance indicators
            key_metrics = ['vpd', 'du', 'cu', 'tti', 'pti', 'aogd', 'prd']
            
            for metric in key_metrics:
                if metric in cor_monthly and not cor_monthly[metric].empty:
                    filename = f"{output_dir}/corridor_monthly_{metric}.csv"
                    cor_monthly[metric].to_csv(filename, index=False)
                    logger.debug(f"Exported {filename}")
        
        # Export health metrics
        if 'health' in package_data:
            health_data = package_data['health']
            
            if 'corridor_health' in health_data:
                health_data['corridor_health'].to_csv(
                    f"{output_dir}/corridor_health_scores.csv", index=False
                )
            
            if 'system_summary' in health_data:
                system_df = pd.DataFrame([health_data['system_summary']])
                system_df.to_csv(f"{output_dir}/system_health_summary.csv", index=False)
        
        # Export signal-level data
        if 'sig' in package_data and 'mo' in package_data['sig']:
            sig_monthly = package_data['sig']['mo']
            
            for metric in ['vpd', 'du', 'cu']:
                if metric in sig_monthly and not sig_monthly[metric].empty:
                    filename = f"{output_dir}/signal_monthly_{metric}.csv"
                    sig_monthly[metric].to_csv(filename, index=False)
                    logger.debug(f"Exported {filename}")
        
        logger.info(f"Summary tables exported to {output_dir}")
        
    except Exception as e:
        logger.error(f"Error exporting summary tables: {e}")

def create_processing_log(package_data):
    """Create a detailed processing log"""
    try:
        log_data = {
            'processing_start': datetime.now().isoformat(),
            'package_version': '2.0',
            'python_version': sys.version,
            'data_sources': [],
            'processing_steps': [],
            'performance_metrics': {},
            'validation_results': {},
            'export_locations': []
        }
        
        # Document data sources
        data_files = [f for f in os.listdir('.') if f.endswith('.pkl')]
        log_data['data_sources'] = data_files
        
        # Add validation results
        log_data['validation_results'] = validate_package_completeness(package_data)
        
        # Add performance metrics
        log_data['performance_metrics'] = {
            'package_size_mb': sys.getsizeof(pickle.dumps(package_data)) / (1024 * 1024),
            'corridor_count': len(package_data.get('cor', {}).get('mo', {}).get('vpd', pd.DataFrame())),
            'processing_time': 'TBD'  # Would be calculated in actual implementation
        }
        
        # Save processing log
        with open('data_output/processing_log.json', 'w') as f:
            import json
            json.dump(log_data, f, indent=2, default=str)
        
        logger.info("Processing log created")
        
        return log_data
        
    except Exception as e:
        logger.error(f"Error creating processing log: {e}")
        return {}

def process_final_performance_summary(dates, config_data):
    """Process final performance summary [29 of 29]"""
    logger.info(f"{datetime.now()} Final Performance Summary [29 of 29]")
    
    try:
        # This function is called from monthly_report_package_1.py
        # Load all processed data and create final summary
        
        summary_data = {
            'report_period': {
                'start_date': dates['calcs_start_date'],
                'end_date': dates['report_end_date']
            },
            'system_overview': {},
            'top_performers': {},
            'areas_for_improvement': {},
            'monthly_trends': {}
        }
        
        # Load key metrics for summary
        monthly_vpd = load_rds_data("cor_monthly_vpd.rds")
        monthly_uptime = load_rds_data("cor_monthly_detector_uptime.rds")
        monthly_tti = load_rds_data("cor_monthly_tti.rds")
        
        if not monthly_vpd.empty:
            latest_month = monthly_vpd['Month'].max()
            summary_data['system_overview']['total_monthly_volume'] = monthly_vpd[
                monthly_vpd['Month'] == latest_month
            ]['vpd'].sum()
        
        if not monthly_uptime.empty:
            latest_month = monthly_uptime['Month'].max()
            summary_data['system_overview']['average_uptime'] = monthly_uptime[
                monthly_uptime['Month'] == latest_month
            ]['uptime'].mean()
        
        if not monthly_tti.empty:
            latest_month = monthly_tti['Month'].max()
            summary_data['system_overview']['average_tti'] = monthly_tti[
                monthly_tti['Month'] == latest_month
            ]['tti'].mean()
        
        # Identify top performers
        if not monthly_uptime.empty:
            latest_data = monthly_uptime[monthly_uptime['Month'] == monthly_uptime['Month'].max()]
            top_uptime = latest_data.nlargest(5, 'uptime')
            summary_data['top_performers']['uptime'] = top_uptime[['Corridor', 'uptime']].to_dict('records')
        
        # Save final summary
        with open('data_output/final_performance_summary.pkl', 'wb') as f:
            pickle.dump(summary_data, f)
        
        logger.info("Final performance summary completed")
        
    except Exception as e:
        logger.error(f"Error in final performance summary: {e}")
        logger.error(traceback.format_exc())

def cleanup_duckdb():
    """Clean up DuckDB resources"""
    try:
        db.close()
        logger.info("DuckDB connection closed")
    except:
        pass

if __name__ == "__main__":
    """Main execution block"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('monthly_report_package_2.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        print("=" * 60)
        print("Monthly Report Package 2 - Data Packaging")
        print("=" * 60)
        
        start_time = datetime.now()
        
        # Run main processing
        main()
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = end_time - start_time
        
        print(f"\nProcessing completed in {processing_time}")
        print("=" * 60)
        
        # Generate final reports
        print("Generating final reports and summaries...")
        
        # Load the generated package
        if os.path.exists('data_output/monthly_report_package.pkl'):
            with open('data_output/monthly_report_package.pkl', 'rb') as f:
                package_data = pickle.load(f)
            
            # Generate additional reports
            generate_package_summary(package_data)
            create_data_quality_report(package_data)
            export_summary_tables(package_data)
            create_processing_log(package_data)
            
            print("All reports generated successfully!")
        else:
            logger.warning("Package file not found - skipping report generation")
        
        # Final cleanup
        cleanup_temporary_files()
        
        print("Monthly Report Package 2 completed successfully!")
        
    except KeyboardInterrupt:
        print("\nProcessing interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Fatal error in Monthly Report Package 2: {e}")
        logger.error(traceback.format_exc())
        print(f"\nProcessing failed with error: {e}")
        sys.exit(1)
        
    finally:
        # Ensure cleanup always runs
        try:
            cleanup_temporary_files()
            cleanup_duckdb()
        except:
            pass


