"""
Corridor Health Metrics Functions - Python conversion of Health_Metrics.R
Calculates health metrics for maintenance, operations, and safety
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime, date
import yaml
import boto3
from database_functions import query_data, get_aurora_connection
from utilities import format_duration
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
def load_health_config() -> Dict[str, Any]:
    """Load health metrics configuration from YAML"""
    try:
        with open("health_metrics.yaml", 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("health_metrics.yaml not found")
        return {}

health_conf = load_health_config()

# Convert YAML data to DataFrames
scoring_lookup = pd.DataFrame(health_conf.get('scoring_lookup', {}))
weights_lookup = pd.DataFrame(health_conf.get('weights_lookup', {}))

def get_summary_data(df: Dict[str, Dict[str, pd.DataFrame]], current_month: Optional[str] = None) -> pd.DataFrame:
    """
    Converts sub or cor data set to a single data frame for the current_month
    for use in get_subcorridor_summary_table function
    
    Args:
        df: Dictionary containing monthly data (cor or sub data structure)
        current_month: Current month in date format
        
    Returns:
        A data frame, monthly data of all metrics by Zone and Corridor
    """
    
    try:
        # Extract monthly data and rename columns
        data_list = []
        
        # Maintenance metrics
        if 'du' in df['mo']:
            du_data = df['mo']['du'].rename(columns={'uptime': 'du', 'delta': 'du_delta'})
            data_list.append(du_data[['Zone_Group', 'Corridor', 'Month', 'du', 'du_delta']])
        
        if 'pau' in df['mo']:
            pau_data = df['mo']['pau'].rename(columns={'uptime': 'pau', 'delta': 'pau_delta'})
            data_list.append(pau_data[['Zone_Group', 'Corridor', 'Month', 'pau', 'pau_delta']])
        
        if 'cctv' in df['mo']:
            cctv_data = df['mo']['cctv'].rename(columns={'uptime': 'cctv', 'delta': 'cctv_delta'})
            data_list.append(cctv_data[['Zone_Group', 'Corridor', 'Month', 'cctv', 'cctv_delta']])
        
        if 'cu' in df['mo']:
            cu_data = df['mo']['cu'].rename(columns={'uptime': 'cu', 'delta': 'cu_delta'})
            data_list.append(cu_data[['Zone_Group', 'Corridor', 'Month', 'cu', 'cu_delta']])
        
        # Operations metrics
        if 'tp' in df['mo']:
            tp_data = df['mo']['tp'].rename(columns={'delta': 'tp_delta'})
            data_list.append(tp_data[['Zone_Group', 'Corridor', 'Month', 'tp_delta']])
        
        if 'aogd' in df['mo']:
            aog_data = df['mo']['aogd'].rename(columns={'delta': 'aog_delta'})
            data_list.append(aog_data[['Zone_Group', 'Corridor', 'Month', 'aog_delta']])
        
        if 'prd' in df['mo']:
            pr_data = df['mo']['prd'].rename(columns={'delta': 'pr_delta'})
            data_list.append(pr_data[['Zone_Group', 'Corridor', 'Month', 'pr_delta']])
        
        if 'qsd' in df['mo']:
            qs_data = df['mo']['qsd'].rename(columns={'delta': 'qs_delta'})
            data_list.append(qs_data[['Zone_Group', 'Corridor', 'Month', 'qs_delta']])
        
        if 'sfd' in df['mo']:
            sf_data = df['mo']['sfd'].rename(columns={'delta': 'sf_delta'})
            data_list.append(sf_data[['Zone_Group', 'Corridor', 'Month', 'sf_delta']])
        
        if 'pd' in df['mo']:
            pd_data = df['mo']['pd'].copy()
            if 'delta' in pd_data.columns:
                pd_data = pd_data.rename(columns={'delta': 'pd_delta'})
            data_list.append(pd_data[['Zone_Group', 'Corridor', 'Month', 'pd', 'pd_delta']])
        
        if 'flash' in df['mo']:
            flash_data = df['mo']['flash'].rename(columns={'flash': 'flash_events', 'delta': 'flash_delta'})
            data_list.append(flash_data[['Zone_Group', 'Corridor', 'Month', 'flash_events', 'flash_delta']])
        
        # Travel time metrics
        if 'tti' in df['mo'] and not df['mo']['tti'].empty:
            tti_data = df['mo']['tti'].rename(columns={'delta': 'tti_delta'})
            data_list.append(tti_data[['Zone_Group', 'Corridor', 'Month', 'tti', 'tti_delta']])
        else:
            # Create empty tti data for signals
            empty_tti = pd.DataFrame(columns=['Zone_Group', 'Corridor', 'Month', 'tti', 'tti_delta'])
            data_list.append(empty_tti)
        
        if 'pti' in df['mo'] and not df['mo']['pti'].empty:
            pti_data = df['mo']['pti'].rename(columns={'delta': 'pti_delta'})
            data_list.append(pti_data[['Zone_Group', 'Corridor', 'Month', 'pti', 'pti_delta']])
        else:
            # Create empty pti data for signals
            empty_pti = pd.DataFrame(columns=['Zone_Group', 'Corridor', 'Month', 'pti', 'pti_delta'])
            data_list.append(empty_pti)
        
        # Safety metrics
        if 'kabco' in df['mo']:
            kabco_data = df['mo']['kabco'].rename(columns={'delta': 'kabco_delta'})
            data_list.append(kabco_data[['Zone_Group', 'Corridor', 'Month', 'kabco', 'kabco_delta']])
        
        if 'cri' in df['mo']:
            cri_data = df['mo']['cri'].rename(columns={'delta': 'cri_delta'})
            data_list.append(cri_data[['Zone_Group', 'Corridor', 'Month', 'cri', 'cri_delta']])
        
        if 'rsi' in df['mo']:
            rsi_data = df['mo']['rsi'].rename(columns={'delta': 'rsi_delta'})
            data_list.append(rsi_data[['Zone_Group', 'Corridor', 'Month', 'rsi', 'rsi_delta']])
        else:
            empty_rsi = pd.DataFrame(columns=['Zone_Group', 'Corridor', 'Month', 'rsi', 'rsi_delta'])
            data_list.append(empty_rsi)
        
        if 'bpsi' in df['mo']:
            bpsi_data = df['mo']['bpsi'].rename(columns={'delta': 'bpsi_delta'})
            data_list.append(bpsi_data[['Zone_Group', 'Corridor', 'Month', 'bpsi', 'bpsi_delta']])
        else:
            empty_bpsi = pd.DataFrame(columns=['Zone_Group', 'Corridor', 'Month', 'bpsi', 'bpsi_delta'])
            data_list.append(empty_bpsi)
        
        # Merge all data
        if data_list:
            # Start with first dataframe
            result = data_list[0]
            
            # Merge remaining dataframes
            for data in data_list[1:]:
                if not data.empty:
                    result = pd.merge(result, data, on=['Zone_Group', 'Corridor', 'Month'], how='outer')
        else:
            result = pd.DataFrame()
        
        if not result.empty:
            # Filter out rows where Zone_Group == Corridor
            result = result[result['Zone_Group'].astype(str) != result['Corridor'].astype(str)]
            
            # Remove unwanted columns
            columns_to_drop = [col for col in result.columns if any(pattern in col for pattern in [
                'uptime.sb', 'uptime.pr', 'Duration', 'Events', 'num', 'Description',
                'ones', 'cycles', 'pct', 'vol'
            ])]
            result = result.drop(columns=columns_to_drop, errors='ignore')
            
            # Remove duplicates
            result = result.drop_duplicates()
            
            # Determine if this is corridor or subcorridor data
            if 'mttr' in df['mo']:  # corridor data
                result = result.sort_values(['Month', 'Zone_Group', 'Corridor'])
            else:  # subcorridor data
                result = result.rename(columns={'Corridor': 'Subcorridor', 'Zone_Group': 'Corridor'})
                result = result.sort_values(['Month', 'Corridor', 'Subcorridor'])
            
            # Reorder columns
            cols = result.columns.tolist()
            if 'Subcorridor' in cols:
                # Subcorridor data
                new_order = ['Corridor', 'Subcorridor'] + [col for col in cols if col not in ['Corridor', 'Subcorridor']]
            else:
                # Corridor data
                new_order = ['Zone_Group', 'Corridor'] + [col for col in cols if col not in ['Zone_Group', 'Corridor']]
            result = result[new_order]
            
            # Filter by current month if specified
            if current_month:
                result['Month'] = pd.to_datetime(result['Month'])
                current_month_dt = pd.to_datetime(current_month)
                result = result[result['Month'] == current_month_dt]
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_summary_data: {e}")
        return pd.DataFrame()


def get_lookup_value(dt: pd.DataFrame, lookup_col: str, lookup_val: str, 
                    x: Union[pd.Series, np.ndarray, List], direction: str = "forward") -> np.ndarray:
    """
    Function used to look up health score for various metric
    
    Args:
        dt: Lookup DataFrame
        lookup_col: Column to lookup against
        lookup_val: Value column to return
        x: Values to lookup
        direction: 'forward' or 'backward' for rolling join
        
    Returns:
        Array of lookup values
    """
    
    try:
        if dt.empty or lookup_col not in dt.columns or lookup_val not in dt.columns:
            return np.full(len(x), np.nan)
        
        # Sort lookup table by lookup column
        dt_sorted = dt.sort_values(lookup_col)
        
        # Convert x to numpy array
        x_array = np.array(x)
        
        # Initialize result array
        result = np.full(len(x_array), np.nan)
        
        for i, val in enumerate(x_array):
            if pd.isna(val):
                continue
                
            if direction == "forward":
                # Find the first value >= val
                mask = dt_sorted[lookup_col] >= val
            else:  # backward
                # Find the last value <= val
                mask = dt_sorted[lookup_col] <= val
            
            if mask.any():
                if direction == "forward":
                    idx = mask.idxmax()  # First True value
                else:
                    # For backward, we want the last True value
                    idx = dt_sorted[mask].index[-1]
                
                result[i] = dt_sorted.loc[idx, lookup_val]
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_lookup_value: {e}")
        return np.full(len(x), np.nan)


def get_health_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to compute scores/weights at sub/sig level
    
    Args:
        df: Input dataframe with health metrics
        
    Returns:
        DataFrame with calculated scores and weights
    """
    
    try:
        # Select and rename columns for health calculation
        health_df = df.copy()
        
        # Rename columns to match expected names
        column_mapping = {
            'du': 'Detection_Uptime',
            'pau': 'Ped_Act_Uptime', 
            'cu': 'Comm_Uptime',
            'cctv': 'CCTV_Uptime',
            'flash_events': 'Flash_Events',
            'pr': 'Platoon_Ratio',
            'pd': 'Ped_Delay',
            'sf_freq': 'Split_Failures',
            'tti': 'TTI',
            'pti': 'pti',
            'cri': 'Crash_Rate_Index',
            'kabco': 'KABCO_Crash_Severity_Index',
            'rsi': 'High_Speed_Index',
            'bpsi': 'Ped_Injury_Exposure_Index'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in health_df.columns:
                health_df = health_df.rename(columns={old_col: new_col})
        
        # Calculate derived metrics
        if 'Flash_Events' in health_df.columns:
            health_df['Flash_Events'] = health_df['Flash_Events'].fillna(0)
        
        if 'TTI' in health_df.columns and 'pti' in health_df.columns:
            health_df['BI'] = health_df['pti'] - health_df['TTI']
            health_df = health_df.drop('pti', axis=1)
        
        # Handle infinite values in crash metrics
        crash_columns = ['Crash_Rate_Index', 'KABCO_Crash_Severity_Index']
        for col in crash_columns:
            if col in health_df.columns:
                health_df[col] = health_df[col].replace([np.inf, -np.inf], np.nan)
        
        # Calculate scores for each metric
        score_calculations = {
            'Detection_Uptime_Score': ('detection', 'Detection_Uptime', 'backward'),
            'Ped_Act_Uptime_Score': ('ped_actuation', 'Ped_Act_Uptime', 'backward'),
            'Comm_Uptime_Score': ('comm', 'Comm_Uptime', 'backward'),
            'CCTV_Uptime_Score': ('cctv', 'CCTV_Uptime', 'backward'),
            'Flash_Events_Score': ('flash_events', 'Flash_Events', 'forward'),
            'Platoon_Ratio_Score': ('pr', 'Platoon_Ratio', 'forward'),
            'Ped_Delay_Score': ('ped_delay', 'Ped_Delay', 'forward'),
            'Split_Failures_Score': ('sf', 'Split_Failures', 'forward'),
            'TTI_Score': ('tti', 'TTI', 'forward'),
            'BI_Score': ('bi', 'BI', 'forward'),
            'Crash_Rate_Index_Score': ('crash_rate', 'Crash_Rate_Index', 'forward'),
            'KABCO_Crash_Severity_Index_Score': ('kabco', 'KABCO_Crash_Severity_Index', 'forward'),
            'High_Speed_Index_Score': ('high_speed', 'High_Speed_Index', 'forward'),
            'Ped_Injury_Exposure_Index_Score': ('ped_injury_exposure', 'Ped_Injury_Exposure_Index', 'forward')
        }
        
        for score_col, (lookup_key, value_col, direction) in score_calculations.items():
            if value_col in health_df.columns:
                # Filter scoring lookup for this metric
                metric_lookup = scoring_lookup[scoring_lookup['metric'] == lookup_key] if 'metric' in scoring_lookup.columns else scoring_lookup
                
                if not metric_lookup.empty:
                    scores = get_lookup_value(
                        metric_lookup, 'value', 'score', 
                        health_df[value_col], direction
                    )
                    health_df[score_col] = scores
        
        # Merge with weights lookup
        if not weights_lookup.empty and 'Context' in health_df.columns:
            health_df = pd.merge(health_df, weights_lookup, on='Context', how='inner')
        
        # Set weights to NA where scores are NA
        weight_score_pairs = [
            ('Detection_Uptime_Weight', 'Detection_Uptime_Score'),
            ('Ped_Act_Uptime_Weight', 'Ped_Act_Uptime_Score'),
            ('Comm_Uptime_Weight', 'Comm_Uptime_Score'),
            ('CCTV_Uptime_Weight', 'CCTV_Uptime_Score'),
            ('Flash_Events_Weight', 'Flash_Events_Score'),
            ('Platoon_Ratio_Weight', 'Platoon_Ratio_Score'),
            ('Ped_Delay_Weight', 'Ped_Delay_Score'),
            ('Split_Failures_Weight', 'Split_Failures_Score'),
            ('TTI_Weight', 'TTI_Score'),
            ('BI_Weight', 'BI_Score'),
            ('Crash_Rate_Index_Weight', 'Crash_Rate_Index_Score'),
            ('KABCO_Crash_Severity_Index_Weight', 'KABCO_Crash_Severity_Index_Score'),
            ('High_Speed_Index_Weight', 'High_Speed_Index_Score'),
            ('Ped_Injury_Exposure_Index_Weight', 'Ped_Injury_Exposure_Index_Score')
        ]
        
        for weight_col, score_col in weight_score_pairs:
            if weight_col in health_df.columns and score_col in health_df.columns:
                health_df.loc[health_df[score_col].isna(), weight_col] = np.nan
        
        return health_df
        
    except Exception as e:
        logger.error(f"Error in get_health_all: {e}")
        return df


def get_percent_health_subtotals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function that computes % health subtotals for corridor and zone level
    
    Args:
        df: DataFrame with health data
        
    Returns:
        DataFrame with subtotals added
    """
    
    try:
        # Corridor subtotals
        corridor_subtotals = df.groupby(['Zone_Group', 'Zone', 'Corridor', 'Month']).agg({
            'Percent_Health': 'mean'
        }).reset_index()
        
        # Zone subtotals
        zone_subtotals = corridor_subtotals.groupby(['Zone_Group', 'Zone', 'Month']).agg({
            'Percent_Health': 'mean'
        }).reset_index()
        
        # Combine all data
        combined = pd.concat([df, corridor_subtotals, zone_subtotals], ignore_index=True)
        
        # Fill missing values
        combined = combined.groupby('Zone').apply(lambda x: x.fillna(method='ffill')).reset_index(drop=True)
        combined = combined.groupby('Corridor').apply(lambda x: x.fillna(method='ffill')).reset_index(drop=True)
        
        # Convert to factors and arrange
        combined['Zone_Group'] = pd.Categorical(combined['Zone_Group'])
        combined['Zone'] = pd.Categorical(combined['Zone'])
        
        combined = combined.sort_values(['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month'])
        
        # Fill Corridor and Subcorridor
        combined['Corridor'] = combined['Corridor'].fillna(combined['Zone'])
        combined['Subcorridor'] = combined['Subcorridor'].fillna(combined['Corridor'])
        
        combined['Corridor'] = pd.Categorical(combined['Corridor'])
        combined['Subcorridor'] = pd.Categorical(combined['Subcorridor'])
        
        return combined
        
    except Exception as e:
        logger.error(f"Error in get_percent_health_subtotals: {e}")
        return df


def get_health_maintenance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to produce maintenance % health data frame
    
    Args:
        df: DataFrame with health data
        
    Returns:
        DataFrame with maintenance health metrics
    """
    
    try:
        # Select maintenance-related columns
        maintenance_cols = [col for col in df.columns if any(metric in col for metric in [
            'Detection', 'Ped_Act', 'Comm', 'CCTV', 'Flash_Events'
        ])]
        
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        health = df[base_cols + maintenance_cols].copy()
        
        # Get score and weight columns
        score_cols = [col for col in health.columns if col.endswith('_Score')]
        weight_cols = [col for col in health.columns if col.endswith('_Weight')]
        
        # Sort columns to ensure alignment
        score_cols.sort()
        weight_cols.sort()
        
        # Calculate percent health
        scores = health[score_cols].fillna(0)
        weights = health[weight_cols].fillna(0)
        
        # Calculate weighted average
        weighted_sum = (scores * weights).sum(axis=1)
        weight_sum = weights.sum(axis=1)
        
        health['Percent_Health'] = weighted_sum / weight_sum / 10
        health['Missing_Data'] = 1 - weight_sum / 100
        
        # Group by and calculate means
        health = health.groupby(['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month']).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data']}
        }).reset_index()
        
        # Add subtotals
        health = get_percent_health_subtotals(health)
        
        # Rename columns for output
        output_columns = {
            'Zone_Group': 'Zone Group',
            'Zone': 'Zone',
            'Corridor': 'Corridor', 
            'Subcorridor': 'Subcorridor',
            'Month': 'Month',
            'Context_Category': 'Context',
            'Percent_Health': 'Percent Health',
            'Missing_Data': 'Missing Data',
            'Detection_Uptime_Score': 'Detection Uptime Score',
            'Ped_Act_Uptime_Score': 'Ped Actuation Uptime Score',
            'Comm_Uptime_Score': 'Comm Uptime Score',
            'CCTV_Uptime_Score': 'CCTV Uptime Score',
            'Flash_Events_Score': 'Flash Events Score',
            'Detection_Uptime': 'Detection Uptime',
            'Ped_Act_Uptime': 'Ped Actuation Uptime',
            'Comm_Uptime': 'Comm Uptime',
            'CCTV_Uptime': 'CCTV Uptime',
            'Flash_Events': 'Flash Events'
        }
        
        # Select and rename columns that exist
        final_cols = {k: v for k, v in output_columns.items() if k in health.columns}
        health = health[list(final_cols.keys())].rename(columns=final_cols)
        
        return health
        
    except Exception as e:
        logger.error(f"Error in get_health_maintenance: {e}")
        return df


def get_health_operations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to produce operations % health data frame
    
    Args:
        df: DataFrame with health data
        
    Returns:
        DataFrame with operations health metrics
    """
    
    try:
        # Select operations-related columns
        operations_cols = [col for col in df.columns if any(metric in col for metric in [
            'Platoon_Ratio', 'Ped_Delay', 'Split_Failures', 'TTI', 'BI'
        ])]
        
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        health = df[base_cols + operations_cols].copy()
        
        # Get score and weight columns
        score_cols = [col for col in health.columns if col.endswith('_Score')]
        weight_cols = [col for col in health.columns if col.endswith('_Weight')]
        
        # Sort columns to ensure alignment
        score_cols.sort()
        weight_cols.sort()
        
        # Calculate percent health
        scores = health[score_cols].fillna(0)
        weights = health[weight_cols].fillna(0)
        
        # Calculate weighted average
        weighted_sum = (scores * weights).sum(axis=1)
        weight_sum = weights.sum(axis=1)
        
        health['Percent_Health'] = weighted_sum / weight_sum / 10
        health['Missing_Data'] = 1 - weight_sum / 100
        
        # Group by and calculate means
        health = health.groupby(['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month']).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data']}
        }).reset_index()
        
        # Add subtotals
        health = get_percent_health_subtotals(health)
        
        # Rename columns for output
        output_columns = {
            'Zone_Group': 'Zone Group',
            'Zone': 'Zone',
            'Corridor': 'Corridor',
            'Subcorridor': 'Subcorridor', 
            'Month': 'Month',
            'Context_Category': 'Context',
            'Percent_Health': 'Percent Health',
            'Missing_Data': 'Missing Data',
            'Platoon_Ratio_Score': 'Platoon Ratio Score',
            'Ped_Delay_Score': 'Ped Delay Score',
            'Split_Failures_Score': 'Split Failures Score',
            'TTI_Score': 'Travel Time Index Score',
            'BI_Score': 'Buffer Index Score',
            'Platoon_Ratio': 'Platoon Ratio',
            'Ped_Delay': 'Ped Delay',
            'Split_Failures': 'Split Failures',
            'TTI': 'Travel Time Index',
            'BI': 'Buffer Index'
        }
        
        # Select and rename columns that exist
        final_cols = {k: v for k, v in output_columns.items() if k in health.columns}
        health = health[list(final_cols.keys())].rename(columns=final_cols)
        
        return health
        
    except Exception as e:
        logger.error(f"Error in get_health_operations: {e}")
        return df


def get_health_safety(df: pd.DataFrame) -> pd.DataFrame:
    """
    Function to produce safety % health data frame
    
    Args:
        df: DataFrame with health data
        
    Returns:
        DataFrame with safety health metrics
    """
    
    try:
        # Select safety-related columns
        safety_cols = [col for col in df.columns if any(metric in col for metric in [
            'Crash_Rate', 'KABCO', 'High_Speed', 'Ped_Injury'
        ])]
        
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        health = df[base_cols + safety_cols].copy()
        
        # Get score and weight columns
        score_cols = [col for col in health.columns if col.endswith('_Score')]
        weight_cols = [col for col in health.columns if col.endswith('_Weight')]
        
        # Sort columns to ensure alignment
        score_cols.sort()
        weight_cols.sort()
        
        # Calculate percent health
        scores = health[score_cols].fillna(0)
        weights = health[weight_cols].fillna(0)
        
        # Calculate weighted average
        weighted_sum = (scores * weights).sum(axis=1)
        weight_sum = weights.sum(axis=1)
        
        health['Percent_Health'] = weighted_sum / weight_sum / 10
        health['Missing_Data'] = 1 - weight_sum / 100
        
        # Group by and calculate means
        health = health.groupby(['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month']).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data']}
        }).reset_index()
        
        # Add subtotals
        health = get_percent_health_subtotals(health)
        
        # Rename columns for output
        output_columns = {
            'Zone_Group': 'Zone Group',
            'Zone': 'Zone',
            'Corridor': 'Corridor',
            'Subcorridor': 'Subcorridor',
            'Month': 'Month',
            'Context_Category': 'Context',
            'Percent_Health': 'Percent Health',
            'Missing_Data': 'Missing Data',
            'Crash_Rate_Index_Score': 'Crash Rate Index Score',
            'KABCO_Crash_Severity_Index_Score': 'KABCO Crash Severity Index Score',
            'High_Speed_Index_Score': 'High Speed Index Score',
            'Ped_Injury_Exposure_Index_Score': 'Ped Injury Exposure Index Score',
            'Crash_Rate_Index': 'Crash Rate Index',
            'KABCO_Crash_Severity_Index': 'KABCO Crash Severity Index',
            'High_Speed_Index': 'High Speed Index',
            'Ped_Injury_Exposure_Index': 'Ped Injury Exposure Index'
        }
        
        # Select and rename columns that exist
        final_cols = {k: v for k, v in output_columns.items() if k in health.columns}
        health = health[list(final_cols.keys())].rename(columns=final_cols)
        
        return health
        
    except Exception as e:
        logger.error(f"Error in get_health_safety: {e}")
        return df


def get_health_metrics_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare health metrics data for plotting (subcorridor level)
    
    Args:
        df: Health metrics DataFrame
        
    Returns:
        Formatted DataFrame for plotting
    """
    
    try:
        plot_df = df.copy()
        
        # Rename columns for subcorridor plotting
        if 'Zone Group' in plot_df.columns and 'Corridor' in plot_df.columns:
            plot_df = plot_df.rename(columns={
                'Zone Group': 'Zone_Group',
                'Corridor': 'Zone_Group_New',
                'Subcorridor': 'Corridor'
            })
            plot_df['Zone_Group'] = plot_df['Zone_Group_New']
            plot_df = plot_df.drop('Zone_Group_New', axis=1)
        
        # Remove Zone Group and Zone columns if they exist
        plot_df = plot_df.drop(['Zone'], axis=1, errors='ignore')
        
        # Convert to categorical
        plot_df['Zone_Group'] = pd.Categorical(plot_df['Zone_Group'])
        plot_df['Corridor'] = pd.Categorical(plot_df['Corridor'])
        
        # Sort data
        plot_df = plot_df.sort_values(['Zone_Group', 'Corridor', 'Month'])
        
        return plot_df
        
    except Exception as e:
        logger.error(f"Error in get_health_metrics_plot_df: {e}")
        return df


def get_cor_health_metrics_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare health metrics data for corridor plotting
    
    Args:
        df: Health metrics DataFrame
        
    Returns:
        Formatted DataFrame for corridor plotting
    """
    
    try:
        plot_df = df.copy()
        
        # Remove Zone Group column and rename Zone to Zone_Group
        plot_df = plot_df.drop(['Zone Group'], axis=1, errors='ignore')
        if 'Zone' in plot_df.columns:
            plot_df = plot_df.rename(columns={'Zone': 'Zone_Group'})
        
        # Convert to categorical
        plot_df['Zone_Group'] = pd.Categorical(plot_df['Zone_Group'])
        plot_df['Corridor'] = pd.Categorical(plot_df['Corridor'])
        
        # Sort data
        plot_df = plot_df.sort_values(['Zone_Group', 'Corridor', 'Month'])
        
        return plot_df
        
    except Exception as e:
        logger.error(f"Error in get_cor_health_metrics_plot_df: {e}")
        return df


def add_subcorridor(df: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Add subcorridor information to signal data
    
    Args:
        df: DataFrame with signal data
        corridors: Corridor mapping DataFrame
        
    Returns:
        DataFrame with subcorridor information added
    """
    
    try:
        # Rename SignalID column
        signal_df = df.rename(columns={'Subcorridor': 'SignalID'})
        
        # Merge with corridors data
        result = pd.merge(
            signal_df,
            corridors[['Zone', 'Corridor', 'Subcorridor', 'SignalID']],
            on=['Zone', 'Corridor', 'SignalID'],
            how='left'
        )
        
        # Reorder columns to put Subcorridor after Corridor
        cols = result.columns.tolist()
        if 'Subcorridor' in cols and 'Corridor' in cols:
            corridor_idx = cols.index('Corridor')
            cols.remove('Subcorridor')
            cols.insert(corridor_idx + 1, 'Subcorridor')
            result = result[cols]
        
        # Filter out rows with missing subcorridor
        result = result.dropna(subset=['Subcorridor'])
        
        return result
        
    except Exception as e:
        logger.error(f"Error in add_subcorridor: {e}")
        return df


def load_corridor_groupings() -> pd.DataFrame:
    """
    Load corridor groupings from Excel file
    
    Returns:
        DataFrame with corridor context information
    """
    
    try:
        # This would need to be adapted based on your configuration
        # For now, return empty DataFrame
        logger.warning("Corridor groupings not implemented - returning empty DataFrame")
        return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error loading corridor groupings: {e}")
        return pd.DataFrame()


def process_health_metrics(sub_data: Dict, sig_data: Dict, cam_config: pd.DataFrame, 
                         corridors: pd.DataFrame) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Main function to process all health metrics
    
    Args:
        sub_data: Subcorridor data dictionary
        sig_data: Signal data dictionary  
        cam_config: Camera configuration DataFrame
        corridors: Corridor mapping DataFrame
        
    Returns:
        Dictionary containing processed health metrics
    """
    
    try:
        logger.info("Starting health metrics processing")
        
        # Load corridor groupings
        corridor_groupings = load_corridor_groupings()
        
        # Get summary data
        csd = get_summary_data(sub_data)
        ssd = get_summary_data(sig_data)
        
        # Process signal summary data
        if not ssd.empty and not cam_config.empty:
            # Merge with camera config to get SignalID for CameraID
            ssd = pd.merge(
                ssd,
                cam_config[['SignalID', 'CameraID']],
                left_on='Corridor',
                right_on='CameraID',
                how='left'
            )
            
            # Use SignalID where available, otherwise keep original Corridor
            ssd['Corridor'] = ssd['SignalID'].fillna(ssd['Corridor'])
            ssd = ssd.drop('SignalID', axis=1)
            
            # Filter out camera-only entries
            ssd = ssd[~ssd['Corridor'].astype(str).str.contains('CAM', na=False)]
            
            # Group by and fill missing values
            ssd = ssd.groupby(['Corridor', 'Zone_Group', 'Month']).apply(
                lambda x: x.fillna(method='ffill').fillna(method='bfill')
            ).reset_index(drop=True)
            
            # Take first row per group
            ssd = ssd.groupby(['Corridor', 'Zone_Group', 'Month']).first().reset_index()
            
            # Replace infinite values with NaN
            numeric_cols = ssd.select_dtypes(include=[np.number]).columns
            ssd[numeric_cols] = ssd[numeric_cols].replace([np.inf, -np.inf], np.nan)
        
        # Create health input dataframes
        if not corridor_groupings.empty:
            # Subcorridor health data
            sub_health_data = pd.merge(
                csd,
                corridor_groupings,
                on=['Corridor', 'Subcorridor'],
                how='inner'
            )
            
            # Signal health data
            sig_health_data = ssd.rename(columns={
                'Corridor': 'SignalID',
                'Zone_Group': 'Corridor'
            })
            
            # Merge with corridors and corridor groupings
            sig_health_data = pd.merge(
                sig_health_data,
                corridors[['Corridor', 'SignalID', 'Subcorridor']],
                on=['Corridor', 'SignalID'],
                how='left'
            )
            
            sig_health_data = pd.merge(
                sig_health_data,
                corridor_groupings,
                on=['Corridor', 'Subcorridor'],
                how='inner'
            )
            
            # Add TTI/PTI from subcorridor data for contexts 1-4
            tti_pti_data = sub_health_data[['Corridor', 'Subcorridor', 'Month', 'tti', 'pti']]
            
            sig_health_data = pd.merge(
                sig_health_data,
                tti_pti_data,
                on=['Corridor', 'Subcorridor', 'Month'],
                how='left',
                suffixes=('_x', '_y')
            )
            
            # Use subcorridor TTI/PTI for contexts 1-4
            context_mask = sig_health_data['Context'].isin([1, 2, 3, 4])
            sig_health_data.loc[context_mask, 'tti_x'] = sig_health_data.loc[context_mask, 'tti_y']
            sig_health_data.loc[context_mask, 'pti_x'] = sig_health_data.loc[context_mask, 'pti_y']
            
            # Clean up columns
            sig_health_data = sig_health_data.drop(['Subcorridor', 'tti_y', 'pti_y'], axis=1)
            sig_health_data = sig_health_data.rename(columns={
                'SignalID': 'Subcorridor',
                'tti_x': 'tti',
                'pti_x': 'pti'
            })
        else:
            sub_health_data = csd
            sig_health_data = ssd
        
        # Generate health metrics
        logger.info("Calculating health scores")
        
        # All health data
        health_all_sub = get_health_all(sub_health_data)
        health_all_sig = get_health_all(sig_health_data)
        
        # Maintenance health
        maintenance_sub = get_health_maintenance(health_all_sub)
        maintenance_sig = get_health_maintenance(health_all_sig)
        
        # Corridor maintenance (aggregated from subcorridor)
        maintenance_cor = maintenance_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
        maintenance_cor = maintenance_cor.groupby(['Zone Group', 'Zone', 'Corridor', 'Month']).agg(
            lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
        ).reset_index()
        
        # Operations health
        operations_sub = get_health_operations(health_all_sub)
        operations_sig = get_health_operations(health_all_sig)
        
        # Corridor operations (aggregated from subcorridor)
        operations_cor = operations_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
        operations_cor = operations_cor.groupby(['Zone Group', 'Zone', 'Corridor', 'Month']).agg(
            lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
        ).reset_index()
        
        # Safety health
        safety_sub = get_health_safety(health_all_sub)
        safety_sig = get_health_safety(health_all_sig)
        
        # Corridor safety (aggregated from subcorridor)
        safety_cor = safety_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
        safety_cor = safety_cor.groupby(['Zone Group', 'Zone', 'Corridor', 'Month']).agg(
            lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
        ).reset_index()
        
        # Create plot dataframes
        plot_data = {
            'cor': {
                'mo': {
                    'maint_plot': get_cor_health_metrics_plot_df(maintenance_cor),
                    'ops_plot': get_cor_health_metrics_plot_df(operations_cor),
                    'safety_plot': get_cor_health_metrics_plot_df(safety_cor),
                    'maint': maintenance_cor,
                    'ops': operations_cor,
                    'safety': safety_cor
                }
            },
            'sub': {
                'mo': {
                    'maint_plot': get_health_metrics_plot_df(maintenance_sub),
                    'ops_plot': get_health_metrics_plot_df(operations_sub),
                    'safety_plot': get_health_metrics_plot_df(safety_sub),
                    'maint': maintenance_sub,
                    'ops': operations_sub,
                    'safety': safety_sub
                }
            },
            'sig': {
                'mo': {
                    'maint_plot': get_health_metrics_plot_df(maintenance_sig),
                    'ops_plot': get_health_metrics_plot_df(operations_sig),
                    'safety_plot': get_health_metrics_plot_df(safety_sig),
                    'maint': add_subcorridor(maintenance_sig, corridors),
                    'ops': add_subcorridor(operations_sig, corridors),
                    'safety': add_subcorridor(safety_sig, corridors)
                }
            }
        }
        
        logger.info("Health metrics processing completed successfully")
        return plot_data
        
    except Exception as e:
        logger.error(f"Error in process_health_metrics: {e}")
        return {}


def export_health_metrics(health_data: Dict, output_dir: str = "output") -> bool:
    """
    Export health metrics to CSV files
    
    Args:
        health_data: Health metrics data dictionary
        output_dir: Output directory for CSV files
        
    Returns:
        Boolean indicating success
    """
    
    try:
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        for level in ['cor', 'sub', 'sig']:
            for metric_type in ['maint', 'ops', 'safety']:
                if level in health_data and 'mo' in health_data[level]:
                    if metric_type in health_data[level]['mo']:
                        df = health_data[level]['mo'][metric_type]
                        if not df.empty:
                            filename = f"{output_dir}/{level}_{metric_type}_health.csv"
                            df.to_csv(filename, index=False)
                            logger.info(f"Exported {filename}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error exporting health metrics: {e}")
        return False


def validate_health_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate health metrics data quality
    
    Args:
        df: Health metrics DataFrame
        
    Returns:
        Dictionary with validation results
    """
    
    try:
        results = {
            'total_rows': len(df),
            'missing_data': {},
            'value_ranges': {},
            'data_quality_score': 0
        }
        
        if df.empty:
            results['data_quality_score'] = 0
            return results
        
        # Check missing data
        for col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                missing_pct = df[col].isna().sum() / len(df) * 100
                results['missing_data'][col] = missing_pct
                
                # Check value ranges for health percentages
                if 'Percent Health' in col or 'Score' in col:
                    valid_range = ((df[col] >= 0) & (df[col] <= 100)).sum() / df[col].notna().sum() * 100
                    results['value_ranges'][col] = valid_range
        
        # Calculate overall data quality score
        avg_missing = np.mean(list(results['missing_data'].values())) if results['missing_data'] else 0
        avg_valid_range = np.mean(list(results['value_ranges'].values())) if results['value_ranges'] else 100
        
        results['data_quality_score'] = max(0, 100 - avg_missing) * (avg_valid_range / 100)
        
        return results
        
    except Exception as e:
        logger.error(f"Error validating health data: {e}")
        return {'error': str(e)}


def create_health_summary_report(health_data: Dict, output_file: str = "health_summary.txt") -> bool:
    """
    Create a summary report of health metrics
    
    Args:
        health_data: Health metrics data dictionary
        output_file: Output file path
        
    Returns:
        Boolean indicating success
    """
    
    try:
        with open(output_file, 'w') as f:
            f.write("HEALTH METRICS SUMMARY REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for level in ['cor', 'sub', 'sig']:
                level_name = {'cor': 'Corridor', 'sub': 'Subcorridor', 'sig': 'Signal'}[level]
                f.write(f"{level_name.upper()} LEVEL METRICS\n")
                f.write("-" * 30 + "\n")
                
                if level in health_data and 'mo' in health_data[level]:
                    for metric_type in ['maint', 'ops', 'safety']:
                        metric_name = {
                            'maint': 'Maintenance', 
                            'ops': 'Operations', 
                            'safety': 'Safety'
                        }[metric_type]
                        
                        if metric_type in health_data[level]['mo']:
                            df = health_data[level]['mo'][metric_type]
                            
                            if not df.empty and 'Percent Health' in df.columns:
                                avg_health = df['Percent Health'].mean()
                                min_health = df['Percent Health'].min()
                                max_health = df['Percent Health'].max()
                                
                                f.write(f"\n{metric_name} Health:\n")
                                f.write(f"  Average: {avg_health:.1f}%\n")
                                f.write(f"  Range: {min_health:.1f}% - {max_health:.1f}%\n")
                                f.write(f"  Records: {len(df)}\n")
                                
                                # Data quality validation
                                validation = validate_health_data(df)
                                f.write(f"  Data Quality Score: {validation['data_quality_score']:.1f}%\n")
                
                f.write("\n")
            
            f.write("\nREPORT COMPLETE\n")
        
        logger.info(f"Health summary report saved to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating health summary report: {e}")
        return False


def calculate_health_trends(df: pd.DataFrame, months: int = 6) -> pd.DataFrame:
    """
    Calculate health trends over time
    
    Args:
        df: Health metrics DataFrame with Month column
        months: Number of months to analyze
        
    Returns:
        DataFrame with trend analysis
    """
    
    try:
        if df.empty or 'Month' not in df.columns or 'Percent Health' not in df.columns:
            return pd.DataFrame()
        
        # Ensure Month is datetime
        df['Month'] = pd.to_datetime(df['Month'])
        
        # Get recent months
        max_month = df['Month'].max()
        min_month = max_month - pd.DateOffset(months=months-1)
        recent_df = df[df['Month'] >= min_month].copy()
        
        if recent_df.empty:
            return pd.DataFrame()
        
        # Calculate trends by corridor
        trend_results = []
        
        groupby_cols = ['Corridor']
        if 'Zone_Group' in recent_df.columns:
            groupby_cols = ['Zone_Group', 'Corridor']
        
        for group_vals, group_df in recent_df.groupby(groupby_cols):
            if len(group_df) < 2:
                continue
                
            group_df = group_df.sort_values('Month')
            
            # Calculate trend
            x = np.arange(len(group_df))
            y = group_df['Percent Health'].values
            
            if len(x) > 1 and not np.isnan(y).all():
                # Simple linear regression
                valid_mask = ~np.isnan(y)
                if valid_mask.sum() > 1:
                    x_valid = x[valid_mask]
                    y_valid = y[valid_mask]
                    
                    slope = np.polyfit(x_valid, y_valid, 1)[0]
                    
                    trend_result = {
                        'Corridor': group_vals[-1] if isinstance(group_vals, tuple) else group_vals,
                        'Trend_Slope': slope,
                        'Trend_Direction': 'Improving' if slope > 0.1 else 'Declining' if slope < -0.1 else 'Stable',
                        'Start_Health': group_df['Percent Health'].iloc[0],
                        'End_Health': group_df['Percent Health'].iloc[-1],
                        'Avg_Health': group_df['Percent Health'].mean(),
                        'Health_Change': group_df['Percent Health'].iloc[-1] - group_df['Percent Health'].iloc[0],
                        'Data_Points': len(group_df)
                    }
                    
                    if len(groupby_cols) > 1:
                        trend_result['Zone_Group'] = group_vals[0]
                    
                    trend_results.append(trend_result)
        
        return pd.DataFrame(trend_results)
        
    except Exception as e:
        logger.error(f"Error calculating health trends: {e}")
        return pd.DataFrame()


def identify_health_outliers(df: pd.DataFrame, threshold: float = 2.0) -> pd.DataFrame:
    """
    Identify corridors with unusually low health scores
    
    Args:
        df: Health metrics DataFrame
        threshold: Number of standard deviations below mean to flag
        
    Returns:
        DataFrame with outlier corridors
    """
    
    try:
        if df.empty or 'Percent Health' not in df.columns:
            return pd.DataFrame()
        
        # Calculate statistics
        mean_health = df['Percent Health'].mean()
        std_health = df['Percent Health'].std()
        
        if std_health == 0:
            return pd.DataFrame()
        
        # Identify outliers
        outlier_threshold = mean_health - (threshold * std_health)
        outliers = df[df['Percent Health'] < outlier_threshold].copy()
        
        if not outliers.empty:
            outliers['Health_Deficit'] = mean_health - outliers['Percent Health']
            outliers['Z_Score'] = (outliers['Percent Health'] - mean_health) / std_health
            outliers = outliers.sort_values('Percent Health')
        
        return outliers
        
    except Exception as e:
        logger.error(f"Error identifying health outliers: {e}")
        return pd.DataFrame()


def generate_health_alerts(health_data: Dict, thresholds: Dict[str, float] = None) -> List[Dict[str, Any]]:
    """
    Generate alerts for health metrics below thresholds
    
    Args:
        health_data: Health metrics data dictionary
        thresholds: Dictionary of alert thresholds by metric type
        
    Returns:
        List of alert dictionaries
    """
    
    try:
        if thresholds is None:
            thresholds = {
                'maint': 80.0,  # Maintenance health below 80%
                'ops': 75.0,    # Operations health below 75%
                'safety': 85.0  # Safety health below 85%
            }
        
        alerts = []
        
        for level in ['cor', 'sub', 'sig']:
            level_name = {'cor': 'Corridor', 'sub': 'Subcorridor', 'sig': 'Signal'}[level]
            
            if level in health_data and 'mo' in health_data[level]:
                for metric_type in ['maint', 'ops', 'safety']:
                    if metric_type in health_data[level]['mo']:
                        df = health_data[level]['mo'][metric_type]
                        
                        if not df.empty and 'Percent Health' in df.columns:
                            threshold = thresholds.get(metric_type, 70.0)
                            
                            # Find corridors below threshold
                            low_health = df[df['Percent Health'] < threshold]
                            
                            for _, row in low_health.iterrows():
                                alert = {
                                    'level': level_name,
                                    'metric_type': metric_type.title(),
                                    'corridor': row.get('Corridor', 'Unknown'),
                                    'zone_group': row.get('Zone Group', row.get('Zone_Group', 'Unknown')),
                                    'health_score': row['Percent Health'],
                                    'threshold': threshold,
                                    'severity': 'High' if row['Percent Health'] < threshold * 0.8 else 'Medium',
                                    'month': row.get('Month', 'Unknown')
                                }
                                alerts.append(alert)
        
        # Sort alerts by severity and health score
        alerts.sort(key=lambda x: (x['severity'] == 'Medium', x['health_score']))
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error generating health alerts: {e}")
        return []


# Main execution function
def main():
    """
    Main function to demonstrate health metrics processing
    This would be called from other modules in practice
    """
    
    try:
        logger.info("Starting health metrics processing demo")
        
        # This is a placeholder - in practice, these would be loaded from your data sources
        # You would replace these with actual data loading calls
        
        # Example data structures (replace with actual data loading)
        sub_data = {'mo': {}}  # Load subcorridor data
        sig_data = {'mo': {}}  # Load signal data
        cam_config = pd.DataFrame()  # Load camera configuration
        corridors = pd.DataFrame()   # Load corridor mapping
        
        # Process health metrics
        health_results = process_health_metrics(sub_data, sig_data, cam_config, corridors)
        
        if health_results:
            # Export results
            export_health_metrics(health_results)
            
            # Create summary report
            create_health_summary_report(health_results)
            
            # Generate alerts
            alerts = generate_health_alerts(health_results)
            if alerts:
                logger.info(f"Generated {len(alerts)} health alerts")
                for alert in alerts[:5]:  # Show first 5 alerts
                    logger.warning(f"Alert: {alert['level']} {alert['corridor']} {alert['metric_type']} health: {alert['health_score']:.1f}%")
        
        logger.info("Health metrics processing completed")
        
    except Exception as e:
        logger.error(f"Error in main health metrics processing: {e}")


if __name__ == "__main__":
    main()



