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

# Try to import Athena helpers for cloud processing
try:
    from health_metrics_athena import (
        athena_get_summary_data,
        athena_get_corridor_health_aggregation,
        athena_merge_with_corridors
    )
    USE_ATHENA = True
    logger.info("Athena optimization helpers loaded - cloud processing available")
except ImportError:
    USE_ATHENA = False
    logger.debug("Athena helpers not available, using local processing only")

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

def get_summary_data(df: Dict[str, Dict[str, pd.DataFrame]], current_month: Optional[str] = None, 
                     conf: Optional[dict] = None) -> pd.DataFrame:
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
        # Try Athena first if available and conf provided
        if USE_ATHENA and conf is not None:
            try:
                # Check if df contains table names (strings) instead of DataFrames
                # This indicates data is in Athena tables
                sample_value = next(iter(df.get('mo', {}).values()), None)
                if isinstance(sample_value, str):
                    logger.info("Using Athena for summary data aggregation (table names detected)")
                    return athena_get_summary_data(df, conf, current_month)
                # If DataFrames are provided but conf is available, we could still use Athena
                # by checking if data is too large. For now, fall through to local processing.
            except Exception as e:
                logger.warning(f"Athena aggregation failed, falling back to local processing: {e}")
        
        # Local processing (original implementation)
        # Extract monthly data and rename columns
        data_list = []
        
        # Maintenance metrics
        if 'du' in df['mo'] and not df['mo']['du'].empty:
            du_data = df['mo']['du'].copy()
            if 'uptime' in du_data.columns:
                du_data = du_data.rename(columns={'uptime': 'du'})
            if 'delta' in du_data.columns:
                du_data = du_data.rename(columns={'delta': 'du_delta'})
            # Only select columns that exist
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'du', 'du_delta'] if c in du_data.columns]
            if select_cols:
                data_list.append(du_data[select_cols])
        
        if 'pau' in df['mo'] and not df['mo']['pau'].empty:
            pau_data = df['mo']['pau'].copy()
            if 'uptime' in pau_data.columns:
                pau_data = pau_data.rename(columns={'uptime': 'pau'})
            if 'delta' in pau_data.columns:
                pau_data = pau_data.rename(columns={'delta': 'pau_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'pau', 'pau_delta'] if c in pau_data.columns]
            if select_cols:
                data_list.append(pau_data[select_cols])
        
        if 'cctv' in df['mo'] and not df['mo']['cctv'].empty:
            cctv_data = df['mo']['cctv'].copy()
            if 'uptime' in cctv_data.columns:
                cctv_data = cctv_data.rename(columns={'uptime': 'cctv'})
            if 'delta' in cctv_data.columns:
                cctv_data = cctv_data.rename(columns={'delta': 'cctv_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'cctv', 'cctv_delta'] if c in cctv_data.columns]
            if select_cols:
                data_list.append(cctv_data[select_cols])
        
        if 'cu' in df['mo'] and not df['mo']['cu'].empty:
            cu_data = df['mo']['cu'].copy()
            if 'uptime' in cu_data.columns:
                cu_data = cu_data.rename(columns={'uptime': 'cu'})
            if 'delta' in cu_data.columns:
                cu_data = cu_data.rename(columns={'delta': 'cu_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'cu', 'cu_delta'] if c in cu_data.columns]
            if select_cols:
                data_list.append(cu_data[select_cols])
        
        # Operations metrics
        if 'tp' in df['mo'] and not df['mo']['tp'].empty:
            tp_data = df['mo']['tp'].copy()
            if 'delta' in tp_data.columns:
                tp_data = tp_data.rename(columns={'delta': 'tp_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'tp_delta'] if c in tp_data.columns]
            if select_cols:
                data_list.append(tp_data[select_cols])
        
        if 'aogd' in df['mo'] and not df['mo']['aogd'].empty:
            aog_data = df['mo']['aogd'].copy()
            if 'delta' in aog_data.columns:
                aog_data = aog_data.rename(columns={'delta': 'aog_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'aog_delta'] if c in aog_data.columns]
            if select_cols:
                data_list.append(aog_data[select_cols])
        
        if 'prd' in df['mo'] and not df['mo']['prd'].empty:
            pr_data = df['mo']['prd'].copy()
            if 'delta' in pr_data.columns:
                pr_data = pr_data.rename(columns={'delta': 'pr_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'pr_delta'] if c in pr_data.columns]
            if select_cols:
                data_list.append(pr_data[select_cols])
        
        if 'qsd' in df['mo'] and not df['mo']['qsd'].empty:
            qs_data = df['mo']['qsd'].copy()
            if 'delta' in qs_data.columns:
                qs_data = qs_data.rename(columns={'delta': 'qs_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'qs_delta'] if c in qs_data.columns]
            if select_cols:
                data_list.append(qs_data[select_cols])
        
        if 'sfd' in df['mo'] and not df['mo']['sfd'].empty:
            sf_data = df['mo']['sfd'].copy()
            if 'delta' in sf_data.columns:
                sf_data = sf_data.rename(columns={'delta': 'sf_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'sf_delta'] if c in sf_data.columns]
            if select_cols:
                data_list.append(sf_data[select_cols])
        
        if 'pd' in df['mo'] and not df['mo']['pd'].empty:
            pd_data = df['mo']['pd'].copy()
            if 'delta' in pd_data.columns:
                pd_data = pd_data.rename(columns={'delta': 'pd_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'pd', 'pd_delta'] if c in pd_data.columns]
            if select_cols:
                data_list.append(pd_data[select_cols])
        
        if 'flash' in df['mo'] and not df['mo']['flash'].empty:
            flash_data = df['mo']['flash'].copy()
            if 'flash' in flash_data.columns:
                flash_data = flash_data.rename(columns={'flash': 'flash_events'})
            if 'delta' in flash_data.columns:
                flash_data = flash_data.rename(columns={'delta': 'flash_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'flash_events', 'flash_delta'] if c in flash_data.columns]
            if select_cols:
                data_list.append(flash_data[select_cols])
        
        # Travel time metrics
        if 'tti' in df['mo'] and not df['mo']['tti'].empty:
            tti_data = df['mo']['tti'].copy()
            if 'delta' in tti_data.columns:
                tti_data = tti_data.rename(columns={'delta': 'tti_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'tti', 'tti_delta'] if c in tti_data.columns]
            if select_cols:
                data_list.append(tti_data[select_cols])
        
        if 'pti' in df['mo'] and not df['mo']['pti'].empty:
            pti_data = df['mo']['pti'].copy()
            if 'delta' in pti_data.columns:
                pti_data = pti_data.rename(columns={'delta': 'pti_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'pti', 'pti_delta'] if c in pti_data.columns]
            if select_cols:
                data_list.append(pti_data[select_cols])
        
        # Safety metrics
        if 'kabco' in df['mo'] and not df['mo']['kabco'].empty:
            kabco_data = df['mo']['kabco'].copy()
            if 'delta' in kabco_data.columns:
                kabco_data = kabco_data.rename(columns={'delta': 'kabco_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'kabco', 'kabco_delta'] if c in kabco_data.columns]
            if select_cols:
                data_list.append(kabco_data[select_cols])
        
        if 'cri' in df['mo'] and not df['mo']['cri'].empty:
            cri_data = df['mo']['cri'].copy()
            if 'delta' in cri_data.columns:
                cri_data = cri_data.rename(columns={'delta': 'cri_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'cri', 'cri_delta'] if c in cri_data.columns]
            if select_cols:
                data_list.append(cri_data[select_cols])
        
        if 'rsi' in df['mo'] and not df['mo']['rsi'].empty:
            rsi_data = df['mo']['rsi'].copy()
            if 'delta' in rsi_data.columns:
                rsi_data = rsi_data.rename(columns={'delta': 'rsi_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'rsi', 'rsi_delta'] if c in rsi_data.columns]
            if select_cols:
                data_list.append(rsi_data[select_cols])
        
        if 'bpsi' in df['mo'] and not df['mo']['bpsi'].empty:
            bpsi_data = df['mo']['bpsi'].copy()
            if 'delta' in bpsi_data.columns:
                bpsi_data = bpsi_data.rename(columns={'delta': 'bpsi_delta'})
            select_cols = [c for c in ['Zone_Group', 'Corridor', 'Month', 'bpsi', 'bpsi_delta'] if c in bpsi_data.columns]
            if select_cols:
                data_list.append(bpsi_data[select_cols])
        
        # Merge all data - use a more memory-efficient approach
        if data_list:
            # Filter out empty dataframes
            data_list = [d for d in data_list if not d.empty]
            
            if not data_list:
                return pd.DataFrame()
            
            # Start with first dataframe
            result = data_list[0].copy()
            
            # Merge remaining dataframes one at a time
            # Use inner join to reduce memory usage, or limit to essential columns
            merge_keys = ['Zone_Group', 'Corridor', 'Month']
            # Verify merge keys exist in all dataframes
            valid_keys = [key for key in merge_keys if all(key in df.columns for df in data_list)]
            
            if not valid_keys:
                # If no common keys, just concatenate (this shouldn't happen normally)
                logger.warning("No common merge keys found, returning first dataframe")
                return result
            
            # Merge remaining dataframes
            for data in data_list[1:]:
                if not data.empty and all(key in data.columns for key in valid_keys):
                    try:
                        # Select only essential columns to reduce memory
                        data_cols = valid_keys + [col for col in data.columns if col not in valid_keys and col not in result.columns]
                        data_subset = data[data_cols] if len(data_cols) <= 20 else data[valid_keys + list(data.columns.difference(valid_keys))[:10]]
                        result = pd.merge(result, data_subset, on=valid_keys, how='outer', suffixes=('', '_dup'))
                        # Drop duplicate columns
                        dup_cols = [col for col in result.columns if col.endswith('_dup')]
                        if dup_cols:
                            result = result.drop(columns=dup_cols)
                    except MemoryError:
                        logger.error(f"Memory error during merge, returning partial result")
                        break
        else:
            result = pd.DataFrame()
        
        if not result.empty:
            # Filter out rows where Zone_Group == Corridor (if both columns exist)
            if 'Zone_Group' in result.columns and 'Corridor' in result.columns:
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
        if df.empty or 'Percent_Health' not in df.columns:
            return df
        
        # Check which columns exist - handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
        zone_group_col = None
        for col in ['Zone Group', 'Zone_Group']:
            if col in df.columns:
                zone_group_col = col
                break
        
        # Build groupby columns based on what exists
        corridor_groupby = []
        if zone_group_col:
            corridor_groupby.append(zone_group_col)
        if 'Zone' in df.columns:
            corridor_groupby.append('Zone')
        if 'Corridor' in df.columns:
            corridor_groupby.append('Corridor')
        if 'Month' in df.columns:
            corridor_groupby.append('Month')
        
        if not corridor_groupby:
            return df
        
        # Corridor subtotals
        corridor_subtotals = df.groupby(corridor_groupby).agg({
            'Percent_Health': 'mean'
        }).reset_index()
        
        # Zone subtotals (if Zone column exists)
        if 'Zone' in corridor_subtotals.columns:
            zone_groupby = [col for col in corridor_groupby if col != 'Corridor']
            if zone_groupby:
                zone_subtotals = corridor_subtotals.groupby(zone_groupby).agg({
                    'Percent_Health': 'mean'
                }).reset_index()
            else:
                zone_subtotals = pd.DataFrame()
        else:
            zone_subtotals = pd.DataFrame()
        
        # Combine all data
        dataframes_to_concat = [df, corridor_subtotals]
        if not zone_subtotals.empty:
            dataframes_to_concat.append(zone_subtotals)
        combined = pd.concat(dataframes_to_concat, ignore_index=True)
        
        # Fill missing values (only if columns exist)
        if 'Zone' in combined.columns:
            combined = combined.groupby('Zone').apply(lambda x: x.fillna(method='ffill')).reset_index(drop=True)
        if 'Corridor' in combined.columns:
            combined = combined.groupby('Corridor').apply(lambda x: x.fillna(method='ffill')).reset_index(drop=True)
        
        # Convert to factors and arrange
        if zone_group_col and zone_group_col in combined.columns:
            combined[zone_group_col] = pd.Categorical(combined[zone_group_col])
        if 'Zone' in combined.columns:
            combined['Zone'] = pd.Categorical(combined['Zone'])
        
        # Build sort columns
        sort_cols = []
        if zone_group_col and zone_group_col in combined.columns:
            sort_cols.append(zone_group_col)
        if 'Zone' in combined.columns:
            sort_cols.append('Zone')
        if 'Corridor' in combined.columns:
            sort_cols.append('Corridor')
        if 'Subcorridor' in combined.columns:
            sort_cols.append('Subcorridor')
        if 'Month' in combined.columns:
            sort_cols.append('Month')
        
        if sort_cols:
            combined = combined.sort_values(sort_cols)
        
        # Fill Corridor and Subcorridor
        if 'Corridor' in combined.columns and 'Zone' in combined.columns:
            combined['Corridor'] = combined['Corridor'].fillna(combined['Zone'])
        if 'Subcorridor' in combined.columns and 'Corridor' in combined.columns:
            combined['Subcorridor'] = combined['Subcorridor'].fillna(combined['Corridor'])
        
        if 'Corridor' in combined.columns:
            combined['Corridor'] = pd.Categorical(combined['Corridor'])
        if 'Subcorridor' in combined.columns:
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
        if df.empty:
            return pd.DataFrame()
        
        # Select maintenance-related columns
        maintenance_cols = [col for col in df.columns if any(metric in col for metric in [
            'Detection', 'Ped_Act', 'Comm', 'CCTV', 'Flash_Events'
        ])]
        
        # Only select base columns that exist in the DataFrame
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        available_base_cols = [col for col in base_cols if col in df.columns]
        
        # If no base columns exist, return empty DataFrame
        if not available_base_cols:
            logger.warning("No required base columns found in health data")
            return pd.DataFrame()
        
        # Select only columns that exist
        all_cols = available_base_cols + maintenance_cols
        health = df[all_cols].copy()
        
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
        
        # Group by and calculate means - only use columns that exist
        groupby_cols = [col for col in ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month'] if col in health.columns]
        if not groupby_cols:
            logger.warning("No groupby columns available for health maintenance")
            return pd.DataFrame()
        
        health = health.groupby(groupby_cols).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data'] + groupby_cols}
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
        if df.empty:
            return pd.DataFrame()
        
        # Select operations-related columns
        operations_cols = [col for col in df.columns if any(metric in col for metric in [
            'Platoon_Ratio', 'Ped_Delay', 'Split_Failures', 'TTI', 'BI'
        ])]
        
        # Only select base columns that exist in the DataFrame
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        available_base_cols = [col for col in base_cols if col in df.columns]
        
        # If no base columns exist, return empty DataFrame
        if not available_base_cols:
            logger.warning("No required base columns found in health data")
            return pd.DataFrame()
        
        # Select only columns that exist
        all_cols = available_base_cols + operations_cols
        health = df[all_cols].copy()
        
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
        
        # Group by and calculate means - only use columns that exist
        groupby_cols = [col for col in ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month'] if col in health.columns]
        if not groupby_cols:
            logger.warning("No groupby columns available for health operations")
            return pd.DataFrame()
        
        health = health.groupby(groupby_cols).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data'] + groupby_cols}
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
        if df.empty:
            return pd.DataFrame()
        
        # Select safety-related columns
        safety_cols = [col for col in df.columns if any(metric in col for metric in [
            'Crash_Rate', 'KABCO', 'High_Speed', 'Ped_Injury'
        ])]
        
        # Only select base columns that exist in the DataFrame
        base_cols = ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month', 'Context', 'Context_Category']
        available_base_cols = [col for col in base_cols if col in df.columns]
        
        # If no base columns exist, return empty DataFrame
        if not available_base_cols:
            logger.warning("No required base columns found in health data")
            return pd.DataFrame()
        
        # Select only columns that exist
        all_cols = available_base_cols + safety_cols
        health = df[all_cols].copy()
        
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
        
        # Group by and calculate means - only use columns that exist
        groupby_cols = [col for col in ['Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 'Month'] if col in health.columns]
        if not groupby_cols:
            logger.warning("No groupby columns available for health safety")
            return pd.DataFrame()
        
        health = health.groupby(groupby_cols).agg({
            'Percent_Health': 'mean',
            'Missing_Data': 'mean',
            **{col: 'first' for col in health.columns if col not in ['Percent_Health', 'Missing_Data'] + groupby_cols}
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
        if df.empty:
            return pd.DataFrame()
        
        plot_df = df.copy()
        
        # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
        zone_group_col = None
        for col in ['Zone Group', 'Zone_Group']:
            if col in plot_df.columns:
                zone_group_col = col
                break
        
        # Rename columns for subcorridor plotting
        if zone_group_col and 'Corridor' in plot_df.columns and 'Subcorridor' in plot_df.columns:
            plot_df = plot_df.rename(columns={
                zone_group_col: 'Zone_Group',
                'Corridor': 'Zone_Group_New',
                'Subcorridor': 'Corridor'
            })
            plot_df['Zone_Group'] = plot_df['Zone_Group_New']
            plot_df = plot_df.drop('Zone_Group_New', axis=1, errors='ignore')
        elif zone_group_col and zone_group_col != 'Zone_Group':
            # Just rename to Zone_Group if needed
            plot_df = plot_df.rename(columns={zone_group_col: 'Zone_Group'})
        
        # Remove Zone Group and Zone columns if they exist
        plot_df = plot_df.drop(['Zone'], axis=1, errors='ignore')
        
        # Check required columns exist before processing
        if 'Zone_Group' not in plot_df.columns or 'Corridor' not in plot_df.columns:
            logger.warning("Required columns (Zone_Group, Corridor) not found for plotting")
            return df
        
        # Convert to categorical
        plot_df['Zone_Group'] = pd.Categorical(plot_df['Zone_Group'])
        plot_df['Corridor'] = pd.Categorical(plot_df['Corridor'])
        
        # Sort data - only use columns that exist
        sort_cols = ['Zone_Group', 'Corridor']
        if 'Month' in plot_df.columns:
            sort_cols.append('Month')
        plot_df = plot_df.sort_values(sort_cols)
        
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
        if df.empty:
            return pd.DataFrame()
        
        plot_df = df.copy()
        
        # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
        zone_group_col = None
        for col in ['Zone Group', 'Zone_Group']:
            if col in plot_df.columns:
                zone_group_col = col
                break
        
        # Remove Zone Group column if it exists (with space)
        plot_df = plot_df.drop(['Zone Group'], axis=1, errors='ignore')
        
        # Rename Zone to Zone_Group if Zone exists and Zone_Group doesn't
        if 'Zone' in plot_df.columns and 'Zone_Group' not in plot_df.columns:
            plot_df = plot_df.rename(columns={'Zone': 'Zone_Group'})
        elif zone_group_col and zone_group_col != 'Zone_Group':
            # Rename the existing zone group column to Zone_Group
            plot_df = plot_df.rename(columns={zone_group_col: 'Zone_Group'})
        
        # Check required columns exist before processing
        if 'Zone_Group' not in plot_df.columns or 'Corridor' not in plot_df.columns:
            logger.warning("Required columns (Zone_Group, Corridor) not found for corridor plotting")
            return df
        
        # Convert to categorical
        plot_df['Zone_Group'] = pd.Categorical(plot_df['Zone_Group'])
        plot_df['Corridor'] = pd.Categorical(plot_df['Corridor'])
        
        # Sort data - only use columns that exist
        sort_cols = ['Zone_Group', 'Corridor']
        if 'Month' in plot_df.columns:
            sort_cols.append('Month')
        plot_df = plot_df.sort_values(sort_cols)
        
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
        if df.empty or corridors.empty:
            return df
        
        # Check if required columns exist in corridors
        required_corridor_cols = ['Zone', 'Corridor', 'Subcorridor', 'SignalID']
        available_corridor_cols = [col for col in required_corridor_cols if col in corridors.columns]
        
        if not available_corridor_cols or 'SignalID' not in available_corridor_cols:
            logger.warning("Required columns not found in corridors DataFrame")
            return df
        
        # Find the column to rename to SignalID
        # Could be 'Subcorridor', 'SignalID', or another identifier
        signal_id_col = None
        for col in ['Subcorridor', 'SignalID', 'Signal', 'CameraID']:
            if col in df.columns:
                signal_id_col = col
                break
        
        if not signal_id_col:
            logger.warning("No signal identifier column found in DataFrame")
            return df
        
        # Rename the identifier column to SignalID for merging
        signal_df = df.copy()
        if signal_id_col != 'SignalID':
            signal_df = signal_df.rename(columns={signal_id_col: 'SignalID'})
        
        # Check merge keys exist
        merge_keys = []
        for key in ['Zone', 'Corridor', 'SignalID']:
            if key in signal_df.columns and key in corridors.columns:
                merge_keys.append(key)
        
        if not merge_keys:
            logger.warning("No common merge keys found between signal data and corridors")
            return df
        
        # Merge with corridors data
        result = pd.merge(
            signal_df,
            corridors[available_corridor_cols],
            on=merge_keys,
            how='left'
        )
        
        # Reorder columns to put Subcorridor after Corridor if it exists
        cols = result.columns.tolist()
        if 'Subcorridor' in cols and 'Corridor' in cols:
            corridor_idx = cols.index('Corridor')
            cols.remove('Subcorridor')
            cols.insert(corridor_idx + 1, 'Subcorridor')
            result = result[cols]
        
        # Filter out rows with missing subcorridor (only if Subcorridor column exists)
        if 'Subcorridor' in result.columns:
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
                         corridors: pd.DataFrame, conf: Optional[dict] = None) -> Dict[str, Dict[str, pd.DataFrame]]:
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
        
        # Get summary data (will use Athena if conf provided and data is in tables)
        csd = get_summary_data(sub_data, conf=conf)
        ssd = get_summary_data(sig_data, conf=conf)
        
        # Process signal summary data
        if not ssd.empty and not cam_config.empty:
            # Check if required columns exist
            if 'SignalID' in cam_config.columns and 'CameraID' in cam_config.columns and 'Corridor' in ssd.columns:
                # Merge with camera config to get SignalID for CameraID
                ssd = pd.merge(
                    ssd,
                    cam_config[['SignalID', 'CameraID']],
                    left_on='Corridor',
                    right_on='CameraID',
                    how='left'
                )
                
                # Use SignalID where available, otherwise keep original Corridor
                if 'SignalID' in ssd.columns:
                    ssd['Corridor'] = ssd['SignalID'].fillna(ssd['Corridor'])
                    ssd = ssd.drop('SignalID', axis=1)
            
            # Filter out camera-only entries
            if 'Corridor' in ssd.columns:
                ssd = ssd[~ssd['Corridor'].astype(str).str.contains('CAM', na=False)]
            
            # Group by and fill missing values - check columns exist
            groupby_cols = [col for col in ['Corridor', 'Zone_Group', 'Month'] if col in ssd.columns]
            if groupby_cols:
                ssd = ssd.groupby(groupby_cols).apply(
                    lambda x: x.fillna(method='ffill').fillna(method='bfill')
                ).reset_index(drop=True)
                
                # Take first row per group
                ssd = ssd.groupby(groupby_cols).first().reset_index()
            
            # Replace infinite values with NaN
            numeric_cols = ssd.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                ssd[numeric_cols] = ssd[numeric_cols].replace([np.inf, -np.inf], np.nan)
        
        # Create health input dataframes
        if not corridor_groupings.empty:
            # Check if required columns exist for subcorridor merge
            sub_merge_keys = [col for col in ['Corridor', 'Subcorridor'] if col in csd.columns and col in corridor_groupings.columns]
            if sub_merge_keys:
                # Subcorridor health data
                sub_health_data = pd.merge(
                    csd,
                    corridor_groupings,
                    on=sub_merge_keys,
                    how='inner'
                )
            else:
                sub_health_data = csd
            
            # Signal health data - check columns exist before renaming
            sig_health_data = ssd.copy()
            rename_map = {}
            if 'Corridor' in sig_health_data.columns:
                rename_map['Corridor'] = 'SignalID'
            # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
            zone_group_col = None
            for col in ['Zone Group', 'Zone_Group']:
                if col in sig_health_data.columns:
                    zone_group_col = col
                    break
            if zone_group_col:
                rename_map[zone_group_col] = 'Corridor'
            
            if rename_map:
                sig_health_data = sig_health_data.rename(columns=rename_map)
            
            # Merge with corridors - check if SignalID exists
            if 'SignalID' in sig_health_data.columns and 'Corridor' in sig_health_data.columns:
                # Check what columns exist in corridors
                available_corridor_cols = [col for col in ['Corridor', 'SignalID', 'Subcorridor'] if col in corridors.columns]
                if available_corridor_cols:
                    merge_keys = [col for col in ['Corridor', 'SignalID'] if col in sig_health_data.columns and col in corridors.columns]
                    if merge_keys:
                        sig_health_data = pd.merge(
                            sig_health_data,
                            corridors[available_corridor_cols],
                            on=merge_keys,
                            how='left'
                        )
            
            # Merge with corridor groupings if Subcorridor exists
            if 'Subcorridor' in sig_health_data.columns:
                sig_merge_keys = [col for col in ['Corridor', 'Subcorridor'] if col in sig_health_data.columns and col in corridor_groupings.columns]
                if sig_merge_keys:
                    sig_health_data = pd.merge(
                        sig_health_data,
                        corridor_groupings,
                        on=sig_merge_keys,
                        how='inner'
                    )
            
            # Add TTI/PTI from subcorridor data for contexts 1-4
            if not sub_health_data.empty:
                tti_pti_cols = [col for col in ['Corridor', 'Subcorridor', 'Month', 'tti', 'pti'] if col in sub_health_data.columns]
                if len(tti_pti_cols) >= 3:  # At least Corridor, Subcorridor, Month
                    tti_pti_data = sub_health_data[tti_pti_cols]
                    
                    merge_cols = [col for col in ['Corridor', 'Subcorridor', 'Month'] if col in sig_health_data.columns and col in tti_pti_data.columns]
                    if merge_cols:
                        sig_health_data = pd.merge(
                            sig_health_data,
                            tti_pti_data,
                            on=merge_cols,
                            how='left',
                            suffixes=('_x', '_y')
                        )
                        
                        # Use subcorridor TTI/PTI for contexts 1-4 if Context column exists
                        if 'Context' in sig_health_data.columns:
                            context_mask = sig_health_data['Context'].isin([1, 2, 3, 4])
                            if 'tti_x' in sig_health_data.columns and 'tti_y' in sig_health_data.columns:
                                sig_health_data.loc[context_mask, 'tti_x'] = sig_health_data.loc[context_mask, 'tti_y']
                            if 'pti_x' in sig_health_data.columns and 'pti_y' in sig_health_data.columns:
                                sig_health_data.loc[context_mask, 'pti_x'] = sig_health_data.loc[context_mask, 'pti_y']
            
            # Clean up columns
            sig_health_data = sig_health_data.drop(['Subcorridor', 'tti_y', 'pti_y'], axis=1, errors='ignore')
            if 'SignalID' in sig_health_data.columns:
                rename_final = {'SignalID': 'Subcorridor'}
                if 'tti_x' in sig_health_data.columns:
                    rename_final['tti_x'] = 'tti'
                if 'pti_x' in sig_health_data.columns:
                    rename_final['pti_x'] = 'pti'
                if rename_final:
                    sig_health_data = sig_health_data.rename(columns=rename_final)
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
        if maintenance_sub.empty:
            maintenance_cor = pd.DataFrame()
        else:
            maintenance_cor = maintenance_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
            # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
            groupby_cols = []
            for col in ['Zone Group', 'Zone_Group']:
                if col in maintenance_cor.columns:
                    groupby_cols.append(col)
                    break
            if 'Zone' in maintenance_cor.columns:
                groupby_cols.append('Zone')
            if 'Corridor' in maintenance_cor.columns:
                groupby_cols.append('Corridor')
            if 'Month' in maintenance_cor.columns:
                groupby_cols.append('Month')
            
            if groupby_cols:
                maintenance_cor = maintenance_cor.groupby(groupby_cols).agg(
                    lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
                ).reset_index()
            else:
                maintenance_cor = pd.DataFrame()
        
        # Operations health
        operations_sub = get_health_operations(health_all_sub)
        operations_sig = get_health_operations(health_all_sig)
        
        # Corridor operations (aggregated from subcorridor)
        if operations_sub.empty:
            operations_cor = pd.DataFrame()
        else:
            operations_cor = operations_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
            # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
            groupby_cols = []
            for col in ['Zone Group', 'Zone_Group']:
                if col in operations_cor.columns:
                    groupby_cols.append(col)
                    break
            if 'Zone' in operations_cor.columns:
                groupby_cols.append('Zone')
            if 'Corridor' in operations_cor.columns:
                groupby_cols.append('Corridor')
            if 'Month' in operations_cor.columns:
                groupby_cols.append('Month')
            
            if groupby_cols:
                operations_cor = operations_cor.groupby(groupby_cols).agg(
                    lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
                ).reset_index()
            else:
                operations_cor = pd.DataFrame()
        
        # Safety health
        safety_sub = get_health_safety(health_all_sub)
        safety_sig = get_health_safety(health_all_sig)
        
        # Corridor safety (aggregated from subcorridor)
        if safety_sub.empty:
            safety_cor = pd.DataFrame()
        else:
            safety_cor = safety_sub.drop(['Subcorridor', 'Context'], axis=1, errors='ignore')
            # Handle both 'Zone Group' (with space) and 'Zone_Group' (with underscore)
            groupby_cols = []
            for col in ['Zone Group', 'Zone_Group']:
                if col in safety_cor.columns:
                    groupby_cols.append(col)
                    break
            if 'Zone' in safety_cor.columns:
                groupby_cols.append('Zone')
            if 'Corridor' in safety_cor.columns:
                groupby_cols.append('Corridor')
            if 'Month' in safety_cor.columns:
                groupby_cols.append('Month')
            
            if groupby_cols:
                safety_cor = safety_cor.groupby(groupby_cols).agg(
                    lambda x: x.mean() if x.dtype in ['float64', 'int64'] else x.first()
                ).reset_index()
            else:
                safety_cor = pd.DataFrame()
        
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



