"""
Aggregations Module

Python conversion of Aggregations.R
Contains functions for aggregating traffic signal data by various time periods and spatial groupings.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
from typing import Optional, Union, List, Dict, Any
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def weighted_mean_by_corridor(df: pd.DataFrame, 
                             period_col: str, 
                             corridors: pd.DataFrame, 
                             variable: str, 
                             weight_col: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate weighted mean by corridor for a given variable and time period
    
    Args:
        df: DataFrame with signal-level data
        period_col: Name of the time period column (e.g., 'Date', 'Month', 'Hour')
        corridors: DataFrame with corridor mappings
        variable: Name of the variable to aggregate
        weight_col: Name of the weight column (optional)
    
    Returns:
        DataFrame with corridor-level aggregated data
    """
    
    if df.empty or corridors.empty:
        return pd.DataFrame()
    
    # Join with corridors
    merged_df = df.merge(corridors, on='SignalID', how='left')
    merged_df = merged_df[merged_df['Corridor'].notna()]
    
    # Group by corridor and period
    group_cols = ['Zone_Group', 'Zone', 'Corridor', period_col]
    grouped = merged_df.groupby(group_cols)
    
    if weight_col is None:
        # Simple mean
        result = grouped[variable].mean().reset_index()
        result['ones'] = 1  # Add dummy weight column
    else:
        # Weighted mean
        def weighted_mean(group):
            weights = group[weight_col]
            values = group[variable]
            valid_mask = ~(np.isnan(values) | np.isnan(weights))
            
            if valid_mask.sum() == 0:
                return np.nan
            
            return np.average(values[valid_mask], weights=weights[valid_mask])
        
        result = grouped.apply(lambda x: pd.Series({
            variable: weighted_mean(x),
            weight_col: x[weight_col].sum()
        })).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(group_cols[:-1] + [period_col])
    result['lag_'] = result.groupby(group_cols[:-1])[variable].shift(1)
    result['delta'] = (result[variable] - result['lag_']) / result['lag_']
    
    # Clean up
    result = result.drop('lag_', axis=1)
    
    return result

def group_corridor_by_period(df: pd.DataFrame, 
                           period_col: str, 
                           variable: str, 
                           weight_col: str, 
                           corridor_group: str) -> pd.DataFrame:
    """
    Group corridor data by time period
    
    Args:
        df: DataFrame with corridor data
        period_col: Name of period column
        variable: Variable to aggregate
        weight_col: Weight column for weighted average
        corridor_group: Name for the corridor group
    
    Returns:
        Aggregated DataFrame
    """
    
    if df.empty:
        return pd.DataFrame()
    
    grouped = df.groupby(period_col).apply(
        lambda x: pd.Series({
            variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    grouped['Corridor'] = corridor_group
    grouped['Zone_Group'] = corridor_group
    
    # Calculate lag and delta
    grouped = grouped.sort_values(period_col)
    grouped['lag_'] = grouped[variable].shift(1)
    grouped['delta'] = (grouped[variable] - grouped['lag_']) / grouped['lag_']
    grouped = grouped.drop('lag_', axis=1)
    
    return grouped

def group_corridors(df: pd.DataFrame, 
                   period_col: str, 
                   variable: str, 
                   weight_col: str) -> pd.DataFrame:
    """
    Create aggregated data for various zone groupings (All RTOP, RTOP1, RTOP2, Zone 7, etc.)
    
    Args:
        df: DataFrame with corridor-level data
        period_col: Period column name
        variable: Variable to aggregate
        weight_col: Weight column
    
    Returns:
        Combined DataFrame with all groupings
    """
    
    if df.empty:
        return pd.DataFrame()
    
    # Original corridor data
    corridor_data = df.copy()
    corridor_data['Zone_Group'] = corridor_data['Zone']
    
    all_results = [corridor_data]
    
    # Group by Zone
    zones = df['Zone'].unique()
    for zone in zones:
        zone_data = df[df['Zone'] == zone].copy()
        if not zone_data.empty:
            zone_agg = group_corridor_by_period(zone_data, period_col, variable, weight_col, zone)
            all_results.append(zone_agg)
    
    # All RTOP (RTOP1 and RTOP2 combined)
    rtop_data = df[df['Zone_Group'].isin(['RTOP1', 'RTOP2'])].copy()
    if not rtop_data.empty:
        rtop_agg = group_corridor_by_period(rtop_data, period_col, variable, weight_col, 'All RTOP')
        all_results.append(rtop_agg)
    
    # RTOP1 only
    rtop1_data = df[df['Zone_Group'] == 'RTOP1'].copy()
    if not rtop1_data.empty:
        rtop1_agg = group_corridor_by_period(rtop1_data, period_col, variable, weight_col, 'RTOP1')
        all_results.append(rtop1_agg)
    
    # RTOP2 only
    rtop2_data = df[df['Zone_Group'] == 'RTOP2'].copy()
    if not rtop2_data.empty:
        rtop2_agg = group_corridor_by_period(rtop2_data, period_col, variable, weight_col, 'RTOP2')
        all_results.append(rtop2_agg)
    
    # Zone 7 combined (Zone 7m and Zone 7d)
    zone7_data = df[df['Zone'].isin(['Zone 7m', 'Zone 7d'])].copy()
    if not zone7_data.empty:
        zone7_agg = group_corridor_by_period(zone7_data, period_col, variable, weight_col, 'Zone 7')
        all_results.append(zone7_agg)
    
    # Combine all results
    if all_results:
        final_result = pd.concat(all_results, ignore_index=True)
        final_result = final_result.drop_duplicates()
        return final_result
    else:
        return pd.DataFrame()

def get_hourly(df: pd.DataFrame, variable: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare hourly data with corridor information
    
    Args:
        df: DataFrame with hourly signal data
        variable: Variable name to process
        corridors: Corridor mapping DataFrame
    
    Returns:
        DataFrame with hourly data and corridor info
    """
    
    if df.empty:
        return pd.DataFrame()
    
    # Join with corridors
    result = df.merge(corridors, on='SignalID', how='inner')
    result = result[result['Corridor'].notna()]
    
    # Calculate lag and delta by SignalID
    result = result.sort_values(['SignalID', 'Hour'])
    result['lag_'] = result.groupby('SignalID')[variable].shift(1)
    result['delta'] = (result[variable] - result['lag_']) / result['lag_']
    result = result.drop('lag_', axis=1)
    
    # Select and order columns
    columns = ['SignalID', 'Hour', variable, 'delta', 'Zone_Group', 'Zone', 'Corridor']
    if 'Subcorridor' in result.columns:
        columns.append('Subcorridor')
    
    return result[columns]

def get_period_avg(df: pd.DataFrame, 
                  variable: str, 
                  period_col: str, 
                  weight_col: str = 'ones') -> pd.DataFrame:
    """
    Calculate period averages by SignalID
    
    Args:
        df: Input DataFrame
        variable: Variable to average
        period_col: Period column name
        weight_col: Weight column (default 'ones' for simple average)
    
    Returns:
        DataFrame with period averages
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if weight_col == 'ones':
        df = df.copy()
        df['ones'] = 1
        weight_col = 'ones'
    
    def weighted_mean(group):
        weights = group[weight_col]
        values = group[variable]
        valid_mask = ~(np.isnan(values) | np.isnan(weights))
        
        if valid_mask.sum() == 0:
            return np.nan
        
        return np.average(values[valid_mask], weights=weights[valid_mask])
    
    # Group by SignalID and period
    grouped = df.groupby(['SignalID', period_col]).apply(
        lambda x: pd.Series({
            variable: weighted_mean(x),
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    # Calculate delta
    grouped = grouped.sort_values(['SignalID', period_col])
    grouped['lag_'] = grouped.groupby('SignalID')[variable].shift(1)
    grouped['delta'] = (grouped[variable] - grouped['lag_']) / grouped['lag_']
    grouped = grouped.drop('lag_', axis=1)
    
    return grouped

def get_period_sum(df: pd.DataFrame, variable: str, period_col: str) -> pd.DataFrame:
    """
    Calculate period sums by SignalID
    
    Args:
        df: Input DataFrame
        variable: Variable to sum
        period_col: Period column name
    
    Returns:
        DataFrame with period sums
    """
    
    if df.empty:
        return pd.DataFrame()
    
    # Group by SignalID and period
    grouped = df.groupby(['SignalID', period_col])[variable].sum().reset_index()
    
    # Calculate delta
    grouped = grouped.sort_values(['SignalID', period_col])
    grouped['lag_'] = grouped.groupby('SignalID')[variable].shift(1)
    grouped['delta'] = (grouped[variable] - grouped['lag_']) / grouped['lag_']
    grouped = grouped.drop('lag_', axis=1)
    
    return grouped

def get_daily_avg(df: pd.DataFrame, 
                 variable: str, 
                 weight_col: str = 'ones', 
                 peak_only: bool = False,
                 am_peak_hours: List[int] = None,
                 pm_peak_hours: List[int] = None) -> pd.DataFrame:
    """
    Calculate daily averages from hourly data
    
    Args:
        df: DataFrame with hourly data including 'Date_Hour' column
        variable: Variable to average
        weight_col: Weight column for weighted average
        peak_only: If True, only use peak hours
        am_peak_hours: List of AM peak hours (default [6,7,8,9])
        pm_peak_hours: List of PM peak hours (default [16,17,18,19])
    
    Returns:
        DataFrame with daily averages
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if am_peak_hours is None:
        am_peak_hours = [6, 7, 8, 9]
    if pm_peak_hours is None:
        pm_peak_hours = [16, 17, 18, 19]
    
    df = df.copy()
    
    if weight_col == 'ones':
        df['ones'] = 1
        weight_col = 'ones'
    
    # Extract date from Date_Hour
    df['Date'] = pd.to_datetime(df['Date_Hour']).dt.date
    
    # Filter to peak hours if requested
    if peak_only:
        df['hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        peak_hours = am_peak_hours + pm_peak_hours
        df = df[df['hour'].isin(peak_hours)]
    
    # Calculate weighted daily averages
    def weighted_mean(group):
        weights = group[weight_col]
        values = group[variable]
        valid_mask = ~(np.isnan(values) | np.isnan(weights))
        
        if valid_mask.sum() == 0:
            return np.nan
        
        return np.average(values[valid_mask], weights=weights[valid_mask])
    
    grouped = df.groupby(['SignalID', 'Date']).apply(
        lambda x: pd.Series({
            variable: weighted_mean(x),
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    # Calculate delta
    grouped = grouped.sort_values(['SignalID', 'Date'])
    grouped['lag_'] = grouped.groupby('SignalID')[variable].shift(1)
    grouped['delta'] = (grouped[variable] - grouped['lag_']) / grouped['lag_']
    grouped = grouped.drop('lag_', axis=1)
    
    return grouped

def get_daily_sum(df: pd.DataFrame, variable: str) -> pd.DataFrame:
    """
    Calculate daily sums from hourly data
    
    Args:
        df: DataFrame with hourly data
        variable: Variable to sum
    
    Returns:
        DataFrame with daily sums
    """
    
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date_Hour']).dt.date
    
    # Group by SignalID and Date
    grouped = df.groupby(['SignalID', 'Date'])[variable].sum().reset_index()
    
    # Calculate delta
    grouped = grouped.sort_values(['SignalID', 'Date'])
    grouped['lag_'] = grouped.groupby('SignalID')[variable].shift(1)
    grouped['delta'] = (grouped[variable] - grouped['lag_']) / grouped['lag_']
    grouped = grouped.drop('lag_', axis=1)
    
    return grouped

def get_tuesdays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get Tuesday dates for week identification
    
    Args:
        df: DataFrame with Date column
    
    Returns:
        DataFrame mapping weeks to Tuesday dates
    """
    
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df['Date'] = pd.to_datetime(df['Date'])
    df['Week'] = df['Date'].dt.isocalendar().week
    df['Year'] = df['Date'].dt.year
    
    # Find Tuesday of each week
    tuesdays = []
    for (year, week), group in df.groupby(['Year', 'Week']):
        # Find the Tuesday (weekday 1) of this week
        week_dates = group['Date'].dt.date.unique()
        tuesday_date = None
        
        for date in week_dates:
            weekday = pd.to_datetime(date).weekday()
            if weekday == 1:  # Tuesday
                tuesday_date = date
                break
        
        # If no Tuesday found, use the first date and adjust
        if tuesday_date is None and len(week_dates) > 0:
            first_date = min(week_dates)
            first_weekday = pd.to_datetime(first_date).weekday()
            days_to_tuesday = (1 - first_weekday) % 7
            tuesday_date = first_date + timedelta(days=days_to_tuesday)
        
        if tuesday_date:
            tuesdays.append({'Week': week, 'Date': tuesday_date})
    
    return pd.DataFrame(tuesdays)

def get_weekly_avg_by_day(df: pd.DataFrame, 
                         variable: str, 
                         weight_col: str = 'ones', 
                         peak_only: bool = True,
                         am_peak_hours: List[int] = None,
                         pm_peak_hours: List[int] = None) -> pd.DataFrame:
    """
    Calculate weekly averages aggregated to representative day (Tuesday)
    
    Args:
        df: DataFrame with hourly data
        variable: Variable to average
        weight_col: Weight column
        peak_only: Whether to use only peak hours
        am_peak_hours: AM peak hours
        pm_peak_hours: PM peak hours
    
    Returns:
        DataFrame with weekly averages by day
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if am_peak_hours is None:
        am_peak_hours = [6, 7, 8, 9]
    if pm_peak_hours is None:
        pm_peak_hours = [16, 17, 18, 19]
    
    df = df.copy()
    
    if weight_col == 'ones':
        df['ones'] = 1
        weight_col = 'ones'
    
    # Extract date and week information
    df['Date'] = pd.to_datetime(df['Date_Hour']).dt.date
    df['Week'] = pd.to_datetime(df['Date_Hour']).dt.isocalendar().week
    
    # Get Tuesday mapping
    tuesdays = get_tuesdays(df)
    
    # Filter to peak hours if requested
    if peak_only:
        df['hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        peak_hours = am_peak_hours + pm_peak_hours
        df = df[df['hour'].isin(peak_hours)]
    
    # Create complete week/phase combinations
    weeks = df['Week'].unique()
    signals = df['SignalID'].unique()
    phases = df['CallPhase'].unique() if 'CallPhase' in df.columns else [0]
    
    complete_combinations = []
    for week in weeks:
        for signal in signals:
            for phase in phases:
                complete_combinations.append({
                    'SignalID': signal,
                    'CallPhase': phase,
                    'Week': week
                })
    
    complete_df = pd.DataFrame(complete_combinations)
    df_with_complete = complete_df.merge(df, on=['SignalID', 'CallPhase', 'Week'], how='left')
    
    # Calculate weekly averages by phase
    def weighted_mean(group):
        weights = group[weight_col].dropna()
        values = group[variable].dropna()
        
        if len(weights) == 0 or len(values) == 0:
            return np.nan, 0
        
        # Align weights and values
        valid_indices = group[variable].notna() & group[weight_col].notna()
        if valid_indices.sum() == 0:
            return np.nan, 0
        
        valid_weights = group.loc[valid_indices, weight_col]
        valid_values = group.loc[valid_indices, variable]
        
        if len(valid_weights) == 0:
            return np.nan, 0
        
        return np.average(valid_values, weights=valid_weights), valid_weights.sum()
    
    # Group by signal, phase, and week
    weekly_by_phase = df_with_complete.groupby(['SignalID', 'CallPhase', 'Week']).apply(
        lambda x: pd.Series({
            variable: weighted_mean(x)[0],
            weight_col: weighted_mean(x)[1]
        })
    ).reset_index()
    
    # Aggregate across phases for each signal and week
    weekly_by_signal = weekly_by_phase.groupby(['SignalID', 'Week']).apply(
        lambda x: pd.Series({
            variable: np.average(x[variable].dropna(), 
                               weights=x.loc[x[variable].notna(), weight_col]) if len(x[variable].dropna()) > 0 else np.nan,
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    # Add Tuesday dates
    weekly_by_signal = weekly_by_signal.merge(tuesdays, on='Week', how='left')
    
    # Calculate delta
    weekly_by_signal = weekly_by_signal.sort_values(['SignalID', 'Date'])
    weekly_by_signal['lag_'] = weekly_by_signal.groupby('SignalID')[variable].shift(1)
    weekly_by_signal['delta'] = (weekly_by_signal[variable] - weekly_by_signal['lag_']) / weekly_by_signal['lag_']
    weekly_by_signal = weekly_by_signal.drop('lag_', axis=1)
    
    return weekly_by_signal

def get_monthly_avg_by_day(df: pd.DataFrame, 
                          variable: str, 
                          weight_col: Optional[str] = None, 
                          peak_only: bool = False,
                          am_peak_hours: List[int] = None,
                          pm_peak_hours: List[int] = None) -> pd.DataFrame:
    """
    Calculate monthly averages from daily or hourly data
    
    Args:
        df: Input DataFrame
        variable: Variable to average
        weight_col: Weight column for weighted average
        peak_only: Whether to use only peak hours
        am_peak_hours: AM peak hours
        pm_peak_hours: PM peak hours
    
    Returns:
        DataFrame with monthly averages
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if am_peak_hours is None:
        am_peak_hours = [6, 7, 8, 9]
    if pm_peak_hours is None:
        pm_peak_hours = [16, 17, 18, 19]
    
    df = df.copy()
    
    # Filter to peak hours if requested and Date_Hour column exists
    if peak_only and 'Date_Hour' in df.columns:
        df['hour'] = pd.to_datetime(df['Date_Hour']).dt.hour
        peak_hours = am_peak_hours + pm_peak_hours
        df = df[df['hour'].isin(peak_hours)]
    
    # Create Month column (first day of month)
    if 'Date' in df.columns:
        df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
    elif 'Date_Hour' in df.columns:
        df['Month'] = pd.to_datetime(df['Date_Hour']).dt.to_period('M').dt.start_time
    else:
        raise ValueError("DataFrame must have either 'Date' or 'Date_Hour' column")
    
    current_month = df['Month'].max()
    
    # Create complete month combinations for signals and phases
    months = pd.date_range(df['Month'].min(), current_month, freq='MS')
    signals = df['SignalID'].unique()
    phases = df['CallPhase'].unique() if 'CallPhase' in df.columns else [0]
    
    complete_combinations = []
    for month in months:
        for signal in signals:
            for phase in phases:
                complete_combinations.append({
                    'SignalID': signal,
                    'CallPhase': phase,
                    'Month': month
                })
    
    complete_df = pd.DataFrame(complete_combinations)
    df_with_complete = complete_df.merge(df, on=['SignalID', 'CallPhase', 'Month'], how='left')
    
    if weight_col is None:
        # Simple mean
        monthly_by_phase = df_with_complete.groupby(['SignalID', 'Month', 'CallPhase'])[variable].mean().reset_index()
        monthly_by_signal = monthly_by_phase.groupby(['SignalID', 'Month'])[variable].sum().reset_index()
    else:
        # Weighted mean
        def weighted_mean_phases(group):
            weights = group[weight_col].dropna()
            values = group[variable].dropna()
            
            if len(weights) == 0 or len(values) == 0:
                return np.nan, 0
            
            # Align weights and values
            valid_indices = group[variable].notna() & group[weight_col].notna()
            if valid_indices.sum() == 0:
                return np.nan, 0
            
            valid_weights = group.loc[valid_indices, weight_col]
            valid_values = group.loc[valid_indices, variable]
            
            if len(valid_weights) == 0:
                return np.nan, 0
            
            return np.average(valid_values, weights=valid_weights), valid_weights.sum()
        
        # Calculate monthly averages by phase
        monthly_by_phase = df_with_complete.groupby(['SignalID', 'Month', 'CallPhase']).apply(
            lambda x: pd.Series({
                variable: weighted_mean_phases(x)[0],
                weight_col: weighted_mean_phases(x)[1]
            })
        ).reset_index()
        
        # Aggregate across phases
        monthly_by_signal = monthly_by_phase.groupby(['SignalID', 'Month']).apply(
            lambda x: pd.Series({
                variable: np.average(x[variable].dropna(), 
                                   weights=x.loc[x[variable].notna(), weight_col]) if len(x[variable].dropna()) > 0 else np.nan,
                weight_col: x[weight_col].sum()
            })
        ).reset_index()
    
    # Calculate delta
    monthly_by_signal = monthly_by_signal.sort_values(['SignalID', 'Month'])
    monthly_by_signal['lag_'] = monthly_by_signal.groupby('SignalID')[variable].shift(1)
    monthly_by_signal['delta'] = (monthly_by_signal[variable] - monthly_by_signal['lag_']) / monthly_by_signal['lag_']
    monthly_by_signal = monthly_by_signal.drop('lag_', axis=1)
    
    # Filter out rows where Month is NaN
    monthly_by_signal = monthly_by_signal[monthly_by_signal['Month'].notna()]
    
    return monthly_by_signal

def get_cor_weekly_avg_by_day(df: pd.DataFrame, 
                             corridors: pd.DataFrame, 
                             variable: str, 
                             weight_col: str = 'ones') -> pd.DataFrame:
    """
    Calculate corridor-level weekly averages
    
    Args:
        df: DataFrame with signal-level weekly data
        corridors: Corridor mapping DataFrame
        variable: Variable to aggregate
        weight_col: Weight column
    
    Returns:
        DataFrame with corridor-level weekly data
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if weight_col == 'ones':
        df = df.copy()
        df['ones'] = 1
        weight_col = 'ones'
    
    # Get corridor averages
    cor_df = weighted_mean_by_corridor(df, 'Date', corridors, variable, weight_col)
    cor_df = cor_df[~np.isnan(cor_df[variable])]
    
    # Group corridors by various aggregations
    result = group_corridors(cor_df, 'Date', variable, weight_col)
    
    # Add week number
    result['Week'] = pd.to_datetime(result['Date']).dt.isocalendar().week
    
    return result

def get_cor_monthly_avg_by_day(df: pd.DataFrame, 
                              corridors: pd.DataFrame, 
                              variable: str, 
                              weight_col: str = 'ones') -> pd.DataFrame:
    """
    Calculate corridor-level monthly averages
    
    Args:
        df: DataFrame with signal-level data
        corridors: Corridor mapping DataFrame
        variable: Variable to aggregate
        weight_col: Weight column
    
    Returns:
        DataFrame with corridor-level monthly data
    """
    
    if df.empty:
        return pd.DataFrame()
    
    if weight_col == 'ones':
        df = df.copy()
        df['ones'] = 1
        weight_col = 'ones'
    
    # Determine period column
    period_col = 'Month' if 'Month' in df.columns else 'Date'
    
    # Get corridor averages
    cor_df = weighted_mean_by_corridor(df, period_col, corridors, variable, weight_col)
    cor_df = cor_df[~np.isnan(cor_df[variable])]
    
    # Group corridors
    result = group_corridors(cor_df, period_col, variable, weight_col)
    
    return result

def get_vph(counts_df: pd.DataFrame, 
           interval: str = "1 hour", 
           mainline_only: bool = True) -> pd.DataFrame:
    """
    Calculate vehicles per hour from counts data
    
    Args:
        counts_df: DataFrame with count data
        interval: Time interval ('1 hour', '15 min', etc.)
        mainline_only: Whether to use only mainline phases (2,6)
    
    Returns:
        DataFrame with VPH data
    """
    
    if counts_df.empty:
        return pd.DataFrame()
    
    df = counts_df.copy()
    
    # Filter to mainline phases if requested
    if mainline_only and 'CallPhase' in df.columns:
        df = df[df['CallPhase'].isin([2, 6])]
    
    # Determine timeperiod column
    time_cols = ['Timeperiod', 'Date_Hour', 'Hour']
    time_col = None
    for col in time_cols:
        if col in df.columns:
            time_col = col
            break
    
    if time_col is None:
        raise ValueError("No valid time column found in DataFrame")
    
    # Rename to standard name if needed
    if time_col != 'Timeperiod':
        df = df.rename(columns={time_col: 'Timeperiod'})
    
    # Floor to specified interval
    df['Timeperiod'] = pd.to_datetime(df['Timeperiod'])
    if interval == "15 min":
        df['Timeperiod'] = df['Timeperiod'].dt.floor('15T')
    elif interval == "1 hour":
        df['Timeperiod'] = df['Timeperiod'].dt.floor('H')
    else:
        # Parse custom interval
        df['Timeperiod'] = df['Timeperiod'].dt.floor(interval)
    
    # Add date and week information
    df['Date'] = df['Timeperiod'].dt.date
    df['DOW'] = df['Timeperiod'].dt.dayofweek  # Monday=0, Sunday=6
    df['Week'] = df['Timeperiod'].dt.isocalendar().week
    
    # Aggregate volumes
    result = df.groupby(['SignalID', 'Week', 'DOW', 'Timeperiod'])['vol'].sum().reset_index()
    result = result.rename(columns={'vol': 'vph'})
    
    # If interval is 1 hour, rename Timeperiod to Hour
    if interval == "1 hour":
        result = result.rename(columns={'Timeperiod': 'Hour'})
    
    return result

def get_avg_by_hr(df: pd.DataFrame, 
                 variable: str, 
                 weight_col: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate hourly averages from sub-hourly data
    
    Args:
        df: DataFrame with Date_Hour column
        variable: Variable to average
        weight_col: Weight column for weighted average
    
    Returns:
        DataFrame with hourly averages
    """
    
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    
    # Add temporal columns
    df['Date_Hour'] = pd.to_datetime(df['Date_Hour'])
    df['DOW'] = df['Date_Hour'].dt.dayofweek
    df['Week'] = df['Date_Hour'].dt.isocalendar().week
    df['Hour'] = df['Date_Hour'].dt.floor('H')
    
    if weight_col is None:
        # Simple mean
        df['weight_temp'] = 1
        weight_col = 'weight_temp'
    
    # Calculate weighted averages by hour
    def weighted_mean(group):
        weights = group[weight_col]
        values = group[variable]
        valid_mask = ~(np.isnan(values) | np.isnan(weights))
        
        if valid_mask.sum() == 0:
            return np.nan, 0
        
        return np.average(values[valid_mask], weights=weights[valid_mask]), weights[valid_mask].sum()
    
    result = df.groupby(['SignalID', 'CallPhase', 'Week', 'DOW', 'Hour']).apply(
        lambda x: pd.Series({
            variable: weighted_mean(x)[0],
            weight_col: weighted_mean(x)[1]
        })
    ).reset_index()
    
    # Calculate delta
    result = result.sort_values(['SignalID', 'CallPhase', 'Hour'])
    result['lag_'] = result.groupby(['SignalID', 'CallPhase'])[variable].shift(1)
    result['delta'] = (result[variable] - result['lag_']) / result['lag_']
    result = result.drop('lag_', axis=1)
    
    # Clean up temporary weight column
    if 'weight_temp' in result.columns:
        result = result.drop('weight_temp', axis=1)
    
    return result

def get_monthly_avg_by_hr(df: pd.DataFrame, 
                         variable: str, 
                         weight_col: str = 'ones') -> pd.DataFrame:
    """
    Calculate monthly averages by hour of day
    
    Args:
        df: DataFrame with hourly data
        variable: Variable to average
        weight_col: Weight column
    
    Returns:
        DataFrame with monthly hourly averages
    """
    
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    
    if weight_col == 'ones':
        df['ones'] = 1
        weight_col = 'ones'
    
    # Group by SignalID and Hour, then calculate monthly averages
    monthly_hourly = df.groupby(['SignalID', 'Hour']).apply(
        lambda x: pd.Series({
            variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    # Set hour to first day of month
    monthly_hourly['Hour'] = pd.to_datetime(monthly_hourly['Hour'])
    monthly_hourly['Hour'] = monthly_hourly['Hour'] - pd.to_timedelta(monthly_hourly['Hour'].dt.day - 1, unit='D')
    
    # Group again by SignalID and adjusted Hour
    final_result = monthly_hourly.groupby(['SignalID', 'Hour']).apply(
        lambda x: pd.Series({
            variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
            weight_col: x[weight_col].sum()
        })
    ).reset_index()
    
    # Calculate delta
    final_result = final_result.sort_values(['SignalID', 'Hour'])
    final_result['lag_'] = final_result.groupby('SignalID')[variable].shift(1)
    final_result['delta'] = (final_result[variable] - final_result['lag_']) / final_result['lag_']
    final_result = final_result.drop('lag_', axis=1)
    
    return final_result

def summarize_by_peak(df: pd.DataFrame, 
                     date_col: str,
                     am_peak_hours: List[int] = None,
                     pm_peak_hours: List[int] = None) -> Dict[str, pd.DataFrame]:
    """
    Summarize data by peak/off-peak periods
    
    Args:
        df: DataFrame with hourly data
        date_col: Name of the datetime column
        am_peak_hours: AM peak hours
        pm_peak_hours: PM peak hours
    
    Returns:
        Dictionary with 'Peak' and 'Off_Peak' DataFrames
    """
    
    if df.empty:
        return {'Peak': pd.DataFrame(), 'Off_Peak': pd.DataFrame()}
    
    if am_peak_hours is None:
        am_peak_hours = [6, 7, 8, 9]
    if pm_peak_hours is None:
        pm_peak_hours = [16, 17, 18, 19]
    
    df = df.copy()
    peak_hours = am_peak_hours + pm_peak_hours
    
    # Add peak indicator
    df['hour'] = pd.to_datetime(df[date_col]).dt.hour
    df['Peak'] = df['hour'].apply(lambda x: 'Peak' if x in peak_hours else 'Off_Peak')
    
    # Split by peak status
    result = {}
    for peak_status, group in df.groupby('Peak'):
        # Aggregate by SignalID and date
        group['Period'] = pd.to_datetime(group[date_col]).dt.date
        
        if 'sf_freq' in group.columns and 'cycles' in group.columns:
            # Special handling for split failure frequency
            agg_data = group.groupby(['SignalID', 'Period']).apply(
                lambda x: pd.Series({
                    'sf_freq': np.average(x['sf_freq'], weights=x['cycles']) if len(x) > 0 else np.nan,
                    'cycles': x['cycles'].sum()
                })
            ).reset_index()
        else:
            # Default aggregation - sum numeric columns
            numeric_cols = group.select_dtypes(include=[np.number]).columns
            agg_data = group.groupby(['SignalID', 'Period'])[numeric_cols].sum().reset_index()
        
        result[peak_status] = agg_data
    
    return result

def get_daily_detector_uptime(filtered_counts: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Calculate daily detector uptime by detector type
    
    Args:
        filtered_counts: DataFrame with filtered count data
    
    Returns:
        Dictionary with uptime data by detector type
    """
    
    if filtered_counts.empty:
        return {}
    
    # Remove time periods with no volume on all detectors
    volume_check = filtered_counts.groupby(['SignalID', 'Timeperiod'])['vol'].sum().reset_index()
    bad_comms = volume_check[volume_check['vol'] == 0][['SignalID', 'Timeperiod']]
    
    # Anti-join to remove bad communication periods
    fc = filtered_counts.merge(bad_comms, on=['SignalID', 'Timeperiod'], how='left', indicator=True)
    fc = fc[fc['_merge'] == 'left_only'].drop('_merge', axis=1)
    
    if fc.empty:
        return {}
    
    # Prepare uptime data
    fc['Date_Hour'] = pd.to_datetime(fc['Timeperiod'])
    fc['Date'] = fc['Date_Hour'].dt.date
    
    uptime_data = fc[['SignalID', 'CallPhase', 'Detector', 'Date', 'Date_Hour', 'Good_Day']].copy()
    
    # Categorize detectors
    uptime_data['setback'] = uptime_data['CallPhase'].apply(
        lambda x: 'Setback' if x in [2, 6] else 'Presence'
    )
    
    result = {}
    
    # Calculate uptime by detector type
    for setback_type, group in uptime_data.groupby('setback'):
        uptime_by_type = group.groupby(['SignalID', 'CallPhase', 'Date', 'Date_Hour', 'setback']).agg({
            'Good_Day': 'sum',
            'Detector': 'count'
        }).reset_index()
        
        uptime_by_type = uptime_by_type.rename(columns={'Detector': 'all'})
        uptime_by_type['uptime'] = uptime_by_type['Good_Day'] / uptime_by_type['all']
        uptime_by_type['all'] = uptime_by_type['all'].astype(float)
        
        result[setback_type] = uptime_by_type
    
    return result

def get_avg_daily_detector_uptime(detector_uptime_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Calculate average daily detector uptime across detector types
    
    Args:
        detector_uptime_dict: Dictionary from get_daily_detector_uptime
    
    Returns:
        DataFrame with daily uptime averages
    """
    
    if not detector_uptime_dict:
        return pd.DataFrame()
    
    # Get daily averages for each detector type
    daily_uptimes = {}
    
    for setback_type, uptime_data in detector_uptime_dict.items():
        if uptime_data.empty:
            continue
            
        filtered_data = uptime_data[uptime_data['setback'] == setback_type].copy()
        daily_avg = get_daily_avg(filtered_data, 'uptime', 'all', peak_only=False)
        daily_uptimes[setback_type] = daily_avg
    
    # Combine setback and presence data
    if 'Setback' in daily_uptimes and 'Presence' in daily_uptimes:
        sb_daily = daily_uptimes['Setback'].add_suffix('.sb')
        pr_daily = daily_uptimes['Presence'].add_suffix('.pr')
        
        combined = sb_daily.merge(pr_daily, 
                                left_on=['SignalID.sb', 'Date.sb'], 
                                right_on=['SignalID.pr', 'Date.pr'], 
                                how='outer')
        
        # Clean up column names
        combined = combined.rename(columns={
            'SignalID.sb': 'SignalID',
            'Date.sb': 'Date',
            'uptime.sb': 'uptime.sb',
            'uptime.pr': 'uptime.pr'
        })
        
        # Drop duplicate columns
        cols_to_drop = [col for col in combined.columns if col.endswith('.pr') and 
                       col.replace('.pr', '.sb') in combined.columns and 
                       col not in ['uptime.pr', 'all.pr']]
        combined = combined.drop(columns=cols_to_drop)
    else:
        combined = pd.DataFrame()
    
    # Calculate overall uptime (all detector types combined)
    all_uptime_data = []
    for uptime_data in detector_uptime_dict.values():
        if not uptime_data.empty:
            all_uptime_data.append(uptime_data)
    
    if all_uptime_data:
        all_combined = pd.concat(all_uptime_data, ignore_index=True)
        all_daily = get_daily_avg(all_combined, 'uptime', 'all', peak_only=False)
        
        if not combined.empty:
            final_result = combined.merge(all_daily, on=['SignalID', 'Date'], how='outer')
        else:
            final_result = all_daily
        
        # Clean up delta columns
        delta_cols = [col for col in final_result.columns if col.startswith('delta')]
        final_result = final_result.drop(columns=delta_cols)
        
        return final_result
    
    return combined

def get_quarterly(monthly_df: pd.DataFrame, 
                 variable: str, 
                 weight_col: str = 'ones', 
                 operation: str = 'avg') -> pd.DataFrame:
    """
    Convert monthly data to quarterly for quarterly reports
    
    Args:
        monthly_df: DataFrame with monthly data
        variable: Variable to aggregate
        weight_col: Weight column
        operation: Type of operation ('avg', 'sum', 'latest')
    
    Returns:
        DataFrame with quarterly data
    """
    
    if monthly_df.empty:
        return pd.DataFrame()
    
    df = monthly_df.copy()
    
    # Add ones column if needed
    if weight_col == 'ones' and 'ones' not in df.columns:
        df['ones'] = 1
    
    # Ensure Month is datetime
    if df['Month'].dtype != 'datetime64[ns]':
        df['Month'] = pd.to_datetime(df['Month'])
    
    # Create quarter column
    df['Quarter'] = df['Month'].dt.year.astype(str) + '.' + df['Month'].dt.quarter.astype(str)
    
    # Group by corridor and quarter
    grouped = df.groupby(['Corridor', 'Zone_Group', 'Quarter'])
    
    if operation == 'avg':
        # Weighted average
        def weighted_avg(group):
            weights = group[weight_col]
            values = group[variable]
            valid_mask = ~(np.isnan(values) | np.isnan(weights))
            
            if valid_mask.sum() == 0:
                return np.nan, 0
            
            return np.average(values[valid_mask], weights=weights[valid_mask]), weights[valid_mask].sum()
        
        quarterly_df = grouped.apply(
            lambda x: pd.Series({
                variable: weighted_avg(x)[0],
                weight_col: weighted_avg(x)[1]
            })
        ).reset_index()
        
    elif operation == 'sum':
        # Sum
        quarterly_df = grouped[variable].sum().reset_index()
        
    elif operation == 'latest':
        # Latest value in quarter
        quarterly_df = df.loc[df.groupby(['Corridor', 'Zone_Group', 'Quarter'])['Month'].idxmax()]
        quarterly_df = quarterly_df[['Corridor', 'Zone_Group', 'Quarter', variable, weight_col]].reset_index(drop=True)
    
    else:
        raise ValueError(f"Unknown operation: {operation}")
    
    # Calculate delta
    quarterly_df = quarterly_df.sort_values(['Zone_Group', 'Corridor', 'Quarter'])
    quarterly_df['lag_'] = quarterly_df.groupby(['Zone_Group', 'Corridor'])[variable].shift(1)
    quarterly_df['delta'] = (quarterly_df[variable] - quarterly_df['lag_']) / quarterly_df['lag_']
    quarterly_df = quarterly_df.drop('lag_', axis=1)
    
    return quarterly_df

def sigify(signal_df: pd.DataFrame, 
          corridor_df: pd.DataFrame, 
          corridors: pd.DataFrame, 
          identifier: str = 'SignalID') -> pd.DataFrame:
    """
    Convert corridor data to signal-level data for detailed analysis
    
    Args:
        signal_df: DataFrame with signal-level data
        corridor_df: DataFrame with corridor-level data
        corridors: Corridor mapping DataFrame
        identifier: Identifier type ('SignalID' or 'CameraID')
    
    Returns:
        Combined DataFrame with both signal and corridor data
    """
    
    if signal_df.empty and corridor_df.empty:
        return pd.DataFrame()
    
    # Determine period column
    period_cols = ['Date', 'Month', 'Hour', 'Timeperiod']
    period_col = None
    for col in period_cols:
        if col in signal_df.columns:
            period_col = col
            break
    
    if identifier == 'SignalID':
        # Get signal descriptions
        descriptions = corridors.groupby(['SignalID', 'Corridor'])['Description'].first().reset_index()
        
        # Transform signal data
        signal_transformed = signal_df.merge(
            corridors[['SignalID', 'Corridor', 'Name']].drop_duplicates(), 
            on='SignalID', how='left'
        )
        signal_transformed = signal_transformed.rename(columns={
            'Corridor': 'Zone_Group',
            'SignalID': 'Corridor'
        })
        
        # Add descriptions
        signal_transformed = signal_transformed.merge(
            descriptions.rename(columns={'SignalID': 'Corridor', 'Corridor': 'Zone_Group'}),
            on=['Corridor', 'Zone_Group'], how='left'
        )
        signal_transformed['Description'] = signal_transformed['Description'].fillna(
            signal_transformed['Corridor'].astype(str)
        )
        
    elif identifier == 'CameraID':
        # Transform camera data
        corridors_cam = corridors.rename(columns={'Location': 'Name'})
        
        signal_transformed = signal_df.drop(columns=[col for col in ['Subcorridor', 'Zone_Group'] 
                                                   if col in signal_df.columns])
        signal_transformed = signal_transformed.merge(
            corridors_cam[['CameraID', 'Corridor', 'Name']].drop_duplicates(),
            on=['Corridor', 'CameraID'], how='left'
        )
        signal_transformed = signal_transformed.rename(columns={
            'Corridor': 'Zone_Group',
            'CameraID': 'Corridor'
        })
        signal_transformed['Description'] = signal_transformed['Description'].fillna(
            signal_transformed['Corridor'].astype(str)
        )
    else:
        raise ValueError("Identifier must be 'SignalID' or 'CameraID'")
    
    # Transform corridor data
    corridor_filtered = corridor_df[corridor_df['Corridor'].isin(signal_transformed['Zone_Group'].unique())].copy()
    corridor_filtered['Zone_Group'] = corridor_filtered['Corridor']
    corridor_filtered['Description'] = corridor_filtered['Corridor']
    
    # Remove subcorridor columns if present
    corridor_filtered = corridor_filtered.drop(columns=[col for col in ['Subcorridor'] 
                                                       if col in corridor_filtered.columns])
    
    # Combine data
    combined = pd.concat([signal_transformed, corridor_filtered], ignore_index=True)
    
    # Convert to categorical
    combined['Corridor'] = combined['Corridor'].astype('category')
    combined['Description'] = combined['Description'].astype('category')
    if 'Zone_Group' in combined.columns:
        combined['Zone_Group'] = combined['Zone_Group'].astype('category')
    
    # Sort by appropriate column
    if period_col:
        combined = combined.sort_values(['Zone_Group', 'Corridor', period_col])
    else:
        combined = combined.sort_values(['Zone_Group', 'Corridor'])
    
    return combined

def create_complete_time_series(df: pd.DataFrame, 
                              time_col: str, 
                              freq: str, 
                              groupby_cols: List[str]) -> pd.DataFrame:
    """
    Create complete time series with missing periods filled
    
    Args:
        df: Input DataFrame
        time_col: Name of time column
        freq: Frequency string ('D', 'H', 'MS', etc.)
        groupby_cols: Columns to group by
    
    Returns:
        DataFrame with complete time series
    """
    
    if df.empty:
        return df
    
    # Get time range
    min_time = df[time_col].min()
    max_time = df[time_col].max()
    
    # Create complete time index
    complete_times = pd.date_range(start=min_time, end=max_time, freq=freq)
    
    # Get all unique combinations of groupby columns
    unique_groups = df[groupby_cols].drop_duplicates()
    
    # Create complete combinations
    complete_combinations = []
    for _, group in unique_groups.iterrows():
        for time_val in complete_times:
            combo = group.to_dict()
            combo[time_col] = time_val
            complete_combinations.append(combo)
    
    complete_df = pd.DataFrame(complete_combinations)
    
    # Merge with original data
    result = complete_df.merge(df, on=groupby_cols + [time_col], how='left')
    
    return result

def calculate_performance_metrics(df: pd.DataFrame, 
                                variable: str,
                                benchmark_col: Optional[str] = None,
                                target_value: Optional[float] = None) -> pd.DataFrame:
    """
    Calculate performance metrics including trends, benchmarks, and targets
    
    Args:
        df: DataFrame with time series data
        variable: Variable to analyze
        benchmark_col: Column with benchmark values
        target_value: Target value for comparison
    
    Returns:
        DataFrame with performance metrics
    """
    
    if df.empty:
        return pd.DataFrame()
    
    result = df.copy()
    
    # Calculate moving averages
    if len(result) >= 7:
        result[f'{variable}_ma7'] = result[variable].rolling(window=7, min_periods=1).mean()
    if len(result) >= 30:
        result[f'{variable}_ma30'] = result[variable].rolling(window=30, min_periods=1).mean()
    
    # Calculate percent change from previous period
    result[f'{variable}_pct_change'] = result[variable].pct_change() * 100
    
    # Calculate comparison to benchmark if provided
    if benchmark_col and benchmark_col in result.columns:
        result[f'{variable}_vs_benchmark'] = ((result[variable] - result[benchmark_col]) / 
                                            result[benchmark_col] * 100)
    
    # Calculate comparison to target if provided
    if target_value is not None:
        result[f'{variable}_vs_target'] = ((result[variable] - target_value) / target_value * 100)
        result[f'{variable}_meets_target'] = result[variable] >= target_value
    
    # Calculate trend direction
    if len(result) >= 3:
        result[f'{variable}_trend'] = np.where(
            result[variable] > result[variable].shift(1), 'Up',
            np.where(result[variable] < result[variable].shift(1), 'Down', 'Flat')
        )
    
    return result

def aggregate_by_time_of_day(df: pd.DataFrame, 
                           datetime_col: str,
                           variable: str,
                           weight_col: Optional[str] = None) -> pd.DataFrame:
    """
    Aggregate data by time of day patterns
    
    Args:
        df: DataFrame with datetime data
        datetime_col: Name of datetime column
        variable: Variable to aggregate
        weight_col: Weight column for weighted average
    
    Returns:
        DataFrame with time-of-day aggregations
    """
    
    if df.empty:
        return pd.DataFrame()
    
    df = df.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    
    # Extract time components
    df['hour'] = df[datetime_col].dt.hour
    df['day_of_week'] = df[datetime_col].dt.dayofweek
    df['month'] = df[datetime_col].dt.month
    df['quarter'] = df[datetime_col].dt.quarter
    
    # Define time periods
    df['time_period'] = pd.cut(df['hour'], 
                              bins=[0, 6, 10, 14, 18, 24], 
                              labels=['Night', 'Morning', 'Midday', 'Afternoon', 'Evening'],
                              include_lowest=True)
    
    # Aggregate by different time dimensions
    aggregations = {}
    
    # By hour of day
    if weight_col:
        hourly = df.groupby('hour').apply(
            lambda x: pd.Series({
                variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
                'count': len(x)
            })
        ).reset_index()
    else:
        hourly = df.groupby('hour')[variable].agg(['mean', 'count']).reset_index()
        hourly = hourly.rename(columns={'mean': variable})
    
    aggregations['hourly'] = hourly
    
    # By day of week
    if weight_col:
        daily = df.groupby('day_of_week').apply(
            lambda x: pd.Series({
                variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
                'count': len(x)
            })
        ).reset_index()
    else:
        daily = df.groupby('day_of_week')[variable].agg(['mean', 'count']).reset_index()
        daily = daily.rename(columns={'mean': variable})
    
    daily['day_name'] = daily['day_of_week'].map({
        0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday',
        4: 'Friday', 5: 'Saturday', 6: 'Sunday'
    })
    aggregations['daily'] = daily
    
    # By time period
    if weight_col:
        period = df.groupby('time_period').apply(
            lambda x: pd.Series({
                variable: np.average(x[variable], weights=x[weight_col]) if len(x) > 0 else np.nan,
                'count': len(x)
            })
        ).reset_index()
    else:
        period = df.groupby('time_period')[variable].agg(['mean', 'count']).reset_index()
        period = period.rename(columns={'mean': variable})
    
    aggregations['time_period'] = period
    
    return aggregations

# Utility functions for specific traffic metrics
def calculate_traffic_kpis(df: pd.DataFrame, 
                          date_col: str = 'Date',
                          volume_col: str = 'vol',
                          corridor_col: str = 'Corridor') -> Dict[str, Any]:
    """
    Calculate key performance indicators for traffic data
    
    Args:
        df: DataFrame with traffic data
        date_col: Date column name
        volume_col: Volume column name  
        corridor_col: Corridor column name
    
    Returns:
        Dictionary with KPIs
    """
    
    if df.empty:
        return {}
    
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    
    # Current month vs previous month
    current_month = df[date_col].max().to_period('M')
    previous_month = current_month - 1
    
    current_data = df[df[date_col].dt.to_period('M') == current_month]
    previous_data = df[df[date_col].dt.to_period('M') == previous_month]
    
    kpis = {
        'current_month': current_month.strftime('%Y-%m'),
        'previous_month': previous_month.strftime('%Y-%m'),
        'total_volume_current': current_data[volume_col].sum() if not current_data.empty else 0,
        'total_volume_previous': previous_data[volume_col].sum() if not previous_data.empty else 0,
        'volume_change_pct': 0,
        'avg_daily_volume_current': 0,
        'avg_daily_volume_previous': 0,
        'corridors_with_data_current': 0,
        'corridors_with_data_previous': 0,
        'top_corridors_current': [],
        'bottom_corridors_current': []
    }
    
    # Calculate percent change
    if kpis['total_volume_previous'] > 0:
        kpis['volume_change_pct'] = ((kpis['total_volume_current'] - kpis['total_volume_previous']) / 
                                   kpis['total_volume_previous'] * 100)
    
    # Average daily volumes
    if not current_data.empty:
        days_current = current_data[date_col].nunique()
        kpis['avg_daily_volume_current'] = kpis['total_volume_current'] / days_current if days_current > 0 else 0
        kpis['corridors_with_data_current'] = current_data[corridor_col].nunique()
    
    if not previous_data.empty:
        days_previous = previous_data[date_col].nunique()
        kpis['avg_daily_volume_previous'] = kpis['total_volume_previous'] / days_previous if days_previous > 0 else 0
        kpis['corridors_with_data_previous'] = previous_data[corridor_col].nunique()
    
    # Top and bottom performing corridors
    if not current_data.empty:
        corridor_volumes = current_data.groupby(corridor_col)[volume_col].sum().sort_values(ascending=False)
        kpis['top_corridors_current'] = corridor_volumes.head(5).to_dict()
        kpis['bottom_corridors_current'] = corridor_volumes.tail(5).to_dict()
    
    return kpis

def detect_anomalies(df: pd.DataFrame, 
                    variable: str,
                    method: str = 'iqr',
                    threshold: float = 1.5) -> pd.DataFrame:
    """
    Detect anomalies in time series data
    
    Args:
        df: DataFrame with time series data
        variable: Variable to check for anomalies
        method: Detection method ('iqr', 'zscore', 'isolation')
        threshold: Threshold for anomaly detection
    
    Returns:
        DataFrame with anomaly flags
    """
    
    if df.empty:
        return df
    
    result = df.copy()
    
    if method == 'iqr':
        Q1 = result[variable].quantile(0.25)
        Q3 = result[variable].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        
        result[f'{variable}_anomaly'] = ((result[variable] < lower_bound) | 
                                       (result[variable] > upper_bound))
        result[f'{variable}_anomaly_score'] = np.where(
            result[variable] < lower_bound, (lower_bound - result[variable]) / IQR,
            np.where(result[variable] > upper_bound, (result[variable] - upper_bound) / IQR, 0)
        )
    
    elif method == 'zscore':
        mean_val = result[variable].mean()
        std_val = result[variable].std()
        z_scores = np.abs((result[variable] - mean_val) / std_val)
        
        result[f'{variable}_anomaly'] = z_scores > threshold
        result[f'{variable}_anomaly_score'] = z_scores
    
    elif method == 'isolation':
        try:
            from sklearn.ensemble import IsolationForest
            
            # Reshape for sklearn
            X = result[[variable]].values
            iso_forest = IsolationForest(contamination=0.1, random_state=42)
            anomaly_pred = iso_forest.fit_predict(X)
            anomaly_scores = iso_forest.score_samples(X)
            
            result[f'{variable}_anomaly'] = anomaly_pred == -1
            result[f'{variable}_anomaly_score'] = -anomaly_scores  # Convert to positive scores
        except ImportError:
            logger.warning("sklearn not available, falling back to IQR method")
            return detect_anomalies(df, variable, method='iqr', threshold=threshold)
    
    else:
        raise ValueError(f"Unknown anomaly detection method: {method}")
    
    return result

def calculate_reliability_metrics(df: pd.DataFrame, 
                                variable: str,
                                target_col: Optional[str] = None,
                                target_value: Optional[float] = None) -> Dict[str, float]:
    """
    Calculate reliability metrics for performance data
    
    Args:
        df: DataFrame with performance data
        variable: Variable to analyze
        target_col: Column with target values
        target_value: Fixed target value
    
    Returns:
        Dictionary with reliability metrics
    """
    
    if df.empty:
        return {}
    
    values = df[variable].dropna()
    
    if len(values) == 0:
        return {}
    
    metrics = {
        'mean': values.mean(),
        'median': values.median(),
        'std': values.std(),
        'min': values.min(),
        'max': values.max(),
        'count': len(values),
        'cv': values.std() / values.mean() if values.mean() != 0 else np.inf,  # Coefficient of variation
        'reliability_index': None,  # Will be calculated below
        'target_achievement_rate': None
    }
    
    # Calculate reliability index (1 - CV, bounded between 0 and 1)
    if not np.isinf(metrics['cv']):
        metrics['reliability_index'] = max(0, min(1, 1 - metrics['cv']))
    
    # Calculate target achievement rate
    if target_col and target_col in df.columns:
        targets = df[target_col].dropna()
        if len(targets) > 0:
            # Align values and targets
            aligned_df = df[[variable, target_col]].dropna()
            if not aligned_df.empty:
                achievements = aligned_df[variable] >= aligned_df[target_col]
                metrics['target_achievement_rate'] = achievements.mean()
    
    elif target_value is not None:
        achievements = values >= target_value
        metrics['target_achievement_rate'] = achievements.mean()
    
    return metrics

def summarize_corridor_performance(df: pd.DataFrame,
                                 corridors: pd.DataFrame,
                                 variable: str,
                                 date_col: str = 'Date') -> pd.DataFrame:
    """
    Create corridor performance summary with rankings and trends
    
    Args:
        df: DataFrame with signal-level data
        corridors: Corridor mapping DataFrame
        variable: Performance variable to summarize
        date_col: Date column name
    
    Returns:
        DataFrame with corridor performance summary
    """
    
    if df.empty or corridors.empty:
        return pd.DataFrame()
    
    # Merge with corridor data
    merged = df.merge(corridors, on='SignalID', how='inner')
    
    # Calculate corridor averages
    corridor_summary = merged.groupby(['Corridor', 'Zone_Group']).agg({
        variable: ['mean', 'std', 'count'],
        date_col: ['min', 'max']
    }).reset_index()
    
    # Flatten column names
    corridor_summary.columns = ['_'.join(col).strip('_') if col[1] else col[0] 
                               for col in corridor_summary.columns.values]
    
    # Calculate rankings
    corridor_summary[f'{variable}_rank'] = corridor_summary[f'{variable}_mean'].rank(ascending=False)
    
    # Calculate trend (comparing first and last month)
    trend_data = []
    for corridor in corridor_summary['Corridor'].unique():
        corr_data = merged[merged['Corridor'] == corridor].copy()
        corr_data[date_col] = pd.to_datetime(corr_data[date_col])
        corr_data['month'] = corr_data[date_col].dt.to_period('M')
        
        monthly_avg = corr_data.groupby('month')[variable].mean()
        if len(monthly_avg) >= 2:
            first_month = monthly_avg.iloc[0]
            last_month = monthly_avg.iloc[-1]
            trend = 'Improving' if last_month > first_month else 'Declining' if last_month < first_month else 'Stable'
            trend_pct = ((last_month - first_month) / first_month * 100) if first_month != 0 else 0
        else:
            trend = 'Insufficient Data'
            trend_pct = 0
        
        trend_data.append({
            'Corridor': corridor,
            'trend': trend,
            'trend_pct': trend_pct
        })
    
    trend_df = pd.DataFrame(trend_data)
    corridor_summary = corridor_summary.merge(trend_df, on='Corridor', how='left')
    
    # Sort by performance
    corridor_summary = corridor_summary.sort_values(f'{variable}_mean', ascending=False)
    
    return corridor_summary

# Specialized aggregation functions for specific metrics

def get_aog_aggregations(df: pd.DataFrame, corridors: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Get all standard aggregations for Arrivals on Green (AOG) data
    """
    
    aggregations = {}
    
    # Daily averages
    if 'Date_Hour' in df.columns:
        daily_aog = get_daily_avg(df, 'aog', 'vol', peak_only=True)
        aggregations['daily'] = daily_aog
        
        # Weekly averages
        weekly_aog = get_weekly_avg_by_day(daily_aog, 'aog', 'vol', peak_only=True)
        aggregations['weekly'] = weekly_aog
        
        # Monthly averages  
        monthly_aog = get_monthly_avg_by_day(daily_aog, 'aog', 'vol', peak_only=True)
        aggregations['monthly'] = monthly_aog
        
        # Corridor aggregations
        cor_weekly = get_cor_weekly_avg_by_day(weekly_aog, corridors, 'aog', 'vol')
        aggregations['corridor_weekly'] = cor_weekly
        
        cor_monthly = get_cor_monthly_avg_by_day(monthly_aog, corridors, 'aog', 'vol')
        aggregations['corridor_monthly'] = cor_monthly
    
    return aggregations

def get_volume_aggregations(df: pd.DataFrame, corridors: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Get all standard aggregations for volume data
    """
    
    aggregations = {}
    
    # VPH (Vehicles per Hour)
    vph = get_vph(df, interval="1 hour", mainline_only=True)
    aggregations['vph'] = vph
    
    # VPD (Vehicles per Day) - requires grouping vph by day
    if not vph.empty:
        vpd = vph.copy()
        vpd['Date'] = pd.to_datetime(vpd['Hour']).dt.date
        vpd = vpd.groupby(['SignalID', 'Date'])['vph'].sum().reset_index()
        vpd = vpd.rename(columns={'vph': 'vpd'})
        aggregations['vpd'] = vpd
        
        # Weekly VPD
        weekly_vpd = get_period_avg(vpd, 'vpd', 'Date')
        aggregations['weekly_vpd'] = weekly_vpd
        
        # Monthly VPD
        monthly_vpd = get_monthly_avg_by_day(vpd, 'vpd', peak_only=False)
        aggregations['monthly_vpd'] = monthly_vpd
        
        # Corridor aggregations
        cor_weekly_vpd = get_cor_weekly_avg_by_day(weekly_vpd, corridors, 'vpd')
        aggregations['corridor_weekly_vpd'] = cor_weekly_vpd
        
        cor_monthly_vpd = get_cor_monthly_avg_by_day(monthly_vpd, corridors, 'vpd')
        aggregations['corridor_monthly_vpd'] = cor_monthly_vpd
    
    return aggregations

# Export main functions
__all__ = [
    'weighted_mean_by_corridor',
    'group_corridors',
    'get_hourly',
    'get_period_avg',
    'get_period_sum', 
    'get_daily_avg',
    'get_daily_sum',
    'get_weekly_avg_by_day',
    'get_monthly_avg_by_day',
    'get_cor_weekly_avg_by_day',
    'get_cor_monthly_avg_by_day',
    'get_vph',
    'get_avg_by_hr',
    'get_monthly_avg_by_hr',
    'summarize_by_peak',
    'get_daily_detector_uptime',
    'get_avg_daily_detector_uptime',
    'get_quarterly',
    'sigify',
    'calculate_performance_metrics',
    'aggregate_by_time_of_day',
    'calculate_traffic_kpis',
    'detect_anomalies',
    'calculate_reliability_metrics',
    'summarize_corridor_performance',
    'get_aog_aggregations',
    'get_volume_aggregations'
]
