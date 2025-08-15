import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional, Dict, List, Union, Callable, Any

logger = logging.getLogger(__name__)

# Constants
AM_PEAK_HOURS = [6, 7, 8, 9]
PM_PEAK_HOURS = [16, 17, 18, 19]
TUE, WED, THU = 2, 3, 4  # weekday numbers


def weighted_mean_by_corridor(df: pd.DataFrame, 
                            per_col: str, 
                            corridors: pd.DataFrame, 
                            var_col: str, 
                            wt_col: Optional[str] = None) -> pd.DataFrame:
    """
    Calculate weighted mean by corridor
    
    Args:
        df: Input DataFrame
        per_col: Period column name
        corridors: Corridor mapping DataFrame
        var_col: Variable column name
        wt_col: Weight column name (optional)
    
    Returns:
        DataFrame with weighted means by corridor
    """
    
    # Join with corridors
    gdf = df.merge(corridors, how='left').dropna(subset=['Corridor'])
    gdf['Corridor'] = gdf['Corridor'].astype('category')
    
    # Group by Zone_Group, Zone, Corridor, and period
    group_cols = ['Zone_Group', 'Zone', 'Corridor', per_col]
    grouped = gdf.groupby(group_cols)
    
    if wt_col is None:
        # Simple mean
        result = grouped[var_col].mean().reset_index()
        result = result.rename(columns={var_col: var_col})
    else:
        # Weighted mean
        def weighted_avg(group):
            weights = group[wt_col]
            values = group[var_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({var_col: np.nan, wt_col: 0})
            
            return pd.Series({
                var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                wt_col: weights[valid_mask].sum()
            })
        
        result = grouped.apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['Zone_Group', 'Zone', 'Corridor', per_col])
    result['lag_'] = result.groupby(['Zone_Group', 'Zone', 'Corridor'])[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    # Select relevant columns
    select_cols = ['Zone_Group', 'Zone', 'Corridor', per_col, var_col, 'delta']
    if wt_col is not None:
        select_cols.insert(-1, wt_col)
    
    return result[select_cols].dropna(subset=[var_col])


def group_corridor_by(df: pd.DataFrame, 
                     per_col: str, 
                     var_col: str, 
                     wt_col: str, 
                     corr_grp: str) -> pd.DataFrame:
    """
    Group corridor data by period
    
    Args:
        df: Input DataFrame
        per_col: Period column name
        var_col: Variable column name
        wt_col: Weight column name
        corr_grp: Corridor group name
    
    Returns:
        Grouped DataFrame
    """
    
    grouped = df.groupby(per_col)
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = grouped.apply(weighted_avg).reset_index()
    result['Corridor'] = corr_grp
    result['Zone_Group'] = corr_grp
    
    # Calculate lag and delta
    result = result.sort_values(per_col)
    result['lag_'] = result[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['Zone_Group', 'Corridor', per_col, var_col, wt_col, 'delta']]


def group_corridors(df: pd.DataFrame, 
                   per_col: str, 
                   var_col: str, 
                   wt_col: str) -> pd.DataFrame:
    """
    Group corridors by zone and zone group
    
    Args:
        df: Input DataFrame
        per_col: Period column name
        var_col: Variable column name
        wt_col: Weight column name
    
    Returns:
        Grouped DataFrame with all corridor summaries
    """
    
    # Get average for each Zone
    zone_groups = []
    for zone_group in df['Zone_Group'].unique():
        zone_df = df[df['Zone_Group'] == zone_group]
        zone_result = group_corridor_by(zone_df, per_col, var_col, wt_col, zone_group)
        zone_groups.append(zone_result)
    
    # Get average for each Zone_Group (without Zone column)
    df_no_zone = df.drop(columns=['Zone'], errors='ignore')
    zone_group_results = []
    for zone_group in df_no_zone['Zone_Group'].unique():
        zg_df = df_no_zone[df_no_zone['Zone_Group'] == zone_group]
        zg_result = group_corridor_by(zg_df, per_col, var_col, wt_col, zone_group)
        zone_group_results.append(zg_result)
    
    # Combine all results
    all_results = []
    
    # Original corridor data
    original_cols = ['Corridor', 'Zone_Group', per_col, var_col, wt_col, 'delta']
    if 'Zone' in df.columns:
        df_renamed = df.rename(columns={'Zone': 'Zone_Group'})
    else:
        df_renamed = df.copy()
    all_results.append(df_renamed[original_cols])
    
    # Zone group results
    all_results.extend(zone_group_results)
    
    # Concatenate and clean up
    result = pd.concat(all_results, ignore_index=True).drop_duplicates()
    result['Zone_Group'] = result['Zone_Group'].astype('category')
    result['Corridor'] = result['Corridor'].astype('category')
    
    return result


def get_hourly(df: pd.DataFrame, var_col: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Get hourly data with corridor information
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        corridors: Corridor mapping DataFrame
    
    Returns:
        DataFrame with hourly data and corridor info
    """
    
    result = df.merge(corridors, on='SignalID', how='outer').dropna(subset=['Corridor'])
    
    # Calculate lag and delta by SignalID
    result = result.sort_values(['SignalID', 'Hour'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    cols = ['SignalID', 'Hour', var_col, 'delta', 'Zone_Group', 'Zone', 'Corridor']
    if 'Subcorridor' in result.columns:
        cols.append('Subcorridor')
    
    return result[cols]


def get_period_avg(df: pd.DataFrame, 
                  var_col: str, 
                  per_col: str, 
                  wt_col: str = 'ones') -> pd.DataFrame:
    """
    Get period average for signals
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        per_col: Period column name
        wt_col: Weight column name
    
    Returns:
        DataFrame with period averages
    """
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    # Group by SignalID and period
    grouped = df_copy.groupby(['SignalID', per_col])
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = grouped.apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', per_col])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', per_col, var_col, wt_col, 'delta']]


def get_period_sum(df: pd.DataFrame, var_col: str, per_col: str) -> pd.DataFrame:
    """
    Get period sum for signals
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        per_col: Period column name
    
    Returns:
        DataFrame with period sums
    """
    
    # Group by SignalID and period
    result = df.groupby(['SignalID', per_col])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', per_col])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', per_col, var_col, 'delta']]


def get_daily_avg(df: pd.DataFrame, 
                 var_col: str, 
                 wt_col: str = 'ones', 
                 peak_only: bool = False) -> pd.DataFrame:
    """
    Get daily average for signals
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        wt_col: Weight column name
        peak_only: Whether to include only peak hours
    
    Returns:
        DataFrame with daily averages
    """
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    if peak_only:
        if 'Date_Hour' in df_copy.columns:
            df_copy = df_copy[df_copy['Date_Hour'].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
        elif 'Hour' in df_copy.columns:
            df_copy = df_copy[df_copy['Hour'].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
    
    # Group by SignalID and Date
    grouped = df_copy.groupby(['SignalID', 'Date'])
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = grouped.apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Date'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', 'Date', var_col, wt_col, 'delta']]


def get_daily_sum(df: pd.DataFrame, var_col: str) -> pd.DataFrame:
    """
    Get daily sum for signals
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
    
    Returns:
        DataFrame with daily sums
    """
    
    # Group by SignalID and Date
    result = df.groupby(['SignalID', 'Date'])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Date'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', 'Date', var_col, 'delta']]


def get_tuesdays(df: pd.DataFrame) -> pd.DataFrame:
    """
    Get Tuesday dates for each week
    
    Args:
        df: DataFrame with Date column
    
    Returns:
        DataFrame mapping Week to Tuesday dates
    """
    
    df_copy = df.copy()
    if 'Date' not in df_copy.columns:
        raise ValueError("DataFrame must have 'Date' column")
    
    df_copy['Week'] = df_copy['Date'].dt.isocalendar().week
    df_copy['Year'] = df_copy['Date'].dt.year
    
    # Find Tuesday of each week
    tuesdays = []
    for (year, week), group in df_copy.groupby(['Year', 'Week']):
        # Find the Tuesday of this week
        monday = group['Date'].min() - pd.Timedelta(days=group['Date'].min().weekday())
        tuesday = monday + pd.Timedelta(days=1)
        tuesdays.append({'Week': week, 'Date': tuesday})
    
    return pd.DataFrame(tuesdays).drop_duplicates()


def get_weekly_avg_by_day(df: pd.DataFrame, 
                         var_col: str, 
                         wt_col: str = 'ones', 
                         peak_only: bool = True) -> pd.DataFrame:
    """
    Get weekly average by day
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        wt_col: Weight column name
        peak_only: Whether to include only peak hours
    
    Returns:
        DataFrame with weekly averages by day
    """
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    # Get Tuesdays for week mapping
    tuesdays = get_tuesdays(df_copy)
    
    if peak_only:
        if 'Date_Hour' in df_copy.columns:
            df_copy = df_copy[df_copy['Date_Hour'].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
    
    # Add Week column
    df_copy['Week'] = df_copy['Date'].dt.isocalendar().week
    
    # Complete weeks for all signal/phase combinations
    signal_phases = df_copy[['SignalID', 'CallPhase']].drop_duplicates()
    weeks = pd.DataFrame({'Week': range(df_copy['Week'].min(), df_copy['Week'].max() + 1)})
    complete_grid = signal_phases.merge(weeks, how='cross')
    
    df_complete = complete_grid.merge(df_copy, on=['SignalID', 'CallPhase', 'Week'], how='left')
    
    # First aggregation: by SignalID, CallPhase, Week (mean over days in week)
    grouped1 = df_complete.groupby(['SignalID', 'CallPhase', 'Week'])
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    weekly_by_phase = grouped1.apply(weighted_avg).reset_index()
    
    # Second aggregation: by SignalID, Week (mean of phases)
    grouped2 = weekly_by_phase.groupby(['SignalID', 'Week'])
    weekly_by_signal = grouped2.apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    weekly_by_signal = weekly_by_signal.sort_values(['SignalID', 'Week'])
    weekly_by_signal['lag_'] = weekly_by_signal.groupby('SignalID')[var_col].shift(1)
    weekly_by_signal['delta'] = (weekly_by_signal[var_col] - weekly_by_signal['lag_']) / weekly_by_signal['lag_']
    
    # Join with Tuesday dates
    result = weekly_by_signal.merge(tuesdays, on='Week', how='left')
    
    return result[['SignalID', 'Date', 'Week', var_col, wt_col, 'delta']]


def get_monthly_avg_by_day(df: pd.DataFrame, 
                          var_col: str, 
                          wt_col: Optional[str] = None, 
                          peak_only: bool = False) -> pd.DataFrame:
    """
    Get monthly average by day
    
    Args:
        df: Input DataFrame
        var_col: Variable column name
        wt_col: Weight column name (optional)
        peak_only: Whether to include only peak hours
    
    Returns:
        DataFrame with monthly averages by day
    """
    
    df_copy = df.copy()
    
    if peak_only:
        if 'Date_Hour' in df_copy.columns:
            df_copy = df_copy[df_copy['Date_Hour'].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
    
    # Create Month column (first day of month)
    df_copy['Month'] = df_copy['Date'].dt.to_period('M').dt.start_time
    
    current_month = df_copy['Month'].max()
    
    # Complete months for all signal/phase combinations
    signal_phases = df_copy[['SignalID', 'CallPhase']].drop_duplicates()
    months = pd.date_range(df_copy['Month'].min(), current_month, freq='MS')
    month_df = pd.DataFrame({'Month': months})
    complete_grid = signal_phases.merge(month_df, how='cross')
    
    df_complete = complete_grid.merge(df_copy, on=['SignalID', 'CallPhase', 'Month'], how='left')
    
    if wt_col is None:
        # Simple mean aggregation
        # First by SignalID, Month, CallPhase
        grouped1 = df_complete.groupby(['SignalID', 'Month', 'CallPhase'])
        monthly_by_phase = grouped1[var_col].mean().reset_index()
        
        # Then by SignalID, Month (sum over phases)
        grouped2 = monthly_by_phase.groupby(['SignalID', 'Month'])
        result = grouped2[var_col].sum().reset_index()
        
    else:
        # Weighted mean aggregation
        def weighted_avg(group):
            weights = group[wt_col]
            values = group[var_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({var_col: np.nan, wt_col: 0})
            
            return pd.Series({
                var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                wt_col: weights[valid_mask].sum()
            })
        
        # First by SignalID, Month, CallPhase
        grouped1 = df_complete.groupby(['SignalID', 'Month', 'CallPhase'])
        monthly_by_phase = grouped1.apply(weighted_avg).reset_index()
        
        # Then by SignalID, Month (weighted mean of phases)
        grouped2 = monthly_by_phase.groupby(['SignalID', 'Month'])
        result = grouped2.apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Month'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    # Remove lag_ column and filter out NaN months
    cols = ['SignalID', 'Month', var_col, 'delta']
    if wt_col is not None:
        cols.insert(-1, wt_col)
    
    return result[cols].dropna(subset=['Month'])


def get_cor_monthly_avg_by_day(df: pd.DataFrame, 
                              corridors: pd.DataFrame, 
                              var_col: str, 
                              wt_col: str = 'ones') -> pd.DataFrame:
    """
    Get corridor monthly average by day
    
    Args:
        df: Input DataFrame
        corridors: Corridor mapping DataFrame
        var_col: Variable column name
        wt_col: Weight column name
    
    Returns:
        DataFrame with corridor monthly averages
    """
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df_copy, 'Month', corridors, var_col, wt_col)
    cor_df_out = cor_df_out[~pd.isna(cor_df_out[var_col])]
    
    return group_corridors(cor_df_out, 'Month', var_col, wt_col)


def get_vph(counts: pd.DataFrame, 
           interval: str = '1 hour', 
           mainline_only: bool = True) -> pd.DataFrame:
    """
    Get vehicles per hour from counts data
    
    Args:
        counts: Counts DataFrame
        interval: Time interval for grouping
        mainline_only: Whether to include only mainline phases (2,6)
    
    Returns:
        DataFrame with VPH data
    """
    
    df = counts.copy()
    
    if mainline_only:
        df = df[df['CallPhase'].isin([2, 6])]
    
    # Find the datetime column
    datetime_cols = [col for col in df.columns if df[col].dtype == 'datetime64[ns]']
    if datetime_cols:
        per_col = datetime_cols[0]
        if per_col != 'Timeperiod':
            df = df.rename(columns={per_col: 'Timeperiod'})
    else:
        raise ValueError("No datetime column found")
    
    # Add time-based columns
    df['DOW'] = df['Timeperiod'].dt.dayofweek + 1  # 1=Monday, 7=Sunday
    df['Week'] = df['Timeperiod'].dt.isocalendar().week
    
    # Group to interval
    if interval == '1 hour':
        df['Timeperiod'] = df['Timeperiod'].dt.floor('H')
    else:
        df['Timeperiod'] = df['Timeperiod'].dt.floor(interval)
    
    # Sum by SignalID, CallPhase, Week, DOW, Timeperiod
    grouped = df.groupby(['SignalID', 'CallPhase', 'Week', 'DOW', 'Timeperiod'])
    vol_by_phase = grouped['vol'].sum().reset_index()
    
    # Calculate lag and delta
    vol_by_phase = vol_by_phase.sort_values(['SignalID', 'CallPhase', 'Timeperiod'])
    vol_by_phase['lag_'] = vol_by_phase.groupby(['SignalID', 'CallPhase'])['vol'].shift(1)
    vol_by_phase['delta'] = (vol_by_phase['vol'] - vol_by_phase['lag_']) / vol_by_phase['lag_']
    
    # Sum across phases (for mainline)
    result = vol_by_phase.groupby(['SignalID', 'Week', 'DOW', 'Timeperiod'])['vol'].sum().reset_index()
    result = result.rename(columns={'vol': 'vph'})
    
    if interval == '1 hour':
        result = result.rename(columns={'Timeperiod': 'Hour'})
    
    return result


def get_quarterly(monthly_df: pd.DataFrame, 
                 var_col: str, 
                 wt_col: str = 'ones', 
                 operation: str = 'avg') -> pd.DataFrame:
    """
    Convert monthly data to quarterly
    
    Args:
        monthly_df: Monthly DataFrame
        var_col: Variable column name
        wt_col: Weight column name
        operation: Type of operation ('avg', 'sum', 'latest')
    
    Returns:
        DataFrame with quarterly data
    """
    
    df = monthly_df.copy()
    
    # Add ones column if needed
    if wt_col == 'ones' and 'ones' not in df.columns:
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
            weights = group[wt_col]
            values = group[var_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({var_col: np.nan, wt_col: 0})
            
            return pd.Series({
                var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                wt_col: weights[valid_mask].sum()
            })
        
        quarterly_df = grouped.apply(weighted_avg).reset_index()
        
    elif operation == 'sum':
        # Sum
        quarterly_df = grouped[var_col].sum().reset_index()
        
    elif operation == 'latest':
        # Latest value in quarter
        def get_latest(group):
            latest_month = group['Month'].max()
            latest_row = group[group['Month'] == latest_month].iloc[0]
            return pd.Series({
                var_col: latest_row[var_col],
                wt_col: latest_row[wt_col] if wt_col in latest_row else np.nan
            })
        
        quarterly_df = grouped.apply(get_latest).reset_index()
    
    else:
        raise ValueError(f"Unknown operation: {operation}")
    
    # Calculate lag and delta
    quarterly_df = quarterly_df.sort_values(['Zone_Group', 'Corridor', 'Quarter'])
    quarterly_df['lag_'] = quarterly_df.groupby(['Zone_Group', 'Corridor'])[var_col].shift(1)
    quarterly_df['delta'] = (quarterly_df[var_col] - quarterly_df['lag_']) / quarterly_df['lag_']
    
    # Select output columns
    cols = ['Corridor', 'Zone_Group', 'Quarter', var_col, 'delta']
    if wt_col in quarterly_df.columns:
        cols.insert(-1, wt_col)
    
    return quarterly_df[cols].drop(columns=['lag_'])


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
        
        signal_transformed = signal_df.drop(
            columns=[col for col in signal_df.columns if 'Subcorridor' in col or 'Zone_Group' in col],
            errors='ignore'
        )
        
        signal_transformed = signal_transformed.merge(
            corridors_cam[['CameraID', 'Corridor', 'Name']].drop_duplicates(),
            on=['Corridor', 'CameraID'], how='left'
        )
        signal_transformed = signal_transformed.rename(columns={
            'Corridor': 'Zone_Group',
            'CameraID': 'Corridor'
        })
        
        if 'Description' not in signal_transformed.columns:
            signal_transformed['Description'] = signal_transformed['Corridor']
        signal_transformed['Description'] = signal_transformed['Description'].fillna(
            signal_transformed['Corridor']
        )
        
    else:
        raise ValueError("Identifier must be 'SignalID' or 'CameraID'")
    
    # Transform corridor data
    corridor_transformed = corridor_df[
        corridor_df['Corridor'].isin(signal_transformed['Zone_Group'].unique())
    ].copy()
    
    corridor_transformed['Zone_Group'] = corridor_transformed['Corridor']
    corridor_transformed['Description'] = corridor_transformed['Corridor']
    
    # Remove subcorridor columns if they exist
    subcorridor_cols = [col for col in corridor_transformed.columns if 'Subcorridor' in col]
    if subcorridor_cols:
        corridor_transformed = corridor_transformed.drop(columns=subcorridor_cols)
    
    # Combine signal and corridor data
    result = pd.concat([signal_transformed, corridor_transformed], ignore_index=True)
    
    # Convert to categorical
    result['Corridor'] = result['Corridor'].astype('category')
    result['Description'] = result['Description'].astype('category')
    
    if 'Zone_Group' in result.columns:
        result['Zone_Group'] = result['Zone_Group'].astype('category')
    
    # Sort by appropriate column
    if period_col:
        if period_col == 'Month':
            result = result.sort_values(['Zone_Group', 'Corridor', 'Month'])
        elif period_col == 'Hour':
            result = result.sort_values(['Zone_Group', 'Corridor', 'Hour'])
        elif period_col == 'Timeperiod':
            result = result.sort_values(['Zone_Group', 'Corridor', 'Timeperiod'])
        elif period_col == 'Date':
            result = result.sort_values(['Zone_Group', 'Corridor', 'Date'])
    
    return result


def get_daily_detector_uptime(filtered_counts: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Calculate daily detector uptime
    
    Args:
        filtered_counts: Filtered counts DataFrame
    
    Returns:
        Dictionary with setback and presence uptime DataFrames
    """
    
    # Remove time periods with no volume on all detectors
    bad_comms = filtered_counts.groupby(['SignalID', 'Timeperiod'])['vol'].sum().reset_index()
    bad_comms = bad_comms[bad_comms['vol'] == 0][['SignalID', 'Timeperiod']]
    
    fc = filtered_counts.merge(bad_comms, on=['SignalID', 'Timeperiod'], how='left', indicator=True)
    fc = fc[fc['_merge'] == 'left_only'].drop(columns=['_merge'])
    
    if fc.empty:
        return {}
    
    ddu = fc.copy()
    ddu['Date_Hour'] = ddu['Timeperiod']
    ddu['Date'] = ddu['Date_Hour'].dt.date
    
    ddu = ddu[['SignalID', 'CallPhase', 'Detector', 'Date', 'Date_Hour', 'Good_Day']]
    
    if len(ddu) == 0:
        return {}
    
    # Classify detectors
    ddu['setback'] = ddu['CallPhase'].apply(lambda x: 'Setback' if x in [2, 6] else 'Presence')
    ddu['setback'] = ddu['setback'].astype('category')
    ddu['SignalID'] = ddu['SignalID'].astype('category')
    
    # Split by setback type
    setback_data = ddu[ddu['setback'] == 'Setback']
    presence_data = ddu[ddu['setback'] == 'Presence']
    
    result = {}
    
    for name, data in [('Setback', setback_data), ('Presence', presence_data)]:
        if not data.empty:
            uptime_calc = data.groupby(['SignalID', 'CallPhase', 'Date', 'Date_Hour', 'setback']).agg({
                'Good_Day': 'sum',
                'Detector': 'count'
            }).reset_index()
            
            uptime_calc = uptime_calc.rename(columns={'Detector': 'all'})
            uptime_calc['uptime'] = uptime_calc['Good_Day'] / uptime_calc['all']
            uptime_calc['all'] = uptime_calc['all'].astype(float)
            
            result[name] = uptime_calc
    
    return result


def get_avg_daily_detector_uptime(ddu: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Get average daily detector uptime
    
    Args:
        ddu: Dictionary with detector uptime data
    
    Returns:
        DataFrame with average daily detector uptime
    """
    
    if not ddu:
        return pd.DataFrame()
    
    results = []
    
    # Process setback data
    if 'Setback' in ddu:
        sb_data = ddu['Setback']
        sb_daily_uptime = get_daily_avg(sb_data, 'uptime', 'all', peak_only=False)
        sb_daily_uptime = sb_daily_uptime.rename(columns={
            'uptime': 'uptime.sb',
            'all': 'all.sb'
        })
        results.append(sb_daily_uptime)
    
    # Process presence data
    if 'Presence' in ddu:
        pr_data = ddu['Presence']
        pr_daily_uptime = get_daily_avg(pr_data, 'uptime', 'all', peak_only=False)
        pr_daily_uptime = pr_daily_uptime.rename(columns={
            'uptime': 'uptime.pr',
            'all': 'all.pr'
        })
        results.append(pr_daily_uptime)
    
    # Process all data
    if ddu:
        all_data = pd.concat(list(ddu.values()), ignore_index=True)
        all_daily_uptime = get_daily_avg(all_data, 'uptime', 'all', peak_only=False)
        results.append(all_daily_uptime)
    
    # Merge all results
    if len(results) == 1:
        return results[0]
    
    final_result = results[0]
    for result in results[1:]:
        final_result = final_result.merge(result, on=['SignalID', 'Date'], how='outer')
    
    # Clean up delta columns
    delta_cols = [col for col in final_result.columns if col.startswith('delta.')]
    if delta_cols:
        final_result = final_result.drop(columns=delta_cols)
    
    return final_result


# Specific metric functions that use the generic aggregation functions
def get_weekly_vpd(vpd: pd.DataFrame) -> pd.DataFrame:
    """Get weekly vehicles per day"""
    vpd_filtered = vpd[vpd['DOW'].isin([TUE, WED, THU])]
    return get_weekly_sum_by_day(vpd_filtered, 'vpd')


def get_weekly_sum_by_day(df: pd.DataFrame, var_col: str) -> pd.DataFrame:
    """Get weekly sum by day"""
    tuesdays = get_tuesdays(df)
    
    df_with_week = df.copy()
    df_with_week['Week'] = df_with_week['Date'].dt.isocalendar().week
    
    # Group by SignalID, Week, CallPhase and take mean over 3 days
    grouped1 = df_with_week.groupby(['SignalID', 'Week', 'CallPhase'])[var_col].mean().reset_index()
    
    # Sum over phases
    grouped2 = grouped1.groupby(['SignalID', 'Week'])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    grouped2 = grouped2.sort_values(['SignalID', 'Week'])
    grouped2['lag_'] = grouped2.groupby('SignalID')[var_col].shift(1)
    grouped2['delta'] = (grouped2[var_col] - grouped2['lag_']) / grouped2['lag_']
    
    # Join with Tuesday dates
    result = grouped2.merge(tuesdays, on='Week', how='left')
    
    return result[['SignalID', 'Date', 'Week', var_col, 'delta']]


def get_monthly_vpd(vpd: pd.DataFrame) -> pd.DataFrame:
    """Get monthly vehicles per day"""
    vpd_filtered = vpd[vpd['DOW'].isin([TUE, WED, THU])]
    return get_monthly_avg_by_day(vpd_filtered, 'vpd', peak_only=False)


def get_cor_monthly_vpd(monthly_vpd: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly vehicles per day"""
    return get_cor_monthly_avg_by_day(monthly_vpd, corridors, 'vpd')


def get_monthly_aog_by_day(daily_aog: pd.DataFrame) -> pd.DataFrame:
    """Get monthly arrival on green by day"""
    return get_monthly_avg_by_day(daily_aog, 'aog', 'vol', peak_only=True)


def get_cor_monthly_aog_by_day(monthly_aog_by_day: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly arrival on green by day"""
    return get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, 'aog', 'vol')


def get_monthly_vph(vph: pd.DataFrame) -> pd.DataFrame:
    """Get monthly vehicles per hour"""
    # Filter to Tuesday, Wednesday, Thursday
    vph_filtered = vph[vph['DOW'].isin([TUE, WED, THU])]
    
    # Sum by SignalID and Hour
    grouped1 = vph_filtered.groupby(['SignalID', 'Hour'])['vph'].sum().reset_index()
    
    # Convert Hour to month-day format (first of month)
    grouped1['Hour'] = grouped1['Hour'] - pd.to_timedelta(grouped1['Hour'].dt.day - 1, unit='D')
    
    # Average by SignalID and Hour
    result = grouped1.groupby(['SignalID', 'Hour'])['vph'].mean().reset_index()
    
    return result


def get_cor_monthly_vph(monthly_vph: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly vehicles per hour"""
    return get_cor_monthly_avg_by_hr(monthly_vph, corridors, 'vph')


def get_cor_monthly_avg_by_hr(df: pd.DataFrame, 
                             corridors: pd.DataFrame, 
                             var_col: str, 
                             wt_col: str = 'ones') -> pd.DataFrame:
    """Get corridor monthly average by hour"""
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, 'Hour', corridors, var_col, wt_col)
    return group_corridors(cor_df_out, 'Hour', var_col, wt_col)


# Travel time index functions
def get_cor_monthly_ti_by_hr(ti: pd.DataFrame, 
                            cor_monthly_vph: pd.DataFrame, 
                            corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly travel time index by hour"""
    df = get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    # Determine the travel time index column
    if 'tti' in ti.columns:
        tindx = 'tti'
    elif 'pti' in ti.columns:
        tindx = 'pti'
    elif 'bi' in ti.columns:
        tindx = 'bi'
    elif 'speed_mph' in ti.columns:
        tindx = 'speed_mph'
    else:
        raise ValueError("No recognized travel time index column found")
    
    return get_cor_monthly_avg_by_hr(df, corridors, tindx, 'pct')


def get_cor_monthly_ti(ti: pd.DataFrame, 
                      cor_monthly_vph: pd.DataFrame, 
                      corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly travel time index"""
    
    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist = cor_monthly_vph.copy()
    day_dist['month'] = day_dist['Hour'].dt.month
    
    # Calculate percentage by hour within each month
    day_dist = day_dist.groupby(['Zone_Group', 'Zone', 'Corridor', 'month']).apply(
        lambda x: x.assign(pct=x['vph'] / x['vph'].sum())
    ).reset_index(drop=True)
    
    # Average percentage by hour across all months
    day_dist = day_dist.groupby(['Zone_Group', 'Zone', 'Corridor', day_dist['Hour'].dt.hour]).agg({
        'pct': 'mean'
    }).reset_index()
    day_dist = day_dist.rename(columns={'Hour': 'hr'})
    
    # Join travel time data with corridors
    result = ti.merge(
        corridors[['Zone_Group', 'Zone', 'Corridor']].drop_duplicates(),
        on=['Zone_Group', 'Zone', 'Corridor'],
        how='left'
    )
    
    # Add hour column and join with day distribution
    result['hr'] = result['Hour'].dt.hour
    result = result.merge(day_dist, on=['Zone_Group', 'Zone', 'Corridor', 'hr'], how='left')
    
    # Fill missing percentages with 1
    result['pct'] = result['pct'].fillna(1)
    
    return result


def get_cor_monthly_ti_by_day(ti: pd.DataFrame, 
                             cor_monthly_vph: pd.DataFrame, 
                             corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly travel time index by day"""
    df = get_cor_monthly_ti(ti, cor_monthly_vph, corridors)
    
    # Determine the travel time index column
    if 'tti' in ti.columns:
        tindx = 'tti'
    elif 'pti' in ti.columns:
        tindx = 'pti'
    elif 'bi' in ti.columns:
        tindx = 'bi'
    elif 'speed_mph' in ti.columns:
        tindx = 'speed_mph'
    else:
        raise ValueError("No recognized travel time index column found")
    
    # Convert Hour to Date (Month)
    df['Month'] = df['Hour'].dt.date
    
    return get_cor_monthly_avg_by_day(df, corridors, tindx, 'pct')


def get_weekly_vph(vph: pd.DataFrame) -> pd.DataFrame:
    """Get weekly vehicles per hour"""
    vph_filtered = vph[vph['DOW'].isin([TUE, WED, THU])]
    return get_weekly_avg_by_hr(vph_filtered, 'vph')


def get_weekly_avg_by_hr(df: pd.DataFrame, 
                        var_col: str, 
                        wt_col: Optional[str] = None) -> pd.DataFrame:
    """Get weekly average by hour"""
    
    # Get Tuesday dates for each week
    df_with_date = df.copy()
    if 'Date' not in df_with_date.columns:
        df_with_date['Date'] = df_with_date['Hour'].dt.date
    
    tuesdays = get_tuesdays(df_with_date)
    
    # Remove Date column and join with Tuesdays
    df_no_date = df.drop(columns=['Date'], errors='ignore')
    df_with_tuesday = df_no_date.merge(tuesdays, on='Week', how='left').dropna(subset=['Date'])
    
    # Adjust Hour to match the Tuesday date
    df_with_tuesday['Hour'] = df_with_tuesday['Hour'].copy()
    df_with_tuesday['Hour'] = df_with_tuesday['Hour'].dt.time  # Keep only time
    df_with_tuesday['Hour'] = pd.to_datetime(
        df_with_tuesday['Date'].astype(str) + ' ' + df_with_tuesday['Hour'].astype(str)
    )
    
    if wt_col is None:
        # Simple mean
        # First by SignalID, Week, Hour, CallPhase (mean over 3 days in week)
        grouped1 = df_with_tuesday.groupby(['SignalID', 'Week', 'Hour', 'CallPhase'])[var_col].mean().reset_index()
        
        # Then by SignalID, Week, Hour (mean of phases)
        result = grouped1.groupby(['SignalID', 'Week', 'Hour'])[var_col].mean().reset_index()
        
        # Calculate lag and delta
        result = result.sort_values(['SignalID', 'Hour'])
        result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
        result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
        
        return result[['SignalID', 'Hour', 'Week', var_col, 'delta']]
        
    else:
        # Weighted mean
        def weighted_avg(group):
            weights = group[wt_col]
            values = group[var_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({var_col: np.nan, wt_col: 0})
            
            return pd.Series({
                var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                wt_col: weights[valid_mask].sum()
            })
        
        # First by SignalID, Week, Hour, CallPhase
        grouped1 = df_with_tuesday.groupby(['SignalID', 'Week', 'Hour', 'CallPhase'])
        weekly_by_phase = grouped1.apply(weighted_avg).reset_index()
        
        # Then by SignalID, Week, Hour
        grouped2 = weekly_by_phase.groupby(['SignalID', 'Week', 'Hour'])
        result = grouped2.apply(weighted_avg).reset_index()
        
        # Calculate lag and delta
        result = result.sort_values(['SignalID', 'Hour'])
        result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
        result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
        
        return result[['SignalID', 'Hour', 'Week', var_col, wt_col, 'delta']]


def get_cor_weekly_vph(weekly_vph: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly vehicles per hour"""
    return get_cor_weekly_avg_by_hr(weekly_vph, corridors, 'vph')


def get_cor_weekly_avg_by_hr(df: pd.DataFrame, 
                            corridors: pd.DataFrame, 
                            var_col: str, 
                            wt_col: str = 'ones') -> pd.DataFrame:
    """Get corridor weekly average by hour"""
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, 'Hour', corridors, var_col, wt_col)
    return group_corridors(cor_df_out, 'Hour', var_col, wt_col)


def get_monthly_vph_peak(monthly_vph: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Get monthly VPH during peak periods"""
    
    # AM peak
    am_data = monthly_vph[monthly_vph['Hour'].dt.hour.isin(AM_PEAK_HOURS)].copy()
    am_data['Date'] = am_data['Hour'].dt.date
    am_daily = get_daily_avg(am_data, 'vph')
    am_result = am_daily.rename(columns={'Date': 'Month'})
    
    # PM peak  
    pm_data = monthly_vph[monthly_vph['Hour'].dt.hour.isin(PM_PEAK_HOURS)].copy()
    pm_data['Date'] = pm_data['Hour'].dt.date
    pm_daily = get_daily_avg(pm_data, 'vph')
    pm_result = pm_daily.rename(columns={'Date': 'Month'})
    
    return {'am': am_result, 'pm': pm_result}


def get_cor_monthly_vph_peak(cor_monthly_vph: pd.DataFrame, 
                            corridors: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Get corridor monthly VPH during peak periods"""
    
    # AM peak
    am_data = cor_monthly_vph[cor_monthly_vph['Hour'].dt.hour.isin(AM_PEAK_HOURS)].copy()
    am_data['Month'] = am_data['Hour'].dt.date
    am_result = weighted_mean_by_corridor(am_data, 'Month', corridors, 'vph')
    am_result = am_result.drop(columns=['Zone'], errors='ignore')
    
    # PM peak
    pm_data = cor_monthly_vph[cor_monthly_vph['Hour'].dt.hour.isin(PM_PEAK_HOURS)].copy()
    pm_data['Month'] = pm_data['Hour'].dt.date
    pm_result = weighted_mean_by_corridor(pm_data, 'Month', corridors, 'vph')
    pm_result = pm_result.drop(columns=['Zone'], errors='ignore')
    
    return {'am': am_result, 'pm': pm_result}


def get_cor_weekly_cctv_uptime(daily_cctv_uptime: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly CCTV uptime"""
    
    df = daily_cctv_uptime.copy()
    df['DOW'] = df['Date'].dt.dayofweek + 1
    df['Week'] = df['Date'].dt.isocalendar().week
    
    tuesdays = get_tuesdays(df)
    
    # Remove Date and join with Tuesdays
    df_no_date = df.drop(columns=['Date'])
    df_with_tuesday = df_no_date.merge(tuesdays, on='Week', how='left')
    
    # Group by Date, Corridor, Zone_Group
    result = df_with_tuesday.groupby(['Date', 'Corridor', 'Zone_Group']).agg({
        'up': 'sum',
        'num': 'sum'
    }).reset_index()
    
    result['uptime'] = result['up'] / result['num']
    
    return result.dropna(subset=['Corridor'])


def get_cor_monthly_cctv_uptime(daily_cctv_uptime: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly CCTV uptime"""
    
    df = daily_cctv_uptime.copy()
    df['Month'] = df['Date'].dt.to_period('M').dt.start_time
    
    # Group by Month, Corridor, Zone_Group
    result = df.groupby(['Month', 'Corridor', 'Zone_Group']).agg({
        'up': 'sum',
        'num': 'sum'
    }).reset_index()
    
    result['uptime'] = result['up'] / result['num']
    
    return result.dropna(subset=['Corridor'])


def summarize_by_peak(df: pd.DataFrame, date_col: str) -> Dict[str, pd.DataFrame]:
    """
    Used to get peak period metrics from hourly results
    
    Args:
        df: Input DataFrame
        date_col: Name of the date/datetime column
    
    Returns:
        Dictionary with 'Peak' and 'Off_Peak' DataFrames
    """
    
    df_copy = df.copy()
    
    # Create Peak column
    hour_col = df_copy[date_col].dt.hour
    df_copy['Peak'] = np.where(
        hour_col.isin(AM_PEAK_HOURS + PM_PEAK_HOURS),
        'Peak',
        'Off_Peak'
    )
    df_copy['Peak'] = df_copy['Peak'].astype('category')
    
    # Split by peak/off-peak and summarize
    result = {}
    
    for peak_type in ['Peak', 'Off_Peak']:
        peak_data = df_copy[df_copy['Peak'] == peak_type]
        
        # Group by SignalID and Period (date part)
        peak_data['Period'] = peak_data[date_col].dt.date
        
        summary = peak_data.groupby(['SignalID', 'Period']).apply(
            lambda x: pd.Series({
                'sf_freq': np.average(x['sf_freq'], weights=x['cycles']) if 'sf_freq' in x.columns else np.nan,
                'cycles': x['cycles'].sum()
            })
        ).reset_index()
        
        result[peak_type] = summary.drop(columns=['Peak'], errors='ignore')
    
    return result


# Additional utility functions for specific metrics
def get_avg_by_hr(df: pd.DataFrame, var_col: str, wt_col: Optional[str] = None) -> pd.DataFrame:
    """Get average by hour using data.table-like operations"""
    
    df_copy = df.copy()
    
    # Add time-based columns
    df_copy['DOW'] = df_copy['Date_Hour'].dt.dayofweek + 1
    df_copy['Week'] = df_copy['Date_Hour'].dt.isocalendar().week
    df_copy['Hour'] = df_copy['Date_Hour'].dt.floor('H')
    
    if wt_col is None:
        # Simple mean
        result = df_copy.groupby(['SignalID', 'CallPhase', 'Week', 'DOW', 'Hour']).agg({
            var_col: 'mean'
        }).reset_index()
        result['ones'] = 1
        result = result.rename(columns={'ones': 'weight'})
        
    else:
        # Weighted mean
        def weighted_avg(group):
            weights = group[wt_col]
            values = group[var_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({var_col: np.nan, wt_col: 0})
            
            return pd.Series({
                var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                wt_col: weights[valid_mask].sum()
            })
        
        result = df_copy.groupby(['SignalID', 'CallPhase', 'Week', 'DOW', 'Hour']).apply(
            weighted_avg
        ).reset_index()
        
        # Calculate lag and delta
        result = result.sort_values(['SignalID', 'CallPhase', 'Hour'])
        result['lag_'] = result.groupby(['SignalID', 'CallPhase'])[var_col].shift(1)
        result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result


def get_aog_by_hr(aog: pd.DataFrame) -> pd.DataFrame:
    """Get arrival on green by hour"""
    return get_avg_by_hr(aog, 'aog', 'vol')


def get_pr_by_hr(pr: pd.DataFrame) -> pd.DataFrame:
    """Get progression ratio by hour"""
    return get_avg_by_hr(pr, 'pr', 'vol')


def get_sf_by_hr(sf: pd.DataFrame) -> pd.DataFrame:
    """Get split failures by hour"""
    return get_avg_by_hr(sf, 'sf_freq', 'cycles')


def get_qs_by_hr(qs: pd.DataFrame) -> pd.DataFrame:
    """Get queue spillback by hour"""
    return get_avg_by_hr(qs, 'qs_freq', 'cycles')


def get_monthly_aog_by_hr(aog_by_hr: pd.DataFrame) -> pd.DataFrame:
    """Get monthly arrival on green by hour"""
    # Group by SignalID and Hour
    grouped1 = aog_by_hr.groupby(['SignalID', 'Hour']).apply(
        lambda x: pd.Series({
            'aog': np.average(x['aog'], weights=x['vol']),
            'vol': x['vol'].sum()
        })
    ).reset_index()
    
    # Convert Hour to month-day format (first of month)
    grouped1['Hour'] = grouped1['Hour'] - pd.to_timedelta(grouped1['Hour'].dt.day - 1, unit='D')
    
    # Average by SignalID and Hour
    result = grouped1.groupby(['SignalID', 'Hour']).apply(
        lambda x: pd.Series({
            'aog': np.average(x['aog'], weights=x['vol']),
            'vol': x['vol'].sum()
        })
    ).reset_index()
    
    return result


def get_monthly_pr_by_hr(pr_by_hr: pd.DataFrame) -> pd.DataFrame:
    """Get monthly progression ratio by hour"""
    pr_renamed = pr_by_hr.rename(columns={'pr': 'aog'})
    result = get_monthly_aog_by_hr(pr_renamed)
    return result.rename(columns={'aog': 'pr'})


def get_monthly_sf_by_hr(sf_by_hr: pd.DataFrame) -> pd.DataFrame:
    """Get monthly split failures by hour"""
    sf_renamed = sf_by_hr.rename(columns={'sf_freq': 'aog', 'cycles': 'vol'})
    result = get_monthly_aog_by_hr(sf_renamed)
    return result.rename(columns={'aog': 'sf_freq', 'vol': 'cycles'})


def get_monthly_qs_by_hr(qs_by_hr: pd.DataFrame) -> pd.DataFrame:
    """Get monthly queue spillback by hour"""
    qs_renamed = qs_by_hr.rename(columns={'qs_freq': 'aog', 'cycles': 'vol'})
    result = get_monthly_aog_by_hr(qs_renamed)
    return result.rename(columns={'aog': 'qs_freq', 'vol': 'cycles'})


def get_cor_monthly_pr_by_hr(monthly_pr_by_hr: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly progression ratio by hour"""
    return get_cor_monthly_avg_by_hr(monthly_pr_by_hr, corridors, 'pr', 'vol')


def get_cor_monthly_sf_by_hr(monthly_sf_by_hr: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly split failures by hour"""
    return get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, 'sf_freq', 'cycles')


def get_cor_monthly_qs_by_hr(monthly_qs_by_hr: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly queue spillback by hour"""
    return get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, 'qs_freq', 'cycles')


def get_monthly_paph(paph: pd.DataFrame) -> pd.DataFrame:
    """Get monthly pedestrian activations per hour"""
    paph_renamed = paph.rename(columns={'paph': 'vph'})
    result = get_monthly_vph(paph_renamed)
    return result.rename(columns={'vph': 'paph'})


def get_cor_monthly_paph(monthly_paph: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly pedestrian activations per hour"""
    return get_cor_monthly_avg_by_hr(monthly_paph, corridors, 'paph')


def get_weekly_paph(paph: pd.DataFrame) -> pd.DataFrame:
    """Get weekly pedestrian activations per hour"""
    paph_filtered = paph[paph['DOW'].isin([TUE, WED, THU])]
    return get_weekly_avg_by_hr(paph_filtered, 'paph')


def get_cor_weekly_paph(weekly_paph: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly pedestrian activations per hour"""
    return get_cor_weekly_avg_by_hr(weekly_paph, corridors, 'paph')


def get_weekly_papd(papd: pd.DataFrame) -> pd.DataFrame:
    """Get weekly pedestrian activations per day"""
    papd_filtered = papd[papd['DOW'].isin([TUE, WED, THU])]
    return get_weekly_sum_by_day(papd_filtered, 'papd')


def get_monthly_papd(papd: pd.DataFrame) -> pd.DataFrame:
    """Get monthly pedestrian activations per day"""
    papd_filtered = papd[papd['DOW'].isin([TUE, WED, THU])]
    return get_monthly_avg_by_day(papd_filtered, 'papd', peak_only=False)


def get_cor_monthly_papd(monthly_papd: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly pedestrian activations per day"""
    return get_cor_monthly_avg_by_day(monthly_papd, corridors, 'papd')


def get_weekly_thruput(throughput: pd.DataFrame) -> pd.DataFrame:
    """Get weekly throughput"""
    return get_weekly_sum_by_day(throughput, 'vph')


def get_monthly_thruput(throughput: pd.DataFrame) -> pd.DataFrame:
    """Get monthly throughput"""
    return get_monthly_avg_by_day(throughput, 'vph', peak_only=False)


def get_cor_monthly_thruput(monthly_throughput: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly throughput"""
    return get_cor_monthly_avg_by_day(monthly_throughput, corridors, 'vph')


def get_weekly_aog_by_day(daily_aog: pd.DataFrame) -> pd.DataFrame:
    """Get weekly arrival on green by day"""
    return get_weekly_avg_by_day(daily_aog, 'aog', 'vol', peak_only=True)


def get_weekly_pr_by_day(daily_pr: pd.DataFrame) -> pd.DataFrame:
    """Get weekly progression ratio by day"""
    return get_weekly_avg_by_day(daily_pr, 'pr', 'vol', peak_only=True)


def get_weekly_sf_by_day(sf: pd.DataFrame) -> pd.DataFrame:
    """Get weekly split failures by day"""
    return get_weekly_avg_by_day(sf, 'sf_freq', 'cycles', peak_only=True)


def get_weekly_qs_by_day(qs: pd.DataFrame) -> pd.DataFrame:
    """Get weekly queue spillback by day"""
    return get_weekly_avg_by_day(qs, 'qs_freq', 'cycles', peak_only=True)


def get_monthly_pr_by_day(daily_pr: pd.DataFrame) -> pd.DataFrame:
    """Get monthly progression ratio by day"""
    return get_monthly_avg_by_day(daily_pr, 'pr', 'vol', peak_only=True)


def get_monthly_sf_by_day(sf: pd.DataFrame) -> pd.DataFrame:
    """Get monthly split failures by day"""
    return get_monthly_avg_by_day(sf, 'sf_freq', 'cycles', peak_only=True)


def get_monthly_qs_by_day(qs: pd.DataFrame) -> pd.DataFrame:
    """Get monthly queue spillback by day"""
    return get_monthly_avg_by_day(qs, 'qs_freq', 'cycles', peak_only=True)


def get_cor_monthly_pr_by_day(monthly_pr_by_day: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly progression ratio by day"""
    return get_cor_monthly_avg_by_day(monthly_pr_by_day, corridors, 'pr', 'vol')


def get_cor_monthly_sf_by_day(monthly_sf_by_day: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly split failures by day"""
    return get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, 'sf_freq', 'cycles')


def get_cor_monthly_qs_by_day(monthly_qs_by_day: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly queue spillback by day"""
    return get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, 'qs_freq', 'cycles')


def get_weekly_detector_uptime(avg_daily_detector_uptime: pd.DataFrame) -> pd.DataFrame:
    """Get weekly detector uptime"""
    df = avg_daily_detector_uptime.copy()
    df['CallPhase'] = 0
    df['Week'] = df['Date'].dt.isocalendar().week
    
    result = get_weekly_avg_by_day(df, 'uptime', 'all', peak_only=False)
    result['uptime'] = result['uptime'].fillna(0)
    
    return result.sort_values(['SignalID', 'Date'])


def get_monthly_detector_uptime(avg_daily_detector_uptime: pd.DataFrame) -> pd.DataFrame:
    """Get monthly detector uptime"""
    df = avg_daily_detector_uptime.copy()
    df['CallPhase'] = 0
    
    result = get_monthly_avg_by_day(df, 'uptime', 'all')
    
    return result.sort_values(['SignalID', 'Month'])


def get_cor_weekly_detector_uptime(avg_weekly_detector_uptime: pd.DataFrame, 
                                  corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly detector uptime"""
    return get_cor_weekly_avg_by_day(avg_weekly_detector_uptime, corridors, 'uptime', 'all')


def get_monthly_flashevent(flash: pd.DataFrame) -> pd.DataFrame:
    """Get monthly flash events"""
    flash_copy = flash[['SignalID', 'Date']].copy()
    
    # Set to first day of month
    flash_copy['Date'] = flash_copy['Date'].dt.to_period('M').dt.start_time
    
    # Count flash events by signal and month
    flash_grouped = flash_copy.groupby(['SignalID', 'Date']).size().reset_index(name='flash')
    
    # Add dummy CallPhase for compatibility with get_monthly_avg_by_day
    flash_grouped['CallPhase'] = 0
    
    return get_monthly_avg_by_day(flash_grouped, 'flash', peak_only=False)


def get_cor_monthly_flash(monthly_flash: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly flash events"""
    return get_cor_monthly_avg_by_day(monthly_flash, corridors, 'flash')


def get_cor_weekly_ti_by_day(ti: pd.DataFrame, 
                            cor_weekly_vph: pd.DataFrame, 
                            corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly travel time index by day"""
    df = get_cor_weekly_ti(ti, cor_weekly_vph, corridors)
    
    # Determine the travel time index column
    if 'tti' in ti.columns:
        tindx = 'tti'
    elif 'pti' in ti.columns:
        tindx = 'pti'
    elif 'bi' in ti.columns:
        tindx = 'bi'
    elif 'speed_mph' in ti.columns:
        tindx = 'speed_mph'
    else:
        raise ValueError("No recognized travel time index column found")
    
    # Convert Hour to Date
    df['Date'] = df['Hour'].dt.date
    
    return get_cor_weekly_avg_by_day(df, corridors, tindx, 'pct')


def get_cor_weekly_ti(ti: pd.DataFrame, 
                     cor_weekly_vph: pd.DataFrame, 
                     corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly travel time index"""
    
    # Add Tuesday dates to weekly VPH
    tuesdays = get_tuesdays(cor_weekly_vph.assign(Date=cor_weekly_vph['Hour'].dt.date))
    cor_weekly_vph_with_week = cor_weekly_vph.copy()
    cor_weekly_vph_with_week['Week'] = cor_weekly_vph_with_week['Hour'].dt.isocalendar().week
    cor_weekly_vph_with_dates = cor_weekly_vph_with_week.merge(tuesdays, on='Week', how='left')
    
    # Get share of volume (as pct) by hour over the day
    day_dist = cor_weekly_vph_with_dates.groupby(['Zone_Group', 'Zone', 'Corridor', 'Date']).apply(
        lambda x: x.assign(pct=x['vph'] / x['vph'].sum())
    ).reset_index(drop=True)
    
    # Average percentage by hour
    day_dist = day_dist.groupby(['Zone_Group', 'Zone', 'Corridor', day_dist['Hour'].dt.hour]).agg({
        'pct': 'mean'
    }).reset_index()
    day_dist = day_dist.rename(columns={'Hour': 'hr'})
    
    # Join travel time data with corridors
    result = ti.merge(
        corridors[['Zone_Group', 'Zone', 'Corridor']].drop_duplicates(),
        on=['Zone_Group', 'Zone', 'Corridor'],
        how='left'
    )
    # Add hour column and join with day distribution
    result['hr'] = result['Hour'].dt.hour
    result = result.merge(day_dist, on=['Zone_Group', 'Zone', 'Corridor', 'hr'], how='left')
    
    # Fill missing percentages with 1
    result['pct'] = result['pct'].fillna(1)
    
    return result


def get_weekly_vph_peak(weekly_vph: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Get weekly VPH during peak periods"""
    dfs = get_monthly_vph_peak(weekly_vph)
    return {
        'am': dfs['am'].rename(columns={'Month': 'Date'}),
        'pm': dfs['pm'].rename(columns={'Month': 'Date'})
    }


def get_cor_weekly_vph_peak(cor_weekly_vph: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """Get corridor weekly VPH during peak periods"""
    dfs = get_cor_monthly_vph_peak(cor_weekly_vph, corridors)
    return {
        'am': dfs['am'].rename(columns={'Month': 'Date'}),
        'pm': dfs['pm'].rename(columns={'Month': 'Date'})
    }


# Additional corridor aggregation functions
def get_cor_weekly_vpd(weekly_vpd: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly vehicles per day"""
    return get_cor_weekly_avg_by_day(weekly_vpd, corridors, 'vpd')


def get_cor_weekly_papd(weekly_papd: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly pedestrian activations per day"""
    return get_cor_weekly_avg_by_day(weekly_papd, corridors, 'papd')


def get_cor_weekly_thruput(weekly_throughput: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly throughput"""
    return get_cor_weekly_avg_by_day(weekly_throughput, corridors, 'vph')


def get_cor_weekly_aog_by_day(weekly_aog: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly arrival on green by day"""
    return get_cor_weekly_avg_by_day(weekly_aog, corridors, 'aog', 'vol')


def get_cor_weekly_pr_by_day(weekly_pr: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly progression ratio by day"""
    return get_cor_weekly_avg_by_day(weekly_pr, corridors, 'pr', 'vol')


def get_cor_weekly_sf_by_day(weekly_sf: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly split failures by day"""
    return get_cor_weekly_avg_by_day(weekly_sf, corridors, 'sf_freq', 'cycles')


def get_cor_weekly_qs_by_day(weekly_qs: pd.DataFrame, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor weekly queue spillback by day"""
    return get_cor_weekly_avg_by_day(weekly_qs, corridors, 'qs_freq', 'cycles')


def get_cor_weekly_avg_by_day(df: pd.DataFrame, 
                             corridors: pd.DataFrame, 
                             var_col: str, 
                             wt_col: str = 'ones') -> pd.DataFrame:
    """Get corridor weekly average by day"""
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, 'Date', corridors, var_col, wt_col)
    cor_df_out = cor_df_out[~pd.isna(cor_df_out[var_col])]
    
    result = group_corridors(cor_df_out, 'Date', var_col, wt_col)
    result['Week'] = result['Date'].dt.isocalendar().week
    
    return result


def get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime: pd.DataFrame, 
                                     corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor average daily detector uptime"""
    
    results = []
    
    # Setback uptime
    if 'uptime.sb' in avg_daily_detector_uptime.columns:
        sb_data = avg_daily_detector_uptime.dropna(subset=['uptime.sb'])
        cor_daily_sb_uptime = get_cor_weekly_avg_by_day(sb_data, corridors, 'uptime.sb', 'all.sb')
        results.append(cor_daily_sb_uptime.drop(columns=['all.sb', 'delta'], errors='ignore'))
    
    # Presence uptime
    if 'uptime.pr' in avg_daily_detector_uptime.columns:
        pr_data = avg_daily_detector_uptime.dropna(subset=['uptime.pr'])
        cor_daily_pr_uptime = get_cor_weekly_avg_by_day(pr_data, corridors, 'uptime.pr', 'all.pr')
        results.append(cor_daily_pr_uptime.drop(columns=['all.pr', 'delta'], errors='ignore'))
    
    # All detector uptime
    if 'uptime' in avg_daily_detector_uptime.columns:
        all_data = avg_daily_detector_uptime.dropna(subset=['uptime'])
        # Use 'ones' instead of 'all' to treat all signals equally
        cor_daily_all_uptime = get_cor_weekly_avg_by_day(all_data, corridors, 'uptime', 'ones')
        results.append(cor_daily_all_uptime.drop(columns=['ones', 'delta'], errors='ignore'))
    
    # Merge all results
    if len(results) == 0:
        return pd.DataFrame()
    
    final_result = results[0]
    for result in results[1:]:
        final_result = final_result.merge(result, on=['Zone_Group', 'Corridor', 'Date'], how='outer')
    
    final_result['Zone_Group'] = final_result['Zone_Group'].astype('category')
    
    return final_result


def get_cor_monthly_detector_uptime(avg_daily_detector_uptime: pd.DataFrame, 
                                   corridors: pd.DataFrame) -> pd.DataFrame:
    """Get corridor monthly detector uptime"""
    
    results = []
    
    # Setback uptime
    if 'uptime.sb' in avg_daily_detector_uptime.columns:
        sb_data = avg_daily_detector_uptime.dropna(subset=['uptime.sb']).copy()
        sb_data['Month'] = sb_data['Date'].dt.to_period('M').dt.start_time
        cor_monthly_sb_uptime = get_cor_monthly_avg_by_day(sb_data, corridors, 'uptime.sb', 'all.sb')
        results.append(cor_monthly_sb_uptime.drop(columns=['all.sb', 'delta'], errors='ignore'))
    
    # Presence uptime
    if 'uptime.pr' in avg_daily_detector_uptime.columns:
        pr_data = avg_daily_detector_uptime.dropna(subset=['uptime.pr']).copy()
        pr_data['Month'] = pr_data['Date'].dt.to_period('M').dt.start_time
        cor_monthly_pr_uptime = get_cor_monthly_avg_by_day(pr_data, corridors, 'uptime.pr', 'all.pr')
        results.append(cor_monthly_pr_uptime.drop(columns=['all.pr', 'delta'], errors='ignore'))
    
    # All detector uptime
    if 'uptime' in avg_daily_detector_uptime.columns:
        all_data = avg_daily_detector_uptime.dropna(subset=['uptime']).copy()
        all_data['Month'] = all_data['Date'].dt.to_period('M').dt.start_time
        cor_monthly_all_uptime = get_cor_monthly_avg_by_day(all_data, corridors, 'uptime', 'all')
        results.append(cor_monthly_all_uptime.drop(columns=['all'], errors='ignore'))
    
    # Merge all results
    if len(results) == 0:
        return pd.DataFrame()
    
    final_result = results[0]
    for result in results[1:]:
        final_result = final_result.merge(result, on=['Zone_Group', 'Corridor', 'Month'], how='outer')
    
    final_result['Corridor'] = final_result['Corridor'].astype('category')
    final_result['Zone_Group'] = final_result['Zone_Group'].astype('category')
    
    return final_result


def get_sum_by_period(df: pd.DataFrame, var_col: str, interval: str) -> pd.DataFrame:
    """Get sum by time period"""
    
    df_copy = df.copy()
    
    # Add time-based columns
    df_copy['DOW'] = df_copy['Timeperiod'].dt.dayofweek + 1
    df_copy['Week'] = df_copy['Timeperiod'].dt.isocalendar().week
    
    # Floor to interval
    df_copy['Timeperiod'] = df_copy['Timeperiod'].dt.floor(interval)
    
    # Group and sum
    result = df_copy.groupby(['SignalID', 'CallPhase', 'Week', 'DOW', 'Timeperiod'])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'CallPhase', 'Timeperiod'])
    result['lag_'] = result.groupby(['SignalID', 'CallPhase'])[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result.drop(columns=['lag_'])


def get_period_avg(df: pd.DataFrame, var_col: str, per_col: str, wt_col: str = 'ones') -> pd.DataFrame:
    """Get period average"""
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    # Group by SignalID and period
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = df_copy.groupby(['SignalID', per_col]).apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', per_col])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', per_col, var_col, wt_col, 'delta']]


def get_period_sum(df: pd.DataFrame, var_col: str, per_col: str) -> pd.DataFrame:
    """Get period sum"""
    
    # Group by SignalID and period
    result = df.groupby(['SignalID', per_col])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', per_col])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', per_col, var_col, 'delta']]


def get_daily_avg(df: pd.DataFrame, var_col: str, wt_col: str = 'ones', peak_only: bool = False) -> pd.DataFrame:
    """Get daily average"""
    
    df_copy = df.copy()
    
    if wt_col == 'ones':
        df_copy['ones'] = 1
    
    if peak_only and 'Date_Hour' in df_copy.columns:
        df_copy = df_copy[df_copy['Date_Hour'].dt.hour.isin(AM_PEAK_HOURS + PM_PEAK_HOURS)]
    
    # Group by SignalID and Date
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = df_copy.groupby(['SignalID', 'Date']).apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Date'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', 'Date', var_col, wt_col, 'delta']]


def get_daily_sum(df: pd.DataFrame, var_col: str) -> pd.DataFrame:
    """Get daily sum"""
    
    # Group by SignalID and Date
    result = df.groupby(['SignalID', 'Date'])[var_col].sum().reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Date'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', 'Date', var_col, 'delta']]


def get_daily_avg_cctv(df: pd.DataFrame, var_col: str = 'uptime', wt_col: str = 'num', peak_only: bool = False) -> pd.DataFrame:
    """Get daily average for CCTV data"""
    
    # Group by CameraID and Date
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = df.groupby(['CameraID', 'Date']).apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    result = result.sort_values(['CameraID', 'Date'])
    result['lag_'] = result.groupby('CameraID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['CameraID', 'Date', var_col, wt_col, 'delta']]


def get_weekly_avg_by_day_cctv(df: pd.DataFrame, var_col: str = 'uptime', wt_col: str = 'num') -> pd.DataFrame:
    """Get weekly average by day for CCTV data"""
    
    df_copy = df.copy()
    df_copy['Week'] = df_copy['Date'].dt.isocalendar().week
    
    # Remove Date column and join with Tuesdays
    df_no_date = df_copy.drop(columns=['Date'])
    tuesdays = get_tuesdays(df_copy)
    df_with_tuesday = df_no_date.merge(tuesdays, on='Week', how='left')
    
    # Group by CameraID, Zone_Group, Zone, Corridor, Subcorridor, Description, Week
    group_cols = ['CameraID', 'Zone_Group', 'Zone', 'Corridor', 'Week']
    if 'Subcorridor' in df_with_tuesday.columns:
        group_cols.insert(-1, 'Subcorridor')
    if 'Description' in df_with_tuesday.columns:
        group_cols.insert(-1, 'Description')
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    # First aggregation
    weekly_avg = df_with_tuesday.groupby(group_cols).apply(weighted_avg).reset_index()
    
    # Second aggregation (removing some grouping columns)
    group_cols_2 = [col for col in group_cols if col not in ['Subcorridor', 'Description']]
    if 'Subcorridor' in group_cols:
        group_cols_2.append('Subcorridor')
    if 'Description' in group_cols:
        group_cols_2.append('Description')
    
    final_result = weekly_avg.groupby(group_cols_2).apply(weighted_avg).reset_index()
    
    # Calculate lag and delta
    final_result = final_result.sort_values(['CameraID', 'Week'])
    final_result['lag_'] = final_result.groupby('CameraID')[var_col].shift(1)
    final_result['delta'] = (final_result[var_col] - final_result['lag_']) / final_result['lag_']
    
    # Join with Tuesday dates
    final_result = final_result.merge(tuesdays, on='Week', how='left')
    
    # Select final columns
    result_cols = ['Zone_Group', 'Zone', 'Corridor', 'CameraID', 'Date', 'Week', var_col, wt_col, 'delta']
    if 'Subcorridor' in final_result.columns:
        result_cols.insert(3, 'Subcorridor')
    if 'Description' in final_result.columns:
        result_cols.insert(-4, 'Description')
    
    return final_result[result_cols]


# Helper function for getting VPH with different intervals
def get_vph(counts: pd.DataFrame, interval: str = '1 hour', mainline_only: bool = True) -> pd.DataFrame:
    """Get vehicles per hour"""
    
    if mainline_only:
        counts = counts[counts['CallPhase'].isin([2, 6])]
    
    # Determine timeperiod column
    per_cols = [col for col in counts.columns if counts[col].dtype == 'datetime64[ns]']
    if per_cols:
        per_col = per_cols[0]
        if per_col != 'Timeperiod':
            counts = counts.rename(columns={per_col: 'Timeperiod'})
    
    # Get sum by period
    df = get_sum_by_period(counts, 'vol', interval)
    
    # Sum over phases
    result = df.groupby(['SignalID', 'Week', 'DOW', 'Timeperiod'])['vol'].sum().reset_index()
    result = result.rename(columns={'vol': 'vph'})
    
    if interval == '1 hour':
        result = result.rename(columns={'Timeperiod': 'Hour'})
    
    return result


# Constants for day of week and peak hours
TUE = 2
WED = 3
THU = 4
AM_PEAK_HOURS = [6, 7, 8, 9]
PM_PEAK_HOURS = [16, 17, 18, 19]


def get_tuesdays(df: pd.DataFrame) -> pd.DataFrame:
    """Get Tuesday dates for each week"""
    
    if 'Week' not in df.columns:
        df = df.copy()
        df['Week'] = df['Date'].dt.isocalendar().week
    
    # Find the Tuesday (day 1) for each week
    tuesday_dates = df.groupby('Week')['Date'].apply(
        lambda dates: min(dates) - pd.Timedelta(days=(min(dates).weekday() + 6) % 7) + pd.Timedelta(days=1)
    ).reset_index()
    
    tuesday_dates.columns = ['Week', 'Date']
    
    return tuesday_dates


# Additional corridor aggregation helper functions
def get_cor_weekly_avg_by_period(df: pd.DataFrame, 
                                corridors: pd.DataFrame, 
                                var_col: str, 
                                per_col: str, 
                                wt_col: str = 'ones') -> pd.DataFrame:
    """Get corridor weekly average by period"""
    
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, 'Date', corridors, var_col, wt_col)
    cor_df_out = cor_df_out[~pd.isna(cor_df_out[var_col])]
    
    result = group_corridors(cor_df_out, 'Date', var_col, wt_col)
    result['Week'] = result['Date'].dt.isocalendar().week
    
    return result


def get_cor_monthly_avg_by_period(df: pd.DataFrame, 
                                 corridors: pd.DataFrame, 
                                 var_col: str, 
                                 per_col: str, 
                                 wt_col: str = 'ones') -> pd.DataFrame:
    """Get corridor monthly average by period"""
    
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, per_col, corridors, var_col, wt_col)
    cor_df_out = cor_df_out[~pd.isna(cor_df_out[var_col])]
    
    return group_corridors(cor_df_out, per_col, var_col, wt_col)


def get_hourly(df: pd.DataFrame, var_col: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """Get hourly data with corridor information"""
    
    result = df.merge(corridors, on='SignalID', how='outer')
    result = result.dropna(subset=['Corridor'])
    
    # Calculate lag and delta
    result = result.sort_values(['SignalID', 'Hour'])
    result['lag_'] = result.groupby('SignalID')[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    return result[['SignalID', 'Hour', var_col, 'delta', 'Zone_Group', 'Zone', 'Corridor', 'Subcorridor']]


# Final utility functions for specific use cases
def get_daily_aog(df: pd.DataFrame) -> pd.DataFrame:
    """Get daily arrival on green"""
    return get_daily_avg(df, var_col='aog', wt_col='vol', peak_only=True)


def get_cor_weekly_avg_by_hr_special(df: pd.DataFrame, 
                                    corridors: pd.DataFrame, 
                                    var_col: str, 
                                    wt_col: str = 'ones') -> pd.DataFrame:
    """Special case of corridor weekly average by hour"""
    
    if wt_col == 'ones':
        df = df.copy()
        df['ones'] = 1
    
    cor_df_out = weighted_mean_by_corridor(df, 'Hour', corridors, var_col, wt_col)
    
    return group_corridors(cor_df_out, 'Hour', var_col, wt_col)


# Functions that were referenced but need implementation
def group_corridor_by(df: pd.DataFrame, 
                     per_col: str, 
                     var_col: str, 
                     wt_col: str, 
                     corr_grp: str) -> pd.DataFrame:
    """Group corridor by specific grouping"""
    
    def weighted_avg(group):
        weights = group[wt_col]
        values = group[var_col]
        valid_mask = ~(pd.isna(values) | pd.isna(weights))
        
        if valid_mask.sum() == 0:
            return pd.Series({var_col: np.nan, wt_col: 0})
        
        return pd.Series({
            var_col: np.average(values[valid_mask], weights=weights[valid_mask]),
            wt_col: weights[valid_mask].sum()
        })
    
    result = df.groupby(per_col).apply(weighted_avg).reset_index()
    
    result['Corridor'] = corr_grp
    result['Corridor'] = result['Corridor'].astype('category')
    
    # Calculate lag and delta
    result = result.sort_values(per_col)
    result['lag_'] = result[var_col].shift(1)
    result['delta'] = (result[var_col] - result['lag_']) / result['lag_']
    
    result['Zone_Group'] = corr_grp
    
    return result[['Zone_Group', 'Corridor', per_col, var_col, wt_col, 'delta']]


# Error handling and logging setup
import logging

logger = logging.getLogger(__name__)

# Performance monitoring context manager (if not already imported)
try:
    from utilities import PerformanceMonitor
except ImportError:
    class PerformanceMonitor:
        def __init__(self, operation_name: str, log_memory: bool = True):
            self.operation_name = operation_name
            
        def __enter__(self):
            logger.info(f"Starting {self.operation_name}")
            return self
            
        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                logger.info(f"Completed {self.operation_name}")
            else:
                logger.error(f"Failed {self.operation_name}: {exc_val}")


# Main aggregation wrapper function
def run_aggregations(data: Dict[str, pd.DataFrame], 
                    corridors: pd.DataFrame, 
                    config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Main function to run all aggregations based on configuration
    
    Args:
        data: Dictionary of input DataFrames
        corridors: Corridor mapping DataFrame
        config: Configuration dictionary
    
    Returns:
        Dictionary of aggregated results
    """
    
    results = {}
    
    with PerformanceMonitor("aggregations"):
        try:
            # Daily aggregations
            if 'counts' in data and config.get('run_daily', True):
                logger.info("Running daily aggregations")
                
                # VPD
                if 'vpd' in data:
                    results['weekly_vpd'] = get_weekly_vpd(data['vpd'])
                    results['monthly_vpd'] = get_monthly_vpd(data['vpd'])
                    results['cor_monthly_vpd'] = get_cor_monthly_vpd(results['monthly_vpd'], corridors)
                
                # AOG
                if 'aog' in data:
                    daily_aog = get_daily_aog(data['aog'])
                    results['monthly_aog'] = get_monthly_aog_by_day(daily_aog)
                    results['cor_monthly_aog'] = get_cor_monthly_aog_by_day(results['monthly_aog'], corridors)
                
                # Detector uptime
                if 'filtered_counts' in data:
                    ddu = get_daily_detector_uptime(data['filtered_counts'])
                    if ddu:
                        avg_daily_uptime = get_avg_daily_detector_uptime(ddu)
                        results['monthly_detector_uptime'] = get_monthly_detector_uptime(avg_daily_uptime)
                        results['cor_monthly_detector_uptime'] = get_cor_monthly_detector_uptime(
                            avg_daily_uptime, corridors
                        )
            
            # Hourly aggregations
            if config.get('run_hourly', True):
                logger.info("Running hourly aggregations")
                
                # VPH
                if 'counts' in data:
                    vph = get_vph(data['counts'])
                    results['monthly_vph'] = get_monthly_vph(vph)
                    results['cor_monthly_vph'] = get_cor_monthly_vph(results['monthly_vph'], corridors)
                    
                    # Peak period analysis
                    results['monthly_vph_peak'] = get_monthly_vph_peak(results['monthly_vph'])
                    results['cor_monthly_vph_peak'] = get_cor_monthly_vph_peak(
                        results['cor_monthly_vph'], corridors
                    )
            
            # Travel time aggregations
            if 'travel_times' in data and config.get('run_travel_times', True):
                logger.info("Running travel time aggregations")
                
                if 'cor_monthly_vph' in results:
                    results['cor_monthly_ti_by_hr'] = get_cor_monthly_ti_by_hr(
                        data['travel_times'], results['cor_monthly_vph'], corridors
                    )
                    results['cor_monthly_ti_by_day'] = get_cor_monthly_ti_by_day(
                        data['travel_times'], results['cor_monthly_vph'], corridors
                    )
            
            # CCTV aggregations
            if 'cctv_uptime' in data and config.get('run_cctv', True):
                logger.info("Running CCTV aggregations")
                
                daily_cctv = get_daily_avg_cctv(data['cctv_uptime'])
                results['weekly_cctv_uptime'] = get_cor_weekly_cctv_uptime(daily_cctv)
                results['monthly_cctv_uptime'] = get_cor_monthly_cctv_uptime(daily_cctv)
            
            # Flash event aggregations
            if 'flash_events' in data and config.get('run_flash', True):
                logger.info("Running flash event aggregations")
                
                results['monthly_flash'] = get_monthly_flashevent(data['flash_events'])
                results['cor_monthly_flash'] = get_cor_monthly_flash(results['monthly_flash'], corridors)
            
            # Quarterly aggregations
            if config.get('run_quarterly', False):
                logger.info("Running quarterly aggregations")
                
                quarterly_metrics = ['vpd', 'aog', 'detector_uptime', 'vph']
                for metric in quarterly_metrics:
                    monthly_key = f'monthly_{metric}'
                    if monthly_key in results:
                        results[f'quarterly_{metric}'] = get_quarterly(
                            results[monthly_key], 
                            metric, 
                            operation=config.get(f'{metric}_quarterly_op', 'avg')
                        )
            
            logger.info(f"Completed aggregations. Generated {len(results)} result sets")
            
        except Exception as e:
            logger.error(f"Error in aggregations: {e}")
            raise
    
    return results


# Configuration validation
def validate_aggregation_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and set defaults for aggregation configuration
    
    Args:
        config: Configuration dictionary
    
    Returns:
        Validated configuration dictionary
    """
    
    defaults = {
        'run_daily': True,
        'run_hourly': True,
        'run_travel_times': True,
        'run_cctv': True,
        'run_flash': True,
        'run_quarterly': False,
        'vpd_quarterly_op': 'avg',
        'aog_quarterly_op': 'avg',
        'detector_uptime_quarterly_op': 'avg',
        'vph_quarterly_op': 'avg'
    }
    
    # Merge with defaults
    validated_config = {**defaults, **config}
    
    # Validate quarterly operations
    valid_ops = ['avg', 'sum', 'latest']
    for key in validated_config:
        if key.endswith('_quarterly_op'):
            if validated_config[key] not in valid_ops:
                logger.warning(f"Invalid quarterly operation '{validated_config[key]}'. Using 'avg'")
                validated_config[key] = 'avg'
    
    return validated_config


# Batch processing for large datasets
def run_aggregations_batch(data_batches: List[Dict[str, pd.DataFrame]], 
                          corridors: pd.DataFrame, 
                          config: Dict[str, Any]) -> Dict[str, pd.DataFrame]:
    """
    Run aggregations on batched data and combine results
    
    Args:
        data_batches: List of data dictionaries (batches)
        corridors: Corridor mapping DataFrame
        config: Configuration dictionary
    
    Returns:
        Combined aggregation results
    """
    
    config = validate_aggregation_config(config)
    batch_results = []
    
    for i, batch_data in enumerate(data_batches):
        logger.info(f"Processing batch {i+1}/{len(data_batches)}")
        
        with PerformanceMonitor(f"batch_{i+1}_aggregations"):
            batch_result = run_aggregations(batch_data, corridors, config)
            batch_results.append(batch_result)
    
    # Combine results
    logger.info("Combining batch results")
    combined_results = {}
    
    for key in batch_results[0].keys():
        # Concatenate DataFrames from all batches
        batch_dfs = [result[key] for result in batch_results if key in result]
        
        if batch_dfs:
            combined_df = pd.concat(batch_dfs, ignore_index=True)
            
            # Re-aggregate if necessary (for metrics that can be combined)
            if 'monthly' in key or 'quarterly' in key:
                # These may need re-aggregation across batches
                combined_results[key] = reaggregate_combined_data(combined_df, key)
            else:
                combined_results[key] = combined_df
    
    return combined_results


def reaggregate_combined_data(df: pd.DataFrame, result_type: str) -> pd.DataFrame:
    """
    Re-aggregate combined data from batches
    
    Args:
        df: Combined DataFrame from batches
        result_type: Type of result (monthly_vpd, etc.)
    
    Returns:
        Re-aggregated DataFrame
    """
    
    # Determine grouping columns and aggregation method based on result type
    if 'monthly' in result_type:
        time_col = 'Month'
        group_cols = ['SignalID', 'Month'] if 'SignalID' in df.columns else ['Corridor', 'Zone_Group', 'Month']
    elif 'quarterly' in result_type:
        time_col = 'Quarter'
        group_cols = ['SignalID', 'Quarter'] if 'SignalID' in df.columns else ['Corridor', 'Zone_Group', 'Quarter']
    else:
        # For other types, just return as-is
        return df
    
    # Determine value and weight columns
    value_cols = [col for col in df.columns 
                 if col not in group_cols + ['delta', 'lag_'] 
                 and not col.endswith('.sb') and not col.endswith('.pr')]
    
    weight_cols = [col for col in df.columns if col in ['vol', 'cycles', 'all', 'ones', 'num']]
    
    if not value_cols:
        return df
    
    value_col = value_cols[0]  # Primary value column
    weight_col = weight_cols[0] if weight_cols else None
    
    if weight_col:
        # Weighted aggregation
        def weighted_avg(group):
            weights = group[weight_col]
            values = group[value_col]
            valid_mask = ~(pd.isna(values) | pd.isna(weights))
            
            if valid_mask.sum() == 0:
                return pd.Series({value_col: np.nan, weight_col: 0})
            
            return pd.Series({
                value_col: np.average(values[valid_mask], weights=weights[valid_mask]),
                weight_col: weights[valid_mask].sum()
            })
        
        result = df.groupby(group_cols).apply(weighted_avg).reset_index()
    else:
        # Simple aggregation
        result = df.groupby(group_cols)[value_col].mean().reset_index()
    
    # Recalculate delta
    if 'SignalID' in result.columns:
        result = result.sort_values(['SignalID', time_col])
        result['lag_'] = result.groupby('SignalID')[value_col].shift(1)
    else:
        result = result.sort_values(['Corridor', 'Zone_Group', time_col])
        result['lag_'] = result.groupby(['Corridor', 'Zone_Group'])[value_col].shift(1)
    
    result['delta'] = (result[value_col] - result['lag_']) / result['lag_']
    result = result.drop(columns=['lag_'])
    
    return result


# Export functions for external use
__all__ = [
    'run_aggregations',
    'run_aggregations_batch',
    'validate_aggregation_config',
    'get_monthly_avg_by_day',
    'get_cor_monthly_avg_by_day',
    'get_weekly_avg_by_day',
    'get_cor_weekly_avg_by_day',
    'get_monthly_avg_by_hr',
    'get_cor_monthly_avg_by_hr',
    'get_quarterly',
    'sigify',
    'weighted_mean_by_corridor',
    'group_corridors',
    'get_daily_detector_uptime',
    'get_avg_daily_detector_uptime',
    'get_vph',
    'get_monthly_vph',
    'get_monthly_vph_peak',
    'get_cor_monthly_vph_peak',
    'get_daily_aog',
    'get_monthly_aog_by_day',
    'get_monthly_flashevent',
    'get_cor_monthly_flash',
    'get_cor_monthly_cctv_uptime',
    'get_cor_weekly_cctv_uptime',
    'summarize_by_peak',
    'PerformanceMonitor'
]


# Module-level configuration
DEFAULT_CONFIG = {
    'run_daily': True,
    'run_hourly': True,
    'run_travel_times': True,
    'run_cctv': True,
    'run_flash': True,
    'run_quarterly': False,
    'batch_size': 1000,
    'memory_threshold': 8192  # MB
}


# Version info
__version__ = "1.0.0"
__author__ = "Converted from R aggregations"
__description__ = "Python port of R aggregation functions for traffic data analysis"


if __name__ == "__main__":
    # Example usage
    logger.info("Aggregations module loaded successfully")
    logger.info(f"Available functions: {len(__all__)}")




