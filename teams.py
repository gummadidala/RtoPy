#!/usr/bin/env python3

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from datetime import datetime, timedelta
import awswrangler as wr
import boto3
from geopy.distance import geodesic
import re
from s3_parquet_io import s3read_using, s3write_using
from configs import read_corridors

def get_teams_locations(locations_df, conf):
    """Map TEAMS locations to ATSPM signals based on geographic proximity"""
    
    # Get signal data
    try:
        session = boto3.Session()
        bucket_objects = session.client('s3').list_objects_v2(
            Bucket=conf['bucket'],
            Prefix='config/maxv_atspm_intersections'
        )
        
        if 'Contents' in bucket_objects:
            last_key = max(obj['Key'] for obj in bucket_objects['Contents'])
            
            signals = wr.s3.read_csv(
                path=f"s3://{conf['bucket']}/{last_key}"
            )
        else:
            signals = pd.DataFrame()
            
    except Exception as e:
        print(f"Error reading signals data: {e}")
        signals = pd.DataFrame()
    
    if signals.empty or locations_df.empty:
        return pd.DataFrame()
    
    # Clean and prepare signals data
    signals['SignalID'] = signals['SignalID'].astype(str)
    signals['Latitude'] = pd.to_numeric(signals['Latitude'], errors='coerce')
    signals['Longitude'] = pd.to_numeric(signals['Longitude'], errors='coerce')
    
    # Filter out signals with no coordinates
    signals = signals[(signals['Latitude'] != 0) & (signals['Longitude'] != 0)].copy()
    
    # Read corridors data
    corridors = read_corridors(conf['corridors_filename_s3'])
    
    # Create GeoDataFrames
    locations_gdf = gpd.GeoDataFrame(
        locations_df,
        geometry=[Point(xy) for xy in zip(locations_df['Longitude'], locations_df['Latitude'])],
        crs='EPSG:4326'
    )
    
    signals_gdf = gpd.GeoDataFrame(
        signals,
        geometry=[Point(xy) for xy in zip(signals['Longitude'], signals['Latitude'])],
        crs='EPSG:4326'
    )
    
    # Find closest signal for each location
    matches = []
    
    for idx, location in locations_gdf.iterrows():
        # Calculate distances to all signals
        distances = signals_gdf.geometry.apply(
            lambda signal_geom: geodesic(
                (location.geometry.y, location.geometry.x),
                (signal_geom.y, signal_geom.x)
            ).meters
        )
        
        # Find closest signal
        min_distance_idx = distances.idxmin()
        min_distance = distances.min()
        
        # Only match if within 100 meters
        if min_distance < 100:
            closest_signal = signals_gdf.loc[min_distance_idx]
            
            # Check if Custom Identifier matches SignalID
            custom_id = str(location.get('Custom Identifier', '')).split(' ')[0]
            good_guess = 1 if custom_id == str(closest_signal['SignalID']) else 0
            
            matches.append({
                'SignalID': closest_signal['SignalID'],
                'PrimaryName': closest_signal['PrimaryName'],
                'SecondaryName': closest_signal['SecondaryName'],
                'distance_m': min_distance,
                'LocationId': location['DB Id'],
                'Maintained By': location['Maintained By'],
                'Custom Identifier': location['Custom Identifier'],
                'City': location['City'],
                'County': location['County'],
                'good_guess': good_guess
            })
    
    matches_df = pd.DataFrame(matches)
    
    if matches_df.empty:
        return pd.DataFrame()
    
    # For each LocationId, keep only the best match (shortest distance, then best guess)
    matches_df = matches_df.sort_values(['LocationId', 'distance_m', 'good_guess'], ascending=[True, True, False])
    matches_df = matches_df.groupby('LocationId').first().reset_index()
    
    return matches_df

def tidy_teams_tasks(tasks_df, bucket, corridors_df, replicate=False):
    """Clean and prepare TEAMS tasks data"""
    
    # Select and prepare corridors data
    corridors_clean = corridors_df[
        ['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Subcorridor', 
         'Latitude', 'Longitude', 'TeamsLocationID']
    ].copy()
    
    # Clean tasks data
    tasks_clean = tasks_df.drop(columns=['Latitude', 'Longitude'], errors='ignore').copy()
    
    # Filter tasks
    today = pd.Timestamp.now().date()
    tasks_clean = tasks_clean[
        (pd.to_datetime(tasks_clean['Date Reported']).dt.date < today) &
        ((pd.to_datetime(tasks_clean['Date Resolved']).dt.date < today) | 
         pd.isna(tasks_clean['Date Resolved']))
    ].copy()
    
    # Join with corridors
    tasks_clean = tasks_clean.merge(
        corridors_clean, 
        left_on='LocationId', 
        right_on='TeamsLocationID', 
        how='left'
    )
    
    # Calculate time to resolve
    tasks_clean['Date Reported'] = pd.to_datetime(tasks_clean['Date Reported'])
    tasks_clean['Date Resolved'] = pd.to_datetime(tasks_clean['Date Resolved'])
    
    tasks_clean['Time To Resolve In Days'] = (
        tasks_clean['Date Resolved'] - tasks_clean['Date Reported']
    ).dt.days
    
    tasks_clean['Time To Resolve In Hours'] = (
        tasks_clean['Date Resolved'] - tasks_clean['Date Reported']
    ).dt.total_seconds() / 3600
    
    # Convert to categorical and handle missing values
    categorical_columns = ['Task Type', 'Task Subtype', 'Task Source', 'Priority', 'Status']
    for col in categorical_columns:
        tasks_clean[col] = tasks_clean[col].fillna('Unspecified').astype('category')
    
    # Rename columns for consistency
    column_mapping = {
        'Due Date': 'Due_Date',
        'Task Type': 'Task_Type',
        'Task Subtype': 'Task_Subtype', 
        'Task Source': 'Task_Source',
        'Date Reported': 'Date_Reported',
        'Date Resolved': 'Date_Resolved',
        'Maintained by': 'Maintained_by',
        'Owned by': 'Owned_by',
        'Primary Route': 'Primary_Route',
        'Secondary Route': 'Secondary_Route',
        'Created on': 'Created_on',
        'Created by': 'Created_by',
        'Modified on': 'Modified_on',
        'Modified by': 'Modified_by'
    }
    
    tasks_clean = tasks_clean.rename(columns=column_mapping)
    
    # Convert dates
    tasks_clean['Date_Reported'] = tasks_clean['Date_Reported'].dt.date
    tasks_clean['Date_Resolved'] = tasks_clean['Date_Resolved'].dt.date
    
    # Add "All" category
    tasks_clean['All'] = 'all'
    
    # Filter final dataset
    all_tasks = tasks_clean[
        tasks_clean['Date_Reported'].notna() &
        tasks_clean['Corridor'].notna() &
        ~((tasks_clean['Zone_Group'] == 'Zone 7') & 
          (tasks_clean['Date_Reported'] < pd.to_datetime('2018-05-01').date())) &
        (tasks_clean['Date_Reported'] < today)
    ].copy()
    
    if replicate:
        # Create additional groupings
        rtop1_tasks = all_tasks[all_tasks['Zone_Group'] == 'RTOP1'].copy()
        rtop1_tasks['Zone_Group'] = 'All RTOP'
        
        rtop2_tasks = all_tasks[all_tasks['Zone_Group'] == 'RTOP2'].copy()
        rtop2_tasks['Zone_Group'] = 'All RTOP'
        
        zone_tasks = all_tasks[all_tasks['Zone'].str.startswith('Zone', na=False)].copy()
        zone_tasks['Zone_Group'] = zone_tasks['Zone']
        
        zone7_tasks = all_tasks[all_tasks['Zone'].isin(['Zone 7m', 'Zone 7d'])].copy()
        zone7_tasks['Zone_Group'] = 'Zone 7'
        
        # Combine all tasks
        all_tasks = pd.concat([
            all_tasks, rtop1_tasks, rtop2_tasks, zone_tasks, zone7_tasks
        ], ignore_index=True)
    
    return all_tasks

def get_teams_tasks_from_s3(bucket, archived_tasks_prefix, current_tasks_key, report_start_date):
    """Read TEAMS tasks from S3 (both archived and current)"""
    
    session = boto3.Session()
    
    # Get archived tasks
    try:
        bucket_objects = session.client('s3').list_objects_v2(
            Bucket=bucket,
            Prefix=archived_tasks_prefix
        )
        
        archived_tasks_list = []
        if 'Contents' in bucket_objects:
            for obj in bucket_objects['Contents']:
                try:
                    task_data = wr.s3.read_csv(
                        path=f"s3://{bucket}/{obj['Key']}",
                        parse_dates=['Due Date', 'Date Reported', 'Date Resolved', 
                                   'Created on', 'Modified on']
                    )
                    
                    # Filter by report start date
                    task_data = task_data[
                        pd.to_datetime(task_data['Date Reported']) >= pd.to_datetime(report_start_date)
                    ]
                    
                    archived_tasks_list.append(task_data)
                    
                except Exception as e:
                    print(f"Error reading {obj['Key']}: {e}")
                    continue
        
        if archived_tasks_list:
            archived_tasks = pd.concat(archived_tasks_list, ignore_index=True)
        else:
            archived_tasks = pd.DataFrame()
            
    except Exception as e:
        print(f"Error reading archived tasks: {e}")
        archived_tasks = pd.DataFrame()
    
    # Get current tasks
    try:
        current_tasks = wr.s3.read_csv(
            path=f"s3://{bucket}/{current_tasks_key}",
            parse_dates=['Due Date', 'Date Reported', 'Date Resolved', 
                        'Created on', 'Modified on']
        )
        
        current_tasks = current_tasks[
            pd.to_datetime(current_tasks['Date Reported']) >= pd.to_datetime(report_start_date)
        ]
        
    except Exception as e:
        print(f"Error reading current tasks: {e}")
        current_tasks = pd.DataFrame()
    
    # Combine and deduplicate
    if not archived_tasks.empty and not current_tasks.empty:
        all_tasks = pd.concat([archived_tasks, current_tasks], ignore_index=True)
    elif not archived_tasks.empty:
        all_tasks = archived_tasks
    elif not current_tasks.empty:
        all_tasks = current_tasks
    else:
        all_tasks = pd.DataFrame()
    
    if not all_tasks.empty:
        all_tasks = all_tasks.drop_duplicates()
    
    return all_tasks

def get_daily_tasks_status(daily_tasks_status_df, groupings):
    """Get number of reported and resolved tasks for each date with cumulative counts"""
    
    if daily_tasks_status_df.empty:
        return pd.DataFrame()
    
    # Group by specified columns and date
    grouped = daily_tasks_status_df.groupby(groupings + ['Date']).agg({
        'n_reported': 'sum',
        'n_resolved': 'sum'
    }).reset_index()
    
    grouped = grouped.rename(columns={'n_reported': 'Reported', 'n_resolved': 'Resolved'})
    
    # Calculate cumulative sums
    group_cols = [col for col in groupings if col != 'Date']
    if group_cols:
        grouped = grouped.sort_values(group_cols + ['Date'])
        grouped['cum_Reported'] = grouped.groupby(group_cols)['Reported'].cumsum()
        grouped['cum_Resolved'] = grouped.groupby(group_cols)['Resolved'].cumsum()
    else:
        grouped = grouped.sort_values('Date')
        grouped['cum_Reported'] = grouped['Reported'].cumsum()
        grouped['cum_Resolved'] = grouped['Resolved'].cumsum()
    
    grouped['Outstanding'] = grouped['cum_Reported'] - grouped['cum_Resolved']
    
    return grouped

def get_outstanding_tasks_by_month(df, task_param):
    """Aggregate daily outstanding tasks by month"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Add month column
    df['Month'] = pd.to_datetime(df['Date']).dt.to_period('M').dt.start_time
    
    # Group by month and get last values (cumulative for month)
    monthly = df.groupby(['Zone_Group', 'Corridor', task_param, 'Month']).agg({
        'Reported': 'sum',
        'Resolved': 'sum', 
        'cum_Reported': 'last',
        'cum_Resolved': 'last',
        'Outstanding': 'last'
    }).reset_index()
    
    # Complete missing combinations and fill forward
    all_months = pd.period_range(
        start=df['Month'].min(),
        end=df['Month'].max(), 
        freq='M'
    ).to_timestamp()
    
    # Get all unique combinations
    unique_combos = df[['Zone_Group', 'Corridor', task_param]].drop_duplicates()
    
    # Create complete grid
    complete_grid = []
    for _, combo in unique_combos.iterrows():
        for month in all_months:
            complete_grid.append({
                'Zone_Group': combo['Zone_Group'],
                'Corridor': combo['Corridor'],
                task_param: combo[task_param],
                'Month': month
            })
    
    complete_df = pd.DataFrame(complete_grid)
    
    # Merge with actual data
    monthly_complete = complete_df.merge(monthly, how='left', 
                                       on=['Zone_Group', 'Corridor', task_param, 'Month'])
    
    # Fill missing values
    monthly_complete['Reported'] = monthly_complete['Reported'].fillna(0)
    monthly_complete['Resolved'] = monthly_complete['Resolved'].fillna(0)
    
    # Forward fill cumulative values within groups
    group_cols = ['Zone_Group', 'Corridor', task_param]
    monthly_complete = monthly_complete.sort_values(group_cols + ['Month'])
    
    for group_name, group in monthly_complete.groupby(group_cols):
        mask = monthly_complete[group_cols].eq(group.iloc[0][group_cols]).all(axis=1)
        
        # Fill first NA values with 0
        if monthly_complete.loc[mask, 'cum_Reported'].iloc[0] != monthly_complete.loc[mask, 'cum_Reported'].iloc[0]:  # Check for NaN
            monthly_complete.loc[mask, 'cum_Reported'] = monthly_complete.loc[mask, 'cum_Reported'].fillna(method='ffill').fillna(0)
        else:
            monthly_complete.loc[mask, 'cum_Reported'] = monthly_complete.loc[mask, 'cum_Reported'].fillna(method='ffill')
        
        monthly_complete.loc[mask, 'cum_Resolved'] = monthly_complete.loc[mask, 'cum_Resolved'].fillna(method='ffill').fillna(0)
        monthly_complete.loc[mask, 'Outstanding'] = monthly_complete.loc[mask, 'Outstanding'].fillna(method='ffill').fillna(0)
    
    # Calculate deltas (month-to-month changes)
    monthly_complete = monthly_complete.sort_values(group_cols + ['Month'])
    
    for col in ['Reported', 'Resolved', 'Outstanding']:
        monthly_complete[f'delta_{col.lower()[:3]}'] = (
            monthly_complete.groupby(group_cols)[col].pct_change()
        )
    
    return monthly_complete

def get_sig_from_monthly_tasks(monthly_data, all_corridors):
    """Filter for Zone_Group = Corridor for all Corridors for use in signal-level tasks"""
    
    if monthly_data.empty:
        return pd.DataFrame()
    
    # Take minimum Zone_Group for each corridor (removes duplicates)
    deduplicated = monthly_data.groupby('Corridor').apply(
        lambda x: x[x['Zone_Group'] == x['Zone_Group'].min()]
    ).reset_index(drop=True)
    
    # Set Zone_Group to Corridor name
    deduplicated['Zone_Group'] = deduplicated['Corridor']
    
    # Filter to corridors in all_corridors
    corridor_names = all_corridors['Corridor'].unique()
    filtered = deduplicated[deduplicated['Corridor'].isin(corridor_names)]
    
    return filtered

def get_outstanding_tasks_by_param(teams_df, task_param, report_start_date, all_corridors):
    """Get outstanding tasks by day, month, corridor/zone group and corridor only by task parameter"""
    
    if teams_df.empty:
        return {
            'cor_daily': pd.DataFrame(),
            'sig_daily': pd.DataFrame(), 
            'cor_monthly': pd.DataFrame(),
            'sig_monthly': pd.DataFrame()
        }
    
    # Tasks Reported by Day, by SignalID
    tasks_reported_by_day = teams_df.groupby([
        'Zone_Group', 'Zone', 'Corridor', 'SignalID', task_param, 'Date_Reported'
    ]).size().reset_index(name='n_reported')
    tasks_reported_by_day = tasks_reported_by_day.rename(columns={'Date_Reported': 'Date'})
    
    # Tasks Resolved by Day, by SignalID  
    resolved_tasks = teams_df[teams_df['Date_Resolved'].notna()].copy()
    tasks_resolved_by_day = resolved_tasks.groupby([
        'Zone_Group', 'Zone', 'Corridor', 'SignalID', task_param, 'Date_Resolved'
    ]).size().reset_index(name='n_resolved')
    tasks_resolved_by_day = tasks_resolved_by_day.rename(columns={'Date_Resolved': 'Date'})
    
    # Combine reported and resolved
    daily_tasks_status = pd.merge(
        tasks_reported_by_day,
        tasks_resolved_by_day,
        on=['Zone_Group', 'Zone', 'Corridor', 'SignalID', task_param, 'Date'],
        how='outer'
    ).fillna(0)
    
    # Get daily status by corridor
    cor_daily_tasks_status = get_daily_tasks_status(
        daily_tasks_status,
        groupings=['Zone_Group', 'Zone', 'Corridor', task_param]
    )
    
    # Filter out certain zone groups and remove Zone column
    cor_daily_tasks_status = cor_daily_tasks_status[
        ~cor_daily_tasks_status['Zone_Group'].isin(['All RTOP', 'RTOP1', 'RTOP2', 'Zone 7'])
    ].drop(columns=['Zone'], errors='ignore')
    
    # Get daily status by zone group
    zone_group_daily_tasks_status = get_daily_tasks_status(
        daily_tasks_status,
        groupings=['Zone_Group', task_param]
    )
    zone_group_daily_tasks_status['Corridor'] = zone_group_daily_tasks_status['Zone_Group']
    
    # Combine corridor and zone group data
    cor_daily_outstanding_tasks = pd.concat([
        cor_daily_tasks_status,
        zone_group_daily_tasks_status
    ], ignore_index=True)
    
    # Filter by date range
    report_start = pd.to_datetime(report_start_date).date()
    today = pd.Timestamp.now().date()
    
    cor_daily_outstanding_tasks = cor_daily_outstanding_tasks[
        (pd.to_datetime(cor_daily_outstanding_tasks['Date']).dt.date >= report_start) &
        (pd.to_datetime(cor_daily_outstanding_tasks['Date']).dt.date <= today)
    ]
    
    # Get signal-level data
    sig_daily_outstanding_tasks = get_sig_from_monthly_tasks(
        cor_daily_tasks_status, all_corridors
    )
    sig_daily_outstanding_tasks = sig_daily_outstanding_tasks[
        (pd.to_datetime(sig_daily_outstanding_tasks['Date']).dt.date >= report_start) &
        (pd.to_datetime(sig_daily_outstanding_tasks['Date']).dt.date <= today)
    ]
    
    # Get monthly data
    cor_monthly_outstanding_tasks = get_outstanding_tasks_by_month(
        cor_daily_outstanding_tasks, task_param
    )
    sig_monthly_outstanding_tasks = get_outstanding_tasks_by_month(
        sig_daily_outstanding_tasks, task_param
    )
    
    return {
        'cor_daily': cor_daily_outstanding_tasks,
        'sig_daily': sig_daily_outstanding_tasks,
        'cor_monthly': cor_monthly_outstanding_tasks, 
        'sig_monthly': sig_monthly_outstanding_tasks
    }

def get_outstanding_tasks_by_day_range(teams_df, report_start_date, first_of_month):
    """Get number of tasks outstanding for different day ranges (0-45, 45-90, 90+)"""
    
    if teams_df.empty:
        return pd.DataFrame()
    
    last_day = first_of_month + pd.DateOffset(months=1) - pd.DateOffset(days=1)
    
    # Calculate Mean Time to Resolution (MTTR)
    resolved_tasks = teams_df[
        (pd.to_datetime(teams_df['Date_Reported']).dt.date <= last_day.date()) &
        (pd.to_datetime(teams_df['Date_Resolved']).dt.date < last_day.date()) &
        (pd.to_datetime(teams_df['Date_Reported']).dt.date > pd.to_datetime(report_start_date).date()) &
        teams_df['Date_Resolved'].notna()
    ].copy()
    
    if not resolved_tasks.empty:
        resolved_tasks['ttr_days'] = (
            pd.to_datetime(resolved_tasks['Date_Resolved']) - 
            pd.to_datetime(resolved_tasks['Date_Reported'])
        ).dt.days
        
        mttr = resolved_tasks.groupby(['Zone_Group', 'Zone', 'Corridor']).agg({
            'ttr_days': 'mean',
            'SignalID': 'count'
        }).reset_index()
        mttr = mttr.rename(columns={'ttr_days': 'mttr', 'SignalID': 'num_resolved'})
        mttr['Month'] = first_of_month
    else:
        mttr = pd.DataFrame()
    
    # Tasks outstanding over 45 days
    over45_cutoff = last_day - pd.DateOffset(days=45)
    over45_tasks = teams_df[
        (pd.to_datetime(teams_df['Date_Reported']).dt.date < over45_cutoff.date()) &
        ((teams_df['Date_Resolved'].isna()) | 
         (pd.to_datetime(teams_df['Date_Resolved']).dt.date > last_day.date()))
    ]
    
    over45 = over45_tasks.groupby(['Zone_Group', 'Zone', 'Corridor']).size().reset_index(name='over45')
    
    # Tasks outstanding over 90 days
    over90_cutoff = last_day - pd.DateOffset(days=90)
    over90_tasks = teams_df[
        (pd.to_datetime(teams_df['Date_Reported']).dt.date < over90_cutoff.date()) &
        ((teams_df['Date_Resolved'].isna()) | 
         (pd.to_datetime(teams_df['Date_Resolved']).dt.date > last_day.date()))
    ]
    
    over90 = over90_tasks.groupby(['Zone_Group', 'Zone', 'Corridor']).size().reset_index(name='over90')
    
    # All outstanding tasks
    all_outstanding_tasks = teams_df[
        ((teams_df['Date_Resolved'].isna()) | 
         (pd.to_datetime(teams_df['Date_Resolved']).dt.date > last_day.date())) &
        (pd.to_datetime(teams_df['Date_Reported']).dt.date <= last_day.date())
    ]
    
    all_outstanding = all_outstanding_tasks.groupby(['Zone_Group', 'Zone', 'Corridor']).size().reset_index(name='all_outstanding')
    
    # Merge all dataframes
    result_dfs = [over45, over90, all_outstanding]
    if not mttr.empty:
        result_dfs.append(mttr)
    
    if result_dfs:
        outstanding_summary = result_dfs[0]
        for df in result_dfs[1:]:
            outstanding_summary = outstanding_summary.merge(
                df, on=['Zone_Group', 'Zone', 'Corridor'], how='outer'
            )
    else:
        outstanding_summary = pd.DataFrame()
    
    if not outstanding_summary.empty:
        # Fill missing values
        outstanding_summary = outstanding_summary.fillna(0)
        
        # Calculate day range categories
        outstanding_summary['0-45'] = outstanding_summary['all_outstanding'] - outstanding_summary['over45']
        outstanding_summary['45-90'] = outstanding_summary['over45'] - outstanding_summary['over90']
        outstanding_summary['num_outstanding'] = outstanding_summary['all_outstanding']
        
        # Add month
        outstanding_summary['Month'] = first_of_month
        
        # Select final columns
        final_cols = [
            'Zone_Group', 'Corridor', 'Month', '0-45', '45-90', 
            'over45', 'over90', 'num_outstanding'
        ]
        
        if 'mttr' in outstanding_summary.columns:
            final_cols.extend(['num_resolved', 'mttr'])
        
        outstanding_summary = outstanding_summary[final_cols]
        
        # Create zone group aggregations
        zone_group_summary = outstanding_summary[
            ~outstanding_summary['Zone_Group'].isin(['All RTOP', 'RTOP1', 'RTOP2', 'Zone 7'])
        ].copy()
        
        # Add zone group level aggregations
        agg_cols = ['0-45', '45-90', 'over45', 'over90', 'num_outstanding']
        if 'num_resolved' in outstanding_summary.columns:
            agg_cols.append('num_resolved')
        
        zone_aggregated = outstanding_summary.groupby(['Zone_Group', 'Month']).agg(
            {col: 'sum' for col in agg_cols}
        ).reset_index()
        zone_aggregated['Corridor'] = zone_aggregated['Zone_Group']
        
        # Calculate weighted average MTTR if available
        if 'mttr' in outstanding_summary.columns and 'num_resolved' in outstanding_summary.columns:
            weighted_mttr = outstanding_summary.groupby(['Zone_Group', 'Month']).apply(
                lambda x: np.average(x['mttr'], weights=x['num_resolved']) if x['num_resolved'].sum() > 0 else 0
            ).reset_index(name='mttr')
            zone_aggregated = zone_aggregated.merge(weighted_mttr, on=['Zone_Group', 'Month'])
        
        # Combine results
        final_result = pd.concat([
            zone_group_summary,
            zone_aggregated
        ], ignore_index=True)
        
        return final_result
    
    return pd.DataFrame()

def get_outstanding_events(teams_df, group_var, spatial_grouping="Zone_Group"):
    """Get outstanding events aggregated by month"""
    
    if teams_df.empty:
        return pd.DataFrame()
    
    # Reported tasks by month
    reported_tasks = teams_df[teams_df['Date_Reported'].notna()].copy()
    reported_tasks['Month'] = pd.to_datetime(reported_tasks['Date_Reported']).dt.to_period('M').dt.start_time
    
    reported = reported_tasks.groupby([spatial_grouping, group_var, 'Month']).size().reset_index(name='Rep')
    reported = reported.sort_values([spatial_grouping, group_var, 'Month'])
    reported['cumRep'] = reported.groupby([spatial_grouping, group_var])['Rep'].cumsum()
    
    # Resolved tasks by month
    resolved_tasks = teams_df[teams_df['Date_Resolved'].notna()].copy()
    resolved_tasks['Month'] = pd.to_datetime(resolved_tasks['Date_Resolved']).dt.to_period('M').dt.start_time
    
    resolved = resolved_tasks.groupby([spatial_grouping, group_var, 'Month']).size().reset_index(name='Res')
    resolved = resolved.sort_values([spatial_grouping, group_var, 'Month'])
    resolved['cumRes'] = resolved.groupby([spatial_grouping, group_var])['Res'].cumsum()
    
    # Merge reported and resolved
    outstanding = reported.merge(
        resolved, 
        on=[spatial_grouping, group_var, 'Month'], 
        how='left'
    )
    
    # Forward fill cumulative resolved and fill NaN with 0
    outstanding = outstanding.sort_values([spatial_grouping, group_var, 'Month'])
    outstanding['cumRes'] = outstanding.groupby([spatial_grouping, group_var])['cumRes'].ffill().fillna(0)
    outstanding = outstanding.fillna(0)
    
    # Calculate outstanding
    outstanding['outstanding'] = outstanding['cumRep'] - outstanding['cumRes']
    
    return outstanding

def create_teams_summary_report(teams_df, corridors_df, report_date):
    """Create comprehensive TEAMS summary report"""
    
    if teams_df.empty:
        return {}
    
    report = {
        'report_date': report_date,
        'summary_stats': {},
        'by_corridor': {},
        'by_task_type': {},
        'by_priority': {},
        'trends': {}
    }
    
    # Overall summary statistics
    total_tasks = len(teams_df)
    open_tasks = len(teams_df[teams_df['Date_Resolved'].isna()])
    closed_tasks = total_tasks - open_tasks
    
    if not teams_df[teams_df['Date_Resolved'].notna()].empty:
        avg_resolution_time = teams_df[teams_df['Time To Resolve In Days'].notna()]['Time To Resolve In Days'].mean()
    else:
        avg_resolution_time = 0
    
    report['summary_stats'] = {
        'total_tasks': total_tasks,
        'open_tasks': open_tasks,
        'closed_tasks': closed_tasks,
        'avg_resolution_days': round(avg_resolution_time, 1),
        'closure_rate': round(closed_tasks / total_tasks * 100, 1) if total_tasks > 0 else 0
    }
    
    # By corridor
    corridor_summary = teams_df.groupby('Corridor').agg({
        'SignalID': 'count',
        'Date_Resolved': lambda x: x.isna().sum(),  # Open tasks
        'Time To Resolve In Days': 'mean'
    }).reset_index()
    
    corridor_summary.columns = ['Corridor', 'total_tasks', 'open_tasks', 'avg_resolution_days']
    corridor_summary['closed_tasks'] = corridor_summary['total_tasks'] - corridor_summary['open_tasks']
    corridor_summary['closure_rate'] = (
        corridor_summary['closed_tasks'] / corridor_summary['total_tasks'] * 100
    ).round(1)
    corridor_summary['avg_resolution_days'] = corridor_summary['avg_resolution_days'].round(1)
    
    report['by_corridor'] = corridor_summary.to_dict('records')
    
    # By task type
    task_type_summary = teams_df.groupby('Task_Type').agg({
        'SignalID': 'count',
        'Date_Resolved': lambda x: x.isna().sum(),
        'Time To Resolve In Days': 'mean'
    }).reset_index()
    
    task_type_summary.columns = ['Task_Type', 'total_tasks', 'open_tasks', 'avg_resolution_days']
    task_type_summary['closed_tasks'] = task_type_summary['total_tasks'] - task_type_summary['open_tasks']
    task_type_summary['closure_rate'] = (
        task_type_summary['closed_tasks'] / task_type_summary['total_tasks'] * 100
    ).round(1)
    task_type_summary['avg_resolution_days'] = task_type_summary['avg_resolution_days'].round(1)
    
    report['by_task_type'] = task_type_summary.to_dict('records')
    
    # By priority
    priority_summary = teams_df.groupby('Priority').agg({
        'SignalID': 'count',
        'Date_Resolved': lambda x: x.isna().sum(),
        'Time To Resolve In Days': 'mean'
    }).reset_index()
    
    priority_summary.columns = ['Priority', 'total_tasks', 'open_tasks', 'avg_resolution_days']
    priority_summary['closed_tasks'] = priority_summary['total_tasks'] - priority_summary['open_tasks']
    priority_summary['closure_rate'] = (
        priority_summary['closed_tasks'] / priority_summary['total_tasks'] * 100
    ).round(1)
    priority_summary['avg_resolution_days'] = priority_summary['avg_resolution_days'].round(1)
    
    report['by_priority'] = priority_summary.to_dict('records')
    
    # Monthly trends for last 12 months
    twelve_months_ago = pd.to_datetime(report_date) - pd.DateOffset(months=12)
    recent_tasks = teams_df[
        pd.to_datetime(teams_df['Date_Reported']) >= twelve_months_ago
    ].copy()
    
    if not recent_tasks.empty:
        recent_tasks['Month'] = pd.to_datetime(recent_tasks['Date_Reported']).dt.to_period('M')
        
        monthly_trends = recent_tasks.groupby('Month').agg({
            'SignalID': 'count',
            'Date_Resolved': lambda x: (~x.isna()).sum()
        }).reset_index()
        
        monthly_trends.columns = ['Month', 'reported', 'resolved']
        monthly_trends['Month'] = monthly_trends['Month'].astype(str)
        
        report['trends']['monthly'] = monthly_trends.to_dict('records')
    
    return report

def export_teams_data(teams_df, output_format='csv', output_path=None):
    """Export TEAMS data in various formats"""
    
    if teams_df.empty:
        print("No TEAMS data to export")
        return
    
    if output_format.lower() == 'csv':
        if output_path:
            teams_df.to_csv(output_path, index=False)
        else:
            return teams_df.to_csv(index=False)
    
    elif output_format.lower() == 'excel':
        if output_path:
            with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
                teams_df.to_excel(writer, sheet_name='All Tasks', index=False)
                
                # Add summary sheets
                if not teams_df.empty:
                    # By corridor
                    corridor_summary = teams_df.groupby('Corridor').agg({
                        'SignalID': 'count',
                        'Date_Resolved': lambda x: x.isna().sum(),
                        'Time To Resolve In Days': 'mean'
                    })
                    corridor_summary.to_excel(writer, sheet_name='By Corridor')
                    
                    # By task type
                    task_type_summary = teams_df.groupby('Task_Type').agg({
                        'SignalID': 'count',
                        'Date_Resolved': lambda x: x.isna().sum(),
                        'Time To Resolve In Days': 'mean'
                    })
                    task_type_summary.to_excel(writer, sheet_name='By Task Type')
    
    elif output_format.lower() == 'json':
        if output_path:
            teams_df.to_json(output_path, orient='records', date_format='iso')
        else:
            return teams_df.to_json(orient='records', date_format='iso')
    
    elif output_format.lower() == 'parquet':
        if output_path:
            teams_df.to_parquet(output_path, index=False)
        else:
            print("Parquet format requires output_path")
    
    else:
        raise ValueError(f"Unsupported output format: {output_format}")

def validate_teams_data(teams_df):
    """Validate TEAMS data for consistency and completeness"""
    
    validation_report = {
        'errors': [],
        'warnings': [],
        'info': [],
        'data_quality_score': 100
    }
    
    if teams_df.empty:
        validation_report['errors'].append("TEAMS dataset is empty")
        validation_report['data_quality_score'] = 0
        return validation_report
    
    # Check for required columns
    required_columns = [
        'LocationId', 'Task_Type', 'Task_Subtype', 'Priority', 'Status',
        'Date_Reported', 'SignalID', 'Corridor'
    ]
    
    missing_columns = [col for col in required_columns if col not in teams_df.columns]
    if missing_columns:
        validation_report['errors'].append(f"Missing required columns: {missing_columns}")
        validation_report['data_quality_score'] -= 20
    
    # Check for missing critical data
    if 'Date_Reported' in teams_df.columns:
        missing_report_dates = teams_df['Date_Reported'].isna().sum()
        if missing_report_dates > 0:
            validation_report['warnings'].append(f"Found {missing_report_dates} tasks with missing report dates")
            validation_report['data_quality_score'] -= 10
    
    if 'SignalID' in teams_df.columns:
        missing_signal_ids = teams_df['SignalID'].isna().sum()
        if missing_signal_ids > 0:
            validation_report['warnings'].append(f"Found {missing_signal_ids} tasks with missing SignalIDs")
            validation_report['data_quality_score'] -= 10
    
    if 'Corridor' in teams_df.columns:
        missing_corridors = teams_df['Corridor'].isna().sum()
        if missing_corridors > 0:
            validation_report['warnings'].append(f"Found {missing_corridors} tasks with missing corridors")
            validation_report['data_quality_score'] -= 5
    
    # Check for data anomalies
    if 'Time To Resolve In Days' in teams_df.columns:
        negative_resolution_times = (teams_df['Time To Resolve In Days'] < 0).sum()
        if negative_resolution_times > 0:
            validation_report['errors'].append(f"Found {negative_resolution_times} tasks with negative resolution times")
            validation_report['data_quality_score'] -= 15
        
        very_long_resolution_times = (teams_df['Time To Resolve In Days'] > 365).sum()
        if very_long_resolution_times > 0:
            validation_report['warnings'].append(f"Found {very_long_resolution_times} tasks taking over 1 year to resolve")
            validation_report['data_quality_score'] -= 5
    
    # Check date consistency
    if 'Date_Reported' in teams_df.columns and 'Date_Resolved' in teams_df.columns:
        resolved_before_reported = (
            pd.to_datetime(teams_df['Date_Resolved']) < pd.to_datetime(teams_df['Date_Reported'])
        ).sum()
        if resolved_before_reported > 0:
            validation_report['errors'].append(f"Found {resolved_before_reported} tasks resolved before they were reported")
            validation_report['data_quality_score'] -= 15
    
    # Data completeness info
    validation_report['info'].append(f"Total tasks: {len(teams_df)}")
    validation_report['info'].append(f"Date range: {teams_df['Date_Reported'].min()} to {teams_df['Date_Reported'].max()}")
    validation_report['info'].append(f"Unique corridors: {teams_df['Corridor'].nunique()}")
    validation_report['info'].append(f"Unique signals: {teams_df['SignalID'].nunique()}")
    
    # Task status breakdown
    if 'Status' in teams_df.columns:
        status_counts = teams_df['Status'].value_counts()
        validation_report['info'].append(f"Task status breakdown: {status_counts.to_dict()}")
    
    return validation_report

def create_teams_dashboard_data(teams_df, corridors_df, date_range_months=12):
    """Create data structure optimized for dashboard display"""
    
    if teams_df.empty:
        return {}
    
    # Filter to recent data
    cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=date_range_months)
    recent_tasks = teams_df[
        pd.to_datetime(teams_df['Date_Reported']) >= cutoff_date
    ].copy()
    
    dashboard_data = {
        'summary_cards': {},
        'charts': {},
        'tables': {},
        'filters': {}
    }
    
    # Summary cards data
    total_tasks = len(recent_tasks)
    open_tasks = len(recent_tasks[recent_tasks['Date_Resolved'].isna()])
    closed_tasks = total_tasks - open_tasks
    
    if closed_tasks > 0:
        avg_resolution = recent_tasks[recent_tasks['Time To Resolve In Days'].notna()]['Time To Resolve In Days'].mean()
    else:
        avg_resolution = 0
    
    dashboard_data['summary_cards'] = {
        'total_tasks': total_tasks,
        'open_tasks': open_tasks,
        'closed_tasks': closed_tasks,
        'avg_resolution_days': round(avg_resolution, 1),
        'closure_rate': round((closed_tasks / total_tasks * 100), 1) if total_tasks > 0 else 0
    }
    
    # Monthly trend chart data
    recent_tasks['report_month'] = pd.to_datetime(recent_tasks['Date_Reported']).dt.to_period('M')
    monthly_reported = recent_tasks.groupby('report_month').size().reset_index(name='reported')
    monthly_reported['month'] = monthly_reported['report_month'].astype(str)
    
    resolved_tasks = recent_tasks[recent_tasks['Date_Resolved'].notna()].copy()
    resolved_tasks['resolve_month'] = pd.to_datetime(resolved_tasks['Date_Resolved']).dt.to_period('M')
    monthly_resolved = resolved_tasks.groupby('resolve_month').size().reset_index(name='resolved')
    monthly_resolved['month'] = monthly_resolved['resolve_month'].astype(str)
    
    monthly_trends = monthly_reported.merge(monthly_resolved, on='month', how='outer').fillna(0)
    
    dashboard_data['charts']['monthly_trends'] = {
        'labels': monthly_trends['month'].tolist(),
        'reported': monthly_trends['reported'].tolist(),
        'resolved': monthly_trends['resolved'].tolist()
    }
    
    # Task type breakdown
    task_type_counts = recent_tasks['Task_Type'].value_counts()
    dashboard_data['charts']['task_types'] = {
        'labels': task_type_counts.index.tolist(),
        'values': task_type_counts.values.tolist()
    }
    
    # Priority breakdown
    priority_counts = recent_tasks['Priority'].value_counts()
    dashboard_data['charts']['priorities'] = {
        'labels': priority_counts.index.tolist(),
        'values': priority_counts.values.tolist()
    }
    
    # Corridor performance table
    corridor_performance = recent_tasks.groupby('Corridor').agg({
        'SignalID': 'count',
        'Date_Resolved': lambda x: x.isna().sum(),
        'Time To Resolve In Days': 'mean'
    }).reset_index()
    
    corridor_performance.columns = ['corridor', 'total_tasks', 'open_tasks', 'avg_resolution_days']
    corridor_performance['closed_tasks'] = corridor_performance['total_tasks'] - corridor_performance['open_tasks']
    corridor_performance['closure_rate'] = (
        corridor_performance['closed_tasks'] / corridor_performance['total_tasks'] * 100
    ).round(1)
    corridor_performance['avg_resolution_days'] = corridor_performance['avg_resolution_days'].round(1)
    corridor_performance = corridor_performance.sort_values('total_tasks', ascending=False)
    
    dashboard_data['tables']['corridor_performance'] = corridor_performance.to_dict('records')
    
    # Filter options
    dashboard_data['filters'] = {
        'corridors': sorted(recent_tasks['Corridor'].dropna().unique().tolist()),
        'task_types': sorted(recent_tasks['Task_Type'].dropna().unique().tolist()),
        'priorities': sorted(recent_tasks['Priority'].dropna().unique().tolist()),
        'zones': sorted(recent_tasks['Zone_Group'].dropna().unique().tolist())
    }
    
    return dashboard_data

def analyze_teams_performance_trends(teams_df, analysis_months=24):
    """Analyze performance trends over time"""
    
    if teams_df.empty:
        return {}
    
    # Filter to analysis period
    cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=analysis_months)
    analysis_data = teams_df[
        pd.to_datetime(teams_df['Date_Reported']) >= cutoff_date
    ].copy()
    
    if analysis_data.empty:
        return {}
    
    trends = {
        'volume_trends': {},
        'resolution_trends': {},
        'corridor_trends': {},
        'seasonal_patterns': {}
    }
    
    # Volume trends by month
    analysis_data['report_month'] = pd.to_datetime(analysis_data['Date_Reported']).dt.to_period('M')
    monthly_volume = analysis_data.groupby('report_month').size().reset_index(name='task_count')
    monthly_volume['month'] = monthly_volume['report_month'].astype(str)
    
    # Calculate trend (simple linear)
    from scipy import stats
    x = np.arange(len(monthly_volume))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, monthly_volume['task_count'])
    
    trends['volume_trends'] = {
        'monthly_counts': monthly_volume[['month', 'task_count']].to_dict('records'),
        'trend_slope': round(slope, 2),
        'trend_direction': 'increasing' if slope > 0 else 'decreasing' if slope < 0 else 'stable',
        'correlation': round(r_value, 3),
        'significance': 'significant' if p_value < 0.05 else 'not_significant'
    }
    
    # Resolution time trends
    resolved_data = analysis_data[analysis_data['Time To Resolve In Days'].notna()].copy()
    if not resolved_data.empty:
        resolved_data['resolve_month'] = pd.to_datetime(resolved_data['Date_Resolved']).dt.to_period('M')
        monthly_resolution = resolved_data.groupby('resolve_month')['Time To Resolve In Days'].mean().reset_index()
        monthly_resolution['month'] = monthly_resolution['resolve_month'].astype(str)
        
        # Resolution time trend
        x_res = np.arange(len(monthly_resolution))
        slope_res, intercept_res, r_value_res, p_value_res, std_err_res = stats.linregress(
            x_res, monthly_resolution['Time To Resolve In Days']
        )
        
        trends['resolution_trends'] = {
            'monthly_avg_days': monthly_resolution[['month', 'Time To Resolve In Days']].to_dict('records'),
            'trend_slope': round(slope_res, 2),
            'trend_direction': 'increasing' if slope_res > 0 else 'decreasing' if slope_res < 0 else 'stable',
            'correlation': round(r_value_res, 3),
            'significance': 'significant' if p_value_res < 0.05 else 'not_significant'
        }
    
    # Corridor performance trends (top 10 by volume)
    top_corridors = analysis_data['Corridor'].value_counts().head(10).index
    corridor_trends_data = {}
    
    for corridor in top_corridors:
        corridor_data = analysis_data[analysis_data['Corridor'] == corridor].copy()
        corridor_monthly = corridor_data.groupby('report_month').size().reset_index(name='task_count')
        
        if len(corridor_monthly) > 3:  # Need at least 4 points for trend
            x_corr = np.arange(len(corridor_monthly))
            slope_corr, _, r_value_corr, p_value_corr, _ = stats.linregress(x_corr, corridor_monthly['task_count'])
            
            corridor_trends_data[corridor] = {
                'trend_slope': round(slope_corr, 2),
                'trend_direction': 'increasing' if slope_corr > 0 else 'decreasing' if slope_corr < 0 else 'stable',
                'significance': 'significant' if p_value_corr < 0.05 else 'not_significant'
            }
    
    trends['corridor_trends'] = corridor_trends_data
        # Seasonal patterns
    analysis_data['report_month_num'] = pd.to_datetime(analysis_data['Date_Reported']).dt.month
    analysis_data['report_quarter'] = pd.to_datetime(analysis_data['Date_Reported']).dt.quarter
    analysis_data['report_dow'] = pd.to_datetime(analysis_data['Date_Reported']).dt.dayofweek
    
    monthly_pattern = analysis_data.groupby('report_month_num').size().reset_index(name='avg_tasks')
    quarterly_pattern = analysis_data.groupby('report_quarter').size().reset_index(name='avg_tasks')
    dow_pattern = analysis_data.groupby('report_dow').size().reset_index(name='avg_tasks')
    
    # Day of week labels
    dow_labels = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_pattern['day_name'] = dow_pattern['report_dow'].map(lambda x: dow_labels[x])
    
    trends['seasonal_patterns'] = {
        'monthly': monthly_pattern.to_dict('records'),
        'quarterly': quarterly_pattern.to_dict('records'),
        'day_of_week': dow_pattern[['day_name', 'avg_tasks']].to_dict('records')
    }
    
    return trends

def calculate_teams_kpis(teams_df, comparison_period_months=12):
    """Calculate key performance indicators for TEAMS data"""
    
    if teams_df.empty:
        return {}
    
    current_date = pd.Timestamp.now()
    
    # Current period (last month)
    current_month_start = current_date.replace(day=1) - pd.DateOffset(months=1)
    current_month_end = current_date.replace(day=1) - pd.DateOffset(days=1)
    
    # Comparison period (same month last year)
    comparison_month_start = current_month_start - pd.DateOffset(months=comparison_period_months)
    comparison_month_end = current_month_end - pd.DateOffset(months=comparison_period_months)
    
    # Filter data for both periods
    current_period = teams_df[
        (pd.to_datetime(teams_df['Date_Reported']).dt.date >= current_month_start.date()) &
        (pd.to_datetime(teams_df['Date_Reported']).dt.date <= current_month_end.date())
    ].copy()
    
    comparison_period = teams_df[
        (pd.to_datetime(teams_df['Date_Reported']).dt.date >= comparison_month_start.date()) &
        (pd.to_datetime(teams_df['Date_Reported']).dt.date <= comparison_month_end.date())
    ].copy()
    
    def calculate_period_metrics(period_df):
        if period_df.empty:
            return {
                'total_tasks': 0,
                'avg_resolution_days': 0,
                'closure_rate': 0,
                'open_tasks': 0,
                'high_priority_tasks': 0,
                'overdue_tasks': 0
            }
        
        total_tasks = len(period_df)
        closed_tasks = len(period_df[period_df['Date_Resolved'].notna()])
        open_tasks = total_tasks - closed_tasks
        
        if closed_tasks > 0:
            avg_resolution = period_df[period_df['Time To Resolve In Days'].notna()]['Time To Resolve In Days'].mean()
        else:
            avg_resolution = 0
        
        closure_rate = (closed_tasks / total_tasks * 100) if total_tasks > 0 else 0
        
        high_priority = len(period_df[period_df['Priority'].isin(['High', 'Critical', 'Urgent'])])
        
        # Calculate overdue tasks (assuming 30 days is the target)
        today = pd.Timestamp.now().date()
        overdue = len(period_df[
            (period_df['Date_Resolved'].isna()) &
            ((today - pd.to_datetime(period_df['Date_Reported']).dt.date).dt.days > 30)
        ])
        
        return {
            'total_tasks': total_tasks,
            'avg_resolution_days': round(avg_resolution, 1),
            'closure_rate': round(closure_rate, 1),
            'open_tasks': open_tasks,
            'high_priority_tasks': high_priority,
            'overdue_tasks': overdue
        }
    
    current_metrics = calculate_period_metrics(current_period)
    comparison_metrics = calculate_period_metrics(comparison_period)
    
    # Calculate percent changes
    def calculate_change(current, previous):
        if previous == 0:
            return 0 if current == 0 else 100
        return round(((current - previous) / previous) * 100, 1)
    
    kpis = {
        'current_period': current_metrics,
        'comparison_period': comparison_metrics,
        'changes': {
            'total_tasks': calculate_change(current_metrics['total_tasks'], comparison_metrics['total_tasks']),
            'avg_resolution_days': calculate_change(current_metrics['avg_resolution_days'], comparison_metrics['avg_resolution_days']),
            'closure_rate': calculate_change(current_metrics['closure_rate'], comparison_metrics['closure_rate']),
            'open_tasks': calculate_change(current_metrics['open_tasks'], comparison_metrics['open_tasks']),
            'high_priority_tasks': calculate_change(current_metrics['high_priority_tasks'], comparison_metrics['high_priority_tasks']),
            'overdue_tasks': calculate_change(current_metrics['overdue_tasks'], comparison_metrics['overdue_tasks'])
        },
        'period_info': {
            'current_period': f"{current_month_start.strftime('%Y-%m')}",
            'comparison_period': f"{comparison_month_start.strftime('%Y-%m')}"
        }
    }
    
    return kpis

def generate_teams_alerts(teams_df, thresholds=None):
    """Generate alerts based on TEAMS data anomalies and thresholds"""
    
    if teams_df.empty:
        return []
    
    # Default thresholds
    if thresholds is None:
        thresholds = {
            'max_resolution_days': 90,
            'high_priority_open_days': 30,
            'corridor_backlog_threshold': 50,
            'monthly_volume_increase': 50  # percent
        }
    
    alerts = []
    current_date = pd.Timestamp.now()
    
    # Alert 1: Tasks taking too long to resolve
    long_resolution_tasks = teams_df[
        (teams_df['Time To Resolve In Days'] > thresholds['max_resolution_days']) &
        teams_df['Date_Resolved'].notna()
    ]
    
    if not long_resolution_tasks.empty:
        alerts.append({
            'type': 'warning',
            'category': 'resolution_time',
            'message': f"{len(long_resolution_tasks)} tasks took longer than {thresholds['max_resolution_days']} days to resolve",
            'count': len(long_resolution_tasks),
            'details': long_resolution_tasks[['SignalID', 'Corridor', 'Task_Type', 'Time To Resolve In Days']].to_dict('records')
        })
    
    # Alert 2: High priority tasks open too long
    high_priority_open = teams_df[
        (teams_df['Priority'].isin(['High', 'Critical', 'Urgent'])) &
        (teams_df['Date_Resolved'].isna()) &
        ((current_date.date() - pd.to_datetime(teams_df['Date_Reported']).dt.date).dt.days > thresholds['high_priority_open_days'])
    ]
    
    if not high_priority_open.empty:
        alerts.append({
            'type': 'critical',
            'category': 'high_priority_overdue',
            'message': f"{len(high_priority_open)} high priority tasks have been open for more than {thresholds['high_priority_open_days']} days",
            'count': len(high_priority_open),
            'details': high_priority_open[['SignalID', 'Corridor', 'Task_Type', 'Priority', 'Date_Reported']].to_dict('records')
        })
    
    # Alert 3: Corridors with high task backlog
    corridor_backlogs = teams_df[teams_df['Date_Resolved'].isna()].groupby('Corridor').size()
    high_backlog_corridors = corridor_backlogs[corridor_backlogs > thresholds['corridor_backlog_threshold']]
    
    if not high_backlog_corridors.empty:
        alerts.append({
            'type': 'warning',
            'category': 'corridor_backlog',
            'message': f"{len(high_backlog_corridors)} corridors have more than {thresholds['corridor_backlog_threshold']} open tasks",
            'count': len(high_backlog_corridors),
            'details': [{'corridor': corridor, 'open_tasks': count} for corridor, count in high_backlog_corridors.items()]
        })
    
    # Alert 4: Significant increase in monthly task volume
    current_month = current_date.replace(day=1)
    last_month = current_month - pd.DateOffset(months=1)
    previous_month = current_month - pd.DateOffset(months=2)
    
    current_month_tasks = len(teams_df[
        (pd.to_datetime(teams_df['Date_Reported']) >= last_month) &
        (pd.to_datetime(teams_df['Date_Reported']) < current_month)
    ])
    
    previous_month_tasks = len(teams_df[
        (pd.to_datetime(teams_df['Date_Reported']) >= previous_month) &
        (pd.to_datetime(teams_df['Date_Reported']) < last_month)
    ])
    
    if previous_month_tasks > 0:
        volume_increase = ((current_month_tasks - previous_month_tasks) / previous_month_tasks) * 100
        if volume_increase > thresholds['monthly_volume_increase']:
            alerts.append({
                'type': 'info',
                'category': 'volume_increase',
                'message': f"Task volume increased by {volume_increase:.1f}% compared to previous month",
                'count': current_month_tasks - previous_month_tasks,
                'details': {
                    'current_month_tasks': current_month_tasks,
                    'previous_month_tasks': previous_month_tasks,
                    'increase_percent': round(volume_increase, 1)
                }
            })
    
    # Alert 5: Tasks with missing critical information
    missing_info_tasks = teams_df[
        teams_df['SignalID'].isna() | 
        teams_df['Corridor'].isna() |
        teams_df['Task_Type'].isna()
    ]
    
    if not missing_info_tasks.empty:
        alerts.append({
            'type': 'warning',
            'category': 'data_quality',
            'message': f"{len(missing_info_tasks)} tasks have missing critical information",
            'count': len(missing_info_tasks),
            'details': {
                'missing_signal_id': missing_info_tasks['SignalID'].isna().sum(),
                'missing_corridor': missing_info_tasks['Corridor'].isna().sum(),
                'missing_task_type': missing_info_tasks['Task_Type'].isna().sum()
            }
        })
    
    # Sort alerts by priority
    priority_order = {'critical': 1, 'warning': 2, 'info': 3}
    alerts.sort(key=lambda x: priority_order.get(x['type'], 4))
    
    return alerts

def create_teams_executive_summary(teams_df, report_period_months=3):
    """Create executive summary of TEAMS performance"""
    
    if teams_df.empty:
        return {}
    
    # Filter to report period
    cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=report_period_months)
    report_data = teams_df[
        pd.to_datetime(teams_df['Date_Reported']) >= cutoff_date
    ].copy()
    
    if report_data.empty:
        return {}
    
    summary = {
        'report_period': f"Last {report_period_months} months",
        'key_metrics': {},
        'top_issues': {},
        'performance_highlights': {},
        'recommendations': []
    }
    
    # Key metrics
    total_tasks = len(report_data)
    resolved_tasks = len(report_data[report_data['Date_Resolved'].notna()])
    open_tasks = total_tasks - resolved_tasks
    
    if resolved_tasks > 0:
        avg_resolution = report_data[report_data['Time To Resolve In Days'].notna()]['Time To Resolve In Days'].mean()
        median_resolution = report_data[report_data['Time To Resolve In Days'].notna()]['Time To Resolve In Days'].median()
    else:
        avg_resolution = 0
        median_resolution = 0
    
    summary['key_metrics'] = {
        'total_tasks': total_tasks,
        'resolved_tasks': resolved_tasks,
        'open_tasks': open_tasks,
        'resolution_rate': round((resolved_tasks / total_tasks * 100), 1) if total_tasks > 0 else 0,
        'avg_resolution_days': round(avg_resolution, 1),
        'median_resolution_days': round(median_resolution, 1)
    }
    
    # Top issues by volume
    task_type_counts = report_data['Task_Type'].value_counts().head(5)
    priority_counts = report_data['Priority'].value_counts()
    corridor_counts = report_data['Corridor'].value_counts().head(10)
    
    summary['top_issues'] = {
        'most_common_task_types': task_type_counts.to_dict(),
        'priority_distribution': priority_counts.to_dict(),
        'busiest_corridors': corridor_counts.to_dict()
    }
    
    # Performance highlights
    best_corridors = report_data.groupby('Corridor').agg({
        'Time To Resolve In Days': 'mean',
        'SignalID': 'count'
    }).reset_index()
    best_corridors = best_corridors[best_corridors['SignalID'] >= 5]  # At least 5 tasks
    best_corridors = best_corridors.nsmallest(5, 'Time To Resolve In Days')
    
    worst_corridors = report_data.groupby('Corridor').agg({
        'Time To Resolve In Days': 'mean',
        'SignalID': 'count'
    }).reset_index()
    worst_corridors = worst_corridors[worst_corridors['SignalID'] >= 5]
    worst_corridors = worst_corridors.nlargest(5, 'Time To Resolve In Days')
    
    
    summary['performance_highlights'] = {
        'best_performing_corridors': best_corridors.to_dict('records'),
        'worst_performing_corridors': worst_corridors.to_dict('records'),
        'fastest_resolution': round(report_data['Time To Resolve In Days'].min(), 1) if not report_data['Time To Resolve In Days'].isna().all() else 0,
        'longest_resolution': round(report_data['Time To Resolve In Days'].max(), 1) if not report_data['Time To Resolve In Days'].isna().all() else 0
    }
    
    # Generate recommendations based on data analysis
    recommendations = []
    
    # High backlog recommendation
    open_task_count = len(report_data[report_data['Date_Resolved'].isna()])
    if open_task_count > total_tasks * 0.3:  # More than 30% open
        recommendations.append({
            'priority': 'high',
            'category': 'backlog_management',
            'recommendation': f"Address high task backlog - {open_task_count} tasks currently open ({round(open_task_count/total_tasks*100, 1)}% of total)"
        })
    
    # Resolution time recommendation
    if avg_resolution > 45:  # More than 45 days average
        recommendations.append({
            'priority': 'medium',
            'category': 'efficiency',
            'recommendation': f"Focus on reducing resolution time - current average is {round(avg_resolution, 1)} days"
        })
    
    # Priority task recommendation
    high_priority_open = len(report_data[
        (report_data['Priority'].isin(['High', 'Critical', 'Urgent'])) &
        (report_data['Date_Resolved'].isna())
    ])
    if high_priority_open > 0:
        recommendations.append({
            'priority': 'high',
            'category': 'priority_management',
            'recommendation': f"Prioritize {high_priority_open} high-priority open tasks"
        })
    
    # Corridor-specific recommendation
    if not worst_corridors.empty:
        worst_corridor = worst_corridors.iloc[0]
        recommendations.append({
            'priority': 'medium',
            'category': 'corridor_focus',
            'recommendation': f"Focus improvement efforts on {worst_corridor['Corridor']} corridor (avg resolution: {round(worst_corridor['Time To Resolve In Days'], 1)} days)"
        })
    
    summary['recommendations'] = recommendations
    
    return summary

# Example usage and utility functions
def main():
    """Example usage of the teams module"""
    
    # Example configuration
    config = {
        'bucket': 'your-s3-bucket',
        'teams_archived_prefix': 'teams/archived/',
        'teams_current_key': 'teams/current_tasks.csv',
        'report_start_date': '2023-01-01'
    }
    
    # Example corridors data
    corridors_df = pd.DataFrame({
        'SignalID': ['1001', '1002', '1003'],
        'Zone_Group': ['Zone 1', 'Zone 1', 'Zone 2'],
        'Zone': ['Zone 1', 'Zone 1', 'Zone 2'],
        'Corridor': ['Corridor A', 'Corridor A', 'Corridor B'],
        'Subcorridor': ['Sub A1', 'Sub A2', 'Sub B1'],
        'Latitude': [33.7490, 33.7500, 33.7510],
        'Longitude': [-84.3880, -84.3890, -84.3900],
        'TeamsLocationID': [101, 102, 103]
    })
    
    print("Teams module loaded successfully!")
    print("Available functions:")
    functions = [name for name in globals() if callable(globals()[name]) and not name.startswith('_')]
    for func in sorted(functions):
        if func != 'main':
            print(f"  - {func}")

if __name__ == "__main__":
    main()

# Additional helper functions for integration with the broader system

def upload_teams_data_to_s3(teams_df, bucket, prefix="teams/processed/", 
                           date_column='Date_Reported', s3_client=None):
    """Upload processed TEAMS data to S3 in parquet format"""
    
    if teams_df.empty:
        print("No data to upload")
        return
    
    try:
        import boto3
        if s3_client is None:
            s3_client = boto3.client('s3')
        
        # Group by date for partitioned upload
        teams_df['upload_date'] = pd.to_datetime(teams_df[date_column]).dt.date
        
        for date, group in teams_df.groupby('upload_date'):
            # Remove the temporary upload_date column
            group = group.drop('upload_date', axis=1)
            
            # Convert to parquet
            parquet_buffer = io.BytesIO()
            group.to_parquet(parquet_buffer, index=False)
            parquet_buffer.seek(0)
            
            # Upload to S3
            key = f"{prefix}date={date}/teams_data_{date}.parquet"
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=parquet_buffer.getvalue()
            )
            
            print(f"Uploaded {len(group)} records for {date} to s3://{bucket}/{key}")
            
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def read_teams_data_from_s3(bucket, prefix="teams/processed/", 
                           start_date=None, end_date=None, s3_client=None):
    """Read TEAMS data from S3 parquet files"""
    
    try:
        import boto3
        if s3_client is None:
            s3_client = boto3.client('s3')
        
        # List objects in the prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            print("No TEAMS data found in S3")
            return pd.DataFrame()
        
        dataframes = []
        
        for obj in response['Contents']:
            key = obj['Key']
            
            # Extract date from key if date filtering is needed
            if start_date or end_date:
                import re
                date_match = re.search(r'date=(\d{4}-\d{2}-\d{2})', key)
                if date_match:
                    file_date = pd.to_datetime(date_match.group(1)).date()
                    
                    if start_date and file_date < pd.to_datetime(start_date).date():
                        continue
                    if end_date and file_date > pd.to_datetime(end_date).date():
                        continue
            
            # Read parquet file from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            dataframes.append(df)
        
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            print(f"Read {len(combined_df)} TEAMS records from S3")
            return combined_df
        else:
            print("No matching TEAMS data found")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"Error reading from S3: {e}")
        return pd.DataFrame()

def create_teams_data_pipeline(config):
    """Create a complete data pipeline for TEAMS processing"""
    
    pipeline_steps = {
        'extract': lambda: get_teams_tasks_from_s3(
            config['bucket'],
            config.get('teams_archived_prefix', 'teams/archived/'),
            config.get('teams_current_key', 'teams/current_tasks.csv'),
            config.get('report_start_date', '2023-01-01')
        ),
        'transform': lambda raw_data: tidy_teams_tasks(
            raw_data,
            config['bucket'],
            config.get('corridors', pd.DataFrame()),
            replicate=True
        ),
        'validate': validate_teams_data,
        'analyze': lambda clean_data: {
            'kpis': calculate_teams_kpis(clean_data),
            'trends': analyze_teams_performance_trends(clean_data),
            'alerts': generate_teams_alerts(clean_data),
            'summary': create_teams_executive_summary(clean_data)
        },
        'load': lambda processed_data: upload_teams_data_to_s3(
            processed_data,
            config['bucket'],
            config.get('output_prefix', 'teams/processed/')
        )
    }
    
    return pipeline_steps

def run_teams_etl_pipeline(config, execute_steps=['extract', 'transform', 'validate', 'analyze']):
    """Execute the complete TEAMS ETL pipeline"""
    
    pipeline = create_teams_data_pipeline(config)
    results = {}
    
    try:
        # Extract
        if 'extract' in execute_steps:
            print("Extracting TEAMS data...")
            raw_data = pipeline['extract']()
            results['raw_data'] = raw_data
            print(f"Extracted {len(raw_data)} raw TEAMS records")
        
        # Transform
        if 'transform' in execute_steps and 'raw_data' in results:
            print("Transforming TEAMS data...")
            clean_data = pipeline['transform'](results['raw_data'])
            results['clean_data'] = clean_data
            print(f"Transformed to {len(clean_data)} clean records")
        
        # Validate
        if 'validate' in execute_steps and 'clean_data' in results:
            print("Validating TEAMS data...")
            validation_results = pipeline['validate'](results['clean_data'])
            results['validation'] = validation_results
            print(f"Data quality score: {validation_results['data_quality_score']}")
        
        # Analyze
        if 'analyze' in execute_steps and 'clean_data' in results:
            print("Analyzing TEAMS data...")
            analysis_results = pipeline['analyze'](results['clean_data'])
            results['analysis'] = analysis_results
            print("Analysis complete")
        
        # Load
        if 'load' in execute_steps and 'clean_data' in results:
            print("Loading TEAMS data...")
            pipeline['load'](results['clean_data'])
            print("Data loaded successfully")
        
        return results
        
    except Exception as e:
        print(f"Pipeline error: {e}")
        return results

# Configuration validation
def validate_teams_config(config):
    """Validate TEAMS configuration"""
    
    required_keys = ['bucket']
    optional_keys = {
        'teams_archived_prefix': 'teams/archived/',
        'teams_current_key': 'teams/current_tasks.csv', 
        'report_start_date': '2023-01-01',
        'output_prefix': 'teams/processed/'
    }
    
    errors = []
    warnings = []
    
    # Check required keys
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing required configuration key: {key}")
    
    # Set default values for optional keys
    for key, default_value in optional_keys.items():
        if key not in config:
            config[key] = default_value
            warnings.append(f"Using default value for {key}: {default_value}")
    
    # Validate date format
    try:
        pd.to_datetime(config.get('report_start_date'))
    except:
        errors.append("Invalid report_start_date format. Use YYYY-MM-DD")
    
    validation_result = {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings,
        'config': config
    }
    
    return validation_result