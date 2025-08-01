#!/usr/bin/env python3

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import awswrangler as wr
import boto3
from s3_parquet_io import s3_upload_parquet_date_split
from database_functions import get_athena_connection
from utilities import keep_trying
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

def get_counts2(date_, bucket, conf_athena, uptime=True, counts=True):
    """Get counts data for a specific date"""
    
    date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)
    
    try:
        # Query to get counts data from Athena
        query = f"""
        SELECT SignalID, CallPhase, Detector, Timeperiod, 
               EventCode, EventParam
        FROM {conf_athena['database']}.{conf_athena['atspm_table']}
        WHERE date = '{date_str}'
        AND EventCode IN (1, 82)  -- Detector events
        """
        
        session = boto3.Session(
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd')
        )
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf_athena['database'],
            s3_output=conf_athena['staging_dir'],
            boto3_session=session
        )
        
        if df.empty:
            print(f"No data found for {date_str}")
            return
        
        # Process counts
        if counts:
            counts_1hr = process_counts_hourly(df, date_str)
            counts_15min = process_counts_15min(df, date_str)
            
            # Upload to S3
            s3_upload_parquet_date_split(
                counts_1hr,
                prefix="counts_1hr",
                bucket=bucket,
                table_name="counts_1hr", 
                conf_athena=conf_athena
            )
            
            s3_upload_parquet_date_split(
                counts_15min,
                prefix="counts_15min", 
                bucket=bucket,
                table_name="counts_15min",
                conf_athena=conf_athena
            )
        
        # Process uptime
        if uptime:
            uptime_data = process_detector_uptime(df, date_str)
            
            s3_upload_parquet_date_split(
                uptime_data,
                prefix="detector_uptime",
                bucket=bucket,
                table_name="detector_uptime",
                conf_athena=conf_athena
            )
            
    except Exception as e:
        print(f"Error processing counts for {date_str}: {e}")
        raise

def process_counts_hourly(df, date_str):
    """Process hourly counts from detector events"""
    
    # Convert Timeperiod to datetime
    df['Timeperiod'] = pd.to_datetime(df['Timeperiod'])
    
    # Filter for detector on events (EventCode = 82)
    on_events = df[df['EventCode'] == 82].copy()
    
    # Group by hour
    on_events['Hour'] = on_events['Timeperiod'].dt.floor('H')
    
    # Count events per hour
    hourly_counts = on_events.groupby(['SignalID', 'CallPhase', 'Detector', 'Hour']).size().reset_index(name='vol')
    
    # Rename Hour back to Timeperiod for consistency
    hourly_counts = hourly_counts.rename(columns={'Hour': 'Timeperiod'})
    
    # Add Date column
    hourly_counts['Date'] = pd.to_datetime(date_str).date()
    
    return hourly_counts

def process_counts_15min(df, date_str):
    """Process 15-minute counts from detector events"""
    
    # Convert Timeperiod to datetime
    df['Timeperiod'] = pd.to_datetime(df['Timeperiod'])
    
    # Filter for detector on events (EventCode = 82)
    on_events = df[df['EventCode'] == 82].copy()
    
    # Group by 15-minute intervals
    on_events['Period15'] = on_events['Timeperiod'].dt.floor('15T')
    
    # Count events per 15-minute period
    counts_15min = on_events.groupby(['SignalID', 'CallPhase', 'Detector', 'Period15']).size().reset_index(name='vol')
    
    # Rename Period15 back to Timeperiod
    counts_15min = counts_15min.rename(columns={'Period15': 'Timeperiod'})
    
    # Add Date column
    counts_15min['Date'] = pd.to_datetime(date_str).date()
    
    return counts_15min

def process_detector_uptime(df, date_str):
    """Process detector uptime from events"""
    
    # Convert Timeperiod to datetime
    df['Timeperiod'] = pd.to_datetime(df['Timeperiod'])
    
    # Group by hour for uptime calculation
    df['Hour'] = df['Timeperiod'].dt.floor('H')
    
    # Calculate uptime based on presence of events
    uptime_data = []
    
    for (signal, phase, detector), group in df.groupby(['SignalID', 'CallPhase', 'Detector']):
        hourly_events = group.groupby('Hour').size()
        
        # Create full hour range for the day
        start_hour = pd.to_datetime(f"{date_str} 00:00:00")
        end_hour = pd.to_datetime(f"{date_str} 23:00:00")
        full_hours = pd.date_range(start=start_hour, end=end_hour, freq='H')
        
        for hour in full_hours:
            # Detector is considered "up" if it has events in that hour
            good_day = 1 if hour in hourly_events.index and hourly_events[hour] > 0 else 0
            
            uptime_data.append({
                'SignalID': signal,
                'CallPhase': phase,
                'Detector': detector,
                'Timeperiod': hour,
                'Good_Day': good_day,
                'Date': pd.to_datetime(date_str).date()
            })
    
    return pd.DataFrame(uptime_data)

def get_vpd(adjusted_counts):
    """Calculate vehicles per day from counts"""
    
    # Group by signal and date, sum volumes
    vpd = adjusted_counts.groupby(['SignalID', 'Date', 'CallPhase'])['vol'].sum().reset_index()
    vpd = vpd.rename(columns={'vol': 'vpd'})
    
    # Add day of week
    vpd['DOW'] = pd.to_datetime(vpd['Date']).dt.dayofweek + 1  # 1=Monday, 7=Sunday
    
    return vpd

def get_vph(counts, interval="1 hour", mainline_only=True):
    """Calculate vehicles per hour/period from counts"""
    
    if mainline_only:
        counts = counts[counts['CallPhase'].isin([2, 6])]
    
    # Group by the specified interval
    if interval == "1 hour":
        counts['Period'] = pd.to_datetime(counts['Timeperiod']).dt.floor('H')
        period_col = 'Hour'
    elif interval == "15 min":
        counts['Period'] = pd.to_datetime(counts['Timeperiod']).dt.floor('15T')
        period_col = 'Period'
    else:
        counts['Period'] = pd.to_datetime(counts['Timeperiod']).dt.floor(interval)
        period_col = 'Period'
    
    # Sum volumes by period
    vph = counts.groupby(['SignalID', 'Period'])['vol'].sum().reset_index()
    vph = vph.rename(columns={'Period': period_col, 'vol': 'vph' if interval == "1 hour" else 'vol'})
    
    # Add additional time columns
    vph['DOW'] = pd.to_datetime(vph[period_col]).dt.dayofweek + 1
    vph['Week'] = pd.to_datetime(vph[period_col]).dt.isocalendar().week
    
    return vph

def get_thruput(adjusted_counts_15min):
    """Calculate throughput from 15-minute counts"""
    
    # Filter for mainline phases only
    mainline_counts = adjusted_counts_15min[adjusted_counts_15min['CallPhase'].isin([2, 6])]
    
    # Group by signal and timeperiod, sum volumes
    throughput = mainline_counts.groupby(['SignalID', 'Timeperiod'])['vol'].sum().reset_index()
    throughput = throughput.rename(columns={'vol': 'vph'})  # Volume per 15-min period
    
    # Add time-based columns
    throughput['DOW'] = pd.to_datetime(throughput['Timeperiod']).dt.dayofweek + 1
    throughput['Week'] = pd.to_datetime(throughput['Timeperiod']).dt.isocalendar().week
    throughput['Date'] = pd.to_datetime(throughput['Timeperiod']).dt.date
    
    return throughput

def prep_db_for_adjusted_counts_arrow(table_name, conf, date_range):
    """Prepare data for adjusted counts calculation using Arrow"""
    
    # Create directory for Arrow dataset
    import os
    os.makedirs(table_name, exist_ok=True)
   
    for date_ in date_range:
        # date_str = date_.strftime('%Y-%m-%d')
        date_str = date_
        
        try:
            # Read raw counts from S3
            s3_path = f"s3://{conf['bucket']}/mark/counts_1hr/date={date_str}/"
            
            session = boto3.Session(
                aws_access_key_id=conf['athena'].get('uid'),
                aws_secret_access_key=conf['athena'].get('pwd')
            )
            
            # Read parquet files for this date
            try:
                df = wr.s3.read_parquet(path=s3_path, boto3_session=session)
            except Exception:
                # If no data for this date, create empty dataframe
                df = pd.DataFrame(columns=['SignalID', 'CallPhase', 'Detector', 'Timeperiod', 'vol'])
            
            if not df.empty:
                # Add date column
                df['Date'] = date_
                df['date'] = date_str  # String version for partitioning
                
                # Filter and clean data
                df = filter_counts_data(df)
                
                # Write to Arrow format
                table = pa.Table.from_pandas(df)
                pq.write_table(table, f"{table_name}/date={date_str}.parquet")
            
        except Exception as e:
            print(f"Error preparing data for {date_str}: {e}")

def filter_counts_data(df):
    """Filter counts data to remove invalid entries"""
    
    # Convert columns to numeric, handling any non-numeric values
    df = df.copy()
    df['vol'] = pd.to_numeric(df['vol'], errors='coerce')
    df['Detector'] = pd.to_numeric(df['Detector'], errors='coerce')
    df['CallPhase'] = pd.to_numeric(df['CallPhase'], errors='coerce')
    
    # Remove entries with NaN values (from conversion errors)
    df = df.dropna(subset=['vol', 'Detector', 'CallPhase'])
    
    # Remove entries with zero or negative volume
    df = df[df['vol'] > 0]
    
    # Remove entries with invalid detector numbers
    df = df[df['Detector'] > 0]
    
    # Remove entries outside reasonable volume ranges (outlier detection)
    df = df[df['vol'] <= 2000]  # Max reasonable hourly volume
    
    # Ensure proper data types (only if dataframe is not empty)
    if not df.empty:
        df['SignalID'] = df['SignalID'].astype(str)
        df['CallPhase'] = df['CallPhase'].astype(int)
        df['Detector'] = df['Detector'].astype(int)
        df['vol'] = df['vol'].astype(int)
    
    return df

def get_adjusted_counts_arrow(input_table, output_table, conf):
    """Calculate adjusted counts using Arrow dataset"""
    
    import os
    import shutil
    
    # Create output directory
    os.makedirs(output_table, exist_ok=True)
    
    try:
        # Read input dataset
        import pyarrow.dataset as ds
        dataset = ds.dataset(input_table, format="parquet")
        
        # Process each date partition
        for partition in dataset.get_fragments():
            try:
                # Read partition data
                df = partition.to_table().to_pandas()
                
                if df.empty:
                    continue
                
                # Get the date for this partition
                date_str = df['date'].iloc[0]
                
                # Apply adjustments
                adjusted_df = apply_count_adjustments(df, conf)
                
                # Write adjusted data
                if not adjusted_df.empty:
                    table = pa.Table.from_pandas(adjusted_df)
                    pq.write_table(table, f"{output_table}/date={date_str}.parquet")
                
            except Exception as e:
                print(f"Error processing partition: {e}")
                continue
                
    except Exception as e:
        print(f"Error in adjusted counts calculation: {e}")

def apply_count_adjustments(df, conf):
    """Apply adjustments to count data (fill gaps, smooth outliers, etc.)"""
    
    if df.empty:
        return df
    
    # Get month boundaries for the data
    dates = pd.to_datetime(df['Date']).dt.to_period('M').unique()
    
    adjusted_dfs = []
    
    for period in dates:
        month_df = df[pd.to_datetime(df['Date']).dt.to_period('M') == period].copy()
        
        # Calculate monthly averages for gap filling
        monthly_avg = calculate_monthly_averages(month_df)
        
        # Fill gaps and adjust outliers
        adjusted_month_df = fill_missing_data(month_df, monthly_avg)
        adjusted_month_df = smooth_outliers(adjusted_month_df, monthly_avg)
        
        adjusted_dfs.append(adjusted_month_df)
    
    return pd.concat(adjusted_dfs, ignore_index=True) if adjusted_dfs else pd.DataFrame()

def calculate_monthly_averages(df):
    """Calculate monthly averages by signal, phase, detector, hour of day, day of week"""
    
    df['Hour_of_Day'] = pd.to_datetime(df['Timeperiod']).dt.hour
    df['Day_of_Week'] = pd.to_datetime(df['Timeperiod']).dt.dayofweek
    
    monthly_avg = df.groupby([
        'SignalID', 'CallPhase', 'Detector', 'Hour_of_Day', 'Day_of_Week'
    ])['vol'].agg(['mean', 'std', 'count']).reset_index()
    
    # Only use averages where we have sufficient data points
    monthly_avg = monthly_avg[monthly_avg['count'] >= 3]
    
    return monthly_avg

def fill_missing_data(df, monthly_avg):
    """Fill missing data using monthly averages"""
    
    # Create complete time series for each detector
    detectors = df[['SignalID', 'CallPhase', 'Detector']].drop_duplicates()
    
    # Get date range
    start_date = df['Date'].min()
    end_date = df['Date'].max()
    
    complete_data = []
    
    for _, detector in detectors.iterrows():
        signal_id = detector['SignalID']
        call_phase = detector['CallPhase']
        detector_num = detector['Detector']
        
        # Create complete hourly time series
        date_range = pd.date_range(
            start=pd.to_datetime(start_date),
            end=pd.to_datetime(end_date) + timedelta(hours=23),
            freq='H'
        )
        
        for timestamp in date_range:
            existing = df[
                (df['SignalID'] == signal_id) &
                (df['CallPhase'] == call_phase) &
                (df['Detector'] == detector_num) &
                (df['Timeperiod'] == timestamp)
            ]
            
            if not existing.empty:
                # Use existing data
                complete_data.append(existing.iloc[0].to_dict())
            else:
                # Fill with monthly average if available
                hour_of_day = timestamp.hour
                day_of_week = timestamp.dayofweek
                
                avg_data = monthly_avg[
                    (monthly_avg['SignalID'] == signal_id) &
                    (monthly_avg['CallPhase'] == call_phase) &
                    (monthly_avg['Detector'] == detector_num) &
                    (monthly_avg['Hour_of_Day'] == hour_of_day) &
                    (monthly_avg['Day_of_Week'] == day_of_week)
                ]
                
                if not avg_data.empty:
                    vol = max(0, int(avg_data['mean'].iloc[0]))
                else:
                    vol = 0  # No data available
                
                complete_data.append({
                    'SignalID': signal_id,
                    'CallPhase': call_phase,
                    'Detector': detector_num,
                    'Timeperiod': timestamp,
                    'vol': vol,
                    'Date': timestamp.date(),
                    'date': timestamp.strftime('%Y-%m-%d')
                })
    
    return pd.DataFrame(complete_data)

def smooth_outliers(df, monthly_avg):
    """Smooth outliers using statistical methods"""
    
    df = df.copy()
    df['Hour_of_Day'] = pd.to_datetime(df['Timeperiod']).dt.hour
    df['Day_of_Week'] = pd.to_datetime(df['Timeperiod']).dt.dayofweek
    
    # Merge with monthly averages
    df = df.merge(
        monthly_avg,
        on=['SignalID', 'CallPhase', 'Detector', 'Hour_of_Day', 'Day_of_Week'],
        how='left',
        suffixes=('', '_avg')
    )
    
    # Identify outliers (values more than 3 standard deviations from mean)
    df['is_outlier'] = (
        (df['vol'] > df['mean'] + 3 * df['std']) |
        (df['vol'] < df['mean'] - 3 * df['std'])
    ) & (df['std'] > 0)
    
    # Replace outliers with capped values
    df.loc[df['is_outlier'] & (df['vol'] > df['mean'] + 3 * df['std']), 'vol'] = \
        df['mean'] + 2 * df['std']
    
    df.loc[df['is_outlier'] & (df['vol'] < df['mean'] - 3 * df['std']), 'vol'] = \
        np.maximum(0, df['mean'] - 2 * df['std'])
    
    # Clean up columns
    keep_cols = ['SignalID', 'CallPhase', 'Detector', 'Timeperiod', 'vol', 'Date', 'date']
    df = df[keep_cols]
    
    # Ensure volume is non-negative integer
    df['vol'] = df['vol'].fillna(0).astype(int).clip(lower=0)
    
    return df

def get_signalids_from_s3(date_, bucket='gdot-spm', prefix='mark/counts_1hr'):
    """Get list of signal IDs that have data for a given date"""
    
    date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)
    
    try:
        s3_path = f"s3://{bucket}/{prefix}/date={date_str}/"
        
        # Read parquet files to get signal IDs
        df = wr.s3.read_parquet(path=s3_path)
        
        if not df.empty and 'SignalID' in df.columns:
            return df['SignalID'].unique().tolist()
        else:
            return []
            
    except Exception as e:
        print(f"Error getting signal IDs for {date_str}: {e}")
        return []

def write_signal_details(date_str, conf, signals_list):
    """Write signal details for a given date"""
    
    try:
        # Get signals that had data on this date
        active_signals = get_signalids_from_s3(date_str, conf['bucket'])
        
        # Create signal details dataframe
        signal_details = pd.DataFrame({
            'SignalID': active_signals,
            'Date': pd.to_datetime(date_str).date(),
            'Active': True
        })
        
        # Upload to S3
        s3_upload_parquet_date_split(
            signal_details,
            prefix="signal_details",
            bucket=conf['bucket'],
            table_name="signal_details",
            conf_athena=conf['athena']
        )
        
    except Exception as e:
        print(f"Error writing signal details for {date_str}: {e}")
    """Identify bad detectors based on data patterns"""
    
def get_bad_detectors(filtered_counts, conf, date_range):
    """Identify bad detectors based on data patterns"""
    
    # This would implement logic to identify problematic detectors
    # based on various criteria like:
    # - No data for extended periods
    # - Unusual volume patterns
    # - Inconsistent reporting
    
    bad_detectors = []
    
    for date_ in date_range:
        try:
            # Load data for the date
            date_str = date_.strftime('%Y-%m-%d')
            s3_path = f"s3://{conf['bucket']}/mark/{filtered_counts}/date={date_str}/"
            
            session = boto3.Session(
                aws_access_key_id=conf['athena'].get('uid'),
                aws_secret_access_key=conf['athena'].get('pwd')
            )
            
            df = wr.s3.read_parquet(path=s3_path, boto3_session=session)
            
            if df.empty:
                continue
            
            # Analyze data patterns
            detector_stats = df.groupby(['SignalID', 'CallPhase', 'Detector']).agg({
                'vol': ['count', 'mean', 'std', 'min', 'max']
            }).reset_index()
            
            detector_stats.columns = ['SignalID', 'CallPhase', 'Detector', 
                                    'count', 'mean_vol', 'std_vol', 'min_vol', 'max_vol']
            
            # Identify bad detectors
            # Too few data points
            bad_count = detector_stats[detector_stats['count'] < 12]  # Less than half day
            
            # Unusual patterns (all zeros, constant values, extreme outliers)
            bad_pattern = detector_stats[
                (detector_stats['std_vol'] == 0) |  # No variation
                (detector_stats['max_vol'] > 1000)   # Extreme values
            ]
            
            date_bad = pd.concat([bad_count, bad_pattern]).drop_duplicates()
            date_bad['Date'] = pd.to_datetime(date_str).date()
            date_bad['Reason'] = 'Pattern Analysis'
            
            bad_detectors.append(date_bad)
            
        except Exception as e:
            print(f"Error analyzing bad detectors for {date_str}: {e}")
            continue
    
    return pd.concat(bad_detectors, ignore_index=True) if bad_detectors else pd.DataFrame()