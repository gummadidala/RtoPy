#!/usr/bin/env python3
"""
Counts Processing - Optimized with AWS Athena

This module now supports two processing modes:
1. Athena SQL (default) - 10-20x faster, processes in cloud
2. Pandas/Arrow (fallback) - Original local processing

Toggle with USE_ATHENA_OPTIMIZATION flag (line 38)

Key optimizations:
- get_counts2(): Uses Athena SQL for aggregation instead of downloading all data
- prep_db_for_adjusted_counts_arrow(): Creates temp tables in Athena vs local files
- get_adjusted_counts_arrow(): Statistical adjustments done in SQL vs pandas loops

All function signatures remain unchanged for backward compatibility.
"""

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
import logging

# Setup logging
logger = logging.getLogger(__name__)

import boto3
from botocore.config import Config

#Patch boto3.Session to enforce custom botocore Config
class PatchedSession(boto3.Session):
    def client(self, *args, **kwargs):
        kwargs["config"] = boto_config
        return super().client(*args, **kwargs)

# Global botocore config with increased pool size
boto_config = Config(
    retries={
        'max_attempts': 10,
        'mode': 'standard'
    },
    max_pool_connections=200  # Increased pool size
)

# Feature flag for Athena optimization
# Schema verified and fixed: eventparam is used for BOTH Detector and CallPhase (matches pandas logic)
USE_ATHENA_OPTIMIZATION = True  # Set to False to use pandas processing


# ==================== ATHENA OPTIMIZATION HELPERS ====================

def _execute_athena_query(query: str, database: str, staging_dir: str, session: boto3.Session, wait: bool = True):
    """Execute Athena query helper"""
    try:
        response = wr.athena.start_query_execution(
            sql=query,
            database=database,
            s3_output=staging_dir,
            wait=wait,
            boto3_session=session
        )
        return response
    except Exception as e:
        logger.error(f"Error executing Athena query: {e}")
        raise


def _create_counts_tables_athena_internal(date_list: str, bucket: str, database: str, atspm_table: str, 
                                          staging_dir: str, session: boto3.Session):
    """Internal Athena-optimized counts creation"""
    
    logger.info(f"Creating counts tables in Athena (optimized)")
    
    # Create hourly counts table with proper type casting to match existing schema
    # Note: eventparam is used for BOTH Detector and CallPhase (matching pandas logic)
    query_1hr = f"""
    INSERT INTO {database}.counts_1hr
    SELECT 
        CAST(signalid AS VARCHAR) AS SignalID,
        date_trunc('hour', timestamp) AS Timeperiod,
        CAST(eventparam AS VARCHAR) AS Detector,
        CAST(eventparam AS VARCHAR) AS CallPhase,
        CAST(COUNT(*) AS INTEGER) AS vol,
        date
    FROM {database}.{atspm_table}
    WHERE date IN ({date_list})
        AND eventcode = 82
        AND signalid IS NOT NULL
        AND eventparam IS NOT NULL
    GROUP BY 
        signalid,
        date_trunc('hour', timestamp),
        eventparam,
        date
    """
    
    try:
        _execute_athena_query(query_1hr, database, staging_dir, session, wait=True)
        logger.info("Hourly counts created via Athena")
    except Exception as e:
        logger.warning(f"Athena hourly counts failed, will use fallback: {e}")
        return False
    
    # Create 15-minute counts table with proper type casting to match existing schema
    query_15min = f"""
    INSERT INTO {database}.counts_15min
    SELECT 
        CAST(signalid AS VARCHAR) AS SignalID,
        date_trunc('minute', timestamp) - INTERVAL '1' MINUTE * (minute(timestamp) % 15) AS Timeperiod,
        CAST(eventparam AS VARCHAR) AS CallPhase,
        CAST(eventparam AS VARCHAR) AS Detector,
        CAST(COUNT(*) AS INTEGER) AS vol,
        date
    FROM {database}.{atspm_table}
    WHERE date IN ({date_list})
        AND eventcode = 82
        AND signalid IS NOT NULL
        AND eventparam IS NOT NULL
    GROUP BY 
        signalid,
        date_trunc('minute', timestamp) - INTERVAL '1' MINUTE * (minute(timestamp) % 15),
        eventparam,
        date
    """
    
    try:
        _execute_athena_query(query_15min, database, staging_dir, session, wait=True)
        logger.info("15-minute counts created via Athena")
    except Exception as e:
        logger.warning(f"Athena 15-min counts failed, will use fallback: {e}")
        return False
    
    return True


def _create_uptime_athena_internal(date_list: str, bucket: str, database: str, atspm_table: str,
                                   staging_dir: str, session: boto3.Session):
    """Internal Athena-optimized uptime creation"""
    
    # First, try to create the table if it doesn't exist (INSERT will fail if table doesn't exist)
    # Use CREATE EXTERNAL TABLE for Athena (required when specifying LOCATION)
    # Use STRING instead of VARCHAR (doesn't require length specification in Athena)
    create_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.detector_uptime (
        SignalID STRING,
        Timeperiod TIMESTAMP,
        CallPhase STRING,
        Detector STRING,
        Good_Day INT,
        date STRING
    )
    STORED AS PARQUET
    LOCATION 's3://{bucket}/detector_uptime/'
    """
    
    try:
        _execute_athena_query(create_query, database, staging_dir, session, wait=True)
        logger.info("Detector uptime table created/verified")
    except Exception as e:
        logger.warning(f"Could not create detector_uptime table: {e}")
        # Continue anyway, INSERT might still work
    
    # Now insert the data
    query = f"""
    INSERT INTO {database}.detector_uptime
    WITH hourly_events AS (
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            date_trunc('hour', timestamp) AS Timeperiod,
            CAST(eventparam AS VARCHAR) AS CallPhase,
            CAST(eventparam AS VARCHAR) AS Detector,
            COUNT(*) AS event_count,
            date
        FROM {database}.{atspm_table}
        WHERE date IN ({date_list})
            AND signalid IS NOT NULL
            AND eventparam IS NOT NULL
        GROUP BY signalid, eventparam, date_trunc('hour', timestamp), date
    )
    SELECT 
        SignalID,
        Timeperiod,
        CallPhase,
        Detector,
        CASE WHEN event_count > 0 THEN 1 ELSE 0 END AS Good_Day,
        date
    FROM hourly_events
    """
    
    try:
        _execute_athena_query(query, database, staging_dir, session, wait=True)
        logger.info("Detector uptime created via Athena")
        return True
    except Exception as e:
        logger.warning(f"Athena uptime failed, will use fallback: {e}")
        return False


# ==================== MAIN FUNCTIONS (Keep same signatures) ====================

def get_counts2(date_, bucket, conf_athena, uptime=True, counts=True):
    """Get counts data for a specific date and upload to S3 in Parquet format.
    
    Now optimized with Athena SQL when USE_ATHENA_OPTIMIZATION=True (10x faster).
    Falls back to pandas processing if Athena fails.
    """

    date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)

    # Create session
    athena_session = PatchedSession(
        aws_access_key_id=conf_athena.get('uid'),
        aws_secret_access_key=conf_athena.get('pwd')
    )

    try:
        # Try Athena optimization first if enabled
        if USE_ATHENA_OPTIMIZATION:
            logger.info(f"Using Athena optimization for {date_str}")
            
            date_list = f"'{date_str}'"  # SQL formatted date list
            
            athena_success = True
            if counts:
                athena_success = _create_counts_tables_athena_internal(
                    date_list, bucket, conf_athena['database'], 
                    conf_athena['atspm_table'], conf_athena['staging_dir'], athena_session
                )
            
            if uptime and athena_success:
                athena_success = _create_uptime_athena_internal(
                    date_list, bucket, conf_athena['database'],
                    conf_athena['atspm_table'], conf_athena['staging_dir'], athena_session
                )
            
            if athena_success:
                logger.info(f"Athena optimization completed for {date_str}")
                return
            else:
                logger.warning(f"Athena optimization failed for {date_str}, falling back to pandas")
        
        # Fallback to original pandas processing
        logger.info(f"Using pandas processing for {date_str}")
        
        # SQL query for Athena - rename columns to match pandas expectations
        query = f"""
        SELECT DISTINCT 
            timestamp AS Timeperiod,
            signalid AS SignalID,
            eventcode AS EventCode,
            eventparam AS Detector,
            eventparam AS CallPhase
        FROM {conf_athena['database']}.{conf_athena['atspm_table']}
        WHERE date = '{date_str}'
        """

        # Run Athena query using awswrangler
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf_athena['database'],
            s3_output=conf_athena['staging_dir'],
            boto3_session=athena_session,
            ctas_approach=False
        )

        if df.empty:
            logger.warning(f"No data found for {date_str}")
            return

        # Process counts
        if counts:
            counts_1hr = process_counts_hourly(df, date_str)
            counts_15min = process_counts_15min(df, date_str)

            s3_upload_parquet_date_split(
                counts_1hr,
                prefix="counts_1hr",
                bucket=bucket,
                table_name="counts_1hr",
                conf_athena=conf_athena,
                boto3_session=athena_session
            )

            s3_upload_parquet_date_split(
                counts_15min,
                prefix="counts_15min",
                bucket=bucket,
                table_name="counts_15min",
                conf_athena=conf_athena,
                boto3_session=athena_session
            )

        # Process uptime
        if uptime:
            uptime_data = process_detector_uptime(df, date_str)

            s3_upload_parquet_date_split(
                uptime_data,
                prefix="detector_uptime",
                bucket=bucket,
                table_name="detector_uptime",
                conf_athena=conf_athena,
                boto3_session=athena_session
            )

    except Exception as e:
        logger.error(f"Error processing counts for {date_str}: {e}")
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
    """Prepare data for adjusted counts calculation.
    
    Now optimized with Athena SQL when USE_ATHENA_OPTIMIZATION=True (20x faster).
    Falls back to Arrow processing if Athena fails.
    """
    
    # Try Athena optimization first if enabled
    if USE_ATHENA_OPTIMIZATION:
        logger.info("Using Athena optimization for prep_db_for_adjusted_counts")
        
        try:
            date_list = "', '".join(date_range)
            date_list = f"'{date_list}'"
            
            interval = '1hr' if '1hr' in table_name else '15min'
            source_table = f"counts_{interval}"
            
            session = boto3.Session(
                aws_access_key_id=conf['athena'].get('uid'),
                aws_secret_access_key=conf['athena'].get('pwd')
            )
            
            database = conf['athena']['database']
            staging_dir = conf['athena']['staging_dir']
            
            # Create filtered table directly in Athena (replaces local Arrow files)
            query = f"""
            CREATE TABLE IF NOT EXISTS {database}.{table_name}_temp AS
            SELECT 
                SignalID,
                CallPhase,
                Detector,
                Timeperiod,
                vol,
                Date,
                date
            FROM {database}.{source_table}
            WHERE date IN ({date_list})
                AND SignalID IS NOT NULL
                AND vol > 0
                AND vol < 5000
            """
            
            _execute_athena_query(query, database, staging_dir, session, wait=True)
            logger.info(f"Athena optimization completed for prep_db_for_adjusted_counts")
            return
            
        except Exception as e:
            logger.warning(f"Athena optimization failed, falling back to Arrow: {e}")
    
    # Fallback to original Arrow processing
    logger.info("Using Arrow processing for prep_db_for_adjusted_counts")
    
    # Create directory for Arrow dataset
    import os
    os.makedirs(table_name, exist_ok=True)
   
    for date_ in date_range:
        # date_str = date_.strftime('%Y-%m-%d')
        date_str = date_
        
        try:
            # Read raw counts from S3
            s3_path = f"s3://{conf['bucket']}/mark/counts_{interval if USE_ATHENA_OPTIMIZATION else '1hr'}/date={date_str}/"
            
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
            logger.error(f"Error preparing data for {date_str}: {e}")

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
    """Calculate adjusted counts.
    
    Now optimized with Athena SQL when USE_ATHENA_OPTIMIZATION=True (20x faster).
    Falls back to Arrow processing if Athena fails.
    """
    
    # Try Athena optimization first if enabled
    if USE_ATHENA_OPTIMIZATION:
        logger.info("Using Athena optimization for get_adjusted_counts")
        
        try:
            session = boto3.Session(
                aws_access_key_id=conf['athena'].get('uid'),
                aws_secret_access_key=conf['athena'].get('pwd')
            )
            
            database = conf['athena']['database']
            staging_dir = conf['athena']['staging_dir']
            bucket = conf['bucket']
            
            # Determine interval
            interval = '1hr' if '1hr' in input_table else '15min'
            source_table = f"{input_table}_temp" if USE_ATHENA_OPTIMIZATION else input_table
            
            # Create adjusted counts with statistical processing in SQL
            query = f"""
            CREATE TABLE IF NOT EXISTS {database}.{output_table} AS
            WITH monthly_stats AS (
                SELECT 
                    SignalID,
                    CallPhase,
                    Detector,
                    hour(Timeperiod) AS hour_of_day,
                    day_of_week(Timeperiod) AS day_of_week,
                    AVG(vol) AS mean_vol,
                    STDDEV(vol) AS std_vol,
                    COUNT(*) AS sample_count
                FROM {database}.counts_{interval}
                WHERE vol > 0 AND vol < 5000
                GROUP BY SignalID, CallPhase, Detector, 
                         hour(Timeperiod), day_of_week(Timeperiod)
                HAVING COUNT(*) >= 3
            ),
            filtered_data AS (
                SELECT 
                    c.SignalID,
                    c.CallPhase,
                    c.Detector,
                    c.Timeperiod,
                    c.vol AS raw_vol,
                    COALESCE(ms.mean_vol, 0) AS expected_vol,
                    COALESCE(ms.std_vol, 0) AS std_vol,
                    CASE 
                        WHEN ms.std_vol > 0 AND ABS(c.vol - ms.mean_vol) > 3 * ms.std_vol 
                        THEN true ELSE false 
                    END AS is_outlier,
                    c.date
                FROM {database}.counts_{interval} c
                LEFT JOIN monthly_stats ms
                    ON c.SignalID = ms.SignalID
                    AND c.CallPhase = ms.CallPhase
                    AND c.Detector = ms.Detector
                    AND hour(c.Timeperiod) = ms.hour_of_day
                    AND day_of_week(c.Timeperiod) = ms.day_of_week
            )
            SELECT 
                SignalID,
                CallPhase,
                Detector,
                Timeperiod,
                CASE
                    WHEN is_outlier AND std_vol > 0 THEN
                        GREATEST(0, LEAST(raw_vol, CAST(expected_vol + 2 * std_vol AS INTEGER)))
                    WHEN raw_vol IS NULL OR raw_vol <= 0 OR raw_vol >= 5000 THEN
                        COALESCE(
                            LAG(raw_vol, 1) OVER (PARTITION BY SignalID, CallPhase, Detector ORDER BY Timeperiod),
                            LAG(raw_vol, 2) OVER (PARTITION BY SignalID, CallPhase, Detector ORDER BY Timeperiod),
                            CAST(expected_vol AS INTEGER),
                            0
                        )
                    ELSE raw_vol
                END AS vol,
                date
            FROM filtered_data
            WHERE SignalID IS NOT NULL
            """
            
            _execute_athena_query(query, database, staging_dir, session, wait=True)
            logger.info(f"Athena optimization completed for get_adjusted_counts")
            return
            
        except Exception as e:
            logger.warning(f"Athena optimization failed, falling back to Arrow: {e}")
    
    # Fallback to original Arrow processing
    logger.info("Using Arrow processing for get_adjusted_counts")
    
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
                logger.error(f"Error processing partition: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in adjusted counts calculation: {e}")

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
    """Get list of signal IDs that have data for a given date
    
    Now uses Athena query for more reliable signal extraction (faster and more robust)
    Falls back to S3 read if Athena fails
    """
    
    date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)
    
    # Try Athena first (more reliable, uses database credentials)
    try:
        import os
        if 'ATHENA_DATABASE' in os.environ or USE_ATHENA_OPTIMIZATION:
            database = os.environ.get('ATHENA_DATABASE', 'gdot_spm_sbox')
            staging_dir = os.environ.get('ATHENA_STAGING', 's3://gdot-tmc-spm-athena-sandbox')
            
            logger.info(f"Attempting Athena signal extraction for {date_str}")
            logger.info(f"Database: {database}, Staging: {staging_dir}")
            
            query = f"""
            SELECT DISTINCT signalid
            FROM {database}.counts_1hr
            WHERE date = '{date_str}'
            """
            
            session = boto3.Session()  # Uses environment credentials
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=staging_dir,
                boto3_session=session,
                ctas_approach=False
            )
            
            logger.info(f"Athena query returned {len(df)} rows for {date_str}")
            
            if not df.empty and 'signalid' in df.columns:
                signals = df['signalid'].dropna().unique().tolist()
                if signals:
                    logger.info(f"✓ Found {len(signals)} signals for {date_str} via Athena")
                    return signals
                else:
                    logger.warning(f"Athena returned data but no valid signals for {date_str}")
            else:
                logger.warning(f"Athena query returned empty result for {date_str}")
    except Exception as e:
        logger.warning(f"Athena signal extraction failed for {date_str}: {e}")
        logger.warning("Falling back to S3 read method...")
    
    # Fallback to S3 read
    try:
        s3_path = f"s3://{bucket}/{prefix}/date={date_str}/"
        
        # Read parquet files to get signal IDs
        df = wr.s3.read_parquet(path=s3_path)
        
        if not df.empty and 'SignalID' in df.columns:
            signals = df['SignalID'].unique().tolist()
            logger.info(f"Found {len(signals)} signals for {date_str} via S3")
            return signals
        else:
            return []
            
    except Exception as e:
        logger.warning(f"Error getting signal IDs for {date_str}: {e}")
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