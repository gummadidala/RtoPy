"""
Athena-based processing functions for monthly report calculations
Cloud-native approach - all processing happens in Athena
"""

import boto3
import awswrangler as wr
import pandas as pd
import logging
from typing import List, Dict, Optional
from datetime import datetime
from botocore.exceptions import WaiterError, ClientError

logger = logging.getLogger(__name__)


def get_boto3_session(conf: dict) -> boto3.Session:
    """
    Get configured boto3 session
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        Boto3 session
    """
    return boto3.Session(
        aws_access_key_id=conf.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=conf.get('AWS_SECRET_ACCESS_KEY'),
        region_name=conf['athena'].get('region', 'us-east-1')
    )


def execute_athena_query(query: str, conf: dict, wait: bool = True) -> Optional[str]:
    """
    Execute Athena query and optionally wait for completion
    
    Args:
        query: SQL query to execute
        conf: Configuration dictionary
        wait: Whether to wait for query completion
    
    Returns:
        Query execution ID
    """
    try:
        database = conf['athena']['database']
        session = get_boto3_session(conf)
        
        response = wr.athena.start_query_execution(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            wait=wait,
            boto3_session=session
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Error executing Athena query: {e}")
        logger.error(f"Query: {query[:200]}...")
        raise


def create_adjusted_counts_athena(date_range: List[str], conf: dict, interval: str = '1hr'):
    """
    Create adjusted counts table directly in Athena using SQL
    All processing happens in the cloud - no local data movement
    
    Args:
        date_range: List of date strings (YYYY-MM-DD)
        conf: Configuration dictionary
        interval: '1hr' or '15min'
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    table_suffix = '1hr' if interval == '1hr' else '15min'
    source_table = f"counts_{table_suffix}"
    target_table = f"adjusted_counts_{table_suffix}"
    
    # Create date filter for SQL IN clause
    date_list = "', '".join(date_range)
    
    logger.info(f"Creating adjusted counts in Athena for {len(date_range)} dates (interval: {interval})")
    
    # Drop and recreate table to ensure clean state
    drop_query = f"DROP TABLE IF EXISTS {database}.{target_table}"
    execute_athena_query(drop_query, conf, wait=True)
    
    # CTAS query that does ALL processing in Athena
    query = f"""
    CREATE TABLE {database}.{target_table}
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/adjusted_counts_{table_suffix}/',
        partitioned_by = ARRAY['date']
    ) AS
    SELECT 
        SignalID,
        CAST(CallPhase AS VARCHAR) AS CallPhase,
        CAST(Detector AS VARCHAR) AS Detector,
        Timeperiod,
        -- Apply adjustments using SQL window functions
        -- Forward fill missing values using LAG
        COALESCE(
            CASE WHEN vol >= 0 AND vol < 5000 THEN vol ELSE NULL END,
            LAG(vol, 1) OVER (PARTITION BY SignalID, CallPhase, Detector ORDER BY Timeperiod),
            LAG(vol, 2) OVER (PARTITION BY SignalID, CallPhase, Detector ORDER BY Timeperiod),
            0
        ) AS vol,
        date
    FROM {database}.{source_table}
    WHERE date IN ('{date_list}')
        AND SignalID IS NOT NULL
        AND CallPhase IS NOT NULL
        AND Detector IS NOT NULL
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        logger.info(f"Adjusted counts table created: {target_table}")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.{target_table}"
        execute_athena_query(repair_query, conf, wait=True)
        logger.info(f"Partitions repaired for {target_table}")
        
    except Exception as e:
        logger.error(f"Error creating adjusted counts in Athena: {e}")
        raise


def calculate_vpd_athena(date_range: List[str], conf: dict):
    """
    Calculate VPD (Vehicles Per Day) directly in Athena
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    date_list = "', '".join(date_range)
    
    logger.info(f"Calculating VPD in Athena for {len(date_range)} dates")
    
    # Drop and recreate
    drop_query = f"DROP TABLE IF EXISTS {database}.vehicles_pd"
    execute_athena_query(drop_query, conf, wait=True)
    
    query = f"""
    CREATE TABLE {database}.vehicles_pd
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/vpd/',
        partitioned_by = ARRAY['date']
    ) AS
    SELECT 
        SignalID,
        CallPhase,
        Detector,
        SUM(vol) as vpd,
        date
    FROM {database}.adjusted_counts_1hr
    WHERE date IN ('{date_list}')
    GROUP BY SignalID, CallPhase, Detector, date
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        logger.info("VPD calculation completed in Athena")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.vehicles_pd"
        execute_athena_query(repair_query, conf, wait=True)
        
    except Exception as e:
        logger.error(f"Error calculating VPD in Athena: {e}")
        raise


def calculate_vph_athena(date_range: List[str], conf: dict):
    """
    Calculate VPH (Vehicles Per Hour) directly in Athena
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    date_list = "', '".join(date_range)
    
    logger.info(f"Calculating VPH in Athena for {len(date_range)} dates")
    
    # Drop and recreate
    drop_query = f"DROP TABLE IF EXISTS {database}.vehicles_ph"
    execute_athena_query(drop_query, conf, wait=True)
    
    query = f"""
    CREATE TABLE {database}.vehicles_ph
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/vph/',
        partitioned_by = ARRAY['date']
    ) AS
    SELECT 
        SignalID,
        CallPhase,
        Detector,
        date_trunc('hour', Timeperiod) as Hour,
        SUM(vol) as vph,
        date
    FROM {database}.adjusted_counts_1hr
    WHERE date IN ('{date_list}')
    GROUP BY SignalID, CallPhase, Detector, date_trunc('hour', Timeperiod), date
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        logger.info("VPH calculation completed in Athena")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.vehicles_ph"
        execute_athena_query(repair_query, conf, wait=True)
        
    except Exception as e:
        logger.error(f"Error calculating VPH in Athena: {e}")
        raise


def calculate_vp15_athena(date_range: List[str], conf: dict):
    """
    Calculate VP15 (Vehicles Per 15 Minutes) directly in Athena
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    date_list = "', '".join(date_range)
    
    logger.info(f"Calculating VP15 in Athena for {len(date_range)} dates")
    
    # Drop and recreate
    drop_query = f"DROP TABLE IF EXISTS {database}.vehicles_15min"
    execute_athena_query(drop_query, conf, wait=True)
    
    query = f"""
    CREATE TABLE {database}.vehicles_15min
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/vp15/',
        partitioned_by = ARRAY['date']
    ) AS
    SELECT 
        SignalID,
        CallPhase,
        Detector,
        Timeperiod,
        vol as vp15,
        date
    FROM {database}.adjusted_counts_15min
    WHERE date IN ('{date_list}')
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        logger.info("VP15 calculation completed in Athena")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.vehicles_15min"
        execute_athena_query(repair_query, conf, wait=True)
        
    except Exception as e:
        logger.error(f"Error calculating VP15 in Athena: {e}")
        raise


def calculate_throughput_athena(date_range: List[str], conf: dict):
    """
    Calculate throughput directly in Athena
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    date_list = "', '".join(date_range)
    
    logger.info(f"Calculating throughput in Athena for {len(date_range)} dates")
    
    # Drop and recreate
    drop_query = f"DROP TABLE IF EXISTS {database}.throughput"
    execute_athena_query(drop_query, conf, wait=True)
    
    query = f"""
    CREATE TABLE {database}.throughput
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/tp/',
        partitioned_by = ARRAY['date']
    ) AS
    SELECT 
        SignalID,
        CallPhase,
        Detector,
        Timeperiod,
        vol,
        -- Calculate throughput: vehicles per 15-min converted to hourly rate
        CAST(vol * 4.0 AS DOUBLE) as throughput_hourly,
        date
    FROM {database}.adjusted_counts_15min
    WHERE date IN ('{date_list}')
        AND vol > 0
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        logger.info("Throughput calculation completed in Athena")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.throughput"
        execute_athena_query(repair_query, conf, wait=True)
        
    except Exception as e:
        logger.error(f"Error calculating throughput in Athena: {e}")
        raise


def get_signals_list_athena(date_range: List[str], conf: dict) -> List[str]:
    """
    Get list of unique signal IDs from Athena
    Small query, OK to download results
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    
    Returns:
        List of signal IDs
    """
    
    database = conf['athena']['database']
    date_list = "', '".join(date_range)
    staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
    
    query = f"""
    SELECT DISTINCT SignalID
    FROM {database}.adjusted_counts_1hr
    WHERE date IN ('{date_list}')
    ORDER BY SignalID
    """
    
    session = get_boto3_session(conf)
    
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            ctas_approach=False,
            s3_output=staging_dir,
            boto3_session=session
        )
        
        signals_list = df['SignalID'].tolist() if not df.empty else []
        logger.info(f"Retrieved {len(signals_list)} signals from Athena")
        
        return signals_list
        
    except (WaiterError, ClientError) as e:
        # Handle bucket existence check failures gracefully
        error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if '404' in str(e) or 'BucketExists' in str(e) or error_code == '404':
            logger.warning(f"Bucket check failed for signals list query (non-critical): {e}")
            logger.info("Attempting query without bucket verification...")
            try:
                # Try with explicit s3_output to bypass bucket check
                df = wr.athena.read_sql_query(
                    sql=query,
                    database=database,
                    ctas_approach=False,
                    s3_output=staging_dir,
                    boto3_session=session,
                    workgroup=conf['athena'].get('workgroup', 'primary')
                )
                signals_list = df['SignalID'].tolist() if not df.empty else []
                logger.info(f"Retrieved {len(signals_list)} signals from Athena (retry successful)")
                return signals_list
            except Exception as retry_e:
                logger.warning(f"Retry also failed, returning empty list: {retry_e}")
                return []
        else:
            logger.error(f"Error getting signals list from Athena: {e}")
            return []
    except Exception as e:
        logger.error(f"Error getting signals list from Athena: {e}")
        return []


def get_data_quality_stats_athena(date_range: List[str], conf: dict) -> pd.DataFrame:
    """
    Get data quality statistics from Athena
    
    Args:
        date_range: List of date strings
        conf: Configuration dictionary
    
    Returns:
        DataFrame with quality stats
    """
    
    database = conf['athena']['database']
    date_list = "', '".join(date_range)
    staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
    
    query = f"""
    SELECT 
        date,
        COUNT(DISTINCT SignalID) as signal_count,
        COUNT(*) as record_count,
        SUM(vol) as total_volume,
        AVG(vol) as avg_volume,
        MIN(vol) as min_volume,
        MAX(vol) as max_volume
    FROM {database}.adjusted_counts_1hr
    WHERE date IN ('{date_list}')
    GROUP BY date
    ORDER BY date
    """
    
    session = get_boto3_session(conf)
    
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            ctas_approach=False,
            s3_output=staging_dir,
            boto3_session=session
        )
        
        logger.info(f"Retrieved data quality stats for {len(df)} dates")
        return df
        
    except (WaiterError, ClientError) as e:
        # Handle bucket existence check failures gracefully
        error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if '404' in str(e) or 'BucketExists' in str(e) or error_code == '404':
            logger.warning(f"Bucket check failed for data quality stats query (non-critical): {e}")
            logger.info("Attempting query without bucket verification...")
            try:
                # Try with explicit s3_output to bypass bucket check
                df = wr.athena.read_sql_query(
                    sql=query,
                    database=database,
                    ctas_approach=False,
                    s3_output=staging_dir,
                    boto3_session=session,
                    workgroup=conf['athena'].get('workgroup', 'primary')
                )
                logger.info(f"Retrieved data quality stats for {len(df)} dates (retry successful)")
                return df
            except Exception as retry_e:
                logger.warning(f"Retry also failed, returning empty DataFrame: {retry_e}")
                return pd.DataFrame()
        else:
            logger.error(f"Error getting data quality stats from Athena: {e}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error getting data quality stats from Athena: {e}")
        return pd.DataFrame()


def verify_athena_tables_exist(conf: dict) -> Dict[str, bool]:
    """
    Verify that required Athena tables exist
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        Dictionary of table names and existence status
    """
    
    database = conf['athena']['database']
    staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
    required_tables = [
        'counts_1hr',
        'counts_15min',
        'adjusted_counts_1hr',
        'adjusted_counts_15min',
        'vehicles_pd',
        'vehicles_ph',
        'vehicles_15min',
        'throughput'
    ]
    
    table_status = {}
    
    for table in required_tables:
        query = f"SHOW TABLES IN {database} LIKE '{table}'"
        
        try:
            session = get_boto3_session(conf)
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                ctas_approach=False,
                s3_output=staging_dir,
                boto3_session=session
            )
            
            table_status[table] = not df.empty
            
        except (WaiterError, ClientError) as e:
            # Handle bucket existence check failures gracefully
            error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
            if '404' in str(e) or 'BucketExists' in str(e) or error_code == '404':
                logger.debug(f"Bucket check failed for table {table} (non-critical), skipping verification")
                table_status[table] = False  # Assume table doesn't exist if we can't verify
            else:
                logger.warning(f"Error checking table {table}: {e}")
                table_status[table] = False
        except Exception as e:
            logger.warning(f"Error checking table {table}: {e}")
            table_status[table] = False
    
    return table_status


def optimize_athena_table(table_name: str, conf: dict):
    """
    Optimize Athena table by compacting small files
    
    Args:
        table_name: Name of table to optimize
        conf: Configuration dictionary
    """
    
    database = conf['athena']['database']
    bucket = conf['bucket']
    
    logger.info(f"Optimizing Athena table: {table_name}")
    
    # Create optimized version using CTAS
    optimized_table = f"{table_name}_optimized"
    
    drop_query = f"DROP TABLE IF EXISTS {database}.{optimized_table}"
    execute_athena_query(drop_query, conf, wait=True)
    
    query = f"""
    CREATE TABLE {database}.{optimized_table}
    WITH (
        format = 'PARQUET',
        parquet_compression = 'SNAPPY',
        external_location = 's3://{bucket}/{table_name}_optimized/'
    ) AS
    SELECT * FROM {database}.{table_name}
    """
    
    try:
        execute_athena_query(query, conf, wait=True)
        
        # Swap tables
        drop_original = f"DROP TABLE {database}.{table_name}"
        execute_athena_query(drop_original, conf, wait=True)
        
        rename_query = f"ALTER TABLE {database}.{optimized_table} RENAME TO {table_name}"
        execute_athena_query(rename_query, conf, wait=True)
        
        logger.info(f"Table {table_name} optimized successfully")
        
    except Exception as e:
        logger.error(f"Error optimizing table {table_name}: {e}")
        raise

