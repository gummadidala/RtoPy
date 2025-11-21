#!/usr/bin/env python3
"""
Athena SQL Aggregation Functions
Performs aggregations in Athena SQL to minimize data transfer and memory usage

CRITICAL: All functions in this module aggregate data IN ATHENA CLOUD using SQL GROUP BY,
NOT by pulling raw data to local machine. This is the key to fast processing.
"""

import pandas as pd
import awswrangler as wr
import boto3
import logging
from typing import List, Optional, Dict, Any, Union
from datetime import date
from SigOps.athena_helpers import get_boto3_session, _detect_date_column, _detect_signalid_column

logger = logging.getLogger(__name__)


def _detect_uptime_column(database: str, table_name: str, conf: dict) -> str:
    """
    Detect the correct uptime column name in the table
    Tries common variations: uptime, Uptime, UPTIME, uptime_pct
    Uses multiple methods: catalog schema, information_schema, and test query
    """
    # Cache for uptime column names per table
    if not hasattr(_detect_uptime_column, '_cache'):
        _detect_uptime_column._cache = {}
    
    # Check cache first
    cache_key = f"{database}.{table_name}"
    if cache_key in _detect_uptime_column._cache:
        return _detect_uptime_column._cache[cache_key]
    
    session = get_boto3_session(conf)
    staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
    
    # Method 1: Try to get table schema from catalog
    try:
        table_info = wr.catalog.get_table(
            database=database,
            table=table_name,
            boto3_session=session
        )
        
        # Extract column names
        columns = []
        if isinstance(table_info, dict):
            if 'Columns' in table_info:
                columns = [col.get('Name', '') for col in table_info['Columns']]
            elif 'StorageDescriptor' in table_info and 'Columns' in table_info['StorageDescriptor']:
                columns = [col.get('Name', '') for col in table_info['StorageDescriptor']['Columns']]
        elif isinstance(table_info, pd.DataFrame):
            if 'Name' in table_info.columns:
                columns = table_info['Name'].tolist()
            elif 'Column Name' in table_info.columns:
                columns = table_info['Column Name'].tolist()
        
        # Look for uptime column (case-insensitive)
        uptime_candidates = ['uptime', 'Uptime', 'UPTIME', 'uptime_pct', 'UptimePct', 'Uptime_Pct']
        for candidate in uptime_candidates:
            if any(col.lower() == candidate.lower() for col in columns):
                # Found it - use the actual column name from the table
                actual_name = next((col for col in columns if col.lower() == candidate.lower()), candidate)
                _detect_uptime_column._cache[cache_key] = actual_name
                logger.info(f"Detected uptime column '{actual_name}' for {table_name} via catalog")
                return actual_name
        
        logger.debug(f"Table schema found but no uptime column detected. Available columns: {columns[:10]}")
    except Exception as e:
        logger.debug(f"Could not get table schema from catalog for {table_name}: {e}")
    
    # Method 2: Query information_schema to get column names
    try:
        info_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{database}' 
        AND table_name = '{table_name}'
        AND LOWER(column_name) LIKE '%uptime%'
        LIMIT 10
        """
        df = wr.athena.read_sql_query(
            sql=info_query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        if not df.empty and 'column_name' in df.columns:
            uptime_col = df.iloc[0]['column_name']
            _detect_uptime_column._cache[cache_key] = uptime_col
            logger.info(f"Detected uptime column '{uptime_col}' for {table_name} via information_schema")
            return uptime_col
    except Exception as e:
        logger.debug(f"Could not query information_schema for {table_name}: {e}")
    
    # Method 3: Try a test query with LIMIT 1 to see actual column names
    try:
        # Use a small date range to minimize query cost
        test_query = f"""
        SELECT * 
        FROM {database}.{table_name} 
        WHERE CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY
        LIMIT 1
        """
        df = wr.athena.read_sql_query(
            sql=test_query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        if not df.empty:
            # Log all columns for debugging
            logger.info(f"Test query returned columns for {table_name}: {list(df.columns)}")
            
            # Look for uptime-like columns in the actual data
            uptime_candidates = ['uptime', 'Uptime', 'UPTIME', 'uptime_pct', 'UptimePct', 'Uptime_Pct']
            for col in df.columns:
                if any(candidate.lower() == col.lower() for candidate in uptime_candidates):
                    _detect_uptime_column._cache[cache_key] = col
                    logger.info(f"Detected uptime column '{col}' for {table_name} via test query")
                    return col
            
            # If no uptime column found, check for any numeric columns that might be uptime
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            logger.warning(f"No uptime column found. Numeric columns available: {numeric_cols}")
    except Exception as e:
        logger.debug(f"Could not run test query for {table_name}: {e}")
        # Try without date filter as fallback
        try:
            test_query = f"SELECT * FROM {database}.{table_name} LIMIT 1"
            df = wr.athena.read_sql_query(
                sql=test_query,
                database=database,
                s3_output=staging_dir,
                boto3_session=session,
                ctas_approach=False
            )
            if not df.empty:
                logger.info(f"Test query (no filter) returned columns: {list(df.columns)}")
                uptime_candidates = ['uptime', 'Uptime', 'UPTIME', 'uptime_pct', 'UptimePct']
                for col in df.columns:
                    if any(candidate.lower() == col.lower() for candidate in uptime_candidates):
                        _detect_uptime_column._cache[cache_key] = col
                        logger.info(f"Detected uptime column '{col}' for {table_name}")
                        return col
        except Exception as e2:
            logger.debug(f"Could not run test query without filter: {e2}")
    
    # If all methods fail, return None to indicate we should fall back
    logger.error(f"Could not detect uptime column for {table_name} using any method")
    return None


def athena_aggregate_daily(
    table_name: str,
    metric_col: str,
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict,
    group_by_cols: Optional[List[str]] = None,
    weight_col: Optional[str] = None,
    additional_filters: Optional[str] = None,
    agg_type: str = "avg"  # "avg", "sum", "count"
) -> pd.DataFrame:
    """
    Aggregate data by day in Athena SQL - returns only aggregated results
    
    CRITICAL: This function performs GROUP BY in Athena SQL, NOT locally.
    Returns ~100K rows instead of millions of raw rows.
    
    Args:
        table_name: Athena table name
        metric_col: Column to aggregate
        start_date: Start date (date object or string)
        end_date: End date (date object or string)
        signals_list: List of SignalIDs
        conf: Configuration dictionary
        group_by_cols: Additional columns to group by (default: SignalID, CallPhase)
        weight_col: Weight column for weighted average
        additional_filters: Additional WHERE conditions
        agg_type: Aggregation type - "avg", "sum", or "count"
    
    Returns:
        DataFrame with daily aggregated results (much smaller than raw data)
    """
    try:
        database = conf.get('athena', {}).get('database')
        if not database:
            raise ValueError("Athena database not specified")
        
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle both date objects and strings
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Default group by columns
        if group_by_cols is None:
            group_by_cols = ['SignalID', 'CallPhase']
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Build aggregation expression
        if weight_col and weight_col in ['vol', 'all', 'num', 'vehicles', 'cycles', 'Events']:
            # Weighted average
            agg_expr = f"""
                SUM(CAST({metric_col} AS DOUBLE) * CAST({weight_col} AS DOUBLE)) / NULLIF(SUM(CAST({weight_col} AS DOUBLE)), 0) AS {metric_col},
                SUM(CAST({weight_col} AS DOUBLE)) AS {weight_col}
            """
        else:
            # Simple aggregation based on type
            if agg_type == "avg":
                agg_expr = f"AVG(CAST({metric_col} AS DOUBLE)) AS {metric_col}"
            elif agg_type == "sum":
                agg_expr = f"SUM(CAST({metric_col} AS DOUBLE)) AS {metric_col}"
            elif agg_type == "count":
                agg_expr = f"COUNT(*) AS {metric_col}"
            else:
                # Default to avg for metrics like uptime, aog, pr
                if metric_col in ['uptime', 'aog', 'pr', 'pd']:
                    agg_expr = f"AVG(CAST({metric_col} AS DOUBLE)) AS {metric_col}"
                else:
                    agg_expr = f"SUM(CAST({metric_col} AS DOUBLE)) AS {metric_col}"
        
        # Build GROUP BY clause
        group_by_clause = ", ".join([
            f"CAST({col.lower()} AS VARCHAR) AS {col}" if col.lower() != 'date' else f"DATE(date) AS Date"
            for col in group_by_cols
        ])
        group_by_clause += ", DATE(date) AS Date"
        
        # Build WHERE clause
        where_parts = [
            f"CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)"
        ]
        if signal_filter:
            where_parts.append(signal_filter.strip().replace("AND ", ""))
        if additional_filters:
            where_parts.append(additional_filters)
        
        where_clause = "WHERE " + " AND ".join(where_parts)
        
        # Build query - AGGREGATION HAPPENS IN ATHENA SQL
        query = f"""
        SELECT 
            {group_by_clause},
            {agg_expr}
        FROM {database}.{table_name}
        {where_clause}
        GROUP BY {", ".join([f"CAST({col.lower()} AS VARCHAR)" if col.lower() != 'date' else "DATE(date)" for col in group_by_cols])}, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD aggregation: {table_name} -> {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        # Normalize column names
        df = df.rename(columns={col.lower(): col for col in df.columns if col.lower() != col})
        
        logger.info(f"Retrieved {len(df)} daily aggregated records from Athena (vs millions of raw rows)")
        return df
        
    except Exception as e:
        logger.error(f"Error in Athena daily aggregation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def athena_get_detector_uptime_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """
    Get daily detector uptime aggregated in Athena CLOUD
    Returns only daily averages, not raw hourly data
    
    CRITICAL: Aggregation happens in Athena SQL with GROUP BY, NOT locally
    """
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle both date objects and strings
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            # Batch if too many signals - use smaller batches for better performance
            if len(signals_list) > 500:
                logger.warning(f"Large signal list ({len(signals_list)}), processing in batches of 500")
                all_results = []
                total_batches = (len(signals_list) + 499) // 500
                for i in range(0, len(signals_list), 500):
                    batch_num = (i // 500) + 1
                    batch = signals_list[i:i+500]
                    logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} signals)")
                    batch_df = athena_get_detector_uptime_daily(start_date, end_date, batch, conf)
                    if not batch_df.empty:
                        all_results.append(batch_df)
                        logger.info(f"Batch {batch_num} completed: {len(batch_df)} records")
                    else:
                        logger.warning(f"Batch {batch_num} returned no data")
                
                if all_results:
                    combined = pd.concat(all_results, ignore_index=True)
                    logger.info(f"Combined {len(all_results)} batches into {len(combined)} total records")
                    return combined
                return pd.DataFrame()
            
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Detect column names
        date_column = _detect_date_column(database, "detector_uptime", conf)
        signalid_column = _detect_signalid_column(database, "detector_uptime", conf)
        
        # CRITICAL: detector_uptime table uses Good_Day (0/1), not uptime directly
        # Uptime = average of Good_Day values (percentage of hours with Good_Day=1)
        # Try both Good_Day and good_day (case variations) - Athena is case-insensitive for column names
        query = f"""
        SELECT 
            CAST({signalid_column} AS VARCHAR) AS SignalID,
            CAST(callphase AS VARCHAR) AS CallPhase,
            DATE({date_column}) AS Date,
            AVG(CAST(COALESCE(Good_Day, good_day, 0) AS DOUBLE)) AS uptime,
            COUNT(*) AS all
        FROM {database}.detector_uptime
        WHERE CAST({date_column} AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        GROUP BY SignalID, CallPhase, DATE({date_column})
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD: Getting daily detector uptime for {len(signals_list)} signals (GROUP BY in SQL)")
        logger.debug(f"Query preview: SELECT ... FROM {database}.detector_uptime WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}' AND signalid IN (...{len(signals_list)} signals)")
        
        session = get_boto3_session(conf)
        
        # Use CTAS approach for large queries (faster for aggregations)
        # CTAS creates a temporary table which can be faster for large GROUP BY operations
        use_ctas = len(signals_list) > 100
        
        try:
            import time
            start_time = time.time()
            
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=staging_dir,
                boto3_session=session,
                ctas_approach=use_ctas
            )
            
            elapsed = time.time() - start_time
            logger.info(f"Athena query completed in {elapsed:.1f}s: Retrieved {len(df)} daily uptime records (aggregated in cloud)")
            return df
            
        except Exception as query_error:
            logger.error(f"Athena query failed: {query_error}")
            logger.error(f"Query: {query[:500]}...")
            raise
        
    except Exception as e:
        logger.error(f"Error getting detector uptime from Athena: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def athena_get_ped_activations_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """
    Get daily pedestrian activations aggregated in Athena CLOUD
    Returns only daily sums, not raw hourly data
    
    CRITICAL: Aggregation happens in Athena SQL with GROUP BY, NOT locally
    """
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle both date objects and strings
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            if len(signals_list) > 1000:
                # Batch processing
                all_results = []
                for i in range(0, len(signals_list), 1000):
                    batch = signals_list[i:i+1000]
                    batch_df = athena_get_ped_activations_daily(start_date, end_date, batch, conf)
                    if not batch_df.empty:
                        all_results.append(batch_df)
                
                if all_results:
                    return pd.concat(all_results, ignore_index=True)
                return pd.DataFrame()
            
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Aggregate by day in Athena SQL - AGGREGATION IN CLOUD
        query = f"""
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            CAST(detector AS VARCHAR) AS Detector,
            CAST(callphase AS VARCHAR) AS CallPhase,
            DATE(date) AS Date,
            SUM(CAST(vol AS DOUBLE)) AS papd
        FROM {database}.counts_ped_1hr
        WHERE CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        AND callphase IS NOT NULL
        GROUP BY SignalID, Detector, CallPhase, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD: Getting daily ped activations for {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} daily ped activation records from Athena (aggregated in cloud)")
        return df
        
    except Exception as e:
        logger.error(f"Error getting ped activations from Athena: {e}")
        return pd.DataFrame()


# ============================================================================
# ADDITIONAL ATHENA AGGREGATION FUNCTIONS FOR ALL METRICS
# All functions below aggregate in Athena SQL, NOT locally
# ============================================================================

def athena_get_comm_uptime_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily comm uptime aggregated in Athena CLOUD"""
    return athena_aggregate_daily(
        table_name="comm_uptime",
        metric_col="uptime",
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        agg_type="avg"
    )


def athena_get_ped_delay_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """
    Get daily ped delay aggregated in Athena CLOUD (weighted by Events)
    
    NOTE: ped_delay table uses 'eventparam' instead of 'callphase'
    """
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle dates
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # CRITICAL: ped_delay table uses 'eventparam' (not 'callphase') and 'duration' (not 'pd')
        # Also need to calculate Events count (each row is an event, so COUNT(*) = Events)
        query = f"""
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            CAST(eventparam AS VARCHAR) AS CallPhase,
            DATE(date) AS Date,
            AVG(CAST(duration AS DOUBLE)) AS pd,
            COUNT(*) AS Events
        FROM {database}.ped_delay
        WHERE CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        GROUP BY SignalID, CallPhase, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD: Getting daily ped delay for {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} daily ped delay records from Athena (aggregated in cloud)")
        return df
        
    except Exception as e:
        logger.error(f"Error getting ped delay from Athena: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def athena_get_vehicles_pd_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily vehicle volumes aggregated in Athena CLOUD"""
    return athena_aggregate_daily(
        table_name="vehicles_pd",
        metric_col="vpd",
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        agg_type="sum"
    )


def athena_get_throughput_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily throughput aggregated in Athena CLOUD"""
    return athena_aggregate_daily(
        table_name="throughput",
        metric_col="vph",
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        agg_type="sum"
    )


def athena_get_arrivals_on_green_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily arrivals on green aggregated in Athena CLOUD (weighted by vehicles)"""
    return athena_aggregate_daily(
        table_name="arrivals_on_green",
        metric_col="aog",
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        weight_col="vehicles",
        agg_type="avg"
    )


def athena_get_split_failures_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily split failures aggregated in Athena CLOUD"""
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle dates
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Aggregate in Athena - calculate sf_freq and sf_hours in SQL
        query = f"""
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            CAST(callphase AS VARCHAR) AS CallPhase,
            DATE(date) AS Date,
            SUM(CAST(sf_events AS DOUBLE)) AS sf_events,
            SUM(CAST(cycles AS DOUBLE)) AS cycles,
            (SUM(CAST(sf_events AS DOUBLE)) / NULLIF(SUM(CAST(cycles AS DOUBLE)), 0)) * 100 AS sf_freq,
            (SUM(CAST(sf_events AS DOUBLE)) / NULLIF(SUM(CAST(cycles AS DOUBLE)), 0)) * 24 AS sf_hours
        FROM {database}.split_failures
        WHERE CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        GROUP BY SignalID, CallPhase, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD: Getting daily split failures for {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} daily split failure records from Athena (aggregated in cloud)")
        return df
        
    except Exception as e:
        logger.error(f"Error getting split failures from Athena: {e}")
        return pd.DataFrame()


def athena_get_progression_ratio_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily progression ratio aggregated in Athena CLOUD (weighted by vehicles)"""
    return athena_aggregate_daily(
        table_name="progression_ratio",
        metric_col="pr",
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        weight_col="vehicles",
        agg_type="avg"
    )


def athena_get_queue_spillback_daily(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get daily queue spillback aggregated in Athena CLOUD"""
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle dates
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Aggregate in Athena - calculate qs_freq in SQL
        query = f"""
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            CAST(callphase AS VARCHAR) AS CallPhase,
            DATE(date) AS Date,
            SUM(CAST(qs_events AS DOUBLE)) AS qs_events,
            SUM(CAST(cycles AS DOUBLE)) AS cycles,
            (SUM(CAST(qs_events AS DOUBLE)) / NULLIF(SUM(CAST(cycles AS DOUBLE)), 0)) * 100 AS qs_freq
        FROM {database}.queue_spillback
        WHERE CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        GROUP BY SignalID, CallPhase, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena CLOUD: Getting daily queue spillback for {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} daily queue spillback records from Athena (aggregated in cloud)")
        return df
        
    except Exception as e:
        logger.error(f"Error getting queue spillback from Athena: {e}")
        return pd.DataFrame()


def athena_get_vehicles_ph_hourly(
    start_date: Union[date, str],
    end_date: Union[date, str],
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """
    Get hourly vehicle volumes aggregated in Athena CLOUD
    Returns hourly sums, not raw minute-level data
    
    CRITICAL: Aggregation happens in Athena SQL with GROUP BY, NOT locally
    """
    try:
        database = conf.get('athena', {}).get('database')
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Handle dates
        if isinstance(start_date, str):
            start_date_str = start_date
        else:
            start_date_str = start_date.strftime('%Y-%m-%d') if hasattr(start_date, 'strftime') else str(start_date)
        
        if isinstance(end_date, str):
            end_date_str = end_date
        else:
            end_date_str = end_date.strftime('%Y-%m-%d') if hasattr(end_date, 'strftime') else str(end_date)
        
        # Build signal filter
        if signals_list and len(signals_list) > 0:
            if len(signals_list) > 500:
                # Batch processing for large signal lists
                all_results = []
                for i in range(0, len(signals_list), 500):
                    batch = signals_list[i:i+500]
                    batch_df = athena_get_vehicles_ph_hourly(start_date, end_date, batch, conf)
                    if not batch_df.empty:
                        all_results.append(batch_df)
                
                if all_results:
                    return pd.concat(all_results, ignore_index=True)
                return pd.DataFrame()
            
            signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
            signal_filter = f"AND CAST(signalid AS VARCHAR) IN ({signals_str})"
        else:
            signal_filter = ""
        
        # Aggregate by hour in Athena SQL - AGGREGATION IN CLOUD
        query = f"""
        SELECT 
            CAST(signalid AS VARCHAR) AS SignalID,
            CAST(callphase AS VARCHAR) AS CallPhase,
            DATE(date) AS Date,
            DATE_TRUNC('hour', CAST(timeperiod AS TIMESTAMP)) AS Hour,
            SUM(CAST(vol AS DOUBLE)) AS vph
        FROM {database}.vehicles_ph
        WHERE CAST(date AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
        {signal_filter}
        AND callphase IN ('2', '6')  -- Mainline only
        GROUP BY SignalID, CallPhase, DATE(date), DATE_TRUNC('hour', CAST(timeperiod AS TIMESTAMP))
        ORDER BY SignalID, Date, Hour
        """
        
        logger.info(f"Athena CLOUD: Getting hourly vehicle volumes for {len(signals_list)} signals (GROUP BY in SQL)")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} hourly vehicle volume records from Athena (aggregated in cloud)")
        return df
        
    except Exception as e:
        logger.error(f"Error getting hourly vehicle volumes from Athena: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()
