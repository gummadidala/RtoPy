#!/usr/bin/env python3
"""
package_athena_helpers.py
Athena optimization functions for monthly_report_package_1.py

Provides cloud-native aggregation functions to replace local pandas operations.
Each function pushes processing to Athena, reducing data transfer and memory usage.
"""

import logging
import pandas as pd
import awswrangler as wr
import boto3
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date

logger = logging.getLogger(__name__)


def get_boto3_session(conf: dict) -> boto3.Session:
    """Create boto3 session with credentials from config"""
    return boto3.Session(
        aws_access_key_id=conf.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=conf.get('AWS_SECRET_ACCESS_KEY'),
        region_name=conf.get('AWS_DEFAULT_REGION', 'us-east-1')
    )


def athena_aggregate_by_time(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    time_granularity: str,
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict,
    additional_filters: Optional[str] = None
) -> pd.DataFrame:
    """
    Generic Athena aggregation function for time-based metrics
    
    Args:
        table_name: Athena table name (e.g., 'vehicles_pd')
        metric_col: Column to aggregate (e.g., 'vpd')
        weight_col: Column to use for weighted average (None for simple average)
        time_granularity: 'day', 'week', 'month', 'hour'
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        signals_list: List of signal IDs
        conf: Configuration dictionary
        additional_filters: Optional additional WHERE conditions
    
    Returns:
        DataFrame with aggregated results
    """
    try:
        database = conf['athena']['database']
        
        # Define time truncation based on granularity
        time_trunc_map = {
            'day': "DATE(date)",
            'week': "DATE_TRUNC('week', CAST(date AS TIMESTAMP))",
            'month': "DATE_TRUNC('month', CAST(date AS TIMESTAMP))",
            'hour': "DATE_TRUNC('hour', CAST(Timeperiod AS TIMESTAMP))"
        }
        
        time_col = time_trunc_map.get(time_granularity, "DATE(date)")
        
        # Build signal filter (batch if needed)
        if len(signals_list) > 1000:
            logger.warning(f"Large signal list ({len(signals_list)}), consider batching")
        
        # For VARCHAR comparison, we need quoted strings
        signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
        
        # Build aggregation
        if weight_col:
            agg_expr = f"""
                SUM({metric_col} * {weight_col}) / NULLIF(SUM({weight_col}), 0) AS {metric_col},
                SUM({weight_col}) AS {weight_col}
            """
        else:
            agg_expr = f"AVG({metric_col}) AS {metric_col}"
        
        # Build query
        where_clause = f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        if signals_list:
            where_clause += f" AND CAST(SignalID AS VARCHAR) IN ({signals_str})"
        if additional_filters:
            where_clause += f" AND {additional_filters}"
        
        query = f"""
        SELECT 
            SignalID,
            CallPhase,
            {time_col} AS Date,
            {agg_expr}
        FROM {database}.{table_name}
        {where_clause}
        GROUP BY SignalID, CallPhase, {time_col}
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena aggregation: {table_name} by {time_granularity}")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} aggregated rows (vs millions raw)")
        return df
        
    except Exception as e:
        logger.error(f"Athena aggregation failed: {e}")
        raise


def athena_corridor_aggregation(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    time_granularity: str,
    start_date: str,
    end_date: str,
    signals_list: List[str],
    corridors_df: pd.DataFrame,
    conf: dict,
    corridor_type: str = 'Corridor'
) -> pd.DataFrame:
    """
    Aggregate metrics by corridor using Athena JOIN
    
    This pushes the corridor JOIN to Athena instead of doing it locally,
    reducing data transfer significantly.
    
    Args:
        corridor_type: 'Corridor' or 'Subcorridor'
    
    Returns:
        DataFrame with corridor-level aggregations
    """
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Upload corridors mapping to S3 for Athena to use
        corridors_table = f"temp_corridors_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Select only needed columns
        corridor_cols = ['SignalID', 'Zone_Group', 'Zone', corridor_type]
        corridors_subset = corridors_df[corridor_cols].drop_duplicates()
        
        # Upload to S3 and create temp Athena table
        wr.s3.to_parquet(
            df=corridors_subset,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        # Now run Athena query with JOIN
        time_trunc_map = {
            'day': "DATE(m.date)",
            'week': "DATE_TRUNC('week', CAST(m.date AS TIMESTAMP))",
            'month': "DATE_TRUNC('month', CAST(m.date AS TIMESTAMP))",
            'hour': "DATE_TRUNC('hour', CAST(m.Timeperiod AS TIMESTAMP))"
        }
        
        time_col = time_trunc_map.get(time_granularity, "DATE(m.date)")
        
        # For VARCHAR comparison, we need quoted strings
        signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
        
        if weight_col:
            agg_expr = f"""
                SUM(m.{metric_col} * m.{weight_col}) / NULLIF(SUM(m.{weight_col}), 0) AS {metric_col},
                SUM(m.{weight_col}) AS {weight_col}
            """
        else:
            agg_expr = f"AVG(m.{metric_col}) AS {metric_col}"
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.{corridor_type} AS Corridor,
            {time_col} AS Date,
            {agg_expr}
        FROM {database}.{table_name} m
        INNER JOIN {database}.{corridors_table} c
            ON CAST(m.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE m.date BETWEEN '{start_date}' AND '{end_date}'
        AND CAST(m.SignalID AS VARCHAR) IN ({signals_str})
        GROUP BY c.Zone_Group, c.Zone, c.{corridor_type}, {time_col}
        ORDER BY Corridor, Date
        """
        
        logger.info(f"Athena corridor aggregation: {table_name} by {corridor_type}")
        logger.info("🔧 USING FIXED VERSION WITH CAST(SignalID AS VARCHAR) 🔧")
        logger.info(f"DEBUG SQL (first 600 chars): {query[:600]}")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(
                database=database,
                table=corridors_table,
                boto3_session=session
            )
            # Delete S3 data
            wr.s3.delete_objects(
                path=f"s3://{bucket}/temp/{corridors_table}/",
                boto3_session=session
            )
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} corridor aggregations")
        return df
        
    except Exception as e:
        logger.error(f"Athena corridor aggregation failed: {e}")
        raise


def athena_weekly_aggregation(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get weekly aggregations from Athena"""
    return athena_aggregate_by_time(
        table_name=table_name,
        metric_col=metric_col,
        weight_col=weight_col,
        time_granularity='week',
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf
    )


def athena_monthly_aggregation(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get monthly aggregations from Athena"""
    return athena_aggregate_by_time(
        table_name=table_name,
        metric_col=metric_col,
        weight_col=weight_col,
        time_granularity='month',
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf
    )


def athena_hourly_aggregation(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """Get hourly aggregations from Athena"""
    return athena_aggregate_by_time(
        table_name=table_name,
        metric_col=metric_col,
        weight_col=weight_col,
        time_granularity='hour',
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf
    )


def athena_peak_hour_aggregation(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict,
    peak_hours: List[int]
) -> pd.DataFrame:
    """
    Get aggregations for peak hours only
    
    Args:
        peak_hours: List of hours (0-23) considered peak
    """
    hours_str = ", ".join([str(h) for h in peak_hours])
    additional_filter = f"HOUR(CAST(Timeperiod AS TIMESTAMP)) IN ({hours_str})"
    
    return athena_aggregate_by_time(
        table_name=table_name,
        metric_col=metric_col,
        weight_col=weight_col,
        time_granularity='day',
        start_date=start_date,
        end_date=end_date,
        signals_list=signals_list,
        conf=conf,
        additional_filters=additional_filter
    )


# Feature flag for gradual rollout
USE_ATHENA_AGGREGATION = True


def get_weekly_metric_athena(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict,
    fallback_func=None
) -> pd.DataFrame:
    """
    Get weekly metric with Athena optimization and fallback
    
    This function tries Athena first, falls back to original function if Athena fails.
    Maintains backward compatibility.
    """
    if not USE_ATHENA_AGGREGATION or fallback_func is None:
        if fallback_func:
            return fallback_func()
        else:
            raise ValueError("No fallback function provided")
    
    try:
        logger.info(f"Using Athena for weekly {metric_col}")
        return athena_weekly_aggregation(
            table_name=table_name,
            metric_col=metric_col,
            weight_col=weight_col,
            start_date=start_date,
            end_date=end_date,
            signals_list=signals_list,
            conf=conf
        )
    except Exception as e:
        logger.warning(f"Athena weekly aggregation failed, using fallback: {e}")
        if fallback_func:
            return fallback_func()
        else:
            raise


def get_monthly_metric_athena(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict,
    fallback_func=None
) -> pd.DataFrame:
    """Get monthly metric with Athena optimization and fallback"""
    if not USE_ATHENA_AGGREGATION or fallback_func is None:
        if fallback_func:
            return fallback_func()
        else:
            raise ValueError("No fallback function provided")
    
    try:
        logger.info(f"Using Athena for monthly {metric_col}")
        return athena_monthly_aggregation(
            table_name=table_name,
            metric_col=metric_col,
            weight_col=weight_col,
            start_date=start_date,
            end_date=end_date,
            signals_list=signals_list,
            conf=conf
        )
    except Exception as e:
        logger.warning(f"Athena monthly aggregation failed, using fallback: {e}")
        if fallback_func:
            return fallback_func()
        else:
            raise


def athena_get_daily_detector_uptime(
    start_date: str,
    end_date: str,
    signals_list: List[str],
    conf: dict
) -> pd.DataFrame:
    """
    Get daily detector uptime aggregated in Athena
    
    Replaces local pandas groupby with Athena GROUP BY.
    Reduces data transfer from 100MB to ~5MB.
    """
    try:
        database = conf['athena']['database']
        # For VARCHAR comparison, we need quoted strings
        signals_str = ", ".join([f"'{str(s)}'" for s in signals_list])
        
        query = f"""
        SELECT 
            SignalID,
            CallPhase,
            Detector,
            DATE(date) AS Date,
            AVG(uptime) AS uptime,
            COUNT(*) AS num_readings
        FROM {database}.detector_uptime
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
        AND CAST(SignalID AS VARCHAR) IN ({signals_str})
        GROUP BY SignalID, CallPhase, Detector, DATE(date)
        ORDER BY SignalID, Date
        """
        
        logger.info(f"Athena: Getting daily detector uptime")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(df)} daily uptime records")
        return df
        
    except Exception as e:
        logger.error(f"Athena daily detector uptime failed: {e}")
        raise


def athena_get_corridor_weekly_avg(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    corridors_df: pd.DataFrame,
    conf: dict
) -> pd.DataFrame:
    """
    Get corridor-level weekly averages using Athena JOIN
    
    This is MUCH faster than:
    1. Pull all signal data
    2. Join with corridors locally
    3. Aggregate by corridor
    
    Instead:
    1. JOIN and aggregate in Athena
    2. Pull only corridor aggregates
    """
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Create temp corridors table
        corridors_table = f"temp_corridors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        corridor_mapping = corridors_df[['SignalID', 'Zone_Group', 'Zone', 'Corridor']].drop_duplicates()
        
        wr.s3.to_parquet(
            df=corridor_mapping,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        # Aggregate in Athena
        if weight_col:
            agg_expr = f"""
                SUM(m.{metric_col} * m.{weight_col}) / NULLIF(SUM(m.{weight_col}), 0) AS {metric_col},
                SUM(m.{weight_col}) AS {weight_col}
            """
        else:
            agg_expr = f"AVG(m.{metric_col}) AS {metric_col}"
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.Corridor,
            DATE_TRUNC('week', CAST(m.date AS TIMESTAMP)) AS Week,
            EXTRACT(DOW FROM CAST(m.date AS TIMESTAMP)) AS DOW,
            {agg_expr}
        FROM {database}.{table_name} m
        INNER JOIN {database}.{corridors_table} c
            ON CAST(m.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE m.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY c.Zone_Group, c.Zone, c.Corridor, DATE_TRUNC('week', CAST(m.date AS TIMESTAMP)), EXTRACT(DOW FROM CAST(m.date AS TIMESTAMP))
        ORDER BY Corridor, Week, DOW
        """
        
        logger.info(f"Athena: Corridor weekly aggregation for {metric_col}")
        logger.info("🔧 USING FIXED VERSION WITH CAST(SignalID AS VARCHAR) 🔧")
        logger.info(f"DEBUG SQL (first 600 chars): {query[:600]}")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(database=database, table=corridors_table, boto3_session=session)
            wr.s3.delete_objects(path=f"s3://{bucket}/temp/{corridors_table}/", boto3_session=session)
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} corridor weekly records")
        return df
        
    except Exception as e:
        logger.error(f"Athena corridor weekly aggregation failed: {e}")
        raise


def athena_get_corridor_monthly_avg(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    corridors_df: pd.DataFrame,
    conf: dict
) -> pd.DataFrame:
    """Get corridor-level monthly averages using Athena JOIN"""
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Create temp corridors table
        corridors_table = f"temp_corridors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        corridor_mapping = corridors_df[['SignalID', 'Zone_Group', 'Zone', 'Corridor']].drop_duplicates()
        
        wr.s3.to_parquet(
            df=corridor_mapping,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        # Aggregate in Athena
        if weight_col:
            agg_expr = f"""
                SUM(m.{metric_col} * m.{weight_col}) / NULLIF(SUM(m.{weight_col}), 0) AS {metric_col},
                SUM(m.{weight_col}) AS {weight_col}
            """
        else:
            agg_expr = f"AVG(m.{metric_col}) AS {metric_col}"
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.Corridor,
            DATE_TRUNC('month', CAST(m.date AS TIMESTAMP)) AS Month,
            EXTRACT(DOW FROM CAST(m.date AS TIMESTAMP)) AS DOW,
            {agg_expr}
        FROM {database}.{table_name} m
        INNER JOIN {database}.{corridors_table} c
            ON CAST(m.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE m.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY c.Zone_Group, c.Zone, c.Corridor, DATE_TRUNC('month', CAST(m.date AS TIMESTAMP)), EXTRACT(DOW FROM CAST(m.date AS TIMESTAMP))
        ORDER BY Corridor, Month, DOW
        """
        
        logger.info(f"Athena: Corridor monthly aggregation for {metric_col}")
        logger.info("🔧 USING FIXED VERSION WITH CAST(SignalID AS VARCHAR) 🔧")
        logger.info(f"DEBUG SQL (first 600 chars): {query[:600]}")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(database=database, table=corridors_table, boto3_session=session)
            wr.s3.delete_objects(path=f"s3://{bucket}/temp/{corridors_table}/", boto3_session=session)
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} corridor monthly records")
        return df
        
    except Exception as e:
        logger.error(f"Athena corridor monthly aggregation failed: {e}")
        raise


def athena_get_corridor_hourly_avg(
    table_name: str,
    metric_col: str,
    weight_col: Optional[str],
    start_date: str,
    end_date: str,
    corridors_df: pd.DataFrame,
    conf: dict,
    time_col: str = 'Timeperiod'
) -> pd.DataFrame:
    """
    Get corridor-level hourly averages using Athena JOIN
    
    For metrics that need hourly aggregation (AOG by hour, SF by hour, etc.)
    """
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Create temp corridors table
        corridors_table = f"temp_corridors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        corridor_mapping = corridors_df[['SignalID', 'Zone_Group', 'Zone', 'Corridor']].drop_duplicates()
        
        wr.s3.to_parquet(
            df=corridor_mapping,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        # Aggregate in Athena
        if weight_col:
            agg_expr = f"""
                SUM(m.{metric_col} * m.{weight_col}) / NULLIF(SUM(m.{weight_col}), 0) AS {metric_col},
                SUM(m.{weight_col}) AS {weight_col}
            """
        else:
            agg_expr = f"AVG(m.{metric_col}) AS {metric_col}"
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.Corridor,
            DATE_TRUNC('month', CAST(m.date AS TIMESTAMP)) AS Month,
            EXTRACT(HOUR FROM CAST(m.{time_col} AS TIMESTAMP)) AS Hour,
            {agg_expr}
        FROM {database}.{table_name} m
        INNER JOIN {database}.{corridors_table} c
            ON CAST(m.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE m.date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY c.Zone_Group, c.Zone, c.Corridor, DATE_TRUNC('month', CAST(m.date AS TIMESTAMP)), EXTRACT(HOUR FROM CAST(m.{time_col} AS TIMESTAMP))
        ORDER BY Corridor, Month, Hour
        """
        
        logger.info(f"Athena: Corridor hourly aggregation for {metric_col}")
        logger.info("🔧 USING FIXED VERSION WITH CAST(SignalID AS VARCHAR) 🔧")
        logger.info(f"DEBUG SQL (first 600 chars): {query[:600]}")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(database=database, table=corridors_table, boto3_session=session)
            wr.s3.delete_objects(path=f"s3://{bucket}/temp/{corridors_table}/", boto3_session=session)
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} corridor hourly records")
        return df
        
    except Exception as e:
        logger.error(f"Athena corridor hourly aggregation failed: {e}")
        raise


def athena_get_bad_detectors(
    start_date: str,
    end_date: str,
    corridors_df: pd.DataFrame,
    conf: dict
) -> pd.DataFrame:
    """
    Get bad vehicle detectors with corridor info using Athena JOIN
    
    Replaces local parquet read + pandas merge
    """
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Create temp corridors table
        corridors_table = f"temp_corridors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        corridor_mapping = corridors_df[['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Name']].drop_duplicates()
        
        wr.s3.to_parquet(
            df=corridor_mapping,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.Corridor,
            CAST(b.SignalID AS VARCHAR) AS SignalID,
            CAST(b.Detector AS VARCHAR) AS Detector,
            b.date AS Date,
            c.Name,
            'Bad Vehicle Detection' AS Alert
        FROM {database}.bad_detectors b
        INNER JOIN {database}.{corridors_table} c
            ON CAST(b.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE b.date BETWEEN '{start_date}' AND '{end_date}'
        AND b.good_day = 0
        ORDER BY Date DESC, SignalID, Detector
        """
        
        logger.info("Athena: Getting bad vehicle detectors with corridor info")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(database=database, table=corridors_table, boto3_session=session)
            wr.s3.delete_objects(path=f"s3://{bucket}/temp/{corridors_table}/", boto3_session=session)
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} bad detector records")
        return df
        
    except Exception as e:
        logger.error(f"Athena bad detectors query failed: {e}")
        raise


def athena_get_bad_ped_detectors(
    start_date: str,
    end_date: str,
    corridors_df: pd.DataFrame,
    conf: dict
) -> pd.DataFrame:
    """Get bad pedestrian detectors with corridor info using Athena JOIN"""
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        
        # Create temp corridors table
        corridors_table = f"temp_corridors_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        corridor_mapping = corridors_df[['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Name']].drop_duplicates()
        
        wr.s3.to_parquet(
            df=corridor_mapping,
            path=f"s3://{bucket}/temp/{corridors_table}/",
            dataset=True,
            database=database,
            table=corridors_table,
            boto3_session=get_boto3_session(conf)
        )
        
        query = f"""
        SELECT 
            c.Zone_Group,
            c.Zone,
            c.Corridor,
            CAST(b.SignalID AS VARCHAR) AS SignalID,
            CAST(b.Detector AS VARCHAR) AS Detector,
            b.date AS Date,
            c.Name,
            'Bad Ped Detection' AS Alert
        FROM {database}.bad_ped_detectors b
        INNER JOIN {database}.{corridors_table} c
            ON CAST(b.SignalID AS VARCHAR) = CAST(c.SignalID AS VARCHAR)
        WHERE b.date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY Date DESC, SignalID, Detector
        """
        
        logger.info("Athena: Getting bad ped detectors with corridor info")
        
        session = get_boto3_session(conf)
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        
        # Cleanup temp table
        try:
            wr.catalog.delete_table_if_exists(database=database, table=corridors_table, boto3_session=session)
            wr.s3.delete_objects(path=f"s3://{bucket}/temp/{corridors_table}/", boto3_session=session)
        except:
            pass
        
        logger.info(f"Retrieved {len(df)} bad ped detector records")
        return df
        
    except Exception as e:
        logger.error(f"Athena bad ped detectors query failed: {e}")
        raise


def batch_signals_for_athena(signals_list: List[str], batch_size: int = 500) -> List[List[str]]:
    """Split signals into batches to avoid Athena query limits"""
    batches = []
    for i in range(0, len(signals_list), batch_size):
        batches.append(signals_list[i:i + batch_size])
    return batches


def athena_aggregate_with_batching(
    query_func,
    signals_list: List[str],
    batch_size: int = 500,
    **kwargs
) -> pd.DataFrame:
    """
    Execute Athena aggregation with signal batching for large signal lists
    
    Args:
        query_func: Function that executes the Athena query
        signals_list: Full list of signals
        batch_size: Signals per batch
        **kwargs: Additional arguments to pass to query_func
    
    Returns:
        Combined DataFrame from all batches
    """
    try:
        batches = batch_signals_for_athena(signals_list, batch_size)
        all_results = []
        
        for i, batch in enumerate(batches):
            logger.info(f"Processing batch {i+1}/{len(batches)} ({len(batch)} signals)")
            
            result = query_func(signals_list=batch, **kwargs)
            
            if not result.empty:
                all_results.append(result)
        
        if all_results:
            combined = pd.concat(all_results, ignore_index=True)
            logger.info(f"Combined {len(batches)} batches into {len(combined)} rows")
            return combined
        else:
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"Batched Athena aggregation failed: {e}")
        raise

