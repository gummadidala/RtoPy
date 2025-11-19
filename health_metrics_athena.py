"""
Athena-optimized health metrics functions
Cloud-native processing to reduce memory usage and improve performance

This module provides Athena-based alternatives to the in-memory pandas operations
in health_metrics.py, particularly for the memory-intensive get_summary_data function.
"""

import pandas as pd
import numpy as np
import awswrangler as wr
import boto3
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)


def get_boto3_session(conf: dict) -> boto3.Session:
    """Create boto3 session with credentials from config"""
    return boto3.Session(
        aws_access_key_id=conf.get('AWS_ACCESS_KEY_ID') or conf.get('athena', {}).get('uid'),
        aws_secret_access_key=conf.get('AWS_SECRET_ACCESS_KEY') or conf.get('athena', {}).get('pwd'),
        region_name=conf.get('AWS_DEFAULT_REGION') or conf.get('athena', {}).get('region', 'us-east-1')
    )


def athena_get_summary_data(
    df: Dict[str, Dict[str, str]],  # Table names instead of DataFrames
    conf: dict,
    current_month: Optional[str] = None,
    data_type: str = 'sub'  # 'sub' or 'cor'
) -> pd.DataFrame:
    """
    Get summary data using Athena - processes all merges in the cloud
    
    This replaces the memory-intensive pandas merge operations with a single
    Athena query that does all the joins and aggregations server-side.
    
    Args:
        df: Dictionary containing table names (e.g., {'mo': {'du': 'detector_uptime'}})
        conf: Configuration dictionary with Athena settings
        current_month: Current month in date format (optional filter)
        data_type: 'sub' for subcorridor, 'cor' for corridor
    
    Returns:
        DataFrame with merged summary data
    """
    try:
        database = conf['athena']['database']
        staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
        
        # Map metric keys to table names
        table_mapping = {
            'du': 'detector_uptime',
            'pau': 'ped_actuation_uptime',
            'cctv': 'cctv_uptime',
            'cu': 'comm_uptime',
            'tp': 'throughput',
            'aogd': 'arrivals_on_green_daily',
            'prd': 'progression_ratio_daily',
            'qsd': 'queue_spillback_daily',
            'sfd': 'split_failures_daily',
            'pd': 'pedestrian_delay',
            'flash': 'flash_events',
            'tti': 'travel_time_index',
            'pti': 'planning_time_index',
            'kabco': 'kabco_index',
            'cri': 'crash_rate_index',
            'rsi': 'high_speed_index',
            'bpsi': 'ped_injury_exposure_index'
        }
        
        # Build UNION ALL query for all metrics
        # This approach uses UNION ALL to combine all metrics, then pivots them
        union_parts = []
        
        # Maintenance metrics
        for metric_key, table_name in [('du', 'detector_uptime'), ('pau', 'ped_actuation_uptime'), 
                                       ('cctv', 'cctv_uptime'), ('cu', 'comm_uptime')]:
            if metric_key in df.get('mo', {}):
                actual_table = df['mo'][metric_key] if isinstance(df['mo'][metric_key], str) else table_name
                union_parts.append(f"""
                    SELECT 
                        Zone_Group,
                        Corridor,
                        Month,
                        '{metric_key}' as metric_type,
                        uptime as metric_value,
                        delta as delta_value
                    FROM {database}.{actual_table}
                    WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
                """)
        
        # Operations metrics (delta only)
        for metric_key, table_name in [('tp', 'throughput'), ('aogd', 'arrivals_on_green_daily'),
                                       ('prd', 'progression_ratio_daily'), ('qsd', 'queue_spillback_daily'),
                                       ('sfd', 'split_failures_daily')]:
            if metric_key in df.get('mo', {}):
                actual_table = df['mo'][metric_key] if isinstance(df['mo'][metric_key], str) else table_name
                union_parts.append(f"""
                    SELECT 
                        Zone_Group,
                        Corridor,
                        Month,
                        '{metric_key}' as metric_type,
                        NULL as metric_value,
                        delta as delta_value
                    FROM {database}.{actual_table}
                    WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
                """)
        
        # Pedestrian delay (special handling)
        if 'pd' in df.get('mo', {}):
            actual_table = df['mo']['pd'] if isinstance(df['mo']['pd'], str) else 'pedestrian_delay'
            union_parts.append(f"""
                SELECT 
                    Zone_Group,
                    Corridor,
                    Month,
                    'pd' as metric_type,
                    COALESCE(pd, Avg.Max.Ped.Delay, duration) as metric_value,
                    delta as delta_value
                FROM {database}.{actual_table}
                WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
            """)
        
        # Flash events
        if 'flash' in df.get('mo', {}):
            actual_table = df['mo']['flash'] if isinstance(df['mo']['flash'], str) else 'flash_events'
            union_parts.append(f"""
                SELECT 
                    Zone_Group,
                    Corridor,
                    Month,
                    'flash' as metric_type,
                    flash_events as metric_value,
                    delta as delta_value
                FROM {database}.{actual_table}
                WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
            """)
        
        # Travel time metrics
        for metric_key, table_name in [('tti', 'travel_time_index'), ('pti', 'planning_time_index')]:
            if metric_key in df.get('mo', {}):
                actual_table = df['mo'][metric_key] if isinstance(df['mo'][metric_key], str) else table_name
                union_parts.append(f"""
                    SELECT 
                        Zone_Group,
                        Corridor,
                        Month,
                        '{metric_key}' as metric_type,
                        {metric_key} as metric_value,
                        delta as delta_value
                    FROM {database}.{actual_table}
                    WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
                """)
        
        # Safety metrics
        for metric_key, table_name in [('kabco', 'kabco_index'), ('cri', 'crash_rate_index'),
                                       ('rsi', 'high_speed_index'), ('bpsi', 'ped_injury_exposure_index')]:
            if metric_key in df.get('mo', {}):
                actual_table = df['mo'][metric_key] if isinstance(df['mo'][metric_key], str) else table_name
                union_parts.append(f"""
                    SELECT 
                        Zone_Group,
                        Corridor,
                        Month,
                        '{metric_key}' as metric_type,
                        {metric_key} as metric_value,
                        delta as delta_value
                    FROM {database}.{actual_table}
                    WHERE Zone_Group IS NOT NULL AND Corridor IS NOT NULL
                """)
        
        if not union_parts:
            logger.warning("No metrics found to aggregate")
            return pd.DataFrame()
        
        # Build main query with PIVOT to convert rows to columns
        # Note: Athena doesn't support PIVOT, so we'll use conditional aggregation
        union_query = " UNION ALL ".join(union_parts)
        
        # Build pivot query using CASE statements
        metric_cols = []
        delta_cols = []
        
        for metric_key in ['du', 'pau', 'cctv', 'cu', 'tp', 'aogd', 'prd', 'qsd', 'sfd', 'pd', 
                          'flash', 'tti', 'pti', 'kabco', 'cri', 'rsi', 'bpsi']:
            if metric_key in df.get('mo', {}):
                metric_cols.append(f"MAX(CASE WHEN metric_type = '{metric_key}' THEN metric_value END) as {metric_key}")
                delta_cols.append(f"MAX(CASE WHEN metric_type = '{metric_key}' THEN delta_value END) as {metric_key}_delta")
        
        pivot_query = f"""
        SELECT 
            Zone_Group,
            Corridor,
            Month,
            {', '.join(metric_cols)},
            {', '.join(delta_cols)}
        FROM (
            {union_query}
        ) AS metrics
        GROUP BY Zone_Group, Corridor, Month
        """
        
        # Add current month filter if specified
        if current_month:
            pivot_query += f" HAVING Month = DATE '{current_month}'"
        
        # Add filter to exclude rows where Zone_Group == Corridor
        pivot_query += " AND Zone_Group != Corridor"
        
        # Execute query
        logger.info("Executing Athena query for summary data aggregation")
        session = get_boto3_session(conf)
        
        result_df = wr.athena.read_sql_query(
            sql=pivot_query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=True  # Use CTAS for large result sets
        )
        
        if not result_df.empty:
            # Remove duplicates
            result_df = result_df.drop_duplicates()
            
            # Determine if this is corridor or subcorridor data
            if 'mttr' in df.get('mo', {}):  # corridor data
                result_df = result_df.sort_values(['Month', 'Zone_Group', 'Corridor'])
            else:  # subcorridor data
                result_df = result_df.rename(columns={'Corridor': 'Subcorridor', 'Zone_Group': 'Corridor'})
                result_df = result_df.sort_values(['Month', 'Corridor', 'Subcorridor'])
        
        logger.info(f"Retrieved {len(result_df)} summary rows from Athena (vs millions in local merge)")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in athena_get_summary_data: {e}")
        logger.warning("Falling back to local processing")
        return pd.DataFrame()


def athena_get_corridor_health_aggregation(
    summary_table: str,
    metric_cols: List[str],
    conf: dict,
    group_by: List[str] = None,
    filters: Optional[str] = None
) -> pd.DataFrame:
    """
    Aggregate health metrics by corridor using Athena
    
    Args:
        summary_table: Athena table name with summary data
        metric_cols: List of metric columns to aggregate
        conf: Configuration dictionary
        group_by: Columns to group by (default: ['Zone_Group', 'Corridor', 'Month'])
        filters: Optional WHERE clause filters
    
    Returns:
        Aggregated DataFrame
    """
    try:
        database = conf['athena']['database']
        staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
        
        if group_by is None:
            group_by = ['Zone_Group', 'Corridor', 'Month']
        
        group_by_str = ', '.join(group_by)
        agg_cols = ', '.join([f"AVG({col}) as {col}" for col in metric_cols])
        
        where_clause = f"WHERE Zone_Group != Corridor"
        if filters:
            where_clause += f" AND {filters}"
        
        query = f"""
        SELECT 
            {group_by_str},
            {agg_cols}
        FROM {database}.{summary_table}
        {where_clause}
        GROUP BY {group_by_str}
        ORDER BY {group_by_str}
        """
        
        logger.info(f"Aggregating {len(metric_cols)} metrics by {group_by_str} in Athena")
        session = get_boto3_session(conf)
        
        result_df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        logger.info(f"Retrieved {len(result_df)} aggregated rows from Athena")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in athena_get_corridor_health_aggregation: {e}")
        return pd.DataFrame()


def athena_merge_with_corridors(
    data_table: str,
    corridors_table: str,
    conf: dict,
    join_keys: List[str] = None,
    select_cols: List[str] = None
) -> pd.DataFrame:
    """
    Merge data with corridors table using Athena JOIN
    
    This pushes the merge operation to Athena instead of doing it locally,
    significantly reducing data transfer.
    
    Args:
        data_table: Source data table name
        corridors_table: Corridors mapping table name
        conf: Configuration dictionary
        join_keys: Columns to join on (default: ['SignalID'])
        select_cols: Columns to select (default: all)
    
    Returns:
        Merged DataFrame
    """
    try:
        database = conf['athena']['database']
        staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
        
        if join_keys is None:
            join_keys = ['SignalID']
        
        join_condition = ' AND '.join([f"d.{key} = c.{key}" for key in join_keys])
        
        if select_cols:
            select_str = ', '.join([f"d.{col} as {col}" for col in select_cols])
            select_str += ', ' + ', '.join([f"c.Zone_Group, c.Corridor, c.Subcorridor"])
        else:
            select_str = "d.*, c.Zone_Group, c.Corridor, c.Subcorridor"
        
        query = f"""
        SELECT 
            {select_str}
        FROM {database}.{data_table} d
        LEFT JOIN {database}.{corridors_table} c
        ON {join_condition}
        WHERE c.SignalID IS NOT NULL
        """
        
        logger.info(f"Merging {data_table} with {corridors_table} in Athena")
        session = get_boto3_session(conf)
        
        result_df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=True  # Use CTAS for large joins
        )
        
        logger.info(f"Retrieved {len(result_df)} merged rows from Athena")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in athena_merge_with_corridors: {e}")
        return pd.DataFrame()


def athena_calculate_health_scores(
    summary_table: str,
    scoring_lookup_table: str,
    conf: dict,
    metric_configs: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Calculate health scores using Athena window functions and CASE statements
    
    This replaces the local pandas lookup operations with SQL-based scoring.
    
    Args:
        summary_table: Table with metric values
        scoring_lookup_table: Table with scoring thresholds
        conf: Configuration dictionary
        metric_configs: List of metric configurations with lookup keys and directions
    
    Returns:
        DataFrame with calculated scores
    """
    try:
        database = conf['athena']['database']
        staging_dir = conf['athena'].get('staging_dir', f"s3://{conf['bucket']}/athena-results/")
        
        # Build CASE statements for each metric score
        score_cases = []
        for config in metric_configs:
            metric_col = config['metric_col']
            lookup_key = config['lookup_key']
            direction = config.get('direction', 'forward')
            
            if direction == 'backward':
                # Find last value <= metric value
                score_cases.append(f"""
                    (SELECT score 
                     FROM {database}.{scoring_lookup_table} 
                     WHERE metric = '{lookup_key}' 
                       AND value <= {metric_col}
                     ORDER BY value DESC 
                     LIMIT 1) as {metric_col}_Score
                """)
            else:  # forward
                # Find first value >= metric value
                score_cases.append(f"""
                    (SELECT score 
                     FROM {database}.{scoring_lookup_table} 
                     WHERE metric = '{lookup_key}' 
                       AND value >= {metric_col}
                     ORDER BY value ASC 
                     LIMIT 1) as {metric_col}_Score
                """)
        
        score_select = ', '.join(score_cases)
        
        query = f"""
        SELECT 
            Zone_Group,
            Corridor,
            Month,
            *,
            {score_select}
        FROM {database}.{summary_table}
        """
        
        logger.info("Calculating health scores in Athena")
        session = get_boto3_session(conf)
        
        result_df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=True
        )
        
        logger.info(f"Retrieved {len(result_df)} rows with calculated scores from Athena")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in athena_calculate_health_scores: {e}")
        return pd.DataFrame()


def create_health_summary_table_athena(
    date_range: List[str],
    conf: dict,
    output_table: str = 'health_summary'
) -> str:
    """
    Create a materialized health summary table in Athena
    
    This pre-aggregates all health metrics into a single table for faster queries.
    
    Args:
        date_range: List of date strings (YYYY-MM-DD)
        conf: Configuration dictionary
        output_table: Name for the output table
    
    Returns:
        Table name
    """
    try:
        database = conf['athena']['database']
        bucket = conf['bucket']
        date_list = "', '".join(date_range)
        
        logger.info(f"Creating health summary table in Athena for {len(date_range)} dates")
        
        # Drop existing table
        drop_query = f"DROP TABLE IF EXISTS {database}.{output_table}"
        session = get_boto3_session(conf)
        wr.athena.start_query_execution(
            sql=drop_query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            wait=True
        )
        
        # Create table with all metrics joined
        # This is a simplified version - in practice, you'd join all metric tables
        query = f"""
        CREATE TABLE {database}.{output_table}
        WITH (
            format = 'PARQUET',
            parquet_compression = 'SNAPPY',
            external_location = 's3://{bucket}/health_summary/',
            partitioned_by = ARRAY['Month']
        ) AS
        SELECT 
            Zone_Group,
            Corridor,
            Month,
            -- Add all metric columns here
            -- This is a template - customize based on your actual table structure
        FROM {database}.detector_uptime
        WHERE Month IN ('{date_list}')
        GROUP BY Zone_Group, Corridor, Month
        """
        
        # Note: This is a template. You'd need to customize based on your actual
        # table structure and join all metric tables together.
        
        wr.athena.start_query_execution(
            sql=query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            wait=True
        )
        
        logger.info(f"Health summary table created: {output_table}")
        
        # Repair partitions
        repair_query = f"MSCK REPAIR TABLE {database}.{output_table}"
        wr.athena.start_query_execution(
            sql=repair_query,
            database=database,
            s3_output=conf['athena']['staging_dir'],
            boto3_session=session,
            wait=True
        )
        
        return output_table
        
    except Exception as e:
        logger.error(f"Error creating health summary table: {e}")
        raise

