# -*- coding: utf-8 -*-
"""
Enhanced parquet library with DuckDB support for faster S3 operations
"""

import duckdb
import boto3
import pandas as pd
import os
import re
from pathlib import Path
from typing import Union, List, Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

# Keep your existing function for backward compatibility
def read_parquet(fn):
    """Original function - kept for backward compatibility"""
    filename = os.path.basename(fn)
    date_ = re.search(r'\d{4}-\d{2}-\d{2}', fn).group(0)
    df = pd.read_parquet(filename).assign(Date=date_)
    return df

def read_parquet_file_duckdb(bucket: str, key: str, 
                            columns: Optional[List[str]] = None,
                            filter_condition: Optional[str] = None,
                            aws_access_key_id: Optional[str] = None,
                            aws_secret_access_key: Optional[str] = None,
                            region: str = 'us-east-1') -> pd.DataFrame:
    """
    Read parquet file from S3 using DuckDB - much faster than pandas
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        columns: Optional list of columns to select
        filter_condition: Optional SQL WHERE clause
        aws_access_key_id: AWS access key (optional)
        aws_secret_access_key: AWS secret key (optional)
        region: AWS region
    
    Returns:
        DataFrame with data
    """
    try:
        conn = duckdb.connect()
        
        # Configure S3 settings
        conn.execute(f"SET s3_region='{region}';")
        if aws_access_key_id and aws_secret_access_key:
            conn.execute(f"SET s3_access_key_id='{aws_access_key_id}';")
            conn.execute(f"SET s3_secret_access_key='{aws_secret_access_key}';")
        
        # Build S3 path
        s3_path = f"s3://{bucket}/{key}"
        
        # Build column selection
        col_select = "*" if columns is None else ", ".join(columns)
        
        # Build query
        query = f"SELECT {col_select} FROM read_parquet('{s3_path}')"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        # Execute and get DataFrame
        df = conn.execute(query).df()
        
        # Extract date from key if present
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', key)
        if date_match and 'Date' not in df.columns:
            df = df.assign(Date=date_match.group(0))
        
        conn.close()
        logger.info(f"Successfully read {len(df)} rows from s3://{bucket}/{key}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading S3 parquet file s3://{bucket}/{key}: {e}")
        return pd.DataFrame()

def read_local_parquet_duckdb(file_path: Union[str, Path],
                             columns: Optional[List[str]] = None,
                             filter_condition: Optional[str] = None) -> pd.DataFrame:
    """
    Read local parquet file using DuckDB
    
    Args:
        file_path: Path to local parquet file
        columns: Optional list of columns to select
        filter_condition: Optional SQL WHERE clause
    
    Returns:
        DataFrame with data
    """
    try:
        conn = duckdb.connect()
        
        file_path = str(file_path)
        col_select = "*" if columns is None else ", ".join(columns)
        
        query = f"SELECT {col_select} FROM read_parquet('{file_path}')"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = conn.execute(query).df()
        
        # Extract date from filename if present
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', file_path)
        if date_match and 'Date' not in df.columns:
            df = df.assign(Date=date_match.group(0))
        
        conn.close()
        logger.info(f"Successfully read {len(df)} rows from {file_path}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading local parquet file {file_path}: {e}")
        return pd.DataFrame()

def read_multiple_s3_parquet_duckdb(bucket: str, 
                                   keys: List[str],
                                   columns: Optional[List[str]] = None,
                                   filter_condition: Optional[str] = None) -> pd.DataFrame:
    """
    Read multiple S3 parquet files at once using DuckDB
    
    Args:
        bucket: S3 bucket name
        keys: List of S3 object keys
        columns: Optional list of columns to select
        filter_condition: Optional SQL WHERE clause
    
    Returns:
        Combined DataFrame
    """
    try:
        if not keys:
            return pd.DataFrame()
        
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        # Build S3 paths
        s3_paths = [f"s3://{bucket}/{key}" for key in keys]
        file_list = "', '".join(s3_paths)
        
        col_select = "*" if columns is None else ", ".join(columns)
        
        query = f"SELECT {col_select} FROM read_parquet(['{file_list}'])"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = conn.execute(query).df()
        conn.close()
        
        logger.info(f"Successfully read {len(df)} rows from {len(keys)} files")
        return df
        
    except Exception as e:
        logger.error(f"Error reading multiple S3 parquet files: {e}")
        return pd.DataFrame()

def read_s3_parquet_pattern_duckdb(bucket: str, 
                                  pattern: str,
                                  columns: Optional[List[str]] = None,
                                  filter_condition: Optional[str] = None) -> pd.DataFrame:
    """
    Read S3 parquet files using glob pattern
    
    Args:
        bucket: S3 bucket name
        pattern: Pattern like 'atspm/date=2024-*/atspm_*.parquet'
        columns: Optional list of columns to select
        filter_condition: Optional SQL WHERE clause
    
    Returns:
        Combined DataFrame
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        col_select = "*" if columns is None else ", ".join(columns)
        
        query = f"SELECT {col_select} FROM read_parquet('{s3_pattern}')"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        
        df = conn.execute(query).df()
        conn.close()
        
        logger.info(f"Successfully read {len(df)} rows using pattern {s3_pattern}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading S3 parquet pattern {pattern}: {e}")
        return pd.DataFrame()

def read_atspm_data_duckdb(bucket: str, signal_id: int, date_str: str,
                          columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Read ATSPM data for specific signal and date using DuckDB
    Optimized for your existing ATSPM file naming pattern
    
    Args:
        bucket: S3 bucket name
        signal_id: Signal ID
        date_str: Date string in YYYY-MM-DD format
        columns: Optional columns to select
    
    Returns:
        DataFrame with ATSPM data
    """
    key = f'atspm/date={date_str}/atspm_{signal_id}_{date_str}.parquet'
    return read_parquet_file_duckdb(bucket, key, columns=columns)

def batch_read_atspm_duckdb(bucket: str, 
                           signal_ids: List[int], 
                           date_range: List[str],
                           columns: Optional[List[str]] = None,
                           filter_condition: Optional[str] = None) -> pd.DataFrame:
    """
    Batch read ATSPM data for multiple signals and dates
    
    Args:
        bucket: S3 bucket name
        signal_ids: List of signal IDs
        date_range: List of date strings
        columns: Optional columns to select
        filter_condition: Optional SQL WHERE clause
    
    Returns:
        Combined DataFrame
    """
    try:
        # Build keys for all combinations
        keys = []
        for date_str in date_range:
            for signal_id in signal_ids:
                key = f'atspm/date={date_str}/atspm_{signal_id}_{date_str}.parquet'
                keys.append(key)
        
        return read_multiple_s3_parquet_duckdb(
            bucket=bucket,
            keys=keys,
            columns=columns,
            filter_condition=filter_condition
        )
        
    except Exception as e:
        logger.error(f"Error batch reading ATSPM data: {e}")
        return pd.DataFrame()

def export_to_parquet_s3_duckdb(df: pd.DataFrame, 
                               bucket: str, 
                               key: str,
                               partition_cols: Optional[List[str]] = None) -> bool:
    """
    Export DataFrame to S3 parquet using DuckDB
    
    Args:
        df: DataFrame to export
        bucket: S3 bucket name
        key: S3 object key
        partition_cols: Optional partition columns
    
    Returns:
        Success status
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        # Register DataFrame as temporary table
        conn.register('temp_df', df)
        
        s3_path = f"s3://{bucket}/{key}"
        
        if partition_cols:
            partition_str = ", ".join(partition_cols)
            query = f"COPY temp_df TO '{s3_path}' (FORMAT PARQUET, PARTITION_BY ({partition_str}))"
        else:
            query = f"COPY temp_df TO '{s3_path}' (FORMAT PARQUET)"
        
        conn.execute(query)
        conn.close()
        
        logger.info(f"Successfully exported {len(df)} rows to s3://{bucket}/{key}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting to S3 parquet: {e}")
        return False

# Enhanced wrapper function for existing code
def read_parquet_file(bucket: str, key: str, use_duckdb: bool = True, **kwargs) -> pd.DataFrame:
    """
    Enhanced wrapper that can use either DuckDB or pandas
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        use_duckdb: Whether to use DuckDB (default True)
        **kwargs: Additional arguments
    
    Returns:
        DataFrame
    """
    if use_duckdb:
        return read_parquet_file_duckdb(bucket, key, **kwargs)
    else:
        # Fallback to your original implementation
        try:
            import boto3
            s3 = boto3.client('s3')
            obj = s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_parquet(obj['Body'])
            
            date_match = re.search(r'\d{4}-\d{2}-\d{2}', key)
            if date_match:
                df = df.assign(Date=date_match.group(0))
            
            return df
        except Exception as e:
            logger.error(f"Error with pandas fallback: {e}")
            return pd.DataFrame()