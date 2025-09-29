# -*- coding: utf-8 -*-
"""
Enhanced parquet library with DuckDB support for faster S3 operations
"""

import duckdb
import boto3
from typing import List, Optional, Union, Dict, Any
import pandas as pd
import logging
import re
import os
from pathlib import Path

logger = logging.getLogger(__name__)

def read_s3_parquet_pattern_duckdb(bucket: str, 
                                  pattern: str,
                                  columns: Optional[List[str]] = None,
                                  filters: Optional[str] = None,
                                  limit: Optional[int] = None) -> pd.DataFrame:
    """
    Read multiple parquet files from S3 using DuckDB pattern matching
    Much faster than reading files individually
    
    Args:
        bucket: S3 bucket name
        pattern: File pattern (e.g., 'atspm/date=2024-01-*/atspm_*.parquet')
        columns: Optional list of columns to read
        filters: Optional SQL WHERE clause filters
        limit: Optional row limit
    
    Returns:
        Combined DataFrame
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        # Build column selection
        if columns:
            column_list = ", ".join(columns)
        else:
            column_list = "*"
        
        # Build WHERE clause
        where_clause = ""
        if filters:
            where_clause = f"WHERE {filters}"
        
        # Build LIMIT clause
        limit_clause = ""
        if limit:
            limit_clause = f"LIMIT {limit}"
        
        # Construct query
        query = f"""
        SELECT {column_list}
        FROM read_parquet('{s3_pattern}')
        {where_clause}
        {limit_clause}
        """
        
        result = conn.execute(query).df()
        conn.close()
        
        logger.info(f"Successfully read {len(result)} rows using DuckDB pattern matching")
        return result
        
    except Exception as e:
        logger.error(f"Error reading S3 parquet with DuckDB pattern: {e}")
        return pd.DataFrame()

def batch_read_atspm_duckdb(bucket: str, 
                           signal_ids: List[int],
                           date_range: List[str],
                           columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Batch read ATSPM data for multiple signals and dates using DuckDB
    
    Args:
        bucket: S3 bucket name
        signal_ids: List of signal IDs
        date_range: List of date strings
        columns: Optional specific columns to read
    
    Returns:
        Combined DataFrame with all data
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        # Build file list for all combinations
        file_patterns = []
        for date_str in date_range:
            for signal_id in signal_ids:
                pattern = f"s3://{bucket}/atspm/date={date_str}/atspm_{signal_id}_{date_str}.parquet"
                file_patterns.append(pattern)
        
        # Use list of files instead of pattern for better control
        file_list_str = "', '".join(file_patterns)
        
        # Build column selection
        if columns:
            column_list = ", ".join(columns)
        else:
            column_list = "*"
        
        # Add Date column from filename if not in columns
        if columns and 'Date' not in columns:
            query = f"""
            SELECT {column_list},
                   regexp_extract(filename, '(\d{{4}}-\d{{2}}-\d{{2}})', 1) as Date
            FROM read_parquet(['{file_list_str}'], filename=true)
            """
        else:
            query = f"""
            SELECT {column_list}
            FROM read_parquet(['{file_list_str}'])
            """
        
        result = conn.execute(query).df()
        conn.close()
        
        logger.info(f"Successfully batch read {len(result)} rows for {len(signal_ids)} signals across {len(date_range)} dates")
        return result
        
    except Exception as e:
        logger.error(f"Error in batch read with DuckDB: {e}")
        return pd.DataFrame()

def read_parquet_with_metadata_duckdb(bucket: str, 
                                     pattern: str) -> Dict[str, Any]:
    """
    Read parquet files and return both data and metadata using DuckDB
    
    Args:
        bucket: S3 bucket name
        pattern: File pattern
    
    Returns:
        Dictionary with 'data' and 'metadata' keys
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        # Read data
        data_query = f"SELECT * FROM read_parquet('{s3_pattern}')"
        data = conn.execute(data_query).df()
        
        # Read metadata
        try:
            metadata_query = f"SELECT * FROM read_parquet_metadata('{s3_pattern}')"
            metadata = conn.execute(metadata_query).df()
        except:
            metadata = pd.DataFrame()
        
        conn.close()
        
        result = {
            'data': data,
            'metadata': metadata,
            'file_count': len(metadata) if not metadata.empty else 0,
            'total_rows': len(data),
            'columns': list(data.columns) if not data.empty else []
        }
        
        logger.info(f"Successfully read data and metadata: {len(data)} rows from {result['file_count']} files")
        return result
        
    except Exception as e:
        logger.error(f"Error reading parquet with metadata: {e}")
        return {'data': pd.DataFrame(), 'metadata': pd.DataFrame(), 'file_count': 0, 'total_rows': 0, 'columns': []}

def create_partitioned_dataset_duckdb(source_bucket: str,
                                     source_pattern: str,
                                     target_bucket: str,
                                     target_prefix: str,
                                     partition_columns: List[str],
                                     file_size_mb: int = 100) -> bool:
    """
    Create partitioned dataset from existing parquet files using DuckDB
    
    Args:
        source_bucket: Source S3 bucket
        source_pattern: Source file pattern
        target_bucket: Target S3 bucket  
        target_prefix: Target prefix for partitioned data
        partition_columns: Columns to partition by
        file_size_mb: Target file size in MB
    
    Returns:
        Success status
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        source_s3_pattern = f"s3://{source_bucket}/{source_pattern}"
        target_s3_path = f"s3://{target_bucket}/{target_prefix}"
        
        # Calculate row limit per file based on target size
        # This is approximate - DuckDB will handle the actual partitioning
        partition_by_clause = ", ".join(partition_columns)
        
        # Create partitioned export
        export_query = f"""
        COPY (
            SELECT * FROM read_parquet('{source_s3_pattern}')
            ORDER BY {partition_by_clause}
        ) TO '{target_s3_path}' 
        (FORMAT PARQUET, PARTITION_BY ({partition_by_clause}), ROW_GROUP_SIZE {file_size_mb * 1024 * 1024 // 100})
        """
        
        conn.execute(export_query)
        conn.close()
        
        logger.info(f"Successfully created partitioned dataset at {target_s3_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating partitioned dataset: {e}")
        return False

def optimize_parquet_files_duckdb(bucket: str,
                                 input_pattern: str,
                                 output_prefix: str,
                                 compression: str = 'snappy',
                                 row_group_size: int = 100000) -> bool:
    """
    Optimize parquet files by rewriting with better compression and row group size
    
    Args:
        bucket: S3 bucket name
        input_pattern: Input file pattern
        output_prefix: Output prefix for optimized files
        compression: Compression algorithm ('snappy', 'gzip', 'lz4', 'zstd')
        row_group_size: Row group size for optimization
    
    Returns:
        Success status
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        input_s3_pattern = f"s3://{bucket}/{input_pattern}"
        output_s3_path = f"s3://{bucket}/{output_prefix}"
        
        # Optimize files
        optimize_query = f"""
        COPY (
            SELECT * FROM read_parquet('{input_s3_pattern}')
        ) TO '{output_s3_path}' 
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group_size})
        """
        
        conn.execute(optimize_query)
        conn.close()
        
        logger.info(f"Successfully optimized parquet files to {output_s3_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error optimizing parquet files: {e}")
        return False

def convert_csv_to_parquet_duckdb(bucket: str,
                                 csv_pattern: str,
                                 output_pattern: str,
                                 schema_hints: Optional[Dict[str, str]] = None) -> bool:
    """
    Convert CSV files to Parquet using DuckDB
    
    Args:
        bucket: S3 bucket name
        csv_pattern: CSV file pattern
        output_pattern: Output parquet pattern
        schema_hints: Optional column type hints
    
    Returns:
        Success status
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        csv_s3_pattern = f"s3://{bucket}/{csv_pattern}"
        parquet_s3_path = f"s3://{bucket}/{output_pattern}"
        
        # Apply schema hints if provided
        if schema_hints:
            cast_clauses = []
            for column, dtype in schema_hints.items():
                cast_clauses.append(f"CAST({column} AS {dtype}) as {column}")
            
            if cast_clauses:
                select_clause = ", ".join(cast_clauses)
                convert_query = f"""
                COPY (
                    SELECT {select_clause}
                    FROM read_csv_auto('{csv_s3_pattern}')
                ) TO '{parquet_s3_path}' (FORMAT PARQUET)
                """
            else:
                convert_query = f"""
                COPY (
                    SELECT * FROM read_csv_auto('{csv_s3_pattern}')
                ) TO '{parquet_s3_path}' (FORMAT PARQUET)
                """
        else:
            convert_query = f"""
            COPY (
                SELECT * FROM read_csv_auto('{csv_s3_pattern}')
            ) TO '{parquet_s3_path}' (FORMAT PARQUET)
            """
        
        conn.execute(convert_query)
        conn.close()
        
        logger.info(f"Successfully converted CSV to Parquet: {parquet_s3_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error converting CSV to Parquet: {e}")
        return False

def validate_parquet_integrity_duckdb(bucket: str, 
                                     pattern: str) -> Dict[str, Any]:
    """
    Validate parquet file integrity using DuckDB
    
    Args:
        bucket: S3 bucket name
        pattern: File pattern to validate
    
    Returns:
        Validation results
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        validation_results = {
            'valid_files': 0,
            'invalid_files': 0,
            'total_rows': 0,
            'errors': [],
            'file_details': []
        }
        
        try:
            # Try to read metadata first
            metadata_query = f"SELECT * FROM read_parquet_metadata('{s3_pattern}')"
            metadata = conn.execute(metadata_query).df()
            
            validation_results['valid_files'] = len(metadata)
            validation_results['file_details'] = metadata.to_dict('records')
            
            # Try to count total rows
            count_query = f"SELECT COUNT(*) as total_rows FROM read_parquet('{s3_pattern}')"
            row_count = conn.execute(count_query).fetchone()[0]
            validation_results['total_rows'] = row_count
            
        except Exception as e:
            validation_results['errors'].append(str(e))
            validation_results['invalid_files'] += 1
        
        conn.close()
        
        logger.info(f"Validation complete: {validation_results['valid_files']} valid files, {validation_results['invalid_files']} invalid files")
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating parquet integrity: {e}")
        return {'valid_files': 0, 'invalid_files': 0, 'total_rows': 0, 'errors': [str(e)], 'file_details': []}

# Enhanced versions of existing functions
def read_parquet_file(bucket: str, 
                     key: str, 
                     use_duckdb: bool = True,
                     columns: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Enhanced version of original read_parquet_file with DuckDB option
    
    Args:
        bucket: S3 bucket name
        key: Object key
        use_duckdb: Whether to use DuckDB for reading
        columns: Optional columns to read
    
    Returns:
        DataFrame
    """
    
    if use_duckdb:
        try:
            conn = duckdb.connect()
            conn.execute("SET s3_region='us-east-1';")
            
            s3_path = f"s3://{bucket}/{key}"
            
            if columns:
                column_list = ", ".join(columns)
                query = f"SELECT {column_list} FROM read_parquet('{s3_path}')"
            else:
                query = f"SELECT * FROM read_parquet('{s3_path}')"
            
            df = conn.execute(query).df()
            conn.close()
            
            # Extract date from filename if not present
            if 'Date' not in df.columns:
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
                if date_match:
                    df['Date'] = date_match.group(1)
            
            return df
            
        except Exception as e:
            logger.warning(f"DuckDB read failed, falling back to pandas: {e}")
    
    # Fallback to original implementation
    try:
        import boto3
        import io
        
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # Extract date from filename
        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', key)
        if date_match and 'Date' not in df.columns:
            df['Date'] = date_match.group(1)
        
        return df
        
    except Exception as e:
        logger.error(f"Error reading parquet file {key}: {e}")
        return pd.DataFrame()

def batch_process_parquet_files_duckdb(bucket: str,
                                      file_patterns: List[str],
                                      process_function: callable,
                                      output_bucket: str,
                                      output_prefix: str,
                                      **kwargs) -> bool:
    """
    Batch process multiple parquet files using DuckDB
    
    Args:
        bucket: Source bucket
        file_patterns: List of file patterns to process
        process_function: Function to apply to each batch
        output_bucket: Output bucket
        output_prefix: Output prefix
        **kwargs: Additional arguments for process function
    
    Returns:
        Success status
    """
    try:
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        processed_results = []
        
        for i, pattern in enumerate(file_patterns):
            try:
                s3_pattern = f"s3://{bucket}/{pattern}"
                
                # Read data using DuckDB
                df = conn.execute(f"SELECT * FROM read_parquet('{s3_pattern}')").df()
                
                if df.empty:
                    logger.warning(f"No data found for pattern: {pattern}")
                    continue
                
                # Apply processing function
                processed_df = process_function(df, **kwargs)
                
                if not processed_df.empty:
                    # Write to S3
                    output_path = f"s3://{output_bucket}/{output_prefix}/batch_{i:04d}.parquet"
                    
                    # Register processed DataFrame and export
                    conn.register('processed_data', processed_df)
                    conn.execute(f"COPY processed_data TO '{output_path}' (FORMAT PARQUET)")
                    
                    processed_results.append(f"batch_{i:04d}.parquet")
                    
                logger.info(f"Successfully processed pattern {i+1}/{len(file_patterns)}")
                
            except Exception as e:
                logger.error(f"Error processing pattern {pattern}: {e}")
                continue
        
        conn.close()
        
        logger.info(f"Batch processing complete: {len(processed_results)} files created")
        return len(processed_results) > 0
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        return False

def analyze_parquet_performance_duckdb(bucket: str,
                                      pattern: str,
                                      test_queries: List[str]) -> Dict[str, Any]:
    """
    Analyze parquet file performance using different query patterns
    
    Args:
        bucket: S3 bucket name
        pattern: File pattern
        test_queries: List of test queries to benchmark
    
    Returns:
        Performance analysis results
    """
    try:
        import time
        
        conn = duckdb.connect()
        conn.execute("SET s3_region='us-east-1';")
        
        s3_pattern = f"s3://{bucket}/{pattern}"
        
        # Create view for testing
        conn.execute(f"CREATE VIEW test_data AS SELECT * FROM read_parquet('{s3_pattern}')")
        
        performance_results = {
            'file_pattern': pattern,
            'query_results': [],
            'total_rows': 0,
            'file_size_mb': 0
        }
        
        # Get basic stats
        try:
            row_count = conn.execute("SELECT COUNT(*) FROM test_data").fetchone()[0]
            performance_results['total_rows'] = row_count
        except:
            pass
        
        # Test each query
        for i, query in enumerate(test_queries):
            try:
                start_time = time.time()
                result = conn.execute(query).df()
                end_time = time.time()
                
                query_result = {
                    'query_index': i,
                    'query': query,
                    'execution_time_seconds': end_time - start_time,
                    'result_rows': len(result),
                    'rows_per_second': len(result) / (end_time - start_time) if end_time > start_time else 0
                }
                
                performance_results['query_results'].append(query_result)
                
            except Exception as e:
                query_result = {
                    'query_index': i,
                    'query': query,
                    'error': str(e)
                }
                performance_results['query_results'].append(query_result)
        
        conn.close()
        
        logger.info(f"Performance analysis complete for {len(test_queries)} queries")
        return performance_results
        
    except Exception as e:
        logger.error(f"Error in performance analysis: {e}")
        return {}

# Keep original function for backward compatibility
def read_parquet(fn):
    """
    Original function maintained for backward compatibility
    """
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

def read_s3_parquet_pattern_duckdb_notinscope(bucket: str, 
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

def batch_read_atspm_duckdb_notinscope(bucket: str, 
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