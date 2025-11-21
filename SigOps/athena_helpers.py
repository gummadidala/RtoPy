#!/usr/bin/env python3
"""
Athena helper functions for SigOps package files
Replaces s3_read_parquet_parallel with Athena queries for better performance
"""

import pandas as pd
import awswrangler as wr
import boto3
import logging
from typing import List, Optional, Callable, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from botocore.exceptions import WaiterError, ClientError

logger = logging.getLogger(__name__)


def get_boto3_session(conf: dict) -> boto3.Session:
    """Create boto3 session with credentials from config"""
    return boto3.Session(
        aws_access_key_id=conf.get('AWS_ACCESS_KEY_ID') or conf.get('athena', {}).get('uid'),
        aws_secret_access_key=conf.get('AWS_SECRET_ACCESS_KEY') or conf.get('athena', {}).get('pwd'),
        region_name=conf.get('AWS_DEFAULT_REGION', conf.get('athena', {}).get('region', 'us-east-1'))
    )


def _get_available_date_range(database: str, table_name: str, date_column: str, conf: dict) -> Optional[tuple]:
    """
    Get the available date range in the table
    Returns (min_date, max_date) or None if can't determine
    """
    try:
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        session = get_boto3_session(conf)
        
        query = f"""
        SELECT 
            MIN(CAST({date_column} AS DATE)) as min_date,
            MAX(CAST({date_column} AS DATE)) as max_date
        FROM {database}.{table_name}
        WHERE CAST({date_column} AS DATE) IS NOT NULL
        """
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=database,
            s3_output=staging_dir,
            boto3_session=session,
            ctas_approach=False
        )
        
        if not df.empty and not pd.isna(df.iloc[0]['min_date']):
            min_date = df.iloc[0]['min_date']
            max_date = df.iloc[0]['max_date']
            if isinstance(min_date, str):
                min_date = pd.to_datetime(min_date).date()
            if isinstance(max_date, str):
                max_date = pd.to_datetime(max_date).date()
            return (min_date, max_date)
    except Exception as e:
        logger.debug(f"Could not get available date range for {table_name}: {e}")
    
    return None


def _detect_date_column(database: str, table_name: str, conf: dict) -> str:
    """
    Detect the correct date column name in the table
    Tries common variations: date, Date, Date_Hour, date_hour
    """
    # Cache for date column names per table
    if not hasattr(_detect_date_column, '_cache'):
        _detect_date_column._cache = {}
    
    # Check cache first
    cache_key = f"{database}.{table_name}"
    if cache_key in _detect_date_column._cache:
        return _detect_date_column._cache[cache_key]
    
    # Try to get table schema
    try:
        session = get_boto3_session(conf)
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
        
        # Look for date column (case-insensitive)
        date_column_candidates = ['date', 'Date', 'DATE', 'Date_Hour', 'date_hour', 'Date_Hour', 'DateHour']
        for candidate in date_column_candidates:
            if any(col.lower() == candidate.lower() for col in columns):
                # Found it - use the actual column name from the table
                actual_name = next((col for col in columns if col.lower() == candidate.lower()), candidate)
                _detect_date_column._cache[cache_key] = actual_name
                logger.debug(f"Detected date column '{actual_name}' for {table_name}")
                return actual_name
    except Exception as e:
        logger.debug(f"Could not detect date column for {table_name}: {e}")
    
    # Default to lowercase 'date' if detection fails
    default = 'date'
    _detect_date_column._cache[cache_key] = default
    return default


def _detect_signalid_column(database: str, table_name: str, conf: dict) -> str:
    """
    Detect the correct SignalID column name in the table
    Tries common variations: signalid, SignalID, signal_id
    """
    # Cache for SignalID column names per table
    if not hasattr(_detect_signalid_column, '_cache'):
        _detect_signalid_column._cache = {}
    
    # Check cache first
    cache_key = f"{database}.{table_name}"
    if cache_key in _detect_signalid_column._cache:
        return _detect_signalid_column._cache[cache_key]
    
    # Try to get table schema
    try:
        session = get_boto3_session(conf)
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
        
        # Look for SignalID column (case-insensitive)
        signalid_candidates = ['signalid', 'SignalID', 'signal_id', 'Signal_ID']
        for candidate in signalid_candidates:
            if any(col.lower() == candidate.lower() for col in columns):
                # Found it - use the actual column name from the table
                actual_name = next((col for col in columns if col.lower() == candidate.lower()), candidate)
                _detect_signalid_column._cache[cache_key] = actual_name
                logger.debug(f"Detected SignalID column '{actual_name}' for {table_name}")
                return actual_name
    except Exception as e:
        logger.debug(f"Could not detect SignalID column for {table_name}: {e}")
    
    # Default to lowercase 'signalid' (matching monthly_report_calcs_init.py)
    default = 'signalid'
    _detect_signalid_column._cache[cache_key] = default
    return default


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names from Athena (lowercase) to expected format (mixed case)
    This ensures compatibility with code that expects SignalID, Date, CallPhase, etc.
    """
    if df.empty:
        return df
    
    # Column name mapping: lowercase -> expected format
    column_mapping = {
        'signalid': 'SignalID',
        'signal_id': 'SignalID',
        'date': 'Date',
        'date_hour': 'Date_Hour',
        'callphase': 'CallPhase',
        'call_phase': 'CallPhase',
        'eventparam': 'CallPhase',  # ped_delay table uses eventparam as CallPhase
        'detector': 'Detector',
        'vol': 'vol',  # Keep lowercase as expected
        'vpd': 'vpd',  # Keep lowercase as expected
        'uptime': 'uptime',  # Keep lowercase as expected
        'pd': 'pd',  # Keep lowercase as expected
        'duration': 'duration',  # ped_delay table has duration column
        'events': 'Events',  # Some tables have Events column
        'all': 'all',  # Keep lowercase as expected
    }
    
    # Create rename dictionary for columns that exist
    rename_dict = {}
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            rename_dict[old_name] = new_name
    
    # Also handle case-insensitive matching
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in column_mapping and col not in rename_dict:
            rename_dict[col] = column_mapping[col_lower]
    
    if rename_dict:
        df = df.rename(columns=rename_dict)
        logger.debug(f"Normalized column names: {rename_dict}")
    
    return df


def athena_read_table(
    table_name: str,
    start_date: date,
    end_date: date,
    signals_list: Optional[List[str]] = None,
    conf: Optional[dict] = None,
    callback: Optional[Callable] = None,
    additional_filters: Optional[str] = None,
    columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Read data from Athena table (replacement for s3_read_parquet_parallel)
    
    Args:
        table_name: Athena table name (e.g., 'counts_ped_1hr', 'vehicles_ph')
        start_date: Start date for filtering
        end_date: End date for filtering
        signals_list: Optional list of SignalIDs to filter
        conf: Configuration dictionary with Athena settings
        callback: Optional callback function to apply to DataFrame
        additional_filters: Optional additional WHERE conditions
        columns: Optional list of columns to select (None = all columns)
    
    Returns:
        DataFrame with query results
    """
    try:
        if conf is None:
            logger.error("Configuration dictionary is required")
            return pd.DataFrame()
        
        database = conf.get('athena', {}).get('database')
        if not database:
            logger.error("Athena database not specified in configuration")
            return pd.DataFrame()
        
        staging_dir = conf.get('athena', {}).get('staging_dir', f"s3://{conf.get('bucket', 'default')}/athena-results/")
        
        # Convert dates to strings
        start_date_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, date) else str(start_date)
        end_date_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, date) else str(end_date)
        
        # Build column selection
        if columns:
            select_cols = ', '.join([f"`{col}`" for col in columns])
        else:
            select_cols = '*'
        
        # Build WHERE clause
        # Try to detect the correct date column name by querying table schema first
        # Common variations: 'date', 'Date', 'Date_Hour', 'date_hour'
        date_column = _detect_date_column(database, table_name, conf)
        
        # Detect the correct SignalID column name (signalid vs SignalID)
        # monthly_report_calcs_init.py uses lowercase 'signalid', but some tables may use 'SignalID'
        signalid_column = _detect_signalid_column(database, table_name, conf)
        
        # Use the exact date range as requested (matching R code behavior)
        # R code queries the full range and gets whatever data exists, skipping missing dates
        # We do the same - query full range, return empty if no data matches
        # Cast both the column and the comparison values to DATE to handle VARCHAR date columns
        where_parts = [f"CAST({date_column} AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)"]
        
        # Add signal filter if provided
        if signals_list and len(signals_list) > 0:
            # Handle large signal lists by batching - reduce batch size for memory-intensive tables
            # Some tables (like vehicles_ph, throughput) are very large, use smaller batches
            large_tables = ['vehicles_ph', 'throughput', 'vehicles_pd', 'detector_uptime']
            batch_threshold = 500 if table_name in large_tables else 1000
            batch_size = 300 if table_name in large_tables else 500
            
            if len(signals_list) > batch_threshold:
                logger.warning(f"Large signal list ({len(signals_list)}), using batching with batch size {batch_size}")
                return _athena_read_table_batched(
                    table_name, start_date, end_date, signals_list, conf, callback, additional_filters, columns, batch_size=batch_size
                )
            
            # Convert SignalIDs to strings properly - handle floats, integers, and strings
            # This ensures they match the format in Athena tables (which are strings)
            # CRITICAL: Remove .0 suffix from floats (2.0 -> '2', not '2.0')
            clean_signals = []
            for s in signals_list:
                if pd.isna(s):
                    continue
                # Convert to string first, then clean up
                if isinstance(s, float):
                    # If it's a whole number float (2.0), convert to int then string
                    if s.is_integer():
                        sig_str = str(int(s))
                    else:
                        # For non-integer floats, use as-is but remove trailing zeros
                        sig_str = str(s).rstrip('0').rstrip('.')
                elif isinstance(s, int):
                    sig_str = str(s)
                else:
                    # Already a string - remove .0 if present
                    sig_str = str(s).strip()
                    # Remove .0 suffix if it exists (handles "2.0" strings)
                    if sig_str.endswith('.0'):
                        sig_str = sig_str[:-2]
                clean_signals.append(sig_str)
            
            signals_str = ", ".join([f"'{s}'" for s in clean_signals])
            # Use detected SignalID column name
            # Try without CAST first (matching monthly_report_calcs_init.py which doesn't use CAST)
            # If that fails, we'll retry with CAST in the exception handler
            where_parts.append(f"{signalid_column} IN ({signals_str})")
        
        # Add additional filters if provided
        if additional_filters:
            where_parts.append(additional_filters)
        
        where_clause = " AND ".join(where_parts)
        
        # Build query
        query = f"""
        SELECT {select_cols}
        FROM {database}.{table_name}
        WHERE {where_clause}
        """
        
        # Log the actual query for debugging (truncate if too long, show key parts)
        # Extract just the WHERE clause for logging
        where_start = query.find("WHERE")
        if where_start > 0:
            where_clause_log = query[where_start:where_start+300] if len(query) > where_start+300 else query[where_start:]
            logger.info(f"Query WHERE clause: {where_clause_log}...")
        
        logger.info(f"Athena query: {table_name} from {start_date_str} to {end_date_str}")
        logger.info(f"Using date column: '{date_column}', SignalID column: '{signalid_column}'")
        if signals_list:
            logger.info(f"Filtering by {len(signals_list)} signals")
            # Log first few SignalIDs for debugging
            sample_signals = signals_list[:5] if len(signals_list) > 5 else signals_list
            logger.info(f"Sample SignalIDs: {sample_signals}")
        
        session = get_boto3_session(conf)
        
        # Execute query
        try:
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                s3_output=staging_dir,
                boto3_session=session,
                ctas_approach=False
            )
        except Exception as query_e:
            # Check for QueryFailed exceptions from awswrangler
            error_message = str(query_e)
            if 'TABLE_NOT_FOUND' in error_message or 'does not exist' in error_message:
                logger.error(f"Table {table_name} does not exist in Athena database. Stopping further attempts.")
                raise ValueError(f"Table {table_name} does not exist in Athena") from query_e
            
            # Check if it's a column not found error - try different date column names
            if 'Column' in error_message and 'not found' in error_message.lower():
                logger.warning(f"Date column '{date_column}' not found for {table_name}. Trying alternatives...")
                # Try alternative date column names
                for alt_date_col in ['Date', 'DATE', 'Date_Hour', 'date_hour']:
                    if alt_date_col.lower() != date_column.lower():
                        try:
                            alt_where = where_clause.replace(date_column, alt_date_col)
                            alt_query = f"""
                            SELECT {select_cols}
                            FROM {database}.{table_name}
                            WHERE {alt_where}
                            """
                            df = wr.athena.read_sql_query(
                                sql=alt_query,
                                database=database,
                                s3_output=staging_dir,
                                boto3_session=session,
                                ctas_approach=False
                            )
                            logger.info(f"Successfully queried {table_name} using date column '{alt_date_col}'")
                            # Update cache
                            cache_key = f"{database}.{table_name}"
                            if hasattr(_detect_date_column, '_cache'):
                                _detect_date_column._cache[cache_key] = alt_date_col
                            # Normalize column names
                            df = _normalize_column_names(df)
                            if callback:
                                df = callback(df)
                            logger.info(f"Retrieved {len(df)} rows from {table_name}")
                            return df
                        except Exception:
                            continue  # Try next alternative
            
            # Re-raise to be caught by outer handlers
            raise
        
        # Normalize column names to match expected format (signalid -> SignalID, date -> Date, etc.)
        df = _normalize_column_names(df)
        
        if df.empty:
            logger.warning(f"No data returned from {table_name}")
            logger.info(f"Query returned empty. Date range: {start_date_str} to {end_date_str}, SignalIDs: {len(signals_list) if signals_list else 0}")
            # Try a test query without SignalID filter to see if data exists
            if signals_list and len(signals_list) > 0:
                try:
                    logger.info(f"Testing if data exists in date range (without SignalID filter)...")
                    test_query = f"""
                    SELECT COUNT(*) as row_count
                    FROM {database}.{table_name}
                    WHERE CAST({date_column} AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
                    LIMIT 1
                    """
                    test_df = wr.athena.read_sql_query(
                        sql=test_query,
                        database=database,
                        s3_output=staging_dir,
                        boto3_session=session,
                        ctas_approach=False
                    )
                    if not test_df.empty:
                        row_count = test_df.iloc[0]['row_count']
                        if row_count > 0:
                            logger.info(f"✓ Data EXISTS in date range ({row_count} rows), but no matches for queried SignalIDs")
                            # Try to get sample SignalIDs from the table
                            try:
                                sample_query = f"""
                                SELECT DISTINCT {signalid_column} as signalid
                                FROM {database}.{table_name}
                                WHERE CAST({date_column} AS DATE) BETWEEN CAST('{start_date_str}' AS DATE) AND CAST('{end_date_str}' AS DATE)
                                LIMIT 10
                                """
                                sample_df = wr.athena.read_sql_query(
                                    sql=sample_query,
                                    database=database,
                                    s3_output=staging_dir,
                                    boto3_session=session,
                                    ctas_approach=False
                                )
                                if not sample_df.empty:
                                    sample_table_signals = sample_df['signalid'].tolist()
                                    logger.info(f"Sample SignalIDs in table: {sample_table_signals}")
                            except Exception as sample_e:
                                logger.debug(f"Could not get sample SignalIDs: {sample_e}")
                        else:
                            logger.info(f"✗ No data exists in date range {start_date_str} to {end_date_str}")
                    else:
                        logger.info(f"✗ No data exists in date range {start_date_str} to {end_date_str}")
                except Exception as test_e:
                    logger.warning(f"Could not run test query: {test_e}")
            return pd.DataFrame()
        
        # Apply callback if provided
        if callback:
            df = callback(df)
        
        logger.info(f"Retrieved {len(df)} rows from {table_name}")
        return df
        
    except ValueError as ve:
        # Re-raise ValueError (table not found) to stop batching
        raise
    except (WaiterError, ClientError) as e:
        error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if '404' in str(e) or 'BucketExists' in str(e) or error_code == '404':
            logger.warning(f"Bucket check failed for {table_name} (non-critical): {e}")
            logger.info("Attempting query without bucket verification...")
            try:
                session = get_boto3_session(conf)
                df = wr.athena.read_sql_query(
                    sql=query,
                    database=database,
                    s3_output=staging_dir,
                    boto3_session=session,
                    ctas_approach=False,
                    workgroup=conf.get('athena', {}).get('workgroup', 'primary')
                )
                if callback:
                    df = callback(df)
                return df
            except Exception as retry_e:
                logger.error(f"Retry also failed for {table_name}: {retry_e}")
                return pd.DataFrame()
        else:
            logger.error(f"Error reading from Athena table {table_name}: {e}")
            return pd.DataFrame()
    except Exception as e:
        error_message = str(e)
        
        # Check for TYPE_MISMATCH errors - handle date column type issues
        if 'TYPE_MISMATCH' in error_message and 'date' in error_message.lower() and 'BETWEEN' in error_message:
            logger.warning(f"Date column type mismatch for {table_name}. Attempting with CAST.")
            # Retry with CAST for date column and comparison values
            # The issue is that both the column and comparison values need to be cast to DATE
            try:
                # Extract date column name and comparison values from the WHERE clause
                # Pattern: "CAST({date_column} AS DATE) BETWEEN CAST('{start}' AS DATE) AND CAST('{end}' AS DATE)"
                # If the pattern doesn't match, try to fix it
                where_clause_modified = where_clause
                
                # Check if we already have the correct pattern
                if "CAST(" in where_clause and "AS DATE)" in where_clause and "BETWEEN" in where_clause:
                    # Already has CAST, but might need to cast the comparison values
                    import re
                    # Pattern: CAST(date AS DATE) BETWEEN '2025-08-19' AND '2025-11-19'
                    # Replace: CAST(date AS DATE) BETWEEN CAST('2025-08-19' AS DATE) AND CAST('2025-11-19' AS DATE)
                    pattern = r"BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'"
                    if re.search(pattern, where_clause_modified):
                        where_clause_modified = re.sub(
                            pattern,
                            r"BETWEEN CAST('\1' AS DATE) AND CAST('\2' AS DATE)",
                            where_clause_modified
                        )
                else:
                    # Try to add CAST to date column if missing
                    if "date BETWEEN" in where_clause:
                        where_clause_modified = where_clause.replace(
                            "date BETWEEN", 
                            "CAST(date AS DATE) BETWEEN"
                        )
                    elif "Date BETWEEN" in where_clause:
                        where_clause_modified = where_clause.replace(
                            "Date BETWEEN", 
                            "CAST(Date AS DATE) BETWEEN"
                        )
                    elif "DATE BETWEEN" in where_clause:
                        where_clause_modified = where_clause.replace(
                            "DATE BETWEEN", 
                            "CAST(DATE AS DATE) BETWEEN"
                        )
                    
                    # Also cast the comparison values
                    import re
                    pattern = r"BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'"
                    if re.search(pattern, where_clause_modified):
                        where_clause_modified = re.sub(
                            pattern,
                            r"BETWEEN CAST('\1' AS DATE) AND CAST('\2' AS DATE)",
                            where_clause_modified
                        )
                
                query_modified = f"""
                SELECT {select_cols}
                FROM {database}.{table_name}
                WHERE {where_clause_modified}
                """
                session = get_boto3_session(conf)
                df = wr.athena.read_sql_query(
                    sql=query_modified,
                    database=database,
                    s3_output=staging_dir,
                    boto3_session=session,
                    ctas_approach=False
                )
                if callback:
                    df = callback(df)
                logger.info(f"Retrieved {len(df)} rows from {table_name} (with CAST)")
                return df
            except Exception as cast_e:
                error_msg_cast = str(cast_e)
                # Check if CAST retry also has table not found
                if 'TABLE_NOT_FOUND' in error_msg_cast or 'does not exist' in error_msg_cast:
                    logger.error(f"Table {table_name} does not exist in Athena database.")
                    raise ValueError(f"Table {table_name} does not exist in Athena") from cast_e
                logger.error(f"CAST retry also failed for {table_name}: {cast_e}")
                return pd.DataFrame()
        
        # Check for TABLE_NOT_FOUND errors (fallback check, should already be handled above)
        if 'TABLE_NOT_FOUND' in error_message or 'does not exist' in error_message:
            logger.error(f"Table {table_name} does not exist in Athena database. Stopping further attempts.")
            raise ValueError(f"Table {table_name} does not exist in Athena") from e
        
        logger.error(f"Error reading from Athena table {table_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return pd.DataFrame()


def _athena_read_table_batched(
    table_name: str,
    start_date: date,
    end_date: date,
    signals_list: List[str],
    conf: dict,
    callback: Optional[Callable] = None,
    additional_filters: Optional[str] = None,
    columns: Optional[List[str]] = None,
    batch_size: int = 500
) -> pd.DataFrame:
    """Read table with batched signal lists to avoid query limits"""
    all_results = []
    table_not_found = False

    for i in range(0, len(signals_list), batch_size):
        batch = signals_list[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(signals_list) + batch_size - 1) // batch_size
        
        logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} signals)")
        
        try:
            batch_df = athena_read_table(
                table_name=table_name,
                start_date=start_date,
                end_date=end_date,
                signals_list=batch,
                conf=conf,
                callback=None,  # Apply callback after combining
                additional_filters=additional_filters,
                columns=columns
            )
            
            if not batch_df.empty:
                all_results.append(batch_df)
        except ValueError as ve:
            # Check if it's a table not found error
            if 'does not exist' in str(ve):
                logger.error(f"Table {table_name} does not exist. Stopping batch processing after batch {batch_num}.")
                table_not_found = True
                break  # Stop processing remaining batches
            else:
                raise  # Re-raise if it's a different ValueError
    
    if table_not_found:
        return pd.DataFrame()  # Return empty DataFrame if table doesn't exist
    
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        if callback:
            combined = callback(combined)
        logger.info(f"Combined {len(all_results)} batches into {len(combined)} rows")
        return combined
    else:
        return pd.DataFrame()




# Table name mapping from s3_read_parquet_parallel table_name to Athena table names
# NOTE: Some tables don't exist in Athena because R code:
# - Calculates them (progression_ratio from arrivals_on_green)
# - Reads from S3 directly (travel_times, safety_data)
# - Uses API calls (tasks from Teams API)
TABLE_NAME_MAPPING = {
    # ✅ Tables that exist in Athena
    'counts_ped_1hr': 'counts_ped_1hr',
    'counts_ped_15min': 'counts_ped_15min',
    'bad_ped_detectors': 'bad_ped_detectors',
    'vehicles_ph': 'vehicles_ph',
    'vehicles_15min': 'vehicles_15min',
    'vehicles_pd': 'vehicles_pd',
    'arrivals_on_green': 'arrivals_on_green',
    'arrivals_on_green_15min': 'arrivals_on_green_15min',  # ⚠️ This table does NOT exist in Athena - may need to create or use alternative
    'split_failures': 'split_failures',
    'split_failures_15min': 'split_failures_15min',
    'queue_spillback': 'queue_spillback',
    'queue_spillback_15min': 'queue_spillback_15min',
    'detector_uptime': 'detector_uptime',
    'detector_uptime_pd': 'detector_uptime',  # Daily detector uptime
    'ped_actuation_uptime': 'ped_actuation_uptime',
    'comm_uptime': 'comm_uptime',
    'cctv_uptime': 'cctv_uptime',  # Exists but accessed via get_daily_cctv_uptime() in R
    'pedestrian_delay': 'ped_delay',  # ✅ Map to existing table (pedestrian_delay doesn't exist, but ped_delay does)
    'ped_delay': 'ped_delay',  # ✅ R uses 'ped_delay' - this table EXISTS
    'throughput': 'throughput',
    'ramp_data': 'ramp_data',  # ⚠️ This table does NOT exist in Athena
    'ramp_meter_data': 'ramp_data',  # Alias
    'flash_events': 'flash_events',  # ⚠️ This table does NOT exist in Athena - used in R code
    'watchdog_alerts': 'watchdog_alerts',  # ⚠️ This table does NOT exist in Athena - R code reads from S3, writes to Aurora
    
    # Tables that may exist with different names or need special handling
    'cor_travel_time_metrics_1hr': 'cor_travel_time_metrics_1hr',  # R uses this from s3root='sigops'
    'sub_travel_time_metrics_1hr': 'sub_travel_time_metrics_1hr',  # R uses this from s3root='sigops'
    
    # ❌ Tables that DON'T exist in Athena (commented out - will cause TABLE_NOT_FOUND)
    # 'travel_times_1hr': 'travel_times_1hr',  # Use cor_travel_time_metrics_1hr instead
    # 'travel_times_15min': 'travel_times_15min',  # R uses different table structure
    # 'travel_times_1min': 'travel_times_1min',  # R uses different table structure
    # 'tasks': 'tasks',  # R uses Teams API (get_teams_tasks_from_s3())
    # 'safety_data': 'safety_data',  # R reads from S3: mark/bike_ped_safety_index/bpsi_sub_{date}.parquet
    # 'progression_ratio': 'progression_ratio',  # R calculates from arrivals_on_green - calculate in Python instead
}


def get_athena_table_name(s3_table_name: str) -> str:
    """Get Athena table name from S3 table name"""
    return TABLE_NAME_MAPPING.get(s3_table_name, s3_table_name)


def s3_read_parquet_parallel_athena(
    table_name: str,
    start_date: date,
    end_date: date,
    signals_list: Optional[List[str]] = None,
    bucket: Optional[str] = None,
    callback: Optional[Callable] = None,
    parallel: bool = False,
    s3root: str = 'mark',
    conf: Optional[dict] = None
) -> pd.DataFrame:
    """
    Replacement for s3_read_parquet_parallel using Athena queries ONLY
    
    This function maintains the same signature as s3_read_parquet_parallel
    but uses Athena queries exclusively. No fallback to S3.
    
    Args:
        table_name: Table name (will be mapped to Athena table name)
        start_date: Start date
        end_date: End date
        signals_list: Optional list of SignalIDs
        bucket: S3 bucket (ignored, kept for compatibility)
        callback: Optional callback function
        parallel: Ignored (Athena handles parallelism)
        s3root: Ignored (for compatibility)
        conf: Configuration dictionary (required for Athena)
    
    Returns:
        DataFrame with query results from Athena
    """
    # Get Athena table name
    athena_table_name = get_athena_table_name(table_name)
    
    # Ensure conf is available
    if conf is None:
        logger.error("Configuration dictionary is required for Athena queries")
        try:
            import yaml
            with open('Monthly_Report.yaml', 'r') as f:
                conf = yaml.safe_load(f)
            logger.info("Loaded configuration from Monthly_Report.yaml")
        except Exception as e:
            logger.error(f"Could not load configuration: {e}")
            raise ValueError("Athena configuration is required. Cannot proceed without valid configuration.")
    
    # Validate Athena configuration
    if not conf.get('athena') or not conf.get('athena', {}).get('database'):
        logger.error("Athena database not specified in configuration")
        raise ValueError("Athena database must be specified in configuration")
    
    # Clean SignalIDs before passing to athena_read_table
    # CRITICAL: Remove .0 suffix from floats (2.0 -> '2', not '2.0')
    clean_signals_list = []
    if signals_list:
        for s in signals_list:
            if pd.isna(s):
                continue
            # Convert to string and remove .0 suffix
            if isinstance(s, float):
                if s.is_integer():
                    sig_str = str(int(s))
                else:
                    sig_str = str(s).rstrip('0').rstrip('.')
            elif isinstance(s, int):
                sig_str = str(s)
            else:
                # Already a string - remove .0 if present
                sig_str = str(s).strip()
                if sig_str.endswith('.0'):
                    sig_str = sig_str[:-2]
            if sig_str:  # Only add non-empty strings
                clean_signals_list.append(sig_str)
    
    # Use Athena to read data
    df = athena_read_table(
        table_name=athena_table_name,
        start_date=start_date,
        end_date=end_date,
        signals_list=clean_signals_list if clean_signals_list else None,  # Use cleaned signals list
        conf=conf,
        callback=callback
    )
    
    return df

