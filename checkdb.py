"""
Database check script - Python conversion of checkdb.R
Checks database tables and generates summary files
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor, as_completed
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import re
import logging
from typing import List, Dict, Any, Optional
import shutil
from database_functions import get_aurora_connection, load_credentials
import boto3

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
conf = load_credentials()

def get_table_list(conn) -> List[str]:
    """
    Get list of tables matching pattern and exclude certain patterns
    Equivalent to R's dbListTables with filtering
    """
    try:
        # Get all table names
        query = "SHOW TABLES"
        result = pd.read_sql(query, conn)
        
        # Get the column name (it varies by database)
        table_col = result.columns[0]
        tables = result[table_col].tolist()
        
        # Filter tables that match pattern ^[a-z]{3}_.*
        tables = [t for t in tables if re.match(r'^[a-z]{3}_.*', t)]
        
        # Exclude patterns
        exclude_patterns = ['_qu_', '_udc', '_safety', '_maint', '_ops', 'summary_data']
        for pattern in exclude_patterns:
            tables = [t for t in tables if pattern not in t]
        
        logger.info(f"Found {len(tables)} tables to process")
        return tables
        
    except Exception as e:
        logger.error(f"Error getting table list: {e}")
        return []


def get_table_fields(conn, table_name: str) -> List[str]:
    """
    Get field names for a table
    Equivalent to R's dbListFields
    """
    try:
        query = f"DESCRIBE {table_name}"
        result = pd.read_sql(query, conn)
        return result['Field'].tolist()
    except Exception as e:
        logger.error(f"Error getting fields for table {table_name}: {e}")
        return []


def process_single_table(table_name: str) -> Optional[pd.DataFrame]:
    """
    Process a single table and return summary data
    This function will be run in parallel
    """
    try:
        # Create new connection for this process
        conn = get_aurora_connection()
        
        print(f"Processing {table_name}")
        logger.info(f"Processing table: {table_name}")
        
        # Get fields for this table
        fields = get_table_fields(conn, table_name)
        
        # Define datetime fields to check
        dt_fields = ["Month", "Date", "Hour", "Timeperiod"]
        
        # Find intersection of dt_fields and table fields
        dt_field = None
        for field in dt_fields:
            if field in fields:
                dt_field = field
                break
        
        if not dt_field:
            logger.warning(f"No datetime field found for table {table_name}")
            conn.close()
            return None
        
        # Calculate start date (60 days ago)
        start_date = date.today() - timedelta(days=60)
        
        # Check if parquet file already exists
        parquet_file = f"tables/{table_name}.parquet"
        if os.path.exists(parquet_file):
            logger.info(f"Parquet file already exists for {table_name}, skipping")
            conn.close()
            return None
        
        # Build and execute query
        query = f"""
        SELECT CAST({dt_field} AS DATE) AS Date, COUNT(*) as Records
        FROM {table_name} 
        WHERE {dt_field} > '{start_date}'
        GROUP BY CAST({dt_field} AS DATE)
        ORDER BY Date
        """
        
        try:
            result_df = pd.read_sql(query, conn)
            
            if not result_df.empty:
                # Add table name and extract period
                result_df.insert(0, 'table', table_name)
                
                # Extract period from table name (pattern after first underscore)
                period_match = re.search(r'_([^_]+)', table_name)
                period = period_match.group(1) if period_match else 'unknown'
                result_df['period'] = period
                
                # Convert Date column to datetime
                result_df['Date'] = pd.to_datetime(result_df['Date'])
                
                # Sort by Date
                result_df = result_df.sort_values('Date')
                
                # Save to parquet
                result_df.to_parquet(parquet_file, index=False)
                logger.info(f"Saved {len(result_df)} records for {table_name}")
                
                conn.close()
                return result_df
            else:
                logger.warning(f"No data found for table {table_name}")
                conn.close()
                return None
                
        except Exception as query_error:
            logger.error(f"Query error for table {table_name}: {query_error}")
            conn.close()
            return None
            
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        try:
            conn.close()
        except:
            pass
        return None


def get_weekday_dates(date_columns: List[str], target_weekday: int = 2) -> List[str]:
    """
    Filter dates to get only specific weekday (default Tuesday = 2)
    Equivalent to R's wday() == 3 (R uses 1=Sunday, Python uses 0=Monday)
    """
    weekday_dates = []
    for date_str in date_columns:
        try:
            date_obj = pd.to_datetime(date_str)
            if date_obj.weekday() == target_weekday:  # Tuesday
                weekday_dates.append(date_str)
        except:
            continue
    return weekday_dates


def get_month_end_dates(date_columns: List[str]) -> List[str]:
    """
    Filter dates to get only month-end dates (ending with -01)
    """
    return [col for col in date_columns if col.endswith('-01')]


def upload_to_s3(file_path: str, bucket: str, s3_key: str) -> bool:
    """
    Upload file to S3
    Equivalent to aws.s3::put_object in R
    """
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_file(file_path, bucket, s3_key)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        logger.error(f"Error uploading {file_path} to S3: {e}")
        return False


def main():
    """Main execution function"""
    
    # Get database connection
    conn = get_aurora_connection()
    
    # Get list of tables to process
    tables = get_table_list(conn)
    conn.close()
    
    if not tables:
        logger.error("No tables found to process")
        return
    
    # Create tables directory
    tables_dir = Path("tables")
    if tables_dir.exists():
        shutil.rmtree(tables_dir)
    tables_dir.mkdir()
    
    # Process tables in parallel
    logger.info(f"Processing {len(tables)} tables in parallel")
    
    # Use ProcessPoolExecutor for parallel processing
    # Equivalent to R's future.apply::future_lapply with plan(multisession)
    max_workers = min(len(tables), os.cpu_count() or 4)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_table = {executor.submit(process_single_table, table): table for table in tables}
        
        # Process completed tasks
        completed_tables = 0
        for future in as_completed(future_to_table):
            table = future_to_table[future]
            try:
                result = future.result()
                completed_tables += 1
                if completed_tables % 10 == 0:
                    logger.info(f"Completed {completed_tables}/{len(tables)} tables")
            except Exception as exc:
                logger.error(f"Table {table} generated an exception: {exc}")
    
    print(f"All {len(tables)} tables processed")
    
    # Read all parquet files and combine
    logger.info("Combining all parquet files")
    all_data = []
    
    for table in tables:
        parquet_file = f"tables/{table}.parquet"
        if os.path.exists(parquet_file):
            try:
                df = pd.read_parquet(parquet_file)
                all_data.append(df)
            except Exception as e:
                logger.error(f"Error reading {parquet_file}: {e}")
    
    if not all_data:
        logger.error("No data files found")
        return
    
    # Combine all dataframes
    sigops_data = pd.concat(all_data, ignore_index=True)
    sigops_data = sigops_data.sort_values('Date')
    
    # Pivot data to wide format
    # Equivalent to R's pivot_wider
    sigops_data_wide = sigops_data.pivot_table(
        index=['table', 'period'],
        columns='Date',
        values='Records',
        fill_value=0
    ).reset_index()
    
    # Convert Date columns to string format for column names
    sigops_data_wide.columns = [
        col.strftime('%Y-%m-%d') if isinstance(col, (pd.Timestamp, datetime)) 
        else str(col) for col in sigops_data_wide.columns
    ]
    
    # Convert float columns to int (equivalent to R's across(where(is.double), as.integer))
    for col in sigops_data_wide.columns:
        if col not in ['table', 'period'] and sigops_data_wide[col].dtype == 'float64':
            sigops_data_wide[col] = sigops_data_wide[col].astype('int32')
    
    # Get date columns (excluding table and period)
    date_columns = [col for col in sigops_data_wide.columns if col not in ['table', 'period']]
    
    # Get weekday dates (Tuesdays)
    wk_dates = get_weekday_dates(date_columns, target_weekday=2)  # Tuesday
    
    # Process weekly data
    logger.info("Processing weekly data")
    weekly_columns = ['table', 'period'] + wk_dates
    weekly_data = sigops_data_wide[
        sigops_data_wide['table'].str.contains('_wk')
    ][weekly_columns]
    
    weekly_data.to_csv('sigops_data_wk.csv', index=False)
    logger.info("Saved sigops_data_wk.csv")
    
    # Process monthly data (dates ending with -01)
    logger.info("Processing monthly data")
    month_end_dates = get_month_end_dates(date_columns)
    monthly_columns = ['table', 'period'] + month_end_dates
    monthly_data = sigops_data_wide[
        sigops_data_wide['table'].str.contains('_mo_')
    ][monthly_columns]
    
    monthly_data.to_csv('sigops_data_mo.csv', index=False)
    logger.info("Saved sigops_data_mo.csv")
    
    # Process daily data (everything else)
    logger.info("Processing daily data")
    daily_data = sigops_data_wide[
        ~sigops_data_wide['table'].str.contains('(_wk_)|(_mo_)')
    ]
    
    daily_data.to_csv('sigops_data_dy.csv', index=False)
    logger.info("Saved sigops_data_dy.csv")
    
    # Upload to S3
    logger.info("Uploading files to S3")
    if 'bucket' in conf:
        bucket = conf['bucket']
        
        upload_files = [
            ('sigops_data_wk.csv', f'code/sigops_data_wk.csv'),
            ('sigops_data_mo.csv', f'code/sigops_data_mo.csv'),
            ('sigops_data_dy.csv', f'code/sigops_data_dy.csv')
        ]
        
        upload_success = 0
        for local_file, s3_key in upload_files:
            if os.path.exists(local_file):
                if upload_to_s3(local_file, bucket, s3_key):
                    upload_success += 1
            else:
                logger.error(f"Local file {local_file} not found")
        
        logger.info(f"Successfully uploaded {upload_success}/{len(upload_files)} files to S3")
        print("All tables uploaded")
    else:
        logger.warning("No S3 bucket configured, skipping upload")
    
    logger.info("Processing complete")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
