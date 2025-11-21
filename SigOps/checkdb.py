import os
import sys
import re
import glob
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import pyarrow.parquet as pq
import boto3
from sqlalchemy import create_engine, text
import logging
from typing import List, Optional
import shutil

# Add parent directory to Python path to access utilities.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import your existing utilities
from utilities import get_usable_cores, retry_on_failure, create_progress_tracker
from SigOps.database_functions import get_aurora_connection  # Assuming you have this function

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_database_tables(conf: dict, max_workers: Optional[int] = None):
    """
    Check database tables and generate summary data files
    
    Args:
        conf: Configuration dictionary containing database and S3 settings
        max_workers: Maximum number of worker threads
    """
    
    if max_workers is None:
        max_workers = get_usable_cores()
    
    # DateTime field options
    dt_fields = ["Month", "Date", "Hour", "Timeperiod"]
    
    # Get database connection
    conn = get_aurora_connection()
    
    try:
        # Get all tables
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE()
        """
        
        result = pd.read_sql(tables_query, conn)
        tables = result['table_name'].tolist()
        
        # Filter tables with regex pattern ^[a-z]{3}_.*
        tables = [table for table in tables if re.match(r'^[a-z]{3}_.*', table)]
        
        # Exclude patterns
        exclude_patterns = ['_qu_', '_udc', '_safety', '_maint', '_ops', 'summary_data']
        for pattern in exclude_patterns:
            tables = [table for table in tables if pattern not in table]
        
        logger.info(f"Found {len(tables)} tables to process")
        
        # Create/recreate tables directory
        if os.path.exists("tables"):
            shutil.rmtree("tables")
        os.makedirs("tables")
        
        # Process tables in parallel
        progress_tracker = create_progress_tracker(len(tables), "Processing tables")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_table = {
                executor.submit(process_single_table, table, dt_fields, conf): table 
                for table in tables
            }
            
            # Collect results
            completed = 0
            for future in as_completed(future_to_table):
                table = future_to_table[future]
                try:
                    future.result()
                    completed += 1
                    progress_tracker(completed)
                    logger.debug(f"Completed processing table: {table}")
                except Exception as e:
                    logger.error(f"Error processing table {table}: {e}")
        
        logger.info(f"All {len(tables)} tables processed")
        
        # Read all parquet files and combine
        sigops_data = combine_table_data(tables)
        
        # Generate output files
        generate_summary_files(sigops_data, conf)
        
    finally:
        pass
    #     conn.close()

@retry_on_failure(max_retries=3, delay=1.0)
def process_single_table(table: str, dt_fields: List[str], conf: dict):
    """
    Process a single table to extract date and record counts
    
    Args:
        table: Table name to process
        dt_fields: List of possible datetime field names
        conf: Configuration dictionary
    """
    
    output_file = f"tables/{table}.parquet"
    
    # Skip if file already exists
    if os.path.exists(output_file):
        logger.debug(f"Skipping {table} - file already exists")
        return
    
    conn = get_aurora_connection()
    
    try:
        # Get table fields
        fields_query = f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() 
        AND TABLE_NAME = '{table}'
        """
        
        fields_result = pd.read_sql(fields_query, conn)
        fields = fields_result['COLUMN_NAME'].tolist()
        
        # Find datetime field
        dt_field = None
        for field in dt_fields:
            if field in fields:
                dt_field = field
                break
        
        if dt_field is None:
            logger.warning(f"No datetime field found for table {table}")
            return
        
        # Calculate start date (60 days ago)
        start_date = datetime.now().date() - timedelta(days=60)
        
        # Query to get date and record counts
        query = f"""
        SELECT CAST({dt_field} AS DATE) AS Date, COUNT(*) as Records
        FROM {table} 
        WHERE {dt_field} > '{start_date}'
        GROUP BY CAST({dt_field} AS DATE)
        """
        
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            # Add table name and period columns
            df.insert(0, 'table', table)
            df = df.sort_values('Date')
            
            # Extract period from table name (pattern after first underscore)
            period_match = re.search(r'_([^_]+)', table)
            period = period_match.group(1) if period_match else ''
            df['period'] = period
            
            # Write to parquet
            df.to_parquet(output_file, index=False)
            logger.debug(f"Written {len(df)} records for table {table}")
        
    except Exception as e:
        logger.error(f"Error processing table {table}: {e}")
        raise
    finally:
        conn.close()

def combine_table_data(tables: List[str]) -> pd.DataFrame:
    """
    Combine all table parquet files into a single DataFrame
    
    Args:
        tables: List of table names
    
    Returns:
        Combined DataFrame with pivot structure
    """
    
    # Read all parquet files
    dfs = []
    for table in tables:
        parquet_file = f"tables/{table}.parquet"
        if os.path.exists(parquet_file):
            df = pd.read_parquet(parquet_file)
            dfs.append(df)
    
    if not dfs:
        logger.warning("No parquet files found to combine")
        return pd.DataFrame()
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = combined_df.sort_values('Date')
    
    # Pivot wider - dates as columns, records as values
    sigops_data = combined_df.pivot_table(
        index=['table', 'period'], 
        columns='Date', 
        values='Records', 
        fill_value=0,
        aggfunc='sum'
    ).reset_index()
    
    # Convert float columns to int
    date_columns = [col for col in sigops_data.columns if isinstance(col, pd.Timestamp)]
    for col in date_columns:
        sigops_data[col] = sigops_data[col].astype(int)
    
    return sigops_data

def get_wednesday_dates(sigops_data: pd.DataFrame) -> List[str]:
    """
    Get dates that fall on Wednesday (weekday 2 in pandas, 0=Monday)
    
    Args:
        sigops_data: DataFrame with date columns
    
    Returns:
        List of Wednesday date column names
    """
    
    date_columns = [col for col in sigops_data.columns 
                   if isinstance(col, pd.Timestamp)]
    
    wednesday_dates = [col.strftime('%Y-%m-%d') for col in date_columns 
                      if col.weekday() == 2]  # Wednesday = 2
    
    return wednesday_dates

def generate_summary_files(sigops_data: pd.DataFrame, conf: dict):
    """
    Generate summary CSV files and upload to S3
    
    Args:
        sigops_data: Combined DataFrame with all table data
        conf: Configuration dictionary
    """
    
    if sigops_data.empty:
        logger.warning("No data to generate summary files")
        return
    
    # Convert timestamp columns to string for easier manipulation
    date_columns = [col for col in sigops_data.columns if isinstance(col, pd.Timestamp)]
    for col in date_columns:
        new_col_name = col.strftime('%Y-%m-%d')
        sigops_data = sigops_data.rename(columns={col: new_col_name})
    
    # Get Wednesday dates
    all_date_cols = [col for col in sigops_data.columns 
                    if re.match(r'\d{4}-\d{2}-\d{2}', str(col))]
    
    wednesday_dates = []
    for date_str in all_date_cols:
        try:
            date_obj = pd.to_datetime(date_str)
            if date_obj.weekday() == 2:  # Wednesday
                wednesday_dates.append(date_str)
        except:
            continue
    
    # Generate weekly data (Wednesday dates, _wk tables)
    wk_columns = ['table', 'period'] + wednesday_dates
    available_wk_columns = [col for col in wk_columns if col in sigops_data.columns]
    
    sigops_wk = sigops_data[
        sigops_data['table'].str.contains('_wk', na=False)
    ][available_wk_columns]
    
    sigops_wk.to_csv('sigops_data_wk.csv', index=False)
    logger.info(f"Generated sigops_data_wk.csv with {len(sigops_wk)} records")
    
    # Generate monthly data (first of month dates, _mo_ tables)
    first_of_month_cols = [col for col in all_date_cols if col.endswith('-01')]
    mo_columns = ['table', 'period'] + first_of_month_cols
    available_mo_columns = [col for col in mo_columns if col in sigops_data.columns]
    
    sigops_mo = sigops_data[
        sigops_data['table'].str.contains('_mo_', na=False)
    ][available_mo_columns]
    
    sigops_mo.to_csv('sigops_data_mo.csv', index=False)
    logger.info(f"Generated sigops_data_mo.csv with {len(sigops_mo)} records")
    
    # Generate daily data (tables without _wk_ or _mo_)
    sigops_dy = sigops_data[
        ~sigops_data['table'].str.contains('(_wk_)|(_mo_)', na=False, regex=True)
    ]
    
    sigops_dy.to_csv('sigops_data_dy.csv', index=False)
    logger.info(f"Generated sigops_data_dy.csv with {len(sigops_dy)} records")
    
    # Upload to S3
    upload_files_to_s3(['sigops_data_wk.csv', 'sigops_data_mo.csv', 'sigops_data_dy.csv'], 
                      conf)

def upload_files_to_s3(file_list: List[str], conf: dict):
    """
    Upload files to S3
    
    Args:
        file_list: List of file paths to upload
        conf: Configuration dictionary containing S3 settings
    """
    
    s3_client = boto3.client('s3')
    bucket = conf.get('bucket')
    
    if not bucket:
        logger.error("No S3 bucket specified in configuration")
        return
    
    for file_path in file_list:
        if os.path.exists(file_path):
            try:
                s3_key = f"code/{file_path}"
                s3_client.upload_file(file_path, bucket, s3_key)
                logger.info(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
            except Exception as e:
                logger.error(f"Error uploading {file_path} to S3: {e}")
        else:
            logger.warning(f"File {file_path} not found for upload")
    
    logger.info("All files uploaded to S3")

def main():
    """
    Main function to run the database check process
    """
    
    # Load configuration with AWS credentials
    try:
        from SigOps.config_loader import load_merged_config
        conf = load_merged_config()
    except FileNotFoundError:
        logger.error("Configuration file not found")
        return
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return
    
    try:
        check_database_tables(conf)
        logger.info("Database check completed successfully")
    except Exception as e:
        logger.error(f"Error in database check: {e}")
        raise

if __name__ == "__main__":
    main()