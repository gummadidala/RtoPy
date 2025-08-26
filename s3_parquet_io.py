#!/usr/bin/env python3

import pandas as pd
import boto3
import awswrangler as wr
from datetime import datetime, timedelta
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import os
# from utilities import keep_trying, convert_to_utc, get_objectkey
from utilities import keep_trying
from database_functions import add_athena_partition
from botocore.config import Config

class S3ParquetHandler:
    def __init__(self, aws_access_key=None, aws_secret_key=None, region='us-east-1'):
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
        boto_config = Config(
            retries = {
                'max_attempts': 10,
                'mode': 'standard'
            },
            max_pool_connections=500  # Increase this number as needed
        )
        self.s3_client = self.session.client('s3', config=boto_config)

def s3_upload_parquet(df, date_, filename, bucket, table_name, conf_athena):
    """Upload dataframe as parquet to S3"""
    
    # Remove grouping if present
    if hasattr(df, 'groupby'):
        df = df.reset_index(drop=True)
    
    # Remove Date column if present
    if 'Date' in df.columns:
        df = df.drop('Date', axis=1)
    
    # Convert specific columns to string
    string_cols = ['Detector', 'CallPhase', 'SignalID']
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    # Upload to S3
    s3_path = f"s3://{bucket}/mark/{table_name}/date={date_}/{filename}.parquet"
    
    def upload_func():
        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            boto3_session=boto3.Session(
                aws_access_key_id=conf_athena.get('uid'),
                aws_secret_access_key=conf_athena.get('pwd')
            )
        )
    
    keep_trying(upload_func, n_tries=5)
    add_athena_partition(conf_athena, bucket, table_name, date_)

def s3_upload_parquet_date_split(df, prefix, bucket, table_name, conf_athena, parallel=False):
    """Upload parquet files split by date"""
    
    if 'Date' not in df.columns:
        if 'Timeperiod' in df.columns:
            df['Date'] = pd.to_datetime(df['Timeperiod']).dt.date
        elif 'Hour' in df.columns:
            df['Date'] = pd.to_datetime(df['Hour']).dt.date
    
    unique_dates = df['Date'].unique()
    
    if len(unique_dates) == 1:
        # Single date upload
        date_ = unique_dates[0]
        filename = f"{prefix}_{date_}"
        s3_upload_parquet(df, date_, filename, bucket, table_name, conf_athena)
    else:
        # Multiple dates - process each
        for date_ in unique_dates:
            date_df = df[df['Date'] == date_].copy()
            filename = f"{prefix}_{date_}"
            s3_upload_parquet(date_df, date_, filename, bucket, table_name, conf_athena)

def s3_read_parquet(bucket, object_key, date_=None):
    """Read parquet file from S3"""
    
    if date_ is None:
        import re
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', object_key)
        if date_match:
            date_ = date_match.group()
    
    try:
        s3_path = f"s3://{bucket}/{object_key}"
        print(f"Reading parquet from S3: {s3_path}")
        df = wr.s3.read_parquet(path=s3_path)
        
        # Remove columns starting with '__'
        df = df.loc[:, ~df.columns.str.startswith('__')]
        
        # Add Date column
        if date_:
            df['Date'] = pd.to_datetime(date_).date()
        return convert_to_utc(df)
        
    except Exception as e:
        print(f"Error reading parquet: {e}")
        return pd.DataFrame()

def s3read_using(func, bucket, object, **kwargs):
    """
    Read data from S3 using specified function with error handling
    
    Args:
        func: Function to use for reading (e.g., pd.read_csv, pd.read_excel, pd.read_parquet)
        bucket: S3 bucket name
        object: S3 object key
        **kwargs: Additional arguments for the reading function
    
    Returns:
        DataFrame or data as returned by func
    """
    try:
        s3_client = boto3.client('s3')
        
        # Check if object exists first
        try:
            s3_client.head_object(Bucket=bucket, Key=object)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"S3 object not found: {object}")
                return pd.DataFrame()
            else:
                raise
        
        # Get the object
        response = s3_client.get_object(Bucket=bucket, Key=object)
        content = response['Body'].read()
        
        # Handle different file types
        if object.endswith('.csv'):
            # For CSV files, handle encoding issues
            encoding = kwargs.pop('encoding', 'utf-8')
            encoding_errors = kwargs.pop('encoding_errors', 'strict')
            
            # Try multiple encodings
            encoding_options = [encoding, 'utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for enc in encoding_options:
                try:
                    content_str = content.decode(enc, errors=encoding_errors)
                    from io import StringIO
                    data_io = StringIO(content_str)
                    return func(data_io, **kwargs)
                except UnicodeDecodeError:
                    if enc == encoding_options[-1]:
                        # Last option - use ignore errors
                        content_str = content.decode('utf-8', errors='ignore')
                        from io import StringIO
                        data_io = StringIO(content_str)
                        return func(data_io, **kwargs)
                    continue
            
        elif object.endswith('.parquet'):
            # For Parquet files, use BytesIO
            from io import BytesIO
            data_io = BytesIO(content)
            return func(data_io, **kwargs)
            
        elif object.endswith(('.xlsx', '.xls')):
            # For Excel files, use BytesIO
            from io import BytesIO
            data_io = BytesIO(content)
            return func(data_io, **kwargs)
            
        else:
            # For other file types, try BytesIO first, then StringIO
            try:
                from io import BytesIO
                data_io = BytesIO(content)
                return func(data_io, **kwargs)
            except:
                from io import StringIO
                content_str = content.decode('utf-8', errors='ignore')
                data_io = StringIO(content_str)
                return func(data_io, **kwargs)
            
    except Exception as e:
        return pd.DataFrame()  # Return empty DataFrame instead of raising

def s3_read_qs_subprocess_enhanced(bucket, object_key):
    """
    Enhanced cross-platform .qs reader with better error handling
    """
    import tempfile
    import os
    import subprocess
    import platform
    import shutil
    
    def find_r_executable():
        """Find R executable across different platforms"""
        possible_names = ['Rscript', 'Rscript.exe']
        
        for name in possible_names:
            if shutil.which(name):
                return name
        
        # Platform-specific paths
        if platform.system() == 'Windows':
            common_paths = [
                r"C:\Program Files\R\R-*\bin\x64\Rscript.exe",
                r"C:\Program Files\R\R-*\bin\Rscript.exe"
            ]
            import glob
            for pattern in common_paths:
                matches = glob.glob(pattern)
                if matches:
                    return matches[0]
        
        return None
    
    try:
        # Check if R is available
        r_executable = find_r_executable()
        if not r_executable:
            raise FileNotFoundError("R executable not found")
        
        s3_client = boto3.client('s3')
        
        # Create temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            qs_filepath = os.path.join(temp_dir, 'data.qs')
            csv_filepath = os.path.join(temp_dir, 'data.csv')
            script_filepath = os.path.join(temp_dir, 'convert.R')
            
            # Download .qs file from S3
            s3_client.download_file(bucket, object_key, qs_filepath)
            
            # Use forward slashes for R (works on all platforms)
            qs_path_r = qs_filepath.replace('\\', '/')
            csv_path_r = csv_filepath.replace('\\', '/')
            
            # Create R script
            r_script = f'''
# Check if qs package is installed
if (!require("qs", quietly = TRUE)) {{
    stop("Package 'qs' is not installed. Please install it with: install.packages('qs')")
}}

tryCatch({{
    # Read .qs file
    cat("Reading .qs file...\\n")
    data <- qs::qread("{qs_path_r}")
    
    # Write to CSV
    cat("Writing to CSV...\\n")
    write.csv(data, "{csv_path_r}", row.names = FALSE)
    cat("Conversion completed successfully\\n")
}}, error = function(e) {{
    cat("Error in R script:", conditionMessage(e), "\\n")
    quit(status = 1)
}})
'''
            
            # Write R script
            with open(script_filepath, 'w', encoding='utf-8') as f:
                f.write(r_script)
            
            # Run R script
            env = os.environ.copy()
            result = subprocess.run(
                [r_executable, script_filepath], 
                capture_output=True, 
                text=True,
                timeout=300,
                env=env,
                cwd=temp_dir
            )
            
            if result.returncode != 0:
                error_msg = f"R script failed (exit code {result.returncode})"
                if result.stderr:
                    error_msg += f": {result.stderr}"
                if result.stdout:
                    error_msg += f"\nR output: {result.stdout}"
                raise Exception(error_msg)
            
            # Check if CSV file was created
            if not os.path.exists(csv_filepath):
                raise Exception("CSV file was not created by R script")
            
            # Read the CSV file
            df = pd.read_csv(csv_filepath)
            
            print(f"Successfully read .qs file: {df.shape[0]} rows, {df.shape[1]} columns")
            return df
                        
    except subprocess.TimeoutExpired:
        raise Exception("R script execution timed out (300 seconds)")
    except FileNotFoundError as e:
        system = platform.system()
        install_msg = {
            'Windows': "Install R from https://cran.r-project.org/bin/windows/base/",
            'Darwin': "Install R with: brew install r",
            'Linux': "Install R with: sudo apt-get install r-base"
        }
        raise Exception(f"R not found. {install_msg.get(system, 'Please install R')}")
    except Exception as e:
        print(f"Error reading .qs file from S3: {e}")
        return pd.DataFrame()

def s3_read_qs(bucket, object_key):
    """
    Read .qs file from S3 using rpy2
    
    Args:
        bucket: S3 bucket name
        object_key: S3 object key
    
    Returns:
        pandas DataFrame
    """
    import tempfile
    import os
    
    try:
        import rpy2.robjects as robjects
        from rpy2.robjects import pandas2ri
        from rpy2.robjects.packages import importr
        
        # Activate automatic conversion
        pandas2ri.activate()
        
        # Import R packages
        qs = importr('qs')
        
        s3_client = boto3.client('s3')
        
        # Download to temporary file
        with tempfile.NamedTemporaryFile(suffix='.qs', delete=False) as tmp_file:
            s3_client.download_file(bucket, object_key, tmp_file.name)
            
            try:
                # Read .qs file
                r_df = qs.qread(tmp_file.name)
                
                # Convert to pandas
                df = pandas2ri.rpy2py(r_df)
                
                return df
                
            finally:
                # Clean up
                os.unlink(tmp_file.name)
                
    except ImportError:
        raise ImportError("rpy2 package required to read .qs files. Install with: pip install rpy2")
    except Exception as e:
        print(f"Error reading .qs file from S3: {e}")
        return pd.DataFrame()

def s3write_using(write_func, bucket, object, **kwargs):
    """Generic S3 write function"""
    import io
    
    s3_client = boto3.client('s3')
    
    try:
        # Create buffer
        buffer = io.BytesIO()
        
        # Call write function with buffer
        write_func(buffer, **kwargs)
        
        # Upload to S3
        buffer.seek(0)
        s3_client.put_object(
            Bucket=bucket,
            Key=object,
            Body=buffer.getvalue()
        )
        
    except Exception as e:
        print(f"Error writing to S3: {e}")
        raise

def convert_to_utc(df):
    """Convert datetime columns to UTC"""
    try:
        datetime_cols = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize('UTC')
            else:
                df[col] = df[col].dt.tz_convert('UTC')
        return df
        
    except Exception as e:
        return df

def s3_read_parquet_parallel(table_name, start_date, end_date, signals_list=None, 
                           bucket=None, callback=None, parallel=False, s3root='mark'):
    """Read multiple parquet files in parallel"""
    
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    def process_date(date_):
        prefix = f"{s3root}/{table_name}/date={date_.strftime('%Y-%m-%d')}"
        
        # List objects with prefix
        response = boto3.client('s3').list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            return pd.DataFrame()
        
        dfs = []
        for obj in response['Contents']:
            object_key = obj['Key']
            df = s3_read_parquet(bucket=bucket, object_key=object_key, date_=date_)
            df = convert_to_utc(df)
            if callback:
                df = callback(df)
            dfs.append(df)
        
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    
    if parallel:
        from concurrent.futures import ProcessPoolExecutor
        with ProcessPoolExecutor() as executor:
            results = list(executor.map(process_date, dates))
    else:
        results = [process_date(date_) for date_ in dates]
    
    # Filter out empty dataframes and concatenate
    valid_results = [df for df in results if not df.empty]
    return pd.concat(valid_results, ignore_index=True) if valid_results else pd.DataFrame()
