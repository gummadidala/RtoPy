"""
Database connection and utility functions
Converted from Database_Functions.R
"""

import os
import sys
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# import pymysql
# import pyodbc
import yaml
from datetime import datetime, timedelta
import boto3
# from botocore.exceptions import ClientError
# from pyathena import connect
# from pyathena.sqlalchemy import AthenaDialect
# import awswrangler as wr
import boto3
from botocore.config import Config

logger = logging.getLogger(__name__)

def load_credentials() -> Dict[str, Any]:
    """Load database credentials from YAML file"""
    try:
        with open("Monthly_Report_AWS.yaml", 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("Monthly_Report_AWS.yaml not found")
        return {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        return {}

# Load credentials
cred = load_credentials()

def my_db_append_table(conn, table_name: str, df: pd.DataFrame, chunksize: int = 10000):
    """
    Custom function to perform multiple inserts at once
    Equivalent to mydbAppendTable in R
    
    Args:
        conn: Database connection
        table_name: Name of the target table
        df: DataFrame to insert
        chunksize: Number of rows per chunk
    """
    
    if df.empty:
        logger.warning("DataFrame is empty, nothing to insert")
        return
    
    try:
        # Prepare DataFrame for insertion
        df_clean = df.copy()
        
        # Convert data types for SQL compatibility
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':  # String columns
                df_clean[col] = df_clean[col].fillna('')
                df_clean[col] = df_clean[col].astype(str).str.replace("'", "\\'")
                df_clean[col] = "'" + df_clean[col] + "'"
            elif pd.api.types.is_datetime64_any_dtype(df_clean[col]):  # Datetime columns
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
                df_clean[col] = df_clean[col].fillna('')
                df_clean[col] = "'" + df_clean[col] + "'"
            elif pd.api.types.is_numeric_dtype(df_clean[col]):  # Numeric columns
                df_clean[col] = df_clean[col].replace([np.inf, -np.inf], np.nan)
                df_clean[col] = df_clean[col].fillna('NULL')
                df_clean[col] = df_clean[col].astype(str)
            else:  # Other types
                df_clean[col] = df_clean[col].astype(str)
                df_clean[col] = df_clean[col].fillna('NULL')
        
        # Create value strings
        values_list = []
        for idx, row in df_clean.iterrows():
            row_values = ','.join(row.values)
            values_list.append(f"({row_values})")
        
        # Split into chunks
        chunks = [values_list[i:i + chunksize] for i in range(0, len(values_list), chunksize)]
        
        # Prepare base query
        columns = '`, `'.join(df.columns)
        base_query = f"INSERT INTO {table_name} (`{columns}`) VALUES "
        
        # Execute insertions
        for chunk in chunks:
            chunk_values = ','.join(chunk)
            query = base_query + chunk_values
            query = query.replace("'NULL'", "NULL")
            
            conn.execute(text(query))
        
        logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
        
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise

def get_atspm_connection(conf_atspm: Dict[str, str]):
    """
    Get ATSPM database connection
    
    Args:
        conf_atspm: ATSPM configuration dictionary
    
    Returns:
        Database connection object
    """
    
    try:
        system_name = os.name
        
        if system_name == 'nt':  # Windows
            connection_string = f"mssql+pyodbc://{os.getenv(conf_atspm['uid_env'])}:{os.getenv(conf_atspm['pwd_env'])}@{conf_atspm['odbc_dsn']}"
        else:  # Linux
            # Using FreeTDS driver
            connection_string = (
                f"mssql+pyodbc://{os.getenv(conf_atspm['uid_env'])}:{os.getenv(conf_atspm['pwd_env'])}"
                f"@{conf_atspm['odbc_dsn']}?driver=FreeTDS"
            )
        
        engine = create_engine(connection_string)
        return engine.connect()
        
    except Exception as e:
        logger.error(f"Error connecting to ATSPM database: {e}")
        raise

def get_maxview_connection(dsn: str = "maxview"):
    """
    Get MaxView database connection
    
    Args:
        dsn: Data source name
    
    Returns:
        Database connection object
    """
    
    try:
        system_name = os.name
        
        if system_name == 'nt':  # Windows
            connection_string = f"mssql+pyodbc://{os.getenv('MAXV_USERNAME')}:{os.getenv('MAXV_PASSWORD')}@{dsn}"
        else:  # Linux
            connection_string = (
                f"mssql+pyodbc://{os.getenv('MAXV_USERNAME')}:{os.getenv('MAXV_PASSWORD')}"
                f"@{dsn}?driver=FreeTDS"
            )
        
        engine = create_engine(connection_string)
        return engine.connect()
        
    except Exception as e:
        logger.error(f"Error connecting to MaxView database: {e}")
        raise

def get_maxview_eventlog_connection():
    """Get MaxView Event Log database connection"""
    return get_maxview_connection(dsn="MaxView_EventLog")

def get_cel_connection():
    """Get CEL database connection (alias for MaxView Event Log)"""
    return get_maxview_eventlog_connection()

def get_aurora_connection(use_pool: bool = False, load_data_local_infile: bool = False):
    """
    Get Aurora (MySQL) database connection
    
    Args:
        use_pool: Whether to use connection pooling
        load_data_local_infile: Enable local infile loading
    
    Returns:
        Database connection or connection pool
    """
    
    try:
        connection_string = (
            f"mysql+pymysql://{cred['RDS_USERNAME']}:{cred['RDS_PASSWORD']}"
            f"@{cred['RDS_HOST']}:3306/{cred['RDS_DATABASE']}"
        )
        
        connect_args = {}
        if load_data_local_infile:
            connect_args['local_infile'] = 1
        
        engine = create_engine(connection_string, connect_args=connect_args)
        
        if use_pool:
            from sqlalchemy.pool import QueuePool
            engine = create_engine(
                connection_string, 
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                connect_args=connect_args
            )
            return engine
        else:
            return engine.connect()
            
    except Exception as e:
        logger.error(f"Error connecting to Aurora database: {e}")
        raise

def get_aurora_connection_pool():
    """Get Aurora connection pool"""
    return get_aurora_connection(use_pool=True)

def get_athena_connection(conf_athena: Dict[str, str], use_pool: bool = False):
    """
    Get Athena database connection using SQLAlchemy and PyAthena
    
    Args:
        conf_athena (dict): Athena configuration dictionary with keys:
            - 'database': Athena database name
            - 'staging_dir': S3 location for query results
            - 'region' (optional): AWS region (default from env or 'us-east-1')
            - 'uid' and 'pwd' (optional): Only needed for federated connectors
        use_pool (bool): If True, returns SQLAlchemy engine with pooling.
    
    Returns:
        sqlalchemy Connection or Engine
    """
    try:
        required_keys = ['database', 'staging_dir']
        missing_keys = [key for key in required_keys if key not in conf_athena]
        if missing_keys:
            raise ValueError(f"Missing required Athena config keys: {missing_keys}")

        region = conf_athena.get('region', os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))

        uid = conf_athena.get('uid', '')
        pwd = conf_athena.get('pwd', '')

        # Create connection string (UID/PWD optional; used for federated connectors only)
        auth_part = f"{uid}:{pwd}@" if uid and pwd else ""
        connection_string = (
            f"awsathena+rest://{auth_part}athena.{region}.amazonaws.com:443/"
            f"{conf_athena['database']}?s3_staging_dir={conf_athena['staging_dir']}"
        )

        logger.info("Creating Athena connection...")

        if use_pool:
            from sqlalchemy.pool import QueuePool
            engine = create_engine(
                connection_string,
                poolclass=QueuePool,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=3600,
            )
            logger.info("Athena connection pool created")
            return engine
        else:
            engine = create_engine(connection_string)
            conn = engine.connect()
            logger.info("Athena connection established")
            return conn

    except ImportError:
        logger.error("Missing dependencies. Install PyAthena with:\n  pip install 'PyAthena[SQLAlchemy]'")
        raise
    except Exception as e:
        logger.error(f"Error creating Athena connection: {e}")
        raise

def get_athena_connection_pool(conf_athena: Dict[str, str]):
    """Get Athena connection pool"""
    return get_athena_connection(conf_athena, use_pool=True)

def add_athena_partition(conf_athena: Dict[str, str], bucket: str, table_name: str, date_: str):
    """
    Add partition to Athena table
    
    Args:
        conf_athena: Athena configuration
        bucket: S3 bucket name
        table_name: Name of the table
        date_: Date string for partition
    """
    
    conn = None
    try:
        conn = get_athena_connection(conf_athena)
        
        alter_sql = f"""
        ALTER TABLE {conf_athena['database']}.{table_name}
        ADD PARTITION (date='{date_}')
        """
        
        conn.execute(text(alter_sql))
        logger.info(f"Successfully created partition (date='{date_}') for {conf_athena['database']}.{table_name}")
        
    except Exception as e:
        error_message = str(e)
        if "already exists" in error_message.lower():
            logger.info(f"Partition (date='{date_}') already exists for {conf_athena['database']}.{table_name}")
        else:
            logger.error(f"Error creating partition: {error_message}")
    finally:
        if conn:
            conn.close()

def debug_athena_connection(conf_athena: Dict[str, str]):
    """
    Debug Athena connection issues
    
    Args:
        conf_athena: Athena configuration dictionary
    """
    
    print("=== Athena Connection Debug ===")
    
    # Check configuration
    print(f"Configuration keys: {list(conf_athena.keys())}")
    print(f"Database: {conf_athena.get('database', 'NOT SET')}")
    print(f"Staging dir: {conf_athena.get('staging_dir', 'NOT SET')}")
    print(f"Region: {conf_athena.get('region', os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))}")
    
    # Check AWS credentials
    try:
        boto_config = Config(
            retries = {
                'max_attempts': 10,
                'mode': 'standard'
            },
            max_pool_connections=500  # Increase this number as needed
        )
        session = boto3.Session(
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd'),
            region_name=conf_athena.get('region', os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        )
        athena_client = session.client('athena', config=boto_config)
        print("✓ AWS credentials valid")
        
        # Test Athena access
        response = athena_client.list_databases(CatalogName='AwsDataCatalog')
        databases = [db['Name'] for db in response['DatabaseList']]
        print(f"✓ Available databases: {databases}")
        
        if conf_athena.get('database') in databases:
            print(f"✓ Target database '{conf_athena['database']}' exists")
        else:
            print(f"✗ Target database '{conf_athena['database']}' not found")
            
    except Exception as e:
        print(f"✗ AWS/Athena error: {e}")
    
    # Check S3 staging directory
    try:
        import boto3
        s3_client = boto3.client('s3',
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd')
        )
        
        staging_dir = conf_athena.get('staging_dir', '')
        if staging_dir.startswith('s3://'):
            bucket = staging_dir.replace('s3://', '').split('/')[0]
            s3_client.head_bucket(Bucket=bucket)
            print(f"✓ S3 staging bucket '{bucket}' accessible")
        else:
            print(f"✗ Invalid staging directory format: {staging_dir}")
            
    except Exception as e:
        print(f"✗ S3 staging directory error: {e}")

def query_data(metric: Dict[str, Any], 
               level: str = "corridor",
               resolution: str = "monthly", 
               hourly: bool = False,
               zone_group: str = "",
               corridor: Optional[str] = None,
               month: Optional[str] = None,
               quarter: Optional[str] = None,
               upto: bool = True,
               connection_pool=None) -> pd.DataFrame:
    """
    Query data from the database with various filters
    
    Args:
        metric: Metric configuration dictionary
        level: Data level (corridor, subcorridor, signal)
        resolution: Time resolution (quarterly, monthly, weekly, daily)
        hourly: Whether to use hourly data
        zone_group: Zone group filter
        corridor: Corridor filter
        month: Month filter
        quarter: Quarter filter
        upto: Whether to include data up to the specified period
        connection_pool: Database connection pool
    
    Returns:
        DataFrame with query results
    """
    
    if connection_pool is None:
        logger.error("No database connection provided")
        return pd.DataFrame()
    
    try:
        # Map resolution to abbreviation
        period_mapping = {
            "quarterly": "qu",
            "monthly": "mo", 
            "weekly": "wk",
            "daily": "dy"
        }
        
        # Map level to abbreviation
        level_mapping = {
            "corridor": "cor",
            "subcorridor": "sub",
            "signal": "sig"
        }
        
        per = period_mapping.get(resolution, "mo")
        mr_ = level_mapping.get(level, "cor")
        
        # Determine table name
        if hourly and metric.get('hourly_table'):
            table = f"{mr_}_{per}_{metric['hourly_table']}"
        else:
            table = f"{mr_}_{per}_{metric['table']}"
        
        # Build WHERE clause based on zone_group
        rtop1_zones = ["Zone 1", "Zone 2", "Zone 3", "Zone 4"]  # Define as needed
        rtop2_zones = ["Zone 5", "Zone 6", "Zone 7", "Zone 8"]  # Define as needed
        
        if level == "corridor" and "RTOP" in zone_group:
            if zone_group == "All RTOP":
                zones = ["All RTOP", "RTOP1", "RTOP2"] + rtop1_zones + rtop2_zones
            elif zone_group == "RTOP1":
                zones = ["All RTOP", "RTOP1"] + rtop1_zones
            elif zone_group == "RTOP2":
                zones = ["All RTOP", "RTOP2"] + rtop2_zones
            
            zones_str = "', '".join(zones)
            where_clause = f"WHERE Zone_Group in ('{zones_str}')"
            where_clause += " AND Corridor NOT LIKE 'Zone%'"
            
        elif zone_group == "Zone 7":
            zones = ["Zone 7", "Zone 7m", "Zone 7d"]
            zones_str = "', '".join(zones)
            where_clause = f"WHERE Zone_Group in ('{zones_str}')"
            
        elif level == "signal" and zone_group == "All":
            where_clause = "WHERE 1=1"
            
        else:
            where_clause = f"WHERE Zone_Group = '{zone_group}'"
        
        # Build base query
        query = f"SELECT * FROM {table} {where_clause}"
        
        # Add time filters
        comparison = "<=" if upto else "="
        
        if month:
            month_date = pd.to_datetime(month)
            
            if hourly and metric.get('hourly_table'):
                if resolution == "monthly":
                    end_time = month_date + pd.DateOffset(months=1) - pd.DateOffset(hours=1)
                    query += f" AND Hour <= '{end_time}'"
                    if not upto:
                        query += f" AND Hour >= '{month_date}'"
            else:
                if resolution == "monthly":
                    query += f" AND Month {comparison} '{month_date.strftime('%Y-%m-%d')}'"
                elif resolution in ["weekly", "daily"]:
                    end_date = month_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
                    query += f" AND Date {comparison} '{end_date.strftime('%Y-%m-%d')}'"
        
        elif quarter:
            query += f" AND Quarter {comparison} {quarter}"
        
        # Add corridor filter if specified
        if corridor:
            query += f" AND Corridor = '{corridor}'"
        
        # Execute query
        df = pd.read_sql(query, connection_pool)
        
        # Convert date/datetime columns
        date_columns = ['Month', 'Date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        datetime_columns = ['Hour']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        return df
        
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return pd.DataFrame()

def query_udc_trend() -> Dict[str, pd.DataFrame]:
    """
    Query UDC (User Delay Cost) trend data
    
    Returns:
        Dictionary of DataFrames with UDC trend data
    """
    
    try:
        # This would need a connection pool - placeholder for now
        conn = get_aurora_connection()
        
        df = pd.read_sql("SELECT * FROM cor_mo_udc_trend_table", conn)
        
        # Parse JSON data
        import json
        udc_list = json.loads(df['data'].iloc[0])
        
        result = {}
        for key, data in udc_list.items():
            df_data = pd.DataFrame(data)
            df_data['Month'] = pd.to_datetime(df_data['Month'])
            
            # Clean column names
            df_data.columns = [col.replace('.', ' ').replace('_', ' ') for col in df_data.columns]
            result[key] = df_data
        
        conn.close()
        return result
        
    except Exception as e:
        logger.error(f"Error querying UDC trend data: {e}")
        return {}

def query_udc_hourly(zone_group: str, month: str) -> pd.DataFrame:
    """
    Query UDC hourly data
    
    Args:
        zone_group: Zone group filter
        month: Month filter
    
    Returns:
        DataFrame with UDC hourly data
    """
    
    try:
        conn = get_aurora_connection()
        
        query = f"""
        SELECT * FROM cor_mo_hourly_udc 
        WHERE Zone = '{zone_group}' AND Month <= '{month}'
        """
        
        df = pd.read_sql(query, conn)
        df['Month'] = pd.to_datetime(df['Month'])
        df['month_hour'] = pd.to_datetime(df['month_hour'])
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Error querying UDC hourly data: {e}")
        return pd.DataFrame()

def query_health_data(health_metric: str,
                     level: str,
                     zone_group: str,
                     corridor: Optional[str] = None,
                     month: Optional[str] = None) -> pd.DataFrame:
    """
    Query health metrics data (operations, maintenance, safety)
    
    Args:
        health_metric: Health metric type (ops, maint, safety)
        level: Data level (corridor, subcorridor, signal)
        zone_group: Zone group filter
        corridor: Corridor filter
        month: Month filter
    
    Returns:
        DataFrame with health data
    """
    
    try:
        per = "mo"
        
        level_mapping = {
            "corridor": "sub",
            "subcorridor": "sub", 
            "signal": "sig"
        }
        
        mr_ = level_mapping.get(level, "sub")
        table = f"{mr_}_{per}_{health_metric}"
        
        # Build WHERE clause
        rtop1_zones = ["Zone 1", "Zone 2", "Zone 3", "Zone 4"]
        rtop2_zones = ["Zone 5", "Zone 6", "Zone 7", "Zone 8"]
        
        if (level in ["corridor", "subcorridor"]) and ("RTOP" in zone_group or zone_group == "Zone 7"):
            if zone_group == "All RTOP":
                zones = ["All RTOP", "RTOP1", "RTOP2"] + rtop1_zones + rtop2_zones
            elif zone_group == "RTOP1":
                zones = ["All RTOP", "RTOP1"] + rtop1_zones
            elif zone_group == "RTOP2":
                zones = ["All RTOP", "RTOP2"] + rtop2_zones
            elif zone_group == "Zone 7":
                zones = ["Zone 7m", "Zone 7d"]
            
            zones_str = "', '".join(zones)
            where_clause = f"WHERE Zone_Group in ('{zones_str}')"
            
        elif (level in ["corridor", "subcorridor"]) and corridor == "All Corridors":
            where_clause = f"WHERE Zone_Group = '{zone_group}'"
            
        else:
            where_clause = f"WHERE Corridor = '{corridor}'"
        
        if month:
            where_clause += f" AND Month = '{month}'"
        
        query = f"SELECT * FROM {table} {where_clause}"
        
        conn = get_aurora_connection()
        df = pd.read_sql(query, conn)
        df['Month'] = pd.to_datetime(df['Month'])
        
        conn.close()
        return df
        
    except Exception as e:
        logger.error(f"Error querying health data: {e}")
        return pd.DataFrame()

def create_aurora_partitioned_table(aurora_conn, table_name: str, period_field: str = "Timeperiod"):
    """
    Create a partitioned table in Aurora MySQL
    
    Args:
        aurora_conn: Aurora database connection
        table_name: Name of the table to create
        period_field: Name of the period field for partitioning
    """
    
    try:
        # Generate partition dates
        start_date = datetime.now() - timedelta(days=300)
        end_date = datetime.now() + timedelta(days=30)
        
        months = pd.date_range(start=start_date, end=end_date, freq='MS')
        partition_dates = [date.strftime('%Y-%m-01') for date in months]
        partition_names = [date.strftime('p_%Y%m') for date in months]
        
        partitions = [f"PARTITION {name} VALUES LESS THAN ('{date} 00:00:00')," 
                     for name, date in zip(partition_names, partition_dates)]
        
        # Determine variable name based on table suffix
        table_suffix = table_name.split('_')[-1]
        
        variable_mapping = {
            'aogh': 'aog',
            'vph': 'vph' if period_field == 'Hour' else 'vol',
            'paph': 'paph' if period_field == 'Hour' else 'vol',
            'prh': 'pr',
            'qsh': 'qs_freq',
            'sfh': 'sf_freq'
        }
        
        var = variable_mapping.get(table_suffix, 'value')
        
        create_sql = f"""
        CREATE TABLE `{table_name}_part` (
          `Zone_Group` varchar(128) DEFAULT NULL,
          `Corridor` varchar(128) DEFAULT NULL,
          `{period_field}` datetime NOT NULL,
          `{var}` double DEFAULT NULL,
          `ones` double DEFAULT NULL,
          `delta` double DEFAULT NULL,
          `Description` varchar(128) DEFAULT NULL,
          `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
          PRIMARY KEY (`id`, `{period_field}`),
          UNIQUE KEY `idx_{table_name}_unique` (`{period_field}`, `Zone_Group`, `Corridor`),
          KEY `idx_{table_name}_zone_period` (`Zone_Group`, `{period_field}`),
          KEY `idx_{table_name}_corridor_period` (`Corridor`, `{period_field}`)
        )
        PARTITION BY RANGE COLUMNS (`{period_field}`) (
            {' '.join(partitions)}
            PARTITION future VALUES LESS THAN (MAXVALUE)
        )
        """
        
        aurora_conn.execute(text(create_sql))
        logger.info(f"Successfully created partitioned table {table_name}_part")
        
    except Exception as e:
        logger.error(f"Error creating partitioned table: {e}")
        raise

def get_aurora_partitions(aurora_conn, table_name: str) -> List[str]:
    """
    Get list of existing partitions for a table
    
    Args:
        aurora_conn: Aurora database connection
        table_name: Name of the table
    
    Returns:
        List of partition names
    """
    
    try:
        query = f"""
        SELECT PARTITION_NAME FROM information_schema.partitions
        WHERE TABLE_NAME = '{table_name}'
        """
        
        df = pd.read_sql(query, aurora_conn)
        
        if len(df) > 1:
            return df['PARTITION_NAME'].tolist()
        else:
            return []
            
    except Exception as e:
        logger.error(f"Error getting partitions: {e}")
        return []

def add_aurora_partition(aurora_conn, table_name: str):
    """
    Add new partition for next month
    
    Args:
        aurora_conn: Aurora database connection
        table_name: Name of the table
    """
    
    try:
        # Calculate next month
        next_month = datetime.now() + timedelta(days=32)
        next_month = next_month.replace(day=1)
        
        new_partition_date = next_month.strftime('%Y-%m-01')
        new_partition_name = next_month.strftime('p_%Y%m')
        
        existing_partitions = get_aurora_partitions(aurora_conn, table_name)
        
        if new_partition_name not in existing_partitions and existing_partitions:
            alter_sql = f"""
            ALTER TABLE {table_name}
            REORGANIZE PARTITION future INTO (
                PARTITION {new_partition_name} VALUES LESS THAN ('{new_partition_date} 00:00:00'),
                PARTITION future VALUES LESS THAN MAXVALUE
            )
            """
            
            aurora_conn.execute(text(alter_sql))
            logger.info(f"Added partition {new_partition_name} to {table_name}")
            
    except Exception as e:
        logger.error(f"Error adding partition: {e}")

def drop_aurora_partitions(aurora_conn, table_name: str, months_to_keep: int = 8):
    """
    Drop old partitions to save space
    
    Args:
        aurora_conn: Aurora database connection
        table_name: Name of the table
        months_to_keep: Number of months of data to retain
    """
    
    try:
        existing_partitions = get_aurora_partitions(aurora_conn, table_name)
        
        # Filter out 'future' partition
        existing_partitions = [p for p in existing_partitions if p != 'future']
        
        if existing_partitions:
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=months_to_keep * 30)
            drop_partition_name = cutoff_date.strftime('p_%Y%m')
            
            # Get partitions to drop (older than cutoff)
            partitions_to_drop = [p for p in existing_partitions if p <= drop_partition_name]
            
            for partition_name in partitions_to_drop:
                drop_sql = f"ALTER TABLE {table_name} DROP PARTITION {partition_name};"
                aurora_conn.execute(text(drop_sql))
                logger.info(f"Dropped partition {partition_name} from {table_name}")
                
    except Exception as e:
        logger.error(f"Error dropping partitions: {e}")

# Connection management classes
class DatabaseManager:
    """Database connection manager with context management"""
    
    def __init__(self, db_type: str, **kwargs):
        self.db_type = db_type
        self.kwargs = kwargs
        self.connection = None
    
    def __enter__(self):
        if self.db_type == 'aurora':
            self.connection = get_aurora_connection(**self.kwargs)
        elif self.db_type == 'athena':
            self.connection = get_athena_connection(**self.kwargs)
        elif self.db_type == 'atspm':
            self.connection = get_atspm_connection(**self.kwargs)
        elif self.db_type == 'maxview':
            self.connection = get_maxview_connection(**self.kwargs)
        else:
            raise ValueError(f"Unknown database type: {self.db_type}")
        
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()

class ConnectionPool:
    """Simple connection pool manager"""
    
    def __init__(self, db_type: str, pool_size: int = 5, **kwargs):
        self.db_type = db_type
        self.pool_size = pool_size
        self.kwargs = kwargs
        self.pool = []
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize the connection pool"""
        for _ in range(self.pool_size):
            if self.db_type == 'aurora':
                conn = get_aurora_connection(**self.kwargs)
            elif self.db_type == 'athena':
                conn = get_athena_connection(**self.kwargs)
            else:
                raise ValueError(f"Pool not supported for {self.db_type}")
            
            self.pool.append(conn)
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self.pool:
            return self.pool.pop()
        else:
            # Create new connection if pool is empty
            if self.db_type == 'aurora':
                return get_aurora_connection(**self.kwargs)
            elif self.db_type == 'athena':
                return get_athena_connection(**self.kwargs)
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if len(self.pool) < self.pool_size:
            self.pool.append(conn)
        else:
            conn.close()
    
    def close_all(self):
        """Close all connections in the pool"""
        for conn in self.pool:
            conn.close()
        self.pool.clear()

# Utility functions for data validation and cleaning
def validate_table_exists(conn, table_name: str) -> bool:
    """
    Check if a table exists in the database
    
    Args:
        conn: Database connection
        table_name: Name of the table to check
    
    Returns:
        Boolean indicating if table exists
    """
    
    try:
        # This query works for most SQL databases
        query = f"""
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_name = '{table_name}'
        """
        
        result = pd.read_sql(query, conn)
        return result['count'].iloc[0] > 0
        
    except Exception as e:
        logger.error(f"Error checking if table exists: {e}")
        return False

def get_table_schema(conn, table_name: str) -> pd.DataFrame:
    """
    Get the schema of a table
    
    Args:
        conn: Database connection
        table_name: Name of the table
    
    Returns:
        DataFrame with column information
    """
    
    try:
        query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
        """
        
        return pd.read_sql(query, conn)
        
    except Exception as e:
        logger.error(f"Error getting table schema: {e}")
        return pd.DataFrame()

def sanitize_data_for_sql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame for SQL insertion
    
    Args:
        df: DataFrame to sanitize
    
    Returns:
        Sanitized DataFrame
    """
    
    df_clean = df.copy()
    
    # Handle string columns
    string_cols = df_clean.select_dtypes(include=['object']).columns
    for col in string_cols:
        df_clean[col] = df_clean[col].astype(str)
        df_clean[col] = df_clean[col].str.replace("'", "''")  # Escape single quotes
        df_clean[col] = df_clean[col].str.replace("\\", "\\\\")  # Escape backslashes
    
    # Handle numeric columns
    numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        df_clean[col] = df_clean[col].replace([np.inf, -np.inf], np.nan)
    
    # Handle datetime columns
    datetime_cols = df_clean.select_dtypes(include=['datetime64[ns]']).columns
    for col in datetime_cols:
        df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df_clean

def bulk_insert_with_retry(conn, table_name: str, df: pd.DataFrame, 
                          max_retries: int = 3, batch_size: int = 1000) -> bool:
    """
    Bulk insert with retry logic and batching
    
    Args:
        conn: Database connection
        table_name: Target table name
        df: DataFrame to insert
        max_retries: Maximum number of retry attempts
        batch_size: Number of rows per batch
    
    Returns:
        Boolean indicating success
    """
    
    if df.empty:
        logger.info("DataFrame is empty, nothing to insert")
        return True
    
    for attempt in range(max_retries):
        try:
            # Split DataFrame into batches
            num_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)
            
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, len(df))
                batch_df = df.iloc[start_idx:end_idx]
                
                # Use pandas to_sql for reliable insertion
                batch_df.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
                
                logger.info(f"Inserted batch {i+1}/{num_batches} ({len(batch_df)} rows)")
            
            logger.info(f"Successfully inserted {len(df)} rows into {table_name}")
            return True
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to insert data after {max_retries} attempts")
                return False
            
            # Wait before retrying
            import time
            time.sleep(2 ** attempt)  # Exponential backoff
    
    return False

# AWS-specific database functions
def create_athena_table_from_s3(athena_conn, 
                               database: str,
                               table_name: str, 
                               s3_location: str,
                               columns: Dict[str, str],
                               partition_columns: Optional[Dict[str, str]] = None,
                               file_format: str = 'PARQUET') -> bool:
    """
    Create an Athena external table pointing to S3 data
    
    Args:
        athena_conn: Athena connection
        database: Database name
        table_name: Table name
        s3_location: S3 path to data
        columns: Dictionary of column names and types
        partition_columns: Dictionary of partition column names and types
        file_format: File format (PARQUET, JSON, etc.)
    
    Returns:
        Boolean indicating success
    """
    
    try:
        # Build column definitions
        column_defs = []
        for col_name, col_type in columns.items():
            column_defs.append(f"`{col_name}` {col_type}")
        
        # Build partition definitions
        partition_def = ""
        if partition_columns:
            partition_defs = []
            for col_name, col_type in partition_columns.items():
                partition_defs.append(f"`{col_name}` {col_type}")
            partition_def = f"PARTITIONED BY ({', '.join(partition_defs)})"
        
        # Create table SQL
        create_sql = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
            {', '.join(column_defs)}
        )
        {partition_def}
        STORED AS {file_format}
        LOCATION '{s3_location}'
        """
        
        athena_conn.execute(text(create_sql))
        logger.info(f"Successfully created Athena table {database}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating Athena table: {e}")
        return False

def repair_athena_partitions(athena_conn, database: str, table_name: str) -> bool:
    """
    Repair Athena table partitions (equivalent to MSCK REPAIR TABLE)
    
    Args:
        athena_conn: Athena connection
        database: Database name
        table_name: Table name
    
    Returns:
        Boolean indicating success
    """
    
    try:
        repair_sql = f"MSCK REPAIR TABLE {database}.{table_name}"
        athena_conn.execute(text(repair_sql))
        logger.info(f"Successfully repaired partitions for {database}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error repairing partitions: {e}")
        return False

# Export main functions
__all__ = [
    'load_credentials',
    'my_db_append_table',
    'get_atspm_connection',
    'get_maxview_connection',
    'get_maxview_eventlog_connection',
    'get_cel_connection',
    'get_aurora_connection',
    'get_aurora_connection_pool',
    'get_athena_connection',
    'get_athena_connection_pool',
    'add_athena_partition',
    'query_data',
    'query_udc_trend',
    'query_udc_hourly',
    'query_health_data',
    'create_aurora_partitioned_table',
    'get_aurora_partitions',
    'add_aurora_partition',
    'drop_aurora_partitions',
    'DatabaseManager',
    'ConnectionPool',
    'validate_table_exists',
    'get_table_schema',
    'sanitize_data_for_sql',
    'bulk_insert_with_retry',
    'create_athena_table_from_s3',
    'repair_athena_partitions'
]
