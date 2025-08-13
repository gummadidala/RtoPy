"""
Database Functions - Python conversion from R

Provides database connection and query utilities for various databases
including ATSPM, MaxView, Aurora, and Athena connections.
"""

import os
import sys
import platform
import pandas as pd
import sqlalchemy as sa
import pyodbc
import pymysql
import boto3
import yaml
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import Optional, List, Dict, Any, Union
import logging
from contextlib import contextmanager
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import urllib.parse

# Setup logging
logger = logging.getLogger(__name__)

# Load credentials
def load_credentials(config_file: str = "Monthly_Report_AWS.yaml") -> Dict[str, Any]:
    """Load credentials from YAML file"""
    try:
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file {config_file} not found, using environment variables")
        return {}

# Global credentials
cred = load_credentials()

def mydb_append_table(conn, table_name: str, df: pd.DataFrame, chunksize: int = 10000):
    """
    Perform multiple inserts at once - equivalent to R's mydbAppendTable
    
    Args:
        conn: Database connection
        table_name: Name of the table to insert into
        df: DataFrame to insert
        chunksize: Number of records per chunk
    """
    try:
        # Process DataFrame similar to R version
        df_processed = df.copy()
        
        # Convert date columns to string format
        for col in df_processed.columns:
            if pd.api.types.is_datetime64_any_dtype(df_processed[col]):
                if df_processed[col].dt.time.eq(pd.Timestamp('00:00:00').time()).all():
                    # Date only
                    df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d')
                else:
                    # DateTime
                    df_processed[col] = df_processed[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Handle character columns - escape single quotes
        for col in df_processed.select_dtypes(include=['object']).columns:
            df_processed[col] = df_processed[col].astype(str)
            df_processed[col] = df_processed[col].replace('nan', '')
            df_processed[col] = df_processed[col].str.replace("'", "\\'")
        
        # Handle numeric columns - replace inf with None
        for col in df_processed.select_dtypes(include=[np.number]).columns:
            df_processed[col] = df_processed[col].replace([np.inf, -np.inf], None)
        
        # Split into chunks and insert
        total_chunks = len(df_processed) // chunksize + (1 if len(df_processed) % chunksize else 0)
        
        for i in range(0, len(df_processed), chunksize):
            chunk = df_processed.iloc[i:i+chunksize]
            chunk.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
            
        logger.info(f"Successfully inserted {len(df_processed)} records into {table_name}")
        
    except Exception as e:
        logger.error(f"Error inserting data into {table_name}: {e}")
        raise

def get_atspm_connection(conf_atspm: Dict[str, Any]):
    """
    Get ATSPM database connection
    
    Args:
        conf_atspm: ATSPM configuration dictionary
    
    Returns:
        Database connection
    """
    try:
        uid = os.getenv(conf_atspm['uid_env'])
        pwd = os.getenv(conf_atspm['pwd_env'])
        
        if platform.system() == "Windows":
            conn_str = f"DSN={conf_atspm['odbc_dsn']};UID={uid};PWD={pwd}"
            connection = pyodbc.connect(conn_str)
        else:  # Linux
            conn_str = f"DRIVER=FreeTDS;DSN={conf_atspm['odbc_dsn']};UID={uid};PWD={pwd}"
            connection = pyodbc.connect(conn_str)
        
        logger.info("Successfully connected to ATSPM database")
        return connection
        
    except Exception as e:
        logger.error(f"Error connecting to ATSPM database: {e}")
        raise

def get_maxview_connection(dsn: str = "maxview"):
    """
    Get MaxView database connection
    
    Args:
        dsn: Data source name
    
    Returns:
        Database connection
    """
    try:
        uid = os.getenv('MAXV_USERNAME')
        pwd = os.getenv('MAXV_PASSWORD')
        
        if platform.system() == "Windows":
            conn_str = f"DSN={dsn};UID={uid};PWD={pwd}"
            connection = pyodbc.connect(conn_str)
        else:  # Linux
            conn_str = f"DRIVER=FreeTDS;DSN={dsn};UID={uid};PWD={pwd}"
            connection = pyodbc.connect(conn_str)
        
        logger.info(f"Successfully connected to MaxView database ({dsn})")
        return connection
        
    except Exception as e:
        logger.error(f"Error connecting to MaxView database ({dsn}): {e}")
        raise

def get_maxview_eventlog_connection():
    """Get MaxView EventLog database connection"""
    return get_maxview_connection(dsn="MaxView_EventLog")

def get_cel_connection():
    """Get CEL database connection (alias for MaxView EventLog)"""
    return get_maxview_eventlog_connection()

def get_aurora_connection(load_data_local_infile: bool = False) -> sa.engine.Engine:
    """
    Get Aurora MySQL database connection
    
    Args:
        load_data_local_infile: Enable local infile loading
    
    Returns:
        SQLAlchemy engine
    """
    try:
        host = cred.get('RDS_HOST')
        port = 3306
        database = cred.get('RDS_DATABASE')
        username = cred.get('RDS_USERNAME')
        password = cred.get('RDS_PASSWORD')
        
        # URL encode password to handle special characters
        password_encoded = urllib.parse.quote_plus(password)
        
        connection_string = f"mysql+pymysql://{username}:{password_encoded}@{host}:{port}/{database}"
        
        engine = create_engine(
            connection_string,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            echo=False
        )
        
        logger.info("Successfully connected to Aurora database")
        return engine
        
    except Exception as e:
        logger.error(f"Error connecting to Aurora database: {e}")
        raise

def get_athena_connection(conf_athena: Dict[str, Any]):
    """
    Get Athena database connection using boto3
    
    Args:
        conf_athena: Athena configuration dictionary
    
    Returns:
        Boto3 Athena client
    """
    try:
        client = boto3.client('athena')
        logger.info("Successfully connected to Athena")
        return client
        
    except Exception as e:
        logger.error(f"Error connecting to Athena: {e}")
        raise

def add_athena_partition(conf_athena: Dict[str, Any], bucket: str, table_name: str, date_: str):
    """
    Add partition to Athena table
    
    Args:
        conf_athena: Athena configuration
        bucket: S3 bucket name
        table_name: Table name
        date_: Date for partition
    """
    try:
        client = get_athena_connection(conf_athena)
        
        query = f"ALTER TABLE {conf_athena['database']}.{table_name} ADD PARTITION (date='{date_}')"
        
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': conf_athena['staging_dir']},
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        # Wait for query to complete
        while True:
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                logger.info(f"Successfully created partition (date='{date_}') for {conf_athena['database']}.{table_name}")
                break
            elif status in ['FAILED', 'CANCELLED']:
                error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Failed to create partition: {error_msg}")
                break
            else:
                import time
                time.sleep(1)
                
    except Exception as e:
        logger.error(f"Error adding Athena partition: {e}")

# Define zone constants (equivalent to R global variables)
RTOP1_ZONES = ["Zone 1", "Zone 2", "Zone 3", "Zone 4", "Zone 5", "Zone 6"]
RTOP2_ZONES = ["Zone 7", "Zone 8", "Zone 9", "Zone 10"]

def query_data(
    metric: Dict[str, Any],
    level: str = "corridor",
    resolution: str = "monthly", 
    hourly: bool = False,
    zone_group: str = None,
    corridor: Optional[str] = None,
    month: Optional[Union[str, datetime]] = None,
    quarter: Optional[str] = None,
    upto: bool = True,
    connection = None
) -> pd.DataFrame:
    """
    Query data from database tables
    
    Args:
        metric: Metric configuration dictionary
        level: One of 'corridor', 'subcorridor', 'signal'
        resolution: One of 'quarterly', 'monthly', 'weekly', 'daily'
        hourly: Whether to use hourly data
        zone_group: Zone group filter
        corridor: Corridor filter
        month: Month filter
        quarter: Quarter filter
        upto: Whether to include data up to the specified period
        connection: Database connection
    
    Returns:
        DataFrame with query results
    """
    try:
        # Map resolution to abbreviation
        per_map = {
            "quarterly": "qu",
            "monthly": "mo", 
            "weekly": "wk",
            "daily": "dy"
        }
        per = per_map.get(resolution)
        
        # Map level to abbreviation
        mr_map = {
            "corridor": "cor",
            "subcorridor": "sub",
            "signal": "sig"
        }
        mr_ = mr_map.get(level)
        
        # Determine table name
        if hourly and metric.get('hourly_table'):
            tab = metric['hourly_table']
        else:
            tab = metric['table']
            
        table = f"{mr_}_{per}_{tab}"
        
        # Build where clause based on zone_group
        if level == "corridor" and "RTOP" in zone_group:
            if zone_group == "All RTOP":
                zones = ["All RTOP", "RTOP1", "RTOP2"] + RTOP1_ZONES + RTOP2_ZONES
            elif zone_group == "RTOP1":
                zones = ["All RTOP", "RTOP1"] + RTOP1_ZONES
            elif zone_group == "RTOP2":
                zones = ["All RTOP", "RTOP2"] + RTOP2_ZONES
            
            zones_str = "', '".join(zones)
            where_clause = f"WHERE Zone_Group in ('{zones_str}') AND Corridor NOT LIKE 'Zone%'"
            
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
        
        # Add date filters
        comparison = "<=" if upto else "="
        
        if isinstance(month, str):
            month = pd.to_datetime(month)
            
        if hourly and metric.get('hourly_table'):
            if resolution == "monthly" and month:
                end_time = month + relativedelta(months=1) - timedelta(hours=1)
                query += f" AND Hour <= '{end_time}'"
                if not upto:
                    query += f" AND Hour >= '{month}'"
                    
        elif resolution == "monthly" and month:
            query += f" AND Month {comparison} '{month.strftime('%Y-%m-%d')}'"
            
        elif resolution == "quarterly" and quarter:
            query += f" AND Quarter {comparison} {quarter}"
            
        elif resolution in ["weekly", "daily"] and month:
            end_date = month + relativedelta(months=1) - timedelta(days=1)
            query += f" AND Date {comparison} '{end_date.strftime('%Y-%m-%d')}'"
        
        # Execute query
        df = pd.read_sql(query, connection)
        
        # Filter by corridor if specified
        if corridor:
            df = df[df['Corridor'] == corridor]
        
        # Convert date columns
        date_cols = ['Month', 'Date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        datetime_cols = ['Hour']
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
        
        return df
        
    except Exception as e:
        logger.error(f"Error querying data: {e}")
        return pd.DataFrame()

def query_udc_trend(connection) -> List[pd.DataFrame]:
    """
    Query UDC trend data
    
    Args:
        connection: Database connection
    
    Returns:
        List of DataFrames with UDC trend data
    """
    try:
        df = pd.read_sql("SELECT * FROM cor_mo_udc_trend_table", connection)
        
        result = []
        for _, row in df.iterrows():
            data_dict = json.loads(row['data'])
            data_df = pd.DataFrame(data_dict)
            data_df['Month'] = pd.to_datetime(data_df['Month'])
            
            # Clean column names (replace punctuation with spaces)
            data_df.columns = [col.replace(':', ' ').replace('.', ' ').replace('-', ' ') 
                              for col in data_df.columns]
            result.append(data_df)
            
        return result
        
    except Exception as e:
        logger.error(f"Error querying UDC trend data: {e}")
        return []

def query_udc_hourly(zone_group: str, month: Union[str, datetime], connection) -> pd.DataFrame:
    """
    Query UDC hourly data
    
    Args:
        zone_group: Zone group filter
        month: Month filter
        connection: Database connection
    
    Returns:
        DataFrame with UDC hourly data
    """
    try:
        df = pd.read_sql("SELECT * FROM cor_mo_hourly_udc", connection)
        df['Month'] = pd.to_datetime(df['Month'])
        df['month_hour'] = pd.to_datetime(df['month_hour'])
        
        if isinstance(month, str):
            month = pd.to_datetime(month)
            
        # Filter data
        filtered_df = df[
            (df['Zone'] == zone_group) & 
            (df['Month'] <= month)
        ]
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"Error querying UDC hourly data: {e}")
        return pd.DataFrame()

def query_health_data(
    health_metric: str,
    level: str,
    zone_group: str,
    corridor: Optional[str] = None,
    month: Optional[Union[str, datetime]] = None,
    connection = None
) -> pd.DataFrame:
    """
    Query health data (ops, maint, safety)
    
    Args:
        health_metric: One of 'ops', 'maint', 'safety'
        level: One of 'corridor', 'subcorridor', 'signal'
        zone_group: Zone group filter
        corridor: Corridor filter
        month: Month filter
        connection: Database connection
    
    Returns:
        DataFrame with health data
    """
    try:
        per = "mo"
        
        mr_map = {
            "corridor": "sub",
            "subcorridor": "sub", 
            "signal": "sig"
        }
        mr_ = mr_map.get(level)
        
        tab = health_metric
        table = f"{mr_}_{per}_{tab}"
        
        # Build where clause
        if (level in ["corridor", "subcorridor"]) and ("RTOP" in zone_group or zone_group == "Zone 7"):
            if zone_group == "All RTOP":
                zones = ["All RTOP", "RTOP1", "RTOP2"] + RTOP1_ZONES + RTOP2_ZONES
            elif zone_group == "RTOP1":
                zones = ["All RTOP", "RTOP1"] + RTOP1_ZONES
            elif zone_group == "RTOP2":
                zones = ["All RTOP", "RTOP2"] + RTOP2_ZONES
            elif zone_group == "Zone 7":
                zones = ["Zone 7m", "Zone 7d"]
            
            zones_str = "', '".join(zones)
            where_clause = f"WHERE Zone_Group in ('{zones_str}')"
            
        elif (level in ["corridor", "subcorridor"]) and corridor == "All Corridors":
            where_clause = f"WHERE Zone_Group = '{zone_group}'"
            
        else:
            where_clause = f"WHERE Corridor = '{corridor}'"
        
        if month:
            if isinstance(month, str):
                month = pd.to_datetime(month)
            where_clause += f" AND Month = '{month.strftime('%Y-%m-%d')}'"
        
        query = f"SELECT * FROM {table} {where_clause}"
        
        df = pd.read_sql(query, connection)
        df['Month'] = pd.to_datetime(df['Month'])
        
        return df
        
    except Exception as e:
        logger.error(f"Error querying health data: {e}")
        return pd.DataFrame()

def create_aurora_partitioned_table(aurora_engine, table_name: str, period_field: str = "Timeperiod"):
    """
    Create partitioned table in Aurora
    
    Args:
        aurora_engine: Aurora database engine
        table_name: Name of table to create
        period_field: Field to partition on
    """
    try:
        # Generate partition dates
        start_date = datetime.now() - relativedelta(months=10)
        end_date = datetime.now() + relativedelta(months=1)
        
        months = pd.date_range(start_date, end_date, freq='MS')
        new_partition_dates = [month.strftime('%Y-%m-01') for month in months]
        new_partition_names = [month.strftime('p_%Y%m') for month in months]
        
        partitions = [f"PARTITION {name} VALUES LESS THAN ('{date} 00:00:00')," 
                     for name, date in zip(new_partition_names, new_partition_dates)]
        
        # Determine variable name based on table suffix
        table_suffix = table_name.split('_')[-1]
        var_map = {
            'aogh': 'aog',
            'vph': 'vol' if period_field == 'Timeperiod' else 'vph',
            'paph': 'vol' if period_field == 'Timeperiod' else 'paph',
            'prh': 'pr',
            'qsh': 'qs_freq',
            'sfh': 'sf_freq'
        }
        var = var_map.get(table_suffix, 'value')
        
        create_stmt = f"""
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
        
        with aurora_engine.connect() as conn:
            conn.execute(text(create_stmt))
            conn.commit()
            
        logger.info(f"Successfully created partitioned table {table_name}_part")
        
    except Exception as e:
        logger.error(f"Error creating partitioned table: {e}")
        raise

def get_aurora_partitions(aurora_engine, table_name: str) -> Optional[List[str]]:
    """
    Get list of partitions for Aurora table
    
    Args:
        aurora_engine: Aurora database engine
        table_name: Table name
    
    Returns:
        List of partition names or None
    """
    try:
        query = f"""
        SELECT PARTITION_NAME FROM information_schema.partitions
        WHERE TABLE_NAME = '{table_name}'
        """
        
        with aurora_engine.connect() as conn:
            result = conn.execute(text(query))
            partitions = [row[0] for row in result.fetchall()]
            
        return partitions if len(partitions) > 1 else None
        
    except Exception as e:
        logger.error(f"Error getting Aurora partitions: {e}")
        return None

def add_aurora_partition(aurora_engine, table_name: str):
    """
    Add new partition to Aurora table
    
    Args:
        aurora_engine: Aurora database engine
        table_name: Table name
    """
    try:
        next_month = datetime.now() + relativedelta(months=1)
        new_partition_date = next_month.strftime('%Y-%m-01')
        new_partition_name = next_month.strftime('p_%Y%m')
        
        existing_partitions = get_aurora_partitions(aurora_engine, table_name)
        
        if existing_partitions and new_partition_name not in existing_partitions:
            statement = f"""
            ALTER TABLE {table_name}
            REORGANIZE PARTITION future INTO (
                PARTITION {new_partition_name} VALUES LESS THAN ('{new_partition_date} 00:00:00'),
                PARTITION future VALUES LESS THAN MAXVALUE)
            """
            
            with aurora_engine.connect() as conn:
                conn.execute(text(statement))
                conn.commit()
                
            logger.info(f"Successfully added partition {new_partition_name} to {table_name}")
            
    except Exception as e:
        logger.error(f"Error adding Aurora partition: {e}")

def drop_aurora_partitions(aurora_engine, table_name: str, months_to_keep: int = 8):
    """
    Drop old partitions from Aurora table
    
    Args:
        aurora_engine: Aurora database engine
        table_name: Table name
        months_to_keep: Number of months of data to keep
    """
    try:
        existing_partitions = get_aurora_partitions(aurora_engine, table_name)
        
        if existing_partitions:
            existing_partitions = [p for p in existing_partitions if p != "future"]
            drop_before_date = datetime.now() - relativedelta(months=months_to_keep)
            drop_partition_name = drop_before_date.strftime('p_%Y%m')
            
            drop_partition_names = [p for p in existing_partitions if p <= drop_partition_name]
            
            for partition_name in drop_partition_names:
                statement = f"ALTER TABLE {table_name} DROP PARTITION {partition_name};"
                
                with aurora_engine.connect() as conn:
                    conn.execute(text(statement))
                    conn.commit()
                    
                logger.info(f"Dropped partition {partition_name} from {table_name}")
                
    except Exception as e:
        logger.error(f"Error dropping Aurora partitions: {e}")

@contextmanager
def database_connection(connection_type: str, **kwargs):
    """
    Context manager for database connections
    
    Args:
        connection_type: Type of connection ('atspm', 'maxview', 'aurora', 'athena')
        **kwargs: Additional arguments for connection functions
    
    Yields:
        Database connection
    """
    conn = None
    try:
        if connection_type == 'atspm':
            conn = get_atspm_connection(kwargs.get('conf_atspm'))
        elif connection_type == 'maxview':
            conn = get_maxview_connection(kwargs.get('dsn', 'maxview'))
        elif connection_type == 'maxview_eventlog':
            conn = get_maxview_eventlog_connection()
        elif connection_type == 'aurora':
            conn = get_aurora_connection(kwargs.get('load_data_local_infile', False))
        elif connection_type == 'athena':
            conn = get_athena_connection(kwargs.get('conf_athena'))
        else:
            raise ValueError(f"Unknown connection type: {connection_type}")
            
        yield conn
        
    finally:
        if conn and hasattr(conn, 'close'):
            conn.close()

def close_all_connections():
    """Close all database connections"""
    try:
        # This is a placeholder - in practice, you might want to track active connections
        # and close them explicitly, or use connection pooling
        logger.info("Closing all database connections")
        
    except Exception as e:
        logger.error(f"Error closing connections: {e}")

# Connection pool management (equivalent to R's connection pools)
class ConnectionManager:
    """Manage database connection pools"""
    
    def __init__(self):
        self._connections = {}
        
    def get_connection(self, connection_type: str, **kwargs):
        """Get a pooled connection"""
        key = f"{connection_type}_{hash(str(sorted(kwargs.items())))}"
        
        if key not in self._connections:
            if connection_type == 'aurora':
                self._connections[key] = get_aurora_connection(**kwargs)
            # Add other connection types as needed
            
        return self._connections[key]
    
    def close_all(self):
        """Close all pooled connections"""
        for conn in self._connections.values():
            if hasattr(conn, 'dispose'):
                conn.dispose()
            elif hasattr(conn, 'close'):
                conn.close()
        self._connections.clear()

# Global connection manager instance
connection_manager = ConnectionManager()

# Convenience functions for getting pooled connections
def get_sigops_connection_pool():
    """Get pooled connection to SigOps database (Aurora)"""
    return connection_manager.get_connection('aurora')

def execute_athena_query(query: str, conf_athena: Dict[str, Any], 
                        wait_for_completion: bool = True) -> Optional[str]:
    """
    Execute Athena query and optionally wait for completion
    
    Args:
        query: SQL query to execute
        conf_athena: Athena configuration
        wait_for_completion: Whether to wait for query completion
    
    Returns:
        Query execution ID or None if failed
    """
    try:
        client = get_athena_connection(conf_athena)
        
        response = client.start_query_execution(
            QueryString=query,
            ResultConfiguration={'OutputLocation': conf_athena['staging_dir']},
            WorkGroup='primary'
        )
        
        query_execution_id = response['QueryExecutionId']
        
        if wait_for_completion:
            while True:
                response = client.get_query_execution(QueryExecutionId=query_execution_id)
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    logger.info(f"Athena query completed successfully: {query_execution_id}")
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Athena query failed: {error_msg}")
                    return None
                else:
                    import time
                    time.sleep(1)
        
        return query_execution_id
        
    except Exception as e:
        logger.error(f"Error executing Athena query: {e}")
        return None

def get_athena_query_results(query_execution_id: str, conf_athena: Dict[str, Any]) -> pd.DataFrame:
    """
    Get results from completed Athena query
    
    Args:
        query_execution_id: Query execution ID
        conf_athena: Athena configuration
    
    Returns:
        DataFrame with query results
    """
    try:
        client = get_athena_connection(conf_athena)
        
        response = client.get_query_results(QueryExecutionId=query_execution_id)
        
        # Parse results into DataFrame
        columns = [col['Label'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row in response['ResultSet']['Rows'][1:]:  # Skip header row
            row_data = [field.get('VarCharValue', '') for field in row['Data']]
            rows.append(row_data)
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Handle pagination if there are more results
        while 'NextToken' in response:
            response = client.get_query_results(
                QueryExecutionId=query_execution_id,
                NextToken=response['NextToken']
            )
            
            for row in response['ResultSet']['Rows']:
                row_data = [field.get('VarCharValue', '') for field in row['Data']]
                rows.append(row_data)
        
        logger.info(f"Retrieved {len(df)} rows from Athena query {query_execution_id}")
        return df
        
    except Exception as e:
        logger.error(f"Error getting Athena query results: {e}")
        return pd.DataFrame()

def validate_database_connections(connections_to_test: List[str] = None) -> Dict[str, bool]:
    """
    Validate database connections before running scripts
    
    Args:
        connections_to_test: List of connection types to test
    
    Returns:
        Dictionary with connection test results
    """
    if connections_to_test is None:
        connections_to_test = ['maxview', 'aurora', 'athena']
    
    results = {}
    
    for conn_type in connections_to_test:
        try:
            logger.info(f"Testing {conn_type} connection...")
            
            if conn_type == 'maxview':
                with database_connection('maxview') as conn:
                    # Simple test query
                    test_df = pd.read_sql("SELECT 1 as test", conn)
                    results[conn_type] = len(test_df) == 1
                    
            elif conn_type == 'aurora':
                engine = get_aurora_connection()
                with engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    results[conn_type] = len(list(result)) == 1
                    
            elif conn_type == 'athena':
                # Test Athena connection by listing databases
                client = boto3.client('athena')
                response = client.list_databases(CatalogName='AwsDataCatalog')
                results[conn_type] = 'DatabaseList' in response
                
            else:
                logger.warning(f"Unknown connection type: {conn_type}")
                results[conn_type] = False
                
            if results[conn_type]:
                logger.info(f"✓ {conn_type} connection successful")
            else:
                logger.error(f"✗ {conn_type} connection failed")
                
        except Exception as e:
            logger.error(f"✗ {conn_type} connection failed: {e}")
            results[conn_type] = False
    
    return results

def get_database_info(connection_type: str = 'maxview') -> Dict[str, Any]:
    """
    Get database information and available databases
    
    Args:
        connection_type: Type of database connection
    
    Returns:
        Dictionary with database information
    """
    try:
        info = {}
        
        if connection_type == 'maxview':
            # Get available MaxView databases
            uid = os.getenv('MAXV_USERNAME')
            pwd = os.getenv('MAXV_PASSWORD')
            
            # Read ODBC configuration
            import configparser
            config = configparser.ConfigParser()
            odbc_ini_path = os.path.expanduser('~/.odbc.ini')
            
            if os.path.exists(odbc_ini_path):
                config.read(odbc_ini_path)
                db_config = dict(config['maxview'])
            else:
                # Fallback configuration
                db_config = {
                    'driver': 'ODBC Driver 17 for SQL Server',
                    'server': 'your_server',
                    'port': '1433'
                }
            
            # Connect without specifying database to list all databases
            conn_str = f"DRIVER={db_config['driver']};SERVER={db_config['server']};PORT={db_config['port']};UID={uid};PWD={pwd}"
            
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT name FROM sys.databases')
                databases = [row[0] for row in cursor.fetchall()]
            
            # Filter for MaxView databases using regex
            import re
            maxview_pattern = re.compile(r"MaxView_\d.*")
            eventlog_pattern = re.compile(r"MaxView_EventLog_\d.*")
            
            maxview_dbs = [db for db in databases if maxview_pattern.match(db)]
            eventlog_dbs = [db for db in databases if eventlog_pattern.match(db)]
            
            info['maxview_databases'] = maxview_dbs
            info['eventlog_databases'] = eventlog_dbs
            
            # Get the latest databases
            if maxview_dbs:
                info['latest_maxview'] = max(maxview_dbs)
            if eventlog_dbs:
                info['latest_eventlog'] = max(eventlog_dbs)
                
        elif connection_type == 'aurora':
            engine = get_aurora_connection()
            with engine.connect() as conn:
                # Get database version
                result = conn.execute(text("SELECT VERSION() as version"))
                info['version'] = list(result)[0][0]
                
                # Get available tables
                result = conn.execute(text("SHOW TABLES"))
                info['tables'] = [row[0] for row in result]
                
        return info
        
    except Exception as e:
        logger.error(f"Error getting database info: {e}")
        return {}

def backup_table(source_table: str, backup_suffix: str = None, 
                 connection_type: str = 'aurora') -> bool:
    """
    Create backup of database table
    
    Args:
        source_table: Name of table to backup
        backup_suffix: Suffix for backup table name
        connection_type: Type of database connection
    
    Returns:
        True if backup successful, False otherwise
    """
    try:
        if backup_suffix is None:
            backup_suffix = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        backup_table_name = f"{source_table}_backup_{backup_suffix}"
        
        if connection_type == 'aurora':
            engine = get_aurora_connection()
            with engine.connect() as conn:
                # Create backup table
                create_stmt = f"CREATE TABLE {backup_table_name} AS SELECT * FROM {source_table}"
                conn.execute(text(create_stmt))
                conn.commit()
                
                # Verify backup
                result = conn.execute(text(f"SELECT COUNT(*) FROM {backup_table_name}"))
                backup_count = list(result)[0][0]
                
                result = conn.execute(text(f"SELECT COUNT(*) FROM {source_table}"))
                source_count = list(result)[0][0]
                
                if backup_count == source_count:
                    logger.info(f"Successfully backed up {source_table} to {backup_table_name} ({backup_count} rows)")
                    return True
                else:
                    logger.error(f"Backup verification failed: {source_count} source rows vs {backup_count} backup rows")
                    return False
                    
        return False
        
    except Exception as e:
        logger.error(f"Error backing up table {source_table}: {e}")
        return False

def optimize_table_performance(table_name: str, connection_type: str = 'aurora'):
    """
    Optimize table performance (analyze, optimize indexes, etc.)
    
    Args:
        table_name: Name of table to optimize
        connection_type: Type of database connection
    """
    try:
        if connection_type == 'aurora':
            engine = get_aurora_connection()
            with engine.connect() as conn:
                # Analyze table
                conn.execute(text(f"ANALYZE TABLE {table_name}"))
                
                # Optimize table
                conn.execute(text(f"OPTIMIZE TABLE {table_name}"))
                
                logger.info(f"Optimized table {table_name}")
                
    except Exception as e:
        logger.error(f"Error optimizing table {table_name}: {e}")

def get_table_statistics(table_name: str, connection_type: str = 'aurora') -> Dict[str, Any]:
    """
    Get statistics about a database table
    
    Args:
        table_name: Name of table
        connection_type: Type of database connection
    
    Returns:
        Dictionary with table statistics
    """
    try:
        stats = {}
        
        if connection_type == 'aurora':
            engine = get_aurora_connection()
            with engine.connect() as conn:
                # Get row count
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                stats['row_count'] = list(result)[0][0]
                
                # Get table size
                size_query = f"""
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'size_mb'
                FROM information_schema.tables 
                WHERE table_name = '{table_name}'
                """
                result = conn.execute(text(size_query))
                size_result = list(result)
                if size_result:
                    stats['size_mb'] = size_result[0][0]
                
                # Get column information
                columns_query = f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                """
                result = conn.execute(text(columns_query))
                stats['columns'] = [
                    {'name': row[0], 'type': row[1], 'nullable': row[2]}
                    for row in result
                ]
                
        return stats
        
    except Exception as e:
        logger.error(f"Error getting table statistics for {table_name}: {e}")
        return {}

# Export commonly used functions and classes
__all__ = [
    'get_atspm_connection',
    'get_maxview_connection', 
    'get_maxview_eventlog_connection',
    'get_cel_connection',
    'get_aurora_connection',
    'get_athena_connection',
    'mydb_append_table',
    'add_athena_partition',
    'query_data',
    'query_udc_trend',
    'query_udc_hourly',
    'query_health_data',
    'create_aurora_partitioned_table',
    'get_aurora_partitions',
    'add_aurora_partition',
    'drop_aurora_partitions',
    'database_connection',
    'close_all_connections',
    'ConnectionManager',
    'connection_manager',
    'get_sigops_connection_pool',
    'execute_athena_query',
    'get_athena_query_results',
    'validate_database_connections',
    'get_database_info',
    'backup_table',
    'optimize_table_performance',
    'get_table_statistics'
]

