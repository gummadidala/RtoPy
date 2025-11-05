"""
PostgreSQL Database Wrapper
Provides robust database operations with connection pooling, error handling, and logging.
"""

import os
import logging
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DisconnectionError
from contextlib import contextmanager
from typing import Optional, Dict, List, Any, Union, Tuple
import psycopg2
from psycopg2 import OperationalError as Psycopg2OperationalError
import time
from datetime import datetime, date
import configparser
from urllib.parse import quote_plus

# Setup logging
logger = logging.getLogger(__name__)

class PostgresWrapper:
    """
    Robust PostgreSQL database wrapper with connection pooling and error handling
    """
    
    def __init__(self, 
                 host: str = None,
                 port: int = 5432,
                 database: str = None,
                 username: str = None,
                 password: str = None,
                 connection_string: str = None,
                 pool_size: int = 5,
                 max_overflow: int = 10,
                 pool_timeout: int = 30,
                 pool_recycle: int = 3600,
                 echo: bool = False,
                 config_file: str = None):
        """
        Initialize PostgreSQL wrapper
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            username: Database username
            password: Database password
            connection_string: Full connection string (overrides individual params)
            pool_size: Size of connection pool
            max_overflow: Maximum overflow connections
            pool_timeout: Timeout for getting connection from pool
            pool_recycle: Recycle connections after this many seconds
            echo: Enable SQL logging
            config_file: Path to config file with database credentials
        """
        
        self.engine = None
        self.metadata = None
        self._connection_params = {}
        
        # Load configuration
        if config_file:
            self._load_config_file(config_file)
        elif connection_string:
            self._connection_string = connection_string
        else:
            self._connection_params = {
                'host': host or os.getenv('POSTGRES_HOST', 'localhost'),
                'port': port or int(os.getenv('POSTGRES_PORT', '5432')),
                'database': database or os.getenv('POSTGRES_DB'),
                'username': username or os.getenv('POSTGRES_USER'),
                'password': password or os.getenv('POSTGRES_PASSWORD')
            }
        
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool_timeout = pool_timeout
        self.pool_recycle = pool_recycle
        self.echo = echo
        
        # Initialize connection
        self._create_engine()
        
    def _load_config_file(self, config_file: str):
        """Load database configuration from file"""
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            
            # Assume 'postgresql' section exists
            pg_config = config['postgresql']
            self._connection_params = {
                'host': pg_config.get('host', 'localhost'),
                'port': int(pg_config.get('port', '5432')),
                'database': pg_config.get('database'),
                'username': pg_config.get('username'),
                'password': pg_config.get('password')
            }
            
        except Exception as e:
            logger.error(f"Error loading config file {config_file}: {e}")
            raise
    
    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        try:
            if hasattr(self, '_connection_string'):
                connection_url = self._connection_string
            else:
                # Validate required parameters
                required_params = ['host', 'database', 'username', 'password']
                missing_params = [p for p in required_params if not self._connection_params.get(p)]
                if missing_params:
                    raise ValueError(f"Missing required connection parameters: {missing_params}")
                
                # URL encode password to handle special characters
                encoded_password = quote_plus(self._connection_params['password'])
                
                connection_url = (
                    f"postgresql://{self._connection_params['username']}:"
                    f"{encoded_password}@{self._connection_params['host']}:"
                    f"{self._connection_params['port']}/{self._connection_params['database']}"
                )
            
            self.engine = create_engine(
                connection_url,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                pool_pre_ping=True,  # Verify connections before use
                echo=self.echo,
                connect_args={
                    "connect_timeout": 10,
                    "application_name": "RtoPy"
                }
            )
            
            # Test connection
            self.test_connection()
            logger.info("PostgreSQL engine created successfully")
            
        except Exception as e:
            logger.error(f"Error creating PostgreSQL engine: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except (OperationalError, DisconnectionError) as e:
            logger.warning(f"Connection error, attempting to reconnect: {e}")
            # Attempt to recreate engine
            self._create_engine()
            if connection:
                connection.close()
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, 
                     query: str, 
                     params: Dict = None, 
                     fetch: bool = True,
                     retry_count: int = 3) -> Optional[List[Dict]]:
        """
        Execute SQL query with error handling and retries
        
        Args:
            query: SQL query string
            params: Query parameters
            fetch: Whether to fetch results
            retry_count: Number of retry attempts
            
        Returns:
            Query results as list of dictionaries or None
        """
        
        for attempt in range(retry_count):
            try:
                with self.get_connection() as conn:
                    result = conn.execute(text(query), params or {})
                    
                    if fetch:
                        # Convert to list of dictionaries
                        rows = result.fetchall()
                        columns = result.keys()
                        return [dict(zip(columns, row)) for row in rows]
                    else:
                        conn.commit()
                        return None
                        
            except (OperationalError, DisconnectionError, Psycopg2OperationalError) as e:
                logger.warning(f"Database error on attempt {attempt + 1}: {e}")
                if attempt == retry_count - 1:
                    logger.error(f"Query failed after {retry_count} attempts")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                logger.error(f"Unexpected error executing query: {e}")
                raise
    
    def read_sql(self, 
                 query: str, 
                 params: Dict = None,
                 chunksize: int = None,
                 dtype: Dict = None,
                 parse_dates: List = None) -> pd.DataFrame:
        """
        Read SQL query into pandas DataFrame
        
        Args:
            query: SQL query string
            params: Query parameters
            chunksize: Number of rows per chunk
            dtype: Data types for columns
            parse_dates: Columns to parse as dates
            
        Returns:
            pandas DataFrame
        """
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(
                    sql=text(query),
                    con=conn,
                    params=params or {},
                    chunksize=chunksize,
                    dtype=dtype,
                    parse_dates=parse_dates
                )
                
                if chunksize:
                    # Concatenate chunks
                    df = pd.concat(df, ignore_index=True)
                
                logger.info(f"Successfully read {len(df)} rows from database")
                return df
                
        except Exception as e:
            logger.error(f"Error reading SQL query: {e}")
            raise
    
    def write_dataframe(self, 
                       df: pd.DataFrame, 
                       table_name: str,
                       if_exists: str = 'append',
                       index: bool = False,
                       chunksize: int = 10000,
                       method: str = 'multi',
                       schema: str = None) -> bool:
        """
        Write pandas DataFrame to database table
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            index: Whether to write DataFrame index
            chunksize: Number of rows per chunk
            method: Write method ('multi' for bulk insert)
            schema: Database schema name
            
        Returns:
            Success boolean
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to write")
                return True
            
            # Convert date columns to string to avoid timezone issues
            date_columns = df.select_dtypes(include=['datetime64', 'datetime']).columns
            df_copy = df.copy()
            for col in date_columns:
                df_copy[col] = df_copy[col].astype(str)
            
            with self.get_connection() as conn:
                rows_written = df_copy.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists=if_exists,
                    index=index,
                    chunksize=chunksize,
                    method=method,
                    schema=schema
                )
                
                logger.info(f"Successfully wrote {len(df_copy)} rows to {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error writing DataFrame to {table_name}: {e}")
            return False
    
    def bulk_insert(self, 
                   table_name: str, 
                   data: List[Dict],
                   schema: str = None,
                   on_conflict: str = 'ignore') -> bool:
        """
        Perform bulk insert using raw SQL
        
        Args:
            table_name: Target table name
            data: List of dictionaries with data
            schema: Database schema
            on_conflict: What to do on conflict ('ignore', 'update')
            
        Returns:
            Success boolean
        """
        try:
            if not data:
                logger.warning("No data provided for bulk insert")
                return True
            
            # Get column names from first record
            columns = list(data[0].keys())
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Build INSERT query
            columns_str = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])
            
            if on_conflict == 'ignore':
                query = f"""
                INSERT INTO {full_table_name} ({columns_str}) 
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
                """
            else:
                query = f"""
                INSERT INTO {full_table_name} ({columns_str}) 
                VALUES ({placeholders})
                """
            
            with self.get_connection() as conn:
                result = conn.execute(text(query), data)
                conn.commit()
                
                logger.info(f"Bulk inserted {len(data)} rows into {table_name}")
                return True
                
        except Exception as e:
            logger.error(f"Error in bulk insert to {table_name}: {e}")
            return False
    
    def table_exists(self, table_name: str, schema: str = None) -> bool:
        """Check if table exists"""
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names(schema=schema)
            return table_name in tables
        except Exception as e:
            logger.error(f"Error checking if table {table_name} exists: {e}")
            return False
    
    def get_table_info(self, table_name: str, schema: str = None) -> Dict:
        """Get table information including columns and types"""
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name, schema=schema)
            
            return {
                'columns': [col['name'] for col in columns],
                'column_info': columns,
                'primary_keys': inspector.get_pk_constraint(table_name, schema=schema),
                'foreign_keys': inspector.get_foreign_keys(table_name, schema=schema),
                'indexes': inspector.get_indexes(table_name, schema=schema)
            }
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {e}")
            return {}
    
    def get_row_count(self, table_name: str, schema: str = None, where_clause: str = None) -> int:
        """Get row count for table"""
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            query = f"SELECT COUNT(*) as count FROM {full_table_name}"
            
            if where_clause:
                query += f" WHERE {where_clause}"
            
            result = self.execute_query(query)
            return result[0]['count'] if result else 0
            
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0
    
    def truncate_table(self, table_name: str, schema: str = None, cascade: bool = False) -> bool:
        """Truncate table"""
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            cascade_str = " CASCADE" if cascade else ""
            query = f"TRUNCATE TABLE {full_table_name}{cascade_str}"
            
            self.execute_query(query, fetch=False)
            logger.info(f"Successfully truncated table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")
            return False
    
    def create_indexes(self, table_name: str, indexes: List[Dict], schema: str = None) -> bool:
        """
        Create indexes on table
        
        Args:
            table_name: Target table name
            indexes: List of index definitions [{'name': 'idx_name', 'columns': ['col1', 'col2'], 'unique': False}]
            schema: Database schema
            
        Returns:
            Success boolean
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            for index_def in indexes:
                index_name = index_def['name']
                columns = index_def['columns']
                unique = index_def.get('unique', False)
                
                unique_str = "UNIQUE " if unique else ""
                columns_str = ", ".join(columns)
                
                query = f"""
                CREATE {unique_str}INDEX IF NOT EXISTS {index_name} 
                ON {full_table_name} ({columns_str})
                """
                
                self.execute_query(query, fetch=False)
                logger.info(f"Created index {index_name} on {table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error creating indexes on {table_name}: {e}")
            return False
    
    def upsert_dataframe(self, 
                        df: pd.DataFrame, 
                        table_name: str,
                        conflict_columns: List[str],
                        update_columns: List[str] = None,
                        schema: str = None,
                        chunksize: int = 1000) -> bool:
        """
        Upsert DataFrame using PostgreSQL's ON CONFLICT
        
        Args:
            df: DataFrame to upsert
            table_name: Target table name
            conflict_columns: Columns that define conflicts
            update_columns: Columns to update on conflict (if None, updates all except conflict columns)
            schema: Database schema
            chunksize: Number of rows per batch
            
        Returns:
            Success boolean
        """
        try:
            if df.empty:
                logger.warning("DataFrame is empty, nothing to upsert")
                return True
            
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            
            # Get columns to update
            if update_columns is None:
                update_columns = [col for col in df.columns if col not in conflict_columns]
            
            # Build upsert query
            columns = list(df.columns)
            columns_str = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])
            conflict_str = ", ".join(conflict_columns)
            
            update_sets = []
            for col in update_columns:
                update_sets.append(f"{col} = EXCLUDED.{col}")
            update_str = ", ".join(update_sets)
            
            query = f"""
            INSERT INTO {full_table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}
            """
            
            # Process in chunks
            total_rows = 0
            for i in range(0, len(df), chunksize):
                chunk = df.iloc[i:i+chunksize]
                chunk_data = chunk.to_dict('records')
                
                with self.get_connection() as conn:
                    conn.execute(text(query), chunk_data)
                    conn.commit()
                
                total_rows += len(chunk)
                logger.debug(f"Upserted chunk {i//chunksize + 1}, total rows: {total_rows}")
            
            logger.info(f"Successfully upserted {total_rows} rows to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error upserting DataFrame to {table_name}: {e}")
            return False
    
    def delete_by_condition(self, 
                           table_name: str, 
                           where_clause: str, 
                           params: Dict = None,
                           schema: str = None) -> int:
        """
        Delete rows by condition
        
        Args:
            table_name: Target table name
            where_clause: WHERE condition
            params: Parameters for WHERE clause
            schema: Database schema
            
        Returns:
            Number of deleted rows
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            query = f"DELETE FROM {full_table_name} WHERE {where_clause}"
            
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                conn.commit()
                deleted_count = result.rowcount
            
            logger.info(f"Deleted {deleted_count} rows from {table_name}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error deleting from {table_name}: {e}")
            return 0
    
    def execute_transaction(self, queries: List[Tuple[str, Dict]]) -> bool:
        """
        Execute multiple queries in a transaction
        
        Args:
            queries: List of (query, params) tuples
            
        Returns:
            Success boolean
        """
        try:
            with self.get_connection() as conn:
                trans = conn.begin()
                try:
                    for query, params in queries:
                        conn.execute(text(query), params or {})
                    trans.commit()
                    logger.info(f"Successfully executed transaction with {len(queries)} queries")
                    return True
                except Exception as e:
                    trans.rollback()
                    logger.error(f"Transaction rolled back due to error: {e}")
                    raise
                    
        except Exception as e:
            logger.error(f"Error executing transaction: {e}")
            return False
    
    def copy_from_csv(self, 
                     table_name: str, 
                     csv_file_path: str,
                     delimiter: str = ',',
                     null_string: str = '',
                     columns: List[str] = None,
                     schema: str = None) -> bool:
        """
        Use PostgreSQL COPY command for fast CSV import
        
        Args:
            table_name: Target table name
            csv_file_path: Path to CSV file
            delimiter: CSV delimiter
            null_string: String representing NULL values
            columns: List of columns to import
            schema: Database schema
            
        Returns:
            Success boolean
        """
        try:
            import psycopg2
            
            # Get raw psycopg2 connection
            raw_conn = self.engine.raw_connection()
            cursor = raw_conn.cursor()
            
            full_table_name = f"{schema}.{table_name}" if schema else table_name
            columns_str = f"({', '.join(columns)})" if columns else ""
            
            with open(csv_file_path, 'r') as f:
                cursor.copy_expert(
                    f"""COPY {full_table_name}{columns_str} 
                        FROM STDIN WITH CSV HEADER 
                        DELIMITER '{delimiter}' 
                        NULL '{null_string}'""",
                    f
                )
            
            raw_conn.commit()
            cursor.close()
            raw_conn.close()
            
            logger.info(f"Successfully copied CSV data to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error copying CSV to {table_name}: {e}")
            return False
    
    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        try:
            stats_query = """
            SELECT 
                schemaname,
                tablename,
                n_tup_ins as inserts,
                n_tup_upd as updates,
                n_tup_del as deletes,
                n_live_tup as live_tuples,
                n_dead_tup as dead_tuples,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            ORDER BY schemaname, tablename
            """
            
            result = self.execute_query(stats_query)
            return result
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def vacuum_analyze(self, table_name: str = None, schema: str = None) -> bool:
        """Run VACUUM ANALYZE on table or entire database"""
        try:
            if table_name:
                full_table_name = f"{schema}.{table_name}" if schema else table_name
                query = f"VACUUM ANALYZE {full_table_name}"
            else:
                query = "VACUUM ANALYZE"
            
            # VACUUM cannot run in a transaction
            with self.engine.connect() as conn:
                conn.execute(text("COMMIT"))  # End any existing transaction
                conn.execute(text(query))
            
            logger.info(f"Successfully ran VACUUM ANALYZE on {table_name or 'database'}")
            return True
            
        except Exception as e:
            logger.error(f"Error running VACUUM ANALYZE: {e}")
            return False
    
    def close(self):
        """Close database engine and connections"""
        try:
            if self.engine:
                self.engine.dispose()
                logger.info("Database engine closed")
        except Exception as e:
            logger.error(f"Error closing database engine: {e}")
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Utility functions for common operations
def create_postgres_wrapper_from_config(config_file: str = None) -> PostgresWrapper:
    """
    Create PostgresWrapper instance from configuration file
    
    Args:
        config_file: Path to configuration file
        
    Returns:
        PostgresWrapper instance
    """
    try:
        if config_file is None:
            # Look for config in common locations
            config_locations = [
                os.path.expanduser('~/.rtopy.ini'),
                os.path.join(os.getcwd(), 'config.ini'),
                '/etc/rtopy/config.ini'
            ]
            
            for location in config_locations:
                if os.path.exists(location):
                    config_file = location
                    break
        
        if config_file and os.path.exists(config_file):
            return PostgresWrapper(config_file=config_file)
        else:
            # Use environment variables
            return PostgresWrapper()
            
    except Exception as e:
        logger.error(f"Error creating PostgresWrapper from config: {e}")
        raise


def batch_process_dataframes(wrapper: PostgresWrapper,
                           dataframes: List[Tuple[pd.DataFrame, str]],
                           if_exists: str = 'append',
                           schema: str = None) -> Dict[str, bool]:
    """
    Batch process multiple DataFrames to database
    
    Args:
        wrapper: PostgresWrapper instance
        dataframes: List of (dataframe, table_name) tuples
        if_exists: What to do if table exists
        schema: Database schema
        
    Returns:
        Dictionary with results for each table
    """
    results = {}
    
    for df, table_name in dataframes:
        try:
            success = wrapper.write_dataframe(
                df=df,
                table_name=table_name,
                if_exists=if_exists,
                schema=schema
            )
            results[table_name] = success
            
        except Exception as e:
            logger.error(f"Error processing DataFrame for {table_name}: {e}")
            results[table_name] = False
    
    return results


# Example usage and testing
if __name__ == "__main__":
    # Example configuration
    config_example = """
    [postgresql]
    host = localhost
    port = 5432
    database = rtopy_db
    username = rtopy_user
    password = your_password
    """
    
    # Example usage
    try:
        # Create wrapper instance
        db = PostgresWrapper(
            host='localhost',
            port=5432,
            database='rtopy_db',
            username='rtopy_user',
            password='your_password'
        )
        
        # Test connection
        if db.test_connection():
            print("Database connection successful!")
            
            # Example: Read data
            df = db.read_sql("SELECT * FROM your_table LIMIT 10")
            print(f"Read {len(df)} rows")
            
            # Example: Write data
            sample_data = pd.DataFrame({
                'signal_id': ['001', '002', '003'],
                'timestamp': pd.to_datetime(['2023-01-01', '2023-01-02', '2023-01-03']),
                'value': [1.0, 2.0, 3.0]
            })
            
            success = db.write_dataframe(sample_data, 'test_table')
            print(f"Write operation successful: {success}")
            
            # Example: Get table info
            table_info = db.get_table_info('test_table')
            print(f"Table columns: {table_info.get('columns', [])}")
            
        db.close()
        
    except Exception as e:
        print(f"Error in example usage: {e}")