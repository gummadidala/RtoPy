import pandas as pd
import mysql.connector
import yaml
import os
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def get_aurora_connection():
    """
    Get connection to Aurora database
    
    Returns:
        Database connection object
    """
    try:
        # Read database configuration
        config_path = "Monthly_Report.yaml"
        with open(config_path, 'r') as file:
            conf = yaml.safe_load(file)
        
        db_config = conf.get('database', {})
        
        connection = mysql.connector.connect(
            host=db_config.get('host'),
            user=db_config.get('user'),
            password=db_config.get('password'),
            database=db_config.get('database'),
            port=db_config.get('port', 3306)
        )
        
        return connection
        
    except Exception as e:
        logger.error(f"Error connecting to Aurora database: {e}")
        raise

def load_bulk_data(connection, table_name: str, df: pd.DataFrame):
    """
    Load DataFrame data into database table
    
    Args:
        connection: Database connection
        table_name: Name of the target table
        df: DataFrame to load
    """
    try:
        cursor = connection.cursor()
        
        # Prepare column names
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        
        # Prepare insert query
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame to list of tuples
        data_tuples = [tuple(row) for row in df.values]
        
        # Execute bulk insert
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        
        logger.info(f"Successfully loaded {len(df)} records into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading bulk data into {table_name}: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()
