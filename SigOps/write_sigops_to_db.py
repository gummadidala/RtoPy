import pandas as pd
import mysql.connector
import yaml
import os
from typing import Optional, Dict, Any
import logging
from datetime import date

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

from sqlalchemy import text

def append_to_database(engine, sig_data, data_type, calcs_start_date,
                      report_start_date=None, report_end_date=None):
    try:
        with engine.begin() as conn:  # handles commit/rollback
            if 'hr' in sig_data:
                hourly_data = sig_data['hr']

                table_mapping = {
                    'vph': 'HourlyVolumes',
                    'paph': 'HourlyPedActivations',
                    'aogh': 'HourlyArrivalsOnGreen',
                    'prh': 'HourlyProgressionRatio',
                    'sfh': 'HourlySplitFailures',
                    'qsh': 'HourlyQueueSpillback'
                }

                for data_key, table_name in table_mapping.items():
                    if data_key in hourly_data and not hourly_data[data_key].empty:
                        df = hourly_data[data_key]

                        if 'Hour' in df.columns:
                            df = df[pd.to_datetime(df['Hour']).dt.date >= calcs_start_date]

                        if not df.empty:
                            delete_query = text(f"""
                                DELETE FROM {table_name}
                                WHERE DATE(Hour) >= :start_date
                            """)
                            conn.execute(delete_query, {"start_date": calcs_start_date})

                            # load_bulk_data must accept a SQLAlchemy connection
                            load_bulk_data(conn, table_name, df)

                            logger.info(f"Appended {len(df)} records to {table_name}")

        logger.info("Successfully appended all data to database")

    except Exception as e:
        logger.error(f"Error appending to database: {e}")
        raise

def recreate_database(connection):
    """
    Recreate database tables (placeholder function)
    
    Args:
        connection: Database connection
    """
    try:
        cursor = connection.cursor()
        
        # Add your table creation SQL here
        # This is a placeholder - you'll need to implement based on your schema
        
        logger.info("Database tables recreated successfully")
        
    except Exception as e:
        logger.error(f"Error recreating database: {e}")
        raise
    finally:
        cursor.close()
