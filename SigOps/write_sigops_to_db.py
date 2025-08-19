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

def append_to_database(connection, sig_data: Dict[str, Any], data_type: str, 
                      calcs_start_date: date, report_start_date: Optional[date] = None, 
                      report_end_date: Optional[date] = None):
    """
    Append signal operations data to database
    
    Args:
        connection: Database connection
        sig_data: Dictionary containing signal data
        data_type: Type of data ('sig')
        calcs_start_date: Start date for calculations
        report_start_date: Start date for report (optional)
        report_end_date: End date for report (optional)
    """
    try:
        cursor = connection.cursor()
        
        # Process hourly data
        if 'hr' in sig_data:
            hourly_data = sig_data['hr']
            
            # Table mapping for hourly data
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
                    
                    # Filter data by date if needed
                    if 'Hour' in df.columns:
                        df = df[pd.to_datetime(df['Hour']).dt.date >= calcs_start_date]
                    
                    if not df.empty:
                        # Delete existing data for the date range
                        delete_query = f"""
                        DELETE FROM {table_name} 
                        WHERE DATE(Hour) >= %s
                        """
                        cursor.execute(delete_query, (calcs_start_date,))
                        
                        # Insert new data
                        load_bulk_data(connection, table_name, df)
                        
                        logger.info(f"Appended {len(df)} records to {table_name}")
        
        connection.commit()
        logger.info("Successfully appended all data to database")
        
    except Exception as e:
        logger.error(f"Error appending to database: {e}")
        connection.rollback()
        raise
    finally:
        cursor.close()

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
