"""
write_sigops_to_db.py - Python conversion of write_sigops_to_db.R
Handles writing packaged monthly report data to Aurora/MySQL database
"""

import pandas as pd
import yaml
import json
import logging
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any, List
import traceback

logger = logging.getLogger(__name__)

# Import existing database connection function
# Note: get_aurora_connection is already defined in database_functions.py
# It uses RDS credentials from Monthly_Report_AWS.yaml


def load_bulk_data(conn, table_name: str, df: pd.DataFrame):
    """
    Load DataFrame data into database table - matches R load_bulk_data()
    
    Only loads columns that exist in both the DataFrame and the database table.
    
    Args:
        conn: SQLAlchemy database connection
        table_name: Name of the target table
        df: DataFrame to load
    """
    try:
        from sqlalchemy import text
        
        # Get columns from database table
        result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
        db_columns = [row[0] for row in result]
        
        # Get columns from DataFrame
        df_columns = list(df.columns)
        
        # Find intersection - columns common to both
        common_cols = [col for col in df_columns if col in db_columns]
        
        if not common_cols:
            logger.warning(f"No common columns between DataFrame and {table_name}")
            return
        
        # Select only common columns in DataFrame order (DataFrame order takes precedence)
        df_subset = df[common_cols].copy()
        
        # Use pandas to_sql for efficient bulk insert
        # This is the Python equivalent of mydbAppendTable() in R
        df_subset.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False,
            method='multi',  # Use multi-row INSERT for speed
            chunksize=1000
        )
        
        logger.info(f"Successfully loaded {len(df_subset):,} records into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading bulk data into {table_name}: {e}")
        raise


def round_to_tuesday(date_val):
    """
    Round date to previous Tuesday - matches R round_to_tuesday()
    
    Args:
        date_val: Date to round
    
    Returns:
        Date of previous Tuesday
    """
    if isinstance(date_val, str):
        date_val = pd.to_datetime(date_val).date()
    elif isinstance(date_val, datetime):
        date_val = date_val.date()
    
    # Get day of week (0=Monday, 6=Sunday)
    # Tuesday is 1
    weekday = date_val.weekday()
    
    if weekday == 1:  # Already Tuesday
        return date_val
    elif weekday == 0:  # Monday
        return date_val - timedelta(days=6)
    else:  # Wednesday-Sunday
        days_since_tuesday = (weekday - 1) % 7
        return date_val - timedelta(days=days_since_tuesday)


def write_sigops_to_db(
    conn,
    df: dict,
    dfname: str,
    recreate: bool = False,
    calcs_start_date = None,
    report_start_date = None,
    report_end_date = None
) -> List[str]:
    """
    Write packaged data structure to database - matches R write_sigops_to_db()
    
    Args:
        conn: SQLAlchemy database connection
        df: Nested dict with structure {period: {table: DataFrame}}
        dfname: Base name for tables (e.g., 'cor', 'sub', 'sig')
        recreate: If True, recreate tables from scratch
        calcs_start_date: Start date for calculations
        report_start_date: Start date for report
        report_end_date: End date for report
    
    Returns:
        List of table names created
    """
    from sqlalchemy import text
    
    # Get aggregation periods (qu, mo, wk, dy, ...)
    periods = [key for key in df.keys() if key != 'summary_data']
    
    table_names = []
    
    for period in periods:
        if not isinstance(df[period], dict):
            continue
            
        for table_key in df[period].keys():
            try:
                table_name = f"{dfname}_{period}_{table_key}"
                table_names.append(table_name)
                
                df_table = df[period][table_key]
                
                # Skip empty dataframes
                if not isinstance(df_table, pd.DataFrame) or df_table.empty:
                    logger.debug(f"Skipping empty table: {table_name}")
                    continue
                
                # Make a copy to avoid modifying original
                df_table = df_table.copy()
                
                # Find date field
                date_fields = [col for col in df_table.columns if col in ['Month', 'Date', 'Hour', 'Timeperiod', 'Quarter']]
                datefield = date_fields[0] if len(date_fields) == 1 else None
                
                # Determine start date based on period
                if period == 'wk' and calcs_start_date:
                    start_date = round_to_tuesday(calcs_start_date)
                else:
                    start_date = calcs_start_date
                
                # Sort DataFrame to align with index (speeds up database writes)
                sort_cols = []
                if datefield and datefield in df_table.columns:
                    sort_cols.append(datefield)
                if 'Zone_Group' in df_table.columns:
                    sort_cols.append('Zone_Group')
                if 'Corridor' in df_table.columns:
                    sort_cols.append('Corridor')
                
                if sort_cols:
                    df_table = df_table.sort_values(sort_cols)
                
                # Start transaction using SQLAlchemy's begin()
                with conn.begin():
                    # Handle recreate mode
                    if recreate:
                        logger.info(f"{datetime.now()} Writing {table_name} | 3 | recreate = {recreate}")
                        # Drop table if exists
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                        
                        # Create table with first 3 rows to establish data types, then truncate
                        if len(df_table) >= 3:
                            df_table.head(3).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
                            conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                        
                    else:
                        # Check if table exists
                        result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
                        table_exists = result.fetchone() is not None
                        
                        if table_exists:
                            # Clear head of table prior to report start date
                            if report_start_date and datefield:
                                result = conn.execute(text(f"SELECT MIN(`{datefield}`) AS first_day FROM {table_name}"))
                                row = result.fetchone()
                                
                                if row and row[0]:
                                    db_first_day = row[0]
                                    
                                    if isinstance(db_first_day, str):
                                        db_first_day = pd.to_datetime(db_first_day).date()
                                    
                                    if db_first_day <= pd.to_datetime(report_start_date).date():
                                        logger.info(f"Clearing old data before {report_start_date}")
                                        conn.execute(text(f"DELETE FROM {table_name} WHERE `{datefield}` < :report_start"), 
                                                   {"report_start": report_start_date})
                            
                            # Clear tail prior to append (delete data from start_date onwards)
                            if start_date and datefield:
                                end_date = report_end_date if report_end_date else datetime.now().date()
                                logger.info(f"Clearing data from {start_date} to {end_date}")
                                conn.execute(text(f"DELETE FROM {table_name} WHERE `{datefield}` >= :start_date"), 
                                           {"start_date": start_date})
                            
                            elif not start_date or 'Quarter' in df_table.columns:
                                # No start date or quarterly data - truncate entire table
                                logger.info(f"Truncating {table_name}")
                                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                            
                            # Filter dataframe by date range
                            if start_date and datefield and datefield in df_table.columns:
                                df_table = df_table[pd.to_datetime(df_table[datefield]) >= pd.to_datetime(start_date)]
                            
                            if report_end_date and datefield and datefield in df_table.columns:
                                end_filter = pd.to_datetime(report_end_date) + pd.DateOffset(months=1)
                                df_table = df_table[pd.to_datetime(df_table[datefield]) < end_filter]
                            
                            # Load data
                            if not df_table.empty:
                                logger.info(f"{datetime.now()} Writing {table_name} | {len(df_table):,} rows | recreate = {recreate}")
                                load_bulk_data(conn, table_name, df_table)
                            else:
                                logger.info(f"{table_name} - no data to write after filtering")
                
                # Transaction commits automatically with 'with conn.begin()' context manager
                
            except Exception as e:
                logger.error(f"{datetime.now()} {table_name} {e}")
                logger.error(traceback.format_exc())
                # Rollback is automatic with 'with conn.begin()' on exception
    
    return table_names


def write_to_db_once_off(
    conn,
    df: pd.DataFrame,
    table_name: str,
    recreate: bool = False,
    calcs_start_date = None,
    report_end_date = None
):
    """
    Write a single DataFrame to database - matches R write_to_db_once_off()
    
    Used for special tables like udc_trend_table, hourly_udc, summary_data
    
    Args:
        conn: SQLAlchemy database connection
        df: DataFrame to write
        table_name: Name of the target table
        recreate: If True, recreate table from scratch
        calcs_start_date: Start date for filtering
        report_end_date: End date for filtering
    """
    try:
        from sqlalchemy import text
        
        # Find date field
        date_fields = [col for col in df.columns if col in ['Month', 'Date', 'Hour', 'Timeperiod']]
        datefield = date_fields[0] if len(date_fields) == 1 else None
        
        if recreate:
            logger.info(f"{datetime.now()} Writing {table_name} | 3 | recreate = {recreate}")
            # Create table with first 3 rows
            if len(df) >= 3:
                df.head(3).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
            
        else:
            # Check if table exists
            result = conn.execute(text(f"SHOW TABLES LIKE '{table_name}'"))
            table_exists = result.fetchone() is not None
            
            if table_exists:
                # Filter by dates
                df_filtered = df.copy()
                
                if calcs_start_date and datefield and datefield in df.columns:
                    df_filtered = df_filtered[pd.to_datetime(df_filtered[datefield]) >= pd.to_datetime(calcs_start_date)]
                
                if report_end_date and datefield and datefield in df.columns:
                    end_filter = pd.to_datetime(report_end_date) + pd.DateOffset(months=1)
                    df_filtered = df_filtered[pd.to_datetime(df_filtered[datefield]) < end_filter]
                
                logger.info(f"{datetime.now()} Writing {table_name} | {len(df_filtered):,} rows | recreate = {recreate}")
                
                # Clear table and load data
                conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                load_bulk_data(conn, table_name, df_filtered)
        
    except Exception as e:
        logger.error(f"{datetime.now()} {e}")
        logger.error(traceback.format_exc())
        raise


def convert_to_key_value_df(key: str, data) -> pd.DataFrame:
    """
    Convert data to key-value DataFrame with JSON data column
    Matches R convert_to_key_value_df()
    
    Args:
        key: Key name
        data: Data to convert to JSON
    
    Returns:
        DataFrame with 'key' and 'data' columns
    """
    return pd.DataFrame({
        'key': [key],
        'data': [json.dumps(data, default=str)]
    })


def set_index_aurora(conn, table_name: str):
    """
    Create indexes on Aurora table - matches R set_index_aurora()
    
    Creates three indexes:
    - idx_{table}_zone_period: (Zone_Group, {period})
    - idx_{table}_corridor_period: (Corridor, {period})
    - idx_{table}_unique: UNIQUE ({period}, Zone_Group, Corridor)
    
    Args:
        conn: SQLAlchemy database connection
        table_name: Name of the table
    """
    try:
        from sqlalchemy import text
        
        # Get table fields
        result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
        fields = [row[0] for row in result]
        
        # Find period field
        period_fields = [f for f in fields if f in ['Month', 'Date', 'Hour', 'Timeperiod', 'Quarter']]
        
        if len(period_fields) != 1:
            logger.warning(f"Cannot create indexes for {table_name}: {len(period_fields)} period fields found")
            return
        
        period = period_fields[0]
        
        # Get existing indexes
        result = conn.execute(text(f"SHOW INDEXES FROM {table_name}"))
        existing_indexes = [row[2] for row in result]  # Key_name is column 2
        
        # Create index on Zone_Group and Period
        idx_name = f"idx_{table_name}_zone_period"
        if idx_name not in existing_indexes and 'Zone_Group' in fields:
            logger.info(f"Creating index {idx_name}")
            conn.execute(text(f"CREATE INDEX {idx_name} ON {table_name} (`Zone_Group`, `{period}`)"))
        
        # Create index on Corridor and Period
        idx_name = f"idx_{table_name}_corridor_period"
        if idx_name not in existing_indexes and 'Corridor' in fields:
            logger.info(f"Creating index {idx_name}")
            conn.execute(text(f"CREATE INDEX {idx_name} ON {table_name} (`Corridor`, `{period}`)"))
        
        # Create unique index on Period, Zone_Group, Corridor
        idx_name = f"idx_{table_name}_unique"
        if idx_name not in existing_indexes and 'Zone_Group' in fields and 'Corridor' in fields:
            logger.info(f"Creating unique index {idx_name}")
            conn.execute(text(f"CREATE UNIQUE INDEX {idx_name} ON {table_name} (`{period}`, `Zone_Group`, `Corridor`)"))
        
    except Exception as e:
        logger.error(f"Error creating indexes for {table_name}: {e}")
        # Don't raise - indexes are optional


def add_primary_key_id_field_aurora(conn, table_name: str):
    """
    Add auto-increment primary key 'id' field - matches R add_primary_key_id_field_aurora()
    
    Args:
        conn: SQLAlchemy database connection
        table_name: Name of the table
    """
    try:
        from sqlalchemy import text
        
        logger.info(f"{datetime.now()} Adding primary key to {table_name}")
        
        with conn.begin():
            # Check if 'id' field already exists
            result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
            fields = [row[0] for row in result]
            
            if 'id' not in fields:
                conn.execute(text(
                    f"ALTER TABLE {table_name} "
                    f"ADD id INT UNSIGNED NOT NULL AUTO_INCREMENT, "
                    f"ADD PRIMARY KEY (id)"
                ))
                logger.info(f"Primary key added to {table_name}")
        
    except Exception as e:
        logger.error(f"Error adding primary key to {table_name}: {e}")


def append_to_database(
    conn,
    df: dict,
    dfname: str,
    calcs_start_date = None,
    report_start_date = None,
    report_end_date = None
):
    """
    Append packaged data to database - matches R append_to_database()
    
    This is the main function called from monthly_report_package_2.py
    
    Args:
        conn: SQLAlchemy database connection
        df: Nested dict with cor/sub/sig structure
        dfname: Base name ('cor', 'sub', or 'sig')
        calcs_start_date: Start date for calculations
        report_start_date: Start date for report
        report_end_date: End date for report
    """
    try:
        from sqlalchemy import text
        
        # Set lock timeout for Aurora
        conn.execute(text("SET SESSION innodb_lock_wait_timeout = 50000;"))
        
        # Prep before writing to db (from Health_Metrics.R)
        if 'mo' in df:
            # Add Zone_Group column for health metrics if needed
            if 'maint' in df['mo'] and isinstance(df['mo']['maint'], pd.DataFrame) and not df['mo']['maint'].empty:
                if 'Zone' in df['mo']['maint'].columns and 'Zone_Group' not in df['mo']['maint'].columns:
                    df['mo']['maint']['Zone_Group'] = df['mo']['maint']['Zone']
            
            if 'ops' in df['mo'] and isinstance(df['mo']['ops'], pd.DataFrame) and not df['mo']['ops'].empty:
                if 'Zone' in df['mo']['ops'].columns and 'Zone_Group' not in df['mo']['ops'].columns:
                    df['mo']['ops']['Zone_Group'] = df['mo']['ops']['Zone']
            
            if 'safety' in df['mo'] and isinstance(df['mo']['safety'], pd.DataFrame) and not df['mo']['safety'].empty:
                if 'Zone' in df['mo']['safety'].columns and 'Zone_Group' not in df['mo']['safety'].columns:
                    df['mo']['safety']['Zone_Group'] = df['mo']['safety']['Zone']
            
            # Convert UDC trend table to JSON format
            if 'udc_trend_table' in df['mo'] and df['mo']['udc_trend_table'] is not None:
                if not isinstance(df['mo']['udc_trend_table'], pd.DataFrame):
                    df['mo']['udc_trend_table'] = convert_to_key_value_df("udc", df['mo']['udc_trend_table'])
                elif 'key' not in df['mo']['udc_trend_table'].columns:
                    df['mo']['udc_trend_table'] = convert_to_key_value_df("udc", df['mo']['udc_trend_table'])
        
        # Write summary data (single table)
        if 'summary_data' in df and isinstance(df['summary_data'], pd.DataFrame):
            write_to_db_once_off(
                conn,
                df['summary_data'],
                f"{dfname}_summary_data",
                recreate=False,
                calcs_start_date=calcs_start_date,
                report_end_date=report_end_date
            )
        
        # Write main nested structure (periods and tables)
        write_sigops_to_db(
            conn,
            df,
            dfname,
            recreate=False,
            calcs_start_date=calcs_start_date,
            report_start_date=report_start_date,
            report_end_date=report_end_date
        )
        
        logger.info(f"Successfully appended {dfname} data to database")
        
    except Exception as e:
        logger.error(f"Error appending to database: {e}")
        logger.error(traceback.format_exc())
        raise


def recreate_database(conn, df: dict, dfname: str):
    """
    Recreate database with proper schema - matches R recreate_database()
    
    WARNING: This drops and recreates all tables!
    
    Args:
        conn: SQLAlchemy database connection
        df: Nested dict with data structure
        dfname: Base name for tables
    """
    try:
        from sqlalchemy import text
        
        # Prep data (same as append_to_database)
        if 'mo' in df:
            if 'maint' in df['mo'] and isinstance(df['mo']['maint'], pd.DataFrame) and not df['mo']['maint'].empty:
                if 'Zone' in df['mo']['maint'].columns:
                    df['mo']['maint']['Zone_Group'] = df['mo']['maint']['Zone']
            
            if 'ops' in df['mo'] and isinstance(df['mo']['ops'], pd.DataFrame) and not df['mo']['ops'].empty:
                if 'Zone' in df['mo']['ops'].columns:
                    df['mo']['ops']['Zone_Group'] = df['mo']['ops']['Zone']
            
            if 'safety' in df['mo'] and isinstance(df['mo']['safety'], pd.DataFrame) and not df['mo']['safety'].empty:
                if 'Zone' in df['mo']['safety'].columns:
                    df['mo']['safety']['Zone_Group'] = df['mo']['safety']['Zone']
            
            if 'udc_trend_table' in df['mo']:
                df['mo']['udc_trend_table'] = convert_to_key_value_df("udc", df['mo']['udc_trend_table'])
        
        # Write all tables with recreate=True
        table_names = write_sigops_to_db(conn, df, dfname, recreate=True)
        
        # Write special tables
        if 'mo' in df:
            if 'udc_trend_table' in df['mo'] and isinstance(df['mo']['udc_trend_table'], pd.DataFrame):
                write_to_db_once_off(conn, df['mo']['udc_trend_table'], f"{dfname}_mo_udc_trend", recreate=True)
            
            if 'hourly_udc' in df['mo'] and isinstance(df['mo']['hourly_udc'], pd.DataFrame):
                write_to_db_once_off(conn, df['mo']['hourly_udc'], f"{dfname}_mo_hourly_udc", recreate=True)
        
        if 'summary_data' in df and isinstance(df['summary_data'], pd.DataFrame):
            write_to_db_once_off(conn, df['summary_data'], f"{dfname}_summary_data", recreate=True)
        
        # For Aurora/MySQL: optimize tables, create indexes, add primary keys
        result = conn.execute(text("SELECT VERSION()"))
        version = result.fetchone()[0]
        
        if 'MySQL' in version or 'MariaDB' in version:
            logger.info(f"{datetime.now()} Aurora/MySQL Database Connection - Optimizing tables")
            
            # Create indexes for all tables
            for table_name in table_names:
                try:
                    set_index_aurora(conn, table_name)
                except Exception as e:
                    logger.warning(f"Could not create indexes for {table_name}: {e}")
            
            # Add primary keys
            for table_name in table_names:
                try:
                    add_primary_key_id_field_aurora(conn, table_name)
                except Exception as e:
                    logger.warning(f"Could not add primary key to {table_name}: {e}")
        
        logger.info("Database recreation completed")
        
    except Exception as e:
        logger.error(f"Error recreating database: {e}")
        logger.error(traceback.format_exc())
        raise
