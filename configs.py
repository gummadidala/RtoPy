"""
Configuration management functions
Converted from Configs.R
"""

import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import logging
import boto3
from pathlib import Path
import yaml
from typing import Optional, Dict, List, Union
import pyarrow as pa
import pyarrow.parquet as pq
from s3_parquet_io import s3read_using, s3write_using

logger = logging.getLogger(__name__)

def get_corridors(corr_fn: str, filter_signals: bool = True, mark_only: bool = False) -> pd.DataFrame:
    """
    Read and process corridors configuration from Excel file
    
    Args:
        corr_fn: Corridor filename (Excel file)
        filter_signals: If True, filter to only include active signals
        mark_only: If True, filter to Mark-specific zones only
    
    Returns:
        DataFrame with corridor configuration
    """
    
    # Define column types
    cols = {
        'SignalID': 'float64',
        'Zone_Group': 'string',
        'Zone': 'string', 
        'Corridor': 'string',
        'Subcorridor': 'string',
        'Agency': 'string',
        'Main Street Name': 'string',
        'Side Street Name': 'string',
        'Milepost': 'float64',
        'Asof': 'datetime64[ns]',
        'Duplicate': 'float64',
        'Include': 'boolean',
        'Modified': 'datetime64[ns]',
        'Note': 'string',
        'Latitude': 'float64',
        'Longitude': 'float64',
        'County': 'string',
        'City': 'string'
    }
    
    try:
        # Read Excel file
        df = pd.read_excel(corr_fn)
        
        # Set column types for existing columns
        for col, dtype in cols.items():
            if col in df.columns:
                if dtype == 'datetime64[ns]':
                    df[col] = pd.to_datetime(df[col])
                elif dtype == 'boolean':
                    df[col] = df[col].astype('boolean')
                elif dtype == 'string':
                    df[col] = df[col].astype('string')
                else:
                    df[col] = df[col].astype(dtype)
        
        # Strip whitespace from string columns
        string_cols = df.select_dtypes(include=['string', 'object']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
        
        # Filter signals if requested
        if filter_signals:
            df = df[
                (df['SignalID'] > 0) & 
                (df['Include'] == True)
            ]
        
        # Mark-only filter
        if mark_only:
            df = df[
                (df['Zone_Group'] == df['Zone']) | 
                (df['Zone_Group'].isin(['RTOP1', 'RTOP2']))
            ]
        
        # Handle missing Modified dates
        df['Modified'] = df['Modified'].fillna(pd.to_datetime('1900-01-01'))
        
        # Get the last modified record for each Signal|Zone|Corridor combination
        df = (df.groupby(['SignalID', 'Zone', 'Corridor'])
              .apply(lambda x: x[x['Modified'] == x['Modified'].max()])
              .reset_index(drop=True))
        
        # Filter out records with missing Corridor
        df = df[df['Corridor'].notna()]
        
        # Create Name field and final processing
        df['Name'] = df['Main Street Name'].astype(str) + ' @ ' + df['Side Street Name'].astype(str)
        
        # Select and rename final columns
        result = df[[
            'SignalID', 'Zone', 'Zone_Group', 'Corridor', 'Subcorridor',
            'Milepost', 'Agency', 'Name', 'Asof', 'Latitude', 'Longitude'
        ]].copy()
        
        # Add TEAMS GUID if it exists
        if 'TEAMS GUID' in df.columns:
            result['TeamsLocationID'] = df['TEAMS GUID'].astype(str).str.strip()
        
        # Convert to categorical where appropriate
        result['SignalID'] = result['SignalID'].astype('category')
        result['Zone'] = result['Zone'].astype('category')
        result['Zone_Group'] = result['Zone_Group'].astype('category')
        result['Corridor'] = result['Corridor'].astype('category')
        result['Subcorridor'] = result['Subcorridor'].astype('category')
        
        # Create Description field
        result['Description'] = (result['SignalID'].astype(str) + ': ' + result['Name'])
        result['Description'] = result['Description'].astype('category')
        
        logger.info(f"Loaded {len(result)} corridor records")
        return result
        
    except Exception as e:
        logger.error(f"Error loading corridors from {corr_fn}: {e}")
        raise

def check_corridors(corridors: pd.DataFrame) -> bool:
    """
    Validate corridors configuration for common issues
    
    Args:
        corridors: Corridors DataFrame
    
    Returns:
        Boolean indicating if validation passed
    """
    
    distinct_corridors = corridors[['Zone', 'Zone_Group', 'Corridor']].drop_duplicates()
    
    # Check 1: Same Corridor in multiple Zones
    corridors_in_multiple_zones = (distinct_corridors.groupby('Corridor')
                                  .size()
                                  .reset_index(name='count'))
    corridors_in_multiple_zones = corridors_in_multiple_zones[
        corridors_in_multiple_zones['count'] > 1
    ]['Corridor'].tolist()
    
    if corridors_in_multiple_zones:
        check1 = distinct_corridors[
            distinct_corridors['Corridor'].isin(corridors_in_multiple_zones)
        ].sort_values(['Corridor', 'Zone', 'Zone_Group'])
        
        logger.warning("Corridors in multiple zones:")
        logger.warning(f"\n{check1}")
    
    # Check 2: Corridors with different cases
    corridor_case_check = (distinct_corridors.assign(corridor_lower=lambda x: x['Corridor'].str.lower())
                          .groupby('corridor_lower')
                          .size()
                          .reset_index(name='count'))
    corridors_with_case_mismatches = corridor_case_check[
        corridor_case_check['count'] > 1
    ]['corridor_lower'].tolist()
    
    if corridors_with_case_mismatches:
        check2 = distinct_corridors[
            distinct_corridors['Corridor'].str.lower().isin(corridors_with_case_mismatches)
        ].sort_values(['Corridor', 'Zone', 'Zone_Group'])
        
        logger.warning("Same corridor, different cases:")
        logger.warning(f"\n{check2}")
    
    # Return validation result
    pass_validation = not (corridors_in_multiple_zones or corridors_with_case_mismatches)
    
    if pass_validation:
        logger.info("Corridors validation passed")
    else:
        logger.error("Corridors validation failed")
    
    return pass_validation

def get_cam_config(object_key: str, bucket: str, corridors: pd.DataFrame) -> pd.DataFrame:
    """
    Get camera configuration from S3 Excel file
    
    Args:
        object_key: S3 object key for camera config file
        bucket: S3 bucket name
        corridors: Corridors DataFrame
    
    Returns:
        DataFrame with camera configuration
    """
    
    try:
        # Read camera config from S3
        cam_config0 = s3read_using(
            pd.read_excel,
            bucket=bucket,
            object=object_key
        )
        
        # Filter and process
        cam_config0 = cam_config0[cam_config0['Include'] == True].copy()
        
        cam_config0 = cam_config0[[
            'CameraID', 'Location', 'MaxView ID', 'As_of_Date'
        ]].rename(columns={'MaxView ID': 'SignalID'}).copy()
        
        cam_config0['CameraID'] = cam_config0['CameraID'].astype('category')
        cam_config0['SignalID'] = cam_config0['SignalID'].astype('category')
        cam_config0['As_of_Date'] = pd.to_datetime(cam_config0['As_of_Date']).dt.date
        
        # Join with corridors
        corrs = corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Subcorridor']]
        
        result = corrs.merge(cam_config0, on='SignalID', how='left')
        result = result[result['CameraID'].notna()]
        
        # Create description and sort
        result['Description'] = (result['CameraID'].astype(str) + ': ' + 
                               result['Location'].astype(str))
        
        result = result.sort_values(['Zone_Group', 'Zone', 'Corridor', 'CameraID'])
        
        logger.info(f"Loaded {len(result)} camera configurations")
        return result
        
    except Exception as e:
        logger.error(f"Error loading camera config: {e}")
        raise

def get_ped_config_factory(bucket: str):
    """
    Factory function to create pedestrian configuration getter
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Function that takes date and returns ped config
    """
    
    def get_ped_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get pedestrian detector configuration for a specific date
        
        Args:
            date_: Date for configuration
        
        Returns:
            DataFrame with pedestrian detector configuration
        """
        
        if isinstance(date_, str):
            date_ = pd.to_datetime(date_).date()
        elif isinstance(date_, datetime):
            date_ = date_.date()
        
        # Don't go back further than 2019-01-01
        date_ = max(date_, date(2019, 1, 1))
        
        try:
            s3_key = f"config/maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv"
            
            # Check if file exists
            s3_client = boto3.client('s3')
            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
            except s3_client.exceptions.ClientError:
                logger.warning(f"Ped config file not found for {date_}")
                return pd.DataFrame()
            
            # Read the file
            df = s3read_using(
                pd.read_csv,
                bucket=bucket,
                object=s3_key,
                usecols=['SignalID', 'IP', 'PrimaryName', 'SecondaryName', 'Detector', 'CallPhase'],
                dtype={'SignalID': str, 'Detector': str, 'CallPhase': str}
            )
            
            # Handle multiple detectors - keep first occurrence
            df = df.groupby(['SignalID', 'Detector']).first().reset_index()
            
            # Convert to categories
            df['SignalID'] = df['SignalID'].astype('category')
            df['Detector'] = df['Detector'].astype('category')
            df['CallPhase'] = df['CallPhase'].astype('category')
            
            result = df[['SignalID', 'Detector', 'CallPhase']].drop_duplicates()
            
            logger.info(f"Loaded {len(result)} pedestrian detector configs for {date_}")
            return result
            
        except Exception as e:
            logger.error(f"Error loading ped config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_ped_config

def get_ped_config_cel_factory(bucket: str, conf_athena: Dict):
    """
    Factory function to create CEL-based pedestrian configuration getter
    
    Args:
        bucket: S3 bucket name
        conf_athena: Athena configuration
    
    Returns:
        Function that takes date and returns ped config from CEL data
    """
    
    def get_ped_config_cel(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get pedestrian detector configuration from CEL data
        
        Args:
            date_: Date for configuration
        
        Returns:
            DataFrame with pedestrian detector configuration
        """
        
        if isinstance(date_, str):
            date_ = pd.to_datetime(date_).date()
        elif isinstance(date_, datetime):
            date_ = date_.date()
        
        try:
            # Calculate start date (6 months back from first of month)
            first_of_month = date_.replace(day=1)
            ped_start_date = first_of_month - pd.DateOffset(months=6)
            
            # Read from S3 using pyarrow
            import pyarrow.dataset as ds
            
            dataset = ds.dataset(f"s3://{bucket}/config/cel_ped_detectors/")
            
            # Filter and read
            table = dataset.to_table(
                filter=ds.field('date') >= ped_start_date.strftime('%Y-%m-%d'),
                columns=['SignalID', 'Detector', 'CallPhase']
            )
            
            df = table.to_pandas()
            
            # Get distinct values
            result = df[['SignalID', 'Detector', 'CallPhase']].drop_duplicates()
            
            logger.info(f"Loaded {len(result)} CEL pedestrian detector configs")
            return result
            
        except Exception as e:
            logger.error(f"Error loading CEL ped config: {e}")
            return pd.DataFrame()
    
    return get_ped_config_cel

def get_det_config_factory(bucket: str, folder: str):
    """
    Factory function to create detector configuration getter
    
    Args:
        bu
        cket: S3 bucket name
        folder: S3 folder name for detector configs
    
    Returns:
        Function that takes date and returns detector config
    """
    
    def get_det_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get detector configuration for a specific date
        
        Args:
            date_: Date for configuration
        
        Returns:
            DataFrame with detector configuration
        """
        
        if isinstance(date_, str):
            date_str = date_
        else:
            date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)
        
        try:
            s3_prefix = f"config/{folder}/date={date_str}"
            
            # List objects with this prefix
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            
            if 'Contents' not in response:
                raise ValueError(f"No detector config found for {date_str}")
            
            # Get the detector config file
            config_obj = response['Contents'][0]
            s3_key = config_obj['Key']
            
            # Read the configuration file
            df = s3read_using(
                pd.read_csv,
                bucket=bucket,
                object=s3_key,
                dtype={
                    'SignalID': str,
                    'IP': str,
                    'PrimaryName': str,
                    'SecondaryName': str,
                    'Detector': str,
                    'CallPhase': str,
                    'TimeFromStopBar': 'float64',
                    'DetectionHardware': str,
                    'LaneType': str,
                    'MovementType': str,
                    'LaneNumber': 'Int64',
                    'DetChannel': 'Int64',
                    'LatencyCorrection': 'float64'
                }
            )
            
            # Convert to categories for memory efficiency
            categorical_cols = ['SignalID', 'Detector', 'CallPhase', 'DetectionHardware', 
                              'LaneType', 'MovementType']
            for col in categorical_cols:
                if col in df.columns:
                    df[col] = df[col].astype('category')
            
            # Remove duplicates keeping the first occurrence
            df = df.groupby(['SignalID', 'Detector']).first().reset_index()
            
            logger.info(f"Loaded {len(df)} detector configurations for {date_str}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading detector config for {date_str}: {e}")
            return pd.DataFrame()
    
    return get_det_config

def get_latest_det_config(conf: Dict) -> pd.DataFrame:
    """
    Get the most recent detector configuration
    
    Args:
        conf: Configuration dictionary containing bucket and paths
    
    Returns:
        DataFrame with latest detector configuration
    """
    
    try:
        # Find the most recent detector config file
        s3_client = boto3.client('s3')
        
        response = s3_client.list_objects_v2(
            Bucket=conf['bucket'],
            Prefix='config/atspm_det_config_good/date=',
            Delimiter='/'
        )
        
        if 'CommonPrefixes' not in response:
            raise ValueError("No detector config dates found")
        
        # Get the latest date
        date_prefixes = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
        latest_date_prefix = max(date_prefixes)
        latest_date = latest_date_prefix.split('=')[1].rstrip('/')
        
        # Get detector config for latest date
        get_det_config = get_det_config_factory(conf['bucket'], 'atspm_det_config_good')
        det_config = get_det_config(latest_date)
        
        logger.info(f"Retrieved latest detector config from {latest_date}")
        return det_config
        
    except Exception as e:
        logger.error(f"Error getting latest detector config: {e}")
        return pd.DataFrame()

def get_watchdog_notes_factory(bucket: str, det_config: pd.DataFrame):
    """
    Factory function to create watchdog notes getter
    
    Args:
        bucket: S3 bucket name
        det_config: Detector configuration DataFrame
    
    Returns:
        Function that takes date and returns watchdog notes
    """
    
    def get_watchdog_notes(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get watchdog detector notes for a specific date
        
        Args:
            date_: Date for notes
        
        Returns:
            DataFrame with watchdog notes
        """
        
        if isinstance(date_, str):
            date_str = date_
        else:
            date_str = date_.strftime('%Y-%m-%d') if hasattr(date_, 'strftime') else str(date_)
        
        try:
            s3_key = f"watchdog/bd/date={date_str}/bad_detectors_{date_str}.csv"
            
            # Check if file exists
            s3_client = boto3.client('s3')
            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
            except s3_client.exceptions.ClientError:
                logger.info(f"No watchdog notes found for {date_str}")
                return pd.DataFrame()
            
            # Read watchdog notes
            wd_notes = s3read_using(
                pd.read_csv,
                bucket=bucket,
                object=s3_key,
                dtype={
                    'SignalID': str,
                    'Detector': str,
                    'uptime': 'float64'
                }
            )
            
            # Process and join with detector config
            wd_notes['SignalID'] = wd_notes['SignalID'].astype('category')
            wd_notes['Detector'] = wd_notes['Detector'].astype('category')
            
            # Join with detector configuration
            result = wd_notes.merge(
                det_config[['SignalID', 'Detector', 'CallPhase', 'LaneType', 'MovementType']],
                on=['SignalID', 'Detector'],
                how='left'
            )
            
            # Filter for main line detectors (phases 2 and 6)
            result = result[result['CallPhase'].isin(['2', '6'])]
            
            # Sort by signal and detector
            result = result.sort_values(['SignalID', 'Detector'])
            
            logger.info(f"Loaded {len(result)} watchdog notes for {date_str}")
            return result
            
        except Exception as e:
            logger.error(f"Error loading watchdog notes for {date_str}: {e}")
            return pd.DataFrame()
    
    return get_watchdog_notes

def get_rsu_config_factory(bucket: str):
    """
    Factory function to create RSU configuration getter
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Function that returns RSU configuration
    """
    
    def get_rsu_config() -> pd.DataFrame:
        """
        Get RSU (Roadside Unit) configuration
        
        Returns:
            DataFrame with RSU configuration
        """
        
        try:
            # Find the most recent RSU config file
            s3_client = boto3.client('s3')
            
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix='RSU_Config_',
                Delimiter='/'
            )
            
            if 'Contents' not in response:
                raise ValueError("No RSU config files found")
            
            # Get the most recent file
            files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
            latest_file = files[0]['Key']
            
            # Read RSU configuration
            rsu_config = s3read_using(
                pd.read_excel,
                bucket=bucket,
                object=latest_file
            )
            
            # Process RSU configuration
            if 'Include' in rsu_config.columns:
                rsu_config = rsu_config[rsu_config['Include'] == True]
            
            # Clean and process columns
            rsu_config = rsu_config.rename(columns=lambda x: x.strip())
            
            # Convert to appropriate types
            if 'RSU_ID' in rsu_config.columns:
                rsu_config['RSU_ID'] = rsu_config['RSU_ID'].astype('category')
            
            if 'Location' in rsu_config.columns:
                rsu_config['Location'] = rsu_config['Location'].astype('string')
            
            logger.info(f"Loaded {len(rsu_config)} RSU configurations")
            return rsu_config
            
        except Exception as e:
            logger.error(f"Error loading RSU config: {e}")
            return pd.DataFrame()
    
    return get_rsu_config

def write_signal_details(date_str: str, conf: Dict, signals_list: List[str]):
    """
    Write signal details file for a specific date
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        conf: Configuration dictionary
        signals_list: List of signal IDs
    """
    
    try:
        # Create signal details DataFrame
        signal_details = pd.DataFrame({
            'SignalID': signals_list,
            'Date': pd.to_datetime(date_str).date()
        })
        
        # Add metadata
        signal_details['Timestamp'] = datetime.now()
        signal_details['RecordCount'] = len(signals_list)
        
        # Write to S3
        s3_key = f"signal_details/date={date_str}/signal_details_{date_str}.parquet"
        
        s3write_using(
            signal_details.to_parquet,
            bucket=conf['bucket'],
            object=s3_key,
            index=False
        )
        
        logger.info(f"Wrote signal details for {date_str}: {len(signals_list)} signals")
        
    except Exception as e:
        logger.error(f"Error writing signal details for {date_str}: {e}")

def read_corridors(filename: str) -> pd.DataFrame:
    """
    Read corridors from feather or Excel file
    
    Args:
        filename: Filename for corridors file
    
    Returns:
        DataFrame with corridors
    """
    
    try:
        if filename.endswith('.feather'):
            return pd.read_feather(filename)
        elif filename.endswith('.xlsx') or filename.endswith('.xls'):
            return get_corridors(filename)
        else:
            raise ValueError(f"Unsupported file format: {filename}")
            
    except Exception as e:
        logger.error(f"Error reading corridors from {filename}: {e}")
        raise

def validate_signal_config(signal_config: pd.DataFrame, required_cols: List[str]) -> bool:
    """
    Validate signal configuration DataFrame
    
    Args:
        signal_config: Signal configuration DataFrame
        required_cols: List of required column names
    
    Returns:
        Boolean indicating if validation passed
    """
    
    try:
        # Check if all required columns exist
        missing_cols = set(required_cols) - set(signal_config.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check for duplicate signal IDs
        if signal_config['SignalID'].duplicated().any():
            duplicates = signal_config[signal_config['SignalID'].duplicated()]['SignalID'].unique()
            logger.error(f"Duplicate SignalIDs found: {duplicates}")
            return False
        
        # Check for null values in critical columns
        critical_cols = ['SignalID', 'Zone', 'Corridor']
        for col in critical_cols:
            if col in signal_config.columns:
                null_count = signal_config[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f"Found {null_count} null values in {col}")
        
        logger.info("Signal configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating signal config: {e}")
        return False

def get_signal_ids_by_zone(corridors: pd.DataFrame, zone: str) -> List[str]:
    """
    Get list of signal IDs for a specific zone
    
    Args:
        corridors: Corridors DataFrame
        zone: Zone name
    
    Returns:
        List of signal IDs in the zone
    """
    
    try:
        zone_signals = corridors[corridors['Zone'] == zone]['SignalID'].unique().tolist()
        logger.info(f"Found {len(zone_signals)} signals in zone {zone}")
        return zone_signals
        
    except Exception as e:
        logger.error(f"Error getting signals for zone {zone}: {e}")
        return []

def get_signal_ids_by_corridor(corridors: pd.DataFrame, corridor: str) -> List[str]:
    """
    Get list of signal IDs for a specific corridor
    
    Args:
        corridors: Corridors DataFrame
        corridor: Corridor name
    
    Returns:
        List of signal IDs in the corridor
    """
    
    try:
        corridor_signals = corridors[corridors['Corridor'] == corridor]['SignalID'].unique().tolist()
        logger.info(f"Found {len(corridor_signals)} signals in corridor {corridor}")
        return corridor_signals
        
    except Exception as e:
        logger.error(f"Error getting signals for corridor {corridor}: {e}")
        return []

def create_zone_mapping(corridors: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Create mapping of zones to signal IDs
    
    Args:
        corridors: Corridors DataFrame
    
    Returns:
        Dictionary mapping zone names to lists of signal IDs
    """
    
    try:
        zone_mapping = {}
        
        for zone in corridors['Zone'].unique():
            if pd.notna(zone):
                zone_mapping[zone] = get_signal_ids_by_zone(corridors, zone)
        
        logger.info(f"Created zone mapping for {len(zone_mapping)} zones")
        return zone_mapping
        
    except Exception as e:
        logger.error(f"Error creating zone mapping: {e}")
        return {}

def create_corridor_mapping(corridors: pd.DataFrame) -> Dict[str, List[str]]:
    """
    Create mapping of corridors to signal IDs
    
    Args:
        corridors: Corridors DataFrame
    
    Returns:
        Dictionary mapping corridor names to lists of signal IDs
    """
    
    try:
        corridor_mapping = {}
        
        for corridor in corridors['Corridor'].unique():
            if pd.notna(corridor):
                corridor_mapping[corridor] = get_signal_ids_by_corridor(corridors, corridor)
        
        logger.info(f"Created corridor mapping for {len(corridor_mapping)} corridors")
        return corridor_mapping
        
    except Exception as e:
        logger.error(f"Error creating corridor mapping: {e}")
        return {}

# Constants for zone definitions (equivalent to R constants)
RTOP1_ZONES = ['Zone 1', 'Zone 2', 'Zone 3', 'Zone 4']
RTOP2_ZONES = ['Zone 5', 'Zone 6', 'Zone 7']

def get_zone_group_signals(corridors: pd.DataFrame, zone_group: str) -> List[str]:
    """
    Get signals for a zone group (including special groups like RTOP1, RTOP2)
    
    Args:
        corridors: Corridors DataFrame
        zone_group: Zone group name
    
    Returns:
        List of signal IDs in the zone group
    """
    
    try:
        if zone_group == "All RTOP":
            zones = RTOP1_ZONES + RTOP2_ZONES
        elif zone_group == "RTOP1":
            zones = RTOP1_ZONES
        elif zone_group == "RTOP2":
            zones = RTOP2_ZONES
        elif zone_group == "Zone 7":
            zones = ["Zone 7m", "Zone 7d"]
        else:
            zones = [zone_group]
        
        signals = corridors[corridors['Zone'].isin(zones)]['SignalID'].unique().tolist()
        logger.info(f"Found {len(signals)} signals in zone group {zone_group}")
        return signals
        
    except Exception as e:
        logger.error(f"Error getting signals for zone group {zone_group}: {e}")
        return []

def save_config_snapshot(config_data: Dict, bucket: str, date_str: str):
    """
    Save a snapshot of configuration data to S3
    
    Args:
        config_data: Configuration dictionary to save
        bucket: S3 bucket name
        date_str: Date string for the snapshot
    """
    
    try:
        import json
        
        s3_key = f"config_snapshots/date={date_str}/config_snapshot_{date_str}.json"
        
        # Convert non-serializable objects to strings
        serializable_config = {}
        for key, value in config_data.items():
            if isinstance(value, (pd.DataFrame, pd.Series)):
                serializable_config[key] = f"DataFrame with {len(value)} records"
            elif callable(value):
                serializable_config[key] = f"Function: {value.__name__}"
            else:
                serializable_config[key] = str(value)
        
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(serializable_config, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Saved config snapshot to {s3_key}")
        
    except Exception as e:
        logger.error(f"Error saving config snapshot: {e}")