import pandas as pd
import numpy as np
import boto3
import awswrangler as wr
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import logging
from typing import Dict, List, Optional, Callable, Union
import re
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Set up logging
logger = logging.getLogger(__name__)

def get_corridors(corr_fn: str, filter_signals: bool = True) -> pd.DataFrame:
    """
    Read and process corridors configuration from Excel file
    
    Args:
        corr_fn: Corridor filename (Excel file)
        filter_signals: If True, filter to only include active signals
    
    Returns:
        DataFrame with corridor configuration
    """
    
    # Define column types
    cols = {
        'SignalID': 'float64',
        'Zone_Group': 'string',
        'Zone': 'string', 
        'Contract': 'string',
        'District': 'string',
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
        'City': 'string',
        'Priority': 'string',
        'Classification': 'string',
        'Subcorridor': 'string',
        'Corridor': 'string',
        'TEAMS GUID': 'string'
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
        
        # Rename columns (equivalent to R's rename)
        df = df.rename(columns={'Contract': 'Zone_Group', 'District': 'Zone'})
        
        # Replace NA values in Modified column (equivalent to R's replace_na)
        df['Modified'] = df['Modified'].fillna(pd.to_datetime('1900-01-01'))
        
        # Get the last modified record for the Signal|Zone|Corridor combination
        df = (df.groupby(['SignalID', 'Zone', 'Corridor'])
              .apply(lambda x: x.loc[x['Modified'].idxmax()])
              .reset_index(drop=True))
        
        # Filter out rows where Corridor is NA
        df = df[df['Corridor'].notna()]
        
        # Strip whitespace from string columns (equivalent to R's str_trim)
        string_cols = df.select_dtypes(include=['string', 'object']).columns
        for col in string_cols:
            df[col] = df[col].astype(str).str.strip()
        
        # Filter signals if requested
        if filter_signals:
            df = df[
                (df['SignalID'] > 0) & 
                (df['Include'] == True)
            ]
        
        # Create Name column (equivalent to R's unite)
        df['Name'] = df['Main Street Name'].astype(str) + ' @ ' + df['Side Street Name'].astype(str)
        
        # Create final dataframe with required columns
        result = pd.DataFrame({
            'SignalID': df['SignalID'].astype('category'),
            'Zone': df['Zone'].astype('category'),
            'Zone_Group': df['Zone_Group'].astype('category'),
            'Corridor': df['Corridor'].astype('category'),
            'Subcorridor': df['Subcorridor'].astype('category'),
            'Milepost': df['Milepost'].astype('float64'),
            'Agency': df['Agency'],
            'Name': df['Name'],
            'Asof': df['Asof'].dt.date,
            'Latitude': df['Latitude'],
            'Longitude': df['Longitude'],
            'TeamsLocationID': df['TEAMS GUID'].astype(str).str.strip() if 'TEAMS GUID' in df.columns else '',
            'Priority': df['Priority'] if 'Priority' in df.columns else '',
            'Classification': df['Classification'] if 'Classification' in df.columns else ''
        })
        
        # Create Description column
        result['Description'] = result['SignalID'].astype(str) + ': ' + result['Name']
        
        logger.info(f"Loaded {len(result)} corridor configurations")
        return result
        
    except Exception as e:
        logger.error(f"Error loading corridors from {corr_fn}: {e}")
        raise

def check_corridors(corridors: pd.DataFrame) -> bool:
    """
    Validate corridor configuration DataFrame
    
    Args:
        corridors: Corridor configuration DataFrame
    
    Returns:
        Boolean indicating if validation passed
    """
    
    try:
        distinct_corridors = corridors[['Zone', 'Zone_Group', 'Corridor']].drop_duplicates()
        
        # Check 1: Same Corridor in multiple Zones
        corridors_in_multiple_zones = (distinct_corridors.groupby('Corridor')
                                     .size()
                                     .reset_index(name='count'))
        corridors_in_multiple_zones = corridors_in_multiple_zones[
            corridors_in_multiple_zones['count'] > 1
        ]['Corridor'].tolist()
        
        check1 = distinct_corridors[
            distinct_corridors['Corridor'].isin(corridors_in_multiple_zones)
        ].sort_values(['Corridor', 'Zone', 'Zone_Group'])
        
        # Check 2: Corridors with different cases
        corridor_lower = distinct_corridors.copy()
        corridor_lower['corridor_lower'] = corridor_lower['Corridor'].str.lower()
        corridors_with_case_mismatches = (corridor_lower.groupby('corridor_lower')
                                        .size()
                                        .reset_index(name='count'))
        corridors_with_case_mismatches = corridors_with_case_mismatches[
            corridors_with_case_mismatches['count'] > 1
        ]['corridor_lower'].tolist()
        
        check2 = distinct_corridors[
            distinct_corridors['Corridor'].str.lower().isin(corridors_with_case_mismatches)
        ].sort_values(['Corridor', 'Zone', 'Zone_Group'])
        
        # Print validation results
        if len(check1) > 0:
            print("Corridors in multiple zones:")
            print(check1)
        
        if len(check2) > 0:
            print("Same corridor, different cases:")
            print(check2)
        
        pass_validation = not (len(check1) > 0 or len(check2) > 0)
        return pass_validation
        
    except Exception as e:
        logger.error(f"Error validating corridors: {e}")
        return False

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
        cam_config0 = wr.s3.read_excel(
            path=f"s3://{bucket}/{object_key}"
        )
        
        # Filter and process
        cam_config0 = cam_config0[cam_config0['Include'] == True].copy()
        
        cam_config0 = cam_config0[[
            'CameraID', 'Location', 'MaxView ID', 'As_of_Date'
        ]].rename(columns={'MaxView ID': 'SignalID'}).copy()
        
        cam_config0['CameraID'] = cam_config0['CameraID'].astype('category')
        cam_config0['SignalID'] = cam_config0['SignalID'].astype('category')
        cam_config0['As_of_Date'] = pd.to_datetime(cam_config0['As_of_Date']).dt.date
        
        cam_config0 = cam_config0.drop_duplicates()
        
        # Join with corridors
        corrs = corridors[['SignalID', 'Zone_Group', 'Zone', 'Corridor', 'Subcorridor']]
        
        result = corrs.merge(cam_config0, on='SignalID', how='left', validate='many_to_many')
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

def get_ped_config_(bucket: str) -> Callable:
    """
    Create a function to get pedestrian configuration for a given date
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Function that takes a date and returns pedestrian configuration
    """
    
    def get_ped_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get pedestrian configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with pedestrian configuration
        """
        
        try:
            if isinstance(date_, str):
                date_ = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_ = date_.date()
            
            # Ensure date is not before 2019-01-01
            min_date = date(2019, 1, 1)
            if date_ < min_date:
                date_ = min_date
            
            s3_key = f"config/maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv"
            
            # Check if file exists
            s3_client = boto3.client('s3')
            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
            except s3_client.exceptions.NoSuchKey:
                return pd.DataFrame()
            
            # Read the file
            df = wr.s3.read_csv(
                path=f"s3://{bucket}/{s3_key}",
                usecols=['SignalID', 'IP', 'PrimaryName', 'SecondaryName', 'Detector', 'CallPhase']
            )
            
            # Process the data
            result = (df.groupby(['SignalID', 'Detector'])
                     .first()
                     .reset_index())
            
            result = result[['SignalID', 'Detector', 'CallPhase']].copy()
            result['SignalID'] = result['SignalID'].astype('category')
            result['Detector'] = result['Detector'].astype('category')
            result['CallPhase'] = result['CallPhase'].astype('category')
            
            result = result.drop_duplicates()
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting pedestrian config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_ped_config

def get_ped_config_cel_(bucket: str, conf_athena: dict) -> Callable:
    """
    Create a function to get CEL pedestrian configuration
    
    Args:
        bucket: S3 bucket name
        conf_athena: Athena configuration
    
    Returns:
        Function that returns CEL pedestrian configuration
    """
    
    def get_ped_config_cel(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get CEL pedestrian configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with CEL pedestrian configuration
        """
        
        try:
            if isinstance(date_, str):
                date_ = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_ = date_.date()
            
            # Calculate start date (6 months back from beginning of month)
            first_of_month = date_.replace(day=1)
            ped_start_date = first_of_month - relativedelta(months=6)
            
            # Read from S3 using arrow dataset
            dataset_path = f"s3://{bucket}/config/cel_ped_detectors/"
            
            try:
                df = wr.s3.read_parquet(
                    path=dataset_path,
                    filters=[('date', '>=', ped_start_date.strftime('%Y-%m-%d'))]
                )
                
                result = df[['SignalID', 'Detector', 'CallPhase']].drop_duplicates()
                return result
                
            except Exception:
                return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error getting CEL pedestrian config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_ped_config_cel

def get_det_config_(bucket: str, folder: str) -> Callable:
    """
    Create a function to get detector configuration for a given date
    
    Args:
        bucket: S3 bucket name
        folder: Folder name in S3
    
    Returns:
        Function that takes a date and returns detector configuration
    """
    
    def get_det_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get detector configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with detector configuration
        """
        
        try:
            if isinstance(date_, str):
                date_str = date_
            elif isinstance(date_, (date, datetime)):
                date_str = date_.strftime('%Y-%m-%d')
            
            s3_prefix = f"config/{folder}/date={date_str}/"
            
            # List objects with this prefix
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)
            
            if 'Contents' not in response:
                raise ValueError(f"No detector config file for {date_str}")
            
            objects = response['Contents']
            
            if len(objects) == 1:
                # Single file - read it directly
                s3_key = objects[0]['Key']
                df = wr.s3.read_feather(path=f"s3://{bucket}/{s3_key}")
                
            elif len(objects) > 1:
                # Multiple files - read and concatenate
                dfs = []
                for obj in objects:
                    s3_key = obj['Key']
                    df_temp = wr.s3.read_feather(path=f"s3://{bucket}/{s3_key}")
                    dfs.append(df_temp)
                df = pd.concat(dfs, ignore_index=True)
                
            else:
                raise ValueError(f"No detector config file for {date_str}")
            
            # Process the data - remove duplicates by SignalID and Detector
            result = (df.groupby(['SignalID', 'Detector'])
                     .first()
                     .reset_index())
            
            # Convert data types
            result['SignalID'] = result['SignalID'].astype(str)
            result['Detector'] = result['Detector'].astype(int)
            result['CallPhase'] = result['CallPhase'].astype(int)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting detector config for {date_}: {e}")
            raise
    
    return get_det_config

def get_det_config_aog(get_det_config_func: Callable) -> Callable:
    """
    Create a function to get AOG (Arrivals on Green) detector configuration
    
    Args:
        get_det_config_func: Base detector config function
    
    Returns:
        Function that returns AOG detector configuration
    """
    
    def get_aog_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get AOG detector configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with AOG detector configuration
        """
        
        try:
            df = get_det_config_func(date_)
            
            if df.empty:
                return df
            
            # Filter for non-null detectors
            df = df[df['Detector'].notna()].copy()
            
            # Create AOG Priority based on DetectionTypeDesc
            def assign_aog_priority(desc):
                if pd.isna(desc):
                    return 2
                if 'Exit' in str(desc):
                    return 0
                elif 'Advanced Count' in str(desc):
                    return 1
                else:
                    return 2
            
            df['AOGPriority'] = df['DetectionTypeDesc'].apply(assign_aog_priority)
            
            # Filter to only Exit and Advanced Count detectors
            df = df[df['AOGPriority'] < 2]
            
            # For each SignalID and CallPhase, keep only the minimum priority
            df = (df.groupby(['SignalID', 'CallPhase'])
                  .apply(lambda x: x[x['AOGPriority'] == x['AOGPriority'].min()])
                  .reset_index(drop=True))
            
            # Select and format final columns
            result = df[['SignalID', 'Detector', 'CallPhase', 'TimeFromStopBar']].copy()
            result['SignalID'] = result['SignalID'].astype('category')
            result['Detector'] = result['Detector'].astype('category')
            result['CallPhase'] = result['CallPhase'].astype('category')
            
            if isinstance(date_, str):
                date_obj = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_obj = date_.date()
            else:
                date_obj = date_
            
            result['Date'] = date_obj
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting AOG detector config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_aog_config

def get_det_config_qs(get_det_config_func: Callable, bucket: str) -> Callable:
    """
    Create a function to get queue service detector configuration
    
    Args:
        get_det_config_func: Base detector config function
        bucket: S3 bucket name
    
    Returns:
        Function that returns queue service detector configuration
    """
    
    def get_qs_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get queue service detector configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with queue service detector configuration
        """
        
        try:
            if isinstance(date_, str):
                date_str = date_
                date_obj = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_str = date_.strftime('%Y-%m-%d')
                date_obj = date_.date()
            else:
                date_str = date_.strftime('%Y-%m-%d')
                date_obj = date_
            
            # Get detector config
            dc = get_det_config_func(date_)
            
            if dc.empty:
                return dc
            
            # Filter for Advanced Count or Advanced Speed detectors
            mask = (dc['DetectionTypeDesc'].str.contains('Advanced Count', na=False) |
                   dc['DetectionTypeDesc'].str.contains('Advanced Speed', na=False))
            dc = dc[mask]
            
            # Filter for non-null DistanceFromStopBar and Detector
            dc = dc[dc['DistanceFromStopBar'].notna()]
            dc = dc[dc['Detector'].notna()]
            
            # Select required columns
            dc = dc[['SignalID', 'Detector', 'CallPhase', 'TimeFromStopBar']].copy()
            dc['SignalID'] = dc['SignalID'].astype('category')
            dc['Detector'] = dc['Detector'].astype('category') 
            dc['CallPhase'] = dc['CallPhase'].astype('category')
            dc['Date'] = date_obj
            
            # Get bad detectors
            try:
                bd = wr.s3.read_parquet(
                    path=f"s3://{bucket}/mark/bad_detectors/date={date_str}/bad_detectors_{date_str}.parquet"
                )
                bd = bd[['SignalID', 'Detector', 'Good_Day']].copy()
                bd['SignalID'] = bd['SignalID'].astype('category')
                bd['Detector'] = bd['Detector'].astype('category')
                
                # Join and filter out bad detectors
                result = dc.merge(bd, on=['SignalID', 'Detector'], how='left')
                result = result[result['Good_Day'].isna()]
                result = result.drop('Good_Day', axis=1)
                
            except Exception:
                # If bad detectors file doesn't exist, return all detectors
                result = dc
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting queue service detector config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_qs_config

def get_det_config_sf(get_det_config_func: Callable) -> Callable:
    """
    Create a function to get split failure detector configuration
    
    Args:
        get_det_config_func: Base detector config function
    
    Returns:
        Function that returns split failure detector configuration
    """
    
    def get_sf_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get split failure detector configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with split failure detector configuration
        """
        
        try:
            df = get_det_config_func(date_)
            
            if df.empty:
                return df
            
            # Filter for Stop Bar Presence detectors
            df = df[df['DetectionTypeDesc'].str.contains('Stop Bar Presence', na=False)]
            df = df[df['Detector'].notna()]
            
            # Select and format final columns
            result = df[['SignalID', 'Detector', 'CallPhase', 'TimeFromStopBar']].copy()
            result['SignalID'] = result['SignalID'].astype('category')
            result['Detector'] = result['Detector'].astype('category')
            result['CallPhase'] = result['CallPhase'].astype('category')
            
            if isinstance(date_, str):
                date_obj = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_obj = date_.date()
            else:
                date_obj = date_
            
            result['Date'] = date_obj
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting split failure detector config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_sf_config

def get_det_config_vol(get_det_config_func: Callable) -> Callable:
    """
    Create a function to get volume detector configuration
    
    Args:
        get_det_config_func: Base detector config function
    
    Returns:
        Function that returns volume detector configuration
    """
    
    def get_vol_config(date_: Union[str, date, datetime]) -> pd.DataFrame:
        """
        Get volume detector configuration for a specific date
        
        Args:
            date_: Date to get configuration for
        
        Returns:
            DataFrame with volume detector configuration
        """
        
        try:
            df = get_det_config_func(date_)
            
            if df.empty:
                return df
            
            # Select and format columns
            result = df[['SignalID', 'Detector', 'CallPhase', 'CountPriority', 'TimeFromStopBar']].copy()
            result['SignalID'] = result['SignalID'].astype('category')
            result['Detector'] = result['Detector'].astype('category')
            result['CallPhase'] = result['CallPhase'].astype('category')
            result['CountPriority'] = result['CountPriority'].astype(int)
            
            if isinstance(date_, str):
                date_obj = pd.to_datetime(date_).date()
            elif isinstance(date_, datetime):
                date_obj = date_.date()
            else:
                date_obj = date_
            
            result['Date'] = date_obj
            
            # For each SignalID and CallPhase combination, keep only minimum CountPriority
            result['minCountPriority'] = (result.groupby(['SignalID', 'CallPhase'])['CountPriority']
                                        .transform('min'))
            result = result[result['CountPriority'] == result['minCountPriority']]
            result = result.drop('minCountPriority', axis=1)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting volume detector config for {date_}: {e}")
            return pd.DataFrame()
    
    return get_vol_config

def get_latest_det_config(conf: dict) -> pd.DataFrame:
    """
    Get the most recent detector configuration file
    
    Args:
        conf: Configuration dictionary containing bucket info
    
    Returns:
        DataFrame with latest detector configuration
    """
    
    try:
        from datetime import timezone
        
        # Start with today's date in Eastern timezone
        date_ = datetime.now(timezone.utc).astimezone().date()
        
        s3_client = boto3.client('s3')
        
        # Work backward from today until we find a config file
        max_attempts = 30  # Don't go back more than 30 days
        attempts = 0
        
        while attempts < max_attempts:
            date_str = date_.strftime('%Y-%m-%d')
            s3_prefix = f"config/atspm_det_config_good/date={date_str}/"
            
            response = s3_client.list_objects_v2(
                Bucket=conf['bucket'],
                Prefix=s3_prefix
            )
            
            if 'Contents' in response and len(response['Contents']) > 0:
                # Found a config file
                s3_key = response['Contents'][0]['Key']
                det_config = wr.s3.read_feather(path=f"s3://{conf['bucket']}/{s3_key}")
                logger.info(f"Found latest detector config for date: {date_str}")
                return det_config
            
            # Move to previous day
            date_ = date_ - timedelta(days=1)
            attempts += 1
        
        raise ValueError(f"No detector config found in the last {max_attempts} days")
        
    except Exception as e:
        logger.error(f"Error getting latest detector config: {e}")
        raise

def get_rsu_config_(bucket: str) -> Callable:
    """
    Create a function to get RSU (Roadside Unit) configuration
    
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
            rsu_config = wr.s3.read_excel(
                path=f"s3://{bucket}/{latest_file}"
            )
            
            logger.info(f"Loaded RSU config from {latest_file}")
            return rsu_config
            
        except Exception as e:
            logger.error(f"Error getting RSU config: {e}")
            return pd.DataFrame()
    
    return get_rsu_config

# Helper functions for signal and corridor operations
def get_signal_ids_by_corridor(corridors: pd.DataFrame, corridor: str) -> List[str]:
    """
    Get list of signal IDs for a specific corridor
    
    Args:
        corridors: Corridors DataFrame
        corridor: Corridor name
    
    Returns:
        List of signal IDs
    """
    
    try:
        corridor_signals = corridors[
            corridors['Corridor'] == corridor
        ]['SignalID'].astype(str).tolist()
        
        return corridor_signals
        
    except Exception as e:
        logger.error(f"Error getting signals for corridor {corridor}: {e}")
        return []

def get_signal_ids_by_zone(corridors: pd.DataFrame, zone: str) -> List[str]:
    """
    Get list of signal IDs for a specific zone
    
    Args:
        corridors: Corridors DataFrame
        zone: Zone name
    
    Returns:
        List of signal IDs
    """
    
    try:
        zone_signals = corridors[
            corridors['Zone'] == zone
                ]['SignalID'].astype(str).tolist()
        
        return zone_signals
        
    except Exception as e:
        logger.error(f"Error getting signals for zone {zone}: {e}")
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

def validate_signal_config(signal_config: pd.DataFrame, 
                          required_cols: List[str] = None) -> bool:
    """
    Validate signal configuration DataFrame
    
    Args:
        signal_config: Signal configuration DataFrame
        required_cols: List of required column names
    
    Returns:
        Boolean indicating if validation passed
    """
    
    if required_cols is None:
        required_cols = ['SignalID', 'Zone', 'Corridor', 'Latitude', 'Longitude']
    
    try:
        # Check if all required columns exist
        missing_cols = set(required_cols) - set(signal_config.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check for duplicate signal IDs
        if signal_config['SignalID'].duplicated().any():
            duplicates = signal_config[signal_config['SignalID'].duplicated()]['SignalID'].tolist()
            logger.error(f"Duplicate signal IDs found: {duplicates}")
            return False
        
        # Check for missing coordinates
        missing_coords = signal_config[
            signal_config['Latitude'].isna() | signal_config['Longitude'].isna()
        ]
        if len(missing_coords) > 0:
            logger.warning(f"Signals with missing coordinates: {len(missing_coords)}")
        
        # Check for valid coordinate ranges
        invalid_coords = signal_config[
            (signal_config['Latitude'] < -90) | (signal_config['Latitude'] > 90) |
            (signal_config['Longitude'] < -180) | (signal_config['Longitude'] > 180)
        ]
        if len(invalid_coords) > 0:
            logger.error(f"Signals with invalid coordinates: {len(invalid_coords)}")
            return False
        
        logger.info("Signal configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating signal configuration: {e}")
        return False

def read_corridors_from_file(filename: Union[str, Path]) -> pd.DataFrame:
    """
    Read corridors from various file formats
    
    Args:
        filename: Filename for corridors file
    
    Returns:
        DataFrame with corridors
    """
    
    try:
        filename = Path(filename)
        
        if filename.suffix == '.feather':
            return pd.read_feather(filename)
        elif filename.suffix in ['.xlsx', '.xls']:
            return get_corridors(str(filename))
        elif filename.suffix == '.parquet':
            return pd.read_parquet(filename)
        elif filename.suffix == '.csv':
            return pd.read_csv(filename)
        else:
            raise ValueError(f"Unsupported file format: {filename.suffix}")
            
    except Exception as e:
        logger.error(f"Error reading corridors from {filename}: {e}")
        raise

def save_corridors_to_file(corridors: pd.DataFrame, 
                          filename: Union[str, Path],
                          format: str = 'auto') -> bool:
    """
    Save corridors DataFrame to file
    
    Args:
        corridors: Corridors DataFrame
        filename: Output filename
        format: File format ('auto', 'feather', 'parquet', 'csv', 'excel')
    
    Returns:
        Boolean indicating success
    """
    
    try:
        filename = Path(filename)
        
        if format == 'auto':
            format = filename.suffix.lstrip('.')
        
        if format == 'feather':
            corridors.to_feather(filename)
        elif format == 'parquet':
            corridors.to_parquet(filename, index=False)
        elif format == 'csv':
            corridors.to_csv(filename, index=False)
        elif format in ['xlsx', 'excel']:
            corridors.to_excel(filename, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Corridors saved to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving corridors to {filename}: {e}")
        return False

def filter_corridors_by_criteria(corridors: pd.DataFrame, 
                                criteria: Dict[str, Union[str, List[str]]]) -> pd.DataFrame:
    """
    Filter corridors by various criteria
    
    Args:
        corridors: Corridors DataFrame
        criteria: Dictionary of filter criteria
    
    Returns:
        Filtered DataFrame
    """
    
    try:
        filtered = corridors.copy()
        
        for column, values in criteria.items():
            if column not in filtered.columns:
                logger.warning(f"Column '{column}' not found in corridors DataFrame")
                continue
            
            if isinstance(values, str):
                values = [values]
            
            filtered = filtered[filtered[column].isin(values)]
        
        logger.info(f"Filtered corridors from {len(corridors)} to {len(filtered)} rows")
        return filtered
        
    except Exception as e:
        logger.error(f"Error filtering corridors: {e}")
        return corridors

def get_corridors_summary(corridors: pd.DataFrame) -> Dict[str, int]:
    """
    Get summary statistics for corridors DataFrame
    
    Args:
        corridors: Corridors DataFrame
    
    Returns:
        Dictionary with summary statistics
    """
    
    try:
        summary = {
            'total_signals': len(corridors),
            'total_corridors': corridors['Corridor'].nunique(),
            'total_zones': corridors['Zone'].nunique(),
            'total_zone_groups': corridors['Zone_Group'].nunique(),
            'signals_with_coordinates': len(corridors[
                corridors['Latitude'].notna() & corridors['Longitude'].notna()
            ]),
            'signals_missing_coordinates': len(corridors[
                corridors['Latitude'].isna() | corridors['Longitude'].isna()
            ])
        }
        
        # Add agency breakdown if available
        if 'Agency' in corridors.columns:
            summary['agencies'] = corridors['Agency'].nunique()
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating corridors summary: {e}")
        return {}

# Factory functions to create configured functions
def create_config_functions(conf: dict) -> dict:
    """
    Create all configuration functions with the given configuration
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        Dictionary of configured functions
    """
    
    try:
        bucket = conf['bucket']
        
        # Create all the function factories
        functions = {
            'get_ped_config': get_ped_config_(bucket),
            'get_ped_config_cel': get_ped_config_cel_(bucket, conf.get('athena', {})),
            'get_det_config': get_det_config_(bucket, "atspm_det_config_good"),
            'get_rsu_config': get_rsu_config_(bucket)
        }
        
        # Create derived detector config functions
        base_det_config = functions['get_det_config']
        functions.update({
            'get_det_config_aog': get_det_config_aog(base_det_config),
            'get_det_config_qs': get_det_config_qs(base_det_config, bucket),
            'get_det_config_sf': get_det_config_sf(base_det_config),
            'get_det_config_vol': get_det_config_vol(base_det_config)
        })
        
        logger.info("Created all configuration functions")
        return functions
        
    except Exception as e:
        logger.error(f"Error creating configuration functions: {e}")
        return {}

# Example usage and initialization
def initialize_configs(conf: dict) -> dict:
    """
    Initialize all configuration functions and data
    
    Args:
        conf: Configuration dictionary
    
    Returns:
        Dictionary containing all config functions and base data
    """
    
    try:
        # Create all configuration functions
        config_functions = create_config_functions(conf)
        
        # Load base corridors data if path is provided
        corridors = None
        if 'corridors_file' in conf:
            corridors = read_corridors_from_file(conf['corridors_file'])
            if not check_corridors(corridors):
                logger.warning("Corridors validation failed")
        
        # Create mappings if corridors are available
        zone_mapping = None
        corridor_mapping = None
        if corridors is not None:
            zone_mapping = create_zone_mapping(corridors)
            corridor_mapping = create_corridor_mapping(corridors)
        
        result = {
            'functions': config_functions,
            'corridors': corridors,
            'zone_mapping': zone_mapping,
            'corridor_mapping': corridor_mapping
        }
        
        logger.info("Configuration initialization completed")
        return result
        
    except Exception as e:
        logger.error(f"Error initializing configurations: {e}")
        return {}

# Global configuration setup (equivalent to R's global assignment)
def setup_global_configs(conf: dict):
    """
    Set up global configuration functions (equivalent to R's global environment)
    
    Args:
        conf: Configuration dictionary
    """
    
    global get_ped_config, get_det_config, get_det_config_aog, get_det_config_qs
    global get_det_config_sf, get_det_config_vol, get_rsu_config
    
    try:
        config_functions = create_config_functions(conf)
        
        # Assign to global variables
        get_ped_config = config_functions['get_ped_config']
        get_det_config = config_functions['get_det_config'] 
        get_det_config_aog = config_functions['get_det_config_aog']
        get_det_config_qs = config_functions['get_det_config_qs']
        get_det_config_sf = config_functions['get_det_config_sf']
        get_det_config_vol = config_functions['get_det_config_vol']
        get_rsu_config = config_functions['get_rsu_config']
        
        logger.info("Global configuration functions set up successfully")
        
    except Exception as e:
        logger.error(f"Error setting up global configurations: {e}")
        raise