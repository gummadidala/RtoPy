import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import logging
import traceback
import pickle
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add the parent directory to the path to import local modules
sys.path.append(str(Path(__file__).parent.parent))

from s3_parquet_io import s3_read_parquet_parallel
from utilities import keep_trying
from aggregations import get_period_sum, get_cor_monthly_avg_by_period, get_period_avg, sigify
from configs import get_date_from_string
from write_sigops_to_db import get_aurora_connection, append_to_database
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def floor_date(dt, unit='months'):
    """
    Floor date to the beginning of the specified unit
    
    Args:
        dt: datetime object
        unit: Unit to floor to ('months', 'days', etc.)
    
    Returns:
        Floored datetime
    """
    if unit == 'months':
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif unit == 'days':
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        return dt

def addtoRDS(df: pd.DataFrame, filename: str, value_col: str, 
            rds_start_date: date, calcs_start_date: date):
    """
    Add data to RDS file (equivalent to R's addtoRDS function)
    
    Args:
        df: DataFrame to save
        filename: Name of the pickle file
        value_col: Name of the value column
        rds_start_date: Start date for RDS data
        calcs_start_date: Start date for calculations
    """
    try:
        # Filter data from calcs_start_date onwards
        if 'Hour' in df.columns:
            df_filtered = df[df['Hour'] >= pd.Timestamp(calcs_start_date)]
        elif 'Date' in df.columns:
            df_filtered = df[df['Date'] >= calcs_start_date]
        else:
            df_filtered = df
        
        # Save to pickle file (Python equivalent of RDS)
        with open(filename, 'wb') as f:
            pickle.dump(df_filtered, f)
        
        logger.info(f"Saved {len(df_filtered)} records to {filename}")
        
    except Exception as e:
        logger.error(f"Error saving to {filename}: {e}")

def load_config():
    """Load configuration from YAML file"""
    try:
        # Source equivalent: Monthly_Report_Package_init.R
        config_path = "Monthly_Report.yaml"
        with open(config_path, 'r') as file:
            conf = yaml.safe_load(file)
        return conf
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

def main():
    """Main function - equivalent to the R script execution"""
    
    try:
        # Load configuration
        conf = load_config()
        
        # For hourly counts (no monthly or weekly), go back to first missing day in the database
        calcs_start_date = get_date_from_string(
            conf['start_date'], 
            table_include_regex_pattern="sig_hr_", 
            exceptions=0
        )
        
        # Get report end date from config
        report_end_date = pd.to_datetime(conf.get('report_end_date', datetime.now())).date()
        report_end_date = min(report_end_date, calcs_start_date + timedelta(days=7))
        
        print(f"{datetime.now()} 1hr Package Start Date: {calcs_start_date}")
        print(f"{datetime.now()} Report End Date: {report_end_date}")
        
        # Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
        rds_start_date = calcs_start_date - timedelta(days=1)
        
        # Only keep 6 months of data for 1hr aggregations
        report_start_date = floor_date(pd.Timestamp(report_end_date), unit='months').date() - relativedelta(months=6)
        
        # Load corridors data (assuming this is available from config or separate file)
        # You'll need to implement this based on your corridor data source
        try:
            corridors = pd.read_csv(conf.get('corridors_file', 'corridors.csv'))
        except:
            logger.warning("Corridors file not found, creating empty DataFrame")
            corridors = pd.DataFrame()
        
        # Load signals list
        try:
            signals_list = conf.get('signals_list', [])
        except:
            signals_list = []
        
        # DETECTOR UPTIME ###########################################################
        print(f"{datetime.now()} Vehicle Detector Uptime [1 of 29 (sigops 1hr)]")
        
        # DAILY PEDESTRIAN PUSHBUTTON UPTIME ######################################
        print(f"{datetime.now()} Ped Pushbutton Uptime [2 of 29 (sigops 1hr)]")
        
        # WATCHDOG #################################################################
        print(f"{datetime.now()} watchdog alerts [3 of 29 (sigops 1hr)]")
        
        # DAILY PEDESTRIAN ACTIVATIONS ############################################
        print(f"{datetime.now()} Daily Pedestrian Activations [4 of 29 (sigops 1hr)]")
        
        # HOURLY PEDESTRIAN ACTIVATIONS ###########################################
        print(f"{datetime.now()} Hourly Pedestrian Activations [5 of 29 (sigops 1hr)]")
        
        try:
            paph = s3_read_parquet_parallel(
                bucket=conf['bucket'],
                table_name="counts_ped_1hr",
                start_date=rds_start_date,
                end_date=report_end_date,
                signals_list=signals_list,
                parallel=False
            )
            
            if not paph.empty:
                # Filter out rows where CallPhase is NA
                paph = paph.dropna(subset=['CallPhase'])
                
                # Convert data types
                paph['SignalID'] = paph['SignalID'].astype('category')
                paph['Detector'] = paph['Detector'].astype('category')
                paph['CallPhase'] = paph['CallPhase'].astype('category')
                paph['Date'] = pd.to_datetime(paph['Date']).dt.date
                paph['DOW'] = pd.to_datetime(paph['Date']).dt.dayofweek + 1  # Monday=1
                paph['Week'] = pd.to_datetime(paph['Date']).dt.isocalendar().week
                paph['vol'] = pd.to_numeric(paph['vol'], errors='coerce')
                
                # Load bad ped detectors
                bad_ped_detectors = s3_read_parquet_parallel(
                    bucket=conf['bucket'],
                    table_name="bad_ped_detectors",
                    start_date=rds_start_date,
                    end_date=report_end_date,
                    signals_list=signals_list,
                    parallel=False
                )
                
                if not bad_ped_detectors.empty:
                    bad_ped_detectors['SignalID'] = bad_ped_detectors['SignalID'].astype('category')
                    bad_ped_detectors['Detector'] = bad_ped_detectors['Detector'].astype('category')
                
                # Filter out bad days
                paph = paph.rename(columns={'Timeperiod': 'Hour', 'vol': 'paph'})
                paph = paph[['SignalID', 'Hour', 'CallPhase', 'Detector', 'paph']]
                
                # Anti-join with bad_ped_detectors
                if not bad_ped_detectors.empty:
                    merge_cols = ['SignalID', 'Detector']
                    paph = paph.merge(bad_ped_detectors[merge_cols], on=merge_cols, how='left', indicator=True)
                    paph = paph[paph['_merge'] == 'left_only'].drop(['_merge'], axis=1)
                
                # Create all timeperiods
                min_hour = pd.to_datetime(paph['Hour']).min().floor('D')
                max_hour = pd.to_datetime(paph['Hour']).max().floor('D') + pd.Timedelta(days=1) - pd.Timedelta(hours=1)
                all_timeperiods = pd.date_range(start=min_hour, end=max_hour, freq='H')
                
                # Expand grid and fill missing values
                signal_detector_combinations = paph[['SignalID', 'Detector', 'CallPhase']].drop_duplicates()
                expanded_grid = []
                
                for _, row in signal_detector_combinations.iterrows():
                    for hour in all_timeperiods:
                        expanded_grid.append({
                            'SignalID': row['SignalID'],
                            'Detector': row['Detector'],
                            'CallPhase': row['CallPhase'],
                            'Hour': hour
                        })
                
                expanded_df = pd.DataFrame(expanded_grid)
                paph = expanded_df.merge(paph, on=['SignalID', 'Detector', 'CallPhase', 'Hour'], how='left')
                paph['paph'] = paph['paph'].fillna(0)
                
                # Get period sum
                hourly_pa = get_period_sum(paph, "paph", "Hour")
                cor_hourly_pa = get_cor_monthly_avg_by_period(hourly_pa, corridors, "paph", "Hour")
                
                hourly_pa = sigify(hourly_pa, cor_hourly_pa, corridors)
                hourly_pa = hourly_pa[['Zone_Group', 'Corridor', 'Hour', 'paph', 'delta']]
                
                addtoRDS(hourly_pa, "hourly_pa.pkl", "paph", rds_start_date, calcs_start_date)
                
                # Clean up memory
                del paph, bad_ped_detectors, hourly_pa, cor_hourly_pa
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in pedestrian activations: {traceback.format_exc()}")
        
        # GET PEDESTRIAN DELAY ####################################################
        print(f"{datetime.now()} Pedestrian Delay [6 of 29 (sigops 1hr)]")
        
        # GET COMMUNICATIONS UPTIME ###############################################
        print(f"{datetime.now()} Communication Uptime [7 of 29 (sigops 1hr)]")
        
        # DAILY VOLUMES ###########################################################
        print(f"{datetime.now()} Daily Volumes [8 of 29 (sigops 1hr)]")
        
        # HOURLY VOLUMES ##########################################################
        print(f"{datetime.now()} Hourly Volumes [9 of 29 (sigops 1hr)]")
        
        try:
            vph = s3_read_parquet_parallel(
                bucket=conf['bucket'],
                table_name="vehicles_ph",
                start_date=rds_start_date,
                end_date=report_end_date,
                signals_list=signals_list
            )
            
            if not vph.empty:
                vph['SignalID'] = vph['SignalID'].astype('category')
                vph['CallPhase'] = pd.Categorical([2] * len(vph))  # Hack because next function needs a CallPhase
                vph['Date'] = pd.to_datetime(vph['Date']).dt.date
                
                hourly_vol = get_period_sum(vph, "vph", "Hour")
                cor_hourly_vol = get_cor_monthly_avg_by_period(hourly_vol, corridors, "vph", "Hour")
                
                hourly_vol = sigify(hourly_vol, cor_hourly_vol, corridors)
                hourly_vol = hourly_vol[['Zone_Group', 'Corridor', 'Hour', 'vph', 'delta']]
                
                addtoRDS(hourly_vol, "hourly_vol.pkl", "vph", rds_start_date, calcs_start_date)
                
                del vph, hourly_vol, cor_hourly_vol
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in hourly volumes: {traceback.format_exc()}")
        
        # DAILY THROUGHPUT ########################################################
        print(f"{datetime.now()} Daily Throughput [10 of 29 (sigops 1hr)]")
        
        # DAILY ARRIVALS ON GREEN #################################################
        print(f"{datetime.now()} Daily AOG [11 of 29 (sigops 1hr)]")
        
        # HOURLY ARRIVALS ON GREEN ################################################
        print(f"{datetime.now()} Hourly AOG [12 of 29 (sigops 1hr)]")
        
        try:
            aog = s3_read_parquet_parallel(
                bucket=conf['bucket'],
                table_name="arrivals_on_green",
                start_date=rds_start_date,
                end_date=report_end_date,
                signals_list=signals_list
            )
            
            if not aog.empty:
                aog['SignalID'] = aog['SignalID'].astype('category')
                aog['CallPhase'] = aog['CallPhase'].astype('category')
                aog['Date'] = pd.to_datetime(aog['Date']).dt.date
                aog['DOW'] = pd.to_datetime(aog['Date']).dt.dayofweek + 1
                aog['Week'] = pd.to_datetime(aog['Date']).dt.isocalendar().week
                
                # Rename and select columns
                aog = aog.rename(columns={'Date_Hour': 'Hour'})
                aog = aog[['SignalID', 'CallPhase', 'Hour', 'aog', 'pr', 'vol', 'Date']]
                
                # Complete time series
                signal_combinations = aog[['SignalID', 'Date']].drop_duplicates()
                hour_range = pd.date_range(
                    start=pd.Timestamp(rds_start_date),
                    end=pd.Timestamp(report_end_date) - pd.Timedelta(hours=1),
                    freq='H'
                )
                
                expanded_grid = []
                for _, row in signal_combinations.iterrows():
                    for hour in hour_range:
                        expanded_grid.append({
                            'SignalID': row['SignalID'],
                            'Date': row['Date'],
                            'Hour': hour})
                
                expanded_df = pd.DataFrame(expanded_grid)
                aog = expanded_df.merge(aog, on=['SignalID', 'Date', 'Hour'], how='left')
                
                hourly_aog = get_period_avg(aog, "aog", "Hour", "vol")
                cor_hourly_aog = get_cor_monthly_avg_by_period(hourly_aog, corridors, "aog", "Hour")
                
                hourly_aog = sigify(hourly_aog, cor_hourly_aog, corridors)
                hourly_aog = hourly_aog[['Zone_Group', 'Corridor', 'Hour', 'aog', 'delta']]
                
                addtoRDS(hourly_aog, "hourly_aog.pkl", "aog", rds_start_date, calcs_start_date)
                
                del hourly_aog, cor_hourly_aog
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in hourly AOG: {traceback.format_exc()}")
        
        # DAILY PROGRESSION RATIO #################################################
        print(f"{datetime.now()} Daily Progression Ratio [13 of 29 (sigops 1hr)]")
        
        # HOURLY PROGESSION RATIO #################################################
        print(f"{datetime.now()} Hourly Progression Ratio [14 of 29 (sigops 1hr)]")
        
        try:
            # Use the aog data from previous step
            if 'aog' in locals() and not aog.empty:
                # Complete time series for progression ratio
                signal_combinations = aog[['SignalID', 'Date']].drop_duplicates()
                hour_range = pd.date_range(
                    start=pd.Timestamp(rds_start_date),
                    end=pd.Timestamp(report_end_date) - pd.Timedelta(hours=1),
                    freq='H'
                )
                
                expanded_grid = []
                for _, row in signal_combinations.iterrows():
                    for hour in hour_range:
                        expanded_grid.append({
                            'SignalID': row['SignalID'],
                            'Date': row['Date'],
                            'Hour': hour
                        })
                
                expanded_df = pd.DataFrame(expanded_grid)
                aog_for_pr = expanded_df.merge(aog, on=['SignalID', 'Date', 'Hour'], how='left')
                
                hourly_pr = get_period_avg(aog_for_pr, "pr", "Hour", "vol")
                cor_hourly_pr = get_cor_monthly_avg_by_period(hourly_pr, corridors, "pr", "Hour")
                
                hourly_pr = sigify(hourly_pr, cor_hourly_pr, corridors)
                hourly_pr = hourly_pr[['Zone_Group', 'Corridor', 'Hour', 'pr', 'delta']]
                
                addtoRDS(hourly_pr, "hourly_pr.pkl", "pr", rds_start_date, calcs_start_date)
                
                del aog, hourly_pr, cor_hourly_pr
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in hourly progression ratio: {traceback.format_exc()}")
        
        # DAILY SPLIT FAILURES ####################################################
        print(f"{datetime.now()} Daily Split Failures [15 of 29 (sigops 1hr)]")
        
        # HOURLY SPLIT FAILURES ###################################################
        print(f"{datetime.now()} Hourly Split Failures [16 of 29 (sigops 1hr)]")
        
        try:
            # Define callback function for filtering
            def filter_callphase_0(df):
                return df[df['CallPhase'] == 0] if 'CallPhase' in df.columns else df
            
            sf = s3_read_parquet_parallel(
                bucket=conf['bucket'],
                table_name="split_failures",
                start_date=rds_start_date,
                end_date=report_end_date,
                signals_list=signals_list,
                callback=filter_callphase_0
            )
            
            if not sf.empty:
                sf['SignalID'] = sf['SignalID'].astype('category')
                sf['CallPhase'] = sf['CallPhase'].astype('category')
                sf['Date'] = pd.to_datetime(sf['Date']).dt.date
                
                sf = sf.rename(columns={'Date_Hour': 'Hour'})
                sf = sf[['SignalID', 'CallPhase', 'Hour', 'sf_freq', 'Date']]
                
                # Complete time series with fill value 0 for sf_freq
                signal_combinations = sf[['SignalID', 'Date', 'CallPhase']].drop_duplicates()
                hour_range = pd.date_range(
                    start=pd.Timestamp(rds_start_date),
                    end=pd.Timestamp(report_end_date) - pd.Timedelta(hours=1),
                    freq='H'
                )
                
                expanded_grid = []
                for _, row in signal_combinations.iterrows():
                    for hour in hour_range:
                        expanded_grid.append({
                            'SignalID': row['SignalID'],
                            'Date': row['Date'],
                            'CallPhase': row['CallPhase'],
                            'Hour': hour
                        })
                
                expanded_df = pd.DataFrame(expanded_grid)
                sf = expanded_df.merge(sf, on=['SignalID', 'Date', 'CallPhase', 'Hour'], how='left')
                sf['sf_freq'] = sf['sf_freq'].fillna(0)
                
                hourly_sf = get_period_avg(sf, "sf_freq", "Hour")
                cor_hourly_sf = get_cor_monthly_avg_by_period(hourly_sf, corridors, "sf_freq", "Hour")
                
                hourly_sf = sigify(hourly_sf, cor_hourly_sf, corridors)
                hourly_sf = hourly_sf[['Zone_Group', 'Corridor', 'Hour', 'sf_freq', 'delta']]
                
                addtoRDS(hourly_sf, "hourly_sf.pkl", "sf_freq", rds_start_date, calcs_start_date)
                
                del sf, hourly_sf, cor_hourly_sf
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in hourly split failures: {traceback.format_exc()}")
        
        # DAILY QUEUE SPILLBACK ###################################################
        print(f"{datetime.now()} Daily Queue Spillback [17 of 29 (sigops 1hr)]")
        
        # HOURLY QUEUE SPILLBACK ##################################################
        print(f"{datetime.now()} Hourly Queue Spillback [18 of 29 (sigops 1hr)]")
        
        try:
            qs = s3_read_parquet_parallel(
                bucket=conf['bucket'],
                table_name="queue_spillback",
                start_date=calcs_start_date,  # Note: using calcs_start_date here as in R
                end_date=report_end_date,
                signals_list=signals_list
            )
            
            if not qs.empty:
                qs['SignalID'] = qs['SignalID'].astype('category')
                qs['CallPhase'] = qs['CallPhase'].astype('category')
                qs['Date'] = pd.to_datetime(qs['Date']).dt.date
                
                qs = qs.rename(columns={'Date_Hour': 'Hour'})
                qs = qs[['SignalID', 'CallPhase', 'Hour', 'qs_freq', 'Date']]
                
                # Complete time series with fill value 0 for qs_freq
                signal_combinations = qs[['SignalID', 'Date', 'CallPhase']].drop_duplicates()
                hour_range = pd.date_range(
                    start=pd.Timestamp(rds_start_date),
                    end=pd.Timestamp(report_end_date) - pd.Timedelta(hours=1),
                    freq='H'
                )
                
                expanded_grid = []
                for _, row in signal_combinations.iterrows():
                    for hour in hour_range:
                        expanded_grid.append({
                            'SignalID': row['SignalID'],
                            'Date': row['Date'],
                            'CallPhase': row['CallPhase'],
                            'Hour': hour
                        })
                
                expanded_df = pd.DataFrame(expanded_grid)
                qs = expanded_df.merge(qs, on=['SignalID', 'Date', 'CallPhase', 'Hour'], how='left')
                qs['qs_freq'] = qs['qs_freq'].fillna(0)
                
                hourly_qs = get_period_avg(qs, "qs_freq", "Hour")
                cor_hourly_qs = get_cor_monthly_avg_by_period(hourly_qs, corridors, "qs_freq", "Hour")
                
                hourly_qs = sigify(hourly_qs, cor_hourly_qs, corridors)
                hourly_qs = hourly_qs[['Zone_Group', 'Corridor', 'Hour', 'qs_freq', 'delta']]
                
                addtoRDS(hourly_qs, "hourly_qs.pkl", "qs_freq", rds_start_date, calcs_start_date)
                
                del qs, hourly_qs, cor_hourly_qs
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error in hourly queue spillback: {traceback.format_exc()}")
        
        # TRAVEL TIME AND BUFFER TIME INDEXES #####################################
        print(f"{datetime.now()} Travel Time Indexes [19 of 29 (sigops 1hr)]")
        
        # CCTV UPTIME From 511 and Encoders
        print(f"{datetime.now()} CCTV Uptimes [20 of 29 (sigops 1hr)]")
        
        # ACTIVITIES ##############################################################
        print(f"{datetime.now()} TEAMS [21 of 29 (sigops 1hr)]")
        
        # USER DELAY COSTS   ######################################################
        print(f"{datetime.now()} User Delay Costs [22 of 29 (sigops 1hr)]")
        
        # Flash Events ############################################################
        print(f"{datetime.now()} Flash Events [23 of 29 (sigops 1hr)]")
        
        # BIKE/PED SAFETY INDEX ###################################################
        print(f"{datetime.now()} Bike/Ped Safety Index [24 of 29 (sigops 1hr)]")
        
        # RELATIVE SPEED INDEX ####################################################
        print(f"{datetime.now()} Relative Speed Index [25 of 29 (sigops 1hr)]")
        
        # CRASH INDICES ###########################################################
        print(f"{datetime.now()} Crash Indices [26 of 29 (sigops 1hr)]")
        
        # Package up for Flexdashboard
        # All of the hourly bins.
        
        try:
            sig = {
                'hr': {}
            }
            
            # Load all hourly data files
            hourly_files = {
                "vph": "hourly_vol.pkl",
                "paph": "hourly_pa.pkl", 
                "aogh": "hourly_aog.pkl",
                "prh": "hourly_pr.pkl",
                "sfh": "hourly_sf.pkl",
                "qsh": "hourly_qs.pkl"
            }
            
            for key, filename in hourly_files.items():
                try:
                    if os.path.exists(filename):
                        with open(filename, 'rb') as f:
                            sig['hr'][key] = pickle.load(f)
                        logger.info(f"Loaded {filename}")
                    else:
                        logger.warning(f"File not found: {filename}")
                        sig['hr'][key] = pd.DataFrame()
                except Exception as e:
                    logger.error(f"Error loading {filename}: {e}")
                    sig['hr'][key] = pd.DataFrame()
            
        except Exception as e:
            print("ENCOUNTERED AN ERROR:")
            print(e)
            logger.error(f"Error packaging hourly data: {traceback.format_exc()}")
        
        print(f"{datetime.now()} Upload to AWS [28 of 29 (sigops 1hr)]")
        
        print(f"{datetime.now()} Write to Database [29 of 29 (sigops 1hr)]")
        
        # Update Aurora Nightly
        try:
            aurora = keep_trying(get_aurora_connection, n_tries=5)
            
            # Append to database
            append_to_database(
                aurora, 
                sig, 
                "sig", 
                calcs_start_date, 
                report_start_date=report_start_date, 
                report_end_date=None
            )
            
            aurora.close()
            logger.info("Successfully wrote to Aurora database")
            
        except Exception as e:
            logger.error(f"Error writing to database: {e}")
            logger.error(traceback.format_exc())
        
        print(f"{datetime.now()} Monthly Report Package 1hr completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in main function: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()

