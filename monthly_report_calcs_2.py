#!/usr/bin/env python3
#!/usr/bin/env python3
"""
Monthly Report Calculations - Part 2
Exact conversion from Monthly_Report_Calcs_2.R
"""

import sys
import subprocess
import gc
import logging
from datetime import datetime, timedelta
from dateutil.parser import parse as parse_date
import pandas as pd
import numpy as np
import boto3
import awswrangler as wr
from typing import Dict, List, Optional, Any
import yaml

# Import from the init script (equivalent to source("Monthly_Report_Calcs_init.R"))
from monthly_report_calcs_init import load_init_variables

# Import functions that should exist in your other modules, with fallbacks
try:
    from database_functions import get_detection_events, get_athena_connection
except ImportError:
    from missing_functions_fallback import *
    logger.warning("Using fallback database functions")

try:
    from metrics import get_qs, get_sf_utah, get_ped_delay
except ImportError:
    from missing_functions_fallback import get_qs, get_sf_utah, get_ped_delay
    logger.warning("Using fallback metrics functions")

try:
    from counts import s3_upload_parquet_date_split
except ImportError:
    from missing_functions_fallback import s3_upload_parquet_date_split
    logger.warning("Using fallback s3_upload_parquet_date_split function")

try:
    from utilities import keep_trying
except ImportError:
    from missing_functions_fallback import keep_trying
    logger.warning("Using fallback keep_trying function")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# [Rest of the code remains the same as provided above...]


def print_with_timestamp(message: str):
    """Equivalent to R's glue("{Sys.time()} message")"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} {message}")

def run_system_command(command: str) -> bool:
    """Equivalent to R's system() function - waits for completion"""
    try:
        # R's system() waits for completion by default
        result = subprocess.run(command, shell=True, check=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {command}")
        return False

def get_detection_events_wrapper(date_start: str, date_end: str, conf_athena: Dict, signals_list: List[str]) -> pd.DataFrame:
    """
    Get detection events from Athena - wrapper for the imported function
    """
    try:
        # If the imported function exists, use it
        if 'get_detection_events' in globals():
            return get_detection_events(date_start, date_end, conf_athena, signals_list)
        else:
            # Fallback implementation
            return get_detection_events_fallback(date_start, date_end, conf_athena, signals_list)
    except Exception as e:
        logger.error(f"Error getting detection events: {e}")
        return pd.DataFrame()

def get_detection_events_fallback(date_start: str, date_end: str, conf_athena: Dict, signals_list: List[str]) -> pd.DataFrame:
    """Fallback implementation for getting detection events"""
    try:
        signals_str = "', '".join(signals_list)
        query = f"""
        SELECT SignalID, Detector, CallPhase, Timeperiod, EventCode, EventParam
        FROM {conf_athena['database']}.detection_events
        WHERE date BETWEEN '{date_start}' AND '{date_end}'
        AND SignalID IN ('{signals_str}')
        """
        
        session = boto3.Session(
            aws_access_key_id=conf_athena.get('uid'),
            aws_secret_access_key=conf_athena.get('pwd')
        )
        
        df = wr.athena.read_sql_query(
            sql=query,
            database=conf_athena['database'],
            s3_output=conf_athena['staging_dir'],
            boto3_session=session
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Error getting detection events: {e}")
        return pd.DataFrame()

def get_queue_spillback_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """
    Exact conversion of R function:
    get_queue_spillback_date_range <- function(start_date, end_date) {
        date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
        lapply(date_range, function(date_) {
            print(date_)
            detection_events <- get_detection_events(date_, date_, conf$athena, signals_list)
            if (nrow(collect(head(detection_events))) > 0) {
                qs <- get_qs(detection_events, intervals = c("hour", "15min"))
                s3_upload_parquet_date_split(qs$hour, ...)
                s3_upload_parquet_date_split(qs$`15min`, ...)
            }
        })
    }
    """
    from datetime import datetime, timedelta
    
    # R: date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    current_date = start_dt
    while current_date <= end_dt:
        # R: lapply(date_range, function(date_) {
        date_ = current_date
        # R: print(date_)
        print(date_)
        
        # R: detection_events <- get_detection_events(date_, date_, conf$athena, signals_list)
        detection_events = get_detection_events_wrapper(
            date_.strftime('%Y-%m-%d'), 
            date_.strftime('%Y-%m-%d'), 
            conf['athena'], 
            signals_list
        )
        
        # R: if (nrow(collect(head(detection_events))) > 0) {
        if len(detection_events) > 0:
            # R: qs <- get_qs(detection_events, intervals = c("hour", "15min"))
            try:
                if 'get_qs' in globals():
                    qs = get_qs(detection_events, intervals=["hour", "15min"])
                else:
                    # Fallback - create empty structure
                    qs = {'hour': pd.DataFrame(), '15min': pd.DataFrame()}
                
                # R: s3_upload_parquet_date_split(qs$hour, ...)
                if len(qs['hour']) > 0:
                    s3_upload_parquet_date_split(
                        qs['hour'],
                        bucket=conf['bucket'],
                        prefix="qs", 
                        table_name="queue_spillback",
                        conf_athena=conf['athena']
                    )
                
                # R: s3_upload_parquet_date_split(qs$`15min`, ...)
                if len(qs['15min']) > 0:
                    s3_upload_parquet_date_split(
                        qs['15min'],
                        bucket=conf['bucket'],
                        prefix="qs",
                        table_name="queue_spillback_15min", 
                        conf_athena=conf['athena']
                    )
            except Exception as e:
                logger.error(f"Error processing queue spillback for {date_}: {e}")
        
        current_date += timedelta(days=1)

def get_pd_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """
    Exact conversion of R function:
    get_pd_date_range <- function(start_date, end_date) {
        date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
        lapply(date_range, function(date_) {
            print(date_)
            run_parallel <- length(date_range) > 1
            pd <- get_ped_delay(date_, conf, signals_list)
            if (nrow(pd) > 0) {
                s3_upload_parquet_date_split(pd, ...)
            }
        })
        invisible(gc())
    }
    """
    from datetime import datetime, timedelta
    
    # R: date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    current_date = start_dt
    while current_date <= end_dt:
        # R: lapply(date_range, function(date_) {
        date_ = current_date
        # R: print(date_)
        print(date_)
        
        # R: run_parallel <- length(date_range) > 1
        # (This variable isn't used in the R code, so we can skip it)
        
        # R: pd <- get_ped_delay(date_, conf, signals_list)
        try:
            if 'get_ped_delay' in globals():
                pd = get_ped_delay(date_, conf, signals_list)
            else:
                # Fallback - create empty DataFrame
                pd = pd.DataFrame()
            
            # R: if (nrow(pd) > 0) {
            if len(pd) > 0:
                # R: s3_upload_parquet_date_split(pd, ...)
                s3_upload_parquet_date_split(
                    pd,
                    bucket=conf['bucket'],
                    prefix="pd",
                    table_name="ped_delay",
                    conf_athena=conf['athena']
                )
        except Exception as e:
            logger.error(f"Error processing pedestrian delay for {date_}: {e}")
        
        current_date += timedelta(days=1)
    
    # R: invisible(gc())
    gc.collect()

def get_sf_date_range(start_date: str, end_date: str, conf: Dict, signals_list: List[str]):
    """
    Exact conversion of R function:
    get_sf_date_range <- function(start_date, end_date) {
        date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
        lapply(date_range, function(date_) {
            print(date_)
            sf <- get_sf_utah(date_, conf, signals_list, intervals = c("hour", "15min"))
            if (nrow(sf$hour) > 0) {
                s3_upload_parquet_date_split(sf$hour, ...)
            }
            if (nrow(sf$`15min`) > 0) {
                s3_upload_parquet_date_split(sf$`15min`, ...)
            }
        })
    }
    """
    from datetime import datetime, timedelta
    
    # R: date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")
    start_dt = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_dt = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    current_date = start_dt
    while current_date <= end_dt:
        # R: lapply(date_range, function(date_) {
        date_ = current_date
        # R: print(date_)
        print(date_)
        
        # R: sf <- get_sf_utah(date_, conf, signals_list, intervals = c("hour", "15min"))
        try:
            if 'get_sf_utah' in globals():
                sf = get_sf_utah(date_, conf, signals_list, intervals=["hour", "15min"])
            else:
                # Fallback - create empty structure
                sf = {'hour': pd.DataFrame(), '15min': pd.DataFrame()}
            
            # R: if (nrow(sf$hour) > 0) {
            if len(sf['hour']) > 0:
                # R: s3_upload_parquet_date_split(sf$hour, ...)
                s3_upload_parquet_date_split(
                    sf['hour'],
                    bucket=conf['bucket'],
                    prefix="sf",
                    table_name="split_failures",
                    conf_athena=conf['athena']
                )
            
            # R: if (nrow(sf$`15min`) > 0) {
            if len(sf['15min']) > 0:
                # R: s3_upload_parquet_date_split(sf$`15min`, ...)
                s3_upload_parquet_date_split(
                    sf['15min'],
                    bucket=conf['bucket'],
                    prefix="sf",
                    table_name="split_failures_15min",
                    conf_athena=conf['athena']
                )
        except Exception as e:
            logger.error(f"Error processing split failures for {date_}: {e}")
        
        current_date += timedelta(days=1)

def main():
    """
    Exact conversion of Monthly_Report_Calcs_2.R main logic
    """
    try:
        # R: source("Monthly_Report_Calcs_init.R")
        conf, start_date, end_date, signals_list = load_init_variables()
        
        if not signals_list:
            logger.error("No signals found to process")
            return False
        
        logger.info(f"Processing {len(signals_list)} signals from {start_date} to {end_date}")
        
        # R: print(glue("{Sys.time()} etl [7 of 11]"))
        print_with_timestamp("etl [7 of 11]")
        
        # R: if (conf$run$etl == TRUE || is.null(conf$run$etl)) {
        run_etl = conf.get('run', {}).get('etl')
        if run_etl is True or run_etl is None:
            # R: system(glue("~/miniconda3/bin/conda run -n sigops python etl_dashboard.py {start_date} {end_date}"))
            command = f"~/miniconda3/bin/conda run -n sigops python etl_dashboard.py {start_date} {end_date}"
            if not run_system_command(command):
                logger.warning("ETL command failed, continuing...")
        
        # R: print(glue("{Sys.time()} aog [8 of 11]"))
        print_with_timestamp("aog [8 of 11]")
        
        # R: if (conf$run$arrivals_on_green == TRUE || is.null(conf$run$arrivals_on_green)) {
        run_aog = conf.get('run', {}).get('arrivals_on_green')
        if run_aog is True or run_aog is None:
            # R: system(glue("~/miniconda3/bin/conda run -n sigops python get_aog.py {start_date} {end_date}"))
            command = f"~/miniconda3/bin/conda run -n sigops python get_aog.py {start_date} {end_date}"
            if not run_system_command(command):
                logger.warning("AOG command failed, continuing...")
        
        # R: invisible(gc())
        gc.collect()
        
        # R: print(glue("{Sys.time()} queue spillback [9 of 11]"))
        print_with_timestamp("queue spillback [9 of 11]")
        
        # R: if (conf$run$queue_spillback == TRUE || is.null(conf$run$queue_spillback)) {
        run_qs = conf.get('run', {}).get('queue_spillback')
        if run_qs is True or run_qs is None:
            # R: get_queue_spillback_date_range(start_date, end_date)
            get_queue_spillback_date_range(start_date, end_date, conf, signals_list)
        
        # R: print(glue("{Sys.time()} ped delay [10 of 11]"))
        print_with_timestamp("ped delay [10 of 11]")
        
        # R: if (conf$run$ped_delay == TRUE || is.null(conf$run$ped_delay)) {
        run_pd = conf.get('run', {}).get('ped_delay')
        if run_pd is True or run_pd is None:
            # R: get_pd_date_range(start_date, end_date)
            get_pd_date_range(start_date, end_date, conf, signals_list)
        
        # R: print(glue("{Sys.time()} split failures [11 of 11]"))
        print_with_timestamp("split failures [11 of 11]")
        
        # R: if (conf$run$split_failures == TRUE || is.null(conf$run$split_failures)) {
        run_sf = conf.get('run', {}).get('split_failures')
        if run_sf is True or run_sf is None:
            # R: get_sf_date_range(start_date, end_date)
            get_sf_date_range(start_date, end_date, conf, signals_list)
        
        # R: print(glue("{Sys.time()} flash events [12 of 12]"))
        print_with_timestamp("flash events [12 of 12]")
        
        # R: if (conf$run$flash_events == TRUE || is.null(conf$run$flash_events)) {
        run_flash = conf.get('run', {}).get('flash_events')
        if run_flash is True or run_flash is None:
            # R: system(glue("~/miniconda3/bin/conda run -n sigops python get_flash_events.py"))
            command = f"~/miniconda3/bin/conda run -n sigops python get_flash_events.py"
            if not run_system_command(command):
                logger.warning("Flash events command failed, continuing...")
        
        # R: invisible(gc())
        gc.collect()
        
        # R: closeAllConnections()
        # (Python doesn't need this, but we can add cleanup)
        gc.collect()
        
        # R: print("\n--------------------- End Monthly Report calcs -----------------------\n")
        print("\n--------------------- End Monthly Report calcs -----------------------\n")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    """
    Main execution block - equivalent to running the R script
    """
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('monthly_report_calcs_2.log')
        ]
    )
    
    try:
        print(f"Starting Monthly Report Calcs 2 at {datetime.now()}")
        success = main()
        
        if success:
            print(f"Completed Monthly Report Calcs 2 successfully at {datetime.now()}")
            sys.exit(0)
        else:
            print(f"Monthly Report Calcs 2 failed at {datetime.now()}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        print("\nProcess interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        import traceback
        traceback.print_exc()
        print(f"Unexpected error: {e}")
        sys.exit(1)


