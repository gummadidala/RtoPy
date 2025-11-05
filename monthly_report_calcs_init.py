"""
Monthly Report Calculations Initialization
Converted from Monthly_Report_Calcs_init.R
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import yaml
import boto3
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing
from functools import partial

# Import custom modules
from monthly_report_functions import *
from database_functions import *
from s3_parquet_io import *

# Setup logging
setup_logging("INFO")
logger = logging.getLogger(__name__)

def main():
    """Main initialization function - exact equivalent of Monthly_Report_Calcs_init.R"""
    
    # R: writeLines(glue("\n\n{Sys.time()} Starting Calcs Script"))
    print(f"\n\n{datetime.now()} Starting Calcs Script")
    
    # Load configuration
    conf, aws_conf = load_configuration()

    if not conf:
        logger.error("Failed to load configuration")
        sys.exit(1)
    
    # R: usable_cores <- get_usable_cores()
    usable_cores = get_usable_cores()
    logger.info(f"Using {usable_cores} cores for parallel processing")
    
    # R: start_date <- get_date_from_string(conf$start_date, s3bucket = conf$bucket, s3prefix = "mark/split_failures")
    # R: end_date <- get_date_from_string(conf$end_date)
    start_date = get_date_from_string(
        conf['start_date'], 
        s3bucket=conf['bucket'], 
        s3prefix="mark/split_failures"
    )
    end_date = get_date_from_string(conf['end_date'])
    
    # Manual overrides (commented in R, keeping as comment)
    # start_date = "2020-01-04"
    # end_date = "2020-01-04"
    
    # R: month_abbrs <- get_month_abbrs(start_date, end_date)
    month_abbrs = get_month_abbrs(start_date, end_date)
    
    # GET CORRIDORS (exact equivalent of R code block)
    # R: corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = TRUE)
    corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=True)
    
    # R: feather_filename <- sub("\\..*", ".feather", conf$corridors_filename_s3)
    # R: write_feather(corridors, feather_filename)
    feather_filename = conf['corridors_filename_s3'].replace('.xlsx', '.feather').replace('.xls', '.feather')
    corridors.to_feather(feather_filename)
    
    # R: qs_filename <- sub("\\..*", ".qs", conf$corridors_filename_s3)
    # R: qsave(corridors, qs_filename)
    parquet_filename = conf['corridors_filename_s3'].replace('.xlsx', '.parquet').replace('.xls', '.parquet')
    corridors.to_parquet(parquet_filename)
    
    # R: all_corridors <- get_corridors(conf$corridors_filename_s3, filter_signals = FALSE)
    all_corridors = get_corridors(conf['corridors_filename_s3'], filter_signals=False)
    
    # R: feather_filename <- sub("\\..*", ".feather", paste0("all_", conf$corridors_filename_s3))
    # R: write_feather(all_corridors, feather_filename)
    all_feather_filename = "all_" + feather_filename
    all_corridors.to_feather(all_feather_filename)
    
    # R: qs_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
    # R: qsave(all_corridors, qs_filename)
    all_parquet_filename = "all_" + parquet_filename
    all_corridors.to_parquet(all_parquet_filename)
    
    # R: signals_list <- mclapply(seq(as_date(start_date), as_date(end_date), by = "1 day"),
    #                            mc.cores = usable_cores, FUN = get_signalids_from_s3) %>%
    #                   unlist() %>% unique()
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    date_strings = [date.strftime('%Y-%m-%d') for date in date_range]
    
    # Create partial function with bucket parameter
    get_signalids_with_bucket = partial(get_signalids_from_s3, bucket=conf['bucket'])
    
    # Process dates in parallel (equivalent to mclapply)
    with ProcessPoolExecutor(max_workers=usable_cores) as executor:
        results = list(executor.map(get_signalids_with_bucket, date_strings))
    
    # Flatten and get unique signals (equivalent to unlist() %>% unique())
    signals_list = []
    for result in results:
        if result:
            signals_list.extend(result)
    
    signals_list = list(set(signals_list))  # unique()
    
    # R: get_latest_det_config(conf) %>% s3write_using(qsave, bucket = conf$bucket, object = "ATSPM_Det_Config_Good_Latest.qs")
    det_config = get_latest_det_config(conf)
    if not det_config.empty:
        det_config.to_parquet("ATSPM_Det_Config_Good_Latest.parquet")
        s3write_using(
            det_config.to_parquet,
            bucket=conf['bucket'],
            object="ATSPM_Det_Config_Good_Latest.parquet"
        )
    
    # Add partitions that don't already exist to Athena ATSPM table (exact equivalent of R code)
    try:
        # R: athena <- get_athena_connection(conf$athena)
        athena = get_athena_connection(conf['athena'])
        
        if athena:
            # R: partitions <- dbGetQuery(athena, glue("SHOW PARTITIONS {conf$athena$atspm_table}"))$partition
            partitions_query = f"SHOW PARTITIONS {conf['athena']['atspm_table']}"
            try:
                partitions_df = pd.read_sql(partitions_query, athena)
                # R: partitions <- sapply(stringr::str_split(partitions, "="), last)
                partitions = [
                    partition.split('=')[-1] 
                    for partition in partitions_df['partition'].tolist()
                ]
            except:
                partitions = []
            
            # R: date_range <- seq(as_date(start_date), as_date(end_date), by = "1 day") %>% as.character()
            # R: missing_partitions <- setdiff(date_range, partitions)
            missing_partitions = [
                date for date in date_strings 
                if date not in partitions
            ]
            
            # R: if (length(missing_partitions) > 10) {
            if len(missing_partitions) > 10:
                # R: print(glue("Adding missing partition: date={missing_partitions}"))
                # R: dbExecute(athena, glue("MSCK REPAIR TABLE {conf$athena$atspm_table}"))
                print(f"Adding missing partition: date={missing_partitions}")
                repair_query = f"MSCK REPAIR TABLE {conf['athena']['atspm_table']}"
                athena.execute(repair_query)
            # R: } else if (length(missing_partitions) > 0) {
            elif len(missing_partitions) > 0:
                # R: print("Adding missing partitions:")
                # R: for (date_ in missing_partitions) {
                #        add_athena_partition(conf$athena, conf$bucket, conf$athena$atspm_table, date_)
                #    }
                print("Adding missing partitions:")
                for date_ in missing_partitions:
                    add_athena_partition(
                        conf['athena'], 
                        conf['bucket'], 
                        conf['athena']['atspm_table'], 
                        date_
                    )
            
            # R: dbDisconnect(athena)
            athena.close()
    except Exception as e:
        logger.error(f"Error setting up Athena partitions: {e}")
    
    logger.info("Initialization completed successfully")

    return {
        'conf': conf,
        'start_date': start_date,
        'end_date': end_date,
        'month_abbrs': month_abbrs,
        'signals_list': signals_list,
        'usable_cores': usable_cores
    }

# Make these variables globally available (equivalent to R's global environment)
def load_init_variables():
    """Load initialization variables (equivalent to sourcing the init script)"""
    init_results = main()
    return (
        init_results['conf'],
        init_results['start_date'], 
        init_results['end_date'],
        init_results['signals_list']
    )

if __name__ == "__main__":
    """Run initialization if called directly"""
    
    start_time = datetime.now()
    
    try:
        # Run main initialization
        init_results = main()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"""
        Initialization Summary
        =====================
        Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}
        End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}
        Duration: {duration}
        
        Configuration:
        - Bucket: {init_results['conf'].get('bucket', 'N/A')}
        - Start Date: {init_results['start_date']}
        - End Date: {init_results['end_date']}
        
        Results:
        - Signals Found: {len(init_results['signals_list'])}
        - Usable Cores: {init_results['usable_cores']}
        """)
        
        logger.info("Initialization completed successfully")
        sys.exit(0)
            
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        sys.exit(1)