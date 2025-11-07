# -*- coding: utf-8 -*-
"""
get_flash_events.py
Processes flash events from ATSPM data
Optimized with better error handling and AWS integration
"""

import os
import sys
import yaml
from datetime import datetime, timedelta
import pandas as pd
import io
import boto3
import awswrangler as wr
import logging

# Setup logging first
base_path = "."
logs_path = os.path.join(base_path, "logs")
if not os.path.exists(logs_path):
    os.mkdir(logs_path)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_path, f'get_flash_events_{datetime.today().strftime("%Y-%m-%d")}.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Import helper functions with fallbacks
try:
    from mark1_logger import mark1_logger
    logger = mark1_logger(
        os.path.join(logs_path, f'get_flash_events_{datetime.today().strftime("%Y-%m-%d")}.log')
    )
    logger.setLevel(logging.INFO)
except ImportError:
    logger.warning("mark1_logger not available, using basic logging")

try:
    from config import get_date_from_string
except ImportError:
    logger.warning("config.get_date_from_string not available, using fallback")
    # Fallback implementation
    def get_date_from_string(date_str, s3bucket=None, s3prefix=None):
        if date_str == 'yesterday':
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        elif date_str.endswith('days ago'):
            days = int(date_str.split()[0])
            return (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        else:
            return date_str

try:
    from utilities import keep_trying
except ImportError:
    logger.warning("utilities.keep_trying not available, using fallback")
    def keep_trying(func, n_tries=3, timeout=None, **kwargs):
        for attempt in range(n_tries):
            try:
                return func(**kwargs)
            except Exception as e:
                if attempt == n_tries - 1:
                    raise
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
        return None


def query_flash_events(date_str: str, athena_conf: dict, session: boto3.Session) -> pd.DataFrame:
    """
    Query flash events for a specific date from Athena
    
    Args:
        date_str: Date string (YYYY-MM-DD)
        athena_conf: Athena configuration
        session: Boto3 session
    
    Returns:
        DataFrame with flash events
    """
    query = f"""
        SELECT signalid as SignalID, 
               timestamp as TimeStamp, 
               eventparam as EventParam
        FROM {athena_conf['database']}.{athena_conf['atspm_table']} 
        WHERE eventcode = 173 
        AND date = '{date_str}'
    """
    
    try:
        df = wr.athena.read_sql_query(
            sql=query,
            database=athena_conf['database'],
            s3_output=athena_conf['staging_dir'],
            boto3_session=session,
            ctas_approach=False
        )
        return df
    except Exception as e:
        logger.error(f"Error querying flash events for {date_str}: {e}")
        return pd.DataFrame()


if __name__ == "__main__":

    try:
        logger.info("Starting Flash Events processing")
        
        # Load configuration
        with open("Monthly_Report.yaml") as yaml_file:
            conf = yaml.load(yaml_file, Loader=yaml.FullLoader)
        
        # Load AWS credentials
        try:
            with open("Monthly_Report_AWS.yaml") as yaml_file:
                aws_conf = yaml.load(yaml_file, Loader=yaml.FullLoader)
                conf.update(aws_conf)
        except Exception as e:
            logger.warning(f"Could not load AWS credentials from file: {e}")

        # Get date range
        if len(sys.argv) > 1:
            start_date = sys.argv[1]
            end_date = sys.argv[2]
        else:
            start_date = get_date_from_string(
                conf["start_date"], s3bucket=conf["bucket"], s3prefix="mark/flash_events"
            )
            end_date = conf["end_date"]
        
        end_date = get_date_from_string(end_date)
        
        logger.info(f"Processing flash events from {start_date} to {end_date}")

        bucket = conf["bucket"]
        athena = conf["athena"]
        
        # Setup AWS session
        session = boto3.Session(
            aws_access_key_id=conf.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=conf.get('AWS_SECRET_ACCESS_KEY'),
            region_name=conf.get('AWS_DEFAULT_REGION', 'us-east-1')
        )
        s3 = session.client("s3")

        dates = pd.date_range(start_date, end_date, freq="1D")
        
        success_count = 0
        error_count = 0

        for date_ in dates:
            date_str = date_.strftime("%Y-%m-%d")
            
            try:
                logger.info(f"Processing flash events for {date_str}")

                # Query flash events with retry logic
                flashes = keep_trying(
                    query_flash_events,
                    n_tries=3,
                    timeout=120,
                    date_str=date_str,
                    athena_conf=athena,
                    session=session
                )
                
                if flashes is None or flashes.empty:
                    logger.info(f"No flash events on {date_str}")
                    continue

                # Process flash events
                flashes["SignalID"] = flashes["SignalID"].astype(str)
                flashes["TimeStamp"] = pd.to_datetime(flashes["TimeStamp"])
                flashes = flashes.sort_values(["SignalID", "TimeStamp"]).reset_index(drop=True)
                flashes["EventChange"] = flashes.groupby(["SignalID"])[["EventParam"]].diff()
                flashes = flashes[flashes.EventChange != 0].drop(columns=["EventChange"])

                flashes["FlashDuration"] = (
                    flashes.groupby(["SignalID"])[["TimeStamp"]]
                    .diff()
                    .shift(-1)["TimeStamp"]
                    .dt.total_seconds()
                )
                flashes["EndParam"] = flashes.groupby(["SignalID"])[["EventParam"]].shift(-1, fill_value=0)
                flashes = flashes[~flashes["EventParam"].isin([2, 3, 4])]

                # Upload to S3
                flash_s3object = f"mark/flash_events/date={date_str}/flashes_{date_str}.parquet"
                
                with io.BytesIO() as data:
                    flashes.to_parquet(data)
                    data.seek(0)
                    s3.upload_fileobj(data, bucket, flash_s3object)

                flash_s3key = os.path.join("s3://", bucket, flash_s3object)
                logger.info(f"{flash_s3key} written - {len(flashes)} flash events")
                success_count += 1
                
            except Exception as e:
                logger.error(f"Error processing flash events for {date_str}: {e}")
                error_count += 1
                continue
        
        # Summary
        logger.info(f"Flash events processing completed: {success_count} success, {error_count} errors")
        logger.info("Flash Events processing completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error in flash events processing: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
