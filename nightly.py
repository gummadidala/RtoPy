#!/usr/bin/env python3
"""
Nightly processing script
Converted from nightly.sh
"""

import os
import sys
import time
import subprocess
import logging
from datetime import datetime, timedelta
import yaml
import boto3
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nightly.log')
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    """Load configuration from YAML file"""
    try:
        with open('Monthly_Report.yaml', 'r') as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return {}

def get_bucket_name():
    """Get bucket name from config"""
    config = load_config()
    return config.get('bucket', '')

def get_current_time_info():
    """Get current date and time info in ET timezone"""
    
    # Set timezone to Eastern Time
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    
    now = datetime.now()
    today_date = now.strftime('%Y-%m-%d')
    current_hour = int(now.strftime('%H'))
    
    return today_date, current_hour

def get_sam_last_run_info(bucket: str):
    """
    Get SAM last run date and time from S3
    
    Args:
        bucket: S3 bucket name
    
    Returns:
        Tuple of (date, time) or (None, None) if not found
    """
    
    try:
        s3_client = boto3.client('s3')
        key = 'SamLastRun'
        
        # List objects with the key
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
        
        if 'Contents' not in response:
            logger.warning(f"SamLastRun file not found in bucket {bucket}")
            return None, None
        
        # Get the first (and should be only) object
        obj = response['Contents'][0]
        last_modified = obj['LastModified']
        
        # Convert to ET timezone and extract date/time
        sam_date = last_modified.strftime('%Y-%m-%d')
        sam_time = last_modified.strftime('%H:%M:%S')
        
        return sam_date, sam_time
        
    except Exception as e:
        logger.error(f"Error getting SAM last run info: {e}")
        return None, None

def wait_for_sam_completion(bucket: str, wait_until_hour: int = 12):
    """
    Wait until SAM last run file timestamp has today's date
    or until the specified hour is reached
    
    Args:
        bucket: S3 bucket name
        wait_until_hour: Hour to stop waiting (24-hour format)
    
    Returns:
        Boolean indicating if SAM completed today
    """
    
    while True:
        today_date, current_hour = get_current_time_info()
        sam_date, sam_time = get_sam_last_run_info(bucket)
        
        if sam_date is None:
            logger.error("Cannot determine SAM last run date")
            return False
        
        # Check if SAM completed today or if we've reached the wait limit
        if today_date == sam_date:
            logger.info(f"SAM completed today at {sam_time}. Proceeding with nightly scripts.")
            return True
        elif current_hour >= wait_until_hour:
            logger.warning(f"Reached hour {wait_until_hour} without SAM completion. Proceeding anyway.")
            return False
        else:
            logger.info(
                f"{today_date} (today) != {sam_date} (SAM last run) and "
                f"{current_hour} (current hour) < {wait_until_hour} (wait until hour) | "
                f"waiting 10 minutes"
            )
            time.sleep(600)  # Wait 10 minutes

def run_script(script_path: str, description: str = None) -> bool:
    """
    Run a script and log the results
    
    Args:
        script_path: Path to the script
        description: Description for logging
    
    Returns:
        Boolean indicating success
    """
    
    if description is None:
        description = script_path
    
    logger.info(f"Starting: {description}")
    
    try:
        # Determine if it's an R script or Python script
        if script_path.endswith('.R'):
            cmd = ['Rscript', script_path]
        elif script_path.endswith('.py'):
            cmd = ['python', script_path]
        else:
            cmd = script_path.split()
        
        # Run the script
        start_time = datetime.now()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout
        )
        end_time = datetime.now()
        duration = end_time - start_time
        
        if result.returncode == 0:
            logger.info(f"Completed successfully: {description} (Duration: {duration})")
            if result.stdout:
                logger.debug(f"Output: {result.stdout}")
            return True
        else:
            logger.error(f"Failed: {description} (Return code: {result.returncode})")
            if result.stderr:
                logger.error(f"Error output: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout: {description}")
        return False
    except Exception as e:
        logger.error(f"Exception running {description}: {e}")
        return False

def run_monthly_user_delay_cost():
    """
    Run User Delay Cost script on the 1st, 11th and 21st of the month
    """
    
    today = datetime.now()
    day_of_month = today.day
    
    if day_of_month in [1, 11, 21]:
        logger.info(f"Running User Delay Cost script (day {day_of_month} of month)")
        
        cmd = [
            'conda', 'run', '-n', 'sigops', 'python', 'user_delay_costs.py'
        ]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            
            if result.returncode == 0:
                logger.info("User Delay Cost script completed successfully")
            else:
                logger.error(f"User Delay Cost script failed: {result.stderr}")
                
        except Exception as e:
            logger.error(f"Error running User Delay Cost script: {e}")
    else:
        logger.info(f"Skipping User Delay Cost script (day {day_of_month} not in [1, 11, 21])")

def main():
    """Main nightly processing function"""
    
    logger.info("=" * 60)
    logger.info("Starting nightly processing")
    logger.info("=" * 60)
    
    # Load configuration
    bucket = get_bucket_name()
    if not bucket:
        logger.error("Could not determine S3 bucket name")
        sys.exit(1)
    
    logger.info(f"Using S3 bucket: {bucket}")
    
    # Wait for SAM completion
    sam_completed_today = wait_for_sam_completion(bucket, wait_until_hour=12)
    
    today_date, current_hour = get_current_time_info()
    sam_date, sam_time = get_sam_last_run_info(bucket)
    
    logger.info(f"Today's date: {today_date}")
    logger.info(f"SAM last run date: {sam_date}")
    logger.info(f"Current time: {datetime.now().strftime('%H:%M:%S')}")
    
    # Proceed if SAM completed today
    if today_date == sam_date:
        logger.info(f"{sam_time} - Run Scripts")
        
        # Track script execution results
        script_results = {}
        
        # Main processing scripts
        scripts = [
            ('monthly_report_calcs_1.py', 'Monthly Report Calculations 1'),
            ('monthly_report_calcs_2.py', 'Monthly Report Calculations 2'),
            ('monthly_report_package_1.py', 'Monthly Report Package 1'),
            ('monthly_report_package_2.py', 'Monthly Report Package 2'),
            ('get_alerts.py', 'Get Alerts')
        ]
        
        for script, description in scripts:
            if os.path.exists(script):
                success = run_script(script, description)
                script_results[script] = success
            else:
                logger.warning(f"Script not found: {script}")
                script_results[script] = False
        
        logger.info("------------------------")
        
        # SigOps directory scripts
        sigops_dir = Path("SigOps")
        if sigops_dir.exists():
            logger.info("Changing to SigOps directory")
            os.chdir(sigops_dir)
            
            sigops_scripts = [
                ('monthly_report_package.py', 'SigOps Monthly Report Package'),
                ('get_alerts_sigops.py', 'SigOps Get Alerts'),
                ('monthly_report_package_1hr.py', 'SigOps Monthly Report Package 1hr'),
                ('monthly_report_package_15min.py', 'SigOps Monthly Report Package 15min'),
                ('checkdb.py', 'Check Database')
            ]
            
            for script, description in sigops_scripts:
                if os.path.exists(script):
                    success = run_script(script, description)
                    script_results[f"SigOps/{script}"] = success
                else:
                    logger.warning(f"SigOps script not found: {script}")
                    script_results[f"SigOps/{script}"] = False
                logger.info("------------------------")
            
            # Return to parent directory
            os.chdir("..")
        else:
            logger.warning("SigOps directory not found")
        
        # Run monthly User Delay Cost script
        run_monthly_user_delay_cost()
        
        # Log summary
        log_execution_summary(script_results)
        
        # Create completion marker
        create_completion_marker(bucket, script_results)
        
    else:
        logger.warning("SAM did not complete today. Skipping nightly scripts.")
        sys.exit(1)
    
    logger.info("Nightly processing completed")

def log_execution_summary(script_results: dict):
    """
    Log summary of script execution results
    
    Args:
        script_results: Dictionary of script -> success mapping
    """
    
    logger.info("=" * 60)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 60)
    
    successful_scripts = []
    failed_scripts = []
    
    for script, success in script_results.items():
        if success:
            successful_scripts.append(script)
            logger.info(f"✓ SUCCESS: {script}")
        else:
            failed_scripts.append(script)
            logger.error(f"✗ FAILED:  {script}")
    
    logger.info("=" * 60)
    logger.info(f"TOTAL SCRIPTS: {len(script_results)}")
    logger.info(f"SUCCESSFUL:    {len(successful_scripts)}")
    logger.info(f"FAILED:        {len(failed_scripts)}")
    
    if failed_scripts:
        logger.warning("Failed scripts may require manual intervention")
    
    logger.info("=" * 60)

def create_completion_marker(bucket: str, script_results: dict):
    """
    Create a completion marker in S3 with execution results
    
    Args:
        bucket: S3 bucket name
        script_results: Dictionary of script execution results
    """
    
    try:
        import json
        
        completion_data = {
            'timestamp': datetime.now().isoformat(),
            'script_results': script_results,
            'successful_count': sum(1 for success in script_results.values() if success),
            'failed_count': sum(1 for success in script_results.values() if not success),
            'total_count': len(script_results)
        }
        
        s3_client = boto3.client('s3')
        key = f"nightly_completion/{datetime.now().strftime('%Y-%m-%d')}.json"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(completion_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Created completion marker: s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Error creating completion marker: {e}")

def send_notification_if_configured(script_results: dict):
    """
    Send notification about nightly processing results if configured
    
    Args:
        script_results: Dictionary of script execution results
    """
    
    try:
        # Load notification configuration
        config = load_config()
        notifications = config.get('notifications', {})
        
        if not notifications:
            return
        
        failed_scripts = [script for script, success in script_results.items() if not success]
        successful_count = sum(1 for success in script_results.values() if success)
        total_count = len(script_results)
        
        if failed_scripts:
            message = f"Nightly processing completed with {len(failed_scripts)} failures:\n"
            message += "\n".join(f"- {script}" for script in failed_scripts)
            message += f"\n\nSuccess rate: {successful_count}/{total_count}"
            notification_type = "warning"
        else:
            message = f"Nightly processing completed successfully. All {total_count} scripts ran without errors."
            notification_type = "info"
        
        # Send notification (implementation would depend on your notification setup)
        # send_notification(message, notification_type, notifications.get('email'), notifications.get('slack'))
        
    except Exception as e:
        logger.error(f"Error sending notification: {e}")

def cleanup_old_logs(days_to_keep: int = 7):
    """
    Clean up old log files
    
    Args:
        days_to_keep: Number of days of logs to keep
    """
    
    try:
        log_dir = Path(".")
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        for log_file in log_dir.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date.timestamp():
                log_file.unlink()
                logger.info(f"Removed old log file: {log_file}")
                
    except Exception as e:
        logger.error(f"Error cleaning up logs: {e}")

def setup_environment():
    """Setup environment variables and paths"""
    
    try:
        # Set timezone
        os.environ['TZ'] = 'America/New_York'
        time.tzset()
        
        # Set up conda environment
        home_dir = os.path.expanduser("~")
        conda_path = os.path.join(home_dir, "miniconda3", "bin")
        
        # Add conda to PATH if not already there
        current_path = os.environ.get('PATH', '')
        if conda_path not in current_path:
            os.environ['PATH'] = f"{conda_path}:{current_path}"
        
        # Set up data directory
        data_dir = "/data/sdb"
        if os.path.exists(data_dir):
            os.chdir(data_dir)
            logger.info(f"Changed to data directory: {data_dir}")
        
    except Exception as e:
        logger.error(f"Error setting up environment: {e}")

def check_prerequisites():
    """Check that required files and directories exist"""
    
    required_files = [
        'Monthly_Report.yaml',
        'monthly_report_calcs_1.py'
    ]
    
    missing_files = []
    for file_path in required_files:
        if not os.path.exists(file_path):
            missing_files.append(file_path)
    
    if missing_files:
        logger.error(f"Missing required files: {missing_files}")
        return False
    
    return True

if __name__ == "__main__":
    """Run nightly processing if called directly"""
    
    try:
        # Setup environment
        setup_environment()
        
        # Check prerequisites
        if not check_prerequisites():
            logger.error("Prerequisites check failed")
            sys.exit(1)
        
        # Run main processing
        main()
        
        # Cleanup old logs
        cleanup_old_logs(days_to_keep=7)
        
        logger.info("Nightly processing script completed successfully")
        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("Nightly processing interrupted by user")
        sys.exit(130)  # Standard exit code for Ctrl+C
    except Exception as e:
        logger.error(f"Unexpected error in nightly processing: {e}")
        sys.exit(1)
