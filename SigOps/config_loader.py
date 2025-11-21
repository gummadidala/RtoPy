#!/usr/bin/env python3
"""
Shared configuration loader for SigOps scripts
Loads Monthly_Report.yaml and Monthly_Report_AWS.yaml and merges them
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


def load_merged_config(config_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Load and merge Monthly_Report.yaml and Monthly_Report_AWS.yaml
    
    Args:
        config_dir: Optional directory path. If None, uses current directory or SigOps directory
        
    Returns:
        Merged configuration dictionary with AWS credentials
    """
    if config_dir is None:
        # Try current directory first, then SigOps directory
        if os.path.exists('Monthly_Report.yaml'):
            config_dir = '.'
        elif os.path.exists('SigOps/Monthly_Report.yaml'):
            config_dir = 'SigOps'
        else:
            # Try parent directory
            config_dir = '..'
    
    config_path = os.path.join(config_dir, 'Monthly_Report.yaml')
    aws_config_path = os.path.join(config_dir, 'Monthly_Report_AWS.yaml')
    
    # Also try in parent directory if not found
    if not os.path.exists(config_path) and os.path.exists('../Monthly_Report.yaml'):
        config_path = '../Monthly_Report.yaml'
    if not os.path.exists(aws_config_path) and os.path.exists('../Monthly_Report_AWS.yaml'):
        aws_config_path = '../Monthly_Report_AWS.yaml'
    
    # Load main configuration
    conf = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                conf = yaml.safe_load(f) or {}
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.error(f"Error loading {config_path}: {e}")
            raise
    else:
        logger.warning(f"Configuration file not found: {config_path}")
    
    # Load AWS configuration
    aws_conf = {}
    if os.path.exists(aws_config_path):
        try:
            with open(aws_config_path, 'r') as f:
                aws_conf = yaml.safe_load(f) or {}
            logger.info(f"Loaded AWS configuration from {aws_config_path}")
        except Exception as e:
            logger.warning(f"Error loading {aws_config_path}: {e}")
    else:
        logger.warning(f"AWS configuration file not found: {aws_config_path}")
    
    # Merge AWS config into main config
    if aws_conf:
        # Set environment variables for boto3
        if 'AWS_ACCESS_KEY_ID' in aws_conf:
            os.environ['AWS_ACCESS_KEY_ID'] = aws_conf['AWS_ACCESS_KEY_ID']
            conf['AWS_ACCESS_KEY_ID'] = aws_conf['AWS_ACCESS_KEY_ID']
        
        if 'AWS_SECRET_ACCESS_KEY' in aws_conf:
            os.environ['AWS_SECRET_ACCESS_KEY'] = aws_conf['AWS_SECRET_ACCESS_KEY']
            conf['AWS_SECRET_ACCESS_KEY'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        
        if 'AWS_DEFAULT_REGION' in aws_conf:
            os.environ['AWS_DEFAULT_REGION'] = aws_conf['AWS_DEFAULT_REGION']
            conf['AWS_DEFAULT_REGION'] = aws_conf['AWS_DEFAULT_REGION']
        
        # Update Athena configuration if it exists
        if 'athena' not in conf:
            conf['athena'] = {}
        
        if 'AWS_ACCESS_KEY_ID' in aws_conf:
            conf['athena']['uid'] = aws_conf['AWS_ACCESS_KEY_ID']
        if 'AWS_SECRET_ACCESS_KEY' in aws_conf:
            conf['athena']['pwd'] = aws_conf['AWS_SECRET_ACCESS_KEY']
        if 'AWS_DEFAULT_REGION' in aws_conf:
            conf['athena']['region'] = aws_conf['AWS_DEFAULT_REGION']
    
    # Ensure athena database is set
    if 'athena' not in conf:
        conf['athena'] = {}
    
    if 'database' not in conf.get('athena', {}):
        # Try to infer from bucket name or use default
        bucket = conf.get('bucket', '')
        if 'sigops' in bucket.lower():
            conf['athena']['database'] = 'sigops'
        else:
            conf['athena']['database'] = 'mark1'
        logger.info(f"Using default Athena database: {conf['athena']['database']}")
    
    # Set staging directory if not present
    if 'staging_dir' not in conf.get('athena', {}):
        bucket = conf.get('bucket', 'gdotspm')
        conf['athena']['staging_dir'] = f"s3://{bucket}/athena-results/"
    
    logger.info(f"Configuration loaded successfully. Athena database: {conf.get('athena', {}).get('database')}")
    
    return conf

