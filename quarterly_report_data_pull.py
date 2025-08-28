"""
Quarterly Report Data Pull - Python conversion of quarterly_report_data_pull.R
Pulls quarterly data for various traffic and infrastructure metrics
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import boto3
import yaml
from database_functions import get_aurora_connection
from utilities import s3read_using
import re

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
def load_config() -> Dict[str, Any]:
    """Load configuration from YAML"""
    try:
        with open("config.yaml", 'r') as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        logger.error("config.yaml not found")
        return {}

conf = load_config()

# Metric definitions (equivalent to the metric objects in R)
class Metric:
    """Class to define metric properties"""
    
    def __init__(self, label: str, table: str, variable: str):
        self.label = label
        self.table = table
        self.variable = variable

# Define metrics (equivalent to the metric structure definitions in R)
vpd = Metric("Daily Traffic Volume", "vpd", "vpd")
am_peak_vph = Metric("AM Peak Hour Volume", "am_peak_vph", "am_peak_vph")
pm_peak_vph = Metric("PM Peak Hour Volume", "pm_peak_vph", "pm_peak_vph")
throughput = Metric("Throughput", "throughput", "throughput")
arrivals_on_green = Metric("Arrivals on Green", "arrivals_on_green", "arrivals_on_green")
progression_ratio = Metric("Progression Ratio", "progression_ratio", "progression_ratio")
queue_spillback_rate = Metric("Queue Spillback Rate", "queue_spillback_rate", "queue_spillback_rate")
peak_period_split_failures = Metric("Peak Period Split Failures", "peak_period_split_failures", "peak_period_split_failures")
off_peak_split_failures = Metric("Off Peak Split Failures", "off_peak_split_failures", "off_peak_split_failures")
travel_time_index = Metric("Travel Time Index", "travel_time_index", "travel_time_index")
planning_time_index = Metric("Planning Time Index", "planning_time_index", "planning_time_index")
average_speed = Metric("Average Speed", "average_speed", "average_speed")
daily_pedestrian_pushbuttons = Metric("Daily Pedestrian Pushbuttons", "daily_pedestrian_pushbuttons", "daily_pedestrian_pushbuttons")
detector_uptime = Metric("Detector Uptime", "detector_uptime", "detector_uptime")
ped_button_uptime = Metric("Pedestrian Button Uptime", "ped_button_uptime", "ped_button_uptime")
cctv_uptime = Metric("CCTV Uptime", "cctv_uptime", "cctv_uptime")
comm_uptime = Metric("Communication Uptime", "comm_uptime", "comm_uptime")
tasks_reported = Metric("Tasks Reported", "tasks_reported", "tasks_reported")
tasks_resolved = Metric("Tasks Resolved", "tasks_resolved", "tasks_resolved")
tasks_outstanding = Metric("Tasks Outstanding", "tasks_outstanding", "tasks_outstanding")
tasks_over45 = Metric("Tasks Over 45 Days", "tasks_over45", "tasks_over45")
tasks_mttr = Metric("Mean Time to Repair", "tasks_mttr", "tasks_mttr")

def get_quarterly_data() -> pd.DataFrame:
    """
    Pull quarterly data for all metrics
    
    Returns:
        DataFrame with quarterly metrics data
    """
    
    try:
        logger.info("Starting quarterly data pull")
        
        # Get database connection
        conn = get_aurora_connection()
        
        # List of all metrics to process
        metrics_list = [
            vpd,
            am_peak_vph,
            pm_peak_vph,
            throughput,
            arrivals_on_green,
            progression_ratio,
            queue_spillback_rate,
            peak_period_split_failures,
            off_peak_split_failures,
            travel_time_index,
            planning_time_index,
            average_speed,
            daily_pedestrian_pushbuttons,
            detector_uptime,
            ped_button_uptime,
            cctv_uptime,
            comm_uptime,
            tasks_reported,
            tasks_resolved,
            tasks_outstanding,
            tasks_over45,
            tasks_mttr
        ]
        
        # Process each metric
        dataframes = []
        
        for metric in metrics_list:
            logger.info(f"Processing metric: {metric.label}")
            
            try:
                # Read table from database
                table_name = f"cor_qu_{metric.table}"
                df = pd.read_sql_table(table_name, conn)
                
                # Add metric label and rename value column
                df['Metric'] = metric.label
                df = df.rename(columns={metric.variable: 'value'})
                
                dataframes.append(df)
                
            except Exception as e:
                logger.warning(f"Failed to process metric {metric.label}: {e}")
                continue
        
        # Combine all dataframes
        if dataframes:
            df = pd.concat(dataframes, ignore_index=True)
        else:
            logger.error("No data retrieved for any metrics")
            return pd.DataFrame()
        
        # Filter out NA Zone_Groups
        df = df[df['Zone_Group'] != "NA"]
        df = df[df['Zone_Group'].notna()]
        
        # Select and reorder columns
        columns_to_select = ['Metric', 'Zone_Group', 'Corridor', 'Quarter', 'value']
        
        # Add weight columns if they exist (ones, vol, num)
        weight_columns = []
        for col in ['ones', 'vol', 'num']:
            if col in df.columns:
                weight_columns.append(col)
                columns_to_select.append(col)
        
        df = df[columns_to_select]
        
        # Create weight column (coalesce ones, num, vol)
        if weight_columns:
            # Convert weight columns to numeric
            for col in weight_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Create weight column by coalescing (first non-null value)
            df['weight'] = df[weight_columns].bfill(axis=1).iloc[:, 0]
            
            # Drop individual weight columns
            df = df.drop(columns=weight_columns)
        else:
            df['weight'] = 1.0
        
        # Filter quarters with valid format (YYYY.Q)
        quarter_pattern = r'\d{4}\.\d{1}'
        df = df[df['Quarter'].astype(str).str.match(quarter_pattern, na=False)]
        
        # Parse quarter information
        df[['yr', 'qu']] = df['Quarter'].str.split('.', expand=True)
        df['yr'] = pd.to_numeric(df['yr'])
        df['qu'] = pd.to_numeric(df['qu'])
        
        # Calculate date - start of quarter
        df['date'] = pd.to_datetime(df['yr'].astype(str) + '-' + 
                                   ((df['qu'] - 1) * 3 + 1).astype(str) + '-01')
        
        # Adjust to end of quarter
        df['date'] = df['date'] + pd.DateOffset(months=3) - pd.DateOffset(days=1)
        
        # Create fiscal quarter (starting July = Q1)
        df['Quarter'] = df['date'].apply(lambda x: 
            f"{x.year if x.month >= 7 else x.year - 1}.{((x.month - 7) % 12) // 3 + 1}")
        
        # Drop temporary columns
        df = df.drop(columns=['yr', 'qu'])
        
        # Process zone data (where Zone_Group == Corridor)
        dfz = df[df['Zone_Group'].astype(str) == df['Corridor'].astype(str)].copy()
        dfz = dfz.rename(columns={'Zone_Group': 'District'})
        dfz = dfz.sort_values(['District', 'Quarter'])
        dfz = dfz[['District', 'date', 'Quarter', 'Metric', 'value']]
        
        # Get regions (unique districts from zone data)
        regions = dfz['District'].unique().tolist()
        
        # Process district data (where Zone_Group != Corridor)
        dfd = df[df['Zone_Group'].astype(str) != df['Corridor'].astype(str)].copy()
        dfd = dfd.rename(columns={'Zone_Group': 'District'})
        
        # Filter out regions from district data
        dfd = dfd[~dfd['District'].isin(regions)]
        
        # Calculate weighted mean by district
        dfd = dfd.groupby(['District', 'date', 'Quarter', 'Metric']).apply(
            lambda x: pd.Series({
                'value': np.average(x['value'], weights=x['weight']) if len(x) > 0 else np.nan
            })
        ).reset_index()
        
        dfd = dfd.sort_values(['District', 'Quarter'])
        dfd = dfd[['District', 'date', 'Quarter', 'Metric', 'value']]
        
        # Combine zone and district data
        result = pd.concat([dfz, dfd], ignore_index=True)
        result = result.sort_values(['District', 'Quarter'])
        
        logger.info(f"Quarterly data pull completed. Retrieved {len(result)} records.")
        
        # Close database connection
        conn.close()
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_quarterly_data: {e}")
        return pd.DataFrame()

def write_quarterly_data(qdata: pd.DataFrame, filename: str = "quarterly_data.csv") -> bool:
    """
    Write quarterly data to CSV file
    
    Args:
        qdata: Quarterly data DataFrame
        filename: Output filename
        
    Returns:
        Boolean indicating success
    """
    
    try:
        qdata.to_csv(filename, index=False)
        logger.info(f"Quarterly data written to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing quarterly data: {e}")
        return False

def get_quarterly_bottlenecks() -> pd.DataFrame:
    """
    Get quarterly bottlenecks data
    
    Returns:
        DataFrame with bottlenecks information
    """
    
    try:
        logger.info("Getting quarterly bottlenecks data")
        
        # Read TMCs data from S3
        tmcs = s3read_using(
            pd.read_excel,
            bucket=conf.get('bucket', ''),
            key="Corridor_TMCs_Latest.xlsx"
        )
        
        # Read bottlenecks data for multiple months
        months = ["2020-10-01", "2020-11-01", "2020-12-01"]
        bottlenecks_list = []
        
        for month in months:
            try:
                bottleneck_file = f"mark/bottlenecks/date={month}/bottleneck_rankings_{month}.parquet"
                bottleneck_data = s3read_using(
                    pd.read_parquet,
                    bucket=conf.get('bucket', ''),
                    key=bottleneck_file
                )
                bottlenecks_list.append(bottleneck_data)
                
            except Exception as e:
                logger.warning(f"Failed to read bottlenecks for {month}: {e}")
                continue
        
        if not bottlenecks_list:
            logger.error("No bottlenecks data retrieved")
            return pd.DataFrame()
        
        # Combine bottlenecks data
        bottlenecks = pd.concat(bottlenecks_list, ignore_index=True)
        
        # Calculate lengths for current and 1-year TMCs
        def calculate_length(tmc_list, tmcs_df):
            """Calculate total length for list of TMCs"""
            if pd.isna(tmc_list) or not tmc_list:
                return 0
            
            # Handle different data types for TMC lists
            if isinstance(tmc_list, str):
                # If string, try to parse as list
                try:
                    import ast
                    tmc_list = ast.literal_eval(tmc_list)
                except:
                    return 0
            elif not isinstance(tmc_list, (list, tuple)):
                return 0
            
            # Filter TMCs that exist in the reference data
            matching_tmcs = tmcs_df[tmcs_df['tmc'].isin(tmc_list)]
            return matching_tmcs['length'].sum() if len(matching_tmcs) > 0 else 0
        
        # Calculate lengths
        if 'tmcs_current' in bottlenecks.columns:
            bottlenecks['length_current'] = bottlenecks['tmcs_current'].apply(
                lambda x: calculate_length(x, tmcs)
            )
        else:
            bottlenecks['length_current'] = 0
        
        if 'tmcs_1yr' in bottlenecks.columns:
            bottlenecks['length_1yr'] = bottlenecks['tmcs_1yr'].apply(
                lambda x: calculate_length(x, tmcs)
            )
        else:
            bottlenecks['length_1yr'] = 0
        
        # Remove TMC list columns (equivalent to select(-c(tmcs_current, tmcs_1yr)))
        columns_to_drop = ['tmcs_current', 'tmcs_1yr']
        bottlenecks = bottlenecks.drop(columns=[col for col in columns_to_drop if col in bottlenecks.columns])
        
        logger.info(f"Bottlenecks data processed. Retrieved {len(bottlenecks)} records.")
        
        return bottlenecks
        
    except Exception as e:
        logger.error(f"Error in get_quarterly_bottlenecks: {e}")
        return pd.DataFrame()

def write_quarterly_bottlenecks(bottlenecks: pd.DataFrame, filename: str = "quarterly_bottlenecks.csv") -> bool:
    """
    Write quarterly bottlenecks data to CSV file
    
    Args:
        bottlenecks: Bottlenecks data DataFrame
        filename: Output filename
        
    Returns:
        Boolean indicating success
    """
    
    try:
        bottlenecks.to_csv(filename, index=False)
        logger.info(f"Quarterly bottlenecks data written to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error writing quarterly bottlenecks data: {e}")
        return False

def validate_quarterly_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate quarterly data quality
    
    Args:
        df: Quarterly data DataFrame
        
    Returns:
        Dictionary with validation results
    """
    
    try:
        validation_results = {
            'total_records': len(df),
            'unique_districts': df['District'].nunique() if 'District' in df.columns else 0,
            'unique_metrics': df['Metric'].nunique() if 'Metric' in df.columns else 0,
            'date_range': {
                'min_date': df['date'].min() if 'date' in df.columns else None,
                'max_date': df['date'].max() if 'date' in df.columns else None
            },
            'missing_values': {},
            'data_quality_issues': []
        }
        
        if df.empty:
            validation_results['data_quality_issues'].append("DataFrame is empty")
            return validation_results
        
        # Check for missing values
        for col in df.columns:
            missing_count = df[col].isna().sum()
            missing_pct = (missing_count / len(df)) * 100
            validation_results['missing_values'][col] = {
                'count': missing_count,
                'percentage': missing_pct
            }
            
            if missing_pct > 10:  # Flag if more than 10% missing
                validation_results['data_quality_issues'].append(
                    f"High missing data in {col}: {missing_pct:.1f}%"
                )
        
        # Check for duplicate records
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            validation_results['data_quality_issues'].append(
                f"Found {duplicates} duplicate records"
            )
        
        # Check for valid quarter format
        if 'Quarter' in df.columns:
            invalid_quarters = df[~df['Quarter'].astype(str).str.match(r'\d{4}\.\d{1}', na=False)]
            if len(invalid_quarters) > 0:
                validation_results['data_quality_issues'].append(
                    f"Found {len(invalid_quarters)} records with invalid quarter format"
                )
        
        # Check for negative values where they shouldn't exist
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col == 'value':
                negative_values = (df[col] < 0).sum()
                if negative_values > 0:
                    validation_results['data_quality_issues'].append(
                        f"Found {negative_values} negative values in {col}"
                    )
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating quarterly data: {e}")
        return {'error': str(e)}

def generate_quarterly_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate summary statistics for quarterly data
    
    Args:
        df: Quarterly data DataFrame
        
    Returns:
        DataFrame with summary statistics
    """
    
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Group by District and Metric to get summary stats
        summary = df.groupby(['District', 'Metric']).agg({
            'value': ['count', 'mean', 'median', 'std', 'min', 'max'],
            'Quarter': ['min', 'max']
        }).round(2)
        
        # Flatten column names
        summary.columns = ['_'.join(col).strip() for col in summary.columns]
        summary = summary.reset_index()
        
        # Rename columns for clarity
        summary = summary.rename(columns={
            'value_count': 'record_count',
            'value_mean': 'avg_value',
            'value_median': 'median_value',
            'value_std': 'std_value',
            'value_min': 'min_value',
            'value_max': 'max_value',
            'Quarter_min': 'first_quarter',
            'Quarter_max': 'last_quarter'
        })
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating quarterly summary: {e}")
        return pd.DataFrame()

def get_quarterly_trends(df: pd.DataFrame, metric: str = None) -> pd.DataFrame:
    """
    Calculate quarterly trends for metrics
    
    Args:
        df: Quarterly data DataFrame
        metric: Specific metric to analyze (if None, analyze all)
        
    Returns:
        DataFrame with trend analysis
    """
    
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Filter by metric if specified
        if metric:
            df_filtered = df[df['Metric'] == metric].copy()
        else:
            df_filtered = df.copy()
        
        if df_filtered.empty:
            return pd.DataFrame()
        
        # Convert Quarter to numeric for trend calculation
        df_filtered['Quarter_Numeric'] = df_filtered['Quarter'].str.replace('.', '').astype(float)
        
        # Calculate trends by District and Metric
        trends = []
        
        for (district, metric_name), group in df_filtered.groupby(['District', 'Metric']):
            if len(group) < 2:
                continue
            
            group_sorted = group.sort_values('Quarter_Numeric')
            
            # Calculate simple trend (slope)
            x = np.arange(len(group_sorted))
            y = group_sorted['value'].values
            
            if len(x) > 1 and not np.isnan(y).all():
                # Remove NaN values
                valid_mask = ~np.isnan(y)
                if valid_mask.sum() > 1:
                    x_valid = x[valid_mask]
                    y_valid = y[valid_mask]
                    
                    # Calculate linear trend
                    slope = np.polyfit(x_valid, y_valid, 1)[0]
                    
                    # Calculate percentage change
                    start_value = group_sorted['value'].iloc[0]
                    end_value = group_sorted['value'].iloc[-1]
                    pct_change = ((end_value - start_value) / start_value * 100) if start_value != 0 else 0
                    
                    trends.append({
                        'District': district,
                        'Metric': metric_name,
                        'Slope': slope,
                        'Percentage_Change': pct_change,
                        'Start_Value': start_value,
                        'End_Value': end_value,
                        'Start_Quarter': group_sorted['Quarter'].iloc[0],
                        'End_Quarter': group_sorted['Quarter'].iloc[-1],
                        'Data_Points': len(group_sorted),
                        'Trend_Direction': 'Increasing' if slope > 0 else 'Decreasing' if slope < 0 else 'Stable'
                    })
        
        return pd.DataFrame(trends)
        
    except Exception as e:
        logger.error(f"Error calculating quarterly trends: {e}")
        return pd.DataFrame()

def export_quarterly_report(data: pd.DataFrame, bottlenecks: pd.DataFrame = None, 
                          output_dir: str = "quarterly_output") -> bool:
    """
    Export complete quarterly report with all components
    
    Args:
        data: Main quarterly data
        bottlenecks: Bottlenecks data (optional)
        output_dir: Output directory
        
    Returns:
        Boolean indicating success
    """
    
    try:
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export main data
        data.to_csv(f"{output_dir}/quarterly_data.csv", index=False)
        logger.info(f"Exported quarterly data to {output_dir}/quarterly_data.csv")
        
        # Export bottlenecks if provided
        if bottlenecks is not None and not bottlenecks.empty:
            bottlenecks.to_csv(f"{output_dir}/quarterly_bottlenecks.csv", index=False)
            logger.info(f"Exported bottlenecks data to {output_dir}/quarterly_bottlenecks.csv")
        
        # Generate and export summary
        summary = generate_quarterly_summary(data)
        if not summary.empty:
            summary.to_csv(f"{output_dir}/quarterly_summary.csv", index=False)
            logger.info(f"Exported summary to {output_dir}/quarterly_summary.csv")
        
        # Generate and export trends
        trends = get_quarterly_trends(data)
        if not trends.empty:
            trends.to_csv(f"{output_dir}/quarterly_trends.csv", index=False)
            logger.info(f"Exported trends to {output_dir}/quarterly_trends.csv")
        
        # Generate validation report
        validation = validate_quarterly_data(data)
        
        # Write validation report
        with open(f"{output_dir}/quarterly_validation_report.txt", 'w') as f:
            f.write("QUARTERLY DATA VALIDATION REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write(f"Total Records: {validation['total_records']}\n")
            f.write(f"Unique Districts: {validation['unique_districts']}\n")
            f.write(f"Unique Metrics: {validation['unique_metrics']}\n")
            
            if validation['date_range']['min_date']:
                f.write(f"Date Range: {validation['date_range']['min_date']} to {validation['date_range']['max_date']}\n")
            
            f.write("\nData Quality Issues:\n")
            if validation['data_quality_issues']:
                for issue in validation['data_quality_issues']:
                    f.write(f"- {issue}\n")
            else:
                f.write("- No major issues detected\n")
            
            f.write("\nMissing Data Summary:\n")
            for col, missing_info in validation['missing_values'].items():
                f.write(f"- {col}: {missing_info['count']} missing ({missing_info['percentage']:.1f}%)\n")
        
        logger.info(f"Exported validation report to {output_dir}/quarterly_validation_report.txt")
        
        return True
        
    except Exception as e:
        logger.error(f"Error exporting quarterly report: {e}")
        return False

def filter_quarterly_data_by_date(df: pd.DataFrame, start_date: str = None, 
                                end_date: str = None) -> pd.DataFrame:
    """
    Filter quarterly data by date range
    
    Args:
        df: Quarterly data DataFrame
        start_date: Start date (YYYY-MM-DD format)
        end_date: End date (YYYY-MM-DD format)
        
    Returns:
        Filtered DataFrame
    """
    
    try:
        if df.empty or 'date' not in df.columns:
            return df
        
        filtered_df = df.copy()
        
        # Convert date column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(filtered_df['date']):
            filtered_df['date'] = pd.to_datetime(filtered_df['date'])
        
        # Apply date filters
        if start_date:
            start_dt = pd.to_datetime(start_date)
            filtered_df = filtered_df[filtered_df['date'] >= start_dt]
        
        if end_date:
            end_dt = pd.to_datetime(end_date)
            filtered_df = filtered_df[filtered_df['date'] <= end_dt]
        
        logger.info(f"Filtered data from {len(df)} to {len(filtered_df)} records")
        
        return filtered_df
        
    except Exception as e:
        logger.error(f"Error filtering quarterly data by date: {e}")
        return df

def get_quarterly_data_for_metrics(metric_names: List[str]) -> pd.DataFrame:
    """
    Get quarterly data for specific metrics only
    
    Args:
        metric_names: List of metric names to retrieve
        
    Returns:
        DataFrame with data for specified metrics
    """
    
    try:
        # Get all quarterly data
        all_data = get_quarterly_data()
        
        if all_data.empty:
            return pd.DataFrame()
        
        # Filter for specified metrics
        filtered_data = all_data[all_data['Metric'].isin(metric_names)]
        
        logger.info(f"Retrieved data for {len(metric_names)} metrics: {', '.join(metric_names)}")
        
        return filtered_data
        
    except Exception as e:
        logger.error(f"Error getting quarterly data for specific metrics: {e}")
        return pd.DataFrame()

def compare_quarterly_performance(df: pd.DataFrame, district1: str, district2: str, 
                                metric: str) -> pd.DataFrame:
    """
    Compare quarterly performance between two districts for a specific metric
    
    Args:
        df: Quarterly data DataFrame
        district1: First district name
        district2: Second district name
        metric: Metric name to compare
        
    Returns:
        DataFrame with comparison results
    """
    
    try:
        if df.empty:
            return pd.DataFrame()
        
        # Filter for specified districts and metric
        comparison_data = df[
            (df['District'].isin([district1, district2])) & 
            (df['Metric'] == metric)
        ].copy()
        
        if comparison_data.empty:
            logger.warning(f"No data found for comparison between {district1} and {district2} for metric {metric}")
            return pd.DataFrame()
        
        # Pivot to get districts as columns
        pivot_data = comparison_data.pivot_table(
            index=['Quarter', 'date'], 
            columns='District', 
            values='value', 
            aggfunc='first'
        ).reset_index()
        
        # Calculate difference and percentage difference
        if district1 in pivot_data.columns and district2 in pivot_data.columns:
            pivot_data['Difference'] = pivot_data[district1] - pivot_data[district2]
            pivot_data['Percent_Difference'] = (
                (pivot_data[district1] - pivot_data[district2]) / pivot_data[district2] * 100
            ).round(2)
            
            # Add comparison summary
            pivot_data['Better_Performance'] = np.where(
                pivot_data['Difference'] > 0, district1,
                np.where(pivot_data['Difference'] < 0, district2, 'Tie')
            )
        
        pivot_data['Metric'] = metric
        
        return pivot_data
        
    except Exception as e:
        logger.error(f"Error comparing quarterly performance: {e}")
        return pd.DataFrame()

def main():
    """
    Main function that executes the quarterly data pull and processing
    This replicates the bottom section of the R script
    """
    
    try:
        logger.info("Starting quarterly report data pull process")
        
        # Get quarterly data (equivalent to: df <- get_quarterly_data())
        df = get_quarterly_data()
        
        if not df.empty:
            # Write quarterly data (equivalent to: write_quarterly_data(df))
            write_quarterly_data(df)
            
            # Get and write bottlenecks data
            bottlenecks = get_quarterly_bottlenecks()
            if not bottlenecks.empty:
                write_quarterly_bottlenecks(bottlenecks)
            
            # Generate comprehensive report
            export_quarterly_report(df, bottlenecks)
            
            # Display summary information
            validation_results = validate_quarterly_data(df)
            logger.info(f"Data validation completed:")
            logger.info(f"  - Total records: {validation_results['total_records']}")
            logger.info(f"  - Unique districts: {validation_results['unique_districts']}")
            logger.info(f"  - Unique metrics: {validation_results['unique_metrics']}")
            
            if validation_results['data_quality_issues']:
                logger.warning("Data quality issues detected:")
                for issue in validation_results['data_quality_issues']:
                    logger.warning(f"  - {issue}")
            else:
                logger.info("No major data quality issues detected")
            
        else:
            logger.error("No quarterly data retrieved")
            
        logger.info("Quarterly report data pull process completed")
        
    except Exception as e:
        logger.error(f"Error in main quarterly data pull process: {e}")

def get_available_metrics() -> List[str]:
    """
    Get list of available metrics
    
    Returns:
        List of metric names
    """
    
    return [
        "Daily Traffic Volume",
        "AM Peak Hour Volume", 
        "PM Peak Hour Volume",
        "Throughput",
        "Arrivals on Green",
        "Progression Ratio",
        "Queue Spillback Rate",
        "Peak Period Split Failures",
        "Off Peak Split Failures",
        "Travel Time Index",
        "Planning Time Index",
        "Average Speed",
        "Daily Pedestrian Pushbuttons",
        "Detector Uptime",
        "Pedestrian Button Uptime",
        "CCTV Uptime",
        "Communication Uptime",
        "Tasks Reported",
        "Tasks Resolved", 
        "Tasks Outstanding",
        "Tasks Over 45 Days",
        "Mean Time to Repair"
    ]

def get_quarterly_data_summary() -> Dict[str, Any]:
    """
    Get a quick summary of available quarterly data
    
    Returns:
        Dictionary with data summary
    """
    
    try:
        df = get_quarterly_data()
        
        if df.empty:
            return {"error": "No data available"}
        
        summary = {
            "total_records": len(df),
            "districts": sorted(df['District'].unique().tolist()),
            "metrics": sorted(df['Metric'].unique().tolist()),
            "quarters": sorted(df['Quarter'].unique().tolist()),
            "date_range": {
                "start": df['date'].min().strftime('%Y-%m-%d') if 'date' in df.columns else None,
                "end": df['date'].max().strftime('%Y-%m-%d') if 'date' in df.columns else None
            }
        }
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting quarterly data summary: {e}")
        return {"error": str(e)}

def create_quarterly_dashboard_data(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Create data structures optimized for dashboard visualization
    
    Args:
        df: Quarterly data DataFrame
        
    Returns:
        Dictionary of DataFrames for different dashboard components
    """
    
    try:
        dashboard_data = {}
        
        if df.empty:
            return dashboard_data
        
        # Overall metrics summary by quarter
        dashboard_data['metrics_by_quarter'] = df.groupby(['Quarter', 'Metric']).agg({
            'value': ['mean', 'count']
        }).reset_index()
        dashboard_data['metrics_by_quarter'].columns = ['Quarter', 'Metric', 'avg_value', 'record_count']
        
        # District performance comparison
        dashboard_data['district_comparison'] = df.groupby(['District', 'Metric']).agg({
            'value': ['mean', 'std', 'count']
        }).reset_index()
        dashboard_data['district_comparison'].columns = ['District', 'Metric', 'avg_value', 'std_value', 'record_count']
        
        # Time series data for trending
        dashboard_data['time_series'] = df.pivot_table(
            index='date',
            columns=['District', 'Metric'],
            values='value',
            aggfunc='mean'
        ).reset_index()
        
        # Latest quarter snapshot
        latest_quarter = df['Quarter'].max()
        dashboard_data['latest_snapshot'] = df[df['Quarter'] == latest_quarter].copy()
        
        # Performance rankings
        latest_data = df[df['Quarter'] == latest_quarter]
        rankings = []
        
        for metric in latest_data['Metric'].unique():
            metric_data = latest_data[latest_data['Metric'] == metric].copy()
            metric_data['rank'] = metric_data['value'].rank(ascending=False, method='dense')
            rankings.append(metric_data)
        
        if rankings:
            dashboard_data['performance_rankings'] = pd.concat(rankings, ignore_index=True)
        
        logger.info(f"Created dashboard data with {len(dashboard_data)} components")
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error creating dashboard data: {e}")
        return {}

def get_quarterly_alerts(df: pd.DataFrame, thresholds: Dict[str, Dict[str, float]] = None) -> List[Dict[str, Any]]:
    """
    Generate alerts for metrics that fall outside normal ranges
    
    Args:
        df: Quarterly data DataFrame
        thresholds: Dictionary of thresholds by metric
        
    Returns:
        List of alert dictionaries
    """
    
    try:
        if df.empty:
            return []
        
        # Default thresholds (can be customized)
        if thresholds is None:
            thresholds = {
                'Detector Uptime': {'min': 85.0, 'max': 100.0},
                'CCTV Uptime': {'min': 90.0, 'max': 100.0},
                'Communication Uptime': {'min': 95.0, 'max': 100.0},
                'Travel Time Index': {'min': 0.8, 'max': 2.0},
                'Queue Spillback Rate': {'min': 0.0, 'max': 15.0}
            }
        
        alerts = []
        latest_quarter = df['Quarter'].max()
        latest_data = df[df['Quarter'] == latest_quarter]
        
        for metric, threshold in thresholds.items():
            metric_data = latest_data[latest_data['Metric'] == metric]
            
            for _, row in metric_data.iterrows():
                value = row['value']
                district = row['District']
                
                alert_type = None
                if 'min' in threshold and value < threshold['min']:
                    alert_type = 'Below Minimum'
                elif 'max' in threshold and value > threshold['max']:
                    alert_type = 'Above Maximum'
                
                if alert_type:
                    severity = 'High' if (
                        ('min' in threshold and value < threshold['min'] * 0.9) or
                        ('max' in threshold and value > threshold['max'] * 1.1)
                    ) else 'Medium'
                    
                    alerts.append({
                        'district': district,
                        'metric': metric,
                        'quarter': row['Quarter'],
                        'value': value,
                        'alert_type': alert_type,
                        'severity': severity,
                        'threshold': threshold
                    })
        
        # Sort alerts by severity
        alerts.sort(key=lambda x: (x['severity'] == 'Medium', x['district'], x['metric']))
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error generating quarterly alerts: {e}")
        return []

def backup_quarterly_data(df: pd.DataFrame, s3_bucket: str = None, s3_prefix: str = "quarterly_backups") -> bool:
    """
    Backup quarterly data to S3
    
    Args:
        df: Quarterly data DataFrame
        s3_bucket: S3 bucket name
        s3_prefix: S3 key prefix
        
    Returns:
        Boolean indicating success
    """
    
    try:
        if s3_bucket is None:
            s3_bucket = conf.get('bucket', '')
        
        if not s3_bucket:
            logger.warning("No S3 bucket specified for backup")
            return False
        
        # Create timestamped filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{s3_prefix}/quarterly_data_{timestamp}.parquet"
        
        # Convert to parquet for efficient storage
        import io
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(parquet_buffer, s3_bucket, s3_key)
        
        logger.info(f"Quarterly data backed up to s3://{s3_bucket}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"Error backing up quarterly data: {e}")
        return False

def load_quarterly_data_from_backup(s3_bucket: str = None, backup_date: str = None) -> pd.DataFrame:
    """
    Load quarterly data from S3 backup
    
    Args:
        s3_bucket: S3 bucket name
        backup_date: Specific backup date (YYYYMMDD format)
        
    Returns:
        DataFrame with backup data
    """
    
    try:
        if s3_bucket is None:
            s3_bucket = conf.get('bucket', '')
        
        if not s3_bucket:
            logger.error("No S3 bucket specified")
            return pd.DataFrame()
        
        s3_client = boto3.client('s3')
        prefix = "quarterly_backups/"
        
        # List available backups
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            logger.error("No backup files found")
            return pd.DataFrame()
        
        # Filter by date if specified
        backup_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.parquet')]
        
        if backup_date:
            backup_files = [f for f in backup_files if backup_date in f]
        
        if not backup_files:
            logger.error(f"No backup files found for date {backup_date}")
            return pd.DataFrame()
        
        # Use most recent backup
        latest_backup = sorted(backup_files)[-1]
        
        # Download and load data
        df = s3read_using(
            pd.read_parquet,
            bucket=s3_bucket,
            key=latest_backup
        )
        
        logger.info(f"Loaded quarterly data from backup: {latest_backup}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading quarterly data from backup: {e}")
        return pd.DataFrame()

# Utility functions for data processing
def convert_quarter_to_fiscal(quarter_str: str, fiscal_start_month: int = 7) -> str:
    """
    Convert calendar quarter to fiscal quarter
    
    Args:
        quarter_str: Quarter string in format "YYYY.Q"
        fiscal_start_month: Month when fiscal year starts (default: July = 7)
        
    Returns:
        Fiscal quarter string
    """
    
    try:
        year, quarter = quarter_str.split('.')
        year = int(year)
        quarter = int(quarter)
        
        # Convert quarter to month
        quarter_start_month = (quarter - 1) * 3 + 1
        
        # Calculate fiscal year and quarter
        if quarter_start_month >= fiscal_start_month:
            fiscal_year = year
            fiscal_quarter = ((quarter_start_month - fiscal_start_month) // 3) + 1
        else:
            fiscal_year = year - 1
            fiscal_quarter = ((quarter_start_month + 12 - fiscal_start_month) // 3) + 1
        
        return f"{fiscal_year}.{fiscal_quarter}"
        
    except Exception as e:
        logger.error(f"Error converting quarter to fiscal: {e}")
        return quarter_str

def calculate_year_over_year_change(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate year-over-year changes for quarterly data
    
    Args:
        df: Quarterly data DataFrame
        
    Returns:
        DataFrame with year-over-year changes
    """
    
    try:
        if df.empty or 'Quarter' not in df.columns:
            return pd.DataFrame()
        
        # Extract year and quarter
        df_copy = df.copy()
        df_copy[['fiscal_year', 'quarter_num']] = df_copy['Quarter'].str.split('.', expand=True)
        df_copy['fiscal_year'] = pd.to_numeric(df_copy['fiscal_year'])
        df_copy['quarter_num'] = pd.to_numeric(df_copy['quarter_num'])
        
        # Calculate year-over-year changes
        yoy_changes = []
        
        for (district, metric, quarter_num), group in df_copy.groupby(['District', 'Metric', 'quarter_num']):
            group_sorted = group.sort_values('fiscal_year')
            
            for i in range(1, len(group_sorted)):
                current = group_sorted.iloc[i]
                previous = group_sorted.iloc[i-1]
                
                if current['fiscal_year'] == previous['fiscal_year'] + 1:
                    change = current['value'] - previous['value']
                    pct_change = (change / previous['value'] * 100) if previous['value'] != 0 else 0
                    
                    yoy_changes.append({
                        'District': district,
                        'Metric': metric,
                        'Quarter': current['Quarter'],
                        'Current_Value': current['value'],
                        'Previous_Year_Value': previous['value'],
                        'YoY_Change': change,
                        'YoY_Percent_Change': pct_change,
                        'Current_Year': current['fiscal_year'],
                        'Previous_Year': previous['fiscal_year']
                    })
        
        return pd.DataFrame(yoy_changes)
        
    except Exception as e:
        logger.error(f"Error calculating year-over-year changes: {e}")
        return pd.DataFrame()

# API-style functions for integration with web services
def get_quarterly_data_api(district: str = None, metric: str = None, 
                          start_quarter: str = None, end_quarter: str = None) -> Dict[str, Any]:
    """
    API-style function to get quarterly data with filtering
    
    Args:
        district: District name filter
        metric: Metric name filter
        start_quarter: Start quarter filter (YYYY.Q format)
        end_quarter: End quarter filter (YYYY.Q format)
        
    Returns:
        Dictionary with data and metadata
    """
    
    try:
        # Get base data
        df = get_quarterly_data()
        
        if df.empty:
            return {
                "status": "error",
                "message": "No data available",
                "data": [],
                "metadata": {}
            }
        
        # Apply filters
        filtered_df = df.copy()
        
        if district:
            filtered_df = filtered_df[filtered_df['District'] == district]
        
        if metric:
            filtered_df = filtered_df[filtered_df['Metric'] == metric]
        
        if start_quarter:
            filtered_df = filtered_df[filtered_df['Quarter'] >= start_quarter]
        
        if end_quarter:
            filtered_df = filtered_df[filtered_df['Quarter'] <= end_quarter]
        
        # Convert to records format for JSON serialization
        data_records = filtered_df.to_dict('records')
        
        # Convert datetime objects to strings
        for record in data_records:
            if 'date' in record and pd.notna(record['date']):
                record['date'] = record['date'].strftime('%Y-%m-%d')
        
        # Create metadata
        metadata = {
            "total_records": len(data_records),
            "filters_applied": {
                "district": district,
                "metric": metric,
                "start_quarter": start_quarter,
                "end_quarter": end_quarter
            },
            "available_districts": sorted(df['District'].unique().tolist()),
            "available_metrics": sorted(df['Metric'].unique().tolist()),
            "quarter_range": {
                "min": df['Quarter'].min(),
                "max": df['Quarter'].max()
            }
        }
        
        return {
            "status": "success",
            "data": data_records,
            "metadata": metadata
        }
        
    except Exception as e:
        logger.error(f"Error in get_quarterly_data_api: {e}")
        return {
            "status": "error",
            "message": str(e),
            "data": [],
            "metadata": {}
        }

def get_quarterly_bottlenecks_api() -> Dict[str, Any]:
    """
    API-style function to get bottlenecks data
    
    Returns:
        Dictionary with bottlenecks data and metadata
    """
    
    try:
        bottlenecks = get_quarterly_bottlenecks()
        
        if bottlenecks.empty:
            return {
                "status": "error",
                "message": "No bottlenecks data available",
                "data": [],
                "metadata": {}
            }
        
        # Convert to records format
        data_records = bottlenecks.to_dict('records')
        
        # Create metadata
        metadata = {
            "total_records": len(data_records),
            "data_columns": list(bottlenecks.columns),
            "summary_stats": {
                "avg_length_current": bottlenecks['length_current'].mean() if 'length_current' in bottlenecks.columns else None,
                "avg_length_1yr": bottlenecks['length_1yr'].mean() if 'length_1yr' in bottlenecks.columns else None
            }
        }
        
        return {
            "status": "success",
            "data": data_records,
            "metadata": metadata
        }
        
    except Exception as e:
        logger.error(f"Error in get_quarterly_bottlenecks_api: {e}")
        return {
            "status": "error",
            "message": str(e),
            "data": [],
            "metadata": {}
        }

def health_check() -> Dict[str, Any]:
    """
    Health check function to verify system status
    
    Returns:
        Dictionary with system health status
    """
    
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "checks": {}
        }
        
        # Check database connection
        try:
            conn = get_aurora_connection()
            health_status["checks"]["database"] = "connected"
            conn.close()
        except Exception as e:
            health_status["checks"]["database"] = f"error: {str(e)}"
            health_status["status"] = "unhealthy"
        
        # Check S3 connection
        try:
            s3_client = boto3.client('s3')
            bucket = conf.get('bucket', '')
            if bucket:
                s3_client.head_bucket(Bucket=bucket)
                health_status["checks"]["s3"] = "connected"
            else:
                health_status["checks"]["s3"] = "no bucket configured"
        except Exception as e:
            health_status["checks"]["s3"] = f"error: {str(e)}"
            health_status["status"] = "unhealthy"
        
        # Check data availability
        try:
            df = get_quarterly_data()
            if not df.empty:
                health_status["checks"]["data"] = f"available ({len(df)} records)"
            else:
                health_status["checks"]["data"] = "no data available"
                health_status["status"] = "degraded"
        except Exception as e:
            health_status["checks"]["data"] = f"error: {str(e)}"
            health_status["status"] = "unhealthy"
        
        return health_status
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }

def schedule_quarterly_data_pull(schedule_time: str = "0 2 * * 1") -> bool:
    """
    Schedule quarterly data pull using cron-like syntax
    
    Args:
        schedule_time: Cron-like schedule string (default: 2 AM every Monday)
        
    Returns:
        Boolean indicating success
    """
    
    try:
        # This would integrate with a job scheduler like Celery, APScheduler, etc.
        # For now, just log the schedule request
        logger.info(f"Quarterly data pull scheduled for: {schedule_time}")
        
        # Example implementation with APScheduler (requires: pip install apscheduler)
        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger
            
            scheduler = BackgroundScheduler()
            
            # Parse cron expression (simplified)
            cron_parts = schedule_time.split()
            if len(cron_parts) == 5:
                minute, hour, day, month, day_of_week = cron_parts
                
                scheduler.add_job(
                    func=main,
                    trigger=CronTrigger(
                        minute=minute,
                        hour=hour,
                        day=day,
                        month=month,
                        day_of_week=day_of_week
                    ),
                    id='quarterly_data_pull',
                    replace_existing=True
                )
                
                scheduler.start()
                logger.info("Scheduler started successfully")
                return True
            else:
                logger.error("Invalid cron expression format")
                return False
                
        except ImportError:
            logger.warning("APScheduler not available. Install with: pip install apscheduler")
            return False
        
    except Exception as e:
        logger.error(f"Error scheduling quarterly data pull: {e}")
        return False

def cleanup_old_backups(s3_bucket: str = None, retention_days: int = 90) -> bool:
    """
    Clean up old backup files from S3
    
    Args:
        s3_bucket: S3 bucket name
        retention_days: Number of days to retain backups
        
    Returns:
        Boolean indicating success
    """
    
    try:
        if s3_bucket is None:
            s3_bucket = conf.get('bucket', '')
        
        if not s3_bucket:
            logger.error("No S3 bucket specified")
            return False
        
        s3_client = boto3.client('s3')
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        # List backup files
        response = s3_client.list_objects_v2(
            Bucket=s3_bucket,
            Prefix='quarterly_backups/'
        )
        
        if 'Contents' not in response:
            logger.info("No backup files found")
            return True
        
        # Delete old files
        deleted_count = 0
        for obj in response['Contents']:
            if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                s3_client.delete_object(Bucket=s3_bucket, Key=obj['Key'])
                deleted_count += 1
                logger.info(f"Deleted old backup: {obj['Key']}")
        
        logger.info(f"Cleaned up {deleted_count} old backup files")
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning up old backups: {e}")
        return False

# Performance monitoring functions
def monitor_data_pull_performance() -> Dict[str, Any]:
    """
    Monitor performance of data pull operations
    
    Returns:
        Dictionary with performance metrics
    """
    
    try:
        start_time = datetime.now()
        
        # Test data pull performance
        df = get_quarterly_data()
        data_pull_time = (datetime.now() - start_time).total_seconds()
        
        # Test bottlenecks pull performance
        start_time = datetime.now()
        bottlenecks = get_quarterly_bottlenecks()
        bottlenecks_pull_time = (datetime.now() - start_time).total_seconds()
        
        performance_metrics = {
            "timestamp": datetime.now().isoformat(),
            "data_pull_time_seconds": data_pull_time,
            "bottlenecks_pull_time_seconds": bottlenecks_pull_time,
            "total_records": len(df) if not df.empty else 0,
            "bottlenecks_records": len(bottlenecks) if not bottlenecks.empty else 0,
            "records_per_second": len(df) / data_pull_time if data_pull_time > 0 and not df.empty else 0
        }
        
        return performance_metrics
        
    except Exception as e:
        logger.error(f"Error monitoring data pull performance: {e}")
        return {"error": str(e)}

# Configuration management
def update_metric_definitions(new_metrics: List[Dict[str, str]]) -> bool:
    """
    Update metric definitions dynamically
    
    Args:
        new_metrics: List of metric dictionaries with 'label', 'table', 'variable' keys
        
    Returns:
        Boolean indicating success
    """
    
    try:
        global vpd, am_peak_vph, pm_peak_vph, throughput, arrivals_on_green
        global progression_ratio, queue_spillback_rate, peak_period_split_failures
        global off_peak_split_failures, travel_time_index, planning_time_index
        global average_speed, daily_pedestrian_pushbuttons, detector_uptime
        global ped_button_uptime, cctv_uptime, comm_uptime, tasks_reported
        global tasks_resolved, tasks_outstanding, tasks_over45, tasks_mttr
        
        # Save current metrics as backup
        current_metrics = [
            vpd, am_peak_vph, pm_peak_vph, throughput, arrivals_on_green,
            progression_ratio, queue_spillback_rate, peak_period_split_failures,
            off_peak_split_failures, travel_time_index, planning_time_index,
            average_speed, daily_pedestrian_pushbuttons, detector_uptime,
            ped_button_uptime, cctv_uptime, comm_uptime, tasks_reported,
            tasks_resolved, tasks_outstanding, tasks_over45, tasks_mttr
        ]
        
        # Update metrics
        for metric_def in new_metrics:
            if all(key in metric_def for key in ['label', 'table', 'variable']):
                # Create new metric object
                new_metric = Metric(
                    metric_def['label'],
                    metric_def['table'],
                    metric_def['variable']
                )
                
                # This is a simplified example - in practice, you'd want
                # more sophisticated metric management
                logger.info(f"Added new metric: {new_metric.label}")
            else:
                logger.warning(f"Invalid metric definition: {metric_def}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating metric definitions: {e}")
        return False

# Data quality monitoring
def monitor_data_quality() -> Dict[str, Any]:
    """
    Monitor data quality metrics over time
    
    Returns:
        Dictionary with data quality assessment
    """
    
    try:
        df = get_quarterly_data()
        
        if df.empty:
            return {"error": "No data available for quality assessment"}
        
        quality_metrics = {
            "timestamp": datetime.now().isoformat(),
            "total_records": len(df),
            "completeness": {},
            "consistency": {},
            "timeliness": {},
            "accuracy": {}
        }
        
        # Completeness - percentage of non-null values
        for col in df.columns:
            non_null_pct = (df[col].notna().sum() / len(df)) * 100
            quality_metrics["completeness"][col] = round(non_null_pct, 2)
        
        # Consistency - check for data format consistency
        if 'Quarter' in df.columns:
            valid_quarter_format = df['Quarter'].astype(str).str.match(r'\d{4}\.\d{1}', na=False).sum()
            quality_metrics["consistency"]["quarter_format"] = round((valid_quarter_format / len(df)) * 100, 2)
        
        # Timeliness - check how recent the data is
        if 'date' in df.columns:
            latest_date = pd.to_datetime(df['date']).max()
            days_since_latest = (datetime.now() - latest_date).days
            quality_metrics["timeliness"]["days_since_latest_data"] = days_since_latest
            quality_metrics["timeliness"]["data_freshness"] = "Fresh" if days_since_latest <= 7 else "Stale"
        
        # Accuracy - check for outliers and invalid values
        if 'value' in df.columns:
            numeric_values = pd.to_numeric(df['value'], errors='coerce')
            outliers = len(numeric_values[(numeric_values < numeric_values.quantile(0.01)) | 
                                        (numeric_values > numeric_values.quantile(0.99))])
            quality_metrics["accuracy"]["outlier_percentage"] = round((outliers / len(df)) * 100, 2)
        
        return quality_metrics
        
    except Exception as e:
        logger.error(f"Error monitoring data quality: {e}")
        return {"error": str(e)}

# Integration with notification systems
def send_data_quality_alert(quality_metrics: Dict[str, Any], 
                           alert_thresholds: Dict[str, float] = None) -> bool:
    """
    Send alerts when data quality falls below thresholds
    
    Args:
        quality_metrics: Data quality metrics from monitor_data_quality()
        alert_thresholds: Thresholds for triggering alerts
        
    Returns:
        Boolean indicating if alerts were sent
    """
    
    try:
        if alert_thresholds is None:
            alert_thresholds = {
                'completeness_threshold': 95.0,  # 95% completeness required
                'outlier_threshold': 5.0,        # Max 5% outliers
                'freshness_days': 3              # Data should be max 3 days old
            }
        
        alerts_triggered = []
        
        # Check completeness
        if 'completeness' in quality_metrics:
            for col, completeness in quality_metrics['completeness'].items():
                if completeness < alert_thresholds['completeness_threshold']:
                    alerts_triggered.append({
                        'type': 'completeness',
                        'column': col,
                        'value': completeness,
                        'threshold': alert_thresholds['completeness_threshold'],
                        'message': f"Column {col} completeness ({completeness}%) below threshold ({alert_thresholds['completeness_threshold']}%)"
                    })
        
        # Check outliers
        if 'accuracy' in quality_metrics and 'outlier_percentage' in quality_metrics['accuracy']:
            outlier_pct = quality_metrics['accuracy']['outlier_percentage']
            if outlier_pct > alert_thresholds['outlier_threshold']:
                alerts_triggered.append({
                    'type': 'outliers',
                    'value': outlier_pct,
                    'threshold': alert_thresholds['outlier_threshold'],
                    'message': f"Outlier percentage ({outlier_pct}%) above threshold ({alert_thresholds['outlier_threshold']}%)"
                })
        
        # Check freshness
        if 'timeliness' in quality_metrics and 'days_since_latest_data' in quality_metrics['timeliness']:
            days_old = quality_metrics['timeliness']['days_since_latest_data']
            if days_old > alert_thresholds['freshness_days']:
                alerts_triggered.append({
                    'type': 'freshness',
                    'value': days_old,
                    'threshold': alert_thresholds['freshness_days'],
                    'message': f"Data is {days_old} days old, exceeding threshold of {alert_thresholds['freshness_days']} days"
                })
        
        # Send alerts if any were triggered
        if alerts_triggered:
            logger.warning(f"Data quality alerts triggered: {len(alerts_triggered)} issues found")
            for alert in alerts_triggered:
                logger.warning(f"ALERT: {alert['message']}")
            
            # Here you would integrate with your notification system
            # Examples: email, Slack, PagerDuty, etc.
            # send_email_alert(alerts_triggered)
            # send_slack_alert(alerts_triggered)
            
            return True
        else:
            logger.info("Data quality check passed - no alerts triggered")
            return False
        
    except Exception as e:
        logger.error(f"Error sending data quality alerts: {e}")
        return False

def generate_quarterly_report_summary() -> str:
    """
    Generate a text summary of the quarterly report data
    
    Returns:
        String with formatted summary
    """
    
    try:
        df = get_quarterly_data()
        
        if df.empty:
            return "No quarterly data available for summary."
        
        # Get basic statistics
        total_records = len(df)
        unique_districts = df['District'].nunique()
        unique_metrics = df['Metric'].nunique()
        date_range = f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}"
        
        # Get latest quarter performance
        latest_quarter = df['Quarter'].max()
        latest_data = df[df['Quarter'] == latest_quarter]
        
        summary = f"""
QUARTERLY REPORT DATA SUMMARY
{'=' * 50}

Data Overview:
- Total Records: {total_records:,}
- Districts: {unique_districts}
- Metrics: {unique_metrics}
- Date Range: {date_range}
- Latest Quarter: {latest_quarter}

Latest Quarter Highlights:
- Records in Latest Quarter: {len(latest_data):,}
"""
        
        # Add top performing districts by metric (example with uptime metrics)
        uptime_metrics = ['Detector Uptime', 'CCTV Uptime', 'Communication Uptime']
        for metric in uptime_metrics:
            metric_data = latest_data[latest_data['Metric'] == metric]
            if not metric_data.empty:
                top_district = metric_data.loc[metric_data['value'].idxmax()]
                summary += f"- Best {metric}: {top_district['District']} ({top_district['value']:.1f}%)\n"
        
        # Add data quality summary
        quality_metrics = monitor_data_quality()
        if 'completeness' in quality_metrics:
            avg_completeness = np.mean(list(quality_metrics['completeness'].values()))
            summary += f"\nData Quality:\n- Average Completeness: {avg_completeness:.1f}%\n"
        
        if 'timeliness' in quality_metrics:
            freshness = quality_metrics['timeliness'].get('data_freshness', 'Unknown')
            summary += f"- Data Freshness: {freshness}\n"
        
        summary += f"\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return summary
        
    except Exception as e:
        logger.error(f"Error generating quarterly report summary: {e}")
        return f"Error generating summary: {str(e)}"

# Export all functions for easy importing
__all__ = [
    'get_quarterly_data',
    'write_quarterly_data', 
    'get_quarterly_bottlenecks',
    'write_quarterly_bottlenecks',
    'validate_quarterly_data',
    'generate_quarterly_summary',
    'get_quarterly_trends',
    'export_quarterly_report',
    'filter_quarterly_data_by_date',
    'get_quarterly_data_for_metrics',
    'compare_quarterly_performance',
    'get_available_metrics',
    'get_quarterly_data_summary',
    'create_quarterly_dashboard_data',
    'get_quarterly_alerts',
    'backup_quarterly_data',
    'load_quarterly_data_from_backup',
    'get_quarterly_data_api',
    'get_quarterly_bottlenecks_api',
    'health_check',
    'schedule_quarterly_data_pull',
    'cleanup_old_backups',
    'monitor_data_pull_performance',
    'monitor_data_quality',
    'send_data_quality_alert',
    'generate_quarterly_report_summary',
    'main'
]

# Main execution (equivalent to the bottom of the R script)
if __name__ == "__main__":
    main()
