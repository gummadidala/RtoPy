"""
Utility functions for health metrics calculations
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

def calculate_weighted_health_score(scores: pd.DataFrame, weights: pd.DataFrame) -> pd.Series:
    """
    Calculate weighted health score from individual metric scores
    
    Args:
        scores: DataFrame with individual metric scores
        weights: DataFrame with corresponding weights
        
    Returns:
        Series with calculated health scores
    """
    
    try:
        # Ensure scores and weights have same columns
        common_cols = scores.columns.intersection(weights.columns)
        
        if len(common_cols) == 0:
            logger.warning("No common columns between scores and weights")
            return pd.Series(np.nan, index=scores.index)
        
        scores_subset = scores[common_cols].fillna(0)
        weights_subset = weights[common_cols].fillna(0)
        
        # Calculate weighted sum
        weighted_sum = (scores_subset * weights_subset).sum(axis=1)
        weight_sum = weights_subset.sum(axis=1)
        
        # Avoid division by zero
        health_score = weighted_sum / weight_sum.replace(0, np.nan) / 10
        
        return health_score
        
    except Exception as e:
        logger.error(f"Error calculating weighted health score: {e}")
        return pd.Series(np.nan, index=scores.index)


def interpolate_missing_scores(df: pd.DataFrame, method: str = 'linear') -> pd.DataFrame:
    """
    Interpolate missing health scores using various methods
    
    Args:
        df: DataFrame with health scores
        method: Interpolation method ('linear', 'forward', 'backward')
        
    Returns:
        DataFrame with interpolated values
    """
    
    try:
        result = df.copy()
        
        # Get numeric columns that likely contain scores
        score_cols = [col for col in df.columns if 'Score' in col or 'Health' in col]
        
        for col in score_cols:
            if col in result.columns:
                if method == 'linear':
                    result[col] = result[col].interpolate(method='linear')
                elif method == 'forward':
                    result[col] = result[col].fillna(method='ffill')
                elif method == 'backward':
                    result[col] = result[col].fillna(method='bfill')
        
        return result
        
    except Exception as e:
        logger.error(f"Error interpolating missing scores: {e}")
        return df


def calculate_health_percentiles(df: pd.DataFrame, percentiles: List[float] = [25, 50, 75, 90]) -> Dict[str, float]:
    """
    Calculate health score percentiles for benchmarking
    
    Args:
        df: DataFrame with health scores
        percentiles: List of percentiles to calculate
        
    Returns:
        Dictionary with percentile values
    """
    
    try:
        if 'Percent Health' not in df.columns:
            return {}
        
        health_scores = df['Percent Health'].dropna()
        
        if len(health_scores) == 0:
            return {}
        
        result = {}
        for p in percentiles:
            result[f'p{p}'] = np.percentile(health_scores, p)
        
        result['mean'] = health_scores.mean()
        result['std'] = health_scores.std()
        result['count'] = len(health_scores)
        
        return result
        
    except Exception as e:
        logger.error(f"Error calculating health percentiles: {e}")
        return {}


def rank_corridors_by_health(df: pd.DataFrame, ascending: bool = False) -> pd.DataFrame:
    """
    Rank corridors by health score
    
    Args:
        df: DataFrame with health scores
        ascending: Whether to rank in ascending order (worst first)
        
    Returns:
        DataFrame with rankings added
    """
    
    try:
        if 'Percent Health' not in df.columns:
            return df
        
        result = df.copy()
        
        # Calculate rank
        result['Health_Rank'] = result['Percent Health'].rank(ascending=ascending, method='min')
        result['Health_Percentile'] = result['Percent Health'].rank(pct=True) * 100
        
        # Sort by rank
        result = result.sort_values('Health_Rank')
        
        return result
        
    except Exception as e:
        logger.error(f"Error ranking corridors by health: {e}")
        return df


def calculate_health_variance(df: pd.DataFrame, group_cols: List[str] = ['Zone_Group']) -> pd.DataFrame:
    """
    Calculate health score variance within groups
    
    Args:
        df: DataFrame with health scores
        group_cols: Columns to group by for variance calculation
        
    Returns:
        DataFrame with variance statistics
    """
    
    try:
        if 'Percent Health' not in df.columns:
            return pd.DataFrame()
        
        # Filter group columns that exist
        existing_group_cols = [col for col in group_cols if col in df.columns]
        
        if not existing_group_cols:
            logger.warning("No valid group columns found")
            return pd.DataFrame()
        
        variance_stats = df.groupby(existing_group_cols)['Percent Health'].agg([
            'count', 'mean', 'std', 'min', 'max', 'var'
        ]).reset_index()
        
        # Calculate coefficient of variation
        variance_stats['cv'] = variance_stats['std'] / variance_stats['mean']
        variance_stats['range'] = variance_stats['max'] - variance_stats['min']
        
        return variance_stats
        
    except Exception as e:
        logger.error(f"Error calculating health variance: {e}")
        return pd.DataFrame()


def detect_health_anomalies(df: pd.DataFrame, method: str = 'iqr', 
                          factor: float = 1.5) -> pd.DataFrame:
    """
    Detect anomalous health scores using statistical methods
    
    Args:
        df: DataFrame with health scores
        method: Detection method ('iqr', 'zscore', 'modified_zscore')
        factor: Multiplier for anomaly detection threshold
        
    Returns:
        DataFrame with anomaly flags
    """
    
    try:
        if 'Percent Health' not in df.columns:
            return df
        
        result = df.copy()
        health_scores = df['Percent Health'].dropna()
        
        if len(health_scores) < 3:
            result['is_anomaly'] = False
            return result
        
        if method == 'iqr':
            Q1 = health_scores.quantile(0.25)
            Q3 = health_scores.quantile(0.75)
            IQR = Q3 - Q1
            
            lower_bound = Q1 - factor * IQR
            upper_bound = Q3 + factor * IQR
            
            result['is_anomaly'] = (result['Percent Health'] < lower_bound) | \
                                 (result['Percent Health'] > upper_bound)
        
        elif method == 'zscore':
            mean_health = health_scores.mean()
            std_health = health_scores.std()
            
            z_scores = np.abs((result['Percent Health'] - mean_health) / std_health)
            result['is_anomaly'] = z_scores > factor
            result['z_score'] = z_scores
        
        elif method == 'modified_zscore':
            median_health = health_scores.median()
            mad = np.median(np.abs(health_scores - median_health))
            
            modified_z_scores = 0.6745 * (result['Percent Health'] - median_health) / mad
            result['is_anomaly'] = np.abs(modified_z_scores) > factor
            result['modified_z_score'] = modified_z_scores
        
        return result
        
    except Exception as e:
        logger.error(f"Error detecting health anomalies: {e}")
        return df


def create_health_dashboard_data(health_data: Dict) -> Dict[str, Any]:
    """
    Prepare data for health metrics dashboard
    
    Args:
        health_data: Health metrics data dictionary
        
    Returns:
        Dictionary with dashboard-ready data
    """
    
    try:
        dashboard_data = {
            'summary': {},
            'trends': {},
            'alerts': [],
            'rankings': {},
            'statistics': {}
        }
        
        for level in ['cor', 'sub', 'sig']:
            level_name = {'cor': 'Corridor', 'sub': 'Subcorridor', 'sig': 'Signal'}[level]
            
            if level in health_data and 'mo' in health_data[level]:
                dashboard_data['summary'][level] = {}
                dashboard_data['rankings'][level] = {}
                dashboard_data['statistics'][level] = {}
                
                for metric_type in ['maint', 'ops', 'safety']:
                    if metric_type in health_data[level]['mo']:
                        df = health_data[level]['mo'][metric_type]
                        
                        if not df.empty and 'Percent Health' in df.columns:
                            # Summary statistics
                            summary = {
                                'mean_health': df['Percent Health'].mean(),
                                'median_health': df['Percent Health'].median(),
                                'min_health': df['Percent Health'].min(),
                                'max_health': df['Percent Health'].max(),
                                'std_health': df['Percent Health'].std(),
                                'count': len(df)
                            }
                            dashboard_data['summary'][level][metric_type] = summary
                            
                            # Rankings
                            rankings = rank_corridors_by_health(df)
                            dashboard_data['rankings'][level][metric_type] = rankings.head(10).to_dict('records')
                            
                            # Percentiles
                            percentiles = calculate_health_percentiles(df)
                            dashboard_data['statistics'][level][metric_type] = percentiles
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error creating dashboard data: {e}")
        return {}


def export_health_metrics_excel(health_data: Dict, filename: str = "health_metrics.xlsx") -> bool:
    """
    Export health metrics to Excel with multiple sheets
    
    Args:
        health_data: Health metrics data dictionary
        filename: Output Excel filename
        
    Returns:
        Boolean indicating success
    """
    
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            
            for level in ['cor', 'sub', 'sig']:
                level_name = {'cor': 'Corridor', 'sub': 'Subcorridor', 'sig': 'Signal'}[level]
                
                if level in health_data and 'mo' in health_data[level]:
                    for metric_type in ['maint', 'ops', 'safety']:
                        if metric_type in health_data[level]['mo']:
                            df = health_data[level]['mo'][metric_type]
                            
                            if not df.empty:
                                sheet_name = f"{level}_{metric_type}"[:31]  # Excel sheet name limit
                                df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # Create summary sheet
            summary_data = []
            for level in ['cor', 'sub', 'sig']:
                if level in health_data and 'mo' in health_data[level]:
                    for metric_type in ['maint', 'ops', 'safety']:
                        if metric_type in health_data[level]['mo']:
                            df = health_data[level]['mo'][metric_type]
                            if not df.empty and 'Percent Health' in df.columns:
                                summary_data.append({
                                    'Level': level,
                                    'Metric_Type': metric_type,
                                    'Count': len(df),
                                    'Mean_Health': df['Percent Health'].mean(),
                                    'Min_Health': df['Percent Health'].min(),
                                    'Max_Health': df['Percent Health'].max(),
                                    'Std_Health': df['Percent Health'].std()
                                })
            
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Health metrics exported to {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")
        return False


def compare_health_periods(df1: pd.DataFrame, df2: pd.DataFrame, 
                         period1_name: str = "Period 1", 
                         period2_name: str = "Period 2") -> pd.DataFrame:
    """
    Compare health metrics between two time periods
    
    Args:
        df1: DataFrame for first period
        df2: DataFrame for second period
        period1_name: Name for first period
        period2_name: Name for second period
        
    Returns:
        DataFrame with comparison results
    """
    
    try:
        if 'Percent Health' not in df1.columns or 'Percent Health' not in df2.columns:
            return pd.DataFrame()
        # Merge dataframes on common identifier columns
        id_cols = ['Corridor']
        if 'Zone_Group' in df1.columns and 'Zone_Group' in df2.columns:
            id_cols = ['Zone_Group', 'Corridor']
        
        # Select relevant columns for comparison
        df1_subset = df1[id_cols + ['Percent Health']].rename(columns={
            'Percent Health': f'{period1_name}_Health'
        })
        
        df2_subset = df2[id_cols + ['Percent Health']].rename(columns={
            'Percent Health': f'{period2_name}_Health'
        })
        
        # Merge dataframes
        comparison = pd.merge(df1_subset, df2_subset, on=id_cols, how='outer')
        
        # Calculate changes
        comparison['Health_Change'] = comparison[f'{period2_name}_Health'] - comparison[f'{period1_name}_Health']
        comparison['Health_Change_Pct'] = (comparison['Health_Change'] / comparison[f'{period1_name}_Health']) * 100
        
        # Categorize changes
        comparison['Change_Category'] = pd.cut(
            comparison['Health_Change'],
            bins=[-np.inf, -5, -1, 1, 5, np.inf],
            labels=['Large Decline', 'Small Decline', 'Stable', 'Small Improvement', 'Large Improvement']
        )
        
        # Sort by change magnitude
        comparison = comparison.sort_values('Health_Change', ascending=False)
        
        return comparison
        
    except Exception as e:
        logger.error(f"Error comparing health periods: {e}")
        return pd.DataFrame()


def calculate_health_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate correlations between different health metrics
    
    Args:
        df: DataFrame with multiple health score columns
        
    Returns:
        DataFrame with correlation matrix
    """
    
    try:
        # Get all score columns
        score_cols = [col for col in df.columns if 'Score' in col]
        
        if len(score_cols) < 2:
            return pd.DataFrame()
        
        # Calculate correlation matrix
        correlation_matrix = df[score_cols].corr()
        
        return correlation_matrix
        
    except Exception as e:
        logger.error(f"Error calculating health correlations: {e}")
        return pd.DataFrame()


def generate_health_improvement_recommendations(df: pd.DataFrame, 
                                               threshold: float = 75.0) -> List[Dict[str, Any]]:
    """
    Generate recommendations for corridors with low health scores
    
    Args:
        df: DataFrame with health metrics
        threshold: Health score threshold for recommendations
        
    Returns:
        List of recommendation dictionaries
    """
    
    try:
        recommendations = []
        
        if 'Percent Health' not in df.columns:
            return recommendations
        
        # Find corridors below threshold
        low_health = df[df['Percent Health'] < threshold]
        
        for _, row in low_health.iterrows():
            corridor = row.get('Corridor', 'Unknown')
            health_score = row['Percent Health']
            
            # Generate recommendations based on individual metric scores
            recs = []
            
            # Check individual metric scores
            if 'Detection Uptime Score' in row and row['Detection Uptime Score'] < 8:
                recs.append("Improve detector maintenance and calibration")
            
            if 'Comm Uptime Score' in row and row['Comm Uptime Score'] < 8:
                recs.append("Address communication infrastructure issues")
            
            if 'Split Failures Score' in row and row['Split Failures Score'] < 8:
                recs.append("Optimize signal timing to reduce split failures")
            
            if 'Travel Time Index Score' in row and row['Travel Time Index Score'] < 8:
                recs.append("Review corridor signal coordination")
            
            if 'Ped Delay Score' in row and row['Ped Delay Score'] < 8:
                recs.append("Optimize pedestrian signal timing")
            
            if not recs:
                recs.append("Conduct comprehensive corridor assessment")
            
            recommendation = {
                'corridor': corridor,
                'zone_group': row.get('Zone Group', row.get('Zone_Group', 'Unknown')),
                'current_health': health_score,
                'target_health': threshold,
                'improvement_needed': threshold - health_score,
                'recommendations': recs,
                'priority': 'High' if health_score < threshold * 0.8 else 'Medium'
            }
            
            recommendations.append(recommendation)
        
        # Sort by improvement needed (descending)
        recommendations.sort(key=lambda x: x['improvement_needed'], reverse=True)
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error generating recommendations: {e}")
        return []


def create_health_heatmap_data(df: pd.DataFrame, 
                              row_col: str = 'Corridor',
                              time_col: str = 'Month') -> Dict[str, Any]:
    """
    Prepare data for health metrics heatmap visualization
    
    Args:
        df: DataFrame with health metrics
        row_col: Column to use for heatmap rows
        time_col: Column to use for heatmap columns
        
    Returns:
        Dictionary with heatmap data
    """
    
    try:
        if 'Percent Health' not in df.columns:
            return {}
        
        # Pivot data for heatmap
        heatmap_data = df.pivot_table(
            index=row_col,
            columns=time_col,
            values='Percent Health',
            aggfunc='mean'
        )
        
        # Fill missing values with NaN
        heatmap_data = heatmap_data.fillna(np.nan)
        
        # Create color mapping based on health scores
        def get_color_scale(value):
            if pd.isna(value):
                return '#CCCCCC'  # Gray for missing data
            elif value >= 90:
                return '#2E7D32'  # Dark green for excellent
            elif value >= 80:
                return '#66BB6A'  # Light green for good
            elif value >= 70:
                return '#FDD835'  # Yellow for fair
            elif value >= 60:
                return '#FB8C00'  # Orange for poor
            else:
                return '#E53935'  # Red for critical
        
        # Prepare data for visualization
        result = {
            'data': heatmap_data,
            'rows': heatmap_data.index.tolist(),
            'columns': heatmap_data.columns.tolist(),
            'values': heatmap_data.values.tolist(),
            'color_mapping': {
                'excellent': '#2E7D32',
                'good': '#66BB6A',
                'fair': '#FDD835',
                'poor': '#FB8C00',
                'critical': '#E53935',
                'missing': '#CCCCCC'
            }
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error creating heatmap data: {e}")
        return {}


def calculate_health_targets(df: pd.DataFrame, 
                           target_percentile: float = 75.0) -> Dict[str, float]:
    """
    Calculate target health scores based on current performance
    
    Args:
        df: DataFrame with health metrics
        target_percentile: Percentile to use for target setting
        
    Returns:
        Dictionary with target values
    """
    
    try:
        targets = {}
        
        if 'Percent Health' not in df.columns:
            return targets
        
        # Overall health target
        targets['overall_health'] = np.percentile(df['Percent Health'].dropna(), target_percentile)
        
        # Individual metric targets
        score_cols = [col for col in df.columns if 'Score' in col]
        
        for col in score_cols:
            metric_name = col.replace(' Score', '').lower().replace(' ', '_')
            targets[metric_name] = np.percentile(df[col].dropna(), target_percentile)
        
        return targets
        
    except Exception as e:
        logger.error(f"Error calculating health targets: {e}")
        return {}


def validate_health_metrics_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Comprehensive validation of health metrics data
    
    Args:
        df: DataFrame with health metrics
        
    Returns:
        Dictionary with validation results
    """
    
    try:
        validation = {
            'data_quality': {},
            'completeness': {},
            'consistency': {},
            'outliers': {},
            'recommendations': []
        }
        
        # Data quality checks
        validation['data_quality']['total_rows'] = len(df)
        validation['data_quality']['empty_rows'] = df.isnull().all(axis=1).sum()
        validation['data_quality']['duplicate_rows'] = df.duplicated().sum()
        
        # Completeness checks
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            validation['completeness'][col] = {
                'missing_count': missing_count,
                'missing_percentage': (missing_count / len(df)) * 100
            }
        
        # Consistency checks for health scores
        if 'Percent Health' in df.columns:
            health_scores = df['Percent Health'].dropna()
            
            # Check for valid range (0-100)
            invalid_range = ((health_scores < 0) | (health_scores > 100)).sum()
            validation['consistency']['invalid_health_range'] = invalid_range
            
            # Check for extreme values
            q1 = health_scores.quantile(0.25)
            q3 = health_scores.quantile(0.75)
            iqr = q3 - q1
            
            outlier_threshold_low = q1 - 1.5 * iqr
            outlier_threshold_high = q3 + 1.5 * iqr
            
            outliers = ((health_scores < outlier_threshold_low) | 
                       (health_scores > outlier_threshold_high)).sum()
            validation['outliers']['health_score_outliers'] = outliers
        
        # Generate recommendations based on validation results
        if validation['data_quality']['empty_rows'] > 0:
            validation['recommendations'].append(
                f"Remove {validation['data_quality']['empty_rows']} empty rows"
            )
        
        if validation['data_quality']['duplicate_rows'] > 0:
            validation['recommendations'].append(
                f"Remove {validation['data_quality']['duplicate_rows']} duplicate rows"
            )
        
        high_missing_cols = [
            col for col, info in validation['completeness'].items()
            if info['missing_percentage'] > 20
        ]
        
        if high_missing_cols:
            validation['recommendations'].append(
                f"Address high missing data in columns: {', '.join(high_missing_cols)}"
            )
        
        return validation
        
    except Exception as e:
        logger.error(f"Error validating health metrics data: {e}")
        return {'error': str(e)}


def create_health_metrics_api_response(health_data: Dict, 
                                     level: str, 
                                     metric_type: str,
                                     filters: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create API response format for health metrics data
    
    Args:
        health_data: Health metrics data dictionary
        level: Data level (cor, sub, sig)
        metric_type: Metric type (maint, ops, safety)
        filters: Optional filters to apply
        
    Returns:
        Dictionary in API response format
    """
    
    try:
        response = {
            'status': 'success',
            'data': {},
            'metadata': {},
            'filters_applied': filters or {}
        }
        
        if level not in health_data or 'mo' not in health_data[level]:
            response['status'] = 'error'
            response['message'] = f'No data available for level: {level}'
            return response
        
        if metric_type not in health_data[level]['mo']:
            response['status'] = 'error'
            response['message'] = f'No data available for metric type: {metric_type}'
            return response
        
        df = health_data[level]['mo'][metric_type].copy()
        
        # Apply filters if provided
        if filters:
            for filter_col, filter_val in filters.items():
                if filter_col in df.columns:
                    if isinstance(filter_val, list):
                        df = df[df[filter_col].isin(filter_val)]
                    else:
                        df = df[df[filter_col] == filter_val]
        
        # Prepare response data
        response['data'] = {
            'records': df.to_dict('records'),
            'summary': {
                'total_records': len(df),
                'mean_health': df['Percent Health'].mean() if 'Percent Health' in df.columns else None,
                'min_health': df['Percent Health'].min() if 'Percent Health' in df.columns else None,
                'max_health': df['Percent Health'].max() if 'Percent Health' in df.columns else None
            }
        }
        
        # Add metadata
        response['metadata'] = {
            'level': level,
            'metric_type': metric_type,
            'columns': df.columns.tolist(),
            'data_types': df.dtypes.astype(str).to_dict(),
            'generated_at': pd.Timestamp.now().isoformat()
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Error creating API response: {e}")
        return {
            'status': 'error',
            'message': str(e),
            'data': {},
            'metadata': {}
        }


# Export utility functions
__all__ = [
    'calculate_weighted_health_score',
    'interpolate_missing_scores',
    'calculate_health_percentiles',
    'rank_corridors_by_health',
    'calculate_health_variance',
    'detect_health_anomalies',
    'create_health_dashboard_data',
    'export_health_metrics_excel',
    'compare_health_periods',
    'calculate_health_correlations',
    'generate_health_improvement_recommendations',
    'create_health_heatmap_data',
    'calculate_health_targets',
    'validate_health_metrics_data',
    'create_health_metrics_api_response'
]
