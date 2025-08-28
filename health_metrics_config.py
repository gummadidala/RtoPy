"""
Configuration and setup for health metrics
"""

# Example health_metrics.yaml structure
HEALTH_CONFIG_TEMPLATE = {
    'scoring_lookup': {
        'metric': ['detection', 'ped_actuation', 'comm', 'cctv', 'flash_events', 
                  'pr', 'ped_delay', 'sf', 'tti', 'bi', 'crash_rate', 'kabco', 
                  'high_speed', 'ped_injury_exposure'],
        'value': [0.95, 0.95, 0.95, 0.95, 0, 
                 0.5, 30, 0.1, 1.2, 0.3, 5.0, 2.0, 
                 85, 0.1],
        'score': [10, 10, 10, 10, 10,
                 10, 10, 10, 10, 10, 10, 10,
                 10, 10]
    },
    'weights_lookup': {
        'Context': [1, 2, 3, 4, 5],
        'Context_Category': ['Urban Arterial', 'Suburban Arterial', 'Rural Highway', 'Interstate', 'Local Road'],
        'Detection_Uptime_Weight': [20, 20, 15, 10, 25],
        'Ped_Act_Uptime_Weight': [15, 10, 5, 0, 20],
        'Comm_Uptime_Weight': [15, 15, 20, 25, 10],
        'CCTV_Uptime_Weight': [10, 15, 10, 15, 5],
        'Flash_Events_Weight': [10, 10, 15, 20, 10],
        'Platoon_Ratio_Weight': [20, 20, 15, 15, 15],
        'Ped_Delay_Weight': [15, 10, 5, 0, 20],
        'Split_Failures_Weight': [15, 20, 20, 25, 15],
        'TTI_Weight': [25, 25, 30, 35, 10],
        'BI_Weight': [15, 15, 20, 25, 5],
        'Crash_Rate_Index_Weight': [25, 25, 30, 30, 20],
        'KABCO_Crash_Severity_Index_Weight': [20, 20, 25, 25, 15],
        'High_Speed_Index_Weight': [30, 25, 35, 40, 10],
        'Ped_Injury_Exposure_Index_Weight': [25, 30, 10, 5, 35]
    }
}

# Metric type mappings
METRIC_TYPE_MAPPING = {
    'maintenance': ['Detection_Uptime', 'Ped_Act_Uptime', 'Comm_Uptime', 'CCTV_Uptime', 'Flash_Events'],
    'operations': ['Platoon_Ratio', 'Ped_Delay', 'Split_Failures', 'TTI', 'BI'],
    'safety': ['Crash_Rate_Index', 'KABCO_Crash_Severity_Index', 'High_Speed_Index', 'Ped_Injury_Exposure_Index']
}

# Default alert thresholds
DEFAULT_THRESHOLDS = {
    'maint': {
        'critical': 60.0,
        'warning': 75.0,
        'good': 85.0
    },
    'ops': {
        'critical': 50.0,
        'warning': 70.0,
        'good': 80.0
    },
    'safety': {
        'critical': 70.0,
        'warning': 80.0,
        'good': 90.0
    }
}

# Data validation rules
VALIDATION_RULES = {
    'percent_health': {'min': 0, 'max': 100},
    'missing_data': {'min': 0, 'max': 1},
    'uptime_metrics': {'min': 0, 'max': 1},
    'score_metrics': {'min': 0, 'max': 10}
}
