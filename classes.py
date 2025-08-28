"""
Define classes and functions that operate on classes
Possibly make this file all about the metrics class and rename to metrics.py
"""

import yaml
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, date
import warnings
from typing import Dict, Any, Optional, Union, List
import html

# Load metrics configuration
with open("metrics.yaml", "r") as f:
    metrics = yaml.safe_load(f)

# Color constants (you'll need to define these based on your application)
GDOT_BLUE = "#1f77b4"
DARK_GRAY = "#555555"
LIGHT_GRAY_BAR = "#cccccc"
DARK_GRAY_BAR = "#666666"
LIGHT_RED = "#ff9999"
BLUE = "#0066cc"
LIGHT_BLUE = "#87ceeb"
BLACK = "#000000"
BROWN = "#8b4513"


def inherit(parent: Dict[str, Any], child: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Function to create a metric (child) by specifying only differences from another (parent) metric.
    Created for health metrics for which there are many, but nearly identical.
    """
    try:
        keys = set(list(parent.keys()) + list(child.keys()))
        result = {}
        for key in keys:
            parent_val = parent.get(key)
            child_val = child.get(key)
            if child_val is not None:
                result[key] = child_val
            elif parent_val is not None:
                result[key] = parent_val
        return result
    except Exception as e:
        print(f"Can't inherit child from parent: {e}")
        return None


class Metric:
    """Metric class to hold metric configuration and properties"""
    def __init__(self, config: Dict[str, Any]):
        for key, value in config.items():
            setattr(self, key, value)


# Create metric instances
vpd = Metric(metrics["daily_traffic_volume"])
am_peak_vph = Metric(metrics["am_peak_hour_volume"])
pm_peak_vph = Metric(metrics["pm_peak_hour_volume"])
throughput = Metric(metrics["throughput"])
aog = arrivals_on_green = Metric(metrics["arrivals_on_green"])
progression_ratio = Metric(metrics["progression_ratio"])
queue_spillback_rate = Metric(metrics["queue_spillback_rate"])
peak_period_split_failures = Metric(metrics["peak_period_split_failures"])
off_peak_split_failures = Metric(metrics["off_peak_split_failures"])
travel_time_index = Metric(metrics["travel_time_index"])
planning_time_index = Metric(metrics["planning_time_index"])
average_speed = Metric(metrics["average_speed"])
daily_pedestrian_pushbuttons = Metric(metrics["daily_pedestrian_pushbuttons"])

# Inherited metrics
detector_uptime = Metric(inherit(metrics["uptime"], metrics["detector_uptime"]))
ped_button_uptime = Metric(inherit(metrics["uptime"], metrics["ped_button_uptime"]))
cctv_uptime = Metric(inherit(metrics["uptime"], metrics["cctv_uptime"]))
comm_uptime = Metric(inherit(metrics["uptime"], metrics["comm_uptime"]))
rsu_uptime = Metric(inherit(metrics["uptime"], metrics["rsu_uptime"]))

# Task metrics
tasks = Metric(inherit(metrics["tasks_template"], metrics["tasks"]))
tasks_by_type = Metric(inherit(metrics["tasks_template"], metrics["tasks_by_type"]))
tasks_by_subtype = Metric(inherit(metrics["tasks_template"], metrics["tasks_by_subtype"]))
tasks_by_source = Metric(inherit(metrics["tasks_template"], metrics["tasks_by_source"]))
tasks_reported = Metric(inherit(metrics["tasks_template"], metrics["tasks_reported"]))
tasks_resolved = Metric(inherit(metrics["tasks_template"], metrics["tasks_resolved"]))
tasks_outstanding = Metric(inherit(metrics["tasks_template"], metrics["tasks_outstanding"]))
tasks_over45 = Metric(inherit(metrics["tasks_template"], metrics["tasks_over45"]))
tasks_mttr = Metric(inherit(metrics["tasks_template"], metrics["tasks_mttr"]))

# Maintenance health metrics
maint_percent_health = Metric(inherit(metrics["health_metrics"], metrics["maint_percent_health"]))
maint_missing_data = Metric(inherit(metrics["health_metrics"], metrics["maint_missing_data"]))
du_score = Metric(inherit(metrics["health_metrics"], metrics["du_score"]))
pau_score = Metric(inherit(metrics["health_metrics"], metrics["pau_score"]))
cu_score = Metric(inherit(metrics["health_metrics"], metrics["cu_score"]))
cctv_score = Metric(inherit(metrics["health_metrics"], metrics["cctv_score"]))
flash_score = Metric(inherit(metrics["health_metrics"], metrics["flash_score"]))
du_health = Metric(inherit(metrics["health_metrics"], metrics["du_health"]))
pau_health = Metric(inherit(metrics["health_metrics"], metrics["pau_health"]))
cu_health = Metric(inherit(metrics["health_metrics"], metrics["cu_health"]))
cctv_health = Metric(inherit(metrics["health_metrics"], metrics["cctv_health"]))
flash_health = Metric(inherit(metrics["health_metrics"], metrics["flash_health"]))

# Operations health metrics
ops_missing_data = Metric(inherit(metrics["health_metrics"], metrics["maint_missing_data"]))
ops_percent_health = Metric(inherit(metrics["health_metrics"], metrics["ops_percent_health"]))
pr_score = Metric(inherit(metrics["health_metrics"], metrics["pr_score"]))
pd_score = Metric(inherit(metrics["health_metrics"], metrics["pd_score"]))
sf_score = Metric(inherit(metrics["health_metrics"], metrics["sf_score"]))
tti_score = Metric(inherit(metrics["health_metrics"], metrics["tti_score"]))
bi_score = Metric(inherit(metrics["health_metrics"], metrics["bi_score"]))
pr_health = Metric(inherit(metrics["health_metrics"], metrics["pr_health"]))
pd_health = Metric(inherit(metrics["health_metrics"], metrics["pd_health"]))
sf_health = Metric(inherit(metrics["health_metrics"], metrics["sf_health"]))
tti_health = Metric(inherit(metrics["health_metrics"], metrics["tti_health"]))
bi_health = Metric(inherit(metrics["health_metrics"], metrics["bi_health"]))

# Safety health metrics
safety_percent_health = Metric(inherit(metrics["health_metrics"], metrics["safety_percent_health"]))
safety_missing_data = Metric(inherit(metrics["health_metrics"], metrics["safety_missing_data"]))
bpsi_score = Metric(inherit(metrics["health_metrics"], metrics["bpsi_score"]))
rsi_score = Metric(inherit(metrics["health_metrics"], metrics["rsi_score"]))
cri_score = Metric(inherit(metrics["health_metrics"], metrics["cri_score"]))
kabco_score = Metric(inherit(metrics["health_metrics"], metrics["kabco_score"]))
bpsi_health = Metric(inherit(metrics["health_metrics"], metrics["bpsi_health"]))
rsi_health = Metric(inherit(metrics["health_metrics"], metrics["rsi_health"]))
cri_health = Metric(inherit(metrics["health_metrics"], metrics["cri_health"]))
kabco_health = Metric(inherit(metrics["health_metrics"], metrics["kabco_health"]))


def data_format(data_type: str):
    """Return formatting function based on data type"""
    formatters = {
        'percent': lambda x: f"{x:.1%}" if pd.notna(x) else "NA",
        'integer': lambda x: f"{int(x):,}" if pd.notna(x) else "NA",
        'float': lambda x: f"{x:.2f}" if pd.notna(x) else "NA",
        'currency': lambda x: f"${x:,.2f}" if pd.notna(x) else "NA"
    }
    return formatters.get(data_type, lambda x: str(x) if pd.notna(x) else "NA")


def tick_format(data_type: str) -> str:
    """Return tick format string for plotly based on data type"""
    formats = {
        'percent': '.1%',
        'integer': ',',
        'float': '.2f',
        'currency': '$,.2f'
    }
    return formats.get(data_type, '')


def as_pct(value: float) -> str:
    """Format value as percentage"""
    if pd.isna(value):
        return "0%"
    return f"{value:.1%}"


def if_else(condition, true_val, false_val):
    """Python equivalent of R's if_else"""
    return true_val if condition else false_val


def glue(template: str, **kwargs) -> str:
    """Simple string formatting similar to R's glue"""
    return template.format(**kwargs)


def query_data(metric: Metric, level: str = None, resolution: str = "monthly", 
               zone_group: str = None, corridor: str = None, month: date = None, 
               quarter: str = None, hourly: bool = False, upto: bool = False) -> pd.DataFrame:
    """
    Query data function - this would need to be implemented based on your data source
    This is a placeholder that returns an empty DataFrame
    """
    # This function needs to be implemented based on your actual data source
    return pd.DataFrame()


def no_data_plot(message: str = "No Data Available"):
    """Create a plot showing no data message"""
    fig = go.Figure()
    fig.add_annotation(
        text=message,
        xref="paper", yref="paper",
        x=0.5, y=0.5,
        xanchor='center', yanchor='middle',
        showarrow=False,
        font=dict(size=16)
    )
    fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
    )
    return fig


def get_description_block(metric: Metric, zone_group: str, month: date, quarter: str = None) -> Dict[str, Any]:
    """
    Get description block data for a metric
    Note: This function references 'cor' which would need to be defined globally
    """
    if quarter is None:  # want monthly, not quarterly data
        # vals = cor.mo[metric.table].query(f"Corridor == '{zone_group}' & Month == '{month}'").to_dict('records')[0]
        vals = {}  # Placeholder
    else:
        # vals = cor.qu[metric.table].query(f"Corridor == '{zone_group}' & Quarter == '{quarter}'").to_dict('records')[0]
        vals = {}  # Placeholder
    
    delta = vals.get('delta', 0)
    if pd.isna(delta):
        delta = 0
    
    if delta > 0:
        delta_prefix = " +"
        color = "success"
        color_icon = "caret-up"
    elif delta == 0:
        delta_prefix = "  "
        color = "black"
        color_icon = None
    else:  # delta < 0
        delta_prefix = "  "
        color = "failure"
        color_icon = "caret-down"
    
    value = data_format(metric.data_type)(vals.get(metric.variable))
    delta_text = f'{delta_prefix} {as_pct(delta)}'
    
    if value == "NA":
        raise ValueError("NA")
    
    return {
        'number': delta_text,
        'numberColor': color,
        'numberIcon': color_icon,
        'header': value,
        'text': metric.label,
        'rightBorder': True,
        'marginBottom': False
    }


def get_minimal_trendline(metric: Metric, zone_group: str):
    """Create a minimal trendline plot"""
    # data = cor.wk[metric.table].query(f"Zone_Group == Corridor & Zone_Group == '{zone_group}'")
    data = pd.DataFrame()  # Placeholder
    
    fig = px.line(data, x='Date', y=metric.variable, color_discrete_sequence=[GDOT_BLUE])
    fig.update_layout(
        showlegend=False,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, title=''),
        margin=dict(l=0, r=0, t=0, b=0)
    )
    return fig


def get_valuebox_value(metric: Metric, zone_group: str, corridor: str, month: date, 
                      quarter: str = None, line_break: bool = False) -> str:
    """Get formatted value for valuebox display"""
    if corridor == "All Corridors":
        corridor = None
    
    if quarter is None:  # want monthly, not quarterly data
        vals = query_data(
            metric,
            level="corridor",
            resolution="monthly",
            zone_group=zone_group,
            corridor=corridor,
            month=month,
            upto=False
        )
    else:
        vals = query_data(
            metric,
            level="corridor", 
            resolution="quarterly",
            zone_group=zone_group,
            corridor=corridor,
            quarter=quarter,
            upto=False
        )
    
    if corridor is None:
        if zone_group in ["All RTOP", "RTOP1", "RTOP2", "Zone 7"]:
            vals = vals[vals['Zone_Group'] == zone_group]
        else:
            vals = vals[vals['Zone_Group'] == vals['Corridor']]
    
    if vals.empty:
        raise ValueError("NA")
    
    vals_dict = vals.iloc[0].to_dict()
    
    value = data_format(metric.data_type)(vals_dict.get(metric.variable))
    
    if value == "NA":
        raise ValueError("NA")
    
    delta = vals_dict.get('delta', 0)
    if pd.isna(delta):
        delta = 0
    
    if delta > 0:
        delta_prefix = "+"
    else:
        delta_prefix = " "
    
    delta_text = f"({delta_prefix}{as_pct(delta)})"
    
    if line_break:
        return f'<div>{value}<p style="font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;">{delta_text}</p></div>'
    else:
        return f'<div>{value}<span style="font-size: 70%;">{delta_text}</span></div>'


def summary_plot(metric: Metric, zone_group: str, corridor: str, month: date):
    """Create a summary plot for a metric"""
    if corridor == "All Corridors":
        corridor = None
    
    data = query_data(
        metric,
        level="corridor",
        resolution="monthly",
        zone_group=zone_group,
        corridor=corridor,
        month=month,
        upto=True
    )
    
    if corridor is None:
        if zone_group in ["All RTOP", "RTOP1", "RTOP2", "Zone 7"]:
            data = data[data['Zone_Group'] == zone_group]
        else:
            data = data[data['Zone_Group'] == data['Corridor']]
    
    if data.empty:
        raise ValueError("No Data")
    
    first = data.loc[data['Month'].idxmin()]
    last = data.loc[data['Month'].idxmax()]
    
    fig = go.Figure()
    
    if hasattr(metric, 'goal') and metric.goal is not None:
        fig.add_trace(go.Scatter(
            x=data['Month'],
            y=[metric.goal] * len(data),
            mode='lines',
            line=dict(color=DARK_GRAY, width=1),
            name="Goal"
        ))
        fig.add_trace(go.Scatter(
            x=data['Month'],
            y=data[metric.variable],
            mode='lines',
            line=dict(color=metric.highlight_color),
            fill='tonexty',
            fillcolor=metric.fill_color,
            name=metric.label
        ))
    else:
        fig.add_trace(go.Scatter(
            x=data['Month'],
            y=[first[metric.variable]] * len(data),
            mode='lines',
            line=dict(color=LIGHT_GRAY_BAR, width=1),
            name=None
        ))
        fig.add_trace(go.Scatter(
            x=data['Month'],
            y=data[metric.variable],
            mode='lines',
            line=dict(color=metric.highlight_color),
            name=metric.label
        ))
    
    # Add annotations
    fig.add_annotation(
        x=0, y=first[metric.variable],
        text=data_format(metric.data_type)(first[metric.variable]),
        showarrow=False,
        xanchor="right",
        xref="paper",
        borderwidth=5
    )
    fig.add_annotation(
        x=1, y=last[metric.variable],
        text=data_format(metric.data_type)(last[metric.variable]),
        font=dict(size=16),
        showarrow=False,
        xanchor="left",
        xref="paper",
        borderwidth=5
    )
    
    fig.update_layout(
        xaxis=dict(title="", showticklabels=True, showgrid=False),
        yaxis=dict(title="", showticklabels=False, showgrid=False, 
                  zeroline=False, tickformat=tick_format(metric.data_type)),
        showlegend=False,
        margin=dict(l=50, r=60, t=10, b=10)
    )
    
    return fig


def get_trend_multiplot(metric: Metric, level: str, zone_group: str, month: date, 
                       line_chart: str = "weekly", accent_average: bool = True):
    """Create a multi-panel trend plot"""
    if isinstance(month, str):
        month = pd.to_datetime(month).date()
    
    mdf = query_data(metric, level, zone_group=zone_group, month=month, upto=False)
    
    if (line_chart == "weekly" and not getattr(metric, 'has_weekly', True)) or (line_chart == "monthly"):
        wdf = query_data(metric, level, resolution="monthly", zone_group=zone_group, month=month, upto=True)
        wdf = wdf.rename(columns={'Month': 'Date'})
    else:
        wdf = query_data(metric, level, resolution=line_chart, zone_group=zone_group, month=month, upto=True)
    
    if not mdf.empty and not wdf.empty:
        # Current Month Data
        mdf = mdf[mdf['Month'] == month].sort_values(metric.variable)
        mdf['var'] = mdf[metric.variable]
        mdf['col'] = mdf['Corridor'].apply(
            lambda x: DARK_GRAY_BAR if accent_average and x == zone_group else LIGHT_GRAY_BAR
        )
        mdf['text_col'] = mdf['Corridor'].apply(
            lambda x: "white" if accent_average and x == zone_group else "black"
        )
        
        # Create bar chart
        bar_chart = go.Figure()
        bar_chart.add_trace(go.Bar(
            x=mdf['var'],
            y=mdf['Corridor'],
            orientation='h',
            marker_color=mdf['col'],
            text=[data_format(metric.data_type)(val) for val in mdf['var']],
            textposition="auto",
            textfont_color=mdf['text_col'],
            customdata=[f"<b>{desc}</b><br>{metric.label}: <b>{data_format(metric.data_type)(val)}</b>" 
                       for desc, val in zip(mdf['Description'], mdf['var'])],
            hovertemplate="%{customdata}",
            name=""
        ))
        
        if hasattr(metric, 'goal') and metric.goal is not None:
            bar_chart.add_shape(
                type="line",
                x0=metric.goal, x1=metric.goal,
                y0=-0.5, y1=len(mdf)-0.5,
                line=dict(color=LIGHT_RED, width=2)
            )
        
        bar_chart.update_layout(
            xaxis_title="Selected Month",
            xaxis_tickformat=tick_format(metric.data_type),
            yaxis_title="",
            showlegend=False,
            font_size=11,
            margin_l=100
        )
        
        # Weekly Data - historical trend
        wdf['var'] = wdf[metric.variable]
        wdf['col'] = wdf['Corridor'].apply(
            lambda x: 1 if accent_average and x == zone_group else 0
        )
        wdf = wdf.dropna(subset=['var']).groupby('Corridor')
        
        weekly_line_chart = go.Figure()
        colors = [LIGHT_GRAY_BAR, BLACK]
        
        for name, group in wdf:
            color_idx = group['col'].iloc[0]
            weekly_line_chart.add_trace(go.Scatter(
                x=group['Date'],
                y=group['var'],
                mode='lines',
                line_color=colors[color_idx],
                opacity=0.6,
                name=name,
                customdata=[f"<b>{desc}</b><br>Week of: <b>{date.strftime('%B %e, %Y')}</b><br>{metric.label}: <b>{data_format(metric.data_type)(val)}</b>"
                           for desc, date, val in zip(group['Description'], group['Date'], group['var'])],
                hovertemplate="%{customdata}"
            ))
        
        weekly_line_chart.update_layout(
            xaxis_title="Weekly Trend",
            yaxis_tickformat=tick_format(metric.data_type),
            title="__plot1_title__",
            showlegend=False,
            margin_t=50
        )
        
        # Create subplot
        if hasattr(metric, 'hourly_table') and metric.hourly_table is not None:
            hdf = query_data(
                metric, level, resolution="monthly", hourly=True,
                zone_group=zone_group, month=month, upto=False
            )
            hdf['var'] = hdf[metric.variable]
            hdf['col'] = hdf['Corridor'].apply(
                lambda x: 1 if accent_average and x == zone_group else 0
            )
            
            hourly_line_chart = go.Figure()
            for name, group in hdf.groupby('Corridor'):
                color_idx = group['col'].iloc[0]
                hourly_line_chart.add_trace(go.Scatter(
                    x=group['Hour'],
                    y=group['var'],
                    mode='lines',
                    line_color=colors[color_idx],
                    opacity=0.6,
                    name=name,
                    customdata=[f"<b>{desc}</b><br>Hour: <b>{hour.strftime('%l:%M %p')}</b><br>{metric.label}: <b>{data_format(metric.data_type)(val)}</b>"
                               for desc, hour, val in zip(group['Description'], group['Hour'], group['var'])],
                    hovertemplate="%{customdata}"
                ))
            
            hourly_line_chart.update_layout(
                xaxis_title=metric.label,
                yaxis_tickformat=tick_format(metric.data_type),
                title="__plot2_title__",
                showlegend=False
            )
            
            # Create 3-panel subplot
            fig = make_subplots(
                rows=1, cols=3,
                subplot_titles=["", "Weekly Trend", "Hourly Pattern"],
                column_widths=[0.2, 0.5, 0.3]
            )
            
            # Add traces to subplot
            for trace in bar_chart.data:
                fig.add_trace(trace, row=1, col=1)
            for trace in weekly_line_chart.data:
                fig.add_trace(trace, row=1, col=2)
            for trace in hourly_line_chart.data:
                fig.add_trace(trace, row=1, col=3)
        else:
            # Create 2-panel subplot
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=["", "Weekly Trend"],
                column_widths=[0.2, 0.8]
            )
            
            for trace in bar_chart.data:
                fig.add_trace(trace, row=1, col=1)
            for trace in weekly_line_chart.data:
                fig.add_trace(trace, row=1, col=2)
        
        fig.update_layout(
            title=metric.label,
            showlegend=False,
            margin_l=100
        )
        
        return fig
    else:
        return no_data_plot("")


def travel_times_plot(level: str, zone_group: str, month: date):
    """Create travel times plot combining TTI and PTI"""
    if isinstance(month, str):
        month = pd.to_datetime(month).date()
    
    format_func = data_format(travel_time_index.data_type)
    tick_fmt = tick_format(travel_time_index.data_type)
    
    cor_monthly_tti = query_data(
        travel_time_index, level=level, zone_group=zone_group, month=month, upto=True)
    cor_monthly_pti = query_data(
        planning_time_index, level=level, zone_group=zone_group, month=month, upto=True)
    
    cor_monthly_tti_by_hr = query_data(
        travel_time_index, level=level, zone_group=zone_group, month=month, hourly=True, upto=False)
    cor_monthly_pti_by_hr = query_data(
        planning_time_index, level=level, zone_group=zone_group, month=month, hourly=True, upto=False)
    
    if not cor_monthly_tti.empty and not cor_monthly_pti.empty:
        # Merge TTI and PTI data
        mott = pd.merge(cor_monthly_tti, cor_monthly_pti, 
                       on=['Corridor', 'Zone_Group', 'Month'], 
                       suffixes=('.tti', '.pti'))
        mott = mott.dropna(subset=['Corridor'])
        mott['bti'] = mott['pti'] - mott['tti']
        mott = mott[['Corridor', 'Zone_Group', 'Month', 'tti', 'pti', 'bti']]
        mott['col'] = mott['Corridor'].apply(lambda x: 1 if x == zone_group else 0)
        mott['text_col'] = mott['Corridor'].apply(lambda x: "white" if x == zone_group else "black")
        
        hrtt = pd.merge(cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
                       on=['Corridor', 'Zone_Group', 'Hour'],
                       suffixes=('.tti', '.pti'))
        hrtt = hrtt.dropna(subset=['Corridor'])
        hrtt['bti'] = hrtt['pti'] - hrtt['tti']
        hrtt = hrtt[['Corridor', 'Zone_Group', 'Hour', 'tti', 'pti', 'bti']]
        hrtt['col'] = hrtt['Corridor'].apply(lambda x: 1 if x == zone_group else 0)
        
        mo_max = round(mott['pti'].max(), 1) + 0.1
        hr_max = round(hrtt['pti'].max(), 1) + 0.1
        mo_min = round(mott['pti'].min(), 1) - 0.1
        hr_min = round(hrtt['pti'].min(), 1) - 0.1
        
        # Current month data
        mdf = mott[mott['Month'] == month].sort_values('tti')
        
        # Create stacked bar chart
        colors = [LIGHT_GRAY_BAR, BLACK]
        pbar = go.Figure()
        
        pbar.add_trace(go.Bar(
            x=mdf['tti'],
            y=mdf['Corridor'],
            orientation='h',
            marker_color=[colors[col] for col in mdf['col']],
            text=[format_func(val) for val in mdf['tti']],
            textposition="auto",
            name="TTI",
            customdata=[f"<b>{corr}</b><br>Travel Time Index: <b>{format_func(tti)}</b><br>Planning Time Index: <b>{format_func(pti)}</b>"
                       for corr, tti, pti in zip(mdf['Corridor'], mdf['tti'], mdf['pti'])],
            hovertemplate="%{customdata}"
        ))
        
        pbar.add_trace(go.Bar(
            x=mdf['bti'],
            y=mdf['Corridor'],
            orientation='h',
            marker_color=LIGHT_BLUE,
            text=[format_func(val) for val in mdf['pti']],
            textposition="auto",
            name="Buffer",
            hoverinfo='skip'
        ))
        
        pbar.update_layout(
            barmode='stack',
            xaxis=dict(
                title=f"{month.strftime('%b %Y')} TTI & PTI",
                zeroline=False,
                tickformat=tick_fmt,
                range=[0, 2]
            ),
            yaxis_title="",
            showlegend=False,
            font_size=11,
            margin_l=100
        )
        
        # Monthly trend plots
        pttimo = go.Figure()
        colors = [LIGHT_GRAY_BAR, BLACK]
        
        for name, group in mott.groupby('Corridor'):
            color_idx = group['col'].iloc[0]
            pttimo.add_trace(go.Scatter(
                x=group['Month'],
                y=group['tti'],
                mode='lines',
                line_color=colors[color_idx],
                opacity=0.6,
                name=name,
                customdata=[f"<b>{corr}</b><br><b>{month.strftime('%B %Y')}</b><br>Travel Time Index: <b>{format_func(tti)}</b>"
                           for corr, month, tti in zip(group['Corridor'], group['Month'], group['tti'])],
                hovertemplate="%{customdata}"
            ))
        
        pttimo.update_layout(
            xaxis_title=travel_time_index.label,
            yaxis=dict(range=[mo_min, mo_max], tickformat=tick_fmt),
            showlegend=False
        )
        
        # Hourly TTI plot
        pttihr = go.Figure()
        for name, group in hrtt.groupby('Corridor'):
            color_idx = group['col'].iloc[0]
            pttihr.add_trace(go.Scatter(
                x=group['Hour'],
                y=group['tti'],
                mode='lines',
                line_color=colors[color_idx],
                opacity=0.6,
                name=name,
                customdata=[f"<b>{corr}</b><br>Hour: <b>{hour.strftime('%l:%M %p')}</b><br>Travel Time Index: <b>{format_func(tti)}</b>"
                           for corr, hour, tti in zip(group['Corridor'], group['Hour'], group['tti'])],
                hovertemplate="%{customdata}"
            ))
        
        pttihr.update_layout(
            xaxis_title=f"{month.strftime('%b %Y')} TTI by Hr",
            yaxis=dict(range=[hr_min, hr_max], tickformat=tick_fmt),
            showlegend=False
        )
        
        # Monthly PTI plot
        pptimo = go.Figure()
        for name, group in mott.groupby('Corridor'):
            color_idx = group['col'].iloc[0]
            pptimo.add_trace(go.Scatter(
                x=group['Month'],
                y=group['pti'],
                mode='lines',
                line_color=colors[color_idx],
                opacity=0.6,
                name=name,
                customdata=[f"<b>{corr}</b><br><b>{month.strftime('%B %Y')}</b><br>Planning Time Index: <b>{format_func(pti)}</b>"
                           for corr, month, pti in zip(group['Corridor'], group['Month'], group['pti'])],
                hovertemplate="%{customdata}"
            ))
        
        pptimo.update_layout(
            xaxis_title=planning_time_index.label,
            yaxis=dict(range=[mo_min, mo_max], tickformat=tick_fmt),
            showlegend=False
        )
        
        # Hourly PTI plot
        pptihr = go.Figure()
        for name, group in hrtt.groupby('Corridor'):
            color_idx = group['col'].iloc[0]
            pptihr.add_trace(go.Scatter(
                x=group['Hour'],
                y=group['pti'],
                mode='lines',
                line_color=colors[color_idx],
                opacity=0.6,
                name=name,
                customdata=[f"<b>{corr}</b><br>Hour: <b>{hour.strftime('%l:%M %p')}</b><br>Planning Time Index: <b>{format_func(pti)}</b>"
                           for corr, hour, pti in zip(group['Corridor'], group['Hour'], group['pti'])],
                hovertemplate="%{customdata}"
            ))
        
        pptihr.update_layout(
            xaxis_title=f"{month.strftime('%b %Y')} PTI by Hr",
            yaxis=dict(range=[hr_min, hr_max], tickformat=tick_fmt),
            showlegend=False
        )
        
        # Create subplot with all panels
        fig = make_subplots(
            rows=2, cols=3,
            subplot_titles=["", "TTI Monthly Trend", "TTI Hourly", "", "PTI Monthly Trend", "PTI Hourly"],
            column_widths=[0.2, 0.4, 0.4],
            row_heights=[0.5, 0.5]
        )
        
        # Add bar chart (spans both rows)
        for trace in pbar.data:
            fig.add_trace(trace, row=1, col=1)
        
        # Add TTI plots
        for trace in pttimo.data:
            fig.add_trace(trace, row=1, col=2)
        for trace in pttihr.data:
            fig.add_trace(trace, row=1, col=3)
        
        # Add PTI plots
        for trace in pptimo.data:
            fig.add_trace(trace, row=2, col=2)
        for trace in pptihr.data:
            fig.add_trace(trace, row=2, col=3)
        
        fig.update_layout(
            title="Travel Time and Planning Time Index",
            showlegend=False,
            margin=dict(l=120, r=80)
        )
        
        return fig
    else:
        return no_data_plot("")


def uptime_multiplot(metric: Metric, level: str, zone_group: str, month: date):
    """Create uptime multi-panel plot"""
    if isinstance(month, str):
        month = pd.to_datetime(month).date()
    
    def uptime_line_plot(df: pd.DataFrame, corr: str, showlegend: bool = False):
        """Plot uptime for a corridor"""
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['Date'],
            y=df['uptime'],
            mode='lines',
            line_color=BLUE,
            name="Uptime",
            showlegend=showlegend
        ))
        fig.add_trace(go.Scatter(
            x=df['Date'],
            y=[0.95] * len(df),
            mode='lines',
            line_color=LIGHT_RED,
            name="Goal (95%)",
            showlegend=showlegend
        ))
        
        fig.update_layout(
            yaxis=dict(title="", range=[0, 1.1], tickformat=".0%"),
            xaxis_title="",
            annotations=[dict(
                text=corr,
                xref="paper", yref="paper",
                yanchor="bottom", xanchor="left",
                align="center",
                x=0.1, y=0.95,
                showarrow=False
            )]
        )
        return fig
    
    def uptime_bar_plot(df: pd.DataFrame, month: date):
        """Create uptime bar plot"""
        df = df.sort_values('uptime').reset_index(drop=True)
        df['col'] = df['Corridor'].apply(
            lambda x: DARK_GRAY_BAR if x == zone_group else LIGHT_GRAY_BAR
        )
        df['text_col'] = df['Corridor'].apply(
            lambda x: "white" if x == zone_group else "black"
        )
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df['uptime'],
            y=df['Corridor'],
            orientation='h',
            marker_color=df['col'],
            text=[as_pct(val) for val in df['uptime']],
            textposition="auto",
            textfont_color=df['text_col'],
            name="",
            customdata=[f"<b>{desc}</b><br>Uptime: <b>{as_pct(uptime)}</b>"
                       for desc, uptime in zip(df['Description'], df['uptime'])],
            hovertemplate="%{customdata}"
        ))
        
        # Add goal line
        fig.add_shape(
            type="line",
            x0=0.95, x1=0.95,
            y0=-0.5, y1=len(df)-0.5,
            line=dict(color=LIGHT_RED, width=2)
        )
        
        fig.update_layout(
            xaxis=dict(
                title=f"{month.strftime('%b %Y')} Uptime (%)",
                zeroline=False,
                tickformat=".0%"
            ),
            yaxis_title="",
            showlegend=False,
            font_size=11,
            margin_l=100
        )
        return fig
    
    avg_daily_uptime = query_data(
        metric, level=level, resolution="daily", zone_group=zone_group, month=month, upto=True
    )
    avg_monthly_uptime = query_data(
        metric, level=level, resolution="monthly", zone_group=zone_group, month=month, upto=False
    )
    
    if not avg_daily_uptime.empty:
        # Create bar plot
        p1 = uptime_bar_plot(avg_monthly_uptime, month)
        
        # Create line plots for each corridor
        corridor_dfs = {name: group for name, group in avg_daily_uptime.groupby('Corridor')}
        corridor_dfs = {k: v for k, v in corridor_dfs.items() if not v.empty}
        
        line_plots = []
        for i, (name, df) in enumerate(corridor_dfs.items()):
            showlegend = i == 0
            line_plots.append(uptime_line_plot(df, name, showlegend))
        
        # Create subplot
        if line_plots:
            num_rows = min(len(line_plots), 4)
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=["Monthly Uptime", "Daily Trend"],
                column_widths=[0.2, 0.8],
                specs=[[{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            # Add bar chart
            for trace in p1.data:
                fig.add_trace(trace, row=1, col=1)
            
            # Add line charts (combine all corridors in one plot)
            for name, df in corridor_dfs.items():
                fig.add_trace(go.Scatter(
                    x=df['Date'],
                    y=df['uptime'],
                    mode='lines',
                    name=name,
                    line_color=BLUE,
                    opacity=0.7
                ), row=1, col=2)
            
            # Add goal line to second subplot
            if corridor_dfs:
                sample_df = list(corridor_dfs.values())[0]
                fig.add_trace(go.Scatter(
                    x=sample_df['Date'],
                    y=[0.95] * len(sample_df),
                    mode='lines',
                    line_color=LIGHT_RED,
                    name="Goal (95%)"
                ), row=1, col=2)
            
            fig.update_layout(
                title=f"{metric.label} - {zone_group}",
                showlegend=True,
                margin_l=100
            )
            
            return fig
        else:
            return p1
    else:
        return no_data_plot("")


def individual_cctvs_plot(zone_group: str, month: date):
    """Create individual CCTVs heatmap plot"""
    daily_cctv_df = query_data(
        cctv_uptime, level="signal", resolution="daily", zone_group=zone_group, month=month
    )
    
    if daily_cctv_df.empty:
        return no_data_plot("No CCTV Data Available")
    
    # Prepare data similar to R's spread function
    spr = daily_cctv_df[daily_cctv_df['Corridor'] != zone_group].copy()
    spr = spr.rename(columns={'Corridor': 'CameraID', 'Zone_Group': 'Corridor'})
    spr = spr[['CameraID', 'Description', 'Date', 'up']].drop_duplicates()
    
    # Pivot data to create matrix
    pivot_df = spr.pivot_table(
        index=['CameraID', 'Description'], 
        columns='Date', 
        values='up', 
        fill_value=0
    )
    
    # Create matrix
    m = pivot_df.values.round(0).astype(int)
    row_names = [f"{idx[0]}" for idx in pivot_df.index]
    col_names = [col.strftime('%Y-%m-%d') for col in pivot_df.columns]
    descriptions = [f"{idx[1]}" for idx in pivot_df.index]
    
    def status_text(x):
        options = ["Camera Down", "Working at encoder, but not 511", "Working on 511"]
        return options[int(x)]
    
    # Create custom data for hover
    customdata = []
    for i, desc in enumerate(descriptions):
        row_customdata = []
        for j, date in enumerate(col_names):
            status = status_text(m[i, j])
            text = f"<b>{desc}</b><br>{status}"
            row_customdata.append(text)
        customdata.append(row_customdata)
    
    fig = go.Figure(data=go.Heatmap(
        z=m,
        x=col_names,
        y=row_names,
        colorscale=[[0, LIGHT_GRAY_BAR], [0.5, "#e48c5b"], [1, BROWN]],
        showscale=False,
        customdata=customdata,
        hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>"
    ))
    
    fig.update_layout(
        yaxis=dict(title="", type='category'),
        xaxis=dict(title="Date"),
        title=f"CCTV Status - {zone_group}",
        margin_l=150
    )
    
    return fig


# Empty plot - space filler (equivalent to p0 in R)
def empty_plot():
    """Create an empty plot for spacing"""
    fig = go.Figure()
    fig.update_layout(
        xaxis=dict(zeroline=False, showticklabels=False, showgrid=False, ticks=""),
        yaxis=dict(zeroline=False, showticklabels=False, showgrid=False, ticks=""),
        showlegend=False,
        margin=dict(l=0, r=0, t=0, b=0)
    )
    return fig


# Global empty plot instance
p0 = empty_plot()


def validate_data(data, message="No data available"):
    """Validation function similar to Shiny's validate/need"""
    if data is None or (hasattr(data, 'empty') and data.empty) or (hasattr(data, '__len__') and len(data) == 0):
        raise ValueError(message)
    return True


def filter_data(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Filter dataframe based on keyword arguments"""
    result = df.copy()
    for key, value in kwargs.items():
        if key in result.columns and value is not None:
            if isinstance(value, list):
                result = result[result[key].isin(value)]
            else:
                result = result[result[key] == value]
    return result


def get_color_palette():
    """Return the color palette used in the application"""
    return {
        'primary': GDOT_BLUE,
        'secondary': DARK_GRAY,
        'success': '#28a745',
        'danger': LIGHT_RED,
        'warning': '#ffc107',
        'info': LIGHT_BLUE,
        'light': LIGHT_GRAY_BAR,
        'dark': BLACK,
        'muted': '#6c757d'
    }


class DataProcessor:
    """Class to handle data processing operations"""
    
    def __init__(self):
        self.cache = {}
    
    def get_cached_data(self, key: str):
        """Get cached data by key"""
        return self.cache.get(key)
    
    def set_cached_data(self, key: str, data):
        """Set cached data by key"""
        self.cache[key] = data
    
    def clear_cache(self):
        """Clear all cached data"""
        self.cache.clear()
    
    def process_metric_data(self, metric: Metric, data: pd.DataFrame) -> pd.DataFrame:
        """Process metric data with standard operations"""
        if data.empty:
            return data
        
        # Add calculated fields common to all metrics
        if hasattr(metric, 'variable') and metric.variable in data.columns:
            data['formatted_value'] = data[metric.variable].apply(
                data_format(metric.data_type)
            )
        
        # Add color coding based on goals if available
        if hasattr(metric, 'goal') and metric.goal is not None:
            data['meets_goal'] = data[metric.variable] >= metric.goal
            data['goal_color'] = data['meets_goal'].apply(
                lambda x: 'success' if x else 'danger'
            )
        
        return data


class MetricCalculator:
    """Class to handle metric calculations and aggregations"""
    
    @staticmethod
    def calculate_percent_change(current: float, previous: float) -> float:
        """Calculate percentage change between two values"""
        if pd.isna(current) or pd.isna(previous) or previous == 0:
            return 0.0
        return (current - previous) / previous
    
    @staticmethod
    def calculate_moving_average(data: pd.Series, window: int = 7) -> pd.Series:
        """Calculate moving average"""
        return data.rolling(window=window, min_periods=1).mean()
    
    @staticmethod
    def calculate_trend(data: pd.Series) -> str:
        """Calculate trend direction (up, down, stable)"""
        if len(data) < 2:
            return 'stable'
        
        recent = data.tail(3).mean() if len(data) >= 3 else data.iloc[-1]
        earlier = data.head(3).mean() if len(data) >= 3 else data.iloc[0]
        
        change = (recent - earlier) / earlier if earlier != 0 else 0
        
        if change > 0.05:  # 5% threshold
            return 'up'
        elif change < -0.05:
            return 'down'
        else:
            return 'stable'
    
    @staticmethod
    def calculate_health_score(metrics_dict: Dict[str, float], weights: Dict[str, float] = None) -> float:
        """Calculate overall health score from multiple metrics"""
        if not metrics_dict:
            return 0.0
        
        if weights is None:
            weights = {k: 1.0 for k in metrics_dict.keys()}
        
        total_weight = sum(weights.get(k, 1.0) for k in metrics_dict.keys())
        weighted_sum = sum(v * weights.get(k, 1.0) for k, v in metrics_dict.items() if pd.notna(v))
        
        return weighted_sum / total_weight if total_weight > 0 else 0.0


class PlotStyler:
    """Class to handle consistent plot styling"""
    
    @staticmethod
    def apply_standard_layout(fig: go.Figure, title: str = "", **kwargs) -> go.Figure:
        """Apply standard layout styling to a plotly figure"""
        default_layout = {
            'font': {'family': 'Source Sans Pro', 'size': 12},
            'paper_bgcolor': 'white',
            'plot_bgcolor': 'white',
            'margin': {'l': 60, 'r': 60, 't': 60, 'b': 60},
            'title': {'text': title, 'x': 0.5, 'xanchor': 'center'},
            'hovermode': 'closest'
        }
        
        # Update with any custom layout options
        default_layout.update(kwargs)
        fig.update_layout(**default_layout)
        
        return fig
    
    @staticmethod
    def style_axis(axis_dict: Dict[str, Any], data_type: str = None) -> Dict[str, Any]:
        """Style axis with consistent formatting"""
        styled_axis = {
            'showgrid': True,
            'gridcolor': '#f0f0f0',
            'linecolor': '#d0d0d0',
            'tickcolor': '#d0d0d0'
        }
        
        if data_type:
            styled_axis['tickformat'] = tick_format(data_type)
        
        styled_axis.update(axis_dict)
        return styled_axis


def create_metric_summary_table(metrics: List[Metric], data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Create a summary table of multiple metrics"""
    summary_rows = []
    
    for metric in metrics:
        if hasattr(metric, 'label') and metric.label in data_dict:
            data = data_dict[metric.label]
            if not data.empty and hasattr(metric, 'variable') and metric.variable in data.columns:
                current_value = data[metric.variable].iloc[-1] if len(data) > 0 else None
                previous_value = data[metric.variable].iloc[-2] if len(data) > 1 else None
                
                change = MetricCalculator.calculate_percent_change(current_value, previous_value)
                trend = MetricCalculator.calculate_trend(data[metric.variable])
                
                summary_rows.append({
                    'Metric': metric.label,
                    'Current Value': data_format(getattr(metric, 'data_type', 'float'))(current_value),
                    'Change': as_pct(change),
                    'Trend': trend,
                    'Goal Met': 'Yes' if hasattr(metric, 'goal') and metric.goal and current_value >= metric.goal else 'No'
                })
    
    return pd.DataFrame(summary_rows)


def export_data_to_csv(data: pd.DataFrame, filename: str, **kwargs):
    """Export dataframe to CSV with standard formatting"""
    data.to_csv(filename, index=False, **kwargs)


def export_plot_to_html(fig: go.Figure, filename: str, **kwargs):
    """Export plotly figure to HTML"""
    fig.write_html(filename, **kwargs)


def create_dashboard_config() -> Dict[str, Any]:
    """Create configuration dictionary for dashboard settings"""
    return {
        'theme': 'light',
        'color_palette': get_color_palette(),
        'default_date_range': 30,  # days
        'refresh_interval': 300,  # seconds
        'cache_timeout': 3600,  # seconds
        'max_data_points': 1000,
        'enable_export': True,
        'enable_filtering': True
    }


# Utility functions for specific metric types
def format_uptime_percentage(value: float) -> str:
    """Format uptime as percentage with appropriate precision"""
    if pd.isna(value):
        return "N/A"
    return f"{value:.1%}"


def format_traffic_volume(value: float) -> str:
    """Format traffic volume with appropriate units"""
    if pd.isna(value):
        return "N/A"
    if value >= 1000000:
        return f"{value/1000000:.1f}M"
    elif value >= 1000:
        return f"{value/1000:.1f}K"
    else:
        return f"{int(value)}"


def format_speed(value: float) -> str:
    """Format speed with mph unit"""
    if pd.isna(value):
        return "N/A"
    return f"{value:.1f} mph"


def format_time_index(value: float) -> str:
    """Format time index values"""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}"


# Exception classes for better error handling
class MetricDataError(Exception):
    """Exception raised for errors in metric data processing"""
    pass


class PlotGenerationError(Exception):
    """Exception raised for errors in plot generation"""
    pass


class DataValidationError(Exception):
    """Exception raised for data validation errors"""
    pass


# Main class to tie everything together
class MetricsManager:
    """Main class to manage all metrics and their operations"""
    
    def __init__(self):
        self.processor = DataProcessor()
        self.calculator = MetricCalculator()
        self.styler = PlotStyler()
        self.config = create_dashboard_config()
        self.metrics = self._load_all_metrics()
    
    def _load_all_metrics(self) -> Dict[str, Metric]:
        """Load all metrics into a dictionary"""
        return {
            'vpd': vpd,
            'am_peak_vph': am_peak_vph,
            'pm_peak_vph': pm_peak_vph,
            'throughput': throughput,
            'arrivals_on_green': arrivals_on_green,
            'progression_ratio': progression_ratio,
            'queue_spillback_rate': queue_spillback_rate,
            'peak_period_split_failures': peak_period_split_failures,
            'off_peak_split_failures': off_peak_split_failures,
            'travel_time_index': travel_time_index,
            'planning_time_index': planning_time_index,
            'average_speed': average_speed,
            'daily_pedestrian_pushbuttons': daily_pedestrian_pushbuttons,
            'detector_uptime': detector_uptime,
            'ped_button_uptime': ped_button_uptime,
            'cctv_uptime': cctv_uptime,
            'comm_uptime': comm_uptime,
            'rsu_uptime': rsu_uptime,
            'tasks': tasks,
            'tasks_by_type': tasks_by_type,
            'tasks_by_subtype': tasks_by_subtype,
            'tasks_by_source': tasks_by_source,
            'tasks_reported': tasks_reported,
            'tasks_resolved': tasks_resolved,
            'tasks_outstanding': tasks_outstanding,
            'tasks_over45': tasks_over45,
            'tasks_mttr': tasks_mttr
        }
    
    def get_metric(self, metric_name: str) -> Metric:
        """Get a metric by name"""
        return self.metrics.get(metric_name)
    
    def get_all_metric_names(self) -> List[str]:
        """Get list of all available metric names"""
        return list(self.metrics.keys())
    
    def generate_metric_plot(self, metric_name: str, plot_type: str, **kwargs):
        """Generate a plot for a specific metric"""
        metric = self.get_metric(metric_name)
        if not metric:
            raise MetricDataError(f"Metric '{metric_name}' not found")
        
        plot_functions = {
            'summary': summary_plot,
            'trend': get_trend_multiplot,
            'uptime': uptime_multiplot,
            'travel_times': travel_times_plot,
            'cctv': individual_cctvs_plot
        }
        
        plot_func = plot_functions.get(plot_type)
        if not plot_func:
            raise PlotGenerationError(f"Plot type '{plot_type}' not supported")
        
        try:
            return plot_func(metric, **kwargs)
        except Exception as e:
            raise PlotGenerationError(f"Error generating {plot_type} plot: {str(e)}")


# Initialize the metrics manager as a global instance
metrics_manager = MetricsManager()


# Export key functions and classes for external use
__all__ = [
    'Metric', 'MetricsManager', 'metrics_manager',
    'query_data', 'data_format', 'tick_format', 'as_pct',
    'summary_plot', 'get_trend_multiplot', 'travel_times_plot',
    'uptime_multiplot', 'individual_cctvs_plot',
    'get_valuebox_value', 'get_description_block',
    'inherit', 'validate_data', 'no_data_plot'
]



