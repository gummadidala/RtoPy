"""
Monthly Report UI Functions - Python conversion from R
Provides dashboard functionality for traffic signal performance reporting
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import boto3
import yaml
import logging
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
import concurrent.futures
import psutil
import warnings
from typing import Dict, List, Optional, Any, Tuple, Union
import re
from pathlib import Path
import time
import functools

# Import our converted modules
from utilities import (
    TimingContext, batch_process, safe_divide, format_duration,
    resample_timeseries, calculate_change_metrics
)
from aggregations import (
    get_daily_avg, get_weekly_avg_by_day, get_monthly_avg_by_day,
    get_vph, get_tuesdays
)
from config import get_date_from_string
from database_functions import get_athena_connection_pool, get_aurora_connection_pool

# Suppress warnings
warnings.filterwarnings('ignore')

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Colors:
    """Color definitions matching R colorbrewer palette"""
    LIGHT_BLUE = "#A6CEE3"
    BLUE = "#1F78B4"
    LIGHT_GREEN = "#B2DF8A"
    GREEN = "#33A02C"
    LIGHT_RED = "#FB9A99"
    RED = "#E31A1C"
    LIGHT_ORANGE = "#FDBF6F"
    ORANGE = "#FF7F00"
    LIGHT_PURPLE = "#CAB2D6"
    PURPLE = "#6A3D9A"
    LIGHT_BROWN = "#FFFF99"
    BROWN = "#B15928"
    
    DARK_GRAY = "#636363"
    BLACK = "#000000"
    DARK_GRAY_BAR = "#252525"
    LIGHT_GRAY_BAR = "#bdbdbd"
    
    RED2 = "#e41a1c"
    GDOT_BLUE = "#045594"
    GDOT_BLUE_RGB = "#2d6797"
    GDOT_GREEN = "#13784B"
    GDOT_YELLOW = "#EEB211"
    GDOT_YELLOW_RGB = "rgba(238, 178, 17, 0.80)"
    SIGOPS_BLUE = "#00458F"
    SIGOPS_GREEN = "#007338"

# Color mappings
COLORS_MAP = {
    "1": Colors.LIGHT_BLUE, "2": Colors.BLUE,
    "3": Colors.LIGHT_GREEN, "4": Colors.GREEN,
    "5": Colors.LIGHT_RED, "6": Colors.RED,
    "7": Colors.LIGHT_ORANGE, "8": Colors.ORANGE,
    "9": Colors.LIGHT_PURPLE, "10": Colors.PURPLE,
    "11": Colors.LIGHT_BROWN, "12": Colors.BROWN,
    "0": Colors.DARK_GRAY, "NA": Colors.DARK_GRAY,
    "Mainline": Colors.BLUE, "Passage": Colors.GREEN,
    "Demand": Colors.RED, "Queue": Colors.ORANGE,
    "Other": Colors.DARK_GRAY
}

# Zone definitions
RTOP1_ZONES = ["Zone 1", "Zone 2", "Zone 3", "Zone 8"]
RTOP2_ZONES = ["Zone 4", "Zone 5", "Zone 6", "Zone 7m", "Zone 7d"]

class MetricDefinitions:
    """Metric definitions and formatting"""
    
    METRIC_ORDER = ["du", "pau", "cctvu", "cu", "tp", "aog", "qs", "sf", "tti", "pti", "tasks"]
    
    METRIC_NAMES = [
        "Vehicle Detector Availability",
        "Ped Pushbutton Availability", 
        "CCTV Availability",
        "Communications Uptime",
        "Traffic Volume (Throughput)",
        "Arrivals on Green",
        "Queue Spillback Rate",
        "Split Failure",
        "Travel Time Index",
        "Planning Time Index",
        "Outstanding Tasks"
    ]
    
    METRIC_GOALS = [
        "> 95%", "> 95%", "> 95%", "> 95%",
        "< 5% dec. from prev. mo",
        "> 80%", "< 10%", "< 5%",
        "< 2.0", "< 2.0",
        "Dec. from prev. mo"
    ]
    
    METRIC_GOALS_NUMERIC = [0.95, 0.95, 0.95, 0.95, -0.05, 0.8, 0.10, 0.05, 2, 2, 0]
    METRIC_GOALS_SIGN = [">", ">", ">", ">", ">", ">", "<", "<", "<", "<", "<"]

class Formatters:
    """Data formatting functions"""
    
    @staticmethod
    def as_int(x: Union[float, int, None]) -> str:
        """Format as integer with commas"""
        if pd.isna(x):
            return "N/A"
        return f"{int(x):,}"
    
    @staticmethod
    def as_2dec(x: Union[float, None]) -> str:
        """Format as 2 decimal places"""
        if pd.isna(x):
            return "N/A"
        return f"{x:.2f}"
    
    @staticmethod
    def as_pct(x: Union[float, None]) -> str:
        """Format as percentage"""
        if pd.isna(x):
            return "N/A"
        return f"{x * 100:.1f}%"
    
    @staticmethod
    def as_currency(x: Union[float, None]) -> str:
        """Format as currency"""
        if pd.isna(x):
            return "N/A"
        return f"${x:,.2f}"
    
    @staticmethod
    def data_format(data_type: str):
        """Get formatter function by data type"""
        formatters = {
            "integer": Formatters.as_int,
            "decimal": Formatters.as_2dec,
            "percent": Formatters.as_pct,
            "currency": Formatters.as_currency
        }
        return formatters.get(data_type, str)
    
    @staticmethod
    def tick_format(data_type: str) -> str:
        """Get tick format string for plotly"""
        formats = {
            "integer": ",.0f",
            "decimal": ".2f", 
            "percent": ".0%",
            "currency": "$,.2f"
        }
        return formats.get(data_type, "")

class ConfigManager:
    """Configuration management"""
    
    def __init__(self, config_path: str = "Monthly_Report.yaml", 
                 aws_config_path: str = "Monthly_Report_AWS.yaml"):
        self.config = self._load_config(config_path)
        self.aws_config = self._load_config(aws_config_path)
        self._setup_aws_credentials()
    
    def _load_config(self, path: str) -> Dict:
        """Load YAML configuration"""
        try:
            with open(path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {path}")
            return {}
    
    def _setup_aws_credentials(self):
        """Setup AWS credentials from config"""
        if self.aws_config:
            import os
            os.environ['AWS_ACCESS_KEY_ID'] = self.aws_config.get('AWS_ACCESS_KEY_ID', '')
            os.environ['AWS_SECRET_ACCESS_KEY'] = self.aws_config.get('AWS_SECRET_ACCESS_KEY', '')
            os.environ['AWS_DEFAULT_REGION'] = self.aws_config.get('AWS_DEFAULT_REGION', 'us-east-1')

class DateHelper:
    """Date utility functions"""
    
    @staticmethod
    def get_last_month() -> date:
        """Get first day of last month"""
        today = date.today()
        first_this_month = today.replace(day=1)
        return first_this_month - relativedelta(months=1)
    
    @staticmethod
    def get_report_months(num_months: int = 12) -> List[date]:
        """Get list of report months"""
        last_month = DateHelper.get_last_month()
        return [last_month - relativedelta(months=i) for i in range(num_months)]
    
    @staticmethod
    def format_month_options(months: List[date]) -> List[str]:
        """Format months for dropdown options"""
        return [month.strftime("%B %Y") for month in months]

class DataFilter:
    """Data filtering utilities"""
    
    @staticmethod
    def filter_mr_data(df: pd.DataFrame, zone_group: str) -> pd.DataFrame:
        """Filter monthly report data by zone group"""
        if df.empty:
            return df
        
        df_filtered = df.copy()
        
        if zone_group == "All RTOP":
            df_filtered = df_filtered[df_filtered['Zone_Group'].isin(['RTOP1', 'RTOP2'])]
        elif zone_group == "Zone 7":
            df_filtered = df_filtered[df_filtered['Zone'].isin(['Zone 7m', 'Zone 7d'])]
        elif zone_group.startswith("Zone"):
            df_filtered = df_filtered[df_filtered['Zone'] == zone_group]
        else:
            df_filtered = df_filtered[df_filtered['Zone_Group'] == zone_group]
        
        return df_filtered

class PlotGenerator:
    """Plot generation utilities"""
    
    @staticmethod
    def create_value_box_content(cor_monthly_df: pd.DataFrame, 
                                var: str, 
                                var_fmt: callable,
                                zone: str, 
                                month: date,
                                break_line: bool = False) -> str:
        """Create value box content with value and delta"""
        try:
            row = cor_monthly_df[
                (cor_monthly_df['Corridor'] == zone) & 
                (cor_monthly_df['Month'] == month)
            ]
            
            if row.empty:
                return "N/A"
            
            vals = row.iloc[0]
            delta = vals.get('delta', 0)
            if pd.isna(delta):
                delta = 0
            
            val = var_fmt(vals[var])
            delta_str = f" ({'+' if delta > 0 else ' ('}{Formatters.as_pct(delta)})"
            
            if break_line:
                return f"{val}<br><small>{delta_str}</small>"
            else:
                return f"{val} <small>{delta_str}</small>"
                
        except Exception as e:
            logger.error(f"Error creating value box: {e}")
            return "N/A"
    
    @staticmethod
    def create_performance_plot(data: pd.DataFrame,
                               value_col: str,
                               name: str,
                               color: str,
                               fill_color: str = None,
                               format_func: callable = None,
                               goal: float = None,
                               title: str = "") -> go.Figure:
        """Create performance line plot with optional goal line and fill"""
        if format_func is None:
            format_func = lambda x: f"{x:.2f}"
        
        fig = go.Figure()
        
        # Add goal line if provided
        if goal is not None:
            fig.add_trace(go.Scatter(
                x=data['Month'],
                y=[goal] * len(data),
                mode='lines',
                line=dict(color=Colors.DARK_GRAY, dash='dot'),
                name='Goal',
                showlegend=False
            ))
        
        # Add main data line
        fig.add_trace(go.Scatter(
            x=data['Month'],
            y=data[value_col],
            mode='lines+markers',
            line=dict(color=color),
            marker=dict(size=8, color=color),
            name=name,
            fill='tonexty' if goal is not None and fill_color else None,
            fillcolor=fill_color if fill_color else None,
            customdata=[format_func(val) for val in data[value_col]],
            hovertemplate='%{customdata}<extra></extra>'
        ))
        
        # Add annotations for first and last values
        if not data.empty:
            first_val = data.iloc[0]
            last_val = data.iloc[-1]
            
            fig.add_annotation(
                x=0, y=first_val[value_col],
                text=format_func(first_val[value_col]),
                showarrow=False,
                xanchor="right",
                xref="paper"
            )
            
            fig.add_annotation(
                x=1, y=last_val[value_col],
                text=format_func(last_val[value_col]),
                showarrow=False,
                xanchor="left", 
                xref="paper",
                font=dict(size=16)
            )
        
        fig.update_layout(
            title=title,
            showlegend=False,
            margin=dict(l=50, r=60, t=10, b=10),
            xaxis=dict(showticklabels=True, showgrid=False),
            yaxis=dict(showticklabels=False, showgrid=False, zeroline=False)
        )
        
        return fig
    
    @staticmethod
    def create_no_data_plot(name: str = "") -> go.Figure:
        """Create placeholder plot for no data"""
        fig = go.Figure()
        
        fig.add_annotation(
            x=0.5, y=0.5,
            text="NO DATA",
            showarrow=False,
            xref="paper",
            yref="paper",
            font=dict(size=16)
        )
        
        if name:
            fig.add_annotation(
                x=-0.02, y=0.4,
                text=name,
                showarrow=False,
                xref="paper", 
                yref="paper",
                xanchor="right",
                font=dict(size=12)
            )
        
        fig.update_layout(
            showlegend=False,
            margin=dict(l=180, r=100),
            xaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
            yaxis=dict(showticklabels=False, showgrid=False, zeroline=False)
        )
        
        return fig

class BarLineDashboardPlot:
    """Complex bar and line dashboard plot generator"""
    
    @staticmethod
    def create_plot(cor_weekly: pd.DataFrame,
                    cor_monthly: pd.DataFrame,
                    var: str,
                    num_format: str,
                    highlight_color: str,
                    month: date,
                    zone_group: str,
                    x_bar_title: str = "___",
                    x_line1_title: str = "___", 
                    x_line2_title: str = "___",
                    plot_title: str = "___",
                    cor_hourly: pd.DataFrame = None,
                    goal: float = None,
                    accent_average: bool = True) -> go.Figure:
        """Create complex bar and line dashboard plot"""
        
        # Get formatter based on num_format
        if num_format == "percent":
            var_fmt = Formatters.as_pct
            tickformat = ".0%"
        elif num_format == "integer":
            var_fmt = Formatters.as_int
            tickformat = ",.0f"
        elif num_format == "decimal":
            var_fmt = Formatters.as_2dec
            tickformat = ".2f"
        else:
            var_fmt = str
            tickformat = ""
        
        # Filter data
        mdf = DataFilter.filter_mr_data(cor_monthly, zone_group)
        wdf = DataFilter.filter_mr_data(cor_weekly, zone_group)
        
        mdf = mdf[mdf['Month'] == month]
        wdf = wdf[wdf['Date'] < month + relativedelta(months=1)]
        
        if mdf.empty or wdf.empty:
            return PlotGenerator.create_no_data_plot()
        
        # Prepare monthly data for bar chart
        mdf = mdf.sort_values(var).copy()
        mdf['var'] = mdf[var]
        
        if accent_average:
            mdf['color'] = mdf['Corridor'].apply(
                lambda x: Colors.DARK_GRAY_BAR if x == zone_group else Colors.LIGHT_GRAY_BAR
            )
        else:
            mdf['color'] = Colors.LIGHT_GRAY_BAR
        
        mdf['Corridor'] = pd.Categorical(mdf['Corridor'], categories=mdf['Corridor'], ordered=True)
        
        # Prepare weekly data
        wdf = wdf.copy()
        wdf['var'] = wdf[var]
        
        if accent_average:
            wdf['color'] = wdf['Corridor'].apply(
                lambda x: Colors.BLACK if x == zone_group else Colors.LIGHT_GRAY_BAR
            )
        else:
            wdf['color'] = Colors.LIGHT_GRAY_BAR
        
        # Create subplots
        if cor_hourly is not None:
            hdf = DataFilter.filter_mr_data(cor_hourly, zone_group)
            hdf = hdf[pd.to_datetime(hdf['Hour']).dt.date == month]
            
            if not hdf.empty:
                hdf = hdf.copy()
                hdf['var'] = hdf[var]
                hdf['color'] = hdf['Corridor'].apply(
                    lambda x: Colors.BLACK if x == zone_group else Colors.LIGHT_GRAY_BAR
                )
                
                fig = make_subplots(
                    rows=3, cols=2,
                    specs=[[{"rowspan": 3}, {}],
                           [None, {}], 
                           [None, {}]],
                    subplot_titles=['', x_line1_title, '', x_line2_title],
                    horizontal_spacing=0.03,
                    vertical_spacing=0.1,
                    column_widths=[0.2, 0.8],
                    row_heights=[0.6, 0.1, 0.3]
                )
                
                # Add hourly subplot
                for corridor in hdf['Corridor'].unique():
                    corridor_data = hdf[hdf['Corridor'] == corridor]
                    color = Colors.BLACK if corridor == zone_group else Colors.LIGHT_GRAY_BAR
                    
                    fig.add_trace(
                        go.Scatter(
                            x=corridor_data['Hour'],
                            y=corridor_data['var'],
                            mode='lines',
                            line=dict(color=color),
                            name=corridor,
                            showlegend=False,
                            customdata=[f"<b>{corridor}</b><br>Hour: <b>{h.strftime('%I:%M %p')}</b><br>{plot_title}: <b>{var_fmt(v)}</b>"
                                      for h, v in zip(pd.to_datetime(corridor_data['Hour']), corridor_data['var'])],
                            hovertemplate='%{customdata}<extra></extra>'
                        ),
                        row=3, col=2
                    )
            else:
                fig = make_subplots(
                    rows=1, cols=2,
                    specs=[[{}, {}]],
                    horizontal_spacing=0.03,
                    column_widths=[0.2, 0.8]
                )
        else:
            fig = make_subplots(
                rows=1, cols=2,
                specs=[[{}, {}]],
                horizontal_spacing=0.03,
                column_widths=[0.2, 0.8]
            )
        
        # Add bar chart
        fig.add_trace(
            go.Bar(
                x=mdf['var'],
                y=mdf['Corridor'],
                orientation='h',
                marker=dict(color=mdf['color']),
                text=[var_fmt(v) for v in mdf['var']],
                textposition='auto',
                textfont=dict(color='black'),
                name="",
                customdata=[f"<b>{desc}</b><br>{plot_title}: <b>{var_fmt(v)}</b>"
                           for desc, v in zip(mdf['Description'], mdf['var'])],
                hovertemplate='%{customdata}<extra></extra>',
                showlegend=False
            ),
            row=1, col=1
        )
        
        # Add goal line to bar chart if provided
        if goal is not None:
            fig.add_shape(
                type="line",
                x0=goal, x1=goal,
                y0=-0.5, y1=len(mdf)-0.5,
                line=dict(color=Colors.LIGHT_RED, width=2),
                row=1, col=1
            )
        
        # Add weekly lines
        row_idx = 1 if cor_hourly is None else 2
        for corridor in wdf['Corridor'].unique():
            corridor_data = wdf[wdf['Corridor'] == corridor]
            color = Colors.BLACK if corridor == zone_group else Colors.LIGHT_GRAY_BAR
            
            fig.add_trace(
                go.Scatter(
                    x=corridor_data['Date'],
                    y=corridor_data['var'],
                    mode='lines',
                    line=dict(color=color),
                    name=corridor,
                    showlegend=False,
                    customdata=[f"<b>{corridor}</b><br>Week of: <b>{d.strftime('%B %e, %Y')}</b><br>{plot_title}: <b>{var_fmt(v)}</b>"
                               for d, v in zip(corridor_data['Date'], corridor_data['var'])],
                    hovertemplate='%{customdata}<extra></extra>'
                ),
                row=row_idx, col=2
            )
        
        # Update layout
        fig.update_xaxes(title_text=x_bar_title, tickformat=tickformat, row=1, col=1)
        fig.update_yaxes(title_text="", row=1, col=1)
        fig.update_xaxes(title_text=x_line1_title, row=row_idx, col=2)
        fig.update_yaxes(tickformat=tickformat, row=row_idx, col=2)
        
        if cor_hourly is not None and not hdf.empty:
            fig.update_xaxes(title_text=x_line2_title, row=3, col=2)
            fig.update_yaxes(tickformat=tickformat, row=3, col=2)
        
        fig.update_layout(
            title=plot_title,
            margin=dict(l=100),
            showlegend=False
        )
        
        return fig

class TravelTimePlot:
    """Travel time index plotting utilities"""
    
    @staticmethod
    def create_tt_plot(cor_monthly_tti: pd.DataFrame,
                       cor_monthly_tti_by_hr: pd.DataFrame,
                       cor_monthly_pti: pd.DataFrame,
                       cor_monthly_pti_by_hr: pd.DataFrame,
                       highlight_color: str = Colors.RED2,
                       month: date = None,
                       zone_group: str = None,
                       x_bar_title: str = "___",
                       x_line1_title: str = "___",
                       x_line2_title: str = "___",
                       plot_title: str = "___") -> go.Figure:
        """Create travel time index plot with TTI and PTI"""
        
        var_fmt = Formatters.as_2dec
        tickformat = ".2f"
        
        # Merge TTI and PTI data
        mott = pd.merge(
            cor_monthly_tti, cor_monthly_pti,
            on=['Corridor', 'Zone_Group', 'Month'],
            suffixes=('.tti', '.pti')
        ).dropna(subset=['Corridor'])
        
        mott['bti'] = mott['pti'] - mott['tti']
        mott = mott[mott['Month'] < month + relativedelta(months=1)]
        mott = mott[['Corridor', 'Zone_Group', 'Month', 'tti', 'pti', 'bti']]
        
        hrtt = pd.merge(
            cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
            on=['Corridor', 'Zone_Group', 'Hour'],
            suffixes=('.tti', '.pti')
        ).dropna(subset=['Corridor'])
        
        hrtt['bti'] = hrtt['pti'] - hrtt['tti']
        hrtt = hrtt[['Corridor', 'Zone_Group', 'Hour', 'tti', 'pti', 'bti']]
        
        # Filter data
        mott = DataFilter.filter_mr_data(mott, zone_group)
        hrtt = DataFilter.filter_mr_data(hrtt, zone_group)
        
        if mott.empty or hrtt.empty:
            return PlotGenerator.create_no_data_plot()
        
        mo_max = round(mott['pti'].max(), 1) + 0.1
        hr_max = round(hrtt['pti'].max(), 1) + 0.1
        
        # Filter for current month
        mott_current = mott[mott['Month'] == month]
        hrtt_current = hrtt[pd.to_datetime(hrtt['Hour']).dt.date == month]
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=3,
            specs=[[{"rowspan": 2}, {}, {}],
                   [None, {}, {}]],
            subplot_titles=['', 'Travel Time Index (TTI)', 'Planning Time Index (PTI)'],
            horizontal_spacing=0.03,
            vertical_spacing=0.1,
            column_widths=[0.2, 0.4, 0.4],
            row_heights=[0.6, 0.4]
        )
        
        # Prepare bar chart data
        bar_data = mott_current.sort_values('tti').copy()
        
        # Add stacked bar chart for TTI and BTI
        fig.add_trace(
            go.Bar(
                x=bar_data['tti'],
                y=bar_data['Corridor'],
                orientation='h',
                marker=dict(color=Colors.DARK_GRAY),
                text=[var_fmt(v) for v in bar_data['tti']],
                textposition='auto',
                name="TTI",
                customdata=[f"<b>{c}</b><br>Travel Time Index: <b>{var_fmt(tti)}</b><br>Planning Time Index: <b>{var_fmt(pti)}</b>"
                           for c, tti, pti in zip(bar_data['Corridor'], bar_data['tti'], bar_data['pti'])],
                hovertemplate='%{customdata}<extra></extra>',
                showlegend=False
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                x=bar_data['bti'],
                y=bar_data['Corridor'],
                orientation='h',
                marker=dict(color=Colors.LIGHT_BLUE),
                text=[var_fmt(v) for v in bar_data['pti']],
                textposition='auto',
                name="BTI",
                showlegend=False,
                hoverinfo='skip'
            ),
            row=1, col=1
        )
        
        # Add hourly TTI trend lines
        for corridor in hrtt_current['Corridor'].unique():
            corridor_data = hrtt_current[hrtt_current['Corridor'] == corridor]
            color = Colors.BLACK if corridor == zone_group else Colors.LIGHT_GRAY_BAR
            
            # TTI trace
            fig.add_trace(
                go.Scatter(
                    x=corridor_data['Hour'],
                    y=corridor_data['tti'],
                    mode='lines',
                    line=dict(color=color),
                    name=corridor,
                    showlegend=False,
                    customdata=[f"<b>{corridor}</b><br>Hour: <b>{pd.to_datetime(h).strftime('%I:%M %p')}</b><br>Travel Time Index: <b>{Formatters.as_2dec(tti)}</b>"
                               for h, tti in zip(corridor_data['Hour'], corridor_data['tti'])],
                    hovertemplate='%{customdata}<extra></extra>'
                ),
                row=2, col=2
            )
            
            # PTI trace
            fig.add_trace(
                go.Scatter(
                    x=corridor_data['Hour'],
                    y=corridor_data['pti'],
                    mode='lines',
                    line=dict(color=color),
                    name=corridor,
                    showlegend=False,
                    customdata=[f"<b>{corridor}</b><br>Hour: <b>{pd.to_datetime(h).strftime('%I:%M %p')}</b><br>Planning Time Index: <b>{Formatters.as_2dec(pti)}</b>"
                               for h, pti in zip(corridor_data['Hour'], corridor_data['pti'])],
                    hovertemplate='%{customdata}<extra></extra>'
                ),
                row=2, col=3
            )
        
        # Update layout
        fig.update_layout(
            title="Travel Time and Planning Time Index",
            barmode='stack',
            margin=dict(l=120, r=80),
            showlegend=False
        )
        
        # Update axes
        fig.update_xaxes(title_text=x_bar_title, tickformat=tickformat, range=[0, 2], row=1, col=1)
        fig.update_yaxes(title_text="", row=1, col=1)
        
        fig.update_yaxes(range=[1, mo_max], tickformat=tickformat, row=1, col=2)
        fig.update_yaxes(range=[1, mo_max], tickformat=tickformat, row=1, col=3)
        fig.update_yaxes(range=[1, hr_max], tickformat=tickformat, row=2, col=2)
        fig.update_yaxes(range=[1, hr_max], tickformat=tickformat, row=2, col=3)
        
        fig.update_xaxes(title_text=x_line1_title, row=2, col=2)
        fig.update_xaxes(title_text=x_line1_title, row=2, col=3)
        
        return fig

class RCompatibleFormatters:
    """Formatters that match R's behavior exactly"""
    
    @staticmethod
    def format_like_r_percent(x: float) -> str:
        """Format percentage exactly like R"""
        if pd.isna(x):
            return "NA"  # R uses "NA" not "N/A"
        return f"{x * 100:.1f}%"
    
    @staticmethod
    def format_like_r_number(x: float, digits: int = 2) -> str:
        """Format numbers exactly like R"""
        if pd.isna(x):
            return "NA"
        return f"{x:.{digits}f}"

class DataProcessor:
    """Data processing utilities matching R functionality"""
    
    @staticmethod
    def validate_and_process_data(df: pd.DataFrame, 
                                 required_cols: List[str]) -> pd.DataFrame:
        """Validate and process data like R version"""
        if df.empty:
            logger.warning("Empty DataFrame provided")
            return pd.DataFrame()
        
        # Check for required columns
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return pd.DataFrame()
        
        # Handle R's NA values (different from Python's NaN)
        df = df.replace({'NA': np.nan, 'NULL': np.nan})
        
        return df

class UptimePlots:
    """Uptime plotting utilities"""
    
    @staticmethod
    def create_detector_uptime_plot(avg_daily_uptime: pd.DataFrame,
                                   avg_monthly_uptime: pd.DataFrame,
                                   month: date,
                                   zone_group: str,
                                   month_name: str) -> go.Figure:
        """Create detector uptime plot with bar chart and time series"""
        
        # Filter data
        avg_daily_uptime = DataFilter.filter_mr_data(avg_daily_uptime, zone_group)
        avg_monthly_uptime = DataFilter.filter_mr_data(avg_monthly_uptime, zone_group)
        
        avg_daily_uptime = avg_daily_uptime[avg_daily_uptime['Date'] <= month + relativedelta(months=1)]
        avg_monthly_uptime = avg_monthly_uptime[avg_monthly_uptime['Month'] == month]
        
        if avg_daily_uptime.empty or avg_monthly_uptime.empty:
            return PlotGenerator.create_no_data_plot("Detector Uptime")
        
        # Complete the implementation
        # Sort monthly data like R would
        monthly_data = avg_monthly_uptime.sort_values('uptime', ascending=True)
        
        # Create the plot structure
        fig = make_subplots(
            rows=1, cols=2,
            specs=[[{}, {}]],
            subplot_titles=['Monthly Uptime', 'Daily Trend'],
            horizontal_spacing=0.1,
            column_widths=[0.3, 0.7]
        )
        
        # Add bar chart (matching R's geom_col)
        fig.add_trace(
            go.Bar(
                x=monthly_data['uptime'],
                y=monthly_data['Corridor'],
                orientation='h',
                marker=dict(color=[Colors.DARK_GRAY_BAR if c == zone_group else Colors.LIGHT_GRAY_BAR 
                                 for c in monthly_data['Corridor']]),
                text=[Formatters.as_pct(v) for v in monthly_data['uptime']],
                textposition='auto',
                name="Monthly Uptime",
                showlegend=False
            ),
            row=1, col=1
        )
        
        # Add goal line (R's geom_vline equivalent)
        fig.add_vline(x=0.95, line_dash="dash", line_color="red", row=1, col=1)
        
        # Add daily trends (R's geom_line equivalent)
        for corridor in avg_daily_uptime['Corridor'].unique():
            corridor_data = avg_daily_uptime[avg_daily_uptime['Corridor'] == corridor]
            color = Colors.BLACK if corridor == zone_group else Colors.LIGHT_GRAY_BAR
            
            fig.add_trace(
                go.Scatter(
                    x=corridor_data['Date'],
                    y=corridor_data['uptime'],
                    mode='lines',
                    line=dict(color=color),
                    name=corridor,
                    showlegend=False
                ),
                row=1, col=2
            )
        
        # Update layout to match R's theme
        fig.update_layout(
            title=f"{month_name} Detector Uptime",
            margin=dict(l=150, r=50, t=80, b=50),
            plot_bgcolor='white',
            paper_bgcolor='white'
        )
        
        # Format axes like R
        fig.update_xaxes(title_text="Uptime (%)", tickformat=".0%", row=1, col=1)
        fig.update_xaxes(title_text="Date", row=1, col=2)
        fig.update_yaxes(title_text="", row=1, col=1)
        fig.update_yaxes(title_text="Uptime (%)", tickformat=".0%", row=1, col=2)
        
        return fig

class AlertsPlots:
    """Watchdog alerts plotting utilities"""
    
    @staticmethod
    def filter_alerts_by_date(alerts: pd.DataFrame, date_range: Tuple[date, date]) -> pd.DataFrame:
        """Filter alerts by date range"""
        start_date, end_date = date_range
        return alerts[(alerts['Date'] >= start_date) & (alerts['Date'] <= end_date)]
    
    @staticmethod
    def filter_alerts(alerts_by_date: pd.DataFrame,
                     alert_type: str,
                     zone_group: str,
                     corridor: str,
                     phase: str,
                     id_filter: str,
                     active_streak: str) -> Dict[str, Any]:
        """Filter alerts based on multiple criteria"""
        
        # Get most recent date for each alert type
        most_recent_date = alerts_by_date.groupby('Alert')['Date'].max().to_dict()
        
        df = alerts_by_date[alerts_by_date['Alert'] == alert_type].copy()
        
        if df.empty:
            return {"plot": pd.DataFrame(), "table": pd.DataFrame(), "intersections": 0}
        
        # Filter by zone/corridor
        if corridor == "All Corridors":
            if zone_group == "All RTOP":
                df = df[df['Zone_Group'].isin(['RTOP1', 'RTOP2'])]
            elif zone_group == "Zone 7":
                df = df[df['Zone'].isin(['Zone 7m', 'Zone 7d'])]
            elif zone_group.startswith("Zone"):
                df = df[df['Zone'] == zone_group]
            else:
                df = df[df['Zone_Group'] == zone_group]
        else:
            df = df[df['Corridor'] == corridor]
        
        # Filter by ID pattern
        if id_filter:
            pattern = re.compile(id_filter, re.IGNORECASE)
            df = df[
                df['Name'].str.contains(pattern, na=False) |
                df['Corridor'].str.contains(pattern, na=False) |
                df['SignalID'].astype(str).str.contains(pattern, na=False)
            ]
        
        # Filter by phase
        if alert_type != "Missing Records" and phase != "All":
            df = df[df['CallPhase'] == int(phase)]
        
        if df.empty:
            return {"plot": pd.DataFrame(), "table": pd.DataFrame(), "intersections": 0}
        
        # Calculate streaks and create table
        df['Streak'] = df.apply(
            lambda row: row['streak'] if row['Date'] == most_recent_date.get(alert_type, None) else 0,
            axis=1
        )
        
        table_df = df.groupby([
            'Zone', 'Corridor', 'SignalID', 'CallPhase', 'Detector', 'ApproachDesc', 'Name', 'Alert'
        ]).agg({
            'Streak': 'max',
            'Date': 'count'
        }).rename(columns={'Date': 'Occurrences'}).reset_index()
        
        table_df = table_df.sort_values(['Streak', 'Occurrences'], ascending=[False, False])
        
        # Filter by active streak
        if active_streak == "Active":
            table_df = table_df[table_df['Streak'] > 0]
            df = df.merge(table_df[['SignalID', 'Detector']], on=['SignalID', 'Detector'])
        elif active_streak == "Active 3-days":
            table_df = table_df[table_df['Streak'] > 2]
            df = df.merge(table_df[['SignalID', 'Detector']], on=['SignalID', 'Detector'])
        
        # Prepare plot data based on alert type
        if alert_type == "Missing Records":
            df['signal_phase'] = df['SignalID'].astype(str) + ": " + df['Name']
            table_df = table_df.drop(['CallPhase', 'Detector'], axis=1)
        elif alert_type in ["Bad Vehicle Detection", "Bad Ped Pushbuttons"]:
            df['signal_phase'] = (df['SignalID'].astype(str) + ": " + df['Name'] + 
                                 " | det " + df['Detector'].astype(str))
        elif alert_type == "No Camera Image":
            df['signal_phase'] = df['SignalID'].astype(str) + ": " + df['Name']
            table_df = table_df.drop(['CallPhase', 'Detector'], axis=1)
        else:
            df['signal_phase'] = (df['SignalID'].astype(str) + ": " + df['Name'] + 
                                 " | ph " + df['CallPhase'].astype(str))
            if alert_type != "Count":
                table_df = table_df.drop(['Detector'], axis=1)
        
        # Handle ramp meters special case
        if not df.empty and df['Zone_Group'].iloc[0] == "Ramp Meters":
            df['signal_phase'] = df['signal_phase'].str.replace(
                " | ", f" | {df['ApproachDesc']} | ", regex=False
            )
        
        # Sort and categorize signal_phase
        df = df.sort_values(['SignalID', 'Name', 'CallPhase'])
        df['signal_phase'] = pd.Categorical(
            df['signal_phase'], 
            categories=df['signal_phase'].unique(), 
            ordered=True
        )
        
        intersections = len(df['signal_phase'].unique())
        
        return {
            "plot": df,
            "table": table_df,
            "intersections": intersections
        }
    
    @staticmethod
    def plot_alerts(df: pd.DataFrame, date_range: Tuple[date, date]) -> go.Figure:
        """Create alerts heatmap plot"""
        
        if df.empty:
            return PlotGenerator.create_no_data_plot()
        
        start_date = max(df['Date'].min(), date_range[0])
        end_date = max(df['Date'].max(), date_range[1])
        
        # Determine color scheme based on zone group
        if not df.empty and df['Zone_Group'].iloc[0] == "Ramp Meters":
            # Use phase colors for ramp meters
            color_col = 'CallPhase'
            colorscale = 'Viridis'
        else:
            # Use streak colors for other alerts
            color_col = 'streak'
            colorscale = [[0, "#fc8d59"], [1, "#7f0000"]]
        
        # Create date range for x-axis
        date_range_full = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Pivot data for heatmap
        pivot_data = df.pivot_table(
            index='signal_phase',
            columns='Date',
            values=color_col,
            fill_value=0
        )
        
        # Reindex to include all dates
        pivot_data = pivot_data.reindex(columns=date_range_full, fill_value=0)
        
        # Reverse order for display
        pivot_data = pivot_data.iloc[::-1]
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot_data.values,
            x=[d.strftime('%Y-%m-%d') for d in pivot_data.columns],
            y=pivot_data.index,
            colorscale=colorscale,
            showscale=False,
            customdata=[[f"Signal: {idx}<br>Date: {col}<br>Value: {val}"
                        for col, val in zip(pivot_data.columns, row)]
                       for idx, row in zip(pivot_data.index, pivot_data.values)],
            hovertemplate='%{customdata}<extra></extra>'
        ))
        
        # Add white grid lines
        for i in range(len(date_range_full)):
            fig.add_vline(x=i-0.5, line_width=1, line_color="white")
        
        if len(pivot_data.index) > 1:
            for i in range(1, len(pivot_data.index)):
                fig.add_hline(y=i-0.5, line_width=1, line_color="white")
        
        fig.update_layout(
            xaxis=dict(side='top'),
            yaxis=dict(title=""),
            margin=dict(l=200),
            showlegend=False
        )
        
        return fig

class TEAMSPlots:
    """TEAMS tasks plotting utilities"""
    
    @staticmethod
    def gather_outstanding_events(cor_monthly_events: pd.DataFrame) -> pd.DataFrame:
        """Gather outstanding events data for plotting"""
        return cor_monthly_events.melt(
            id_vars=['Month', 'Corridor', 'Zone_Group'],
            value_vars=['Reported', 'Resolved', 'Outstanding'],
            var_name='Events',
            value_name='Status'
        )
    
    @staticmethod
    def plot_teams_tasks(tab: pd.DataFrame,
                        var: str,
                        title: str = "",
                        textpos: str = "auto",
                        height: int = 300) -> go.Figure:
        """Create TEAMS tasks plot"""
        
        # Sort data by reported values
        tab_sorted = tab.sort_values('Reported')
        tab_sorted[var] = pd.Categorical(
            tab_sorted[var], 
            categories=tab_sorted[var], 
            ordered=True
        )
        
        # Create subplots
        fig = make_subplots(
            rows=1, cols=3,
            subplot_titles=['Reported', 'Resolved', 'Cum. Outstanding'],
            shared_yaxes=True
        )
        
        # Reported tasks
        fig.add_trace(
            go.Bar(
                x=tab_sorted['Reported'],
                y=tab_sorted[var],
                orientation='h',
                marker=dict(color=Colors.LIGHT_BLUE),
                text=tab_sorted['Reported'],
                textposition=textpos,
                textfont=dict(size=11, color='black'),
                name="Reported",
                showlegend=False
            ),
            row=1, col=1
        )
        
        # Resolved tasks
        fig.add_trace(
            go.Bar(
                x=tab_sorted['Resolved'],
                y=tab_sorted[var],
                orientation='h',
                marker=dict(color=Colors.BLUE),
                text=tab_sorted['Resolved'],
                textposition=textpos,
                textfont=dict(size=11, color='white'),
                name="Resolved",
                showlegend=False
            ),
            row=1, col=2
        )
        
        # Outstanding tasks
        fig.add_trace(
            go.Bar(
                x=tab_sorted['Outstanding'],
                y=tab_sorted[var],
                orientation='h',
                marker=dict(color=Colors.SIGOPS_GREEN),
                text=tab_sorted['Outstanding'],
                textposition=textpos,
                textfont=dict(size=11, color='white'),
                name="Outstanding",
                showlegend=False
            ),
            row=1, col=3
        )
        
        fig.update_layout(
            title=dict(text=title, font=dict(size=12)),
            height=height,
            margin=dict(l=180),
            showlegend=False
        )
        
        # Update axes
        for col in range(1, 4):
            fig.update_xaxes(title_text=["Reported", "Resolved", "Cum. Outstanding"][col-1], 
                           zeroline=False, row=1, col=col)
        fig.update_yaxes(title_text="", row=1, col=1)
        
        return fig
    
    @staticmethod
    def cum_events_plot(df: pd.DataFrame, height: int = 300) -> go.Figure:
        """Create cumulative events plot"""
        
        fig = go.Figure()
        
        # Reported events
        reported_data = df[df['Events'] == 'Reported']
        fig.add_trace(
            go.Bar(
                x=reported_data['Month'],
                y=reported_data['Status'],
                name='Reported',
                marker=dict(color=Colors.LIGHT_BLUE)
            )
        )
        
        # Resolved events
        resolved_data = df[df['Events'] == 'Resolved']
        fig.add_trace(
            go.Bar(
                x=resolved_data['Month'],
                y=resolved_data['Status'],
                name='Resolved',
                marker=dict(color=Colors.BLUE)
            )
        )
        
        # Outstanding events line
        outstanding_data = df[df['Events'] == 'Outstanding']
        fig.add_trace(
            go.Scatter(
                x=outstanding_data['Month'],
                y=outstanding_data['Status'],
                mode='lines+markers',
                name='Outstanding',
                line=dict(color=Colors.SIGOPS_GREEN),
                marker=dict(color=Colors.SIGOPS_GREEN)
            )
        )
        
        fig.update_layout(
            barmode='group',
            height=height,
            yaxis=dict(title='Events'),
            xaxis=dict(title=''),
            legend=dict(x=0.5, y=0.9, orientation='h')
        )
        
        return fig

class VolumeAnalysis:
    """Volume analysis and plotting"""
    
    @staticmethod
    def read_signal_data(conn, signal_id: int, start_date: date, end_date: date) -> pd.DataFrame:
        """Read signal data from database"""
        query = f"""
        SELECT signalid, date, dat.hour, dat.detector, dat.callphase,
               dat.vol_rc, dat.vol_ac, dat.bad_day
        FROM (
            SELECT signalid, date, dat 
            FROM signal_details, unnest(data) t(dat)
            WHERE signalid = {signal_id}
            AND date >= '{start_date}' AND date <= '{end_date}'
        ) 
        ORDER BY dat.detector, date, dat.hour
        """
        
        try:
            df = pd.read_sql(query, conn)
            if not df.empty:
                df['Timeperiod'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'], unit='h')
                df['SignalID'] = df['signalid'].astype('category')
                df['Detector'] = df['detector'].astype('category')
                df['CallPhase'] = df['callphase'].fillna(0).astype('category')
                df['bad_day'] = df['bad_day'].astype(bool)
            return df
        except Exception as e:
            logger.error(f"Error reading signal data: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def create_volume_plot(dbpool, signal_id: int, start_date: date, end_date: date,
                          title: str = "Volume Plot", ymax: int = None) -> go.Figure:
        """Create volume plot for signal"""
        
        try:
            # Get data
            df = VolumeAnalysis.read_signal_data(dbpool, signal_id, start_date, end_date)
            
            if df.empty:
                return PlotGenerator.create_no_data_plot(title)
            
            # Group by detector
            detectors = df['Detector'].unique()
            num_detectors = len(detectors)
            
            # Create subplots
            fig = make_subplots(
                rows=num_detectors, cols=1,
                shared_xaxes=True,
                subplot_titles=[f"Det #{det}" for det in detectors],
                vertical_spacing=0.02
            )
            
            for i, detector in enumerate(detectors):
                det_data = df[df['Detector'] == detector]
                row_idx = i + 1
                
                # Group by phase for this detector
                phases = det_data['CallPhase'].unique()
                
                for phase in phases:
                    phase_data = det_data[det_data['CallPhase'] == phase]
                    color = COLORS_MAP.get(str(phase), Colors.DARK_GRAY)
                    
                    # Raw counts
                    fig.add_trace(
                        go.Scatter(
                            x=phase_data['Timeperiod'],
                            y=phase_data['vol_rc'],
                            mode='lines',
                            fill='tozeroy',
                            line=dict(color=color),
                            name=f'Phase {phase}' if i == 0 else "",
                            legendgroup=f'phase_{phase}',
                            showlegend=(i == 0),
                            customdata=[f"<b>Detector: {detector}</b><br>{t.strftime('%a %d %B %I:%M %p')}<br>Volume: <b>{Formatters.as_int(v)}</b>"
                                       for t, v in zip(phase_data['Timeperiod'], phase_data['vol_rc'])],
                            hovertemplate='%{customdata}<extra></extra>'
                        ),
                        row=row_idx, col=1
                    )
                
                # Adjusted counts
                if 'vol_ac' in det_data.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=det_data['Timeperiod'],
                            y=det_data['vol_ac'],
                            mode='lines',
                            line=dict(color=Colors.DARK_GRAY),
                            name='Adjusted Count' if i == 0 else "",
                            legendgroup='adjusted',
                            showlegend=(i == 0),
                            customdata=[f"<b>Detector: {detector}</b><br>{t.strftime('%a %d %B %I:%M %p')}<br>Volume: <b>{Formatters.as_int(v)}</b>"
                                       for t, v in zip(det_data['Timeperiod'], det_data['vol_ac'])],
                            hovertemplate='%{customdata}<extra></extra>'
                        ),
                        row=row_idx, col=1
                    )
                
                # Bad days overlay
                if 'bad_day' in det_data.columns:
                    bad_day_data = det_data[det_data['bad_day'] == True]
                    if not bad_day_data.empty:
                        max_vol = det_data['vol_rc'].max() if not det_data['vol_rc'].empty else 1000
                        
                        fig.add_trace(
                            go.Scatter(
                                x=bad_day_data['Timeperiod'],
                                y=[max_vol] * len(bad_day_data),
                                mode='lines',
                                fill='tozeroy',
                                line=dict(color='rgba(0,0,0,0.1)', shape='vh'),
                                fillcolor='rgba(0,0,0,0.2)',
                                name='Bad Days' if i == 0 else "",
                                legendgroup='bad_days',
                                showlegend=(i == 0),
                                customdata=[f"<b>Detector: {detector}</b><br>{t.strftime('%a %d %B %Y')}<br><b>Bad Day</b>"
                                           for t in bad_day_data['Timeperiod']],
                                hovertemplate='%{customdata}<extra></extra>'
                            ),
                            row=row_idx, col=1
                        )
                
                # Update y-axis for this
                if ymax is not None:
                    fig.update_yaxes(range=[0, ymax], row=row_idx, col=1)
                else:
                    fig.update_yaxes(rangemode='tozero', row=row_idx, col=1)
                
                fig.update_yaxes(tickformat=',.0f', row=row_idx, col=1)
                
                # Add detector annotation
                fig.add_annotation(
                    x=-0.03, y=0.5,
                    text=f"Det #{detector}",
                    showarrow=False,
                    xref=f"x{row_idx} domain" if row_idx > 1 else "x domain",
                    yref=f"y{row_idx} domain" if row_idx > 1 else "y domain",
                    xanchor="right",
                    font=dict(size=12)
                )
            
            # Update layout
            fig.update_layout(
                title=title,
                showlegend=True,
                margin=dict(l=120),
                xaxis=dict(type='date')
            )
            
            # Only show x-axis labels on bottom subplot
            for i in range(1, num_detectors):
                fig.update_xaxes(showticklabels=False, row=i, col=1)
            
            return fig
            
        except Exception as e:
            logger.error(f"Error creating volume plot: {e}")
            return PlotGenerator.create_no_data_plot(title)

class UserDelayCostPlot:
    """User delay cost plotting utilities"""
    
    @staticmethod
    def create_udc_plot(hourly_udc: pd.DataFrame) -> go.Figure:
        """Create user delay cost plot with multiple time periods"""
        
        if hourly_udc.empty:
            return PlotGenerator.create_no_data_plot("User Delay Costs")
        
        # Get time periods
        current_month = hourly_udc['Month'].max()
        last_month = current_month - relativedelta(months=1)
        last_year = current_month - relativedelta(years=1)
        
        # Format labels
        current_month_str = current_month.strftime("%B %Y")
        last_month_str = last_month.strftime("%B %Y")
        last_year_str = last_year.strftime("%B %Y")
        
        # Filter data for each period
        this_month_data = hourly_udc[hourly_udc['Month'] == current_month].copy()
        last_month_data = hourly_udc[hourly_udc['Month'] == last_month].copy()
        last_year_data = hourly_udc[hourly_udc['Month'] == last_year].copy()
        
        # Adjust dates for comparison
        if not last_month_data.empty:
            last_month_data['month_hour'] = last_month_data['month_hour'] + relativedelta(months=1)
        if not last_year_data.empty:
            last_year_data['month_hour'] = last_year_data['month_hour'] + relativedelta(years=1)
        
        # Get unique corridors
        corridors = hourly_udc['Corridor'].unique()
        num_corridors = len(corridors)
        
        # Calculate y-axis max
        max_delay_cost = hourly_udc['delay_cost'].max()
        ymax = round(max_delay_cost, -3) + 1000
        
        # Create subplots
        rows = (num_corridors + 1) // 2
        fig = make_subplots(
            rows=rows, cols=2,
            subplot_titles=[corridor for corridor in corridors],
            shared_xaxes=False,
            shared_yaxes=False,
            vertical_spacing=0.1,
            horizontal_spacing=0.1
        )
        
        for i, corridor in enumerate(corridors):
            row = (i // 2) + 1
            col = (i % 2) + 1
            
            # Filter data for this corridor
            corridor_this = this_month_data[this_month_data['Corridor'] == corridor]
            corridor_last_month = last_month_data[last_month_data['Corridor'] == corridor]
            corridor_last_year = last_year_data[last_year_data['Corridor'] == corridor]
            
            # Last year data (with fill)
            if not corridor_last_year.empty:
                fig.add_trace(
                    go.Scatter(
                        x=corridor_last_year['month_hour'],
                        y=corridor_last_year['delay_cost'],
                        mode='lines',
                        fill='tozeroy',
                        line=dict(color=Colors.LIGHT_BLUE),
                        fillcolor='rgba(174, 214, 226, 0.3)',
                        name=last_year_str if i == 0 else "",
                        legendgroup='last_year',
                        showlegend=(i == 0),
                        customdata=[f"<b>{corridor}</b><br><b>{h.strftime('%I:%M %p')}</b><br>User Delay Cost: <b>{Formatters.as_currency(c)}</b>"
                                   for h, c in zip(corridor_last_year['month_hour'], corridor_last_year['delay_cost'])],
                        hovertemplate='%{customdata}<extra></extra>'
                    ),
                    row=row, col=col
                )
            
            # Last month data
            if not corridor_last_month.empty:
                fig.add_trace(
                    go.Scatter(
                        x=corridor_last_month['month_hour'],
                        y=corridor_last_month['delay_cost'],
                        mode='lines',
                        line=dict(color=Colors.BLUE),
                        name=last_month_str if i == 0 else "",
                        legendgroup='last_month',
                        showlegend=(i == 0),
                        customdata=[f"<b>{corridor}</b><br><b>{h.strftime('%I:%M %p')}</b><br>User Delay Cost: <b>{Formatters.as_currency(c)}</b>"
                                   for h, c in zip(corridor_last_month['month_hour'], corridor_last_month['delay_cost'])],
                        hovertemplate='%{customdata}<extra></extra>'
                    ),
                    row=row, col=col
                )
            
            # Current month data
            if not corridor_this.empty:
                fig.add_trace(
                    go.Scatter(
                        x=corridor_this['month_hour'],
                        y=corridor_this['delay_cost'],
                        mode='lines',
                        line=dict(color=Colors.SIGOPS_GREEN),
                        name=current_month_str if i == 0 else "",
                        legendgroup='current',
                        showlegend=(i == 0),
                        customdata=[f"<b>{corridor}</b><br><b>{h.strftime('%I:%M %p')}</b><br>User Delay Cost: <b>{Formatters.as_currency(c)}</b>"
                                   for h, c in zip(corridor_this['month_hour'], corridor_this['delay_cost'])],
                        hovertemplate='%{customdata}<extra></extra>'
                    ),
                    row=row, col=col
                )
            
            # Update axes for this subplot
            fig.update_xaxes(tickformat='%I:%M %p', row=row, col=col)
            fig.update_yaxes(range=[0, ymax], tickformat='$,.0f', row=row, col=col)
            
            # Add corridor title annotation
            fig.add_annotation(
                x=0.2, y=0.5,
                text=f"<b>{corridor}</b>",
                showarrow=False,
                xref=f"x{i+1} domain" if i > 0 else "x domain",
                yref=f"y{i+1} domain" if i > 0 else "y domain",
                xanchor="left",
                font=dict(size=14)
            )
        
        fig.update_layout(
            title="User Delay Costs ($)",
            margin=dict(t=60),
            showlegend=True
        )
        
        return fig

class CorridorSummaryTable:
    """Corridor summary table generation"""
    
    @staticmethod
    def metric_formats(x: Union[float, int, None]) -> str:
        """Format metrics based on value range"""
        if pd.isna(x):
            return "N/A"
        
        if x <= 1:
            return Formatters.as_pct(x)
        elif x <= 10 and (x - round(x, 0) != 0):  # TTI and PTI
            return Formatters.as_2dec(x)
        else:  # throughput and outstanding tasks
            return Formatters.as_int(x)
    
    @staticmethod
    def get_col_order(num_corridors: int) -> List[int]:
        """Get column order for table display"""
        col_order = [0, 1]  # Metrics and Goal columns
        
        # Add metric and delta columns
        for i in range(num_corridors):
            col_order.extend([i + 2, i + num_corridors + 2])
        
        # Add check columns
        for i in range(num_corridors):
            col_order.extend([i + num_corridors * 2 + 2, i + num_corridors * 3 + 2])
        
        return col_order
    
    @staticmethod
    def create_corridor_summary_table(data: pd.DataFrame) -> dash_table.DataTable:
        """Create corridor summary table with formatting"""
        
        if data.empty:
            return dash_table.DataTable(data=[])
        
        # Prepare data - remove delta columns for main table
        dt = data.drop(columns=[col for col in data.columns if col.endswith('.delta')])
        dt = dt.drop(columns=['Zone_Group', 'Month'])
        
        # Melt data for metrics
        dt_melted = dt.melt(
            id_vars=['Corridor'],
            var_name='Metric',
            value_name='value'
        )
        
        # Pivot to get corridors as columns
        dt_pivot = dt_melted.pivot(index='Metric', columns='Corridor', values='value')
        dt_pivot = dt_pivot.reindex(MetricDefinitions.METRIC_ORDER)
        dt_pivot['Metrics'] = MetricDefinitions.METRIC_NAMES
        dt_pivot['Goal'] = MetricDefinitions.METRIC_GOALS
        
        # Reorder columns
        cols = ['Metrics', 'Goal'] + [col for col in dt_pivot.columns if col not in ['Metrics', 'Goal']]
        dt_pivot = dt_pivot[cols]
        
        # Prepare delta data
        dt_deltas = data.select_dtypes(include=[np.number])
        delta_cols = [col for col in dt_deltas.columns if col.endswith('.delta')]
        dt_deltas = dt_deltas[['Corridor'] + delta_cols]
        dt_deltas.columns = dt_deltas.columns.str.replace('.delta', '')
        
        # Format the data
        numeric_cols = [col for col in dt_pivot.columns if col not in ['Metrics', 'Goal']]
        for col in numeric_cols:
            if col in dt_pivot.columns:
                dt_pivot[col] = dt_pivot[col].apply(CorridorSummaryTable.metric_formats)
        
        # Convert to dict for DataTable
        table_data = dt_pivot.reset_index(drop=True).to_dict('records')
        
        # Define columns
        columns = [{"name": "Metrics", "id": "Metrics"},
                   {"name": "Goal", "id": "Goal"}]
        
        for col in numeric_cols:
            columns.append({"name": col, "id": col})
        
        # Create conditional formatting
        style_data_conditional = []
        
        # Add formatting based on goals (simplified version)
        for i, (metric, goal_numeric, goal_sign) in enumerate(zip(
            MetricDefinitions.METRIC_ORDER,
            MetricDefinitions.METRIC_GOALS_NUMERIC,
            MetricDefinitions.METRIC_GOALS_SIGN
        )):
            style_data_conditional.append({
                'if': {'row_index': i},
                'backgroundColor': '#f0f0f0'
            })
        
        return dash_table.DataTable(
            data=table_data,
            columns=columns,
            style_data_conditional=style_data_conditional,
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'fontFamily': 'Arial'
            },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            }
        )

class HealthMetricsTables:
    """Health metrics table utilities"""
    
    @staticmethod
    def create_maintenance_health_table(data: pd.DataFrame) -> dash_table.DataTable:
        """Create maintenance health metrics table"""
        
        if data.empty:
            return dash_table.DataTable(data=[])
        
        # Define column types for formatting
        all_cols = data.columns.tolist()
        rounded_cols = [col for col in all_cols if col.endswith('Score')] + ['Flash Events']
        percent_cols = (['Percent Health', 'Missing Data'] + 
                       [col for col in all_cols if col.endswith('Uptime')])
        
        # Prepare data
        table_data = data.to_dict('records')
        
        # Define columns
        columns = [{"name": col, "id": col, "type": "numeric", "format": {"specifier": ".0f"}}
                  if col in rounded_cols
                  else {"name": col, "id": col, "type": "numeric", "format": {"specifier": ".1%"}}
                  if col in percent_cols
                  else {"name": col, "id": col}
                  for col in all_cols]
        
        # Style conditional formatting
        style_data_conditional = [
            {
                'if': {'filter_query': '{Subcorridor} = ALL'},
                'backgroundColor': 'lightgray',
                'fontStyle': 'italic'
            },
            {
                'if': {'filter_query': '{Corridor} = ALL'},
                'backgroundColor': 'gray',
                'fontWeight': 'bold'
            },
            {
                'if': {'column_id': 'Percent Health'},
                'fontWeight': 'bold',
                'fontSize': '20px'
            }
        ]
        
        # Add color coding for Missing Data
        for threshold, color in [(0.05, '#D6604D'), (0.3, '#B2182B'), (0.5, '#67001F')]:
            style_data_conditional.append({
                'if': {
                    'filter_query': f'{{Missing Data}} > {threshold}',
                    'column_id': 'Missing Data'
                },
                'color': color
            })
        
        return dash_table.DataTable(
            data=table_data,
            columns=columns,
            style_data_conditional=style_data_conditional,
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'fontFamily': 'Arial',
                'fontSize': '12px'
            },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold',
                'textAlign': 'center'
            },
            filter_action="native",
            sort_action="native",
            page_size=1000,
            style_table={'height': '550px', 'overflowY': 'auto', 'overflowX': 'auto'}
        )
    
    @staticmethod
    def create_operations_health_table(data: pd.DataFrame) -> dash_table.DataTable:
        """Create operations health metrics table"""
        
        if data.empty:
            return dash_table.DataTable(data=[])
        
        # Define column types for formatting
        all_cols = data.columns.tolist()
        rounded0_cols = [col for col in all_cols if col.endswith('Score')]
        rounded1_cols = ['Ped Delay']
        rounded2_cols = ['Platoon Ratio', 'Travel Time Index', 'Buffer Index']
        percent_cols = ['Percent Health', 'Missing Data', 'Split Failures']
        
        # Prepare data
        table_data = data.to_dict('records')
        
        # Define columns with appropriate formatting
        columns = []
        for col in all_cols:
            if col in rounded0_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".0f"}})
            elif col in rounded1_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".1f"}})
            elif col in rounded2_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".2f"}})
            elif col in percent_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".1%"}})
            else:
                columns.append({"name": col, "id": col})
        
        # Style conditional formatting
        style_data_conditional = [
            {
                'if': {'filter_query': '{Subcorridor} = ALL'},
                'backgroundColor': 'lightgray',
                'fontStyle': 'italic'
            },
            {
                'if': {'filter_query': '{Corridor} = ALL'},
                'backgroundColor': 'gray',
                'fontWeight': 'bold'
            },
            {
                'if': {'column_id': 'Percent Health'},
                'fontWeight': 'bold',
                'fontSize': '20px'
            }
        ]
        
        # Add color coding for Missing Data
        for threshold, color in [(0.05, '#D6604D'), (0.3, '#B2182B'), (0.5, '#67001F')]:
            style_data_conditional.append({
                'if': {
                    'filter_query': f'{{Missing Data}} > {threshold}',
                    'column_id': 'Missing Data'
                },
                'color': color
            })
        
        return dash_table.DataTable(
            data=table_data,
            columns=columns,
            style_data_conditional=style_data_conditional,
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'fontFamily': 'Arial',
                'fontSize': '12px'
            },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold',
                'textAlign': 'center'
            },
            filter_action="native",
            sort_action="native",
            page_size=1000,
            style_table={'height': '550px', 'overflowY': 'auto', 'overflowX': 'auto'}
        )
    
    @staticmethod
    def create_safety_health_table(data: pd.DataFrame) -> dash_table.DataTable:
        """Create safety health metrics table"""
        
        if data.empty:
            return dash_table.DataTable(data=[])
        
        # Define column types for formatting
        all_cols = data.columns.tolist()
        rounded0_cols = [col for col in all_cols if col.endswith('Score')]
        rounded2_cols = [col for col in all_cols if col.endswith('Index')]
        percent_cols = ['Percent Health', 'Missing Data']
        
        # Prepare data
        table_data = data.to_dict('records')
        
        # Define columns with appropriate formatting
        columns = []
        for col in all_cols:
            if col in rounded0_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".0f"}})
            elif col in rounded2_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".2f"}})
            elif col in percent_cols:
                columns.append({"name": col, "id": col, "type": "numeric", "format": {"specifier": ".1%"}})
            else:
                columns.append({"name": col, "id": col})
        
        # Style conditional formatting
        style_data_conditional = [
            {
                'if': {'filter_query': '{Subcorridor} = ALL'},
                'backgroundColor': 'lightgray',
                'fontStyle': 'italic'
            },
            {
                'if': {'filter_query': '{Corridor} = ALL'},
                'backgroundColor': 'gray',
                'fontWeight': 'bold'
            },
            {
                'if': {'column_id': 'Percent Health'},
                'fontWeight': 'bold',
                'fontSize': '20px'
            }
        ]
        
        # Add color coding for Missing Data
        for threshold, color in [(0.05, '#D6604D'), (0.3, '#B2182B'), (0.5, '#67001F')]:
            style_data_conditional.append({
                'if': {
                    'filter_query': f'{{Missing Data}} > {threshold}',
                    'column_id': 'Missing Data'
                },
                'color': color
            })
        
        return dash_table.DataTable(
            data=table_data,
            columns=columns,
            style_data_conditional=style_data_conditional,
            style_cell={
                'textAlign': 'center',
                'padding': '10px',
                'fontFamily': 'Arial',
                'fontSize': '12px'
            },
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold',
                'textAlign': 'center'
            },
            filter_action="native",
            sort_action="native",
            page_size=1000,
            style_table={'height': '550px', 'overflowY': 'auto', 'overflowX': 'auto'}
        )
    
    @staticmethod
    def filter_health_data(data: pd.DataFrame, zone_group: str, corridor: str) -> pd.DataFrame:
        """Filter health data based on zone group and corridor selection"""
        
        # Add Zone_Group column if missing
        if 'Zone_Group' not in data.columns and 'Zone' in data.columns:
            data = data.copy()
            data['Zone_Group'] = data['Zone']
        
        if corridor == "All Corridors":
            return DataFilter.filter_mr_data(data, zone_group)
        else:
            return data[data['Corridor'] == corridor]

class CCTVPlots:
    """CCTV camera plotting utilities"""
    
    @staticmethod
    def plot_individual_cctvs(daily_cctv_df: pd.DataFrame,
                             month: date,
                             zone_group: str) -> go.Figure:
        """Create individual CCTV camera status heatmap"""
        
        # Filter data
        filtered_df = daily_cctv_df[
            (daily_cctv_df['Date'] < month + relativedelta(months=1)) &
            (daily_cctv_df['Zone_Group'] == zone_group) &
            (daily_cctv_df['Corridor'].astype(str) != zone_group)
        ].copy()
        
        if filtered_df.empty:
            return PlotGenerator.create_no_data_plot("CCTV Status")
        
        # Rename columns for clarity
        filtered_df = filtered_df.rename(columns={
            'Corridor': 'CameraID',
            'Zone_Group': 'Corridor'
        })
        
        # Pivot data for heatmap
        pivot_data = filtered_df.pivot_table(
            index=['CameraID', 'Description'],
            columns='Date',
            values='up',
            fill_value=0
        )
        
        # Sort by CameraID descending
        pivot_data = pivot_data.sort_index(ascending=False)
        
        # Round values and define status mapping
        pivot_matrix = pivot_data.values.round(0).astype(int)
        
        def get_status(x):
            status_map = {
                0: "Camera Down",
                1: "Working at encoder, but not 511",
                2: "Working on 511"
            }
            return status_map.get(x, "Unknown")
        
        # Create custom data for hover
        customdata = []
        for i, (camera_id, desc) in enumerate(pivot_data.index):
            row_data = []
            for j, status_val in enumerate(pivot_matrix[i]):
                status_text = get_status(status_val)
                hover_text = f"<b>{desc}</b><br>{status_text}"
                row_data.append(hover_text)
            customdata.append(row_data)
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=pivot_matrix,
            x=[d.strftime('%Y-%m-%d') for d in pivot_data.columns],
            y=[f"{idx[0]}: {idx[1]}" for idx in pivot_data.index],
            colorscale=[
                [0, Colors.LIGHT_GRAY_BAR],      # Camera Down
                [0.5, "#e48c5b"],                # Working at encoder
                [1, Colors.BROWN]                # Working on 511
            ],
            showscale=False,
            customdata=customdata,
            hovertemplate='%{customdata}<br>%{x}<extra></extra>'
        ))
        
        fig.update_layout(
            title="CCTV Camera Status",
            yaxis=dict(title=""),
            margin=dict(l=150),
            showlegend=False
        )
        
        return fig

# Utility functions for data processing and configuration
class ConfigLoader:
    """Configuration loading utilities"""
    
    @staticmethod
    def load_config(config_path: str = "Monthly_Report.yaml") -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return {}
    
    @staticmethod
    def load_aws_config(config_path: str = "Monthly_Report_AWS.yaml") -> Dict[str, Any]:
        """Load AWS configuration from YAML file"""
        try:
            with open(config_path, 'r') as file:
                return yaml.safe_load(file)
        except Exception as e:
            logger.error(f"Error loading AWS config: {e}")
            return {}

class S3DataLoader:
    """S3 data loading utilities"""
    
    @staticmethod
    def create_s3_reactive_poll(interval_ms: int, bucket: str, object_key: str, 
                               aws_config: Dict[str, str]):
        """Create reactive polling for S3 objects (for Shiny apps)"""
        # This would be implemented for Shiny/Dash reactive components
        pass
    
    @staticmethod
    def read_s3_object(bucket: str, object_key: str, aws_config: Dict[str, str]) -> pd.DataFrame:
        """Read object from S3"""
        try:
            import boto3
            
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_config.get('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=aws_config.get('AWS_SECRET_ACCESS_KEY'),
                region_name=aws_config.get('AWS_DEFAULT_REGION', 'us-east-1')
            )
            
            # Determine file type and read accordingly
            if object_key.endswith('.qs'):
                # Would need to implement qs reading for Python
                logger.warning("QS format not supported in Python, use parquet instead")
                return pd.DataFrame()
            elif object_key.endswith('.feather'):
                import tempfile
                with tempfile.NamedTemporaryFile() as tmp_file:
                    s3_client.download_file(bucket, object_key, tmp_file.name)
                    return pd.read_feather(tmp_file.name)
            elif object_key.endswith('.parquet'):
                import tempfile
                with tempfile.NamedTemporaryFile() as tmp_file:
                    s3_client.download_file(bucket, object_key, tmp_file.name)
                    return pd.read_parquet(tmp_file.name)
            else:
                logger.error(f"Unsupported file format: {object_key}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error reading S3 object {object_key}: {e}")
            return pd.DataFrame()

class DatabaseConnections:
    """Database connection utilities"""
    
    @staticmethod
    def get_athena_connection(config: Dict[str, str]):
        """Get Athena database connection"""
        try:
            import sqlalchemy as sa
            from sqlalchemy import create_engine
            
            # Build Athena connection string
            connection_string = (
                f"awsathena+rest://{config['uid']}:{config['pwd']}@"
                f"athena.{config.get('region', 'us-east-1')}.amazonaws.com:443/"
                f"{config['database']}?s3_staging_dir={config['s3_staging_dir']}"
            )
            
            engine = create_engine(connection_string)
            return engine
            
        except Exception as e:
            logger.error(f"Error creating Athena connection: {e}")
            return None
    
    @staticmethod
    def get_aurora_connection(config: Dict[str, str]):
        """Get Aurora database connection"""
        try:
            import sqlalchemy as sa
            from sqlalchemy import create_engine
            
            # Build Aurora connection string
            connection_string = (
                f"postgresql://{config['uid']}:{config['pwd']}@"
                f"{config['host']}:{config.get('port', 5432)}/{config['database']}"
            )
            
            engine = create_engine(connection_string)
            return engine
            
        except Exception as e:
            logger.error(f"Error creating Aurora connection: {e}")
            return None

# Initialize constants and global variables
# Date utilities
def current_month() -> date:
    """Get current month (first day)"""
    today = date.today()
    return today.replace(day=1)

def last_month() -> date:
    """Get last month (first day)"""
    today = date.today()
    first_of_month = today.replace(day=1)
    return first_of_month - relativedelta(months=1)

def get_month_options(num_months: int = 13) -> List[Tuple[str, date]]:
    """Get list of month options for dropdowns"""
    last_month_date = last_month()
    months = []
    
    for i in range(num_months):
        month_date = last_month_date - relativedelta(months=i)
        month_str = month_date.strftime("%B %Y")
        months.append((month_str, month_date))
    
    return months

# Zone group mappings
RTOP1_ZONES = ["Zone 1", "Zone 2", "Zone 3", "Zone 8"]
RTOP2_ZONES = ["Zone 4", "Zone 5", "Zone 6", "Zone 7m", "Zone 7d"]

# Performance goals
PERFORMANCE_GOALS = {
    "tp": None,      # Throughput
    "aogd": 0.80,    # Arrivals on Green
    "prd": 1.20,     # Platoon Ratio
    "qsd": None,     # Queue Spillback
    "sfd": None,     # Split Failures
    "tti": 1.20,     # Travel Time Index
    "pti": 1.30,     # Planning Time Index
    "du": 0.95,      # Detector Uptime
    "ped": 0.95,     # Pedestrian Uptime
    "cctv": 0.95,    # CCTV Uptime
    "cu": 0.95,      # Communications Uptime
    "pau": 0.95      # Pushbutton Availability Uptime
}

class ValueBoxGenerator:
    """Generate value boxes for dashboard"""
    
    @staticmethod
    def get_value_box(data: pd.DataFrame, 
                     variable: str,
                     format_func: Callable,
                     zone: str,
                     month: date,
                     break_line: bool = False,
                     quarter: str = None) -> Dict[str, Any]:
        """Generate value box data"""
        
        try:
            if quarter is None:
                filtered_data = data[
                    (data['Corridor'] == zone) & 
                    (data['Month'] == month)
                ]
            else:
                filtered_data = data[
                    (data['Corridor'] == zone) & 
                    (data['Quarter'] == quarter)
                ]
            
            if filtered_data.empty:
                return {"value": "N/A", "delta": "N/A", "valid": False}
            
            row = filtered_data.iloc[0]
            value = row[variable]
            delta = row.get('delta', 0)
            
            if pd.isna(value):
                return {"value": "N/A", "delta": "N/A", "valid": False}
            
            if pd.isna(delta):
                delta = 0
            
            formatted_value = format_func(value)
            delta_sign = "+" if delta > 0 else ""
            formatted_delta = f"({delta_sign}{Formatters.as_pct(delta)})"
            
            return {
                "value": formatted_value,
                "delta": formatted_delta,
                "break_line": break_line,
                "valid": True
            }
            
        except Exception as e:
            logger.error(f"Error generating value box: {e}")
            return {"value": "Error", "delta": "Error", "valid": False}

class ReportGenerator:
    """Main report generation utilities"""
    
    @staticmethod
    def get_last_modified_data(df: pd.DataFrame, 
                              zone: str = None, 
                              month: date = None) -> pd.DataFrame:
        """Get last modified data for zone and month"""
        
        # Group by Month and Zone, get latest data
        result = df.groupby(['Month', 'Zone']).apply(
            lambda x: x.loc[x['LastModified'].idxmax()]
        ).reset_index(drop=True)
        
        # Filter by zone if provided
        if zone is not None:
            result = result[result['Zone'] == zone]
        
        # Filter by month if provided
        if month is not None:
            result = result[result['Month'] == month]
        
        return result
    
    @staticmethod
    def read_from_database(connection) -> pd.DataFrame:
        """Read progress report content from database"""
        try:
            query = """
            SELECT * FROM progress_report_content
            ORDER BY Month DESC, Zone, LastModified DESC
            """
            
            df = pd.read_sql(query, connection)
            
            # Convert date columns
            df['Month'] = pd.to_datetime(df['Month']).dt.date
            df['LastModified'] = pd.to_datetime(df['LastModified'])
            
            # Get top 10 most recent records per Month/Zone
            df = df.groupby(['Month', 'Zone']).head(10)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading from database: {e}")
            return pd.DataFrame()

class DashboardUtilities:
    """Dashboard-specific utilities"""
    
    @staticmethod
    def create_empty_plot(title: str = "No Data Available") -> go.Figure:
        """Create empty plot placeholder"""
        fig = go.Figure()
        fig.add_annotation(
            x=0.5, y=0.5,
            text=title,
            showarrow=False,
            font=dict(size=16),
            xref="paper",
            yref="paper"
        )
        fig.update_layout(
            xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
            yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
            margin=dict(l=50, r=50, t=50, b=50)
        )
        return fig
    
    @staticmethod
    def format_table_data(df: pd.DataFrame, 
                         format_rules: Dict[str, str]) -> pd.DataFrame:
        """Format table data according to rules"""
        
        df_formatted = df.copy()
        
        for column, format_type in format_rules.items():
            if column in df_formatted.columns:
                if format_type == 'percent':
                    df_formatted[column] = df_formatted[column].apply(
                        lambda x: Formatters.as_pct(x) if pd.notna(x) else 'N/A'
                    )
                elif format_type == 'integer':
                    df_formatted[column] = df_formatted[column].apply(
                        lambda x: Formatters.as_int(x) if pd.notna(x) else 'N/A'
                    )
                elif format_type == 'decimal':
                    df_formatted[column] = df_formatted[column].apply(
                        lambda x: Formatters.as_2dec(x) if pd.notna(x) else 'N/A'
                    )
                elif format_type == 'currency':
                    df_formatted[column] = df_formatted[column].apply(
                        lambda x: Formatters.as_currency(x) if pd.notna(x) else 'N/A'
                    )
        
        return df_formatted
    
    @staticmethod
    def get_usable_cores() -> int:
        """Get number of usable CPU cores"""
        import multiprocessing
        total_cores = multiprocessing.cpu_count()
        # Leave one core free for system processes
        return max(1, total_cores - 1)

class DataValidation:
    """Data validation utilities"""
    
    @staticmethod
    def validate_date_range(start_date: date, end_date: date) -> bool:
        """Validate date range"""
        return start_date <= end_date
    
    @staticmethod
    def validate_dataframe(df: pd.DataFrame, required_columns: List[str]) -> bool:
        """Validate DataFrame has required columns"""
        if df.empty:
            return False
        
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            logger.warning(f"Missing required columns: {missing_cols}")
            return False
        
        return True
    
    @staticmethod
    def sanitize_signal_id(signal_id: Union[str, int]) -> Optional[int]:
        """Sanitize and validate signal ID"""
        try:
            signal_id_int = int(signal_id)
            if signal_id_int > 0:
                return signal_id_int
            else:
                logger.warning(f"Invalid signal ID: {signal_id}")
                return None
        except (ValueError, TypeError):
            logger.warning(f"Cannot convert signal ID to integer: {signal_id}")
            return None

class CacheManager:
    """Simple caching utilities"""
    
    def __init__(self, max_size: int = 100, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = {}
        self.access_times = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if key not in self.cache:
            return None
        
        # Check if expired
        if time.time() - self.access_times[key] > self.ttl_seconds:
            del self.cache[key]
            del self.access_times[key]
            return None
        
        self.access_times[key] = time.time()
        return self.cache[key]
    
    def set(self, key: str, value: Any) -> None:
        """Set value in cache"""
        # Remove oldest entries if at max size
        while len(self.cache) >= self.max_size:
            oldest_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
            del self.cache[oldest_key]
            del self.access_times[oldest_key]
        
        self.cache[key] = value
        self.access_times[key] = time.time()
    
    def clear(self) -> None:
        """Clear cache"""
        self.cache.clear()
        self.access_times.clear()

# Global cache instance
cache_manager = CacheManager()

class PerformanceMonitor:
    """Performance monitoring utilities"""
    
    @staticmethod
    def measure_execution_time(func: Callable) -> Callable:
        """Decorator to measure function execution time"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time
                logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
                return result
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds: {e}")
                raise
        return wrapper
    
    @staticmethod
    def get_memory_usage() -> float:
        """Get current memory usage in MB"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            logger.warning("psutil not available for memory monitoring")
            return 0.0

# Export main classes and functions for easy import
__all__ = [
    'Colors',
    'Formatters', 
    'MetricDefinitions',
    'DataFilter',
    'PlotGenerator',
    'PerformancePlots',
    'TravelTimePlots',
    'UptimePlots',
    'AlertsPlots',
    'TEAMSPlots',
    'VolumeAnalysis',
    'UserDelayCostPlot',
    'CorridorSummaryTable',
    'HealthMetricsTables',
    'CCTVPlots',
    'ConfigLoader',
    'S3DataLoader',
    'DatabaseConnections',
    'ValueBoxGenerator',
    'ReportGenerator',
    'DashboardUtilities',
    'DataValidation',
    'CacheManager',
    'PerformanceMonitor',
    'current_month',
    'last_month',
    'get_month_options',
    'RTOP1_ZONES',
    'RTOP2_ZONES',
    'PERFORMANCE_GOALS',
    'cache_manager'
]

# Initialize logging for the module
logger.info("Monthly Report UI Functions module loaded successfully")