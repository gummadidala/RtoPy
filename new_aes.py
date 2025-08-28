# -*- coding: utf-8 -*-
"""
New Aesthetics for Matplotlib/Seaborn

Python conversion of new_aes.R - allows adding multiple color/fill scales
to the same plot, similar to the ggnewscale R package functionality.

This provides the same functionality as the R ggnewscale package:
https://github.com/eliocamp/ggnewscale
"""

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import seaborn as sns
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
import warnings
from copy import deepcopy
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NewScale:
    """
    Class to handle new aesthetic scales for matplotlib/seaborn plots
    Equivalent to the new_aes class in R
    """
    
    def __init__(self, aesthetic: str):
        """
        Initialize a new scale for the given aesthetic
        
        Args:
            aesthetic: The aesthetic to create new scale for ('color', 'fill', etc.)
        """
        self.aesthetic = self._standardize_aes_names(aesthetic)
        self.scale_count = 1
        
    def _standardize_aes_names(self, aes: Union[str, List[str]]) -> Union[str, List[str]]:
        """
        Standardize aesthetic names (equivalent to ggplot2::standardise_aes_names)
        
        Args:
            aes: Aesthetic name(s)
            
        Returns:
            Standardized aesthetic name(s)
        """
        if isinstance(aes, str):
            # Convert common aliases
            if aes in ['colour', 'col']:
                return 'color'
            elif aes == 'fill':
                return 'fill'
            else:
                return aes
        elif isinstance(aes, list):
            return [self._standardize_aes_names(a) for a in aes]
        else:
            return aes

def new_scale(aesthetic: str) -> NewScale:
    """
    Create a new scale for the specified aesthetic
    
    Args:
        aesthetic: Aesthetic name ('color', 'fill', etc.)
        
    Returns:
        NewScale object
    """
    return NewScale(aesthetic)

def new_scale_color() -> NewScale:
    """
    Convenient function to create new color scale
    
    Returns:
        NewScale object for color aesthetic
    """
    return new_scale("color")

def new_scale_colour() -> NewScale:
    """
    Convenient function to create new colour scale (British spelling)
    
    Returns:
        NewScale object for color aesthetic
    """
    return new_scale("color")

def new_scale_fill() -> NewScale:
    """
    Convenient function to create new fill scale
    
    Returns:
        NewScale object for fill aesthetic
    """
    return new_scale("fill")

class MultiScalePlot:
    """
    Class to handle plots with multiple color/fill scales
    Equivalent to the ggplot behavior modification in R
    """
    
    def __init__(self, figure=None, ax=None):
        """
        Initialize multi-scale plot handler
        
        Args:
            figure: Matplotlib figure object
            ax: Matplotlib axes object
        """
        self.figure = figure or plt.gcf()
        self.ax = ax or plt.gca()
        self.color_scales = {}
        self.fill_scales = {}
        self.scale_mappings = {}
        self.current_color_scale = 0
        self.current_fill_scale = 0
        
    def add_layer_with_new_scale(self, plot_func, new_scale_obj: NewScale, 
                                *args, **kwargs):
        """
        Add a plot layer with a new scale
        
        Args:
            plot_func: Plotting function (e.g., plt.scatter, sns.scatterplot)
            new_scale_obj: NewScale object
            *args: Arguments for plot function
            **kwargs: Keyword arguments for plot function
        """
        
        # Increment scale counter for the aesthetic
        if new_scale_obj.aesthetic == 'color':
            self.current_color_scale += 1
            scale_suffix = f"_new_{self.current_color_scale}"
        elif new_scale_obj.aesthetic == 'fill':
            self.current_fill_scale += 1
            scale_suffix = f"_new_{self.current_fill_scale}"
        else:
            scale_suffix = "_new"
        
        # Modify color/fill parameters if present
        modified_kwargs = self._modify_aesthetic_kwargs(kwargs, new_scale_obj.aesthetic, scale_suffix)
        
        # Execute the plotting function
        result = plot_func(*args, **modified_kwargs)
        
        return result
    
    def _modify_aesthetic_kwargs(self, kwargs: Dict, aesthetic: str, suffix: str) -> Dict:
        """
        Modify aesthetic keyword arguments to use new scale
        
        Args:
            kwargs: Original keyword arguments
            aesthetic: Aesthetic type ('color', 'fill')
            suffix: Suffix to add to aesthetic name
            
        Returns:
            Modified keyword arguments
        """
        
        modified_kwargs = kwargs.copy()
        
        # Handle different aesthetic parameter names
        aesthetic_params = {
            'color': ['c', 'color', 'colors'],
            'fill': ['facecolor', 'facecolors', 'fc']
        }
        
        if aesthetic in aesthetic_params:
            for param in aesthetic_params[aesthetic]:
                if param in modified_kwargs:
                    # Store original mapping
                    original_values = modified_kwargs[param]
                    new_param = f"{param}{suffix}"
                    modified_kwargs[new_param] = original_values
                    
                    # Store mapping for legend creation
                    self.scale_mappings[new_param] = original_values
        
        return modified_kwargs

class MultiColormap:
    """
    Handler for multiple colormaps in a single plot
    """
    
    def __init__(self):
        self.colormaps = {}
        self.normalizers = {}
        self.scale_count = 0
        
    def add_colormap(self, data: Union[np.ndarray, pd.Series], 
                     cmap: str = 'viridis', 
                     name: Optional[str] = None) -> Tuple[np.ndarray, str]:
        """
        Add a new colormap for data
        
        Args:
            data: Data to map colors to
            cmap: Colormap name
            name: Optional name for this colormap
            
        Returns:
            Tuple of (colors, colormap_name)
        """
        
        if name is None:
            name = f"scale_{self.scale_count}"
            self.scale_count += 1
        
        # Create normalizer for data range
        vmin, vmax = np.nanmin(data), np.nanmax(data)
        norm = mcolors.Normalize(vmin=vmin, vmax=vmax)
        
        # Get colormap
        colormap = plt.cm.get_cmap(cmap)
        
        # Generate colors
        colors = colormap(norm(data))
        
        # Store for legend creation
        self.colormaps[name] = colormap
        self.normalizers[name] = norm
        
        return colors, name
    
    def create_colorbar(self, ax, colormap_name: str, label: str = ""):
        """
        Create colorbar for a specific colormap
        
        Args:
            ax: Matplotlib axes
            colormap_name: Name of colormap to create colorbar for
            label: Label for colorbar
        """
        
        if colormap_name in self.colormaps:
            cmap = self.colormaps[colormap_name]
            norm = self.normalizers[colormap_name]
            
            # Create ScalarMappable for colorbar
            sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
            sm.set_array([])
            
            # Create colorbar
            cbar = plt.colorbar(sm, ax=ax)
            cbar.set_label(label)
            
            return cbar
        else:
            logger.warning(f"Colormap '{colormap_name}' not found")
            return None

def create_multi_scale_plot(data1: pd.DataFrame = None, data2: pd.DataFrame = None,
                           x1: str = 'x', y1: str = 'y', z1: str = 'z',
                           x2: str = 'x', y2: str = 'y', color2: str = 'value',
                           cmap1: str = 'plasma', cmap2: str = 'viridis',
                           figsize: Tuple[int, int] = (10, 8)) -> plt.Figure:
    """
    Create a plot with multiple color scales (equivalent to the R example)
    
    Args:
        data1: First dataset (for contour/background)
        data2: Second dataset (for points)
        x1, y1, z1: Column names for first dataset
        x2, y2, color2: Column names for second dataset
        cmap1, cmap2: Colormaps for each scale
        figsize: Figure size
        
    Returns:
        Matplotlib figure object
    """
    
    fig, ax = plt.subplots(figsize=figsize)
    
    # Initialize multi-colormap handler
    multi_cmap = MultiColormap()
    
    # First layer: contour plot with first colormap
    if data1 is not None:
        # Reshape data for contour if needed
        if z1 in data1.columns:
            # Pivot data for contour plot
            contour_data = data1.pivot(index=y1, columns=x1, values=z1)
            
            # Create contour plot
            contour = ax.contour(contour_data.columns, contour_data.index, 
                               contour_data.values, cmap=cmap1)
            
            # Add colorbar for contour
            cbar1 = plt.colorbar(contour, ax=ax)
            cbar1.set_label(z1)
    
    # Second layer: scatter plot with second colormap
    if data2 is not None:
        # Get colors for second dataset
        colors2, cmap_name = multi_cmap.add_colormap(
            data2[color2], cmap=cmap2, name='scatter_colors'
        )
        
        # Create scatter plot
        scatter = ax.scatter(data2[x2], data2[y2], c=colors2, s=50, alpha=0.8)
        
        # Add second colorbar
        multi_cmap.create_colorbar(ax, cmap_name, label=color2)
    
    # Set labels
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_title('Multi-Scale Plot')
    
    plt.tight_layout()
    
    return fig

def demonstrate_multi_scale_plot():
    """
    Demonstrate multi-scale plotting (equivalent to R example)
    Creates the same plot as shown in the R script
    """
    
    # Create volcano-like data (equivalent to reshape2::melt(volcano))
    np.random.seed(42)
    x_vals = np.arange(1, 81)
    y_vals = np.arange(1, 61)
    X, Y = np.meshgrid(x_vals, y_vals)
    
    # Generate volcano-like surface
    Z = 100 + 50 * np.sin(X/10) * np.cos(Y/8) + 20 * np.random.random(X.shape)
    
    # Create DataFrame equivalent to melted volcano data
    vd = pd.DataFrame({
        'x': X.flatten(),
        'y': Y.flatten(), 
        'z': Z.flatten()
    })
    
    # Create point measurements (equivalent to d in R)
    np.random.seed(123)
    d = pd.DataFrame({
        'x': np.random.uniform(1, 80, 30),
        'y': np.random.uniform(1, 60, 30),
        'abund': np.random.normal(0, 1, 30)
    })
    
    # Create the multi-scale plot
    fig = create_multi_scale_plot(
        data1=vd, data2=d,
        x1='x', y1='y', z1='z',
        x2='x', y2='y', color2='abund',
        cmap1='plasma', cmap2='viridis'
    )
    
    plt.show()
    
    return fig

class SeabornMultiScale:
    """
    Multi-scale support for Seaborn plots
    """
    
    @staticmethod
    def multi_scale_scatterplot(data: pd.DataFrame, x: str, y: str,
                               color_vars: List[str], 
                               cmaps: List[str] = None,
                               **kwargs) -> plt.Figure:
        """
        Create scatter plot with multiple color scales using Seaborn
        
        Args:
            data: DataFrame with data
            x, y: Column names for x and y coordinates
            color_vars: List of columns to use for different color scales
            cmaps: List of colormaps to use
            **kwargs: Additional arguments for scatterplot
            
        Returns:
            Matplotlib figure
        """
        
        if cmaps is None:
            cmaps = ['viridis', 'plasma', 'inferno', 'magma'][:len(color_vars)]
        
        fig, ax = plt.subplots(figsize=(10, 8))
        multi_cmap = MultiColormap()
        
        # Plot each color variable with different colormap
        for i, (color_var, cmap) in enumerate(zip(color_vars, cmaps)):
            if color_var in data.columns:
                # Get colors for this variable
                colors, cmap_name = multi_cmap.add_colormap(
                    data[color_var], cmap=cmap, name=f'scale_{color_var}'
                )
                
                # Create scatter plot
                scatter = ax.scatter(
                    data[x], data[y], 
                    c=colors, 
                    alpha=0.7,
                    s=kwargs.get('s', 50),
                    label=color_var
                )
                
                # Add colorbar
                multi_cmap.create_colorbar(ax, cmap_name, label=color_var)
        
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        ax.legend()
        plt.tight_layout()
        
        return fig

# Utility functions for compatibility with R-style plotting

def scale_color_viridis(option: str = "viridis", **kwargs) -> Dict:
    """
    Create viridis color scale parameters (equivalent to R's scale_color_viridis_c)
    
    Args:
        option: Viridis option ('viridis', 'plasma', 'inferno', 'magma')
        **kwargs: Additional parameters
        
    Returns:
        Dictionary with colormap parameters
    """
    
    viridis_options = {
        "A": "viridis",
        "B": "inferno", 
        "C": "plasma",
        "D": "viridis"
    }
    
    if option in viridis_options:
        cmap_name = viridis_options[option]
    else:
        cmap_name = option
    
    return {
        'cmap': cmap_name,
        'vmin': kwargs.get('vmin', None),
        'vmax': kwargs.get('vmax', None),
        'alpha': kwargs.get('alpha', 1.0)
    }

def scale_fill_viridis(option: str = "viridis", **kwargs) -> Dict:
    """
    Create viridis fill scale parameters (equivalent to R's scale_fill_viridis_c)
    
    Args:
        option: Viridis option ('viridis', 'plasma', 'inferno', 'magma')
        **kwargs: Additional parameters
        
    Returns:
        Dictionary with colormap parameters
    """
    return scale_color_viridis(option, **kwargs)

class PlotlyMultiScale:
    """
    Multi-scale support for Plotly (alternative to matplotlib)
    """
    
    def __init__(self):
        self.traces = []
        self.colorbars = []
        
    def add_trace_with_colorscale(self, trace_type: str, data: Dict, 
                                 colorscale: str = "Viridis", 
                                 showscale: bool = True, 
                                 colorbar_x: float = None):
        """
        Add trace with custom colorscale
        
        Args:
            trace_type: Type of trace ('scatter', 'contour', etc.)
            data: Data dictionary for trace
            colorscale: Plotly colorscale name
            showscale: Whether to show colorbar
            colorbar_x: X position for colorbar
        """
        
        try:
            import plotly.graph_objects as go
            
            if colorbar_x is None:
                colorbar_x = 1.02 + (len(self.colorbars) * 0.15)
            
            trace_data = data.copy()
            trace_data.update({
                'colorscale': colorscale,
                'showscale': showscale,
                'colorbar': dict(
                    x=colorbar_x,
                    thickness=15,
                    len=0.7
                )
            })
            
            if trace_type == 'scatter':
                trace = go.Scatter(**trace_data)
            elif trace_type == 'contour':
                trace = go.Contour(**trace_data)
            elif trace_type == 'heatmap':
                trace = go.Heatmap(**trace_data)
            else:
                raise ValueError(f"Unsupported trace type: {trace_type}")
            
            self.traces.append(trace)
            if showscale:
                self.colorbars.append(colorbar_x)
            
        except ImportError:
            logger.warning("Plotly not available. Install with: pip install plotly")
    
    def create_figure(self, title: str = "Multi-Scale Plot"):
        """
        Create Plotly figure with all traces
        """
        
        try:
            import plotly.graph_objects as go
            
            fig = go.Figure(data=self.traces)
            
            # Adjust layout for multiple colorbars
            fig.update_layout(
                title=title,
                xaxis_title="X",
                yaxis_title="Y",
                showlegend=True
            )
            
            return fig
            
        except ImportError:
            logger.warning("Plotly not available. Install with: pip install plotly")
            return None

def create_plotly_multi_scale_example():
    """
    Create example with Plotly multi-scale plot
    """
    
    try:
        import plotly.graph_objects as go
        
        # Create sample data
        np.random.seed(42)
        
        # Contour data
        x_contour = np.linspace(0, 10, 50)
        y_contour = np.linspace(0, 10, 50)
        X, Y = np.meshgrid(x_contour, y_contour)
        Z = np.sin(X) * np.cos(Y)
        
        # Scatter data
        x_scatter = np.random.uniform(0, 10, 30)
        y_scatter = np.random.uniform(0, 10, 30)
        colors_scatter = np.random.normal(0, 1, 30)
        
        # Create multi-scale plot
        multi_plot = PlotlyMultiScale()
        
        # Add contour with first colorscale
        multi_plot.add_trace_with_colorscale(
            'contour',
            {
                'x': x_contour,
                'y': y_contour,
                'z': Z,
                'name': 'Contour',
                'contours': dict(coloring='heatmap')
            },
            colorscale='Plasma',
            colorbar_x=1.02
        )
        
        # Add scatter with second colorscale
        multi_plot.add_trace_with_colorscale(
            'scatter',
            {
                'x': x_scatter,
                'y': y_scatter,
                'mode': 'markers',
                'marker': dict(
                    color=colors_scatter,
                    size=10,
                    opacity=0.8
                ),
                'name': 'Points'
            },
            colorscale='Viridis',
            colorbar_x=1.2
        )
        
        fig = multi_plot.create_figure("Multi-Scale Plot Example")
        return fig
        
    except ImportError:
        logger.warning("Plotly not available for this example")
        return None

# Context manager for new scales (similar to R's '+' operator behavior)

class NewScaleContext:
    """
    Context manager for applying new scales
    """
    
    def __init__(self, scale_obj: NewScale):
        self.scale_obj = scale_obj
        self.original_rcParams = {}
        
    def __enter__(self):
        # Store original matplotlib rcParams that might be modified
        self.original_rcParams = {
            'axes.prop_cycle': plt.rcParams['axes.prop_cycle']
        }
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original rcParams
        for key, value in self.original_rcParams.items():
            plt.rcParams[key] = value

def with_new_scale(scale_obj: NewScale):
    """
    Decorator to apply new scale to plotting function
    
    Args:
        scale_obj: NewScale object
    
    Returns:
        Decorator function
    """
    
    def decorator(plot_func):
        def wrapper(*args, **kwargs):
            with NewScaleContext(scale_obj) as ctx:
                return plot_func(*args, **kwargs)
        return wrapper
    return decorator

# Example usage functions (equivalent to R example)

def create_volcano_data():
    """
    Create volcano-like dataset (equivalent to reshape2::melt(volcano) in R)
    """
    
    # Create a volcano-like surface
    x = np.linspace(0, 80, 87)
    y = np.linspace(0, 60, 61)
    X, Y = np.meshgrid(x, y)
    
    # Generate volcano-like elevation data
    center_x, center_y = 40, 30
    Z = 100 + 50 * np.exp(-((X - center_x)**2 + (Y - center_y)**2) / 200) + \
        10 * np.sin(X/5) * np.cos(Y/5) + 5 * np.random.random(X.shape)
    
    # Melt to long format (equivalent to reshape2::melt)
    vd = pd.DataFrame({
        'x': X.flatten(),
        'y': Y.flatten(),
        'z': Z.flatten()
    })
    
    return vd

def create_point_measurements():
    """
    Create point measurement data (equivalent to data.frame in R example)
    """
    
    np.random.seed(42)
    d = pd.DataFrame({
        'x': np.random.uniform(1, 80, 30),
        'y': np.random.uniform(1, 60, 30),
        'abund': np.random.normal(0, 1, 30)
    })
    
    return d

def replicate_r_example():
    """
    Replicate the exact R example with Python
    
    This creates the same plot as the R code:
    ggplot(mapping = aes(x, y)) +
      geom_contour(aes(z = z, color = ..level..), data = vd) +
      scale_color_viridis_c(option = "D") +
      new_scale_color() +
      geom_point(data = d, size = 3, aes(color = abund)) +
      scale_color_viridis_c(option = "A")
    """
    
    # Create data
    vd = create_volcano_data()
    d = create_point_measurements()
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(12, 8))
    
    # Initialize multi-colormap handler
    multi_cmap = MultiColormap()
    
    # First layer: contour plot (equivalent to geom_contour)
    # Pivot data for contour
    vd_pivot = vd.pivot_table(index='y', columns='x', values='z', 
                             fill_value=np.nan)
    
    # Create contour plot with first colorscale (option "D" = viridis)
    contour = ax.contour(vd_pivot.columns, vd_pivot.index, vd_pivot.values, 
                        levels=15, cmap='viridis')
    
    # Add first colorbar
    cbar1 = plt.colorbar(contour, ax=ax, pad=0.1)
    cbar1.set_label('Elevation Level')
    cbar1.ax.yaxis.set_label_position('left')
    
    # Second layer: scatter plot with new color scale (option "A" = viridis)
    # Get colors for abundance data
    colors_scatter, _ = multi_cmap.add_colormap(
        d['abund'], cmap='plasma', name='abundance'
    )
    
    # Create scatter plot
    scatter = ax.scatter(d['x'], d['y'], c=colors_scatter, s=100, 
                        alpha=0.8, edgecolors='black', linewidth=0.5)
    
    # Add second colorbar
    # Create new axes for second colorbar
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    divider = make_axes_locatable(ax)
    cax2 = divider.append_axes("right", size="5%", pad=0.3)
    
    # Create second colorbar
    norm = mcolors.Normalize(vmin=d['abund'].min(), vmax=d['abund'].max())
    sm = plt.cm.ScalarMappable(cmap='plasma', norm=norm)
    sm.set_array([])
    cbar2 = plt.colorbar(sm, cax=cax2)
    cbar2.set_label('Abundance')
    
    # Set labels and title
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_title('Multi-Scale Plot: Elevation Contours + Point Measurements')
    
    # Adjust layout
    plt.tight_layout()
    
    return fig

def advanced_multi_scale_example():
    """
    Advanced example with multiple datasets and scales
    """
    
    # Create multiple datasets
    np.random.seed(123)
    
    # Dataset 1: Temperature field
    x1 = np.linspace(0, 10, 50)
    y1 = np.linspace(0, 10, 50)
    X1, Y1 = np.meshgrid(x1, y1)
    temp = 20 + 10 * np.sin(X1) * np.cos(Y1) + np.random.normal(0, 1, X1.shape)
    
    # Dataset 2: Pressure measurements
    x2 = np.random.uniform(0, 10, 25)
    y2 = np.random.uniform(0, 10, 25)
    pressure = np.random.uniform(1000, 1020, 25)
    
    # Dataset 3: Wind speed
    x3 = np.random.uniform(0, 10, 15)
    y3 = np.random.uniform(0, 10, 15)
    wind_speed = np.random.uniform(0, 20, 15)
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 10))
    multi_cmap = MultiColormap()
    
    # Layer 1: Temperature contours
    contour = ax.contourf(X1, Y1, temp, levels=20, cmap='coolwarm', alpha=0.7)
    cbar1 = plt.colorbar(contour, ax=ax, shrink=0.8, pad=0.02)
    cbar1.set_label('Temperature (°C)')
    
    # Layer 2: Pressure points
    pressure_colors, _ = multi_cmap.add_colormap(pressure, cmap='viridis')
    scatter1 = ax.scatter(x2, y2, c=pressure_colors, s=150, 
                         marker='s', alpha=0.9, edgecolors='black',
                         label='Pressure')
    
    # Layer 3: Wind speed points
    wind_colors, _ = multi_cmap.add_colormap(wind_speed, cmap='plasma')
    scatter2 = ax.scatter(x3, y3, c=wind_colors, s=200, 
                         marker='^', alpha=0.9, edgecolors='black',
                         label='Wind Speed')
    
    # Create additional colorbars
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    divider = make_axes_locatable(ax)
    
    # Pressure colorbar
    cax2 = divider.append_axes("right", size="5%", pad=0.3)
    norm2 = mcolors.Normalize(vmin=pressure.min(), vmax=pressure.max())
    sm2 = plt.cm.ScalarMappable(cmap='viridis', norm=norm2)
    sm2.set_array([])
    cbar2 = plt.colorbar(sm2, cax=cax2)
    cbar2.set_label('Pressure (hPa)')
    
    # Wind speed colorbar
    cax3 = divider.append_axes("right", size="5%", pad=0.6)
    norm3 = mcolors.Normalize(vmin=wind_speed.min(), vmax=wind_speed.max())
    sm3 = plt.cm.ScalarMappable(cmap='plasma', norm=norm3)
    sm3.set_array([])
    cbar3 = plt.colorbar(sm3, cax=cax3)
    cbar3.set_label('Wind Speed (m/s)')
    
    # Formatting
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_title('Advanced Multi-Scale Plot: Temperature, Pressure, and Wind')
    ax.legend(loc='upper left')
    
    plt.tight_layout()
    return fig

# Main demonstration function

def main():
    """
    Main function to demonstrate all multi-scale plotting capabilities
    """
    
    print("Creating multi-scale plots...")
    
    # Example 1: Basic multi-scale plot
    print("1. Creating basic multi-scale plot...")
    fig1 = demonstrate_multi_scale_plot()
    
    # Example 2: Replicate R example exactly
    print("2. Replicating R ggnewscale example...")
    fig2 = replicate_r_example()
    
    # Example 3: Advanced multi-scale example
    print("3. Creating advanced multi-scale example...")
    fig3 = advanced_multi_scale_example()
    
    # Example 4: Plotly version (if available)
    print("4. Creating Plotly multi-scale example...")
    fig4 = create_plotly_multi_scale_example()
    if fig4 is not None:
        try:
            fig4.show()
        except:
            print("Plotly figure created but display failed")
    
    # Show matplotlib figures
    plt.show()
    
    print("Multi-scale plotting demonstration complete!")
    
    return {
        'basic_plot': fig1,
        'r_equivalent': fig2, 
        'advanced_plot': fig3,
        'plotly_plot': fig4
    }

# Utility classes for seamless integration with existing plotting workflows

class PandasPlotMultiScale:
    """
    Multi-scale plotting integration with pandas plotting
    """
    
    @staticmethod
    def scatter_multi_color(df: pd.DataFrame, x: str, y: str, 
                           color_columns: List[str], 
                           cmaps: List[str] = None,
                           figsize: Tuple[int, int] = (10, 8),
                           **kwargs):
        """
        Create scatter plot with multiple color mappings from DataFrame
        
        Args:
            df: DataFrame with data
            x, y: Column names for coordinates
            color_columns: List of columns to use for coloring
            cmaps: List of colormaps
            figsize: Figure size
            **kwargs: Additional scatter plot arguments
        """
        
        if cmaps is None:
            cmaps = ['viridis', 'plasma', 'inferno', 'magma'][:len(color_columns)]
        
        fig, ax = plt.subplots(figsize=figsize)
        multi_cmap = MultiColormap()
        
        # Create base scatter plot for positioning
        ax.scatter(df[x], df[y], alpha=0, s=0)  # Invisible base
        
        # Add colored layers
        for i, (col, cmap) in enumerate(zip(color_columns, cmaps)):
            if col in df.columns:
                colors, cmap_name = multi_cmap.add_colormap(
                    df[col], cmap=cmap, name=f'scale_{col}'
                )
                
                # Calculate position offset for multiple point clouds
                offset = i * 0.1
                x_vals = df[x] + offset
                y_vals = df[y] + offset
                
                scatter = ax.scatter(
                    x_vals, y_vals, 
                    c=colors,
                    alpha=kwargs.get('alpha', 0.7),
                    s=kwargs.get('s', 50),
                    label=col
                )
                
                # Add colorbar
                multi_cmap.create_colorbar(ax, cmap_name, label=col)
        
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        ax.legend()
        plt.tight_layout()
        
        return fig

class SeabornMultiScaleExtended:
    """
    Extended multi-scale support for Seaborn
    """
    
    @staticmethod
    def heatmap_with_points(heatmap_data: pd.DataFrame,
                           points_data: pd.DataFrame,
                           x_col: str, y_col: str, color_col: str,
                           heatmap_cmap: str = 'Blues',
                           points_cmap: str = 'Reds',
                           figsize: Tuple[int, int] = (12, 8)):
        """
        Create heatmap with overlaid scatter points using different color scales
        
        Args:
            heatmap_data: DataFrame for heatmap (should be pivot table format)
            points_data: DataFrame for scatter points
            x_col, y_col, color_col: Column names for scatter plot
            heatmap_cmap: Colormap for heatmap
            points_cmap: Colormap for points
            figsize: Figure size
        """
        
        fig, ax = plt.subplots(figsize=figsize)
        
        # Create heatmap
        heatmap = sns.heatmap(heatmap_data, cmap=heatmap_cmap, 
                             cbar_kws={'label': 'Heatmap Values'},
                             ax=ax)
        
        # Overlay scatter points with different colormap
        multi_cmap = MultiColormap()
        colors, cmap_name = multi_cmap.add_colormap(
            points_data[color_col], cmap=points_cmap, name='scatter'
        )
        
        scatter = ax.scatter(
            points_data[x_col], points_data[y_col],
            c=colors, s=100, alpha=0.8, 
            edgecolors='black', linewidth=0.5
        )
        
        # Add second colorbar
        multi_cmap.create_colorbar(ax, cmap_name, label=color_col)
        
        plt.tight_layout()
        return fig

# Compatibility functions for R-like syntax

def aes(**kwargs):
    """
    Create aesthetic mapping (R ggplot2 style)
    
    Args:
        **kwargs: Aesthetic mappings (x=, y=, color=, etc.)
        
    Returns:
        Dictionary of aesthetic mappings
    """
    return kwargs

def geom_contour(data: pd.DataFrame, mapping: Dict, **kwargs):
    """
    Create contour plot layer (R ggplot2 style)
    
    Args:
        data: DataFrame with data
        mapping: Aesthetic mappings from aes()
        **kwargs: Additional parameters
        
    Returns:
        Contour plot result
    """
    
    ax = plt.gca()
    
    # Extract mappings
    x = mapping.get('x', 'x')
    y = mapping.get('y', 'y') 
    z = mapping.get('z', 'z')
    
    # Pivot data for contour
    contour_data = data.pivot_table(index=y, columns=x, values=z)
    
    # Create contour
    contour = ax.contour(contour_data.columns, contour_data.index, 
                        contour_data.values, **kwargs)
    
    return contour

def geom_point(data: pd.DataFrame, mapping: Dict, **kwargs):
    """
    Create scatter plot layer (R ggplot2 style)
    
    Args:
        data: DataFrame with data
        mapping: Aesthetic mappings from aes()
        **kwargs: Additional parameters
        
    Returns:
        Scatter plot result
    """
    
    ax = plt.gca()
    
    # Extract mappings
    x = mapping.get('x', 'x')
    y = mapping.get('y', 'y')
    color = mapping.get('color', None)
    size = kwargs.get('size', 50)
    
    # Create scatter plot
    if color and color in data.columns:
        scatter = ax.scatter(data[x], data[y], c=data[color], 
                           s=size, **kwargs)
    else:
        scatter = ax.scatter(data[x], data[y], s=size, **kwargs)
    
    return scatter

# Export functions for easy importing
__all__ = [
    'NewScale', 'new_scale', 'new_scale_color', 'new_scale_colour', 
    'new_scale_fill', 'MultiScalePlot', 'MultiColormap',
    'create_multi_scale_plot', 'demonstrate_multi_scale_plot',
    'replicate_r_example', 'scale_color_viridis', 'scale_fill_viridis',
    'PlotlyMultiScale', 'SeabornMultiScale', 'PandasPlotMultiScale',
    'SeabornMultiScaleExtended', 'aes', 'geom_contour', 'geom_point',
    'create_volcano_data', 'create_point_measurements'
]

# Example notebook/script for testing
if __name__ == "__main__":
    main()
