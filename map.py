#!/usr/bin/env python3

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point, LineString
import re
import json
from s3_parquet_io import s3_read_parquet
import awswrangler as wr
import boto3

def points_to_line(data, long_col, lat_col, id_field=None, sort_field=None):
    """Convert points to line geometry"""
    
    # Sort data if sort field is provided
    if sort_field is not None:
        if id_field is not None:
            data = data.sort_values([id_field, sort_field])
        else:
            data = data.sort_values(sort_field)
    
    # Create geometry from coordinates
    geometry = [Point(xy) for xy in zip(data[long_col], data[lat_col])]
    gdf = gpd.GeoDataFrame(data, geometry=geometry)
    
    if id_field is None:
        # Single line from all points
        line_coords = list(zip(data[long_col], data[lat_col]))
        line = LineString(line_coords)
        return gpd.GeoDataFrame({'id': ['line1']}, geometry=[line])
    else:
        # Multiple lines grouped by id_field
        lines = []
        line_ids = []
        
        for group_id, group in gdf.groupby(id_field):
            if len(group) >= 2:  # Need at least 2 points for a line
                line_coords = list(zip(group[long_col], group[lat_col]))
                line = LineString(line_coords)
                lines.append(line)
                line_ids.append(f"line_{group_id}")
        
        return gpd.GeoDataFrame({'id': line_ids}, geometry=lines)

def get_tmc_coords(coords_string):
    """Extract TMC coordinates from coordinate string"""
    
    if pd.isna(coords_string):
        return pd.DataFrame(columns=['longitude', 'latitude'])
    
    try:
        # Extract coordinates from string pattern
        coord_match = re.search(r"'(.*?)'", coords_string)
        if not coord_match:
            return pd.DataFrame(columns=['longitude', 'latitude'])
        
        coord_str = coord_match.group(1)
        coord_pairs = coord_str.split(',')
        
        coordinates = []
        for pair in coord_pairs:
            coords = pair.strip().split(' ')
            if len(coords) >= 2:
                try:
                    lon = float(coords[0])
                    lat = float(coords[1])
                    coordinates.append({'longitude': lon, 'latitude': lat})
                except ValueError:
                    continue
        
        return pd.DataFrame(coordinates)
        
    except Exception as e:
        print(f"Error parsing TMC coordinates: {e}")
        return pd.DataFrame(columns=['longitude', 'latitude'])

def get_geom_coords(coords_string):
    """Extract geometry coordinates from coordinate string"""
    
    if pd.isna(coords_string):
        return pd.DataFrame(columns=['longitude', 'latitude'])
    
    try:
        # Split by comma or colon
        coord_parts = re.split('[,:]', coords_string)
        
        coordinates = []
        for part in coord_parts:
            coords = part.strip().split(' ')
            if len(coords) >= 2:
                try:
                    lon = float(coords[0])
                    lat = float(coords[1])
                    coordinates.append({'longitude': lon, 'latitude': lat})
                except ValueError:
                    continue
        
        return pd.DataFrame(coordinates)
        
    except Exception as e:
        print(f"Error parsing geometry coordinates: {e}")
        return pd.DataFrame(columns=['longitude', 'latitude'])

def get_signals_sp(bucket, corridors):
    """Get signals spatial data"""
    
    # Color palette for corridors
    corridor_palette = [
        "#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
        "#ff7f00", "#ffff33", "#a65628", "#f781bf"
    ]
    
    BLACK = "#000000"
    WHITE = "#FFFFFF"
    GRAY = "#D0D0D0"
    
    # Get RTOP corridors
    rtop_corridors = corridors[corridors['Zone'].str.startswith('Zone', na=False)]['Corridor'].drop_duplicates()
    num_corridors = len(rtop_corridors)
    
    # Assign colors to corridors
    corridor_colors = pd.DataFrame({
        'Corridor': rtop_corridors.tolist() + ['None'],
        'color': (corridor_palette * ((num_corridors // 8) + 1))[:num_corridors] + [GRAY]
    })
    
    # Reorder to put 'None' first
    corridor_colors['Corridor'] = pd.Categorical(
        corridor_colors['Corridor'],
        categories=['None'] + [c for c in corridor_colors['Corridor'] if c != 'None'],
        ordered=True
    )
    corridor_colors = corridor_colors.sort_values('Corridor')
    
    # Get most recent intersections data
    try:
        session = boto3.Session()
        bucket_objects = session.client('s3').list_objects_v2(
            Bucket=bucket,
            Prefix='maxv_atspm_intersections'
        )
        
        if 'Contents' in bucket_objects:
            most_recent_key = max(obj['Key'] for obj in bucket_objects['Contents'])
            
            # Read intersections data
            intersections = wr.s3.read_csv(
                path=f"s3://{bucket}/{most_recent_key}",
                dtype={
                    'PrimaryName': 'string',
                    'SecondaryName': 'string',
                    'IPAddress': 'string',
                    'Enabled': 'boolean',
                    'Note_atspm': 'string',
                    'Name': 'string',
                    'Note_maxv': 'string',
                    'HostAddress': 'string'
                }
            )
        else:
            intersections = pd.DataFrame()
            
    except Exception as e:
        print(f"Error reading intersections data: {e}")
        intersections = pd.DataFrame()
    
    if intersections.empty:
        return gpd.GeoDataFrame()
    
    # Filter and clean intersections data
    intersections = intersections[
        (intersections['Latitude'] != 0) & 
        (intersections['Longitude'] != 0)
    ].copy()
    
    intersections['SignalID'] = intersections['SignalID'].astype(str)
    
    # Join with corridors
    signals_sp = intersections.merge(corridors, on='SignalID', how='left')
    
    # Fill missing corridor info
    signals_sp['Corridor'] = signals_sp['Corridor'].fillna('None')
    
    # Join with corridor colors
    signals_sp = signals_sp.merge(corridor_colors, on='Corridor', how='left')
    
    # Create description
    signals_sp['Description'] = signals_sp.apply(
        lambda row: row['Description'] if pd.notna(row['Description']) 
        else f"{row['SignalID']}: {row['PrimaryName']} @ {row['SecondaryName']}",
        axis=1
    )
    
    # Set visual properties
    signals_sp['fill_color'] = signals_sp['Zone'].apply(
        lambda x: BLACK if pd.notna(x) and x.startswith('Z') else WHITE
    )
    
    signals_sp['stroke_color'] = signals_sp['Corridor'].apply(
        lambda x: GRAY if x == 'None' else BLACK
    )
    
    # Override stroke color for Zone signals
    mask = signals_sp['Zone'].str.startswith('Z', na=False)
    signals_sp.loc[mask, 'stroke_color'] = signals_sp.loc[mask, 'color']
    
    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(signals_sp['Longitude'], signals_sp['Latitude'])]
    signals_gdf = gpd.GeoDataFrame(signals_sp, geometry=geometry, crs='EPSG:4326')
    
    return signals_gdf

def get_map_data(conf):
    """Get all map data including corridors, subcorridors, and signals"""
    
    BLACK = "#000000"
    WHITE = "#FFFFFF"
    GRAY = "#D0D0D0"
    DARK_GRAY = "#7A7A7A"
    DARK_DARK_GRAY = "#494949"
    
    corridor_palette = [
        "#e41a1c", "#377eb8", "#4daf4a", "#984ea3",
        "#ff7f00", "#ffff33", "#a65628", "#f781bf"
    ]
    
    try:
        # Read corridors data
        corridors = wr.s3.read_feather(
            path=f"s3://{conf['bucket']}/all_Corridors_Latest.feather"
        )
        
        # Get RTOP corridors and assign colors
        rtop_corridors = corridors[corridors['Zone'].str.startswith('Zone', na=False)]['Corridor'].drop_duplicates()
        num_corridors = len(rtop_corridors)
        
        corridor_colors = pd.DataFrame({
            'Corridor': rtop_corridors.tolist() + ['None'],
            'color': (corridor_palette * ((num_corridors // 8) + 1))[:num_corridors] + [GRAY]
        })
        
        # Zone colors
        zone_colors = pd.DataFrame({
            'zone': [f"Zone {i}" for i in range(1, 9)],
            'color': corridor_palette
        })
        zone_colors.loc[zone_colors['color'] == "#ffff33", 'color'] = "#f7f733"
        
        # Read TMC data
        tmcs = wr.s3.read_excel(
            path=f"s3://{conf['bucket']}/Corridor_TMCs_Latest.xlsx"
        )
        
        # Process TMC coordinates
        tmcs['Corridor'] = tmcs['Corridor'].fillna('None')
        tmcs = tmcs.merge(corridor_colors, on='Corridor', how='left')
        
        # Set colors for corridors without explicit color
        mask = (tmcs['Corridor'] != 'None') & tmcs['color'].isna()
        tmcs.loc[mask, 'color'] = DARK_DARK_GRAY
        
        # Process TMC geometries
        tmcs['tmc_coords'] = tmcs['coordinates'].apply(get_tmc_coords)
        
        # Create spatial data for TMCs
        corridors_lines = []
        corridor_data = []
        
        for idx, row in tmcs.iterrows():
            coords_df = row['tmc_coords']
            if len(coords_df) >= 2:
                line_gdf = points_to_line(coords_df, 'longitude', 'latitude')
                if not line_gdf.empty:
                    corridors_lines.extend(line_gdf.geometry.tolist())
                    corridor_data.append(row.to_dict())
        
        if corridors_lines:
            corridors_sp = gpd.GeoDataFrame(corridor_data, geometry=corridors_lines, crs='EPSG:4326')
        else:
            corridors_sp = gpd.GeoDataFrame()
        
        # Create subcorridors spatial data
        subcor_tmcs = tmcs[tmcs['Subcorridor'].notna()].copy()
        
        subcorridors_lines = []
        subcorridor_data = []
        
        for idx, row in subcor_tmcs.iterrows():
            coords_df = row['tmc_coords']
            if len(coords_df) >= 2:
                line_gdf = points_to_line(coords_df, 'longitude', 'latitude')
                if not line_gdf.empty:
                    subcorridors_lines.extend(line_gdf.geometry.tolist())
                    subcorridor_data.append(row.to_dict())
        
        if subcorridors_lines:
            subcorridors_sp = gpd.GeoDataFrame(subcorridor_data, geometry=subcorridors_lines, crs='EPSG:4326')
        else:
            subcorridors_sp = gpd.GeoDataFrame()
        
        # Get signals spatial data
        signals_sp = get_signals_sp(conf['bucket'], corridors)
        
        map_data = {
            'corridors_sp': corridors_sp,
            'subcorridors_sp': subcorridors_sp,
            'signals_sp': signals_sp,
            'corridor_colors': corridor_colors,
            'zone_colors': zone_colors
        }
        
        return map_data
        
    except Exception as e:
        print(f"Error creating map data: {e}")
        return {
            'corridors_sp': gpd.GeoDataFrame(),
            'subcorridors_sp': gpd.GeoDataFrame(), 
            'signals_sp': gpd.GeoDataFrame(),
            'corridor_colors': pd.DataFrame(),
            'zone_colors': pd.DataFrame()
        }

def create_interactive_map(map_data, performance_data=None):
    """Create an interactive map with signals and corridors"""
    
    import folium
    from folium import plugins
    
    # Create base map centered on data
    if not map_data['signals_sp'].empty:
        center_lat = map_data['signals_sp'].geometry.y.mean()
        center_lon = map_data['signals_sp'].geometry.x.mean()
    else:
        center_lat, center_lon = 33.7490, -84.3880  # Default to Atlanta
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=11,
        tiles='OpenStreetMap'
    )
    
    # Add corridors
    if not map_data['corridors_sp'].empty:
        for idx, corridor in map_data['corridors_sp'].iterrows():
            if corridor.geometry.geom_type == 'LineString':
                coords = [[point[1], point[0]] for point in corridor.geometry.coords]
                
                folium.PolyLine(
                    locations=coords,
                    color=corridor.get('color', '#000000'),
                    weight=3,
                    opacity=0.8,
                    popup=f"Corridor: {corridor.get('Corridor', 'Unknown')}"
                ).add_to(m)
    
    # Add signals
    if not map_data['signals_sp'].empty:
        for idx, signal in map_data['signals_sp'].iterrows():
            
            # Determine marker color based on performance if provided
            if performance_data is not None and 'SignalID' in performance_data.columns:
                perf_row = performance_data[performance_data['SignalID'] == signal['SignalID']]
                if not perf_row.empty:
                    # Color based on performance metric
                    perf_value = perf_row.iloc[0].get('performance_score', 0.5)
                    if perf_value >= 0.8:
                        marker_color = 'green'
                    elif perf_value >= 0.6:
                        marker_color = 'orange'
                    else:
                        marker_color = 'red'
                else:
                    marker_color = 'blue'
            else:
                marker_color = signal.get('stroke_color', 'blue')
                if marker_color in ['#000000', BLACK]:
                    marker_color = 'blue'
                elif marker_color in ['#D0D0D0', GRAY]:
                    marker_color = 'gray'
            
            # Create popup content
            popup_content = f"""
            <b>Signal ID:</b> {signal.get('SignalID', 'Unknown')}<br>
            <b>Location:</b> {signal.get('Description', 'Unknown')}<br>
            <b>Corridor:</b> {signal.get('Corridor', 'None')}<br>
            <b>Zone:</b> {signal.get('Zone', 'None')}
            """
            
            if performance_data is not None:
                perf_row = performance_data[performance_data['SignalID'] == signal['SignalID']]
                if not perf_row.empty:
                    perf_score = perf_row.iloc[0].get('performance_score', 'N/A')
                    popup_content += f"<br><b>Performance Score:</b> {perf_score}"
            
            folium.CircleMarker(
                location=[signal.geometry.y, signal.geometry.x],
                radius=8,
                color='black',
                fillColor=marker_color,
                fillOpacity=0.7,
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=signal.get('Description', signal.get('SignalID', 'Unknown'))
            ).add_to(m)
    
    # Add legend
    legend_html = create_map_legend(map_data.get('corridor_colors'), performance_data is not None)
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m

def create_map_legend(corridor_colors=None, has_performance=False):
    """Create HTML legend for the map"""
    
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 200px; height: auto; 
                background-color: white; border:2px solid grey; z-index:9999; 
                font-size:14px; padding: 10px
                ">
    <h4>Legend</h4>
    '''
    
    if has_performance:
        legend_html += '''
        <p><i class="fa fa-circle" style="color:green"></i> High Performance (≥80%)</p>
        <p><i class="fa fa-circle" style="color:orange"></i> Medium Performance (60-80%)</p>
        <p><i class="fa fa-circle" style="color:red"></i> Low Performance (<60%)</p>
        <hr>
        '''
    
    if corridor_colors is not None and not corridor_colors.empty:
        legend_html += '<h5>Corridors</h5>'
        for _, row in corridor_colors.iterrows():
            if row['Corridor'] != 'None':
                legend_html += f'<p><i class="fa fa-minus" style="color:{row["color"]}"></i> {row["Corridor"]}</p>'
    
    legend_html += '</div>'
    
    return legend_html

def export_map_data(map_data, output_format='geojson', output_path=None):
    """Export map data to various formats"""
    
    if output_format.lower() == 'geojson':
        # Export as GeoJSON
        result = {}
        
        if not map_data['signals_sp'].empty:
            result['signals'] = json.loads(map_data['signals_sp'].to_json())
        
        if not map_data['corridors_sp'].empty:
            result['corridors'] = json.loads(map_data['corridors_sp'].to_json())
        
        if not map_data['subcorridors_sp'].empty:
            result['subcorridors'] = json.loads(map_data['subcorridors_sp'].to_json())
        
        if output_path:
            with open(output_path, 'w') as f:
                json.dump(result, f, indent=2)
        
        return result
    
    elif output_format.lower() == 'shapefile':
        # Export as Shapefiles
        if output_path:
            base_path = output_path.replace('.shp', '')
            
            if not map_data['signals_sp'].empty:
                map_data['signals_sp'].to_file(f"{base_path}_signals.shp")
            
            if not map_data['corridors_sp'].empty:
                map_data['corridors_sp'].to_file(f"{base_path}_corridors.shp")
            
            if not map_data['subcorridors_sp'].empty:
                map_data['subcorridors_sp'].to_file(f"{base_path}_subcorridors.shp")
    
    elif output_format.lower() == 'kml':
        # Export as KML
        if output_path:
            import simplekml
            
            kml = simplekml.Kml()
            
            # Add signals
            if not map_data['signals_sp'].empty:
                signals_folder = kml.newfolder(name="Traffic Signals")
                for _, signal in map_data['signals_sp'].iterrows():
                    pnt = signals_folder.newpoint(
                        name=signal.get('SignalID', 'Unknown'),
                        description=signal.get('Description', ''),
                        coords=[(signal.geometry.x, signal.geometry.y)]
                    )
            
            # Add corridors
            if not map_data['corridors_sp'].empty:
                corridors_folder = kml.newfolder(name="Corridors")
                for _, corridor in map_data['corridors_sp'].iterrows():
                    if corridor.geometry.geom_type == 'LineString':
                        coords = [(point[0], point[1]) for point in corridor.geometry.coords]
                        line = corridors_folder.newlinestring(
                            name=corridor.get('Corridor', 'Unknown'),
                            coords=coords
                        )
                        line.style.linestyle.color = corridor.get('color', '#000000')
                        line.style.linestyle.width = 3
            
            kml.save(output_path)

def calculate_corridor_bounds(corridor_gdf):
    """Calculate bounding box for corridor geometries"""
    
    if corridor_gdf.empty:
        return None
    
    bounds = corridor_gdf.total_bounds
    return {
        'min_lon': bounds[0],
        'min_lat': bounds[1], 
        'max_lon': bounds[2],
        'max_lat': bounds[3],
        'center_lon': (bounds[0] + bounds[2]) / 2,
        'center_lat': (bounds[1] + bounds[3]) / 2
    }

def filter_signals_by_corridor(signals_gdf, corridor_name):
    """Filter signals by corridor name"""
    
    if signals_gdf.empty:
        return gpd.GeoDataFrame()
    
    return signals_gdf[signals_gdf['Corridor'] == corridor_name].copy()

def get_signals_within_buffer(signals_gdf, geometry, buffer_distance_meters=1000):
    """Get signals within a buffer distance of a geometry"""
    
    if signals_gdf.empty:
        return gpd.GeoDataFrame()
    
    # Reproject to a projected CRS for accurate distance calculation
    # Using Georgia State Plane West (EPSG:3857 for web mercator approximation)
    signals_projected = signals_gdf.to_crs('EPSG:3857')
    
    if hasattr(geometry, 'to_crs'):
        geometry_projected = geometry.to_crs('EPSG:3857')
    else:
        # Single geometry
        from shapely.ops import transform
        import pyproj
        
        project = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:3857', always_xy=True).transform
        geometry_projected = transform(project, geometry)
    
    # Create buffer
    if hasattr(geometry_projected, 'buffer'):
        buffer_geom = geometry_projected.buffer(buffer_distance_meters)
    else:
        buffer_geom = geometry_projected.buffer(buffer_distance_meters)
    
    # Find signals within buffer
    within_buffer = signals_projected[signals_projected.geometry.within(buffer_geom)]
    
    # Convert back to original CRS
    return within_buffer.to_crs('EPSG:4326')

def create_corridor_summary_stats(signals_gdf, corridors_gdf):
    """Create summary statistics for each corridor"""
    
    if signals_gdf.empty:
        return pd.DataFrame()
    
    corridor_stats = signals_gdf.groupby('Corridor').agg({
        'SignalID': 'count',
        'Latitude': ['min', 'max', 'mean'],
        'Longitude': ['min', 'max', 'mean']
    }).round(4)
    
    # Flatten column names
    corridor_stats.columns = [
        'signal_count', 'min_lat', 'max_lat', 'center_lat',
        'min_lon', 'max_lon', 'center_lon'
    ]
    
    corridor_stats = corridor_stats.reset_index()
    
    # Add corridor length if corridors_gdf is provided
    if not corridors_gdf.empty:
        corridor_lengths = corridors_gdf.groupby('Corridor').apply(
            lambda x: x.geometry.length.sum()
        ).reset_index(name='length_degrees')
        
        corridor_stats = corridor_stats.merge(corridor_lengths, on='Corridor', how='left')
    
    return corridor_stats

def validate_coordinates(df, lat_col='Latitude', lon_col='Longitude'):
    """Validate and clean coordinate data"""
    
    df = df.copy()
    
    # Check for valid coordinate ranges
    valid_lat = (df[lat_col] >= -90) & (df[lat_col] <= 90)
    valid_lon = (df[lon_col] >= -180) & (df[lon_col] <= 180)
    
    # Check for non-zero coordinates (assuming 0,0 is invalid)
    non_zero = (df[lat_col] != 0) | (df[lon_col] != 0)
    
    # Filter to valid coordinates
    valid_coords = valid_lat & valid_lon & non_zero
    
    if not valid_coords.all():
        invalid_count = (~valid_coords).sum()
        print(f"Removing {invalid_count} records with invalid coordinates")
    
    return df[valid_coords]

def create_heatmap_data(signals_gdf, metric_column=None):
    """Create heatmap data for visualization"""
    
    if signals_gdf.empty:
        return []
    
    heatmap_data = []
    
    for _, signal in signals_gdf.iterrows():
        lat = signal.geometry.y
        lon = signal.geometry.x
        
        if metric_column and metric_column in signal:
            weight = signal[metric_column]
        else:
            weight = 1.0
        
        heatmap_data.append([lat, lon, weight])
    
    return heatmap_data

def get_corridor_centerline(tmc_data, corridor_name):
    """Get centerline geometry for a specific corridor"""
    
    corridor_tmcs = tmc_data[tmc_data['Corridor'] == corridor_name]
    
    if corridor_tmcs.empty:
        return None
    
    all_coords = []
    
    for _, tmc in corridor_tmcs.iterrows():
        coords_df = get_tmc_coords(tmc['coordinates'])
        if not coords_df.empty:
            coords_list = list(zip(coords_df['longitude'], coords_df['latitude']))
            all_coords.extend(coords_list)
    
    if len(all_coords) >= 2:
        return LineString(all_coords)
    else:
        return None

def calculate_signal_spacing(signals_gdf, corridor_name=None):
    """Calculate spacing between signals along a corridor"""
    
    if corridor_name:
        signals = signals_gdf[signals_gdf['Corridor'] == corridor_name].copy()
    else:
        signals = signals_gdf.copy()
    
    if len(signals) < 2:
        return pd.DataFrame()
    
    # Sort signals by latitude (assuming north-south corridor)
    # This is a simplification - would need proper corridor ordering
    signals = signals.sort_values('Latitude')
    
    spacing_data = []
    
    for i in range(len(signals) - 1):
        signal1 = signals.iloc[i]
        signal2 = signals.iloc[i + 1]
        
        # Calculate distance using Haversine formula
        from geopy.distance import geodesic
        
        point1 = (signal1.geometry.y, signal1.geometry.x)
        point2 = (signal2.geometry.y, signal2.geometry.x)
        
        distance_miles = geodesic(point1, point2).miles
        
        spacing_data.append({
            'from_signal': signal1['SignalID'],
            'to_signal': signal2['SignalID'],
            'from_location': signal1['Description'],
            'to_location': signal2['Description'],
            'distance_miles': distance_miles,
            'corridor': signal1.get('Corridor', 'Unknown')
        })
    
    return pd.DataFrame(spacing_data)

def create_route_analysis(signals_gdf, start_signal_id, end_signal_id):
    """Analyze route between two signals"""
    
    start_signal = signals_gdf[signals_gdf['SignalID'] == start_signal_id]
    end_signal = signals_gdf[signals_gdf['SignalID'] == end_signal_id]
    
    if start_signal.empty or end_signal.empty:
        return None
    
    start_point = (start_signal.iloc[0].geometry.y, start_signal.iloc[0].geometry.x)
    end_point = (end_signal.iloc[0].geometry.y, end_signal.iloc[0].geometry.x)
    
    from geopy.distance import geodesic
    total_distance = geodesic(start_point, end_point).miles
    
    # Find intermediate signals (simplified - would need proper routing)
    corridor = start_signal.iloc[0].get('Corridor')
    if corridor and corridor != 'None':
        corridor_signals = signals_gdf[signals_gdf['Corridor'] == corridor]
        
        # Get signals between start and end (by latitude)
        start_lat = start_signal.iloc[0].geometry.y
        end_lat = end_signal.iloc[0].geometry.y
        
        min_lat = min(start_lat, end_lat)
        max_lat = max(start_lat, end_lat)
        
        intermediate_signals = corridor_signals[
            (corridor_signals.geometry.y >= min_lat) & 
            (corridor_signals.geometry.y <= max_lat) &
            (corridor_signals['SignalID'] != start_signal_id) &
            (corridor_signals['SignalID'] != end_signal_id)
        ]
        
        return {
            'start_signal': start_signal_id,
            'end_signal': end_signal_id,
            'total_distance_miles': total_distance,
            'corridor': corridor,
            'intermediate_signals': intermediate_signals['SignalID'].tolist(),
            'total_signals': len(intermediate_signals) + 2
        }
    
    return {
        'start_signal': start_signal_id,
        'end_signal': end_signal_id,
        'total_distance_miles': total_distance,
        'corridor': 'Cross-corridor',
        'intermediate_signals': [],
        'total_signals': 2
    }

def create_network_analysis(signals_gdf, corridors_gdf):
    """Create network analysis of signal connectivity"""
    
    network_stats = {}
    
    # Overall network statistics
    network_stats['total_signals'] = len(signals_gdf)
    network_stats['total_corridors'] = signals_gdf['Corridor'].nunique()
    
    # Corridor statistics
    corridor_stats = signals_gdf.groupby('Corridor').agg({
        'SignalID': 'count',
        'Zone': 'first'
    }).rename(columns={'SignalID': 'signal_count'})
    
    # Calculate corridor densities if corridor geometries are available
    if not corridors_gdf.empty:
        corridor_lengths = corridors_gdf.groupby('Corridor').apply(
            lambda x: x.geometry.length.sum() * 69  # Convert degrees to approximate miles
        )
        
        corridor_stats['length_miles'] = corridor_lengths
        corridor_stats['signals_per_mile'] = corridor_stats['signal_count'] / corridor_stats['length_miles']
    
    network_stats['corridor_details'] = corridor_stats.to_dict('index')
    
    # Zone analysis
    zone_stats = signals_gdf.groupby('Zone').agg({
        'SignalID': 'count',
        'Corridor': 'nunique'
    }).rename(columns={'SignalID': 'signal_count', 'Corridor': 'corridor_count'})
    
    network_stats['zone_details'] = zone_stats.to_dict('index')
    
    return network_stats

def export_to_web_map(map_data, output_file='traffic_signals_map.html', include_heatmap=False):
    """Export complete interactive web map"""
    
    # Create the interactive map
    interactive_map = create_interactive_map(map_data)
    
    # Add heatmap layer if requested
    if include_heatmap and not map_data['signals_sp'].empty:
        from folium import plugins
        
        # Create heatmap data (uniform weights for demonstration)
        heat_data = create_heatmap_data(map_data['signals_sp'])
        
        # Add heatmap layer
        plugins.HeatMap(heat_data, name='Signal Density').add_to(interactive_map)
        
        # Add layer control
        folium.LayerControl().add_to(interactive_map)
    
    # Add additional controls
    from folium import plugins
    
    # Add fullscreen button
    plugins.Fullscreen().add_to(interactive_map)
    
    # Add measure control
    plugins.MeasureControl().add_to(interactive_map)
    
    # Add geocoder
    plugins.Geocoder().add_to(interactive_map)
    
    # Save map
    interactive_map.save(output_file)
    
    return interactive_map

def create_corridor_profile(signals_gdf, corridor_name, elevation_data=None):
    """Create a corridor profile showing signal locations and elevations"""
    
    corridor_signals = signals_gdf[signals_gdf['Corridor'] == corridor_name].copy()
    
    if corridor_signals.empty:
        return None
    
    # Sort signals along corridor (simplified by latitude)
    corridor_signals = corridor_signals.sort_values('Latitude')
    
    # Calculate cumulative distance
    from geopy.distance import geodesic
    
    distances = [0]
    for i in range(1, len(corridor_signals)):
        prev_point = (corridor_signals.iloc[i-1].geometry.y, corridor_signals.iloc[i-1].geometry.x)
        curr_point = (corridor_signals.iloc[i].geometry.y, corridor_signals.iloc[i].geometry.x)
        
        segment_distance = geodesic(prev_point, curr_point).miles
        distances.append(distances[-1] + segment_distance)
    
    corridor_signals['cumulative_distance'] = distances
    
    profile_data = {
        'corridor': corridor_name,
        'signals': corridor_signals[['SignalID', 'Description', 'cumulative_distance']].to_dict('records'),
        'total_length': distances[-1]
    }
    
    # Add elevation data if provided
    if elevation_data is not None:
        # This would integrate with elevation service/data
        profile_data['elevations'] = elevation_data
    
    return profile_data

def validate_map_data(map_data):
    """Validate map data for consistency and completeness"""
    
    validation_report = {
        'errors': [],
        'warnings': [],
        'info': []
    }
    
    # Check signals data
    signals_gdf = map_data.get('signals_sp')
    if signals_gdf is not None and not signals_gdf.empty:
        
        # Check for missing coordinates
        missing_coords = signals_gdf[
            signals_gdf.geometry.is_empty | 
            signals_gdf.geometry.isna()
        ]
        if not missing_coords.empty:
            validation_report['errors'].append(
                f"Found {len(missing_coords)} signals with missing coordinates"
            )
        
        # Check for duplicate signal IDs
        duplicate_ids = signals_gdf[signals_gdf['SignalID'].duplicated()]
        if not duplicate_ids.empty:
            validation_report['errors'].append(
                f"Found {len(duplicate_ids)} duplicate signal IDs"
            )
        
        # Check coordinate ranges (assuming Georgia/Atlanta area)
        out_of_range = signals_gdf[
            (signals_gdf.geometry.y < 30) | (signals_gdf.geometry.y > 36) |
            (signals_gdf.geometry.x < -88) | (signals_gdf.geometry.x > -80)
        ]
        if not out_of_range.empty:
            validation_report['warnings'].append(
                f"Found {len(out_of_range)} signals with coordinates outside expected range"
            )
        
        validation_report['info'].append(f"Total signals: {len(signals_gdf)}")
    else:
        validation_report['errors'].append("No signals data found")
    
    # Check corridors data
    corridors_gdf = map_data.get('corridors_sp')
    if corridors_gdf is not None and not corridors_gdf.empty:
        
        # Check for empty geometries
        empty_geoms = corridors_gdf[corridors_gdf.geometry.is_empty]
        if not empty_geoms.empty:
            validation_report['warnings'].append(
                f"Found {len(empty_geoms)} corridors with empty geometries"
            )
        
        validation_report['info'].append(f"Total corridors: {len(corridors_gdf)}")
    else:
        validation_report['warnings'].append("No corridors data found")
    
    # Check corridor color assignments
    corridor_colors = map_data.get('corridor_colors')
    if corridor_colors is not None and not corridor_colors.empty:
        
        if signals_gdf is not None:
            signal_corridors = set(signals_gdf['Corridor'].unique())
            color_corridors = set(corridor_colors['Corridor'].unique())
            
            missing_colors = signal_corridors - color_corridors
            if missing_colors:
                validation_report['warnings'].append(
                    f"Corridors without color assignments: {missing_colors}"
                )
    
    return validation_report

# Utility functions for coordinate transformations and projections

def convert_to_state_plane(gdf, state_plane_epsg=3857):
    """Convert GeoDataFrame to state plane coordinates"""
    return gdf.to_crs(f'EPSG:{state_plane_epsg}')

def convert_to_wgs84(gdf):
    """Convert GeoDataFrame to WGS84 (lat/lon)"""
    return gdf.to_crs('EPSG:4326')

def calculate_true_distances(gdf):
    """Calculate true distances using projected coordinates"""
    
    # Convert to appropriate projected CRS for accurate distance calculation
    gdf_projected = convert_to_state_plane(gdf)
    
    distances = []
    for i in range(len(gdf_projected) - 1):
        p1 = gdf_projected.iloc[i].geometry
        p2 = gdf_projected.iloc[i + 1].geometry
        
        if hasattr(p1, 'coords') and hasattr(p2, 'coords'):
            # Point to point distance
            dist = p1.distance(p2)
        else:
            # Use centroid for complex geometries
            dist = p1.centroid.distance(p2.centroid)
        
        distances.append(dist)
    
    return distances

# Main function to create all map components
def create_complete_map_package(conf, output_dir='map_output'):
    """Create complete map package with all components"""
    
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    print("Creating map data...")
    map_data = get_map_data(conf)
    
    print("Validating map data...")
    validation_report = validate_map_data(map_data)
    
    # Save validation report
    with open(os.path.join(output_dir, 'validation_report.json'), 'w') as f:
        json.dump(validation_report, f, indent=2)
    
    print("Creating interactive map...")
    interactive_map = export_to_web_map(
        map_data, 
        output_file=os.path.join(output_dir, 'interactive_map.html'),
        include_heatmap=True
    )
    
    print("Exporting spatial data...")
    # Export as GeoJSON
    export_map_data(
        map_data, 
        output_format='geojson',
        output_path=os.path.join(output_dir, 'map_data.geojson')
    )
    
    # Export as KML
    export_map_data(
        map_data,
        output_format='kml', 
        output_path=os.path.join(output_dir, 'map_data.kml')
    )
    
    print("Creating analysis reports...")
    # Network analysis
    if not map_data['signals_sp'].empty and not map_data['corridors_sp'].empty:
        network_analysis = create_network_analysis(
            map_data['signals_sp'], 
            map_data['corridors_sp']
        )
        
        with open(os.path.join(output_dir, 'network_analysis.json'), 'w') as f:
            json.dump(network_analysis, f, indent=2, default=str)
    
    # Corridor summary stats
    if not map_data['signals_sp'].empty:
        corridor_stats = create_corridor_summary_stats(
            map_data['signals_sp'],
            map_data['corridors_sp']
        )
        corridor_stats.to_csv(os.path.join(output_dir, 'corridor_stats.csv'), index=False)
    
    print(f"Map package created in: {output_dir}")
    
    return {
        'map_data': map_data,
        'validation_report': validation_report,
        'output_directory': output_dir
    }

if __name__ == "__main__":
    # Example usage
    import yaml
    
    # Load configuration
    with open('monthly_report.yaml', 'r') as f:
        conf = yaml.safe_load(f)
    
    # Create complete map package
    result = create_complete_map_package(conf)
    
    print("Map creation completed successfully!")