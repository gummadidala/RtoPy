"""
Teams Locations Processing - Python conversion of teams_locations.R
Downloads Teams Locations Report from TEAMS Application and processes spatial data
"""

import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import geodist
import logging
from typing import Dict, List, Any, Tuple, Optional
import warnings
import folium
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_teams_locations(filename: str = "TEAMS-Locations-MVID_2021-07-01.xlsx") -> pd.DataFrame:
    """
    Load and process TEAMS locations data from Excel file
    
    Args:
        filename: Path to TEAMS locations Excel file
        
    Returns:
        Processed DataFrame with location data
    """
    
    try:
        logger.info(f"Loading TEAMS locations from {filename}")
        
        # Read Excel file (equivalent to readxl::read_excel)
        locs = pd.read_excel(filename)
        
        # Rename columns to handle duplicate column names (equivalent to R rename)
        locs = locs.rename(columns={
            'Latitude...16': 'Latitude',
            'Longitude...17': 'Longitude', 
            'Custom Identifier...8': 'Custom Identifier'
        })
        
        # Replace NaN values with empty strings (equivalent to replace_na)
        na_columns = [
            'Route 1 Designation',
            'Route 1 Cardinality', 
            'Route 2 Designation',
            'Route 2 Cardinality'
        ]
        
        for col in na_columns:
            if col in locs.columns:
                locs[col] = locs[col].fillna("")
        
        # Create Route1 and Route2 columns (equivalent to mutate in R)
        locs['Route1'] = (
            locs['Route 1 Name'].astype(str) + ' ' + 
            locs['Route 1 Designation'].astype(str) + ' ' + 
            locs['Route 1 Cardinality'].astype(str)
        ).str.strip()
        
        locs['Route2'] = (
            locs['Route 2 Name'].astype(str) + ' ' + 
            locs['Route 2 Designation'].astype(str) + ' ' + 
            locs['Route 2 Cardinality'].astype(str)
        ).str.strip()
        
        # Select specific columns (equivalent to R column selection)
        columns_to_keep = [
            "DB Id", 
            "Custom Identifier", 
            "Route1", 
            "Route2", 
            "Assummed MVID", 
            "Latitude", 
            "Longitude"
        ]
        
        # Only keep columns that exist in the dataframe
        existing_columns = [col for col in columns_to_keep if col in locs.columns]
        locs = locs[existing_columns]
        
        # Fix longitude values (convert positive to negative - equivalent to R logic)
        longitude_mask = locs['Longitude'] > 1
        locs.loc[longitude_mask, 'Longitude'] = locs.loc[longitude_mask, 'Longitude'] * -1
        
        logger.info(f"Loaded {len(locs)} TEAMS locations")
        
        return locs
        
    except Exception as e:
        logger.error(f"Error loading TEAMS locations: {e}")
        return pd.DataFrame()

def perform_spatial_join_with_states(locs: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Perform spatial join with state polygons (equivalent to R spatial join)
    
    Args:
        locs: DataFrame with location data
        
    Returns:
        GeoDataFrame with spatial join results
    """
    
    try:
        logger.info("Performing spatial join with state boundaries")
        
        # Create points from coordinates (equivalent to st_as_sf in R)
        geometry = [Point(xy) for xy in zip(locs['Longitude'], locs['Latitude'])]
        teams_points = gpd.GeoDataFrame(locs, geometry=geometry, crs='EPSG:4326')
        
        # Load US states data (equivalent to map("state") in R)
        # Note: You'll need to have a states shapefile or use a different source
        # For this example, we'll create a simplified version
        
        # Alternative approach using built-in data or external source
        try:
            # Try to use naturalearth data if available
            import geopandas as gpd
            
            # You can download states data from:
            # https://www.naturalearthdata.com/downloads/110m-cultural-vectors/
            # or use: world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
            
            # For this example, we'll create Southeast states boundaries
            # In practice, you would load actual state boundary data
            se_states = ['Georgia', 'Alabama', 'Florida', 'South Carolina']
            
            # Create a mock states GeoDataFrame (replace with actual data loading)
            states_data = create_mock_states_data(se_states)
            
            # Perform spatial join (equivalent to st_join in R)
            joined_data = gpd.sjoin(teams_points, states_data, how='left', predicate='within')
            
            logger.info(f"Spatial join completed. {len(joined_data)} points processed")
            
            return joined_data
            
        except Exception as e:
            logger.warning(f"Could not perform spatial join: {e}")
            return teams_points
        
    except Exception as e:
        logger.error(f"Error in spatial join: {e}")
        return gpd.GeoDataFrame()

def create_mock_states_data(state_names: List[str]) -> gpd.GeoDataFrame:
    """
    Create mock state boundary data for demonstration
    In practice, you would load actual state boundary shapefiles
    
    Args:
        state_names: List of state names
        
    Returns:
        GeoDataFrame with mock state boundaries
    """
    
    try:
        # This is a simplified mock - in practice, load actual state boundaries
        # You can get state boundaries from sources like:
        # - US Census Bureau
        # - Natural Earth Data
        # - OpenStreetMap
        
        from shapely.geometry import Polygon
        
        # Mock boundaries (very simplified - replace with actual data)
        mock_states = []
        
        # These are approximate and simplified boundaries for demonstration
        state_bounds = {
            'Georgia': Polygon([(-85.6, 30.4), (-80.8, 30.4), (-80.8, 35.0), (-85.6, 35.0)]),
            'Alabama': Polygon([(-88.5, 30.2), (-84.9, 30.2), (-84.9, 35.0), (-88.5, 35.0)]),
            'Florida': Polygon([(-87.6, 24.5), (-80.0, 24.5), (-80.0, 31.0), (-87.6, 31.0)]),
            'South Carolina': Polygon([(-83.4, 32.0), (-78.5, 32.0), (-78.5, 35.2), (-83.4, 35.2)])
        }
        
        for state in state_names:
            if state in state_bounds:
                mock_states.append({
                    'ID': state.lower(),
                    'NAME': state,
                    'geometry': state_bounds[state]
                })
        
        return gpd.GeoDataFrame(mock_states, crs='EPSG:4326')
        
    except Exception as e:
        logger.error(f"Error creating mock states data: {e}")
        return gpd.GeoDataFrame()

def load_sigops_corridors(filename: str = "SigOpsRegions_20210628_Edited.xlsx") -> pd.DataFrame:
    """
    Load SigOps corridors file
    
    Args:
        filename: Path to SigOps regions Excel file
        
    Returns:
        DataFrame with SigOps corridor data
    """
    
    try:
        logger.info(f"Loading SigOps corridors from {filename}")
        
        # Read sigops corridors file from Excel (equivalent to readxl::read_excel)
        sigops = pd.read_excel(filename)
        
        logger.info(f"Loaded {len(sigops)} SigOps corridors")
        
        return sigops
        
    except Exception as e:
        logger.error(f"Error loading SigOps corridors: {e}")
        return pd.DataFrame()

def calculate_distance_matrix(locs: pd.DataFrame, sigops: pd.DataFrame) -> np.ndarray:
    """
    Calculate pairwise distance matrix between locations and sigops
    
    Args:
        locs: TEAMS locations DataFrame
        sigops: SigOps corridors DataFrame
        
    Returns:
        Distance matrix as numpy array
    """
    
    try:
        logger.info("Calculating pairwise distance matrix")
        
        # Prepare coordinate arrays for distance calculation
        locs_coords = locs[['Longitude', 'Latitude']].values
        sigops_coords = sigops[['Longitude', 'Latitude']].values
        
        # Calculate geodesic distances (equivalent to geodist::geodist in R)
        # Note: geodist package needs to be installed: pip install geodist
        
        try:
            import geodist
            # Calculate distance matrix in meters
            dists = geodist.distance_matrix(locs_coords, sigops_coords)
            
        except ImportError:
            logger.warning("geodist package not available, using alternative distance calculation")
            # Alternative using geopy or manual calculation
            dists = calculate_haversine_distance_matrix(locs_coords, sigops_coords)
        
        # Replace NaN values with large number (equivalent to R: dists[is.na(dists)] = 999)
        dists = np.nan_to_num(dists, nan=999000)  # 999 km in meters
        
        logger.info(f"Distance matrix calculated: {dists.shape}")
        
        return dists
        
    except Exception as e:
        logger.error(f"Error calculating distance matrix: {e}")
        return np.array([])

def calculate_haversine_distance_matrix(coords1: np.ndarray, coords2: np.ndarray) -> np.ndarray:
    """
    Calculate haversine distance matrix between two sets of coordinates
    
    Args:
        coords1: First set of coordinates (lon, lat)
        coords2: Second set of coordinates (lon, lat)
        
    Returns:
        Distance matrix in meters
    """
    
    try:
        from math import radians, cos, sin, asin, sqrt
        
        def haversine_distance(lon1, lat1, lon2, lat2):
            """Calculate the great circle distance between two points in meters"""
            # Convert decimal degrees to radians
            lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
            
            # Haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * asin(sqrt(a))
            r = 6371000  # Radius of earth in meters
            return c * r
        
        # Create distance matrix
        n_locs = len(coords1)
        n_sigops = len(coords2)
        dists = np.zeros((n_locs, n_sigops))
        
        for i in range(n_locs):
            for j in range(n_sigops):
                dists[i, j] = haversine_distance(
                    coords1[i, 0], coords1[i, 1],  # lon1, lat1
                    coords2[j, 0], coords2[j, 1]   # lon2, lat2
                )
        
        return dists
        
    except Exception as e:
        logger.error(f"Error in haversine distance calculation: {e}")
        return np.array([])

def find_minimum_distances(dists: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Find minimum distances and corresponding indices
    
    Args:
        dists: Distance matrix
        
    Returns:
        Tuple of (minimum indices, minimum distances)
    """
    
    try:
        # Find indices of minimum distances (equivalent to apply(dists, 1, which.min))
        min_indices = np.argmin(dists, axis=1)
        
        # Get minimum distances (equivalent to apply(dists, 1, min))
        min_distances = np.min(dists, axis=1)
        
        return min_indices, min_distances
        
    except Exception as e:
        logger.error(f"Error finding minimum distances: {e}")
        return np.array([]), np.array([])

def merge_locations_with_sigops(locs: pd.DataFrame, sigops: pd.DataFrame, 
                               min_indices: np.ndarray, min_distances: np.ndarray) -> pd.DataFrame:
    """
    Merge locations with closest SigOps corridors
    
    Args:
        locs: TEAMS locations DataFrame
        sigops: SigOps corridors DataFrame
        min_indices: Indices of closest SigOps for each location
        min_distances: Minimum distances for each location
        
    Returns:
        Merged DataFrame with locations and closest SigOps
    """
    
    try:
        logger.info("Merging locations with SigOps data")
        
        # Add distance column to locations
        locs_with_dist = locs.copy()
        locs_with_dist['dist'] = min_distances
        
        # Rename latitude/longitude columns to avoid conflicts
        locs_with_dist = locs_with_dist.rename(columns={
            'Latitude': 'Latitude_teams',
            'Longitude': 'Longitude_teams'
        })
        
        # Get corresponding SigOps data for minimum distances
        closest_sigops = sigops.iloc[min_indices].reset_index(drop=True)
        
        # Combine dataframes (equivalent to dplyr::bind_cols)
        locations = pd.concat([locs_with_dist, closest_sigops], axis=1)
        
        # Sort by MVID and distance (equivalent to arrange in R)
        if 'Assummed MVID' in locations.columns:
            locations = locations.sort_values(['Assummed MVID', 'dist'])
        else:
            locations = locations.sort_values('dist')
        
        logger.info(f"Merged {len(locations)} location records")
        
        return locations
        
    except Exception as e:
        logger.error(f"Error merging locations with SigOps: {e}")
        return pd.DataFrame()

def create_location_summary(locations: pd.DataFrame) -> pd.DataFrame:
    """
    Create summary statistics by MVID
    
    Args:
        locations: Merged locations DataFrame
        
    Returns:
        Summary DataFrame grouped by MVID
    """
    
    try:
        logger.info("Creating location summary statistics")
        
        # Group by MVID and calculate summary stats (equivalent to R group_by + summarize)
        mvid_col = 'Assumed MVID' if 'Assumed MVID' in locations.columns else 'Assummed MVID'
        
        if mvid_col not in locations.columns:
            logger.warning("MVID column not found, skipping summary")
            return pd.DataFrame()
        
        # Convert MVID to integer (equivalent to as.integer in R)
        locations['MVID'] = pd.to_numeric(locations[mvid_col], errors='coerce').astype('Int64')
        
        # Group by MVID and summarize (equivalent to R dplyr operations)
        summary = locations.groupby('MVID').agg({
            'dist': ['count', 'min', 'max']
        }).reset_index()
        
        # Flatten column names
        summary.columns = ['MVID', 'n', 'mindist', 'maxdist']
        
        logger.info(f"Created summary for {len(summary)} MVIDs")
        
        return summary
        
    except Exception as e:
        logger.error(f"Error creating location summary: {e}")
        return pd.DataFrame()

def merge_sigops_with_summary(sigops: pd.DataFrame, summary: pd.DataFrame) -> pd.DataFrame:
    """
    Merge SigOps data with location summary
    
    Args:
        sigops: SigOps corridors DataFrame
        summary: Location summary DataFrame
        
    Returns:
        Merged SigOps DataFrame with summary statistics
    """
    
    try:
        logger.info("Merging SigOps with summary statistics")
        
        # Convert MVID to integer in sigops (equivalent to mutate in R)
        sigops_copy = sigops.copy()
        sigops_copy['MVID'] = pd.to_numeric(sigops_copy['MVID'], errors='coerce').astype('Int64')
        
        # Left join with summary (equivalent to left_join in R)
        sigops2 = pd.merge(sigops_copy, summary, on='MVID', how='left')
        
        logger.info(f"Merged SigOps data: {len(sigops2)} records")
        
        return sigops2
        
    except Exception as e:
        logger.error(f"Error merging SigOps with summary: {e}")
        return pd.DataFrame()

def create_leaflet_map(locations: pd.DataFrame, output_file: str = "teams_locations_map.html") -> str:
    """
    Create interactive map with location data (equivalent to leaflet in R)
    
    Args:
        locations: Locations DataFrame with coordinates and distance data
        output_file: Output HTML file path
        
    Returns:
        Path to generated HTML file
    """
    
    try:
        logger.info("Creating interactive map")
        
        if locations.empty:
            logger.warning("No location data available for mapping")
            return ""
        
        # Get coordinate columns
        lat_col = 'Latitude_teams'
        lon_col = 'Longitude_teams'
        
        if lat_col not in locations.columns or lon_col not in locations.columns:
            logger.error("Required coordinate columns not found")
            return ""
        
        # Remove rows with missing coordinates
        map_data = locations.dropna(subset=[lat_col, lon_col])
        
        if map_data.empty:
            logger.warning("No valid coordinates found for mapping")
            return ""
        
        # Calculate map center
        center_lat = map_data[lat_col].mean()
        center_lon = map_data[lon_col].mean()
        
        # Create base map (equivalent to leaflet() %>% addProviderTiles())
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=8,
            tiles='CartoDB positron'  # Equivalent to CartoDB.Positron in R
        )
        
        # Normalize distance values for color mapping
        if 'dist' in map_data.columns:
            dist_values = map_data['dist'].values
            min_dist = np.min(dist_values)
            max_dist = np.max(dist_values)
            
            # Create color map based on distance
            def get_color(distance):
                if max_dist > min_dist:
                    normalized = (distance - min_dist) / (max_dist - min_dist)
                    # Use a color scale from green (close) to red (far)
                    if normalized < 0.33:
                        return 'green'
                    elif normalized < 0.66:
                        return 'orange'
                    else:
                        return 'red'
                else:
                    return 'blue'
        else:
            def get_color(distance):
                return 'blue'
        
        # Add circle markers (equivalent to addCircleMarkers in R)
        for idx, row in map_data.iterrows():
            lat = row[lat_col]
            lon = row[lon_col]
            dist = row.get('dist', 0)
            
            # Create popup text
            popup_text = f"""
            <b>Location:</b> {row.get('Custom Identifier', 'Unknown')}<br>
            <b>Distance:</b> {dist:.0f}m<br>
            <b>Route 1:</b> {row.get('Route1', 'N/A')}<br>
            <b>Route 2:</b> {row.get('Route2', 'N/A')}<br>
            <b>MVID:</b> {row.get('Assummed MVID', 'N/A')}
            """
            
            folium.CircleMarker(
                location=[lat, lon],
                radius=5,
                color=get_color(dist),
                weight=1,
                opacity=0.8,
                fillColor=get_color(dist),
                fillOpacity=0.6,
                popup=folium.Popup(popup_text, max_width=300)
            ).add_to(m)
        
        # Add color legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 150px; height: 90px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><b>Distance from SigOps</b></p>
        <p><span style="color:green;">●</span> Close</p>
        <p><span style="color:orange;">●</span> Medium</p>
        <p><span style="color:red;">●</span> Far</p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        m.save(output_file)
        logger.info(f"Interactive map saved to {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error creating leaflet map: {e}")
        return ""

def save_output_files(locations: pd.DataFrame, sigops2: pd.DataFrame, 
                     locations_file: str = "teams_locations2.csv",
                     sigops_file: str = "sigops2.csv") -> bool:
    """
    Save processed data to CSV files
    
    Args:
        locations: Processed locations DataFrame
        sigops2: Processed SigOps DataFrame
        locations_file: Output file for locations
        sigops_file: Output file for SigOps
        
    Returns:
        Boolean indicating success
    """
    
    try:
        # Save locations data (equivalent to readr::write_csv in R)
        locations.to_csv(locations_file, index=False)
        logger.info(f"Locations data saved to {locations_file}")
        
        # Save SigOps data
        sigops2.to_csv(sigops_file, index=False)
        logger.info(f"SigOps data saved to {sigops_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error saving output files: {e}")
        return False

def validate_coordinates(df: pd.DataFrame, lat_col: str = 'Latitude', 
                        lon_col: str = 'Longitude') -> pd.DataFrame:
    """
    Validate and clean coordinate data
    
    Args:
        df: DataFrame with coordinate data
        lat_col: Latitude column name
        lon_col: Longitude column name
        
    Returns:
        DataFrame with validated coordinates
    """
    
    try:
        df_clean = df.copy()
        
        # Check for valid latitude range (-90 to 90)
        if lat_col in df_clean.columns:
            invalid_lat = (df_clean[lat_col] < -90) | (df_clean[lat_col] > 90)
            if invalid_lat.any():
                logger.warning(f"Found {invalid_lat.sum()} invalid latitude values")
                df_clean.loc[invalid_lat, lat_col] = np.nan
        
        # Check for valid longitude range (-180 to 180)
        if lon_col in df_clean.columns:
            invalid_lon = (df_clean[lon_col] < -180) | (df_clean[lon_col] > 180)
            if invalid_lon.any():
                logger.warning(f"Found {invalid_lon.sum()} invalid longitude values")
                df_clean.loc[invalid_lon, lon_col] = np.nan
        
        # Remove rows with missing coordinates
        initial_count = len(df_clean)
        df_clean = df_clean.dropna(subset=[lat_col, lon_col])
        final_count = len(df_clean)
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} rows with missing coordinates")
        
        return df_clean
        
    except Exception as e:
        logger.error(f"Error validating coordinates: {e}")
        return df

def generate_summary_report(locations: pd.DataFrame, sigops2: pd.DataFrame, 
                          output_file: str = "teams_locations_summary.txt") -> bool:
    """
    Generate a summary report of the processing results
    
    Args:
        locations: Processed locations DataFrame
        sigops2: Processed SigOps DataFrame
        output_file: Output file path
        
    Returns:
        Boolean indicating success
    """
    
    try:
        with open(output_file, 'w') as f:
            f.write("TEAMS LOCATIONS PROCESSING SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Locations summary
            f.write("LOCATIONS DATA:\n")
            f.write(f"Total locations processed: {len(locations)}\n")
            
            if 'dist' in locations.columns:
                f.write(f"Average distance to SigOps: {locations['dist'].mean():.0f}m\n")
                f.write(f"Minimum distance: {locations['dist'].min():.0f}m\n")
                f.write(f"Maximum distance: {locations['dist'].max():.0f}m\n")
            
            if 'Assummed MVID' in locations.columns:
                unique_mvids = locations['Assummed MVID'].nunique()
                f.write(f"Unique MVIDs: {unique_mvids}\n")
            
            f.write("\nSIGOPS DATA:\n")
            f.write(f"Total SigOps corridors: {len(sigops2)}\n")
            
            if 'n' in sigops2.columns:
                matched_sigops = sigops2['n'].notna().sum()
                f.write(f"SigOps with matched locations: {matched_sigops}\n")
                
                if matched_sigops > 0:
                    avg_locations_per_sigops = sigops2['n'].mean()
                    f.write(f"Average locations per SigOps: {avg_locations_per_sigops:.1f}\n")
            
            # Data quality metrics
            f.write("\nDATA QUALITY:\n")
            
            # Check for missing coordinates
            if 'Latitude_teams' in locations.columns and 'Longitude_teams' in locations.columns:
                missing_coords = locations[['Latitude_teams', 'Longitude_teams']].isna().any(axis=1).sum()
                f.write(f"Locations with missing coordinates: {missing_coords}\n")
            
            # Check coordinate ranges
            if 'Latitude_teams' in locations.columns:
                lat_range = f"{locations['Latitude_teams'].min():.4f} to {locations['Latitude_teams'].max():.4f}"
                f.write(f"Latitude range: {lat_range}\n")
            
            if 'Longitude_teams' in locations.columns:
                lon_range = f"{locations['Longitude_teams'].min():.4f} to {locations['Longitude_teams'].max():.4f}"
                f.write(f"Longitude range: {lon_range}\n")
            
            f.write("\nPROCESSING COMPLETE\n")
        
        logger.info(f"Summary report saved to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error generating summary report: {e}")
        return False

def main(teams_file: str = "TEAMS-Locations-MVID_2021-07-01.xlsx",
         sigops_file: str = "SigOpsRegions_20210628_Edited.xlsx") -> Dict[str, Any]:
    """
    Main function that replicates the complete R script functionality
    
    Args:
        teams_file: Path to TEAMS locations Excel file
        sigops_file: Path to SigOps regions Excel file
        
    Returns:
        Dictionary with processing results and status
    """
    
    try:
        logger.info("Starting TEAMS locations processing")
        
        results = {
            'status': 'success',
            'files_created': [],
            'summary': {}
        }
        
        # Step 1: Load TEAMS locations data
        locs = load_teams_locations(teams_file)
        if locs.empty:
            raise ValueError("Failed to load TEAMS locations data")
        
        # Validate coordinates
        locs = validate_coordinates(locs)
        
        # Step 2: Perform spatial join with states (equivalent to R spatial operations)
        spatial_result = perform_spatial_join_with_states(locs)
        logger.info("Spatial join with states completed")
        
        # Step 3: Load SigOps corridors data
        sigops = load_sigops_corridors(sigops_file)
        if sigops.empty:
            raise ValueError("Failed to load SigOps corridors data")
        
        # Validate SigOps coordinates
        sigops = validate_coordinates(sigops)
        
        # Step 4: Calculate pairwise distance matrix
        dists = calculate_distance_matrix(locs, sigops)
        if dists.size == 0:
            raise ValueError("Failed to calculate distance matrix")
        
        # Step 5: Find minimum distances and indices
        min_indices, min_distances = find_minimum_distances(dists)
        
        # Step 6: Merge locations with closest SigOps
        locations = merge_locations_with_sigops(locs, sigops, min_indices, min_distances)
        if locations.empty:
            raise ValueError("Failed to merge locations with SigOps data")
        
        # Step 7: Create location summary by MVID
        summary = create_location_summary(locations)
        
        # Step 8: Merge SigOps with summary statistics
        sigops2 = merge_sigops_with_summary(sigops, summary)
        
        # Step 9: Save output files (equivalent to readr::write_csv in R)
        if save_output_files(locations, sigops2):
            results['files_created'].extend(['teams_locations2.csv', 'sigops2.csv'])
        
        # Step 10: Create interactive map (equivalent to leaflet in R)
        map_file = create_leaflet_map(locations)
        if map_file:
            results['files_created'].append(map_file)
        
        # Step 11: Generate summary report
        if generate_summary_report(locations, sigops2):
            results['files_created'].append('teams_locations_summary.txt')
        
        # Compile summary statistics
        results['summary'] = {
            'total_locations': len(locations),
            'total_sigops': len(sigops2),
            'unique_mvids': locations['Assummed MVID'].nunique() if 'Assummed MVID' in locations.columns else 0,
            'average_distance': float(locations['dist'].mean()) if 'dist' in locations.columns else 0,
            'coordinate_range': {
                'lat_min': float(locations['Latitude_teams'].min()) if 'Latitude_teams' in locations.columns else None,
                'lat_max': float(locations['Latitude_teams'].max()) if 'Latitude_teams' in locations.columns else None,
                'lon_min': float(locations['Longitude_teams'].min()) if 'Longitude_teams' in locations.columns else None,
                'lon_max': float(locations['Longitude_teams'].max()) if 'Longitude_teams' in locations.columns else None
            }
        }
        
        logger.info("TEAMS locations processing completed successfully")
        logger.info(f"Files created: {', '.join(results['files_created'])}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        return {
            'status': 'error',
            'error': str(e),
            'files_created': [],
            'summary': {}
        }

def load_from_s3(bucket: str, key: str, file_type: str = 'excel') -> pd.DataFrame:
    """
    Load data files from S3 (utility function for cloud deployment)
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        file_type: Type of file ('excel', 'csv', 'parquet')
        
    Returns:
        DataFrame with loaded data
    """
    
    try:
        import boto3
        import io
        
        s3_client = boto3.client('s3')
        
        # Download file from S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        
        # Load based on file type
        if file_type.lower() == 'excel':
            df = pd.read_excel(io.BytesIO(file_content))
        elif file_type.lower() == 'csv':
            df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
        elif file_type.lower() == 'parquet':
            df = pd.read_parquet(io.BytesIO(file_content))
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        logger.info(f"Loaded {len(df)} rows from s3://{bucket}/{key}")
        return df
        
    except Exception as e:
        logger.error(f"Error loading from S3: {e}")
        return pd.DataFrame()

def save_to_s3(df: pd.DataFrame, bucket: str, key: str, file_type: str = 'csv') -> bool:
    """
    Save DataFrame to S3 (utility function for cloud deployment)
    
    Args:
        df: DataFrame to save
        bucket: S3 bucket name
        key: S3 object key
        file_type: Type of file ('csv', 'parquet')
        
    Returns:
        Boolean indicating success
    """
    
    try:
        import boto3
        import io
        
        s3_client = boto3.client('s3')
        
        # Prepare file content
        if file_type.lower() == 'csv':
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            content = buffer.getvalue().encode('utf-8')
        elif file_type.lower() == 'parquet':
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            content = buffer.getvalue()
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        # Upload to S3
        s3_client.put_object(Bucket=bucket, Key=key, Body=content)
        logger.info(f"Saved to s3://{bucket}/{key}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error saving to S3: {e}")
        return False

def create_advanced_map(locations: pd.DataFrame, sigops: pd.DataFrame, 
                       output_file: str = "advanced_teams_map.html") -> str:
    """
    Create advanced interactive map with both locations and SigOps corridors
    
    Args:
        locations: Locations DataFrame
        sigops: SigOps DataFrame
        output_file: Output HTML file path
        
    Returns:
        Path to generated HTML file
    """
    
    try:
        logger.info("Creating advanced interactive map")
        
        if locations.empty and sigops.empty:
            logger.warning("No data available for mapping")
            return ""
        
        # Calculate map center from available data
        all_lats = []
        all_lons = []
        
        if 'Latitude_teams' in locations.columns:
            all_lats.extend(locations['Latitude_teams'].dropna().tolist())
        if 'Longitude_teams' in locations.columns:
            all_lons.extend(locations['Longitude_teams'].dropna().tolist())
        if 'Latitude' in sigops.columns:
            all_lats.extend(sigops['Latitude'].dropna().tolist())
        if 'Longitude' in sigops.columns:
            all_lons.extend(sigops['Longitude'].dropna().tolist())
        
        if not all_lats or not all_lons:
            logger.error("No valid coordinates found for mapping")
            return ""
        
        center_lat = np.mean(all_lats)
        center_lon = np.mean(all_lons)
        
        # Create base map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=8,
            tiles='CartoDB positron'
        )
        
        # Add TEAMS locations
        if not locations.empty and 'Latitude_teams' in locations.columns:
            teams_group = folium.FeatureGroup(name="TEAMS Locations")
            
            for idx, row in locations.iterrows():
                if pd.notna(row.get('Latitude_teams')) and pd.notna(row.get('Longitude_teams')):
                    popup_text = f"""
                    <b>TEAMS Location</b><br>
                    <b>ID:</b> {row.get('Custom Identifier', 'Unknown')}<br>
                    <b>Distance to SigOps:</b> {row.get('dist', 0):.0f}m<br>
                    <b>Route 1:</b> {row.get('Route1', 'N/A')}<br>
                    <b>Route 2:</b> {row.get('Route2', 'N/A')}<br>
                    <b>MVID:</b> {row.get('Assummed MVID', 'N/A')}
                    """
                    
                    folium.CircleMarker(
                        location=[row['Latitude_teams'], row['Longitude_teams']],
                        radius=6,
                        color='blue',
                        weight=2,
                        opacity=0.8,
                        fillColor='lightblue',
                        fillOpacity=0.6,
                        popup=folium.Popup(popup_text, max_width=300)
                    ).add_to(teams_group)
            
            teams_group.add_to(m)
        
        # Add SigOps corridors
        if not sigops.empty and 'Latitude' in sigops.columns:
            sigops_group = folium.FeatureGroup(name="SigOps Corridors")
            
            for idx, row in sigops.iterrows():
                if pd.notna(row.get('Latitude')) and pd.notna(row.get('Longitude')):
                    popup_text = f"""
                    <b>SigOps Corridor</b><br>
                    <b>MVID:</b> {row.get('MVID', 'Unknown')}<br>
                    <b>Matched Locations:</b> {row.get('n', 0)}<br>
                    <b>Min Distance:</b> {row.get('mindist', 0):.0f}m<br>
                    <b>Max Distance:</b> {row.get('maxdist', 0):.0f}m
                    """
                    
                    folium.Marker(
                        location=[row['Latitude'], row['Longitude']],
                        icon=folium.Icon(color='red', icon='road', prefix='fa'),
                        popup=folium.Popup(popup_text, max_width=300)
                    ).add_to(sigops_group)
            
            sigops_group.add_to(m)
        
        # Add layer control
        folium.LayerControl().add_to(m)
        
        # Add custom legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 120px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <p><b>Legend</b></p>
        <p><span style="color:blue;">●</span> TEAMS Locations</p>
        <p><span style="color:red;">📍</span> SigOps Corridors</p>
        <p><i>Click markers for details</i></p>
        </div>
        '''
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Save map
        m.save(output_file)
        logger.info(f"Advanced interactive map saved to {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"Error creating advanced map: {e}")
        return ""

def analyze_distance_patterns(locations: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze distance patterns between TEAMS locations and SigOps corridors
    
    Args:
        locations: Locations DataFrame with distance data
        
    Returns:
        Dictionary with distance analysis results
    """
    
    try:
        if 'dist' not in locations.columns:
            return {'error': 'Distance data not available'}
        
        distances = locations['dist'].dropna()
        
        if distances.empty:
            return {'error': 'No valid distance data'}
        
        analysis = {
            'total_locations': len(distances),
            'distance_stats': {
                'mean': float(distances.mean()),
                'median': float(distances.median()),
                'std': float(distances.std()),
                'min': float(distances.min()),
                'max': float(distances.max()),
                'q25': float(distances.quantile(0.25)),
                'q75': float(distances.quantile(0.75))
            },
            'distance_ranges': {
                'very_close': len(distances[distances <= 500]),  # <= 500m
                'close': len(distances[(distances > 500) & (distances <= 1000)]),  # 500m-1km
                'medium': len(distances[(distances > 1000) & (distances <= 5000)]),  # 1-5km
                'far': len(distances[distances > 5000])  # > 5km
            }
        }
        
        # Calculate percentages
        total = len(distances)
        for range_name, count in analysis['distance_ranges'].items():
            analysis['distance_ranges'][f'{range_name}_pct'] = round((count / total) * 100, 1)
        
        # Identify outliers (locations very far from any SigOps)
        q75 = distances.quantile(0.75)
        q25 = distances.quantile(0.25)
        iqr = q75 - q25
        outlier_threshold = q75 + 1.5 * iqr
        
        outliers = locations[locations['dist'] > outlier_threshold]
        analysis['outliers'] = {
            'count': len(outliers),
            'threshold': float(outlier_threshold),
            'locations': outliers[['Custom Identifier', 'dist', 'Route1', 'Route2']].to_dict('records') if len(outliers) < 20 else []
        }
        
        logger.info(f"Distance analysis completed for {len(distances)} locations")
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error in distance analysis: {e}")
        return {'error': str(e)}

def create_distance_histogram(locations: pd.DataFrame, output_file: str = "distance_histogram.png") -> bool:
    """
    Create histogram of distances between TEAMS locations and SigOps corridors
    
    Args:
        locations: Locations DataFrame with distance data
        output_file: Output image file path
        
    Returns:
        Boolean indicating success
    """
    
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        if 'dist' not in locations.columns:
            logger.error("Distance data not available for histogram")
            return False
        
        distances = locations['dist'].dropna()
        
        if distances.empty:
            logger.error("No valid distance data for histogram")
            return False
        
        # Create figure
        plt.figure(figsize=(10, 6))
        
        # Create histogram
        plt.hist(distances, bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        
        # Add statistics lines
        plt.axvline(distances.mean(), color='red', linestyle='--', 
                   label=f'Mean: {distances.mean():.0f}m')
        plt.axvline(distances.median(), color='green', linestyle='--', 
                   label=f'Median: {distances.median():.0f}m')
        
        # Formatting
        plt.xlabel('Distance to Nearest SigOps Corridor (meters)')
        plt.ylabel('Number of Locations')
        plt.title('Distribution of Distances: TEAMS Locations to SigOps Corridors')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Save plot
        plt.tight_layout()
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Distance histogram saved to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error creating distance histogram: {e}")
        return False

def export_to_geojson(locations: pd.DataFrame, sigops: pd.DataFrame, 
                     locations_file: str = "teams_locations.geojson",
                     sigops_file: str = "sigops_corridors.geojson") -> bool:
    """
    Export data to GeoJSON format for use in GIS applications
    
    Args:
        locations: Locations DataFrame
        sigops: SigOps DataFrame
        locations_file: Output file for locations GeoJSON
        sigops_file: Output file for SigOps GeoJSON
        
    Returns:
        Boolean indicating success
    """
    
    try:
        # Export TEAMS locations
        if not locations.empty and 'Latitude_teams' in locations.columns:
            locations_clean = locations.dropna(subset=['Latitude_teams', 'Longitude_teams'])
            
            if not locations_clean.empty:
                geometry = [Point(xy) for xy in zip(locations_clean['Longitude_teams'], 
                                                   locations_clean['Latitude_teams'])]
                locations_gdf = gpd.GeoDataFrame(locations_clean, geometry=geometry, crs='EPSG:4326')
                locations_gdf.to_file(locations_file, driver='GeoJSON')
                logger.info(f"TEAMS locations exported to {locations_file}")
        
        # Export SigOps corridors
        if not sigops.empty and 'Latitude' in sigops.columns:
            sigops_clean = sigops.dropna(subset=['Latitude', 'Longitude'])
            
            if not sigops_clean.empty:
                geometry = [Point(xy) for xy in zip(sigops_clean['Longitude'], 
                                                   sigops_clean['Latitude'])]
                sigops_gdf = gpd.GeoDataFrame(sigops_clean, geometry=geometry, crs='EPSG:4326')
                sigops_gdf.to_file(sigops_file, driver='GeoJSON')
                logger.info(f"SigOps corridors exported to {sigops_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error exporting to GeoJSON: {e}")
        return False

def create_summary_statistics_table(locations: pd.DataFrame) -> pd.DataFrame:
    """
    Create detailed summary statistics table by MVID and Route
    
    Args:
        locations: Locations DataFrame
        
    Returns:
        Summary statistics DataFrame
    """
    
    try:
        if locations.empty:
            return pd.DataFrame()
        
        # Group by MVID
        mvid_summary = locations.groupby('Assummed MVID').agg({
            'dist': ['count', 'mean', 'min', 'max', 'std'],
            'Custom Identifier': 'count',
            'Route1': lambda x: x.mode().iloc[0] if not x.mode().empty else 'N/A',
            'Route2': lambda x: x.mode().iloc[0] if not x.mode().empty else 'N/A'
        }).round(2)
        
        # Flatten column names
        mvid_summary.columns = [
            'location_count', 'avg_distance', 'min_distance', 'max_distance', 'std_distance',
            'identifier_count', 'primary_route1', 'primary_route2'
        ]
        
        mvid_summary = mvid_summary.reset_index()
        
        # Add distance categories
        mvid_summary['distance_category'] = pd.cut(
            mvid_summary['avg_distance'],
            bins=[0, 500, 1000, 5000, float('inf')],
            labels=['Very Close', 'Close', 'Medium', 'Far']
        )
        
        logger.info(f"Created summary statistics for {len(mvid_summary)} MVIDs")
        
        return mvid_summary
        
    except Exception as e:
        logger.error(f"Error creating summary statistics table: {e}")
        return pd.DataFrame()

def validate_data_quality(locations: pd.DataFrame, sigops: pd.DataFrame) -> Dict[str, Any]:
    """
    Comprehensive data quality validation
    
    Args:
        locations: Locations DataFrame
        sigops: SigOps DataFrame
        
    Returns:
        Dictionary with data quality metrics
    """
    
    try:
        quality_report = {
            'timestamp': pd.Timestamp.now().isoformat(),
            'locations': {},
            'sigops': {},
            'overall_score': 0
        }
        
        # Validate locations data
        if not locations.empty:
            loc_quality = {
                'total_records': len(locations),
                'missing_coordinates': 0,
                'invalid_coordinates': 0,
                'missing_identifiers': 0,
                'duplicate_records': 0,
                'completeness_score': 0
            }
            
            # Check coordinates
            if 'Latitude_teams' in locations.columns and 'Longitude_teams' in locations.columns:
                missing_coords = locations[['Latitude_teams', 'Longitude_teams']].isna().any(axis=1).sum()
                loc_quality['missing_coordinates'] = missing_coords
                
                # Check coordinate validity
                valid_lat = locations['Latitude_teams'].between(-90, 90, na=False)
                valid_lon = locations['Longitude_teams'].between(-180, 180, na=False)
                invalid_coords = (~(valid_lat & valid_lon)).sum()
                loc_quality['invalid_coordinates'] = invalid_coords
            
            # Check identifiers
            if 'Custom Identifier' in locations.columns:
                missing_ids = locations['Custom Identifier'].isna().sum()
                loc_quality['missing_identifiers'] = missing_ids
            
            # Check duplicates
            if 'DB Id' in locations.columns:
                duplicates = locations['DB Id'].duplicated().sum()
                loc_quality['duplicate_records'] = duplicates
            
            # Calculate completeness score
            total_issues = (loc_quality['missing_coordinates'] + 
                          loc_quality['invalid_coordinates'] + 
                          loc_quality['missing_identifiers'] + 
                          loc_quality['duplicate_records'])
            
            if len(locations) > 0:
                loc_quality['completeness_score'] = max(0, 100 - (total_issues / len(locations) * 100))
            
            quality_report['locations'] = loc_quality
        
        # Validate SigOps data
        if not sigops.empty:
            sigops_quality = {
                'total_records': len(sigops),
                'missing_coordinates': 0,
                'invalid_coordinates': 0,
                'missing_mvids': 0,
                'completeness_score': 0
            }
            
            # Check coordinates
            if 'Latitude' in sigops.columns and 'Longitude' in sigops.columns:
                missing_coords = sigops[['Latitude', 'Longitude']].isna().any(axis=1).sum()
                sigops_quality['missing_coordinates'] = missing_coords
                
                # Check coordinate validity
                valid_lat = sigops['Latitude'].between(-90, 90, na=False)
                valid_lon = sigops['Longitude'].between(-180, 180, na=False)
                invalid_coords = (~(valid_lat & valid_lon)).sum()
                sigops_quality['invalid_coordinates'] = invalid_coords
            
            # Check MVIDs
            if 'MVID' in sigops.columns:
                missing_mvids = sigops['MVID'].isna().sum()
                sigops_quality['missing_mvids'] = missing_mvids
            
            # Calculate completeness score
            total_issues = (sigops_quality['missing_coordinates'] + 
                          sigops_quality['invalid_coordinates'] + 
                          sigops_quality['missing_mvids'])
            
            if len(sigops) > 0:
                sigops_quality['completeness_score'] = max(0, 100 - (total_issues / len(sigops) * 100))
            
            quality_report['sigops'] = sigops_quality
        
        # Calculate overall score
        loc_score = quality_report['locations'].get('completeness_score', 0)
        sigops_score = quality_report['sigops'].get('completeness_score', 0)
        quality_report['overall_score'] = (loc_score + sigops_score) / 2
        
        logger.info(f"Data quality validation completed. Overall score: {quality_report['overall_score']:.1f}%")
        
        return quality_report
        
    except Exception as e:
        logger.error(f"Error in data quality validation: {e}")
        return {'error': str(e)}

def create_dashboard_data(locations: pd.DataFrame, sigops: pd.DataFrame) -> Dict[str, Any]:
    """
    Create data structure for dashboard visualization
    
    Args:
        locations: Locations DataFrame
        sigops: SigOps DataFrame
        
    Returns:
        Dictionary with dashboard data
    """
    
    try:
        dashboard_data = {
            'summary_metrics': {},
            'distance_analysis': {},
            'geographic_coverage': {},
            'data_quality': {}
        }
        
        # Summary metrics
        if not locations.empty:
            dashboard_data['summary_metrics'] = {
                'total_locations': len(locations),
                'unique_mvids': locations['Assummed MVID'].nunique() if 'Assummed MVID' in locations.columns else 0,
                'total_sigops': len(sigops) if not sigops.empty else 0,
                'avg_distance_to_sigops': float(locations['dist'].mean()) if 'dist' in locations.columns else 0
            }
        
        # Distance analysis
        dashboard_data['distance_analysis'] = analyze_distance_patterns(locations)
        
        # Geographic coverage
        if 'Latitude_teams' in locations.columns and 'Longitude_teams' in locations.columns:
            lat_range = locations['Latitude_teams'].max() - locations['Latitude_teams'].min()
            lon_range = locations['Longitude_teams'].max() - locations['Longitude_teams'].min()
            
            dashboard_data['geographic_coverage'] = {
                'lat_range': float(lat_range),
                'lon_range': float(lon_range),
                'center_lat': float(locations['Latitude_teams'].mean()),
                'center_lon': float(locations['Longitude_teams'].mean()),
                'bounding_box': {
                    'north': float(locations['Latitude_teams'].max()),
                    'south': float(locations['Latitude_teams'].min()),
                    'east': float(locations['Longitude_teams'].max()),
                    'west': float(locations['Longitude_teams'].min())
                }
            }
        
        # Data quality metrics
        dashboard_data['data_quality'] = validate_data_quality(locations, sigops)
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error creating dashboard data: {e}")
        return {'error': str(e)}

# Export all functions for easy importing
__all__ = [
    'load_teams_locations',
    'perform_spatial_join_with_states',
    'load_sigops_corridors',
    'calculate_distance_matrix',
    'find_minimum_distances',
    'merge_locations_with_sigops',
    'create_location_summary',
    'merge_sigops_with_summary',
    'create_leaflet_map',
    'save_output_files',
    'validate_coordinates',
    'generate_summary_report',
    'main',
    'load_from_s3',
    'save_to_s3',
    'create_advanced_map',
    'analyze_distance_patterns',
    'create_distance_histogram',
    'export_to_geojson',
    'create_summary_statistics_table',
    'validate_data_quality',
    'create_dashboard_data'
]

# Equivalent to the R script execution: df <- get_quarterly_data(); write_quarterly_data(df)
if __name__ == "__main__":
    """
    Main execution equivalent to the R script bottom section:
    - Load TEAMS locations
    - Perform spatial operations
    - Calculate distances
    - Create outputs
    """
    
    # Run the main processing pipeline
    results = main()
    
    # Print results summary
    if results['status'] == 'success':
        print("TEAMS Locations Processing Completed Successfully!")
        print(f"Files created: {', '.join(results['files_created'])}")
        print(f"Total locations processed: {results['summary'].get('total_locations', 0)}")
        print(f"Average distance to SigOps: {results['summary'].get('average_distance', 0):.0f}m")
    else:
        print(f"Processing failed: {results.get('error', 'Unknown error')}")