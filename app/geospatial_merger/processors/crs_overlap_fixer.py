# =============================================================================
# CRS AND OVERLAP DIAGNOSTIC FIXER
# File: app/geospatial_merger/processors/crs_overlap_fixer.py
# =============================================================================

import os
import zipfile
import tempfile
import geopandas as gpd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import json
from datetime import datetime
from shapely.geometry import box
import pyproj
from pyproj import CRS

class CRSOverlapFixer:
    """Diagnose and fix CRS and overlap issues between boundaries and slope data"""
    
    def __init__(self, process_id: str):
        self.process_id = process_id
        self.diagnostic_results = {}
        self.temp_base = tempfile.gettempdir()
        
    def diagnose_and_fix(self, geojson_path: str, geotiff_path: str):
        """Main diagnostic and fixing process"""
        print("=" * 80)
        print("CRS AND OVERLAP DIAGNOSTIC ANALYSIS")
        print("=" * 80)
        
        # Step 1: Load data and analyze CRS
        boundaries_gdf, slope_src = self.load_and_analyze_crs(geojson_path, geotiff_path)
        
        # Step 2: Detailed bounds analysis
        self.analyze_bounds_detailed(boundaries_gdf, slope_src)
        
        # Step 3: Attempt CRS fixes
        fixed_boundaries, fixed_slope_path = self.attempt_crs_fixes(boundaries_gdf, slope_src, geotiff_path)
        
        # Step 4: Verify overlap after fixes
        final_overlap = self.verify_final_overlap(fixed_boundaries, fixed_slope_path)
        
        # Step 5: Return results
        return self.generate_fix_results(fixed_boundaries, fixed_slope_path, final_overlap)
    
    def load_and_analyze_crs(self, geojson_path: str, geotiff_path: str):
        """Load data and analyze CRS in detail"""
        print("\n1. LOADING DATA AND ANALYZING CRS:")
        print("-" * 50)
        
        # Load boundaries
        if geojson_path.lower().endswith(".zip"):
            boundaries_gdf = self.load_from_zip(geojson_path)
        else:
            boundaries_gdf = gpd.read_file(geojson_path)
        
        # Load slope raster
        slope_src = rasterio.open(geotiff_path)
        
        # Analyze CRS
        boundary_crs = boundaries_gdf.crs
        slope_crs = slope_src.crs
        
        print(f"Boundary CRS: {boundary_crs}")
        print(f"Slope CRS: {slope_crs}")
        print(f"CRS Match: {'YES' if boundary_crs == slope_crs else 'NO'}")
        
        # Analyze CRS types
        self.analyze_crs_types(boundary_crs, slope_crs)
        
        self.diagnostic_results["boundary_crs"] = str(boundary_crs)
        self.diagnostic_results["slope_crs"] = str(slope_crs)
        self.diagnostic_results["crs_match"] = boundary_crs == slope_crs
        
        return boundaries_gdf, slope_src
    
    def load_from_zip(self, zip_path):
        """Load boundaries from ZIP file"""
        extract_dir = os.path.join(self.temp_base, f"diagnostic_extract_{self.process_id}")
        
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Find shapefiles
        shp_files = []
        for root, dirs, files in os.walk(extract_dir):
            for f in files:
                if f.lower().endswith(".shp"):
                    shp_files.append(os.path.join(root, f))
        
        if not shp_files:
            raise FileNotFoundError("No shapefiles found in ZIP")
        
        # Use first or village file
        shp_file = shp_files[0]
        for f in shp_files:
            if "village" in f.lower():
                shp_file = f
                break
        
        print(f"Using shapefile: {os.path.basename(shp_file)}")
        return gpd.read_file(shp_file)
    
    def analyze_crs_types(self, boundary_crs, slope_crs):
        """Analyze what types of CRS we're dealing with"""
        print(f"\nCRS TYPE ANALYSIS:")
        
        def describe_crs(crs, name):
            try:
                crs_obj = CRS(crs)
                print(f"{name}:")
                print(f"  Name: {crs_obj.name}")
                print(f"  Type: {crs_obj.coordinate_system.name}")
                print(f"  Units: {[axis.unit_name for axis in crs_obj.coordinate_system.axis_list]}")
                print(f"  Geographic: {crs_obj.is_geographic}")
                print(f"  Projected: {crs_obj.is_projected}")
                if crs_obj.is_geographic:
                    print(f"  Datum: {crs_obj.datum.name}")
                return crs_obj
            except Exception as e:
                print(f"{name}: Error analyzing - {e}")
                return None
        
        boundary_crs_obj = describe_crs(boundary_crs, "Boundary CRS")
        slope_crs_obj = describe_crs(slope_crs, "Slope CRS")
        
        return boundary_crs_obj, slope_crs_obj
    
    def analyze_bounds_detailed(self, boundaries_gdf, slope_src):
        """Detailed bounds analysis in multiple CRS"""
        print("\n2. DETAILED BOUNDS ANALYSIS:")
        print("-" * 50)
        
        # Original bounds
        boundary_bounds = boundaries_gdf.total_bounds
        slope_bounds = slope_src.bounds
        
        print(f"Boundary bounds (original CRS): {boundary_bounds}")
        print(f"Slope bounds (original CRS): {slope_bounds}")
        
        # Convert both to WGS84 for comparison
        try:
            # Convert boundaries to WGS84
            boundaries_wgs84 = boundaries_gdf.to_crs("EPSG:4326")
            boundary_bounds_wgs84 = boundaries_wgs84.total_bounds
            
            # Convert slope bounds to WGS84
            from rasterio.warp import transform_bounds
            slope_bounds_wgs84 = transform_bounds(slope_src.crs, "EPSG:4326", *slope_bounds)
            
            print(f"\nIN WGS84 (EPSG:4326):")
            print(f"Boundary bounds: [{boundary_bounds_wgs84[0]:.6f}, {boundary_bounds_wgs84[1]:.6f}, {boundary_bounds_wgs84[2]:.6f}, {boundary_bounds_wgs84[3]:.6f}]")
            print(f"Slope bounds: [{slope_bounds_wgs84[0]:.6f}, {slope_bounds_wgs84[1]:.6f}, {slope_bounds_wgs84[2]:.6f}, {slope_bounds_wgs84[3]:.6f}]")
            
            # Check overlap in WGS84
            overlap_check = self.check_bounds_overlap(boundary_bounds_wgs84, slope_bounds_wgs84)
            print(f"Overlap in WGS84: {overlap_check}")
            
            # Store results
            self.diagnostic_results["boundary_bounds_wgs84"] = boundary_bounds_wgs84.tolist()
            self.diagnostic_results["slope_bounds_wgs84"] = slope_bounds_wgs84
            self.diagnostic_results["overlap_wgs84"] = overlap_check
            
            # Analyze distances if no overlap
            if not overlap_check:
                self.analyze_distance_between_datasets(boundary_bounds_wgs84, slope_bounds_wgs84)
            
        except Exception as e:
            print(f"Error converting to WGS84: {e}")
    
    def check_bounds_overlap(self, bounds1, bounds2):
        """Check if two bounding boxes overlap"""
        return not (bounds1[2] < bounds2[0] or bounds1[0] > bounds2[2] or 
                   bounds1[3] < bounds2[1] or bounds1[1] > bounds2[3])
    
    def analyze_distance_between_datasets(self, boundary_bounds, slope_bounds):
        """Calculate approximate distance between non-overlapping datasets"""
        print(f"\n3. DISTANCE ANALYSIS (NO OVERLAP DETECTED):")
        print("-" * 50)
        
        # Calculate center points
        boundary_center = [(boundary_bounds[0] + boundary_bounds[2])/2, (boundary_bounds[1] + boundary_bounds[3])/2]
        slope_center = [(slope_bounds[0] + slope_bounds[2])/2, (slope_bounds[1] + slope_bounds[3])/2]
        
        print(f"Boundary center: {boundary_center}")
        print(f"Slope center: {slope_center}")
        
        # Calculate rough distance
        from geopy.distance import geodesic
        try:
            distance = geodesic((boundary_center[1], boundary_center[0]), (slope_center[1], slope_center[0])).kilometers
            print(f"Approximate distance between datasets: {distance:.2f} km")
            
            if distance > 1000:
                print("ERROR: Datasets are on different continents!")
            elif distance > 100:
                print("ERROR: Datasets are in different regions!")
            else:
                print("WARNING: Datasets are close but not overlapping - CRS issue likely")
                
        except Exception as e:
            print(f"Could not calculate distance: {e}")
    
    def attempt_crs_fixes(self, boundaries_gdf, slope_src, geotiff_path):
        """Attempt various CRS fixes"""
        print("\n4. ATTEMPTING CRS FIXES:")
        print("-" * 50)
        
        fixed_boundaries = boundaries_gdf.copy()
        fixed_slope_path = geotiff_path
        
        # Fix 1: Try common Rwanda projections
        rwanda_projections = [
            "EPSG:32736",  # UTM Zone 36S (often used for Rwanda)
            "EPSG:32735",  # UTM Zone 35S
            "EPSG:4326",   # WGS84 Geographic
            "EPSG:3857"    # Web Mercator
        ]
        
        print("Trying common Rwanda projections...")
        for proj in rwanda_projections:
            try:
                test_boundaries = boundaries_gdf.to_crs(proj)
                
                # Convert slope bounds to same projection
                from rasterio.warp import transform_bounds
                slope_bounds_proj = transform_bounds(slope_src.crs, proj, *slope_src.bounds)
                boundary_bounds_proj = test_boundaries.total_bounds
                
                overlap = self.check_bounds_overlap(boundary_bounds_proj, slope_bounds_proj)
                print(f"  {proj}: {'OVERLAP' if overlap else 'NO OVERLAP'}")
                
                if overlap:
                    print(f"SUCCESS: Found overlap with {proj}")
                    fixed_boundaries = test_boundaries
                    
                    # Optionally reproject the raster too
                    if slope_src.crs != proj:
                        fixed_slope_path = self.reproject_raster(slope_src, geotiff_path, proj)
                    
                    break
                    
            except Exception as e:
                print(f"  {proj}: ERROR - {e}")
        
        # Fix 2: If still no overlap, try assuming wrong CRS on boundaries
        if not hasattr(self, 'overlap_found') or not self.overlap_found:
            print("\nTrying CRS assumption fixes...")
            self.try_crs_assumptions(boundaries_gdf, slope_src)
        
        return fixed_boundaries, fixed_slope_path
    
    def try_crs_assumptions(self, boundaries_gdf, slope_src):
        """Try assuming the boundaries have the wrong CRS assigned"""
        print("Trying to assume boundaries have slope raster CRS...")
        
        try:
            # Assume boundaries are actually in the slope raster's CRS
            boundaries_fixed = boundaries_gdf.copy()
            boundaries_fixed = boundaries_fixed.set_crs(slope_src.crs, allow_override=True)
            
            # Check overlap
            boundary_bounds = boundaries_fixed.total_bounds
            slope_bounds = list(slope_src.bounds)
            
            overlap = self.check_bounds_overlap(boundary_bounds, slope_bounds)
            print(f"Assuming boundaries are in {slope_src.crs}: {'OVERLAP' if overlap else 'NO OVERLAP'}")
            
            if overlap:
                print("SUCCESS: Overlap found by assuming wrong CRS assignment!")
                return boundaries_fixed
                
        except Exception as e:
            print(f"CRS assumption failed: {e}")
        
        return boundaries_gdf
    
    def reproject_raster(self, src, original_path, target_crs):
        """Reproject raster to target CRS"""
        try:
            output_path = os.path.join(self.temp_base, f"reprojected_slope_{self.process_id}.tif")
            
            transform, width, height = calculate_default_transform(
                src.crs, target_crs, src.width, src.height, *src.bounds
            )
            
            kwargs = src.meta.copy()
            kwargs.update({
                'crs': target_crs,
                'transform': transform,
                'width': width,
                'height': height
            })
            
            with rasterio.open(output_path, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.bilinear
                    )
            
            print(f"Reprojected raster saved to: {output_path}")
            return output_path
            
        except Exception as e:
            print(f"Raster reprojection failed: {e}")
            return original_path
    
    def verify_final_overlap(self, boundaries_gdf, slope_path):
        """Final verification of overlap"""
        print("\n5. FINAL OVERLAP VERIFICATION:")
        print("-" * 50)
        
        try:
            with rasterio.open(slope_path) as src:
                boundary_bounds = boundaries_gdf.total_bounds
                slope_bounds = list(src.bounds)
                
                overlap = self.check_bounds_overlap(boundary_bounds, slope_bounds)
                
                print(f"Final boundary bounds: {boundary_bounds}")
                print(f"Final slope bounds: {slope_bounds}")
                print(f"Final overlap status: {'SUCCESS' if overlap else 'FAILED'}")
                
                if overlap:
                    # Calculate overlap area percentage
                    overlap_area = self.calculate_overlap_percentage(boundary_bounds, slope_bounds)
                    print(f"Overlap coverage: {overlap_area:.1f}%")
                
                return overlap
                
        except Exception as e:
            print(f"Final verification failed: {e}")
            return False
    
    def calculate_overlap_percentage(self, bounds1, bounds2):
        """Calculate what percentage of boundary area overlaps with slope data"""
        # Calculate intersection
        x1 = max(bounds1[0], bounds2[0])
        y1 = max(bounds1[1], bounds2[1])
        x2 = min(bounds1[2], bounds2[2])
        y2 = min(bounds1[3], bounds2[3])
        
        if x1 < x2 and y1 < y2:
            intersection_area = (x2 - x1) * (y2 - y1)
            boundary_area = (bounds1[2] - bounds1[0]) * (bounds1[3] - bounds1[1])
            return (intersection_area / boundary_area) * 100
        return 0
    
    def generate_fix_results(self, fixed_boundaries, fixed_slope_path, final_overlap):
        """Generate summary of fixes applied"""
        print("\n" + "=" * 80)
        print("CRS FIX SUMMARY")
        print("=" * 80)
        
        if final_overlap:
            print("SUCCESS: Spatial overlap achieved!")
            print(f"Fixed boundaries CRS: {fixed_boundaries.crs}")
            print(f"Fixed slope path: {fixed_slope_path}")
            
            # Save the fixed data
            fixed_boundaries_path = os.path.join(self.temp_base, f"fixed_boundaries_{self.process_id}.geojson")
            fixed_boundaries.to_file(fixed_boundaries_path, driver="GeoJSON")
            
            return {
                "success": True,
                "fixed_boundaries_path": fixed_boundaries_path,
                "fixed_slope_path": fixed_slope_path,
                "final_crs": str(fixed_boundaries.crs),
                "diagnostic_results": self.diagnostic_results
            }
        else:
            print("FAILED: Could not achieve spatial overlap")
            print("This suggests the datasets are for different geographic areas")
            print("Please verify you have the correct boundary and slope files for the same region")
            
            return {
                "success": False,
                "error": "No spatial overlap could be achieved",
                "diagnostic_results": self.diagnostic_results
            }

# Usage function to integrate with your processor
def fix_crs_overlap_issues(process_id, geojson_path, geotiff_path):
    """Fix CRS and overlap issues before processing"""
    fixer = CRSOverlapFixer(process_id)
    return fixer.diagnose_and_fix(geojson_path, geotiff_path)