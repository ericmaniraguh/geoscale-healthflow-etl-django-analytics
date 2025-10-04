# =============================================================================
# 4. FILE: processors/file_validator.py
# =============================================================================

import os
import zipfile
import tempfile
import geopandas as gpd
import rasterio
from typing import Tuple

class FileValidator:
    """Handles file validation for GeoJSON and GeoTIFF files"""
    
    def __init__(self):
        self.allowed_geojson_extensions = ['.geojson', '.json', '.zip']
        self.allowed_geotiff_extensions = ['.tif', '.tiff']
    
    def validate_geojson(self, file) -> Tuple[bool, str]:
        """Validate GeoJSON file"""
        try:
            name = file.name.lower()
            
            # Check extension
            if not any(name.endswith(ext) for ext in self.allowed_geojson_extensions):
                return False, "Invalid file extension. Use .geojson, .json, or .zip"
            
            # Size check (100MB limit)
            if file.size > 100 * 1024 * 1024:
                return False, "File too large. Maximum size is 100MB"
            
            # For ZIP files, check if contains shapefile
            if name.endswith('.zip'):
                return self._validate_shapefile_zip(file)
            
            # For GeoJSON, try to read first few lines
            file.seek(0)
            content = file.read(1024).decode('utf-8')
            file.seek(0)
            
            if 'geometry' not in content or 'features' not in content:
                return False, "File doesn't appear to be valid GeoJSON"
            
            return True, "Valid GeoJSON file"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def validate_geotiff(self, file) -> Tuple[bool, str]:
        """Validate GeoTIFF file"""
        try:
            name = file.name.lower()
            
            # Check extension
            if not any(name.endswith(ext) for ext in self.allowed_geotiff_extensions):
                return False, "Invalid file extension. Use .tif or .tiff"
            
            # Size check
            if file.size > 500 * 1024 * 1024:  # 500MB limit for raster
                return False, "File too large. Maximum size is 500MB"
            
            # Try to read header (basic validation)
            file.seek(0)
            header = file.read(4)
            file.seek(0)
            
            # Check TIFF magic numbers
            if header not in [b'II*\x00', b'MM\x00*']:
                return False, "File doesn't appear to be a valid TIFF"
            
            return True, "Valid GeoTIFF file"
            
        except Exception as e:
            return False, f"Validation error: {str(e)}"
    
    def _validate_shapefile_zip(self, zip_file) -> Tuple[bool, str]:
        """Validate ZIP file contains shapefile"""
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                
                # Check for required shapefile components
                has_shp = any(f.endswith('.shp') for f in file_list)
                has_dbf = any(f.endswith('.dbf') for f in file_list)
                has_shx = any(f.endswith('.shx') for f in file_list)
                
                if not (has_shp and has_dbf and has_shx):
                    return False, "ZIP must contain .shp, .dbf, and .shx files"
                
                return True, "Valid shapefile ZIP"
                
        except zipfile.BadZipFile:
            return False, "Invalid ZIP file"
        except Exception as e:
            return False, f"ZIP validation error: {str(e)}"
    
    def extract_shapefile(self, zip_file, temp_dir: str, process_id: str) -> str:
        """Extract shapefile from ZIP and convert to GeoJSON"""
        extract_dir = os.path.join(temp_dir, f"extracted_{process_id}")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Find shapefile
        shp_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.lower().endswith('.shp'):
                    shp_files.append(os.path.join(root, file))
        
        if not shp_files:
            raise ValueError("No shapefile found in ZIP archive")
        
        # Use the first shapefile found
        shp_file = shp_files[0]
        
        # Convert to GeoJSON
        gdf = gpd.read_file(shp_file)
        geojson_path = os.path.join(temp_dir, f"converted_{process_id}.geojson")
        gdf.to_file(geojson_path, driver='GeoJSON')
        
        return geojson_path