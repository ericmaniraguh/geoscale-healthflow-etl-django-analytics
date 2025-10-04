
# =============================================================================
# WGS84 COORDINATE ATLAS BATCH PROCESSOR - UPDATED WITH DJANGO MONGODB
# File: app/geospatial_merger/processors/batch_processor.py
# =============================================================================

import os
import tempfile
import geopandas as gpd
import rasterio
import rasterio.mask
import numpy as np
import pandas as pd
import json
import time
from datetime import datetime
from shapely.geometry import shape, mapping
from .crs_overlap_fixer import fix_crs_overlap_issues
from .mongo_saver import create_geospatial_saver


class GeospatialBatchProcessor:
    """WGS84 coordinate processor with Django MongoDB integration"""

    def __init__(self, process_id: str, batch_size: int = 100):
        self.process_id = process_id
        self.batch_size = batch_size
        self.results = []
        
        # Enhanced tracking counters
        self.file_stats = {
            "boundary_files_uploaded": 0,
            "slope_files_uploaded": 0,
            "total_boundary_features": 0,
            "total_slope_points": 0,
            "slope_points_after_conversion": 0,
            "processed_features": 0,
            "failed_features": 0,
            "batches_completed": 0,
            "total_batches": 0,
            "crs_fix_applied": False,
            "original_overlap_status": "unknown",
            "final_overlap_status": "unknown",
            "storage_coordinates": "WGS84"
        }
        
        # Coordinate information for dashboard
        self.coordinate_info = {
            "boundary_bounds": "Not processed",
            "slope_bounds": "Not processed", 
            "overlap_status": "Not calculated",
            "overlap_coverage": "Not calculated"
        }

        # File paths
        self.temp_base = tempfile.gettempdir()
        self.progress_file = os.path.join(self.temp_base, f"progress_{process_id}.json")
        self.results_file = os.path.join(self.temp_base, f"results_{process_id}.json")
        self.geojson_file = os.path.join(self.temp_base, f"villages_slope_{process_id}.geojson")
        os.makedirs(self.temp_base, exist_ok=True)

        # MongoDB setup using Django settings (same as ETL)
        self.mongo_saver = create_geospatial_saver(process_id)

    @property
    def mongodb_available(self):
        """Check if MongoDB is available"""
        return self.mongo_saver is not None and self.mongo_saver.is_connected()
    
    @property
    def mongodb_error(self):
        """Get MongoDB error if any"""
        if self.mongo_saver is None:
            return "MongoDB saver not initialized"
        return self.mongo_saver.mongodb_error

    def update_progress(self, stage: str, percentage: int, message: str):
        """Update progress with WGS84 coordinate tracking"""
        progress_data = {
            "process_id": self.process_id,
            "stage": stage,
            "progress": percentage,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "file_statistics": self.file_stats,
            "coordinate_info": self.coordinate_info,
            "coordinate_system": "WGS84",
            "mongodb_atlas_status": self.mongo_saver.get_connection_status(),
            "completed": stage == "completed"
        }
        
        # Save to local file
        try:
            with open(self.progress_file, "w") as f:
                json.dump(progress_data, f, indent=2, default=str)

        except Exception as e:
            print(f"Progress file save error: {e}")
        
        # Save to MongoDB using Django settings
        self.mongo_saver.update_progress_metadata(
            stage, percentage, message, 
            self.file_stats, self.coordinate_info
        )
        
        print(f"Progress: {percentage}% - {message}")

    def process_files(self, geojson_path: str, geotiff_path: str):
        """Main processing with WGS84 coordinate storage"""
        try:
            print("=" * 80)
            print("GEOSPATIAL PROCESSING WITH WGS84 COORDINATES")
            print("=" * 80)
            print(f"MongoDB Status: {'Connected' if self.mongodb_available else 'Disconnected'}")
            print(f"Coordinate System: WGS84 (EPSG:4326)")
            print("=" * 80)
            
            # Step 1: Diagnose and fix CRS/overlap issues
            self.update_progress("diagnosing", 5, "Diagnosing CRS and overlap issues...")
            fix_results = fix_crs_overlap_issues(self.process_id, geojson_path, geotiff_path)
            
            if not fix_results["success"]:
                error_msg = f"CRS/Overlap fix failed: {fix_results['error']}"
                self.update_progress("error", 0, error_msg)
                raise Exception(error_msg)
            
            # Update file stats and coordinate info
            self.file_stats["crs_fix_applied"] = True
            self.file_stats["original_overlap_status"] = "NO_OVERLAP"
            self.file_stats["final_overlap_status"] = "OVERLAP_ACHIEVED"
            
            # Update coordinate info from fix results
            if "coordinate_info" in fix_results:
                self.coordinate_info.update(fix_results["coordinate_info"])
            
            # Use fixed file paths
            fixed_geojson_path = fix_results["fixed_boundaries_path"]
            fixed_geotiff_path = fix_results["fixed_slope_path"]
            
            self.update_progress("loading", 15, "Loading data and converting to WGS84...")
            
            # Step 2: Load and convert to WGS84
            boundaries_gdf, slope_src = self.load_and_convert_to_wgs84(fixed_geojson_path, fixed_geotiff_path)
            
            # Update coordinate bounds information
            self.update_coordinate_bounds(boundaries_gdf, slope_src)
            
            self.file_stats["boundary_files_uploaded"] = 1
            self.file_stats["slope_files_uploaded"] = 1
            self.file_stats["total_boundary_features"] = len(boundaries_gdf)
            
            # Count slope points
            slope_array = slope_src.read(1)
            valid_mask = (slope_array != slope_src.nodata) & (~np.isnan(slope_array))
            self.file_stats["total_slope_points"] = int(np.sum(valid_mask))
            
            print(f"Loaded {len(boundaries_gdf)} boundaries in WGS84")
            print(f"Slope data: {self.file_stats['total_slope_points']} valid pixels")
            print(f"Boundaries CRS: {boundaries_gdf.crs}")
            print(f"Slope CRS: {slope_src.crs}")
            
            # Step 3: Process slope analysis in WGS84
            self.update_progress("processing", 25, "Processing slope analysis in WGS84...")
            self.process_slope_analysis_wgs84(boundaries_gdf, slope_src)
            slope_src.close()
            
            # Step 4: Save results
            self.update_progress("saving", 90, "Saving WGS84 results...")
            self.save_wgs84_results()
            
            # Step 5: Final summary
            self.print_wgs84_summary()
            self.update_progress("completed", 100, "Processing completed with WGS84 coordinates!")
            
        except Exception as e:
            error_msg = f"Processing failed: {str(e)}"
            self.update_progress("error", 0, error_msg)
            print(f"ERROR: {error_msg}")
            raise

    def update_coordinate_bounds(self, boundaries_gdf, slope_src):
        """Update coordinate bounds information for dashboard"""
        try:
            # Get boundaries bounds in WGS84
            bounds = boundaries_gdf.bounds
            boundary_bounds = [
                round(bounds.minx.min(), 6),
                round(bounds.miny.min(), 6), 
                round(bounds.maxx.max(), 6),
                round(bounds.maxy.max(), 6)
            ]
            
            # Get slope bounds
            slope_bounds_raw = slope_src.bounds
            slope_bounds = [
                round(slope_bounds_raw.left, 6),
                round(slope_bounds_raw.bottom, 6),
                round(slope_bounds_raw.right, 6), 
                round(slope_bounds_raw.top, 6)
            ]
            
            # Calculate overlap
            overlap_exists = (
                boundary_bounds[0] < slope_bounds[2] and boundary_bounds[2] > slope_bounds[0] and
                boundary_bounds[1] < slope_bounds[3] and boundary_bounds[3] > slope_bounds[1]
            )
            
            # Calculate overlap percentage (simplified)
            if overlap_exists:
                boundary_area = (boundary_bounds[2] - boundary_bounds[0]) * (boundary_bounds[3] - boundary_bounds[1])
                slope_area = (slope_bounds[2] - slope_bounds[0]) * (slope_bounds[3] - slope_bounds[1])
                overlap_percentage = min(boundary_area, slope_area) / max(boundary_area, slope_area) * 100
                coverage = f"{overlap_percentage:.1f}%"
            else:
                coverage = "0%"
            
            self.coordinate_info.update({
                "boundary_bounds": str(boundary_bounds),
                "slope_bounds": str(slope_bounds),
                "overlap_status": "OVERLAP_ACHIEVED" if overlap_exists else "NO_OVERLAP",
                "overlap_coverage": coverage
            })
            
        except Exception as e:
            print(f"Error updating coordinate bounds: {e}")

    def load_and_convert_to_wgs84(self, geojson_path, geotiff_path):
        """Load data and ensure everything is in WGS84"""
        # Load boundaries
        boundaries_gdf = gpd.read_file(geojson_path)
        
        # Convert boundaries to WGS84 if not already
        if boundaries_gdf.crs != "EPSG:4326":
            print(f"Converting boundaries from {boundaries_gdf.crs} to WGS84...")
            boundaries_gdf = boundaries_gdf.to_crs("EPSG:4326")
        
        # Load slope raster
        slope_src = rasterio.open(geotiff_path)
        
        return boundaries_gdf, slope_src

    def process_slope_analysis_wgs84(self, boundaries_wgs84: gpd.GeoDataFrame, slope_src):
        """Process slope analysis and store in WGS84 coordinates"""
        total_features = len(boundaries_wgs84)
        self.file_stats["total_batches"] = (total_features + self.batch_size - 1) // self.batch_size
        
        print("\n" + "=" * 60)
        print("SLOPE ANALYSIS WITH WGS84 STORAGE")
        print("=" * 60)
        print(f"Total features: {total_features}")
        print(f"Batch size: {self.batch_size}")
        print(f"Total batches: {self.file_stats['total_batches']}")
        print(f"Storage coordinates: WGS84 (EPSG:4326)")
        print(f"Processing coordinates: {slope_src.crs}")
        
        # Create processing version if needed
        if slope_src.crs != "EPSG:4326":
            boundaries_for_processing = boundaries_wgs84.to_crs(slope_src.crs)
        else:
            boundaries_for_processing = boundaries_wgs84
        
        slope_points_used_total = 0
        
        for batch_num in range(self.file_stats["total_batches"]):
            batch_start = batch_num * self.batch_size
            batch_end = min(batch_start + self.batch_size, total_features)
            
            # Get both WGS84 and processing versions of the batch
            batch_wgs84 = boundaries_wgs84.iloc[batch_start:batch_end]
            batch_processing = boundaries_for_processing.iloc[batch_start:batch_end]
            
            print(f"\nBatch {batch_num + 1}/{self.file_stats['total_batches']}: Features {batch_start + 1}-{batch_end}")
            
            batch_processed = 0
            batch_failed = 0
            batch_slope_points = 0
            batch_results = []
            
            for local_idx, ((idx_wgs84, feature_wgs84), (idx_proc, feature_proc)) in enumerate(zip(batch_wgs84.iterrows(), batch_processing.iterrows())):
                global_idx = batch_start + local_idx
                
                try:
                    # Use processing geometry for slope extraction
                    geom_processing = feature_proc.geometry
                    if not geom_processing.is_valid:
                        geom_processing = geom_processing.buffer(0)
                    
                    # Get WGS84 geometry for storage
                    geom_wgs84 = feature_wgs84.geometry
                    if not geom_wgs84.is_valid:
                        geom_wgs84 = geom_wgs84.buffer(0)
                    
                    # Perform raster mask with processing geometry
                    geom_for_mask = [mapping(geom_processing)]
                    
                    out_image, out_transform = rasterio.mask.mask(
                        slope_src, 
                        geom_for_mask, 
                        crop=True, 
                        nodata=slope_src.nodata,
                        all_touched=True
                    )
                    
                    # Extract valid slope data
                    data = out_image[0]
                    valid_data = data[data != slope_src.nodata]
                    valid_data = valid_data[~np.isnan(valid_data)]
                    
                    if len(valid_data) > 0:
                        slope_points_count = len(valid_data)
                        batch_slope_points += slope_points_count
                        mean_val = float(np.mean(valid_data))
                        
                        # Create record with WGS84 coordinates for storage
                        record = {
                            "feature_id": global_idx,
                            "mean_slope": mean_val,
                            "max_slope": float(np.max(valid_data)),
                            "min_slope": float(np.min(valid_data)),
                            "slope_class": self.classify_slope(mean_val),
                            "slope_points_used": slope_points_count,
                            "geometry": mapping(geom_wgs84),  # Store in WGS84
                            "coordinates_system": "WGS84",
                            "processing_metadata": {
                                "process_id": self.process_id,
                                "batch_number": batch_num + 1,
                                "feature_index": global_idx,
                                "processing_date": datetime.now().isoformat(),
                                "stored_crs": "EPSG:4326",
                                "processing_crs": str(slope_src.crs)
                            }
                        }
                        
                        # Add all original attributes (from WGS84 version)
                        for col in boundaries_wgs84.columns:
                            if col != 'geometry':
                                try:
                                    value = feature_wgs84[col]
                                    if pd.isna(value):
                                        value = None
                                    elif hasattr(value, 'item'):
                                        value = value.item()
                                    record[col] = value
                                except Exception:
                                    record[col] = str(feature_wgs84[col]) if feature_wgs84[col] is not None else None
                        
                        self.results.append(record)
                        batch_results.append(record)
                        batch_processed += 1
                        
                    else:
                        # No slope data - store boundary in WGS84
                        record = self.create_no_slope_record_wgs84(feature_wgs84, geom_wgs84, global_idx, batch_num + 1, boundaries_wgs84)
                        self.results.append(record)
                        batch_results.append(record)
                        batch_failed += 1
                        
                except Exception as e:
                    print(f"   Error processing feature {global_idx + 1}: {e}")
                    try:
                        record = self.create_error_record_wgs84(feature_wgs84, global_idx, batch_num + 1, str(e), boundaries_wgs84)
                        self.results.append(record)
                        batch_results.append(record)
                    except:
                        pass
                    batch_failed += 1
                    continue
            
            # Update statistics
            self.file_stats["batches_completed"] += 1
            self.file_stats["processed_features"] += batch_processed
            self.file_stats["failed_features"] += batch_failed
            slope_points_used_total += batch_slope_points
            
            # Save batch to MongoDB using Django settings
            mongodb_saved = self.mongo_saver.save_batch_results(batch_results, batch_num + 1)
            
            print(f"   Processed: {batch_processed}")
            print(f"   Failed: {batch_failed}")
            print(f"   Slope points: {batch_slope_points}")
            print(f"   MongoDB saved: {'Yes' if mongodb_saved else 'No'}")
            
            # Update progress
            progress = 25 + int(((batch_num + 1) / self.file_stats["total_batches"]) * 60)
            self.update_progress(
                "processing", progress,
                f"Batch {batch_num + 1}/{self.file_stats['total_batches']}: {self.file_stats['processed_features']} features in WGS84"
            )
        
        self.file_stats["slope_points_after_conversion"] = slope_points_used_total
        
        print(f"\nSlope analysis completed in WGS84:")
        print(f"   Total processed: {self.file_stats['processed_features']}")
        print(f"   Success rate: {(self.file_stats['processed_features'] / total_features * 100):.1f}%")

    def create_no_slope_record_wgs84(self, feature, geom, global_idx, batch_num, boundaries_gdf):
        """Create WGS84 record for boundary with no slope data"""
        record = {
            "feature_id": global_idx,
            "mean_slope": 0.0,
            "max_slope": 0.0,
            "min_slope": 0.0,
            "slope_class": "No Data",
            "slope_points_used": 0,
            "geometry": mapping(geom),
            "coordinates_system": "WGS84",
            "processing_metadata": {
                "process_id": self.process_id,
                "batch_number": batch_num,
                "feature_index": global_idx,
                "processing_date": datetime.now().isoformat(),
                "stored_crs": "EPSG:4326",
                "status": "no_slope_data"
            }
        }
        
        # Add original attributes
        for col in boundaries_gdf.columns:
            if col != 'geometry':
                try:
                    value = feature[col]
                    if pd.isna(value):
                        value = None
                    elif hasattr(value, 'item'):
                        value = value.item()
                    record[col] = value
                except Exception:
                    record[col] = str(feature[col]) if feature[col] is not None else None
        
        return record

    def create_error_record_wgs84(self, feature, global_idx, batch_num, error_msg, boundaries_gdf):
        """Create WGS84 error record"""
        record = {
            "feature_id": global_idx,
            "mean_slope": None,
            "max_slope": None,
            "min_slope": None,
            "slope_class": "Processing Error",
            "slope_points_used": 0,
            "geometry": None,
            "coordinates_system": "WGS84",
            "processing_metadata": {
                "process_id": self.process_id,
                "batch_number": batch_num,
                "feature_index": global_idx,
                "processing_date": datetime.now().isoformat(),
                "stored_crs": "EPSG:4326",
                "status": "error",
                "error_message": error_msg
            }
        }
        
        # Add original attributes
        for col in boundaries_gdf.columns:
            if col != 'geometry':
                try:
                    record[col] = str(feature[col]) if feature[col] is not None else None
                except:
                    record[col] = "Error reading attribute"
        
        return record

    def classify_slope(self, value: float) -> str:
        """Classify slope values"""
        if value < 5:
            return "Flat (0–5°)"
        elif value < 15:
            return "Moderate (5–15°)"
        elif value < 30:
            return "Steep (15–30°)"
        else:
            return "Very Steep (>30°)"

    def save_wgs84_results(self):
        """Save WGS84 results to MongoDB and files"""
        print("\nSaving WGS84 results...")

        # MongoDB save using Django settings
        mongodb_saved = False
        if self.mongodb_available:
            try:
                stats = self.mongo_saver.get_process_statistics()
                if stats.get("total_records", 0) > 0:
                    print(f"Found {stats['total_records']} WGS84 records in MongoDB")
                    mongodb_saved = True
                elif self.results:
                    mongodb_saved = self.mongo_saver.save_all_results(self.results)
            except Exception as e:
                print(f"MongoDB save error: {e}")

        # File save with WGS84 metadata
        self.save_wgs84_files(mongodb_saved)


            
    def save_wgs84_files(self, mongodb_saved):
        """Save WGS84 files"""
        try:
            # JSON with WGS84 information
            output_data = {
                "coordinate_system": "WGS84 (EPSG:4326)",
                "processing_summary": {
                    "process_id": self.process_id,
                    "total_features": self.file_stats["total_boundary_features"],
                    "processed_features": self.file_stats["processed_features"],
                    "success_rate": (self.file_stats["processed_features"] / self.file_stats["total_boundary_features"] * 100)
                                    if self.file_stats["total_boundary_features"] > 0 else 0,
                    "coordinate_system": "WGS84",
                    "mongodb_storage": mongodb_saved
                },
                "coordinate_info": self.coordinate_info,
                "file_statistics": self.file_stats,
                "results": self.results
            }

            with open(self.results_file, "w") as f:
                json.dump(output_data, f, indent=2, default=str)

            #  Only build GeoJSON if results exist
            if self.results:
                features = []
                for result in self.results:
                    if result.get("geometry"):
                        feature = {
                            "type": "Feature",
                            "geometry": result["geometry"],
                            "properties": {k: v for k, v in result.items() if k != "geometry"}
                        }
                        features.append(feature)

                geojson_output = {
                    "type": "FeatureCollection",
                    "crs": {
                        "type": "name",
                        "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
                    },
                    "features": features,
                    "metadata": {
                        "coordinate_system": "WGS84 (EPSG:4326)",
                        "process_id": self.process_id,
                        "mongodb_storage": mongodb_saved,
                        "created_at": datetime.now().isoformat()
                    }
                }

                with open(self.geojson_file, "w", encoding='utf-8') as f:
                    json.dump(geojson_output, f, indent=2, ensure_ascii=False, default=str)

                print("WGS84 results + GeoJSON saved to files")
            else:
                print(" No results in memory, skipped GeoJSON export (already saved to MongoDB).")

        except Exception as e:
            print(f"File save error: {e}")
            raise


    def print_wgs84_summary(self):
        """Print WGS84 processing summary"""
        print("\n" + "=" * 80)
        print("WGS84 COORDINATE PROCESSING SUMMARY")
        print("=" * 80)
        
        print("\nCOORDINATE SYSTEM:")
        print("   Storage: WGS84 (EPSG:4326)")
        print("   Format: Decimal degrees (human-readable)")
        print(f"   Boundary bounds: {self.coordinate_info['boundary_bounds']}")
        print(f"   Slope bounds: {self.coordinate_info['slope_bounds']}")
        
        print("\nMONGODB CONNECTION:")
        print(f"   Status: {'Connected' if self.mongodb_available else 'Failed'}")
        if self.mongodb_error:
            print(f"   Error: {self.mongodb_error}")
        
        print("\nPROCESSING RESULTS:")
        print(f"   Total features: {self.file_stats['total_boundary_features']}")
        print(f"   Successfully processed: {self.file_stats['processed_features']}")
        print(f"   Success rate: {(self.file_stats['processed_features'] / self.file_stats['total_boundary_features'] * 100):.1f}%")
        print(f"   Slope points used: {self.file_stats['slope_points_after_conversion']}")
        print(f"   Overlap coverage: {self.coordinate_info['overlap_coverage']}")
        
        print("\nSTORAGE:")
        print(f"   MongoDB (WGS84): {'Success' if self.mongodb_available else 'Failed'}")
        print(f"   Local files: Success")
        print(f"   Database: {self.mongo_saver.mongo_db_name}")
        print(f"   Collection: {self.mongo_saver.main_collection_name}")
        
        if self.file_stats["processed_features"] > 0:
            print(f"\nSUCCESS: {self.file_stats['processed_features']} boundaries with slope data stored in WGS84!")
        
        print("=" * 80)

    def __del__(self):
        """Cleanup"""
        if hasattr(self, 'mongo_saver') and self.mongo_saver:
            self.mongo_saver.close_connection()