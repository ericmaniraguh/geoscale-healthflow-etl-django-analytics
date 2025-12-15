
import pytest
from unittest.mock import MagicMock, patch, ANY
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import rasterio
import numpy as np
import tempfile
import os
import shutil
from app.geospatial_merger.processors.batch_processor import GeospatialBatchProcessor
from app.geospatial_merger.processors.crs_overlap_fixer import CRSOverlapFixer
from app.geospatial_merger.processors.mongo_saver import GeospatialMongoSaver

# =============================================================================
# Test GeospatialMongoSaver
# =============================================================================

class TestGeospatialMongoSaver:
    @patch('app.geospatial_merger.processors.mongo_saver.MongoClient')
    def test_initialization_success(self, mock_client):
        # Setup mock client
        mock_db = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db
        mock_client.return_value.admin.command.return_value = {"ok": 1}
        
        saver = GeospatialMongoSaver("test_process")
        
        assert saver.mongodb_available is True
        assert saver.process_id == "test_process"
        assert saver.client is not None

    @patch('app.geospatial_merger.processors.mongo_saver.MongoClient')
    def test_save_batch_results(self, mock_client):
        # Setup
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        mock_collection.insert_many.return_value.inserted_ids = [1, 2]
        
        saver = GeospatialMongoSaver("test_process")
        saver.collection = mock_collection # Force set collection
        
        batch_results = [{"id": 1}, {"id": 2}]
        success = saver.save_batch_results(batch_results, 1)
        
        assert success is True
        mock_collection.insert_many.assert_called_once()
        # Verify metadata injection
        assert "_batch_info" in batch_results[0]

    @patch('app.geospatial_merger.processors.mongo_saver.MongoClient')
    def test_save_all_results_fallback(self, mock_client):
        saver = GeospatialMongoSaver("test_process")
        # Simulate connection failure initially but success later? 
        # Or just test logic.
        saver.mongodb_available = True
        saver.collection = MagicMock()
        saver.collection.count_documents.return_value = 0 # No existing data
        saver.collection.insert_many.return_value.inserted_ids = [1]
        
        results = [{"data": "test"}]
        success = saver.save_all_results(results)
        
        assert success is True
        saver.collection.insert_many.assert_called()

# =============================================================================
# Test CRSOverlapFixer
# =============================================================================

class TestCRSOverlapFixer:
    @pytest.fixture
    def fixer(self):
        return CRSOverlapFixer("test_fix_process")

    def test_check_bounds_overlap(self, fixer):
        # Overlapping boxes
        b1 = [0, 0, 10, 10]
        b2 = [5, 5, 15, 15]
        assert fixer.check_bounds_overlap(b1, b2) is True
        
        # Non-overlapping
        b3 = [20, 20, 30, 30]
        assert fixer.check_bounds_overlap(b1, b3) is False

    @patch('app.geospatial_merger.processors.crs_overlap_fixer.gpd.read_file')
    @patch('app.geospatial_merger.processors.crs_overlap_fixer.rasterio.open')
    def test_diagnose_and_fix_flow(self, mock_raster_open, mock_read_file, fixer):
        # Mock raster
        mock_src = MagicMock()
        mock_bounds = MagicMock()
        mock_bounds.left = 0
        mock_bounds.bottom = 0
        mock_bounds.right = 10
        mock_bounds.top = 10
        mock_bounds.__iter__.side_effect = lambda: iter((0, 0, 10, 10))
        mock_src.bounds = mock_bounds
        mock_src.crs = "EPSG:4326"
        mock_src.__enter__.return_value = mock_src
        mock_src.__exit__.return_value = None
        mock_raster_open.return_value = mock_src
        
        # Mock GeoDataFrame
        mock_gdf = MagicMock()
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.total_bounds = [2, 2, 8, 8] # Inside raster
        mock_gdf.to_crs.return_value = mock_gdf # Mock no-op conversion
        mock_read_file.return_value = mock_gdf
        
        # Mock overlap check to return True
        with patch.object(fixer, 'check_bounds_overlap', return_value=True):
            results = fixer.diagnose_and_fix("fake.geojson", "fake.tif")
            
        assert results['success'] is True
        assert results['final_crs'] == "EPSG:4326"

# =============================================================================
# Test GeospatialBatchProcessor
# =============================================================================

class TestGeospatialBatchProcessor:
    @pytest.fixture
    def processor(self):
        with patch('app.geospatial_merger.processors.batch_processor.create_geospatial_saver') as mock_create:
            mock_saver = MagicMock()
            mock_saver.is_connected.return_value = True
            mock_create.return_value = mock_saver
            proc = GeospatialBatchProcessor("test_batch_process")
            return proc

    @patch('app.geospatial_merger.processors.batch_processor.fix_crs_overlap_issues')
    @patch('app.geospatial_merger.processors.batch_processor.gpd.read_file')
    @patch('app.geospatial_merger.processors.batch_processor.rasterio.open')
    def test_process_files_flow(self, mock_raster_open, mock_read_file, mock_fix_crs, processor):
        # Setup Fix CRS Mock
        mock_fix_crs.return_value = {
            "success": True,
            "fixed_boundaries_path": "fixed.geojson",  # CORRECT KEY
            "fixed_slope_path": "fixed.tif",
            "coordinate_info": {"overlap": "yes"}
        }
        
        # Setup GDF Mock
        mock_gdf = MagicMock()
        mock_gdf.__len__.return_value = 1
        mock_gdf.crs = "EPSG:4326"
        # Create a real geometry for mapping() to work
        poly = Polygon([(0,0), (1,1), (1,0)])
        mock_gdf.geometry = [poly] # Support access
        mock_gdf.iloc.__getitem__.return_value = mock_gdf # Slicing returns itself
        
        # Correctly mock iterrows to yield index and row with geometry
        row_mock = MagicMock()
        row_mock.geometry = poly
        # Mocking __getitem__ on row to handle string col access
        def row_getitem(key):
             if key == 'geometry': return poly
             return "some_val"
        row_mock.__getitem__.side_effect = row_getitem
        
        mock_gdf.iterrows.side_effect = lambda: iter([(0, row_mock)])
        
        mock_read_file.return_value = mock_gdf
        
        # Setup Raster Mock
        mock_src = MagicMock()
        mock_src.read.return_value = np.array([[10, 20], [30, 40]]) # valid slope data
        mock_src.nodata = -9999
        mock_src.crs = "EPSG:4326"
        mock_raster_open.return_value = mock_src
        
        # Run
        with patch('app.geospatial_merger.processors.batch_processor.rasterio.mask.mask') as mock_mask:
             # Mask returns data and transform
             mock_mask.return_value = (np.array([[[15]]]), None) 
             
             processor.process_files("orig.geojson", "orig.tif")
             
        assert processor.file_stats["processed_features"] == 1
        assert processor.file_stats["failed_features"] == 0
        processor.mongo_saver.save_batch_results.assert_called()

    def test_classify_slope(self, processor):
        assert "Flat" in processor.classify_slope(2.0)
        assert "Moderate" in processor.classify_slope(10.0)
        assert "Steep" in processor.classify_slope(20.0)
        assert "Very Steep" in processor.classify_slope(45.0)
