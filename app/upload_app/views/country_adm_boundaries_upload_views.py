import os
import zipfile
import tempfile
import geopandas as gpd
import json
import logging
import uuid
import re
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logger = logging.getLogger(__name__)

def create_shapefile_collection_name(country, shapefile_type, year):
    """
    Create a standardized collection name for shapefiles
    Format: shapefile-{country}-{type}-{year}
    """
    # Clean and normalize the names
    clean_country = re.sub(r'[^a-zA-Z0-9]', '', country.lower())
    clean_type = re.sub(r'[^a-zA-Z0-9]', '', shapefile_type.lower())
    
    collection_name = f"shapefile-{clean_country}-{clean_type}-{year}"
    return collection_name

def detect_village_level_shapefile(tmpdirname):
    """
    Detect village-level shapefiles by analyzing file names and content
    Prioritizes files that likely contain village/lowest administrative level data
    """
    shp_files = []
    
    # Find all .shp files
    for root, dirs, files in os.walk(tmpdirname):
        for file in files:
            if file.endswith('.shp'):
                shp_file_path = os.path.join(root, file)
                shp_files.append({
                    'path': shp_file_path,
                    'filename': file,
                    'priority': 0
                })
    
    if not shp_files:
        return None, []
    
    # Priority keywords for village/lowest admin level (case insensitive)
    village_keywords = [
        'village', 'villages', 'settlements', 'locality', 'localities',
        'ward', 'wards', 'commune', 'communes', 'parish', 'parishes',
        'hamlet', 'hamlets', 'town', 'towns', 'community', 'communities',
        'adm4', 'adm5', 'level4', 'level5', 'lowest', 'smallest',
        'cell', 'cells', 'sector', 'sectors'  # Common in Rwanda
    ]
    
    # Assign priority scores based on filename
    for shp_info in shp_files:
        filename_lower = shp_info['filename'].lower()
        for keyword in village_keywords:
            if keyword in filename_lower:
                shp_info['priority'] += 10
                logger.info(f"Village keyword '{keyword}' found in {shp_info['filename']}")
    
    # Sort by priority (highest first)
    shp_files.sort(key=lambda x: x['priority'], reverse=True)
    
    # Try to analyze content to confirm village level
    for shp_info in shp_files:
        try:
            # Quick read to check attributes
            gdf_sample = gpd.read_file(shp_info['path'])
            if not gdf_sample.empty:
                columns = [col.lower() for col in gdf_sample.columns]
                
                # Look for village name columns
                village_name_indicators = [
                    'village', 'village_name', 'villagename', 'vill_name',
                    'settlement', 'locality', 'community', 'ward',
                    'name', 'nom', 'nome', 'nazwa', 'cell_name',  # Different languages
                    'sector_name', 'commune_name'
                ]
                
                for indicator in village_name_indicators:
                    if any(indicator in col for col in columns):
                        shp_info['priority'] += 20
                        logger.info(f"Village name column indicator '{indicator}' found in {shp_info['filename']}")
                        break
                
                # Check feature count - villages usually have more features than higher admin levels
                feature_count = len(gdf_sample)
                if feature_count > 100:  # Likely village level if many features
                    shp_info['priority'] += 5
                elif feature_count > 500:
                    shp_info['priority'] += 10
                
        except Exception as e:
            logger.warning(f"Could not analyze {shp_info['filename']}: {str(e)}")
            continue
    
    # Re-sort after content analysis
    shp_files.sort(key=lambda x: x['priority'], reverse=True)
    
    # Return the highest priority shapefile
    selected_shp = shp_files[0]['path']
    logger.info(f"Selected village-level shapefile: {shp_files[0]['filename']} (priority: {shp_files[0]['priority']})")
    
    return selected_shp, shp_files

def identify_village_name_column(gdf):
    """
    Identify the column that most likely contains village names
    """
    columns = gdf.columns.tolist()
    column_priorities = {}
    
    # Priority keywords for village name columns
    village_name_keywords = [
        'village', 'village_name', 'villagename', 'vill_name',
        'settlement', 'settlement_name', 'locality', 'locality_name',
        'community', 'community_name', 'ward', 'ward_name',
        'name', 'nom', 'nome', 'nazwa',  # Different languages
        'cell', 'cell_name', 'sector', 'sector_name',  # Rwanda specific
        'commune', 'commune_name', 'parish', 'parish_name'
    ]
    
    for col in columns:
        col_lower = col.lower()
        priority = 0
        
        # Exact matches get highest priority
        if col_lower in village_name_keywords:
            priority += 100
        
        # Partial matches
        for keyword in village_name_keywords:
            if keyword in col_lower:
                priority += 50
                break
        
        # General name indicators
        if 'name' in col_lower:
            priority += 20
        if 'nom' in col_lower:  # French
            priority += 20
        
        # Avoid geometry and technical columns
        if col_lower in ['geometry', 'geom', 'shape', 'objectid', 'fid', 'id']:
            priority = 0
        
        column_priorities[col] = priority
    
    # Find the column with highest priority
    if column_priorities:
        village_name_column = max(column_priorities, key=column_priorities.get)
        if column_priorities[village_name_column] > 0:
            logger.info(f"Identified village name column: {village_name_column} (priority: {column_priorities[village_name_column]})")
            return village_name_column
    
    # Fallback: look for any column with 'name' in it
    name_columns = [col for col in columns if 'name' in col.lower()]
    if name_columns:
        logger.info(f"Using fallback name column: {name_columns[0]}")
        return name_columns[0]
    
    logger.warning("No village name column identified")
    return None

class UploadShapefileCountryView(APIView):
    def post(self, request):
        file = request.FILES.get('file')
        
        if not file or not file.name.endswith('.zip'):
            return Response({'error': 'Please upload a .zip file.'}, status=status.HTTP_400_BAD_REQUEST)
        
        # Extract metadata from request
        dataset_name = request.data.get('dataset_name', '')
        country = request.data.get('country', '')
        shapefile_type = request.data.get('shapefile_type', 'villages')  # Default to villages
        year = request.data.get('year', '')
        source = request.data.get('source', '')
        description = request.data.get('description', '')
        
        # Validate required metadata
        if not all([dataset_name, country, year]):
            return Response({
                "error": "Missing required metadata. Please provide: dataset_name, country, year"
            }, status=status.HTTP_400_BAD_REQUEST)

        # Save uploaded zip file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            for chunk in file.chunks():
                tmp_zip.write(chunk)
            tmp_zip_path = tmp_zip.name

        try:
            # Generate unique upload ID and timestamp
            upload_id = str(uuid.uuid4())
            upload_date = datetime.now()
            
            # Create dynamic collection name
            data_collection_name = create_shapefile_collection_name(country, shapefile_type, year)
            metadata_collection_name = f"{data_collection_name}_metadata"
            
            # Check if this combination already exists and handle replacement
            replacement_info = self.check_and_handle_existing_data(
                data_collection_name, 
                metadata_collection_name,
                country, 
                shapefile_type, 
                year
            )
            
            # Process village-level shapefile with metadata
            processing_result = self.process_village_shapefile_zip_and_store(
                tmp_zip_path, 
                upload_id,
                data_collection_name,
                metadata_collection_name,
                {
                    "upload_id": upload_id,
                    "dataset_name": dataset_name,
                    "country": country,
                    "shapefile_type": shapefile_type,
                    "year": int(year),
                    "source": source,
                    "description": description,
                    "upload_date": upload_date,
                    "original_filename": file.name,
                    "file_size": file.size,
                    "data_type": "village_shapefile"
                }
            )
            
            response_data = {
                'message': 'Village-level shapefile uploaded and stored successfully.',
                'upload_id': upload_id,
                'features_inserted': processing_result['features_count'],
                'village_analysis': processing_result['village_analysis'],
                'collection_info': {
                    'data_collection': data_collection_name,
                    'metadata_collection': metadata_collection_name
                },
                'metadata': {
                    'dataset_name': dataset_name,
                    'country': country,
                    'shapefile_type': shapefile_type,
                    'year': year,
                    'source': source,
                    'upload_date': upload_date.isoformat(),
                    'features_count': processing_result['features_count'],
                    'village_name_column': processing_result['village_analysis']['village_name_column'],
                    'sample_village_names': processing_result['village_analysis']['sample_village_names']
                }
            }
            
            # Add replacement information to response
            if replacement_info['replaced']:
                response_data['replacement_info'] = replacement_info
                response_data['message'] = 'Village-level shapefile uploaded successfully, replacing existing data.'
            
            return Response(response_data, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Error processing village shapefile: {e}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            os.remove(tmp_zip_path)

    def check_and_handle_existing_data(self, data_collection_name, metadata_collection_name, country, shapefile_type, year):
        """
        Check if data for this combination already exists and remove it if found
        """
        try:
            client = MongoClient(settings.MONGO_SHAPEFILE_URI)
            db = client[settings.MONGO_SHAPEFILE_DB]
            
            replacement_info = {
                'replaced': False,
                'previous_upload_ids': [],
                'features_removed': 0,
                'metadata_removed': 0
            }
            
            # Check if collections exist
            all_collections = db.list_collection_names()
            
            if data_collection_name in all_collections:
                # Collection exists, check for existing data
                data_collection = db[data_collection_name]
                
                # Find existing records for this country/type/year combination
                existing_query = {
                    '_country': country,
                    '_shapefile_type': shapefile_type,
                    '_year': int(year)
                }
                
                # Get upload IDs of existing data before deletion
                existing_records = data_collection.find(existing_query, {'_upload_id': 1})
                existing_upload_ids = list(set([record.get('_upload_id') for record in existing_records if record.get('_upload_id')]))
                
                if existing_upload_ids:
                    # Remove existing data
                    delete_result = data_collection.delete_many(existing_query)
                    replacement_info['features_removed'] = delete_result.deleted_count
                    replacement_info['previous_upload_ids'] = existing_upload_ids
                    replacement_info['replaced'] = True
                    
                    logger.info(f"Removed {delete_result.deleted_count} existing features for {country}-{shapefile_type}-{year}")
            
            # Check and clean metadata collection
            if metadata_collection_name in all_collections:
                metadata_collection = db[metadata_collection_name]
                
                # Remove existing metadata for this combination
                metadata_query = {
                    'country': country,
                    'shapefile_type': shapefile_type,
                    'year': int(year)
                }
                
                metadata_delete_result = metadata_collection.delete_many(metadata_query)
                replacement_info['metadata_removed'] = metadata_delete_result.deleted_count
                
                if metadata_delete_result.deleted_count > 0:
                    replacement_info['replaced'] = True
                    logger.info(f"Removed {metadata_delete_result.deleted_count} existing metadata records for {country}-{shapefile_type}-{year}")
            
            client.close()
            return replacement_info
            
        except Exception as e:
            logger.error(f"Error checking/removing existing data: {str(e)}")
            # If there's an error, continue with upload but log the issue
            return {
                'replaced': False,
                'error': str(e),
                'previous_upload_ids': [],
                'features_removed': 0,
                'metadata_removed': 0
            }

    def process_village_shapefile_zip_and_store(self, zip_path, upload_id, data_collection_name, metadata_collection_name, metadata):
        """
        Enhanced processing specifically for village-level shapefiles
        """
        if not zipfile.is_zipfile(zip_path):
            raise ValueError("Provided file is not a valid zip archive.")

        with tempfile.TemporaryDirectory() as tmpdirname:
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(tmpdirname)
            except zipfile.BadZipFile:
                raise ValueError("Corrupted zip file.")
            
            logger.info(f"Extracted files to: {tmpdirname}")
            
            # Detect village-level shapefile
            village_shp_file, all_shp_files = detect_village_level_shapefile(tmpdirname)
            
            if not village_shp_file:
                raise FileNotFoundError("No .shp file found in the ZIP archive.")
            
            logger.info(f"Processing village-level shapefile: {village_shp_file}")

            # Read shapefile
            try:
                gdf = gpd.read_file(village_shp_file)
                logger.info(f"Successfully read village shapefile with {len(gdf)} features")
            except Exception as e:
                raise ValueError(f"Error reading village shapefile: {str(e)}")
            
            # Identify village name column
            village_name_column = identify_village_name_column(gdf)
            
            # Analyze village data
            village_analysis = {
                'total_villages': len(gdf),
                'village_name_column': village_name_column,
                'sample_village_names': [],
                'all_columns': gdf.columns.tolist(),
                'geometry_type': str(gdf.geom_type.iloc[0]) if not gdf.empty else "unknown",
                'crs': str(gdf.crs) if gdf.crs else "unknown",
                'selected_shapefile': os.path.basename(village_shp_file),
                'available_shapefiles': [{'filename': os.path.basename(shp['path']), 'priority': shp['priority']} for shp in all_shp_files]
            }
            
            # Get sample village names
            if village_name_column and village_name_column in gdf.columns:
                sample_names = gdf[village_name_column].dropna().head(10).tolist()
                village_analysis['sample_village_names'] = sample_names
                logger.info(f"Sample village names: {sample_names[:5]}...")
            
            # Convert to GeoJSON
            try:
                geojson_dict = json.loads(gdf.to_json())
                features = geojson_dict['features']
                logger.info(f"Converted village data to GeoJSON with {len(features)} features")
            except Exception as e:
                raise ValueError(f"Error converting village data to GeoJSON: {str(e)}")
            
            # Add metadata to each feature
            for i, feature in enumerate(features):
                feature['_upload_id'] = upload_id
                feature['_dataset_name'] = metadata['dataset_name']
                feature['_country'] = metadata['country']
                feature['_shapefile_type'] = metadata['shapefile_type']
                feature['_year'] = metadata['year']
                feature['_source'] = metadata['source']
                feature['_upload_date'] = metadata['upload_date']
                feature['_data_type'] = "village_shapefile"
                feature['_collection_name'] = data_collection_name
                feature['_village_index'] = i + 1
                
                # Add village name if identified
                if village_name_column and village_name_column in gdf.columns:
                    village_name = gdf.iloc[i][village_name_column] if i < len(gdf) else None
                    feature['_village_name'] = str(village_name) if village_name is not None else None
                    feature['_village_name_column'] = village_name_column

            # Update metadata with village analysis
            metadata['features_count'] = len(features)
            metadata['columns'] = gdf.columns.tolist()
            metadata['geometry_type'] = village_analysis['geometry_type']
            metadata['crs'] = village_analysis['crs']
            metadata['collection_name'] = data_collection_name
            metadata['village_name_column'] = village_name_column
            metadata['village_analysis'] = village_analysis

            # Connect to MongoDB and store data
            client = MongoClient(settings.MONGO_SHAPEFILE_URI)
            db = client[settings.MONGO_SHAPEFILE_DB]
            
            # Insert features into data collection
            features_count = self.upload_geojson_to_mongo(db, features, data_collection_name)
            
            # Insert metadata
            metadata_collection = db[metadata_collection_name]
            metadata_collection.insert_one(metadata)
            
            client.close()
            
            return {
                'features_count': features_count,
                'village_analysis': village_analysis
            }

    def upload_geojson_to_mongo(self, db, features, collection_name, batch_size=500):
        try:
            if not features:
                logger.warning("No village features to upload.")
                return 0

            collection = db[collection_name]
            total_inserted = 0

            for i in range(0, len(features), batch_size):
                batch = features[i:i+batch_size]
                try:
                    result = collection.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                    logger.info(f"Inserted village batch {i // batch_size + 1}: {len(result.inserted_ids)} features")
                except BulkWriteError as bwe:
                    # Count successful inserts even with some errors
                    total_inserted += bwe.details.get('nInserted', 0)
                    logger.warning(f"Bulk insert error in village batch {i // batch_size + 1}: {bwe.details}")
            
            logger.info(f"Successfully inserted {total_inserted} village features into {collection_name}")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error processing village GeoJSON data: {e}")
            raise


# Keep existing views for compatibility
class ShapefileDataExtractionView(APIView):
    """
    View to extract shapefile data based on metadata filters
    """
    def get(self, request):
        # Get filter parameters
        dataset_name = request.query_params.get('dataset_name')
        country = request.query_params.get('country')
        shapefile_type = request.query_params.get('shapefile_type')
        year = request.query_params.get('year')
        source = request.query_params.get('source')
        upload_id = request.query_params.get('upload_id')
        collection_name = request.query_params.get('collection_name')
        village_name = request.query_params.get('village_name')  # New filter for villages
        
        try:
            client = MongoClient(settings.MONGO_SHAPEFILE_URI)
            db = client[settings.MONGO_SHAPEFILE_DB]
            
            # Determine collections to search
            if collection_name:
                collections_to_search = [collection_name]
            elif country and shapefile_type and year:
                specific_collection = create_shapefile_collection_name(country, shapefile_type, year)
                collections_to_search = [specific_collection]
            else:
                # Search all shapefile collections
                all_collections = db.list_collection_names()
                collections_to_search = [col for col in all_collections if col.startswith('shapefile-') and not col.endswith('_metadata')]
            
            all_features = []
            collections_searched = []
            
            for collection_name in collections_to_search:
                try:
                    collection = db[collection_name]
                    
                    # Build query filter
                    query_filter = {}
                    if dataset_name:
                        query_filter['_dataset_name'] = dataset_name
                    if country:
                        query_filter['_country'] = country
                    if shapefile_type:
                        query_filter['_shapefile_type'] = shapefile_type
                    if year:
                        query_filter['_year'] = int(year)
                    if source:
                        query_filter['_source'] = source
                    if upload_id:
                        query_filter['_upload_id'] = upload_id
                    if village_name:
                        # Search for village name using regex for partial matching
                        query_filter['_village_name'] = {'$regex': village_name, '$options': 'i'}
                    
                    # Execute query
                    cursor = collection.find(query_filter)
                    features = list(cursor)
                    
                    # Remove ObjectId for JSON serialization
                    for feature in features:
                        if '_id' in feature:
                            feature['_id'] = str(feature['_id'])
                        if '_upload_date' in feature:
                            feature['_upload_date'] = feature['_upload_date'].isoformat()
                    
                    all_features.extend(features)
                    collections_searched.append(collection_name)
                    
                except Exception as collection_error:
                    logger.warning(f"Error searching collection {collection_name}: {str(collection_error)}")
                    continue
            
            client.close()
            
            return Response({
                "total_features": len(all_features),
                "collections_searched": collections_searched,
                "filters_applied": {
                    "_dataset_name": dataset_name,
                    "_country": country,
                    "_shapefile_type": shapefile_type,
                    "_year": int(year) if year else None,
                    "_source": source,
                    "_upload_id": upload_id,
                    "_village_name": village_name
                },
                "data": all_features
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to extract shapefile data: {str(e)}")
            return Response({"error": f"Failed to extract shapefile data: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class ShapefileMetadataListView(APIView):
    """
    View to list all available shapefile datasets with their metadata
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_SHAPEFILE_URI)
            db = client[settings.MONGO_SHAPEFILE_DB]
            
            # Get all metadata collections
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'shapefile-' in col]
            
            all_metadata = []
            
            for metadata_collection_name in metadata_collections:
                try:
                    metadata_collection = db[metadata_collection_name]
                    
                    # Get all metadata records from this collection
                    cursor = metadata_collection.find({})
                    metadata_records = list(cursor)
                    
                    # Remove ObjectId and format dates for JSON serialization
                    for record in metadata_records:
                        if '_id' in record:
                            record['_id'] = str(record['_id'])
                        if 'upload_date' in record:
                            record['upload_date'] = record['upload_date'].isoformat()
                        record['source_metadata_collection'] = metadata_collection_name
                    
                    all_metadata.extend(metadata_records)
                    
                except Exception as collection_error:
                    logger.warning(f"Error reading metadata collection {metadata_collection_name}: {str(collection_error)}")
                    continue
            
            client.close()
            
            # Group metadata by country and type
            grouped_metadata = {}
            for record in all_metadata:
                country = record.get('country', 'unknown')
                shapefile_type = record.get('shapefile_type', 'unknown')
                year = record.get('year', 'unknown')
                key = f"{country}-{shapefile_type}-{year}"
                
                if key not in grouped_metadata:
                    grouped_metadata[key] = {
                        "country": country,
                        "shapefile_type": shapefile_type,
                        "year": year,
                        "datasets": []
                    }
                grouped_metadata[key]["datasets"].append(record)
            
            return Response({
                "total_datasets": len(all_metadata),
                "total_country_type_year_groups": len(grouped_metadata),
                "metadata_collections_found": len(metadata_collections),
                "grouped_by_country_type_year": grouped_metadata,
                "all_datasets": all_metadata
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to retrieve shapefile metadata: {str(e)}")
            return Response({"error": f"Failed to retrieve shapefile metadata: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DeleteShapefileDatasetView(APIView):
    """
    View to delete a specific shapefile dataset by upload_id
    """
    def delete(self, request, upload_id):
        try:
            client = MongoClient(settings.MONGO_SHAPEFILE_URI)
            db = client[settings.MONGO_SHAPEFILE_DB]
            
            # Find which collection contains this upload_id by searching metadata
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'shapefile-' in col]
            
            target_data_collection = None
            target_metadata_collection = None
            
            # Search through metadata collections to find the upload_id
            for metadata_collection_name in metadata_collections:
                metadata_collection = db[metadata_collection_name]
                metadata_record = metadata_collection.find_one({"upload_id": upload_id})
                
                if metadata_record:
                    target_metadata_collection = metadata_collection_name
                    target_data_collection = metadata_collection_name.replace('_metadata', '')
                    break
            
            if not target_data_collection:
                return Response({"error": "Shapefile dataset not found"}, status=status.HTTP_404_NOT_FOUND)
            
            # Delete data records from the specific collection
            data_collection = db[target_data_collection]
            data_result = data_collection.delete_many({"_upload_id": upload_id})
            
            # Delete metadata record
            metadata_collection = db[target_metadata_collection]
            metadata_result = metadata_collection.delete_one({"upload_id": upload_id})
            
            client.close()
            
            return Response({
                "message": "Shapefile dataset deleted successfully",
                "upload_id": upload_id,
                "features_deleted": data_result.deleted_count,
                "metadata_deleted": metadata_result.deleted_count,
                "collections_affected": {
                    "data_collection": target_data_collection,
                    "metadata_collection": target_metadata_collection
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to delete shapefile dataset: {str(e)}")
            return Response({"error": f"Failed to delete shapefile dataset: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)