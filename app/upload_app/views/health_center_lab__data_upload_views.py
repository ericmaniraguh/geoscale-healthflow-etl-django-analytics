
# health_center_views.py - Make sure this file contains these exact class names

import pandas as pd
import chardet
import logging
from io import StringIO
from datetime import datetime
import uuid
import re
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from pymongo import MongoClient

logger = logging.getLogger(__name__)

def create_collection_name(district, sector, year):
    """
    Create a standardized collection name based on district, sector, and year
    Format: healthcenter-data-{district}-{sector}-{year}
    """
    # Clean and normalize the names (remove spaces, special chars, convert to lowercase)
    clean_district = re.sub(r'[^a-zA-Z0-9]', '', district.lower())
    clean_sector = re.sub(r'[^a-zA-Z0-9]', '', sector.lower())
    
    collection_name = f"healthcenter-data-{clean_district}-{clean_sector}-{year}"
    return collection_name

class UploadHealthCenterLabDataView(APIView):
    def post(self, request):
        file = request.FILES.get('file')
        
        if not file:
            return Response({"error": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)
        
        # Extract metadata from request (now including sector)
        dataset_name = request.data.get('dataset_name', '')
        district = request.data.get('district', '')
        sector = request.data.get('sector', '')  # NEW: Added sector
        health_center = request.data.get('health_center', '')
        year = request.data.get('year', '')
        
        # Validate required metadata (now including sector)
        if not all([dataset_name, district, sector, health_center, year]):
            return Response({
                "error": "Missing required metadata. Please provide: dataset_name, district, sector, health_center, year"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            filename = file.name.lower()
            
            # Process file based on type
            if filename.endswith('.csv'):
                # Read raw bytes from file
                file_bytes = file.read()
                
                # Detect encoding
                detected = chardet.detect(file_bytes)
                encoding = detected['encoding']
                
                if not encoding:
                    return Response({"error": "Could not detect file encoding."}, status=status.HTTP_400_BAD_REQUEST)
                
                # Decode bytes using detected encoding and wrap in StringIO
                decoded_file = StringIO(file_bytes.decode(encoding))
                
                # Read into DataFrame
                df = pd.read_csv(decoded_file)
                
            elif filename.endswith(('.xls', '.xlsx')):
                df = pd.read_excel(file)
                
            else:
                return Response({"error": "Unsupported file format. Upload a CSV or Excel file."},
                              status=status.HTTP_400_BAD_REQUEST)
            
            if df.empty:
                return Response({"error": "Health Center (Lab Records) uploaded file is empty."}, 
                              status=status.HTTP_400_BAD_REQUEST)
            
            # Generate unique upload ID and timestamp
            upload_id = str(uuid.uuid4())
            upload_date = datetime.now()
            
            # Create dynamic collection name based on district, sector, and year
            data_collection_name = create_collection_name(district, sector, year)
            metadata_collection_name = f"{data_collection_name}_metadata"
            
            # Create metadata document (now including sector)
            metadata = {
                "upload_id": upload_id,
                "dataset_name": dataset_name,
                "district": district,
                "sector": sector,  # NEW: Added sector to metadata
                "health_center": health_center,
                "year": int(year),
                "upload_date": upload_date,
                "original_filename": file.name,
                "file_size": file.size,
                "records_count": len(df),
                "columns": list(df.columns),
                "data_type": "health_center_lab_records",
                "collection_name": data_collection_name  # Store collection name for reference
            }
            
            # Add metadata to each record (now including sector)
            records = df.to_dict(orient='records')
            for record in records:
                record['_upload_id'] = upload_id
                record['_dataset_name'] = dataset_name
                record['_district'] = district
                record['_sector'] = sector  # NEW: Added sector to each record
                record['_health_center'] = health_center
                record['_year'] = int(year)
                record['_upload_date'] = upload_date
                record['_data_type'] = "health_center_lab_records"
                record['_collection_name'] = data_collection_name
            
            # MongoDB operations with dynamic collection names
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            
            # Insert data records into dynamic collection
            data_collection = db[data_collection_name]
            data_result = data_collection.insert_many(records)
            
            # Insert metadata record into dynamic metadata collection
            metadata_collection = db[metadata_collection_name]
            metadata_result = metadata_collection.insert_one(metadata)
            
            client.close()
            
            return Response({
                "message": "Health Center (Lab Records) Data uploaded successfully",
                "upload_id": upload_id,
                "records_inserted": len(data_result.inserted_ids),
                "collection_info": {
                    "data_collection": data_collection_name,
                    "metadata_collection": metadata_collection_name
                },
                "metadata": {
                    "dataset_name": dataset_name,
                    "district": district,
                    "sector": sector,  # NEW: Include sector in response
                    "health_center": health_center,
                    "year": year,
                    "upload_date": upload_date.isoformat(),
                    "records_count": len(df)
                }
            }, status=status.HTTP_201_CREATED)
            
        except Exception as e:
            logger.error(f"Failed to process file: {str(e)}")
            return Response({"error": f"Failed to process file: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DataExtractionView(APIView):
    """
    View to extract data based on metadata filters with dynamic collection support
    """
    def get(self, request):
        # Get filter parameters (now including sector)
        dataset_name = request.query_params.get('dataset_name')
        district = request.query_params.get('district')
        sector = request.query_params.get('sector')  # NEW: Added sector filter
        health_center = request.query_params.get('health_center')
        year = request.query_params.get('year')
        upload_id = request.query_params.get('upload_id')
        collection_name = request.query_params.get('collection_name')  # NEW: Allow direct collection specification
        
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            
            # If specific collection is provided, use it; otherwise search all collections
            if collection_name:
                collections_to_search = [collection_name]
            elif district and sector and year:
                # Create collection name from parameters
                specific_collection = create_collection_name(district, sector, year)
                collections_to_search = [specific_collection]
            else:
                # Search all health center data collections
                all_collections = db.list_collection_names()
                collections_to_search = [col for col in all_collections if col.startswith('healthcenter-data-') and not col.endswith('_metadata')]
            
            all_records = []
            collections_searched = []
            
            for collection_name in collections_to_search:
                try:
                    collection = db[collection_name]
                    
                    # Build query filter (now including sector)
                    query_filter = {}
                    if dataset_name:
                        query_filter['_dataset_name'] = dataset_name
                    if district:
                        query_filter['_district'] = district
                    if sector:
                        query_filter['_sector'] = sector  # NEW: Added sector filter
                    if health_center:
                        query_filter['_health_center'] = health_center
                    if year:
                        query_filter['_year'] = int(year)
                    if upload_id:
                        query_filter['_upload_id'] = upload_id
                    
                    # Execute query
                    cursor = collection.find(query_filter)
                    records = list(cursor)
                    
                    # Remove ObjectId for JSON serialization
                    for record in records:
                        if '_id' in record:
                            record['_id'] = str(record['_id'])
                        if '_upload_date' in record:
                            record['_upload_date'] = record['_upload_date'].isoformat()
                    
                    all_records.extend(records)
                    collections_searched.append(collection_name)
                    
                except Exception as collection_error:
                    logger.warning(f"Error searching collection {collection_name}: {str(collection_error)}")
                    continue
            
            client.close()
            
            return Response({
                "total_records": len(all_records),
                "collections_searched": collections_searched,
                "filters_applied": {
                    "_dataset_name": dataset_name,
                    "_district": district,
                    "_sector": sector,  # NEW: Include sector in response
                    "_health_center": health_center,
                    "_year": int(year) if year else None,
                    "_upload_id": upload_id
                },
                "data": all_records
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to extract data: {str(e)}")
            return Response({"error": f"Failed to extract data: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class MetadataListView(APIView):
    """
    View to list all available datasets with their metadata from all collections
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            
            # Get all metadata collections
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'healthcenter-data-' in col]
            
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
                        # Add source collection info
                        record['source_metadata_collection'] = metadata_collection_name
                    
                    all_metadata.extend(metadata_records)
                    
                except Exception as collection_error:
                    logger.warning(f"Error reading metadata collection {metadata_collection_name}: {str(collection_error)}")
                    continue
            
            client.close()
            
            # Group metadata by district, sector, and year for better organization
            grouped_metadata = {}
            for record in all_metadata:
                district = record.get('district', 'unknown')
                sector = record.get('sector', 'unknown')
                year = record.get('year', 'unknown')
                key = f"{district}-{sector}-{year}"
                
                if key not in grouped_metadata:
                    grouped_metadata[key] = {
                        "district": district,
                        "sector": sector,
                        "year": year,
                        "datasets": []
                    }
                grouped_metadata[key]["datasets"].append(record)
            
            return Response({
                "total_datasets": len(all_metadata),
                "total_district_sector_year_groups": len(grouped_metadata),
                "metadata_collections_found": len(metadata_collections),
                "grouped_by_location_year": grouped_metadata,
                "all_datasets": all_metadata
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to retrieve metadata: {str(e)}")
            return Response({"error": f"Failed to retrieve metadata: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DeleteDatasetView(APIView):
    """
    View to delete a specific dataset by upload_id with dynamic collection support
    """
    def delete(self, request, upload_id):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            
            # First, find which collection contains this upload_id by searching metadata
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'healthcenter-data-' in col]
            
            target_data_collection = None
            target_metadata_collection = None
            
            # Search through metadata collections to find the upload_id
            for metadata_collection_name in metadata_collections:
                metadata_collection = db[metadata_collection_name]
                metadata_record = metadata_collection.find_one({"upload_id": upload_id})
                
                if metadata_record:
                    target_metadata_collection = metadata_collection_name
                    # Get corresponding data collection name (remove _metadata suffix)
                    target_data_collection = metadata_collection_name.replace('_metadata', '')
                    break
            
            if not target_data_collection:
                return Response({"error": "Dataset not found"}, status=status.HTTP_404_NOT_FOUND)
            
            # Delete data records from the specific collection
            data_collection = db[target_data_collection]
            data_result = data_collection.delete_many({"_upload_id": upload_id})
            
            # Delete metadata record
            metadata_collection = db[target_metadata_collection]
            metadata_result = metadata_collection.delete_one({"upload_id": upload_id})
            
            client.close()
            
            return Response({
                "message": "Dataset deleted successfully",
                "upload_id": upload_id,
                "records_deleted": data_result.deleted_count,
                "metadata_deleted": metadata_result.deleted_count,
                "collections_affected": {
                    "data_collection": target_data_collection,
                    "metadata_collection": target_metadata_collection
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to delete dataset: {str(e)}")
            return Response({"error": f"Failed to delete dataset: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class CollectionListView(APIView):
    """
    NEW: View to list all health center collections and their structure
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_DB]
            
            # Get all collections
            all_collections = db.list_collection_names()
            
            # Filter health center collections
            data_collections = [col for col in all_collections if col.startswith('healthcenter-data-') and not col.endswith('_metadata')]
            metadata_collections = [col for col in all_collections if col.startswith('healthcenter-data-') and col.endswith('_metadata')]
            
            collection_info = []
            
            for data_collection in data_collections:
                collection = db[data_collection]
                metadata_collection_name = f"{data_collection}_metadata"
                
                # Get collection stats
                record_count = collection.count_documents({})
                
                # Get sample record to understand structure
                sample_record = collection.find_one({})
                
                # Parse collection name to extract district, sector, year
                parts = data_collection.replace('healthcenter-data-', '').split('-')
                if len(parts) >= 3:
                    district, sector, year = parts[0], parts[1], parts[2]
                else:
                    district, sector, year = "unknown", "unknown", "unknown"
                
                collection_info.append({
                    "data_collection": data_collection,
                    "metadata_collection": metadata_collection_name,
                    "district": district,
                    "sector": sector,
                    "year": year,
                    "record_count": record_count,
                    "has_metadata": metadata_collection_name in metadata_collections,
                    "sample_fields": list(sample_record.keys()) if sample_record else []
                })
            
            client.close()
            
            return Response({
                "total_data_collections": len(data_collections),
                "total_metadata_collections": len(metadata_collections),
                "collections": collection_info
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to list collections: {str(e)}")
            return Response({"error": f"Failed to list collections: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)