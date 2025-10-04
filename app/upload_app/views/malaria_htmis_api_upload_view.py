 
import pandas as pd
import logging
import chardet
import uuid
import re
from datetime import datetime
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from pymongo import MongoClient
from io import StringIO

logger = logging.getLogger(__name__)

def create_hmis_collection_name(district, health_facility, year):
    """
    Create a standardized collection name for HMIS data
    Format: hmis-data-{district}-{facility}-{year}
    """
    # Clean and normalize the names
    clean_district = re.sub(r'[^a-zA-Z0-9]', '', district.lower())
    clean_facility = re.sub(r'[^a-zA-Z0-9]', '', health_facility.lower())
    
    collection_name = f"hmis-data-{clean_district}-{clean_facility}-{year}"
    return collection_name

class UploadHMISAPIDataView(APIView):
    def post(self, request):
        file = request.FILES.get('file')
        
        if not file:
            return Response({"error": "No file uploaded"}, status=status.HTTP_400_BAD_REQUEST)
        
        # Extract metadata from request
        dataset_name = request.data.get('dataset_name', '')
        district = request.data.get('district', '')
        health_facility = request.data.get('health_facility', '')
        year = request.data.get('year', '')
        data_period = request.data.get('data_period', '')  # e.g., 'Q1', 'Q2', 'January', 'Annual'
        reporting_level = request.data.get('reporting_level', '')  # e.g., 'facility', 'district', 'national'
        source = request.data.get('source', '')  # e.g., 'HMIS_API', 'manual_entry', 'district_report'
        description = request.data.get('description', '')
        
        # Validate required metadata
        if not all([dataset_name, district, health_facility, year]):
            return Response({
                "error": "Missing required metadata. Please provide: dataset_name, district, health_facility, year"
            }, status=status.HTTP_400_BAD_REQUEST)
        
        try:
            # Generate unique upload ID and timestamp
            upload_id = str(uuid.uuid4())
            upload_date = datetime.now()
            
            # Create dynamic collection name
            data_collection_name = create_hmis_collection_name(district, health_facility, year)
            metadata_collection_name = f"{data_collection_name}_metadata"
            
            # Process file with metadata
            records_count = self.process_hmis_excel_or_csv_and_store(
                file,
                upload_id,
                data_collection_name,
                metadata_collection_name,
                {
                    "upload_id": upload_id,
                    "dataset_name": dataset_name,
                    "district": district,
                    "health_facility": health_facility,
                    "year": int(year),
                    "data_period": data_period,
                    "reporting_level": reporting_level,
                    "source": source,
                    "description": description,
                    "upload_date": upload_date,
                    "original_filename": file.name,
                    "file_size": file.size,
                    "data_type": "hmis_records"
                }
            )
            
            return Response({
                "message": "HMIS Data uploaded successfully",
                "upload_id": upload_id,
                "records_inserted": records_count,
                "collection_info": {
                    "data_collection": data_collection_name,
                    "metadata_collection": metadata_collection_name
                },
                "metadata": {
                    "dataset_name": dataset_name,
                    "district": district,
                    "health_facility": health_facility,
                    "year": year,
                    "data_period": data_period,
                    "reporting_level": reporting_level,
                    "source": source,
                    "upload_date": upload_date.isoformat(),
                    "records_count": records_count
                }
            }, status=status.HTTP_201_CREATED)
            
        except ValueError as ve:
            logger.error(f"Validation error: {str(ve)}")
            return Response({"error": str(ve)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Failed to process HMIS file: {str(e)}")
            return Response({"error": f"Failed to process file: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def process_hmis_excel_or_csv_and_store(self, file, upload_id, data_collection_name, metadata_collection_name, metadata):
        """
        Enhanced version with metadata support
        """
        filename = file.name.lower()
        
        # Handle CSV files with encoding detection
        if filename.endswith('.csv'):
            file_bytes = file.read()
            detected = chardet.detect(file_bytes)
            encoding = detected.get("encoding")
            
            if not encoding:
                raise ValueError("Could not detect encoding of CSV file.")
            
            decoded_file = StringIO(file_bytes.decode(encoding))
            df = pd.read_csv(decoded_file)
            
        # Handle Excel files
        elif filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file)
            
        else:
            raise ValueError("Unsupported file format. Only CSV and Excel files are allowed.")
        
        if df.empty:
            raise ValueError("Uploaded file is empty.")
        
        # Convert DataFrame to records and add metadata
        records = df.to_dict(orient='records')
        for record in records:
            record['_upload_id'] = upload_id
            record['_dataset_name'] = metadata['dataset_name']
            record['_district'] = metadata['district']
            record['_health_facility'] = metadata['health_facility']
            record['_year'] = metadata['year']
            record['_data_period'] = metadata['data_period']
            record['_reporting_level'] = metadata['reporting_level']
            record['_source'] = metadata['source']
            record['_upload_date'] = metadata['upload_date']
            record['_data_type'] = "hmis_records"
            record['_collection_name'] = data_collection_name
        
        # Update metadata with final record count and column info
        metadata['records_count'] = len(records)
        metadata['columns'] = list(df.columns.tolist())
        metadata['collection_name'] = data_collection_name
        
        # Connect to MongoDB and insert data
        client = MongoClient(settings.MONGO_URI)
        db = client[settings.MONGO_HMIS_DB]
        
        # Insert data records
        data_collection = db[data_collection_name]
        data_result = data_collection.insert_many(records)
        
        # Insert metadata
        metadata_collection = db[metadata_collection_name]
        metadata_collection.insert_one(metadata)
        
        client.close()
        
        return len(data_result.inserted_ids)


class HMISDataExtractionView(APIView):
    """
    View to extract HMIS data based on metadata filters
    """
    def get(self, request):
        # Get filter parameters
        dataset_name = request.query_params.get('dataset_name')
        district = request.query_params.get('district')
        health_facility = request.query_params.get('health_facility')
        year = request.query_params.get('year')
        data_period = request.query_params.get('data_period')
        reporting_level = request.query_params.get('reporting_level')
        source = request.query_params.get('source')
        upload_id = request.query_params.get('upload_id')
        collection_name = request.query_params.get('collection_name')
        
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_HMIS_DB]
            
            # Determine collections to search
            if collection_name:
                collections_to_search = [collection_name]
            elif district and health_facility and year:
                specific_collection = create_hmis_collection_name(district, health_facility, year)
                collections_to_search = [specific_collection]
            else:
                # Search all HMIS collections
                all_collections = db.list_collection_names()
                collections_to_search = [col for col in all_collections if col.startswith('hmis-data-') and not col.endswith('_metadata')]
            
            all_records = []
            collections_searched = []
            
            for collection_name in collections_to_search:
                try:
                    collection = db[collection_name]
                    
                    # Build query filter
                    query_filter = {}
                    if dataset_name:
                        query_filter['_dataset_name'] = dataset_name
                    if district:
                        query_filter['_district'] = district
                    if health_facility:
                        query_filter['_health_facility'] = health_facility
                    if year:
                        query_filter['_year'] = int(year)
                    if data_period:
                        query_filter['_data_period'] = data_period
                    if reporting_level:
                        query_filter['_reporting_level'] = reporting_level
                    if source:
                        query_filter['_source'] = source
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
                    "_health_facility": health_facility,
                    "_year": int(year) if year else None,
                    "_data_period": data_period,
                    "_reporting_level": reporting_level,
                    "_source": source,
                    "_upload_id": upload_id
                },
                "data": all_records
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to extract HMIS data: {str(e)}")
            return Response({"error": f"Failed to extract HMIS data: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class HMISMetadataListView(APIView):
    """
    View to list all available HMIS datasets with their metadata
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_HMIS_DB]
            
            # Get all metadata collections
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'hmis-data-' in col]
            
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
            
            # Group metadata by district, facility, and year
            grouped_metadata = {}
            for record in all_metadata:
                district = record.get('district', 'unknown')
                health_facility = record.get('health_facility', 'unknown')
                year = record.get('year', 'unknown')
                key = f"{district}-{health_facility}-{year}"
                
                if key not in grouped_metadata:
                    grouped_metadata[key] = {
                        "district": district,
                        "health_facility": health_facility,
                        "year": year,
                        "datasets": []
                    }
                grouped_metadata[key]["datasets"].append(record)
            
            return Response({
                "total_datasets": len(all_metadata),
                "total_district_facility_year_groups": len(grouped_metadata),
                "metadata_collections_found": len(metadata_collections),
                "grouped_by_district_facility_year": grouped_metadata,
                "all_datasets": all_metadata
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to retrieve HMIS metadata: {str(e)}")
            return Response({"error": f"Failed to retrieve HMIS metadata: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DeleteHMISDatasetView(APIView):
    """
    View to delete a specific HMIS dataset by upload_id
    """
    def delete(self, request, upload_id):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_HMIS_DB]
            
            # Find which collection contains this upload_id by searching metadata
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and 'hmis-data-' in col]
            
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
                return Response({"error": "HMIS dataset not found"}, status=status.HTTP_404_NOT_FOUND)
            
            # Delete data records from the specific collection
            data_collection = db[target_data_collection]
            data_result = data_collection.delete_many({"_upload_id": upload_id})
            
            # Delete metadata record
            metadata_collection = db[target_metadata_collection]
            metadata_result = metadata_collection.delete_one({"upload_id": upload_id})
            
            client.close()
            
            return Response({
                "message": "HMIS dataset deleted successfully",
                "upload_id": upload_id,
                "records_deleted": data_result.deleted_count,
                "metadata_deleted": metadata_result.deleted_count,
                "collections_affected": {
                    "data_collection": target_data_collection,
                    "metadata_collection": target_metadata_collection
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to delete HMIS dataset: {str(e)}")
            return Response({"error": f"Failed to delete HMIS dataset: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)