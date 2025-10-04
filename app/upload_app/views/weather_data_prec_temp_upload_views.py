

import pandas as pd
import logging
import chardet
import re
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from pymongo import MongoClient
from io import StringIO
from datetime import datetime
import uuid

logger = logging.getLogger(__name__)

# Updated helper function
def normalize_station_name(station_string):
    """
    Convert station names like 'Nyamata, Ruhuha and Juru' to 'nyamata_ruhuha_and_juru'
    """
    station_string = station_string.lower().strip()
    station_string = re.sub(r',\s*', '_', station_string)
    station_string = re.sub(r'\s+and\s+', '_and_', station_string)
    station_string = re.sub(r'\s+', '_', station_string)
    return station_string


# Modify this function to use cleaned station name

def create_weather_collection_name(data_type, station, years):
    """
    Create a standardized collection name based on data_type, station, and years
    Format: weather_{data_type}_{station}_{years_range}
    """
    import re
    # Clean and normalize the names (remove spaces, special chars, convert to lowercase)
    clean_data_type = re.sub(r'[^a-zA-Z0-9]', '', data_type.lower())
    clean_station = normalize_station_name(station)  # <- Use new cleaning logic here

    # Handle years - could be single year or multiple years
    if isinstance(years, list):
        years_sorted = sorted([int(y) for y in years])
        if len(years_sorted) == 1:
            years_str = str(years_sorted[0])
        else:
            years_str = f"{years_sorted[0]}to{years_sorted[-1]}"
    else:
        years_str = str(years)

    collection_name = f"weather_{clean_data_type}_{clean_station}_{years_str}"
    return collection_name


def parse_years_from_string(years_string):
    """
    Parse years from a comma-separated string
    Example: "2021, 2022, 2023" -> [2021, 2022, 2023]
    """
    if not years_string:
        return []
    
    # Split by comma and clean each year
    years_list = [year.strip() for year in years_string.split(',')]
    
    # Convert to integers and remove duplicates
    unique_years = list(set(int(year) for year in years_list if year.isdigit()))
    
    return sorted(unique_years)



def _process_weather_file(file, station, data_type, years, mongo_uri, db_name, metadata=None):
    """
    Enhanced weather file processing with dynamic collection naming and multi-year support
    """
    filename = file.name.lower()

    # Generate unique upload_id
    upload_id = str(uuid.uuid4())
    upload_time = datetime.utcnow()

    # Handle CSV
    if filename.endswith('.csv'):
        file_bytes = file.read()
        detected = chardet.detect(file_bytes)
        encoding = detected.get("encoding")
        if not encoding:
            raise ValueError("Could not detect encoding of CSV file.")
        decoded_file = StringIO(file_bytes.decode(encoding))
        df = pd.read_csv(decoded_file)

    # Handle Excel
    elif filename.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file)

    else:
        raise ValueError("Unsupported file format. Only CSV and Excel files are allowed.")

    if df.empty:
        raise ValueError("Uploaded file is empty.")

    # Parse years if it's a string
    if isinstance(years, str):
        years_list = parse_years_from_string(years)
    elif isinstance(years, list):
        years_list = sorted([int(y) for y in years])
    else:
        years_list = [int(years)]

    # Create dynamic collection name
    data_collection_name = create_weather_collection_name(data_type, station, years_list)
    metadata_collection_name = f"{data_collection_name}_metadata"

    # Add upload_id and metadata to each record
    records = df.to_dict(orient='records')
    if metadata is None:
        metadata = {}

    for record in records:
        record['_upload_id'] = upload_id
        record['_station'] = station
        record['_data_type'] = data_type
        record['_years'] = years_list  # Store as list of years
        record['_dataset_years'] = ", ".join(str(y) for y in years_list)  # Store as formatted string
        record['_upload_time'] = upload_time
        record['_collection_name'] = data_collection_name
        # Add other metadata fields
        for key, value in metadata.items():
            record[f'_{key}'] = value

    client = MongoClient(mongo_uri)
    db = client[db_name]

    # Insert records into dynamic collection
    collection = db[data_collection_name]
    result = collection.insert_many(records)

    # Prepare metadata info
    metadata_record = metadata.copy()
    metadata_record.update({
        "_id": upload_id,
        "upload_id": upload_id,
        "station": station,
        "data_type": data_type,
        "years": years_list,  # Store as list
        "dataset_years": ", ".join(str(y) for y in years_list),  # Store as formatted string
        "record_count": len(records),
        "upload_time": upload_time,
        "columns": list(df.columns),
        "data_collection_name": data_collection_name,
        "metadata_collection_name": metadata_collection_name,
        "original_filename": file.name,
        "file_size": file.size
    })

    # Save metadata in separate collection
    metadata_collection = db[metadata_collection_name]
    metadata_collection.insert_one(metadata_record)

    client.close()

    # Return info
    return {
        "upload_id": upload_id,
        "records_inserted": len(result.inserted_ids),
        "collection_info": {
            "data_collection": data_collection_name,
            "metadata_collection": metadata_collection_name
        },
        "metadata": metadata_record
    }

class UploadTemperatureView(APIView):
    def post(self, request):
        uploaded_file = request.FILES.get("temperature")
        if not uploaded_file:
            return Response({"error": "No file uploaded"}, status=400)

        # Extract required metadata
        station = request.data.get("station", "")
        years = request.data.get("years", "")  # Changed from "year" to "years"
        
        # Validate required metadata
        if not all([station, years]):
            return Response({
                "error": "Missing required metadata. Please provide: station, years"
            }, status=status.HTTP_400_BAD_REQUEST)

        # Parse years
        years_list = parse_years_from_string(years)
        if not years_list:
            return Response({
                "error": "Invalid years format. Please provide comma-separated years (e.g., '2021, 2022, 2023')"
            }, status=status.HTTP_400_BAD_REQUEST)

        metadata = {
            "dataset_name": request.data.get("dataset_name", "Temperature Dataset"),
            "uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
            "description": request.data.get("description", ""),
        }

        try:
            upload_info = _process_weather_file(
                uploaded_file,
                station=station,
                data_type="temperature",
                years=years_list,
                mongo_uri=settings.MONGO_URI,
                db_name=settings.MONGO_WEATHER_DB,
                metadata=metadata
            )
            
            # Return detailed info including upload_id, counts, metadata
            return Response({
                "message": "Temperature data uploaded with metadata",
                "upload_id": upload_info["upload_id"],
                "records_inserted": upload_info["records_inserted"],
                "collection_info": upload_info["collection_info"],
                "metadata": {
                    "dataset_name": upload_info["metadata"]["dataset_name"],
                    "station": station,
                    "data_type": "temperature",
                    "dataset_years": upload_info["metadata"]["dataset_years"],  # Changed from "year" to "dataset_years"
                    "upload_time": upload_info["metadata"]["upload_time"].isoformat(),
                    "records_count": upload_info["metadata"]["record_count"]
                }
            }, status=status.HTTP_201_CREATED)

        except ValueError as ve:
            return Response({"error": str(ve)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Failed to process temperature file: {str(e)}")
            return Response({"error": f"Failed to process temperature file: {str(e)}"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UploadPrecipitationView(APIView):
    def post(self, request):
        uploaded_file = request.FILES.get("precipitation")
        if not uploaded_file:
            return Response({"error": "No file uploaded"}, status=400)

        # Extract required metadata
        station = request.data.get("station", "")
        years = request.data.get("years", "")  # Changed from "year" to "years"
        
        # Validate required metadata
        if not all([station, years]):
            return Response({
                "error": "Missing required metadata. Please provide: station, years"
            }, status=status.HTTP_400_BAD_REQUEST)

        # Parse years
        years_list = parse_years_from_string(years)
        if not years_list:
            return Response({
                "error": "Invalid years format. Please provide comma-separated years (e.g., '2021, 2022, 2023')"
            }, status=status.HTTP_400_BAD_REQUEST)

        metadata = {
            "dataset_name": request.data.get("dataset_name", "Precipitation Dataset"),
            "uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
            "description": request.data.get("description", ""),
        }

        try:
            upload_info = _process_weather_file(
                uploaded_file,
                station=station,
                data_type="precipitation",
                years=years_list,
                mongo_uri=settings.MONGO_URI,
                db_name=settings.MONGO_WEATHER_DB,
                metadata=metadata
            )
            
            return Response({
                "message": "Precipitation data uploaded with metadata",
                "upload_id": upload_info["upload_id"],
                "records_inserted": upload_info["records_inserted"],
                "collection_info": upload_info["collection_info"],
                "metadata": {
                    "dataset_name": upload_info["metadata"]["dataset_name"],
                    "station": station,
                    "data_type": "precipitation",
                    "dataset_years": upload_info["metadata"]["dataset_years"],  # Changed from "year" to "dataset_years"
                    "upload_time": upload_info["metadata"]["upload_time"].isoformat(),
                    "records_count": upload_info["metadata"]["record_count"]
                }
            }, status=status.HTTP_201_CREATED)

        except ValueError as ve:
            return Response({"error": str(ve)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            logger.error(f"Failed to process precipitation file: {str(e)}")
            return Response({"error": f"Failed to process precipitation file: {str(e)}"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class WeatherDataExtractionView(APIView):
    """
    View to extract weather data based on metadata filters with dynamic collection support
    """
    def get(self, request):
        # Get filter parameters
        dataset_name = request.query_params.get('dataset_name')
        station = request.query_params.get('station')
        data_type = request.query_params.get('data_type')  # temperature or precipitation
        years = request.query_params.get('years')  # Can be comma-separated
        year = request.query_params.get('year')  # Keep for backward compatibility
        upload_id = request.query_params.get('upload_id')
        collection_name = request.query_params.get('collection_name')
        
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_WEATHER_DB]
            
            # Parse years if provided
            years_list = []
            if years:
                years_list = parse_years_from_string(years)
            elif year:  # Backward compatibility
                years_list = [int(year)]
            
            # If specific collection is provided, use it
            if collection_name:
                collections_to_search = [collection_name]
            elif station and data_type and years_list:
                # Create collection name from parameters
                specific_collection = create_weather_collection_name(data_type, station, years_list)
                collections_to_search = [specific_collection]
            else:
                # Search all weather data collections
                all_collections = db.list_collection_names()
                collections_to_search = [col for col in all_collections if col.startswith('weather_') and not col.endswith('_metadata')]
            
            all_records = []
            collections_searched = []
            
            for collection_name in collections_to_search:
                try:
                    collection = db[collection_name]
                    
                    # Build query filter
                    query_filter = {}
                    if dataset_name:
                        query_filter['_dataset_name'] = dataset_name
                    if station:
                        query_filter['_station'] = station
                    if data_type:
                        query_filter['_data_type'] = data_type
                    if years_list:
                        # Search for records that contain any of the specified years
                        query_filter['_years'] = {"$in": years_list}
                    if upload_id:
                        query_filter['_upload_id'] = upload_id
                    
                    # Execute query
                    cursor = collection.find(query_filter)
                    records = list(cursor)
                    
                    # Remove ObjectId for JSON serialization
                    for record in records:
                        if '_id' in record:
                            record['_id'] = str(record['_id'])
                        if '_upload_time' in record:
                            record['_upload_time'] = record['_upload_time'].isoformat()
                    
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
                    "_station": station,
                    "_data_type": data_type,
                    "_years": years_list if years_list else None,
                    "_upload_id": upload_id
                },
                "data": all_records
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to extract weather data: {str(e)}")
            return Response({"error": f"Failed to extract weather data: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class WeatherMetadataListView(APIView):
    """
    View to list all available weather datasets with their metadata from all collections
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_WEATHER_DB]
            
            # Get all metadata collections
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and col.startswith('weather_')]
            
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
                        if 'upload_time' in record:
                            record['upload_time'] = record['upload_time'].isoformat()
                        # Add source collection info
                        record['source_metadata_collection'] = metadata_collection_name
                    
                    all_metadata.extend(metadata_records)
                    
                except Exception as collection_error:
                    logger.warning(f"Error reading metadata collection {metadata_collection_name}: {str(collection_error)}")
                    continue
            
            client.close()
            
            # Group metadata by station, data_type, and years for better organization
            grouped_metadata = {}
            for record in all_metadata:
                station = record.get('station', 'unknown')
                data_type = record.get('data_type', 'unknown')
                dataset_years = record.get('dataset_years', 'unknown')
                key = f"{data_type}-{station}-{dataset_years.replace(', ', '_')}"
                
                if key not in grouped_metadata:
                    grouped_metadata[key] = {
                        "station": station,
                        "data_type": data_type,
                        "dataset_years": dataset_years,
                        "collection_name": record.get('data_collection_name', ''),
                        "datasets": []
                    }
                grouped_metadata[key]["datasets"].append(record)
            
            return Response({
                "total_datasets": len(all_metadata),
                "total_station_datatype_year_groups": len(grouped_metadata),
                "metadata_collections_found": len(metadata_collections),
                "grouped_by_station_type_years": grouped_metadata,
                "all_datasets": all_metadata
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to retrieve weather metadata: {str(e)}")
            return Response({"error": f"Failed to retrieve weather metadata: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DeleteWeatherDatasetView(APIView):
    """
    View to delete a specific weather dataset by upload_id with dynamic collection support
    """
    def delete(self, request, upload_id):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_WEATHER_DB]
            
            # First, find which collection contains this upload_id by searching metadata
            all_collections = db.list_collection_names()
            metadata_collections = [col for col in all_collections if col.endswith('_metadata') and col.startswith('weather_')]
            
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
                return Response({"error": "Weather dataset not found"}, status=status.HTTP_404_NOT_FOUND)
            
            # Delete data records from the specific collection
            data_collection = db[target_data_collection]
            data_result = data_collection.delete_many({"_upload_id": upload_id})
            
            # Delete metadata record
            metadata_collection = db[target_metadata_collection]
            metadata_result = metadata_collection.delete_one({"upload_id": upload_id})
            
            client.close()
            
            return Response({
                "message": "Weather dataset deleted successfully",
                "upload_id": upload_id,
                "records_deleted": data_result.deleted_count,
                "metadata_deleted": metadata_result.deleted_count,
                "collections_affected": {
                    "data_collection": target_data_collection,
                    "metadata_collection": target_metadata_collection
                }
            }, status=status.HTTP_200_OK)
            
        except Exception as e:
            logger.error(f"Failed to delete weather dataset: {str(e)}")
            return Response({"error": f"Failed to delete weather dataset: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class WeatherCollectionListView(APIView):
    """
    View to list all weather collections and their structure
    """
    def get(self, request):
        try:
            client = MongoClient(settings.MONGO_URI)
            db = client[settings.MONGO_WEATHER_DB]
            
            # Get all collections
            all_collections = db.list_collection_names()
            
            # Filter weather collections
            data_collections = [col for col in all_collections if col.startswith('weather_') and not col.endswith('_metadata')]
            metadata_collections = [col for col in all_collections if col.startswith('weather_') and col.endswith('_metadata')]
            
            collection_info = []
            
            for data_collection in data_collections:
                collection = db[data_collection]
                metadata_collection_name = f"{data_collection}_metadata"
                
                # Get collection stats
                record_count = collection.count_documents({})
                
                # Get sample record to understand structure
                sample_record = collection.find_one({})
                
                # Parse collection name to extract data_type, station, years
                # Format: weather_{datatype}_{station}_{years}
                parts = data_collection.replace('weather_', '').split('_')
                
                # Get metadata for this collection
                metadata_info = None
                if metadata_collection_name in metadata_collections:
                    metadata_collection = db[metadata_collection_name]
                    metadata_record = metadata_collection.find_one({})
                    if metadata_record:
                        metadata_info = {
                            "dataset_years": metadata_record.get("dataset_years", "unknown"),
                            "years": metadata_record.get("years", [])
                        }
                
                collection_info.append({
                    "data_collection": data_collection,
                    "metadata_collection": metadata_collection_name,
                    "data_type": parts[0] if len(parts) >= 1 else "unknown",
                    "station": parts[1] if len(parts) >= 2 else "unknown",
                    "dataset_years": metadata_info["dataset_years"] if metadata_info else "unknown",
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
            logger.error(f"Failed to list weather collections: {str(e)}")
            return Response({"error": f"Failed to list weather collections: {str(e)}"},
                          status=status.HTTP_500_INTERNAL_SERVER_ERROR)