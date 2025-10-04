
# etl_app/views/health_center_lab_view.py - FIXED VERSION
"""Dedicated Health Center Lab Data ETL View - Fixed Structure"""

from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
import json
import logging
import traceback
from datetime import datetime
import pandas as pd

from ..services.health_center_mongodb_service import HealthCenterMongoDBService
from ..services.data_transformer import DataTransformer
from ..services.analytics_calculator import AnalyticsCalculator
from ..services.postgresql_service import PostgreSQLService
from ..utils.validators import ETLValidator
from ..utils.helpers import format_timestamp, generate_dynamic_table_name

logger = logging.getLogger(__name__)

@method_decorator(csrf_exempt, name='dispatch')
class HealthCenterLabDataETLView(View):
    """Dedicated Health Center Lab Data ETL View"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Initialize services specifically for health center data
        try:
            self.mongodb_service = HealthCenterMongoDBService()
            self.data_transformer = DataTransformer()
            self.analytics_calculator = AnalyticsCalculator()
            self.postgresql_service = PostgreSQLService()
            self.validator = ETLValidator()
            
            logger.info("Health Center Lab ETL initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Health Center ETL: {str(e)}")
    
    def dispatch(self, request, *args, **kwargs):
        """Handle CORS and ensure proper response"""
        if request.method == 'OPTIONS':
            response = JsonResponse({'message': 'OPTIONS request handled'})
            response['Access-Control-Allow-Origin'] = '*'
            response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
            return response
        
        try:
            response = super().dispatch(request, *args, **kwargs)
            if response is not None:
                response['Access-Control-Allow-Origin'] = '*'
                response['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
                response['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
                return response
            else:
                logger.error("Health Center dispatch returned None response")
                error_response = JsonResponse({
                    'success': False,
                    'error': 'Internal server error: No response generated',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=500)
                error_response['Access-Control-Allow-Origin'] = '*'
                return error_response
        except Exception as e:
            logger.error(f"Exception in Health Center dispatch: {str(e)}")
            error_response = JsonResponse({
                'success': False,
                'error': f'Dispatch error: {str(e)}',
                'data_source': 'health_center',
                'timestamp': format_timestamp(datetime.now())
            }, status=500)
            error_response['Access-Control-Allow-Origin'] = '*'
            return error_response
    
    def post(self, request):
        """Handle POST requests for health center data"""
        start_time = datetime.now()
        
        try:
            logger.info("HEALTH CENTER ETL: Starting POST request processing")
            
            # Parse JSON request
            try:
                data = json.loads(request.body) if request.body else {}
                logger.info(f"HEALTH CENTER ETL: Received data: {data}")
            except json.JSONDecodeError as e:
                return JsonResponse({
                    'success': False,
                    'error': f'Invalid JSON: {str(e)}',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=400)
            
            # Extract parameters
            years = data.get('years', 'all')
            if isinstance(years, list):
                years = ','.join(map(str, years))
            
            district = data.get('district', '').strip()
            sector = data.get('sector', '').strip()
            show_available = data.get('show_available', False)
            calculate_analytics = data.get('calculate_analytics', True)
            save_to_db = data.get('save_to_db', True)
            table_prefix = data.get('table_prefix', 'hc_data')
            update_mode = data.get('update_mode', 'replace').lower()
            
            logger.info(f"HEALTH CENTER ETL: Parameters - years={years}, district='{district}', sector='{sector}', update_mode={update_mode}")
            
            # Validate update mode
            if update_mode not in ['replace', 'append']:
                return JsonResponse({
                    'success': False,
                    'error': 'update_mode must be either "replace" or "append"',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=400)
            
            # Get available filters from health center data
            logger.info("HEALTH CENTER ETL: Getting available filters...")
            try:
                available_filters = self.mongodb_service.get_available_filters()
                logger.info(f"HEALTH CENTER ETL: Available filters retrieved: {available_filters}")
            except Exception as filter_error:
                logger.error(f"HEALTH CENTER ETL: Filter error: {str(filter_error)}")
                return JsonResponse({
                    'success': False,
                    'error': f'Could not retrieve available filters: {str(filter_error)}',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=503)
            
            if show_available:
                return JsonResponse({
                    'success': True,
                    'message': 'Available filters for Health Center Lab Data',
                    'data_source': 'health_center',
                    'available_filters': available_filters,
                    'usage_examples': {
                        'get_all_data': '?years=all',
                        'specific_years': '?years=2020,2021,2022',
                        'filter_by_district': '?district=Bugesera&years=2022',
                        'filter_by_sector': '?district=Bugesera&sector=Kamabuye&years=2022'
                    },
                    'timestamp': format_timestamp(datetime.now())
                })
            
            # SIMPLIFIED: Skip strict year validation and use available years
            logger.info("HEALTH CENTER ETL: Getting processed years...")
            
            if years == 'all' or not available_filters['years']:
                # Use all available years from data
                processed_years = available_filters['years'] if available_filters['years'] else []
                logger.info(f"HEALTH CENTER ETL: Using all available years: {processed_years}")
            else:
                # Try to parse requested years, but don't fail if they don't match
                try:
                    if isinstance(years, str):
                        requested_years = [int(y.strip()) for y in years.split(',') if y.strip().isdigit()]
                    else:
                        requested_years = years
                    
                    # Check if any requested years are available
                    matching_years = [y for y in requested_years if y in available_filters['years']]
                    
                    if matching_years:
                        processed_years = matching_years
                        logger.info(f"HEALTH CENTER ETL: Using matching years: {processed_years}")
                    else:
                        # Use available years instead of failing
                        processed_years = available_filters['years']
                        logger.info(f"HEALTH CENTER ETL: No matching years. Using available: {processed_years}")
                        
                except Exception as parse_error:
                    # Fallback to available years
                    processed_years = available_filters['years']
                    logger.warning(f"HEALTH CENTER ETL: Could not parse years. Using available: {processed_years}")
            
            # Only fail if no data exists at all
            if not processed_years:
                return JsonResponse({
                    'success': False,
                    'error': 'No data found in health center collections',
                    'available_years': available_filters['years'],
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=404)
            
            # STEP 1: Extract data from MongoDB
            logger.info(f"HEALTH CENTER ETL: STEP 1/4 - Extracting data for {len(processed_years)} years...")
            try:
                raw_data = self.mongodb_service.extract_data_for_analytics(district, sector, processed_years)
                
                if not raw_data:
                    return JsonResponse({
                        'success': False,
                        'message': 'No health center data found matching the specified filters',
                        'data_source': 'health_center',
                        'filters_applied': {
                            'years': processed_years,
                            'district': district or 'all',
                            'sector': sector or 'all'
                        },
                        'available_filters': available_filters,
                        'timestamp': format_timestamp(datetime.now())
                    }, status=404)
                
                logger.info(f"HEALTH CENTER ETL: Extracted {len(raw_data)} raw records")
                
            except Exception as extract_error:
                logger.error(f"HEALTH CENTER ETL: Data extraction error: {str(extract_error)}")
                return JsonResponse({
                    'success': False,
                    'error': f'Data extraction failed: {str(extract_error)}',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=500)
            
            # STEP 2: Transform data
            logger.info(f"HEALTH CENTER ETL: STEP 2/4 - Transforming {len(raw_data)} raw records...")
            try:
                transformed_data = self.data_transformer.clean_and_transform_data(raw_data)
                logger.info(f"HEALTH CENTER ETL: Transformed to {len(transformed_data)} clean records")
                
                # Log sample transformed data for debugging
                if transformed_data:
                    sample_record = transformed_data[0]
                    logger.info(f"HEALTH CENTER ETL: Sample transformed record keys: {list(sample_record.keys())}")
                    logger.info(f"HEALTH CENTER ETL: Sample values: year={sample_record.get('year')}, district={sample_record.get('district')}, sector={sample_record.get('sector')}")
                
            except Exception as transform_error:
                logger.error(f"HEALTH CENTER ETL: Data transformation error: {str(transform_error)}")
                return JsonResponse({
                    'success': False,
                    'error': f'Data transformation failed: {str(transform_error)}',
                    'data_source': 'health_center',
                    'timestamp': format_timestamp(datetime.now())
                }, status=500)
            
            # STEP 3: Calculate analytics
            analytics = {}
            if calculate_analytics:
                logger.info(f"HEALTH CENTER ETL: STEP 3/4 - Calculating analytics from {len(transformed_data)} records...")
                try:
                    df = pd.DataFrame(transformed_data)
                    logger.info(f"HEALTH CENTER ETL: DataFrame created with shape: {df.shape}")
                    logger.info(f"HEALTH CENTER ETL: DataFrame columns: {list(df.columns)}")
                    
                    analytics = self.analytics_calculator.calculate_analytics(df)
                    logger.info(f"HEALTH CENTER ETL: Calculated {len(analytics)} analytics types: {list(analytics.keys())}")
                    
                    # Debug each analytics type
                    for analytics_type, analytics_data in analytics.items():
                        data_length = len(analytics_data) if isinstance(analytics_data, list) else 1 if analytics_data else 0
                        logger.info(f"HEALTH CENTER ETL: {analytics_type} = {data_length} records")
                        
                    if not analytics:
                        logger.warning("HEALTH CENTER ETL: No analytics data was calculated!")
                        
                except Exception as analytics_error:
                    logger.error(f"HEALTH CENTER ETL: Analytics calculation error: {str(analytics_error)}")
                    logger.error(f"HEALTH CENTER ETL: Analytics traceback: {traceback.format_exc()}")
                    # Continue without analytics rather than failing completely
                    logger.warning("HEALTH CENTER ETL: Continuing without analytics due to calculation error")
                    analytics = {}
            else:
                logger.info("HEALTH CENTER ETL: Skipping analytics calculation as requested")
            
            # STEP 4: Save to database
            save_results = {}
            table_names_created = {}
            
            if save_to_db:
                logger.info(f"HEALTH CENTER ETL: STEP 4/4 - Saving to PostgreSQL (mode: {update_mode})...")
                
                try:
                    # Save raw data with health center prefix
                    raw_table_name = generate_dynamic_table_name("health_center_raw_data", district, sector, processed_years)
                    success, message = self.postgresql_service.save_raw_data(transformed_data, raw_table_name, update_mode)
                    save_results['raw_data'] = {'success': success, 'message': message}
                    if success:
                        table_names_created['raw_data'] = raw_table_name
                        logger.info(f"HEALTH CENTER ETL: Raw data saved to {raw_table_name}")
                    
                    # Save analytics if available
                    if analytics:
                        logger.info("HEALTH CENTER ETL: Saving analytics...")
                        success, results = self.postgresql_service.save_analytics(
                            analytics, table_prefix, district, sector, processed_years, update_mode
                        )
                        save_results['analytics'] = {'success': success, 'details': results}
                        if success and isinstance(results, dict):
                            for analytics_type, result_msg in results.items():
                                if isinstance(result_msg, str) and "Saved to" in result_msg:
                                    table_name = result_msg.split("Saved to ")[-1]
                                    table_names_created[analytics_type] = table_name
                        logger.info(f"HEALTH CENTER ETL: Analytics save completed: {success}")
                    else:
                        logger.info("HEALTH CENTER ETL: No analytics data to save")
                        
                except Exception as save_error:
                    logger.error(f"HEALTH CENTER ETL: Database save error: {str(save_error)}")
                    save_results['error'] = str(save_error)
            
            # Calculate processing time and build response
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"HEALTH CENTER ETL COMPLETE: {processing_time:.2f} seconds")
            
            response_data = {
                'success': True,
                'message': f'Successfully processed {len(transformed_data)} health center lab records',
                'data_source': 'health_center',
                'summary': {
                    'total_records_processed': len(transformed_data),
                    'analytics_calculated': len(analytics) if analytics else 0,
                    'filters_applied': {
                        'years': processed_years,
                        'district': district or 'all',
                        'sector': sector or 'all',
                        'update_mode': update_mode
                    }
                },
                'processing_time_seconds': round(processing_time, 2),
                'timestamp': format_timestamp(datetime.now())
            }
            
            # Add optional fields
            if table_names_created:
                response_data['table_names_created'] = table_names_created
                logger.info(f"HEALTH CENTER ETL: Tables created: {list(table_names_created.keys())}")
            
            if analytics and not save_to_db:
                response_data['analytics'] = analytics
            
            if save_results:
                response_data['database_save_results'] = save_results
            
            logger.info("HEALTH CENTER ETL: Returning success response")
            return JsonResponse(response_data)
            
        except Exception as e:
            logger.error(f"HEALTH CENTER ETL: Main processing error: {str(e)}")
            logger.error(f"HEALTH CENTER ETL: Traceback: {traceback.format_exc()}")
            
            error_response = JsonResponse({
                'success': False,
                'error': f'Health center processing failed: {str(e)}',
                'data_source': 'health_center',
                'timestamp': format_timestamp(datetime.now())
            }, status=500)
            
            logger.info("HEALTH CENTER ETL: Returning error response")
            return error_response
        
        finally:
            # Clean up connections
            try:
                if hasattr(self, 'mongodb_service') and self.mongodb_service:
                    self.mongodb_service.close_connection()
                    logger.info("HEALTH CENTER ETL: MongoDB connection closed")
            except Exception as cleanup_error:
                logger.error(f"HEALTH CENTER ETL: Cleanup error: {str(cleanup_error)}")
    
    def get(self, request):
        """Handle GET requests by converting to POST format"""
        get_data = {
            'years': request.GET.get('years', 'all'),
            'district': request.GET.get('district', ''),
            'sector': request.GET.get('sector', ''),
            'show_available': request.GET.get('show_available', 'false').lower() == 'true',
            'calculate_analytics': request.GET.get('calculate_analytics', 'true').lower() == 'true',
            'save_to_db': request.GET.get('save_to_db', 'true').lower() == 'true',
            'table_prefix': request.GET.get('table_prefix', 'hc_data'),
            'update_mode': request.GET.get('update_mode', 'replace')
        }
        
        logger.info(f"HEALTH CENTER ETL: Converting GET to POST: {get_data}")
        request.body = json.dumps(get_data).encode('utf-8')
        return self.post(request)