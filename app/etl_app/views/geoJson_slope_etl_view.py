

# Django ETL Project - Slope GeoJSON -> (bbox extract) -> Elasticsearch geo_shape (+ optional Postgres)
# Visualize in Kibana Maps using the created index.

from django.http import JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.conf import settings

from pymongo import MongoClient
from sqlalchemy import create_engine, text

from elasticsearch import Elasticsearch, helpers

import json
import logging
import re
from datetime import datetime
import traceback
import numpy as np
import uuid
import os

from shapely.geometry import Polygon, shape
from shapely.prepared import prep
from shapely.ops import transform as shp_transform
from pyproj import CRS, Transformer

logger = logging.getLogger(__name__)

# -----------------------------
# Rwanda bounds + common CRSs
# -----------------------------
RWANDA_LON_MIN, RWANDA_LON_MAX = 28.6, 31.0
RWANDA_LAT_MIN, RWANDA_LAT_MAX = -3.1, -0.8

CANDIDATE_EPSGS = [
    4326,         # WGS84 lon/lat (already OK)
    32735, 32736, # WGS84 / UTM 35S, 36S
    20935, 20936, # Arc 1950 / UTM 35S, 36S
    26335, 26336  # Arc 1960 / UTM 35S, 36S
]

def _fmt_ts(dt):
    return dt.strftime('%Y-%m-%d %H:%M')

def _gen_id():
    return str(uuid.uuid4())

def _sanitize_name(s: str, fallback: str):
    if not s:
        return fallback
    out = re.sub(r'[^a-zA-Z0-9]', '_', str(s).lower())
    out = out.strip('_')
    return out or fallback

# -----------------------------------
# Elasticsearch helpers (Kibana Maps)
# -----------------------------------
def _es_client_from_settings():
    hosts = getattr(settings, "ELASTICSEARCH_HOSTS", ["http://localhost:9200"])
    http_auth = None
    if getattr(settings, "ELASTICSEARCH_USERNAME", None) and getattr(settings, "ELASTICSEARCH_PASSWORD", None):
        http_auth = (settings.ELASTICSEARCH_USERNAME, settings.ELASTICSEARCH_PASSWORD)
    verify = getattr(settings, "ELASTICSEARCH_VERIFY_CERTS", True)
    # CA bundle via ELASTICSEARCH_CA_PATH optionally:
    ca_certs = getattr(settings, "ELASTICSEARCH_CA_PATH", None)
    return Elasticsearch(hosts=hosts, basic_auth=http_auth, verify_certs=verify, ca_certs=ca_certs)

def _es_index_name(district, sector, year, base="slope_polygons"):
    d = _sanitize_name(district, "district")
    s = _sanitize_name(sector, "sector")
    y = str(year or datetime.utcnow().year)
    idx = f"{base}_{d}_{s}_{y}"
    # ES index must be lowercase and <= 255 chars
    idx = idx.lower()[:200]
    return idx

def _ensure_es_index(es: Elasticsearch, index_name: str, replace: bool):
    exists = es.indices.exists(index=index_name)
    if exists and replace:
        es.indices.delete(index=index_name, ignore=[404])
        exists = False
    if not exists:
        mapping = {
            "mappings": {
                "properties": {
                    # Geometry for Kibana Maps
                    "geometry_wgs84": {"type": "geo_shape"},
                    # Convenient centroid point
                    "centroid_point": {"type": "geo_point"},
                    # Numeric attributes
                    "slope_value": {"type": "float"},
                    "intersection_area": {"type": "double"},
                    "coverage_percentage": {"type": "float"},
                    # Strings / metadata
                    "associated_district": {"type": "keyword"},
                    "associated_sector": {"type": "keyword"},
                    "associated_year": {"type": "integer"},
                    "extraction_type": {"type": "keyword"},
                    "created_at": {"type": "date", "format": "yyyy-MM-dd HH:mm"},
                    "upload_id": {"type": "keyword"},
                    "dataset_name": {"type": "keyword"},
                    # Keep originals for debug/joins
                    "original_properties": {"type": "object", "enabled": True},
                    "intersection_geometry": {"type": "geo_shape"},
                    "bounding_box": {"type": "object", "enabled": True},
                }
            }
        }
        es.indices.create(index=index_name, **mapping)

# -----------------------------------
# Main view
# -----------------------------------
@method_decorator(csrf_exempt, name='dispatch')
class SlopeGeoJsonToESView(View):
    """
    POST /extract/slope-geojson/
    Body:
    {
      "extraction_type": "coordinates" | "all",
      "coordinates": [min_lon, min_lat, max_lon, max_lat],   # required for "coordinates"
      "district": "Bugesera",
      "sector": "Kamabuye",
      "year": 2025,
      "update_mode": "replace" | "append",
      "save_to_postgres": false,                # optional (kept for compatibility)
      "save_to_elasticsearch": true,            # <-- set true to push to Kibana
      "source_epsg": 4326                        # optional override if CRS guess fails
    }
    """

    def __init__(self):
        super().__init__()
        # Mongo
        self.mongo_uri = getattr(settings, 'MONGO_URI', "mongodb://localhost:27017")
        self.mongo_geojson_db = getattr(settings, 'MONGO_DB_GEOJSON', 'slope_raster_GeoJson_db')
        self.mongo_geojson_collection = getattr(settings, 'MONGO_COLLECTION_GEOJSON', 'slope_GeoJson_uploads')

        # Postgres (optional)
        db_config = settings.DATABASES.get('default', {})
        self.pg_config = {
            'host': db_config.get('HOST', 'localhost'),
            'port': db_config.get('PORT', 5432),
            'database': db_config.get('NAME', 'postgres'),
            'user': db_config.get('USER', 'postgres'),
            'password': db_config.get('PASSWORD', '')
        }

    def get(self, request):
        """Simple status."""
        try:
            client = MongoClient(self.mongo_uri)
            coll = client[self.mongo_geojson_db][self.mongo_geojson_collection]
            count = coll.count_documents({})
            sample = coll.find_one({})
            client.close()

            features_count = 0
            dataset_info = {}
            if sample and 'geojson_data' in sample:
                features_count = len(sample['geojson_data'].get('features', []))
                md = sample['geojson_data'].get('metadata', {})
                dataset_info = {
                    'dataset_name': sample.get('_dataset_name', 'Unknown'),
                    'region': sample.get('_region', 'Unknown'),
                    'resolution': sample.get('resolution', 'Unknown'),
                    'year': sample.get('_year', 'Unknown'),
                    'features_truncated': md.get('features_truncated', False)
                }

            return JsonResponse({
                'success': True,
                'message': 'Slope GeoJSON -> ES API ready',
                'mongo': {'db': self.mongo_geojson_db, 'collection': self.mongo_geojson_collection, 'docs': count},
                'sample': {'features_count': features_count, 'dataset_info': dataset_info},
                'usage': {
                    'POST /extract/slope-geojson/': {
                        'extraction_type': 'coordinates|all',
                        'coordinates': '[min_lon, min_lat, max_lon, max_lat]',
                        'district': 'Bugesera', 'sector': 'Kamabuye', 'year': 2025,
                        'update_mode': 'replace|append',
                        'save_to_elasticsearch': True,
                        'source_epsg': 4326
                    }
                },
                'timestamp': _fmt_ts(datetime.now())
            })
        except Exception as e:
            logger.exception(e)
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    def post(self, request):
        try:
            body = json.loads(request.body or "{}")

            extraction_type = (body.get('extraction_type') or "").lower()
            coords = body.get('coordinates', [])
            district = body.get('district', '')
            sector = body.get('sector', '')
            year = body.get('year', datetime.utcnow().year)
            update_mode = (body.get('update_mode') or 'replace').lower()
            source_epsg = body.get('source_epsg')

            save_to_postgres = bool(body.get('save_to_postgres', False))  # default off here
            save_to_elasticsearch = bool(body.get('save_to_elasticsearch', True))
            index_name = body.get('index_name')  # optional override

            if extraction_type not in ('coordinates', 'all'):
                return JsonResponse({'success': False, 'error': 'extraction_type must be "coordinates" or "all"'}, status=400)

            if extraction_type == 'coordinates':
                if not coords or len(coords) != 4:
                    return JsonResponse({'success': False, 'error': 'coordinates must be [min_lon, min_lat, max_lon, max_lat]'}, status=400)
            if not district or not sector:
                return JsonResponse({'success': False, 'error': 'district and sector are required'}, status=400)
            if update_mode not in ('replace', 'append'):
                return JsonResponse({'success': False, 'error': 'update_mode must be replace|append'}, status=400)

            # Load slope GeoJSON from Mongo
            client = MongoClient(self.mongo_uri)
            try:
                slope_data = self._get_slope_data(client)
                if not slope_data:
                    return JsonResponse({'success': False, 'error': 'No slope geojson found'}, status=404)

                # Extract features
                if extraction_type == 'coordinates':
                    result = self._extract_by_coordinates(
                        slope_data, coords, district, sector, year, source_epsg=source_epsg
                    )
                else:
                    result = self._extract_all(
                        slope_data, district, sector, year, source_epsg=source_epsg
                    )

                if 'error' in result:
                    return JsonResponse({'success': False, 'error': result['error']}, status=400)

                # Optionally compute stats
                if result.get('slope_features'):
                    result['statistics'] = self._calculate_slope_statistics(result['slope_features'])

                # Save to Elasticsearch (geo_shape) for Kibana Maps
                es_save = None
                if save_to_elasticsearch and result.get('slope_features'):
                    es_save = self._save_slope_to_elasticsearch(
                        result, district, sector, year,
                        update_mode=update_mode,
                        index_name=index_name
                    )
                    result['elasticsearch_save'] = es_save

                # (Optional) Save to Postgres JSONB (kept from your flow)
                if save_to_postgres and result.get('slope_features'):
                    pg_result = self._save_slope_to_postgres(
                        result, extraction_type, district, sector, year, update_mode
                    )
                    result['postgres_save'] = pg_result

                # Warnings if truncated at conversion time
                md = slope_data['geojson'].get('metadata', {})
                if md.get('features_truncated'):
                    result.setdefault('warnings', []).append(
                        "GeoJSON features were truncated at conversion time. "
                        "If bbox returns no features, re-export with higher max_features or quantize_step, "
                        "or switch to raster-on-the-fly extraction."
                    )

                result.update({
                    'success': True,
                    'extraction_type': extraction_type,
                    'update_mode': update_mode,
                    'timestamp': _fmt_ts(datetime.now())
                })
                return JsonResponse(result)

            finally:
                client.close()

        except json.JSONDecodeError as e:
            return JsonResponse({'success': False, 'error': f'Invalid JSON: {e}'}, status=400)
        except Exception as e:
            logger.error(f"ETL error: {e}\n{traceback.format_exc()}")
            return JsonResponse({'success': False, 'error': str(e)}, status=500)

    # ---------------- Data access ----------------
    def _get_slope_data(self, mongo_client):
        coll = mongo_client[self.mongo_geojson_db][self.mongo_geojson_collection]
        doc = coll.find_one({})
        if not doc:
            return None
        gj = doc.get('geojson_data')
        if not isinstance(gj, dict) or 'features' not in gj:
            return None
        return {
            'metadata': {
                'dataset_name': doc.get('_dataset_name'),
                'region': doc.get('_region'),
                'resolution': doc.get('resolution'),
                'year': doc.get('_year'),
                'upload_id': doc.get('_upload_id'),
                'source': doc.get('source')
            },
            'geojson': gj
        }

    # ---------------- CRS helpers ----------------
    def _coord_looks_like_degrees(self, x, y):
        return -180 <= x <= 180 and -90 <= y <= 90

    def _in_rwanda_bounds(self, lon, lat):
        return (RWANDA_LON_MIN <= lon <= RWANDA_LON_MAX) and (RWANDA_LAT_MIN <= lat <= RWANDA_LAT_MAX)

    def _first_xy_from_geojson(self, geom):
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if not gtype or coords is None:
            return None
        try:
            if gtype == "Point":
                x, y = coords[0], coords[1]
            elif gtype == "LineString":
                x, y = coords[0][0], coords[0][1]
            elif gtype == "Polygon":
                x, y = coords[0][0][0], coords[0][0][1]
            elif gtype == "MultiPolygon":
                x, y = coords[0][0][0][0], coords[0][0][0][1]
            else:
                c = coords
                while isinstance(c, (list, tuple)):
                    c = c[0]
                x, y = c[0], c[1]
            return float(x), float(y)
        except Exception:
            return None

    def _pick_source_epsg(self, sample_x, sample_y):
        if self._coord_looks_like_degrees(sample_x, sample_y) and self._in_rwanda_bounds(sample_x, sample_y):
            return 4326, "Assumed EPSG:4326"
        for epsg in CANDIDATE_EPSGS:
            if epsg == 4326:
                continue
            try:
                t = Transformer.from_crs(CRS.from_epsg(epsg), CRS.from_epsg(4326), always_xy=True)
                lon, lat = t.transform(sample_x, sample_y)
                if self._in_rwanda_bounds(lon, lat):
                    return epsg, f"Auto-detected EPSG:{epsg}"
            except Exception:
                pass
        return None, "Could not detect CRS; pass 'source_epsg'"

    def _to_wgs84_geom(self, geom, src_epsg):
        if src_epsg == 4326:
            return geom
        t = Transformer.from_crs(CRS.from_epsg(src_epsg), CRS.from_epsg(4326), always_xy=True)
        return shp_transform(lambda x, y, *args: t.transform(x, y), geom)

    def _ensure_dataset_wgs84(self, geojson_obj, override_epsg=None):
        feats = geojson_obj.get("features", [])
        if not feats:
            return [], 4326, "No features"
        sample_xy = None
        for f in feats:
            g = f.get("geometry")
            sample_xy = self._first_xy_from_geojson(g) if g else None
            if sample_xy:
                break
        if not sample_xy:
            return None, None, "No sample coords"
        sx, sy = sample_xy
        if override_epsg is not None:
            override_epsg = int(override_epsg)
            if override_epsg != 4326 and self._coord_looks_like_degrees(sx, sy) and self._in_rwanda_bounds(sx, sy):
                src_epsg, note = 4326, f"Ignored override EPSG:{override_epsg}; looks like lon/lat in Rwanda"
            else:
                src_epsg, note = override_epsg, f"Forced EPSG:{override_epsg}"
        else:
            src_epsg, note = self._pick_source_epsg(sx, sy)
            if not src_epsg:
                return None, None, note

        out = []
        for f in feats:
            geom = shape(f.get("geometry"))
            geom_wgs84 = self._to_wgs84_geom(geom, src_epsg)
            f2 = dict(f)
            f2["geometry"] = json.loads(json.dumps(geom_wgs84.__geo_interface__))
            out.append(f2)
        return out, src_epsg, note

    # ---------------- Extraction ----------------
    def _extract_all(self, slope_data, district, sector, year, source_epsg=None):
        feats_wgs84, src_epsg, note = self._ensure_dataset_wgs84(
            slope_data['geojson'], override_epsg=source_epsg
        )
        if feats_wgs84 is None:
            return {'error': f'CRS detection failed. {note}'}

        slope_features = []
        d_minx = float("inf"); d_miny = float("inf")
        d_maxx = float("-inf"); d_maxy = float("-inf")

        for f in feats_wgs84:
            try:
                geom = shape(f.get('geometry'))
                if not geom.is_valid:
                    geom = geom.buffer(0)
                gx0, gy0, gx1, gy1 = geom.bounds
                d_minx = min(d_minx, gx0); d_miny = min(d_miny, gy0)
                d_maxx = max(d_maxx, gx1); d_maxy = max(d_maxy, gy1)

                props = f.get('properties', {}) or {}
                slope_val = props.get('slope_value', props.get('value', 0.0))
                centroid = geom.centroid

                slope_features.append({
                    'unique_id': _gen_id(),
                    'slope_value': float(slope_val),
                    'intersection_area': float(getattr(geom, 'area', 0.0)),
                    'coverage_percentage': 100.0,  # full polygon
                    'feature_centroid': {'longitude': centroid.x, 'latitude': centroid.y},
                    'centroid_point': {'lon': centroid.x, 'lat': centroid.y},
                    'geometry_wgs84': f['geometry'],
                    'intersection_geometry': None,
                    'original_properties': props,
                    'associated_district': district,
                    'associated_sector': sector,
                    'associated_year': year,
                    'created_at': _fmt_ts(datetime.now())
                })
            except Exception as e:
                logger.debug(f"extract_all skip feature: {e}")
                continue

        dataset_bounds = None
        if d_minx != float("inf"):
            dataset_bounds = {'min_lon': d_minx, 'min_lat': d_miny, 'max_lon': d_maxx, 'max_lat': d_maxy}

        return {
            'slope_features': slope_features,
            'bounding_box': None,
            'extraction_summary': {
                'total_slope_features': len(slope_features),
                'slope_data_source': slope_data['metadata'],
                'processing_method': 'all_features',
                'associated_location': {'district': district, 'sector': sector, 'year': year},
                'crs_note': note,
                'source_epsg_detected': src_epsg
            },
            'dataset_bounds': dataset_bounds
        }

    def _extract_by_coordinates(self, slope_data, coordinates, district, sector, year, source_epsg=None):
        try:
            if not coordinates or len(coordinates) != 4:
                return {'error': 'coordinates must be [min_lon, min_lat, max_lon, max_lat]'}
            min_lon, min_lat, max_lon, max_lat = map(float, coordinates)
            if min_lon > max_lon: min_lon, max_lon = max_lon, min_lon
            if min_lat > max_lat: min_lat, max_lat = max_lat, min_lat

            feats_wgs84, src_epsg, note = self._ensure_dataset_wgs84(
                slope_data['geojson'], override_epsg=source_epsg
            )
            if feats_wgs84 is None:
                return {'error': f'CRS detection failed. {note}. If values look like meters, pass "source_epsg".'}

            bbox = Polygon([(min_lon, min_lat), (max_lon, min_lat),
                            (max_lon, max_lat), (min_lon, max_lat), (min_lon, min_lat)])
            pb = prep(bbox)

            slope_features = []
            total_intersection_area = 0.0

            d_minx = float("inf"); d_miny = float("inf")
            d_maxx = float("-inf"); d_maxy = float("-inf")

            for i, f in enumerate(feats_wgs84):
                try:
                    geom = shape(f.get('geometry'))
                    if not geom.is_valid:
                        geom = geom.buffer(0)

                    gx0, gy0, gx1, gy1 = geom.bounds
                    d_minx = min(d_minx, gx0); d_miny = min(d_miny, gy0)
                    d_maxx = max(d_maxx, gx1); d_maxy = max(d_maxy, gy1)

                    if not pb.intersects(geom):
                        continue

                    inter = bbox.intersection(geom)
                    inter_area = float(getattr(inter, 'area', 0.0) or 0.0)
                    if inter_area <= 1e-12:
                        continue

                    props = f.get('properties', {}) or {}
                    slope_val = props.get('slope_value', props.get('value', 0.0))
                    geom_area = float(getattr(geom, 'area', 0.0) or 0.0)
                    coverage_pct = (inter_area / geom_area * 100.0) if geom_area > 0 else 0.0

                    centroid = geom.centroid
                    slope_features.append({
                        'unique_id': _gen_id(),
                        'slope_value': float(slope_val),
                        'intersection_area': inter_area,
                        'coverage_percentage': round(coverage_pct, 4),
                        'feature_centroid': {'longitude': centroid.x, 'latitude': centroid.y},
                        'centroid_point': {'lon': centroid.x, 'lat': centroid.y},
                        'geometry_wgs84': f['geometry'],  # full polygon for geo_shape
                        'intersection_geometry': inter.__geo_interface__ if hasattr(inter, '__geo_interface__') else None,
                        'original_properties': props,
                        'associated_district': district,
                        'associated_sector': sector,
                        'associated_year': year,
                        'created_at': _fmt_ts(datetime.now())
                    })
                    total_intersection_area += inter_area
                except Exception as e:
                    logger.debug(f"Skipping feature {i}: {e}")
                    continue

            area_weighted_slope = 0.0
            if slope_features and total_intersection_area > 0:
                area_weighted_slope = sum(sf['slope_value'] * sf['intersection_area'] for sf in slope_features) / total_intersection_area

            dataset_bounds = None
            if d_minx != float("inf"):
                dataset_bounds = {'min_lon': d_minx, 'min_lat': d_miny, 'max_lon': d_maxx, 'max_lat': d_maxy}

            return {
                'slope_features': slope_features,
                'bounding_box': {
                    'coordinates': [min_lon, min_lat, max_lon, max_lat],
                    'district': district, 'sector': sector, 'year': year,
                    'bbox_area_degrees_sq': float(bbox.area),
                    'bbox_area_km2_approx': float(bbox.area * 111.32 * 111.32)
                },
                'extraction_summary': {
                    'total_slope_features': len(slope_features),
                    'total_slope_coverage_area': float(total_intersection_area),
                    'area_weighted_average_slope': round(area_weighted_slope, 4),
                    'coverage_percentage': round((total_intersection_area / bbox.area * 100), 2) if bbox.area > 0 else 0,
                    'slope_data_source': slope_data['metadata'],
                    'processing_method': 'coordinate_intersection',
                    'associated_location': {'district': district, 'sector': sector, 'year': year},
                    'crs_note': note, 'source_epsg_detected': src_epsg
                },
                'dataset_bounds': dataset_bounds
            }
        except Exception as e:
            logger.error(f"bbox extract error: {e}")
            return {'error': str(e)}

    # ---------------- Stats ----------------
    def _calculate_slope_statistics(self, slope_features):
        try:
            if not slope_features:
                return {'error': 'No slope features'}
            vals = [f['slope_value'] for f in slope_features]
            areas = [f.get('intersection_area', 1.0) for f in slope_features]
            stats = {
                'count': len(vals),
                'min_slope': float(min(vals)),
                'max_slope': float(max(vals)),
                'mean_slope': float(np.mean(vals)),
                'median_slope': float(np.median(vals)),
                'std_slope': float(np.std(vals)),
            }
            tot_area = sum(areas)
            stats['area_weighted_mean_slope'] = float(sum(v * a for v, a in zip(vals, areas)) / tot_area) if tot_area > 0 else stats['mean_slope']
            return stats
        except Exception as e:
            logger.error(f"stats error: {e}")
            return {'error': str(e)}

    # ---------------- Elasticsearch save ----------------
    def _save_slope_to_elasticsearch(self, result, district, sector, year, update_mode='replace', index_name=None):
        try:
            es = _es_client_from_settings()
            idx = index_name or _es_index_name(district, sector, year)
            _ensure_es_index(es, idx, replace=(update_mode == 'replace'))

            feats = result.get('slope_features', [])
            bbox_info = result.get('bounding_box')
            meta = result.get('extraction_summary', {}).get('slope_data_source', {})
            upload_id = meta.get('upload_id')
            dataset_name = meta.get('dataset_name')

            actions = []
            for f in feats:
                doc = {
                    "unique_id": f.get('unique_id'),
                    "slope_value": f.get('slope_value'),
                    "intersection_area": f.get('intersection_area'),
                    "coverage_percentage": f.get('coverage_percentage'),
                    "centroid_point": f.get('centroid_point'),            # geo_point
                    "geometry_wgs84": f.get('geometry_wgs84'),            # geo_shape (Polygon/MultiPolygon)
                    "intersection_geometry": f.get('intersection_geometry'),  # geo_shape
                    "original_properties": f.get('original_properties'),
                    "associated_district": f.get('associated_district'),
                    "associated_sector": f.get('associated_sector'),
                    "associated_year": f.get('associated_year'),
                    "extraction_type": "coordinates" if bbox_info else "all",
                    "created_at": f.get('created_at'),
                    "upload_id": upload_id,
                    "dataset_name": dataset_name,
                    "bounding_box": bbox_info
                }
                actions.append({
                    "_op_type": "index" if update_mode == "append" else "index",
                    "_index": idx,
                    "_id": f.get('unique_id'),
                    "_source": doc
                })

            if actions:
                helpers.bulk(es, actions, chunk_size=1000, request_timeout=180)

            # simple count
            es_count = es.count(index=idx).get('count', 0)
            return {
                'success': True,
                'index': idx,
                'indexed_docs': len(actions),
                'current_index_count': es_count,
                'kibana_note': 'Use Kibana Maps -> add layer from Documents, index = this index, geometry = geometry_wgs84'
            }
        except Exception as e:
            logger.error(f"Elasticsearch save error: {e}")
            return {'success': False, 'error': str(e)}

    # ---------------- Optional: Postgres JSONB save (unchanged idea) ----------------
    def _save_slope_to_postgres(self, result, extraction_type, district, sector, year, update_mode='replace'):
        try:
            engine = create_engine(
                f"postgresql://{self.pg_config['user']}:{self.pg_config['password']}@"
                f"{self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}"
            )
            table = self._generate_slope_table_name(district, sector, year)
            drop_sql = f"DROP TABLE IF EXISTS {table} CASCADE;"
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                unique_id VARCHAR(36) UNIQUE NOT NULL,
                slope_value NUMERIC(10,4),
                intersection_area NUMERIC(15,10),
                coverage_percentage NUMERIC(5,2),
                feature_centroid_lon NUMERIC(10,6),
                feature_centroid_lat NUMERIC(10,6),
                geometry JSONB,
                intersection_geometry JSONB,
                original_properties JSONB,
                associated_district VARCHAR(100),
                associated_sector VARCHAR(100),
                associated_year INTEGER,
                extraction_type VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()

            );
            """
            with engine.connect() as conn:
                if update_mode == 'replace':
                    conn.execute(text(drop_sql))
                conn.execute(text(create_sql))
                conn.commit()

                feats = result.get('slope_features', [])
                ins_sql = f"""
                INSERT INTO {table}
                (unique_id, slope_value, intersection_area, coverage_percentage,
                 feature_centroid_lon, feature_centroid_lat, geometry, intersection_geometry,
                 original_properties, associated_district, associated_sector, associated_year,
                 extraction_type, created_at, updated_at)
                VALUES
                (:unique_id, :slope_value, :intersection_area, :coverage_percentage,
                 :feature_centroid_lon, :feature_centroid_lat, :geometry, :intersection_geometry,
                 :original_properties, :associated_district, :associated_sector, :associated_year,
                 :extraction_type, :created_at, :updated_at)
                ON CONFLICT (unique_id) DO UPDATE SET
                    slope_value = EXCLUDED.slope_value,
                    intersection_area = EXCLUDED.intersection_area,
                    coverage_percentage = EXCLUDED.coverage_percentage,
                    feature_centroid_lon = EXCLUDED.feature_centroid_lon,
                    feature_centroid_lat = EXCLUDED.feature_centroid_lat,
                    geometry = EXCLUDED.geometry,
                    intersection_geometry = EXCLUDED.intersection_geometry,
                    original_properties = EXCLUDED.original_properties,
                    updated_at = EXCLUDED.updated_at;
                """
                for f in feats:
                    centroid = f.get('feature_centroid', {})
                    conn.execute(text(ins_sql), {
                        'unique_id': f.get('unique_id'),
                        'slope_value': f.get('slope_value'),
                        'intersection_area': f.get('intersection_area'),
                        'coverage_percentage': f.get('coverage_percentage'),
                        'feature_centroid_lon': centroid.get('longitude'),
                        'feature_centroid_lat': centroid.get('latitude'),
                        'geometry': json.dumps(f.get('geometry_wgs84')),
                        'intersection_geometry': json.dumps(f.get('intersection_geometry')) if f.get('intersection_geometry') else None,
                        'original_properties': json.dumps(f.get('original_properties', {})),
                        'associated_district': district,
                        'associated_sector': sector,
                        'associated_year': year,
                        'extraction_type': extraction_type,
                        'created_at': f.get('created_at'),
                        'updated_at': _fmt_ts(datetime.now())
                    })
                conn.commit()
            return {'success': True, 'table': table, 'saved': len(result.get('slope_features', []))}
        except Exception as e:
            logger.error(f"Postgres save error: {e}")
            return {'success': False, 'error': str(e)}

    def _generate_slope_table_name(self, district, sector, year):
        d = _sanitize_name(district, "district")
        s = _sanitize_name(sector, "sector")
        y = str(year or datetime.utcnow().year)
        name = f"geojson_slope_data_{d}_{s}_{y}"
        if name[0].isdigit():
            name = "t_" + name
        return name[:60]

# Backward-compat alias so existing URLs keep working
class SlopeGeoJsonETLView(SlopeGeoJsonToESView):
    pass
