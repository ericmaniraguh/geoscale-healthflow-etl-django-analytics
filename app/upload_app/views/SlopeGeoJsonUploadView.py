"""
Fiona-free geo upload & convert services for Rwanda slope datasets.

- GeoTIFF -> GeoJSON (EPSG:4326, clipped to Rwanda)
- Shapefile ZIP -> GeoJSON (EPSG:4326, clipped to Rwanda) using pyshp
- Optional district/sector tagging by centroid (if boundary files are provided)
- MongoDB storage for data + metadata
- DRF endpoints

Settings you can add (optional):
--------------------------------
MONGO_URI
MONGO_DB_GEOJSON_NAME              (default 'slope_raster_GeoJson_db')
MONGO_COLLECTION_GEOJSON_NAME      (default 'slope_GeoJson_uploads')
RWANDA_BOUNDARY_GEOJSON            (path to Rwanda admin-0 polygon, WGS84)
RWANDA_DISTRICTS_GEOJSON           (path to Rwanda district polygons, WGS84)
RWANDA_SECTORS_GEOJSON             (path to Rwanda sector polygons, WGS84)
RWANDA_DISTRICT_NAME_FIELD         (default 'district')
RWANDA_SECTOR_NAME_FIELD           (default 'sector')
"""

import os
import re
import io
import json
import uuid
import zipfile
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any, List
import numpy as np
import rasterio
from rasterio.features import shapes
from rasterio.mask import mask as rio_mask
from rasterio.warp import transform_geom as rio_transform_geom

from shapely.geometry import shape as shp_shape, mapping as shp_mapping, Polygon
from shapely.ops import unary_union
from shapely.strtree import STRtree

# Fiona removed. Use pyshp + pyproj for shapefile handling and reprojection
import shapefile  # pyshp
from pyproj import CRS, Transformer

# Django/DRF
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings

# Mongo
from pymongo import MongoClient

import logging
logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# Rwanda loose bounds (WGS84)
# ----------------------------------------------------------
RWANDA_LON_MIN, RWANDA_LON_MAX = 28.6, 31.0
RWANDA_LAT_MIN, RWANDA_LAT_MAX = -3.1, -0.8

def _rwanda_bbox_polygon_wgs84() -> Polygon:
    return Polygon([
        (RWANDA_LON_MIN, RWANDA_LAT_MIN),
        (RWANDA_LON_MAX, RWANDA_LAT_MIN),
        (RWANDA_LON_MAX, RWANDA_LAT_MAX),
        (RWANDA_LON_MIN, RWANDA_LAT_MAX),
        (RWANDA_LON_MIN, RWANDA_LAT_MIN),
    ])

def _load_geojson_file(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    if not os.path.exists(path):
        logger.warning(f"Boundary file not found: {path}")
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to read geojson {path}: {e}")
        return None

def _rwanda_boundary_geojson() -> Dict[str, Any]:
    gj = _load_geojson_file(getattr(settings, "RWANDA_BOUNDARY_GEOJSON", None))
    if gj and "features" in gj and gj["features"]:
        return gj  # assumed WGS84
    # fallback to bbox
    poly = shp_mapping(_rwanda_bbox_polygon_wgs84())
    return {"type": "FeatureCollection", "features": [{"type": "Feature", "geometry": poly, "properties": {}}]}

def _union_features_to_geom(gj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        geoms = [shp_shape(f["geometry"]) for f in gj.get("features", []) if f.get("geometry")]
        if not geoms:
            return None
        return shp_mapping(unary_union(geoms))
    except Exception as e:
        logger.warning(f"Could not union features: {e}")
        feats = gj.get("features", [])
        return feats[0].get("geometry") if feats else None

def _build_admin_index(gj: Optional[Dict[str, Any]]):
    if not gj or "features" not in gj:
        return None
    polys, props_list = [], []
    for f in gj["features"]:
        geom = f.get("geometry")
        props = f.get("properties", {}) or {}
        if not geom:
            continue
        try:
            p = shp_shape(geom)
            if p.is_valid and not p.is_empty:
                polys.append(p); props_list.append(props)
        except Exception:
            continue
    if not polys:
        return None
    tree = STRtree(polys)
    return {"tree": tree, "polys": polys, "props": props_list}

def _tag_admin_by_centroid(geom_wgs84: Dict[str, Any], district_index=None, sector_index=None):
    tags = {"district": None, "sector": None}
    try:
        g = shp_shape(geom_wgs84)
        if g.is_empty:
            return tags
        c = g.centroid
        if district_index:
            hits = district_index["tree"].query(c)
            for poly in hits:
                i = district_index["polys"].index(poly)
                props = district_index["props"][i]
                name_field = getattr(settings, "RWANDA_DISTRICT_NAME_FIELD", "district")
                tags["district"] = props.get(name_field) or props.get(name_field.upper()) or props.get("DISTRICT")
                if tags["district"]:
                    break
        if sector_index:
            hits = sector_index["tree"].query(c)
            for poly in hits:
                i = sector_index["polys"].index(poly)
                props = sector_index["props"][i]
                name_field = getattr(settings, "RWANDA_SECTOR_NAME_FIELD", "sector")
                tags["sector"] = props.get(name_field) or props.get(name_field.upper()) or props.get("SECTOR")
                if tags["sector"]:
                    break
    except Exception:
        pass
    return tags

# ----------------------------------------------------------
# Tiny helpers for CRS/transform (pyshp fallback)
# ----------------------------------------------------------
def _detect_shapefile_crs(shp_path: str) -> CRS:
    prj = os.path.splitext(shp_path)[0] + ".prj"
    if os.path.exists(prj):
        try:
            with open(prj, "r", encoding="utf-8") as f:
                wkt = f.read()
            return CRS.from_wkt(wkt)
        except Exception:
            pass
    # default to WGS84
    return CRS.from_epsg(4326)

def _transform_coords_recursive(coords, transformer: Transformer):
    # coords can be nested lists; base case is [x, y] or [x, y, z]
    if isinstance(coords, (list, tuple)) and coords and isinstance(coords[0], (int, float)):
        x, y = coords[0], coords[1]
        x2, y2 = transformer.transform(x, y)
        if len(coords) > 2:
            return [x2, y2] + list(coords[2:])
        return [x2, y2]
    # recurse
    return [_transform_coords_recursive(c, transformer) for c in coords]

def _reproject_geom_to_wgs84(geom: Dict[str, Any], src_crs: CRS) -> Dict[str, Any]:
    if not geom:
        return geom
    if int(src_crs.to_epsg() or 0) == 4326:
        return geom
    transformer = Transformer.from_crs(src_crs, CRS.from_epsg(4326), always_xy=True)
    gtype = geom.get("type")
    coords = geom.get("coordinates")
    if coords is None:
        return geom
    new_coords = _transform_coords_recursive(coords, transformer)
    return {"type": gtype, "coordinates": new_coords}

# ----------------------------------------------------------
# GeoTIFF -> GeoJSON (WGS84, Rwanda-clipped)
# ----------------------------------------------------------
def tif_to_geojson(
    tif_path: str,
    simplify_tolerance: float = 0.001,   # degrees (after reproject to WGS84)
    max_features: int = 8000,
    force_epsg: Optional[int] = None,
    clip_to_rwanda: bool = True,
    rwanda_boundary_path: Optional[str] = None,
    quantize_step: Optional[float] = None,
    add_admin_tags: bool = False
) -> Dict[str, Any]:
    with rasterio.open(tif_path) as src:
        src_crs = src.crs
        if force_epsg:
            try:
                src_crs = CRS.from_epsg(int(force_epsg))
                logger.info(f"Forced source CRS: EPSG:{force_epsg}")
            except Exception as e:
                raise ValueError(f"Invalid force_epsg {force_epsg}: {e}")
        if src_crs is None:
            raise ValueError("Raster has no CRS. Provide 'force_epsg'.")

        data = src.read(1)
        nodata = src.nodata
        transform = src.transform

        # Clip to Rwanda
        if clip_to_rwanda:
            rwa_gj = _load_geojson_file(rwanda_boundary_path) or _rwanda_boundary_geojson()
            rwa_geom_wgs84 = _union_features_to_geom(rwa_gj)
            if not rwa_geom_wgs84:
                raise ValueError("Rwanda boundary geometry not available")
            rwa_geom_src = rio_transform_geom("EPSG:4326", src_crs.to_string(), rwa_geom_wgs84, precision=6)
            data, out_transform = rio_mask(src, [rwa_geom_src], crop=True, filled=True)
            data = data[0]
            transform = out_transform
            logger.info("Raster clipped to Rwanda boundary")

        if quantize_step and quantize_step > 0:
            with np.errstate(invalid="ignore"):
                data = np.round(data / quantize_step) * quantize_step

        if nodata is not None:
            mask_valid = (data != nodata) & np.isfinite(data)
        else:
            mask_valid = np.isfinite(data)
        valid_pixels = int(np.sum(mask_valid))
        if valid_pixels == 0:
            raise ValueError("No valid data found after masking/clipping")

        # Optional admin indices
        district_index = sector_index = None
        if add_admin_tags:
            districts_gj = _load_geojson_file(getattr(settings, "RWANDA_DISTRICTS_GEOJSON", None))
            sectors_gj   = _load_geojson_file(getattr(settings, "RWANDA_SECTORS_GEOJSON", None))
            district_index = _build_admin_index(districts_gj)
            sector_index   = _build_admin_index(sectors_gj)

        features: List[Dict[str, Any]] = []
        count = 0
        for geom, value in shapes(data, mask=mask_valid, transform=transform):
            if count >= max_features:
                logger.warning(f"Reached max features limit ({max_features})")
                break
            try:
                geom_wgs84 = rio_transform_geom(src_crs.to_string(), "EPSG:4326", geom, precision=6)
                g = shp_shape(geom_wgs84)
                if not g.is_valid or g.is_empty:
                    continue
                if simplify_tolerance and simplify_tolerance > 0:
                    g = g.simplify(simplify_tolerance, preserve_topology=True)
                    if not g.is_valid or g.is_empty:
                        continue
                geom_wgs84 = shp_mapping(g)
            except Exception:
                continue

            props = {
                "value": float(value) if np.isfinite(value) else None,
                "slope_value": float(value) if np.isfinite(value) else None
            }

            if add_admin_tags:
                tag = _tag_admin_by_centroid(geom_wgs84, district_index, sector_index)
                if tag.get("district"): props["district"] = tag["district"]
                if tag.get("sector"):   props["sector"]   = tag["sector"]

            features.append({"type": "Feature", "geometry": geom_wgs84, "properties": props})
            count += 1

        # Dataset bounds (in WGS84)
        src_bounds = list(src.bounds)
        bounds_poly_src = {
            "type": "Polygon",
            "coordinates": [[
                [src_bounds[0], src_bounds[1]],
                [src_bounds[2], src_bounds[1]],
                [src_bounds[2], src_bounds[3]],
                [src_bounds[0], src_bounds[3]],
                [src_bounds[0], src_bounds[1]],
            ]]
        }
        try:
            bounds_poly_wgs84 = rio_transform_geom(src_crs.to_string(), "EPSG:4326", bounds_poly_src, precision=6)
        except Exception:
            bounds_poly_wgs84 = None

        return {
            "type": "FeatureCollection",
            "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
            "features": features,
            "metadata": {
                "raster_width": src.width,
                "raster_height": src.height,
                "raster_bounds_src": src_bounds,
                "raster_crs": str(src_crs),
                "dataset_bounds_wgs84": bounds_poly_wgs84,
                "total_features": len(features),
                "nodata_value": nodata,
                "max_features_limit": max_features,
                "features_truncated": count >= max_features,
                "valid_pixels": valid_pixels,
                "simplify_tolerance_deg": simplify_tolerance,
                "quantize_step": quantize_step,
                "clipped_to_rwanda": bool(clip_to_rwanda),
                "admin_tagging": bool(add_admin_tags)
            }
        }

# ----------------------------------------------------------
# Shapefile ZIP -> GeoJSON (pyshp + pyproj)
# ----------------------------------------------------------
def shapefile_zip_to_geojson(
    zip_path: str,
    simplify_tolerance: float = 0.001,
    clip_to_rwanda: bool = True,
    add_admin_tags: bool = False
) -> Dict[str, Any]:
    """
    Convert a Shapefile (zipped) to WGS84 GeoJSON, optionally clipped to Rwanda and tagged.
    Uses pyshp (no Fiona).
    """
    if not zipfile.is_zipfile(zip_path):
        raise ValueError("Provided file is not a valid .zip")

    # Extract to temp dir
    tmpdir = tempfile.mkdtemp(prefix="shpzip_")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(tmpdir)

    # Find .shp
    shp_files = [os.path.join(tmpdir, f) for f in os.listdir(tmpdir) if f.lower().endswith(".shp")]
    if not shp_files:
        raise ValueError("No .shp file found inside the zip")
    shp_path = shp_files[0]

    # Read with pyshp
    r = shapefile.Reader(shp_path)
    fields = r.fields[1:]  # skip DeletionFlag
    field_names = [f[0] for f in fields]

    # CRS detection and transformer
    src_crs = _detect_shapefile_crs(shp_path)
    transformer = Transformer.from_crs(src_crs, CRS.from_epsg(4326), always_xy=True)

    # Rwanda boundary shape (for clipping)
    rwanda_fc = _rwanda_boundary_geojson()
    rwanda_union_wgs84 = _union_features_to_geom(rwanda_fc)
    rwanda_shape = shp_shape(rwanda_union_wgs84) if rwanda_union_wgs84 else None

    # Admin indices
    district_index = sector_index = None
    if add_admin_tags:
        districts_gj = _load_geojson_file(getattr(settings, "RWANDA_DISTRICTS_GEOJSON", None))
        sectors_gj   = _load_geojson_file(getattr(settings, "RWANDA_SECTORS_GEOJSON", None))
        district_index = _build_admin_index(districts_gj)
        sector_index   = _build_admin_index(sectors_gj)

    features: List[Dict[str, Any]] = []

    for sr in r.iterShapeRecords():
        geom = sr.shape.__geo_interface__
        props = dict(zip(field_names, sr.record))

        # reproject to WGS84 if needed
        geom_wgs84 = _reproject_geom_to_wgs84(geom, src_crs)

        # to shapely
        try:
            g = shp_shape(geom_wgs84)
            if not g.is_valid or g.is_empty:
                continue
        except Exception:
            continue

        # clip to Rwanda
        if clip_to_rwanda and rwanda_shape:
            g = g.intersection(rwanda_shape)
            if g.is_empty:
                continue

        # simplify in degrees
        if simplify_tolerance and simplify_tolerance > 0:
            g = g.simplify(simplify_tolerance, preserve_topology=True)
            if g.is_empty:
                continue

        geom_wgs84 = shp_mapping(g)

        if add_admin_tags:
            tag = _tag_admin_by_centroid(geom_wgs84, district_index, sector_index)
            if tag.get("district"): props["district"] = tag["district"]
            if tag.get("sector"):   props["sector"]   = tag["sector"]

        features.append({"type": "Feature", "geometry": geom_wgs84, "properties": props})

    return {
        "type": "FeatureCollection",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": features,
        "metadata": {
            "source": "Shapefile zip (pyshp)",
            "total_features": len(features),
            "simplify_tolerance_deg": simplify_tolerance,
            "clipped_to_rwanda": bool(clip_to_rwanda),
            "admin_tagging": bool(add_admin_tags),
            "src_crs": str(src_crs)
        }
    }

# ----------------------------------------------------------
# Mongo helpers
# ----------------------------------------------------------
def _create_collection_name(region: str, data_type: str, year: int) -> str:
    clean_region = re.sub(r'[^a-zA-Z0-9]', '', str(region).lower())
    clean_type = re.sub(r'[^a-zA-Z0-9]', '', str(data_type).lower())
    return f"slope_{clean_type}_{clean_region}_{year}"

def _get_mongo_collection(collection_name: Optional[str] = None, geojson: bool = True):
    try:
        mongo_uri = getattr(settings, 'MONGO_URI', None) or "mongodb://localhost:27017"
        if geojson:
            mongo_db_name = getattr(settings, 'MONGO_DB_GEOJSON_NAME', 'slope_raster_GeoJson_db')
            collection_name = collection_name or getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads')
        else:
            mongo_db_name = getattr(settings, 'MONGO_SLOPE_DB', 'slope_raster_database')
            collection_name = collection_name or 'slope_uploads'
        client = MongoClient(mongo_uri)
        db = client[mongo_db_name]
        collection = db[collection_name]
        client.admin.command('ping')
        return client, collection
    except Exception as e:
        logger.error(f"MongoDB connection error: {e}")
        return None, None

# ----------------------------------------------------------
# DRF Views
# ----------------------------------------------------------
class GeoTiffUploadView(APIView):
    """
    POST /api/upload/tif
    form-data:
      - tif_file: <.tif>
      - region: Rwanda (required)
      - year: 2025
      - data_type: slope
      - simplify_tolerance: 0.003
      - max_features: 8000
      - force_epsg: 32736    # if your tif lacks/has wrong CRS
      - clip_to_rwanda: true
      - quantize_step: 0.5
      - add_admin_tags: true|false
    """

    def post(self, request):
        # file
        uploaded = request.FILES.get('tif_file') or (next(iter(request.FILES.values())) if request.FILES else None)
        if not uploaded:
            return Response({'error': 'No TIF file provided'}, status=status.HTTP_400_BAD_REQUEST)

        # params
        region = request.data.get('region', '')
        year = int(request.data.get('year', datetime.utcnow().year))
        data_type = request.data.get('data_type', 'slope')
        if not region:
            return Response({'error': 'Region is required'}, status=status.HTTP_400_BAD_REQUEST)

        dataset_name = request.data.get('dataset_name', uploaded.name)
        description = request.data.get('description', '')
        resolution = request.data.get('resolution', '')
        source = request.data.get('source', '')

        try:
            simplify_tolerance = float(request.data.get('simplify_tolerance', 0.002))
            max_features = int(request.data.get('max_features', 8000))
            force_epsg = request.data.get('force_epsg')
            force_epsg = int(force_epsg) if force_epsg not in (None, '', 'null') else None
            clip_to_rwanda = str(request.data.get('clip_to_rwanda', 'true')).lower() == 'true'
            quantize_step = request.data.get('quantize_step')
            quantize_step = float(quantize_step) if quantize_step not in (None, '', 'null') else None
            add_admin_tags = str(request.data.get('add_admin_tags', 'false')).lower() == 'true'
        except (ValueError, TypeError):
            return Response({'error': 'Invalid conversion parameters'}, status=status.HTTP_400_BAD_REQUEST)

        # checks
        if not uploaded.name.lower().endswith(('.tif', '.tiff')):
            return Response({'error': 'Expected .tif or .tiff'}, status=status.HTTP_400_BAD_REQUEST)
        if uploaded.size > 100 * 1024 * 1024:
            return Response({'error': 'File too large (max 100MB)'}, status=status.HTTP_400_BAD_REQUEST)

        tmp_path = None
        client = None
        try:
            upload_id = str(uuid.uuid4())
            upload_time = datetime.utcnow()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".tif") as tmp:
                for chunk in uploaded.chunks():
                    tmp.write(chunk)
                tmp_path = tmp.name

            geojson = tif_to_geojson(
                tmp_path,
                simplify_tolerance=simplify_tolerance,
                max_features=max_features,
                force_epsg=force_epsg,
                clip_to_rwanda=clip_to_rwanda,
                rwanda_boundary_path=getattr(settings, "RWANDA_BOUNDARY_GEOJSON", None),
                quantize_step=quantize_step,
                add_admin_tags=add_admin_tags
            )

            client, coll = _get_mongo_collection(geojson=True)
            if not coll:
                raise RuntimeError("Mongo connection failed")

            geojson_bytes = json.dumps(geojson).encode('utf-8')
            if len(geojson_bytes) > 15 * 1024 * 1024:
                return Response({
                    'error': f'GeoJSON too large: {len(geojson_bytes)/1024/1024:.1f}MB (>16MB Mongo limit)',
                    'suggestions': {
                        'increase_simplify_tolerance': max(0.003, simplify_tolerance * 2),
                        'reduce_max_features': max(1000, max_features // 2),
                        'use_quantize_step': quantize_step or 0.5
                    }
                }, status=status.HTTP_400_BAD_REQUEST)

            features_count = len(geojson["features"])
            doc = {
                "_upload_id": upload_id,
                "_dataset_name": dataset_name,
                "_region": region,
                "_data_type": data_type,
                "_year": int(year),
                "_upload_time": upload_time,
                "_uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
                "original_filename": uploaded.name,
                "original_file_size": uploaded.size,
                "geojson_size": len(geojson_bytes),
                "features_count": features_count,
                "description": description,
                "resolution": resolution,
                "source": source,
                "simplify_tolerance": simplify_tolerance,
                "max_features": max_features,
                "processing_type": "tif_to_geojson",
                "storage_type": "MongoDB_only",
                "geojson_data": geojson
            }
            coll.insert_one(doc)

            # metadata
            _, meta_coll = _get_mongo_collection(
                collection_name=f"{getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads')}_metadata",
                geojson=True
            )
            if meta_coll:
                meta_coll.insert_one({
                    "upload_id": upload_id,
                    "dataset_name": dataset_name,
                    "region": region,
                    "data_type": data_type,
                    "year": int(year),
                    "upload_time": upload_time,
                    "uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
                    "original_filename": uploaded.name,
                    "original_file_size": uploaded.size,
                    "geojson_size": len(geojson_bytes),
                    "features_count": features_count,
                    "data_collection_name": getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads'),
                    "file_type": "GeoJSON_from_GeoTIFF",
                    "processing_type": "tif_to_geojson",
                    "storage_type": "MongoDB_only",
                    "conversion_metadata": geojson.get('metadata', {})
                })

            return Response({
                "message": "GeoTIFF converted to Rwanda-safe GeoJSON and stored.",
                "upload_id": upload_id,
                "features_count": features_count
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f"TIF upload/convert failed: {e}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if client:
                try: client.close()
                except: pass
            if tmp_path and os.path.exists(tmp_path):
                try: os.remove(tmp_path)
                except Exception as ce: logger.warning(f"Temp cleanup failed: {ce}")

class ShapefileUploadView(APIView):
    """
    POST /api/upload/shapefile
    form-data:
      - shp_zip: <.zip> (must contain .shp/.shx/.dbf and .prj ideally)
      - region: Rwanda (required)
      - year: 2025
      - data_type: slope (or any)
      - simplify_tolerance: 0.003
      - clip_to_rwanda: true
      - add_admin_tags: true|false
    """
    def post(self, request):
        uploaded = request.FILES.get('shp_zip')
        if not uploaded:
            return Response({'error': 'No shapefile zip provided as "shp_zip"'}, status=status.HTTP_400_BAD_REQUEST)

        region = request.data.get('region', '')
        year = int(request.data.get('year', datetime.utcnow().year))
        data_type = request.data.get('data_type', 'slope')
        if not region:
            return Response({'error': 'Region is required'}, status=status.HTTP_400_BAD_REQUEST)

        dataset_name = request.data.get('dataset_name', uploaded.name)
        description = request.data.get('description', '')
        try:
            simplify_tolerance = float(request.data.get('simplify_tolerance', 0.002))
            clip_to_rwanda = str(request.data.get('clip_to_rwanda', 'true')).lower() == 'true'
            add_admin_tags = str(request.data.get('add_admin_tags', 'false')).lower() == 'true'
        except (ValueError, TypeError):
            return Response({'error': 'Invalid parameters'}, status=status.HTTP_400_BAD_REQUEST)

        if uploaded.size > 100 * 1024 * 1024:
            return Response({'error': 'Zip too large (max 100MB)'}, status=status.HTTP_400_BAD_REQUEST)

        tmp_zip = None
        client = None
        try:
            upload_id = str(uuid.uuid4())
            upload_time = datetime.utcnow()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                for chunk in uploaded.chunks():
                    tmp.write(chunk)
                tmp_zip = tmp.name

            geojson = shapefile_zip_to_geojson(
                tmp_zip,
                simplify_tolerance=simplify_tolerance,
                clip_to_rwanda=clip_to_rwanda,
                add_admin_tags=add_admin_tags
            )

            client, coll = _get_mongo_collection(geojson=True)
            if not coll:
                raise RuntimeError("Mongo connection failed")

            geojson_bytes = json.dumps(geojson).encode('utf-8')
            if len(geojson_bytes) > 15 * 1024 * 1024:
                return Response({'error': 'GeoJSON too large for Mongo (16MB limit)'}, status=status.HTTP_400_BAD_REQUEST)

            features_count = len(geojson["features"])
            coll.insert_one({
                "_upload_id": upload_id,
                "_dataset_name": dataset_name,
                "_region": region,
                "_data_type": data_type,
                "_year": int(year),
                "_upload_time": upload_time,
                "_uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
                "original_filename": uploaded.name,
                "original_file_size": uploaded.size,
                "geojson_size": len(geojson_bytes),
                "features_count": features_count,
                "description": description,
                "processing_type": "shapefile_to_geojson_pyshp",
                "storage_type": "MongoDB_only",
                "geojson_data": geojson
            })

            _, meta_coll = _get_mongo_collection(
                collection_name=f"{getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads')}_metadata",
                geojson=True
            )
            if meta_coll:
                meta_coll.insert_one({
                    "upload_id": upload_id,
                    "dataset_name": dataset_name,
                    "region": region,
                    "data_type": data_type,
                    "year": int(year),
                    "upload_time": upload_time,
                    "uploaded_by": request.user.username if request.user.is_authenticated else "anonymous",
                    "original_filename": uploaded.name,
                    "original_file_size": uploaded.size,
                    "geojson_size": len(geojson_bytes),
                    "features_count": features_count,
                    "data_collection_name": getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads'),
                    "file_type": "GeoJSON_from_Shapefile",
                    "processing_type": "shapefile_to_geojson_pyshp",
                    "storage_type": "MongoDB_only",
                    "conversion_metadata": geojson.get('metadata', {})
                })

            return Response({
                "message": "Shapefile (zip) converted to Rwanda-safe GeoJSON and stored.",
                "upload_id": upload_id,
                "features_count": features_count
            }, status=status.HTTP_201_CREATED)

        except Exception as e:
            logger.error(f"Shapefile convert failed: {e}")
            return Response({'error': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if client:
                try: client.close()
                except: pass
            if tmp_zip and os.path.exists(tmp_zip):
                try: os.remove(tmp_zip)
                except Exception as ce: logger.warning(f"Temp cleanup failed: {ce}")

class GeoJSONFetchView(APIView):
    """GET /api/geojson/{upload_id}  -> returns geojson_data + minimal metadata."""
    def get(self, request, upload_id):
        try:
            client, dbcoll = _get_mongo_collection(geojson=True)
            if not dbcoll:
                return Response({'error': 'Mongo connection failed'}, status=500)

            rec = dbcoll.find_one({"_upload_id": upload_id})
            if not rec:
                return Response({'error': 'Not found'}, status=404)

            rec['_id'] = str(rec['_id'])
            if '_upload_time' in rec:
                rec['_upload_time'] = rec['_upload_time'].isoformat()

            client.close()
            return Response({
                "upload_id": upload_id,
                "metadata": {
                    "dataset_name": rec.get('_dataset_name'),
                    "region": rec.get('_region'),
                    "features_count": rec.get('features_count'),
                    "storage_type": rec.get('storage_type', 'MongoDB_only')
                },
                "geojson": rec.get('geojson_data')
            }, status=200)
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            return Response({'error': str(e)}, status=500)

class SlopeMetadataListView(APIView):
    """GET /api/geojson/metadata  -> list all metadata records."""
    def get(self, request):
        try:
            client, db = _get_mongo_collection(
                collection_name=f"{getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads')}_metadata",
                geojson=True
            )
            if not db:
                return Response({'error': 'Mongo connection failed'}, status=500)

            records = list(db.find({}))
            for r in records:
                r['_id'] = str(r['_id'])
                if 'upload_time' in r:
                    r['upload_time'] = r['upload_time'].isoformat()

            client.close()
            return Response({
                "total_datasets": len(records),
                "all_datasets": records
            }, status=200)
        except Exception as e:
            logger.error(f"List metadata failed: {e}")
            return Response({'error': str(e)}, status=500)

class SlopeDataSearchView(APIView):
    """
    GET /api/geojson/search?dataset_name=...&region=...&data_type=...&year=...&upload_id=...&include_geojson=false
    """
    def get(self, request):
        dataset_name = request.query_params.get('dataset_name')
        region = request.query_params.get('region')
        data_type = request.query_params.get('data_type')
        year = request.query_params.get('year')
        upload_id = request.query_params.get('upload_id')
        include_geojson = request.query_params.get('include_geojson', 'false').lower() == 'true'

        try:
            client, coll = _get_mongo_collection(geojson=True)
            if not coll:
                return Response({'error': 'Mongo connection failed'}, status=500)

            q: Dict[str, Any] = {}
            if dataset_name: q['_dataset_name'] = dataset_name
            if region: q['_region'] = region
            if data_type: q['_data_type'] = data_type
            if year: q['_year'] = int(year)
            if upload_id: q['_upload_id'] = upload_id

            proj = {} if include_geojson else {"geojson_data": 0}
            docs = list(coll.find(q, proj))

            for d in docs:
                d['_id'] = str(d['_id'])
                if '_upload_time' in d:
                    d['_upload_time'] = d['_upload_time'].isoformat()

            client.close()
            return Response({"total_records": len(docs), "data": docs}, status=200)
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return Response({'error': str(e)}, status=500)

class DeleteDatasetView(APIView):
    """DELETE /api/geojson/{upload_id}"""
    def delete(self, request, upload_id):
        try:
            client, data_coll = _get_mongo_collection(geojson=True)
            if not data_coll:
                return Response({'error': 'Mongo connection failed'}, status=500)

            _, meta_coll = _get_mongo_collection(
                collection_name=f"{getattr(settings, 'MONGO_COLLECTION_GEOJSON_NAME', 'slope_GeoJson_uploads')}_metadata",
                geojson=True
            )

            data_res = data_coll.delete_many({"_upload_id": upload_id})
            meta_res = meta_coll.delete_one({"upload_id": upload_id}) if meta_coll else None

            client.close()
            return Response({
                "message": "Dataset deleted",
                "upload_id": upload_id,
                "records_deleted": data_res.deleted_count,
                "metadata_deleted": (meta_res.deleted_count if meta_res else 0)
            }, status=200)
        except Exception as e:
            logger.error(f"Delete failed: {e}")
            return Response({'error': str(e)}, status=500)

# Backwards-compat alias for your urls.py import:
# from .country_slope_GeoJson_upload_view import SlopeGeoJsonUploadView
# -> will map to GeoTiffUploadView (keeps old name working)
class SlopeGeoJsonUploadView(GeoTiffUploadView):
    pass
