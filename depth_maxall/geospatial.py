import geopandas as gpd
from shapely.geometry import box

from .constants import JGD2011
from .errors import InputDataError

def normalize_clip_bounds(clip_bounds):
    if not clip_bounds:
        return None

    if len(clip_bounds) != 4:
        raise InputDataError(
            '警告：clip_bounds は [minx, miny, maxx, maxy] の4値で指定してください。'
        )

    minx, miny, maxx, maxy = [float(value) for value in clip_bounds]
    if minx >= maxx or miny >= maxy:
        raise InputDataError(
            '警告：clip_bounds の座標順が不正です。minx < maxx かつ miny < maxy としてください。'
        )
    return minx, miny, maxx, maxy
def clip_to_bounds(depth_gpd, clip_bounds):
    clip_bounds = normalize_clip_bounds(clip_bounds)
    if clip_bounds is None:
        return depth_gpd

    clip_geom = gpd.GeoDataFrame(
        geometry=[box(*clip_bounds)],
        crs=f"EPSG:{JGD2011}",
    )
    clipped = gpd.clip(depth_gpd, clip_geom)
    return clipped.reset_index(drop=True)
def dissolve_input_values(depth_gpd):
    return depth_gpd.dissolve(by="value").reset_index()
