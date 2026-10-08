import geopandas as gpd
from shapely import make_valid
from shapely.geometry import MultiPolygon, Polygon, box

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
def polygon_parts(geometries):
    """集約済みの形状から面だけを取り出して1つの MultiPolygon にする。

    ランク別に集約した面には、切り出し等で生じた線・点の切れ端が混ざる（GeometryCollection）。
    そのままだと差分・結合の計算が数倍遅くなるため、面だけを残す。集約済みで面どうしは
    重ならないので、結合し直さずに部品を並べるだけでよい。
    """
    import shapely

    polygons = []
    for geometry in geometries:
        for part in shapely.get_parts(geometry):
            if part.geom_type == "Polygon":
                polygons.append(part)
            elif part.geom_type == "MultiPolygon":
                polygons.extend(shapely.get_parts(part))
            elif part.geom_type == "GeometryCollection":
                polygons.extend(shapely.get_parts(polygon_parts([part])))
    return shapely.MultiPolygon(polygons) if polygons else shapely.MultiPolygon()


def dissolve_input_values(depth_gpd):
    return depth_gpd.dissolve(by="value").reset_index()
def _extract_polygonal(geom):
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    parts = []
    for part in getattr(geom, "geoms", []):
        if isinstance(part, Polygon):
            parts.append(part)
        elif isinstance(part, MultiPolygon):
            parts.extend(part.geoms)
    return MultiPolygon(parts)
def repair_output_geometries(gdf):
    # 出力直前の不正ジオメトリを修復し、ポリゴン成分のみを残す
    invalid_mask = ~gdf.geometry.is_valid
    invalid_count = int(invalid_mask.sum())
    if invalid_count == 0:
        return gdf, 0

    gdf = gdf.copy()
    gdf.loc[invalid_mask, "geometry"] = [
        _extract_polygonal(make_valid(geom)) for geom in gdf.geometry[invalid_mask]
    ]
    return gdf, invalid_count
