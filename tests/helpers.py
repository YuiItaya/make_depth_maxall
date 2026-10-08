"""テスト用の合成データ作成と、一時フォルダでの実行を助ける共通処理。"""
import contextlib
import io
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# 中間ファイル（./split, ./rank）のため作業フォルダを移動するので、並列処理の子プロセスからも
# パッケージを読めるよう、リポジトリの絶対パスを通しておく
REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import geopandas as gpd  # noqa: E402
import shapely  # noqa: E402

GEOGRAPHIC_EPSG = 6668  # 作成ツールの作業座標系（JGD2011 緯度経度）
PLANE_EPSG = 6677       # 平面直角座標系 第IX系（面積をm²で測る用）


def write_shp(path, features, field="rank", crs=GEOGRAPHIC_EPSG, encoding="shift-jis"):
    """[(値, 形状)] を Shapefile に書いてパス文字列を返す。"""
    gdf = gpd.GeoDataFrame({field: [v for v, _ in features]}, geometry=[g for _, g in features], crs=crs)
    gdf.to_file(path, encoding=encoding)
    return str(path)


def area_m2(geom, crs=GEOGRAPHIC_EPSG):
    """形状の面積（m²）。緯度経度の形状は平面直角座標に変換して測る。"""
    if geom is None or geom.is_empty:
        return 0.0
    return float(gpd.GeoSeries([geom], crs=crs).to_crs(PLANE_EPSG).area.iloc[0])


def rank_geometries(path):
    """出力Shapefileを読み、{rank: 緯度経度の形状} を返す。

    面の切り取りは緯度経度で行われるため、比較も緯度経度に戻して行う
    （平面直角座標で直線を引き直すと、長い辺に沿って投影による微小なずれが出る）。
    """
    gdf = gpd.read_file(path).to_crs(GEOGRAPHIC_EPSG)
    return {int(r): shapely.union_all(g.geometry.values) for r, g in gdf.groupby("rank")}


class TempWorkdirTestCase(unittest.TestCase):
    """一時フォルダを作業フォルダにして実行するテストの基底クラス。"""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._cwd = os.getcwd()
        os.chdir(self.tmp)

    def tearDown(self):
        os.chdir(self._cwd)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_pipeline(self, config):
        """処理を実行する（ログはテスト出力を汚さないよう捨てる）。"""
        from depth_maxall import processing

        with contextlib.redirect_stdout(io.StringIO()):
            processing.run_pipeline(config)

    def assertAreaClose(self, a, b, tol_m2=0.01, msg=None):
        """2つの形状の食い違い（対称差）の面積が tol_m2 以下であること。"""
        a = a if a is not None else shapely.Polygon()
        b = b if b is not None else shapely.Polygon()
        diff = area_m2(a.symmetric_difference(b))
        self.assertLessEqual(diff, tol_m2, msg or f"食い違い {diff:.4f}㎡")
