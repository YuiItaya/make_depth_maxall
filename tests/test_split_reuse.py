"""河川別出力でランク分解結果を再利用しても、他の入力のデータが混ざらないことを確かめる。

実行: venv\\Scripts\\python.exe -m unittest discover -s tests
"""
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

# 中間ファイルのため作業フォルダを移動するので、並列処理の子プロセスからも
# パッケージを読めるよう、リポジトリの絶対パスを通しておく
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import geopandas as gpd
import shapely
from shapely.geometry import box

from depth_maxall import processing
from depth_maxall.config import normalize_config


def write_shp(path, polygons):
    """[(rank, polygon)] を EPSG:6668 の Shapefile に書く。"""
    gdf = gpd.GeoDataFrame(
        {"rank": [r for r, _ in polygons]}, geometry=[g for _, g in polygons], crs=6668)
    gdf.to_file(path, encoding="shift-jis")
    return str(path)


class SplitReuseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.cwd = os.getcwd()
        os.chdir(self.tmp)  # 中間ファイル（./split, ./rank）を一時フォルダに作る

    def tearDown(self):
        os.chdir(self.cwd)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_renamed_duplicate_names_do_not_collide(self):
        a = write_shp(self.tmp / "a.shp", [(1, box(139.0, 36.0, 139.01, 36.01))])
        config = normalize_config({"inputs": [
            {"path": a, "field": "rank", "name": "A_003"},
            {"path": a, "field": "rank", "name": "A"},
            {"path": a, "field": "rank", "name": "A"},  # 3件目に番号を付けると A_003（番号は1始まり）になり、1件目と重なる
        ]})
        names = [item["name"] for item in config["inputs"]]
        self.assertEqual(len(names), len(set(names)), names)

    def test_copy_selects_exact_input_names(self):
        split = self.tmp / "src"
        extra = split / "ex"
        extra.mkdir(parents=True)
        for name in ("M_1", "M_2", "M_1_1", "M_1_2", "MX_1"):
            (split / f"{name}.gpkg").write_bytes(b"")
        (extra / "M_3.gpkg").write_bytes(b"")
        target, target_extra = self.tmp / "dst", self.tmp / "dst" / "low"
        target_extra.mkdir(parents=True)
        processing.copy_split_files(
            [{"name": "M", "path": "M.shp"}], split, extra, target, target_extra)
        self.assertEqual(sorted(p.name for p in target.glob("*.gpkg")), ["M_1.gpkg", "M_2.gpkg"])
        self.assertEqual(list(target_extra.glob("*.gpkg")), [])

        shutil.rmtree(target)
        target_extra.mkdir(parents=True)
        processing.copy_split_files(
            [{"name": "M_1", "path": "M_1.shp"}, {"name": "M", "path": "M.shp", "is_extra": True}],
            split, extra, target, target_extra)
        self.assertEqual(sorted(p.name for p in target.glob("*.gpkg")), ["M_1_1.gpkg", "M_1_2.gpkg"])
        self.assertEqual(sorted(p.name for p in target_extra.glob("*.gpkg")), ["M_3.gpkg"])

    def test_group_outputs_contain_only_own_inputs(self):
        # 入力名を「M」「M_1」にして、中間ファイル名 M_1.gpkg（M のランク1）と
        # M_1_1.gpkg（M_1 のランク1）が並ぶ紛らわしい状態にする
        west = write_shp(self.tmp / "west.shp", [
            (1, box(139.00, 36.00, 139.04, 36.04)), (2, box(139.01, 36.01, 139.02, 36.02))])
        east = write_shp(self.tmp / "east.shp", [
            (1, box(139.10, 36.00, 139.14, 36.04)), (3, box(139.12, 36.02, 139.13, 36.03))])
        low = write_shp(self.tmp / "low.shp", [(4, box(139.03, 36.03, 139.12, 36.05))])
        out = self.tmp / "out" / "L2.shp"
        config = {
            "processing": {"output_path": str(out), "output_epsg": 6677, "output_group_files": True},
            "inputs": [
                {"path": west, "field": "rank", "name": "M", "group": "west", "output_group": "西"},
                {"path": east, "field": "rank", "name": "M_1", "group": "east", "output_group": "東"},
                {"path": low, "field": "rank", "name": "M_1_1", "group": "low", "output_group": "西",
                 "is_extra": True},
            ],
        }
        processing.run_pipeline(config)
        group_dir = out.parent / "L2_by_group"
        reused = {p.stem: gpd.read_file(p) for p in group_dir.glob("*.shp")}

        # 面の切り取りは緯度経度（EPSG:6668）で行われるため、比較も緯度経度で行う
        # （平面直角座標で直線を引き直すと、長い辺に沿って投影による微小なずれが出る）
        def area_outside(output, inputs):
            out_geom = output.to_crs(6668).geometry.union_all()
            in_geom = shapely.union_all([gpd.read_file(p).geometry.union_all() for p in inputs])
            return gpd.GeoSeries([out_geom.difference(in_geom)], crs=6668).to_crs(6677).area.iloc[0]

        # 東には西・低優先の範囲が一切含まれない
        self.assertLess(area_outside(reused["東"], [east]), 0.01)
        self.assertEqual(sorted(reused["東"]["rank"]), [1, 3])
        # 西は西＋低優先（西グループ）だけ
        self.assertLess(area_outside(reused["西"], [west, low]), 0.01)
        self.assertEqual(sorted(reused["西"]["rank"]), [1, 2, 4])

        # 再利用しない従来の方法（入力から分解し直す）と同じ結果になる
        normalized = normalize_config(config)
        processing.generate_group_outputs(normalized["inputs"], normalized["processing"], reuse_split=None)
        for name, gdf in reused.items():
            fresh = gpd.read_file(group_dir / f"{name}.shp")
            for rank in set(gdf["rank"]) | set(fresh["rank"]):
                a = gdf[gdf["rank"] == rank].geometry.union_all()
                b = fresh[fresh["rank"] == rank].geometry.union_all()
                self.assertAlmostEqual(a.symmetric_difference(b).area, 0.0, places=3, msg=f"{name} rank{rank}")


if __name__ == "__main__":
    unittest.main()
