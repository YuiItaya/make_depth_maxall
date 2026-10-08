"""河川別出力でランク分解結果を再利用しても、他の入力のデータが混ざらないことを確かめる。"""
import contextlib
import io
import shutil
import unittest

import geopandas as gpd
import shapely
from shapely.geometry import box

from helpers import TempWorkdirTestCase, area_m2, rank_geometries, write_shp
from depth_maxall import processing
from depth_maxall.config import normalize_config


class SplitReuseTest(TempWorkdirTestCase):
    def test_copy_selects_exact_input_names(self):
        split = self.tmp / "src"
        extra = split / "ex"
        extra.mkdir(parents=True)
        for name in ("M_1", "M_2", "M_1_1", "M_1_2", "MX_1"):
            (split / f"{name}.gpkg").write_bytes(b"")
        (extra / "M_3.gpkg").write_bytes(b"")
        target, target_extra = self.tmp / "dst", self.tmp / "dst" / "low"
        target_extra.mkdir(parents=True)
        with contextlib.redirect_stdout(io.StringIO()):
            processing.copy_split_files([{"name": "M", "path": "M.shp"}], split, extra, target, target_extra)
        self.assertEqual(sorted(p.name for p in target.glob("*.gpkg")), ["M_1.gpkg", "M_2.gpkg"])
        self.assertEqual(list(target_extra.glob("*.gpkg")), [])

        shutil.rmtree(target)
        target_extra.mkdir(parents=True)
        with contextlib.redirect_stdout(io.StringIO()):
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
        self.run_pipeline(config)
        group_dir = out.parent / "L2_by_group"
        reused = {p.stem: rank_geometries(p) for p in group_dir.glob("*.shp")}

        def outside(ranks, inputs):
            out_geom = shapely.union_all(list(ranks.values()))
            in_geom = shapely.union_all([gpd.read_file(p).geometry.union_all() for p in inputs])
            return area_m2(out_geom.difference(in_geom))

        self.assertLess(outside(reused["東"], [east]), 0.01)
        self.assertEqual(sorted(reused["東"]), [1, 3])
        self.assertLess(outside(reused["西"], [west, low]), 0.01)
        self.assertEqual(sorted(reused["西"]), [1, 2, 4])

        # 再利用しない従来の方法（入力から分解し直す）と同じ結果になる
        normalized = normalize_config(config)
        with contextlib.redirect_stdout(io.StringIO()):
            processing.generate_group_outputs(normalized["inputs"], normalized["processing"], reuse_split=None)
        for name, ranks in reused.items():
            fresh = rank_geometries(group_dir / f"{name}.shp")
            self.assertEqual(sorted(ranks), sorted(fresh), name)
            for rank in ranks:
                self.assertAreaClose(ranks[rank], fresh[rank], msg=f"{name} rank{rank}")


if __name__ == "__main__":
    unittest.main()
