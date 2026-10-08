"""合成データで処理全体（入力→ランク分解→結合→重なり削除→出力）を確かめる。"""
from pathlib import Path

import geopandas as gpd
import shapely
from shapely.geometry import Polygon, box

from helpers import PLANE_EPSG, TempWorkdirTestCase, rank_geometries, write_shp


def expected_ranks(normal, extra=()):
    """仕様どおりの期待値: 高いランクが優先。低優先は通常データが無い範囲だけ各ランクのまま。"""
    by_rank = {}
    for rank, geom in normal:
        by_rank[rank] = shapely.union(by_rank.get(rank, shapely.Polygon()), geom)
    out, higher = {}, shapely.Polygon()
    for rank in sorted(by_rank, reverse=True):
        out[rank] = shapely.difference(by_rank[rank], higher)
        higher = shapely.union(higher, by_rank[rank])
    for rank, geom in extra:
        piece = shapely.difference(geom, higher)
        out[rank] = shapely.union(out.get(rank, shapely.Polygon()), piece)
    return {r: g for r, g in out.items() if not g.is_empty}


class PipelineTest(TempWorkdirTestCase):
    def config(self, inputs, **processing):
        out = self.tmp / "out" / "L2.shp"
        return out, {"processing": {"output_path": str(out), "output_epsg": PLANE_EPSG, **processing},
                     "inputs": inputs}

    def assertRanks(self, out_path, expected):
        actual = rank_geometries(out_path)
        self.assertEqual(sorted(actual), sorted(expected))
        for rank in expected:
            self.assertAreaClose(actual[rank], expected[rank], msg=f"rank{rank}")

    def test_higher_rank_wins_where_ranks_overlap(self):
        features = [
            (1, box(139.00, 36.00, 139.03, 36.03)),
            (2, box(139.01, 36.01, 139.04, 36.02)),
            (3, box(139.015, 36.005, 139.02, 36.025)),
        ]
        path = write_shp(self.tmp / "a.shp", features)
        out, cfg = self.config([{"path": path, "field": "rank"}])
        self.run_pipeline(cfg)
        self.assertRanks(out, expected_ranks(features))
        # 出力のランク同士は重ならない
        geoms = list(rank_geometries(out).values())
        for i in range(len(geoms)):
            for j in range(i + 1, len(geoms)):
                self.assertAreaClose(geoms[i].intersection(geoms[j]), shapely.Polygon())

    def test_overlapping_inputs_take_maximum_rank(self):
        a = write_shp(self.tmp / "a.shp", [(2, box(139.00, 36.00, 139.02, 36.02))])
        b = write_shp(self.tmp / "b.shp", [(4, box(139.01, 36.01, 139.03, 36.03))])
        out, cfg = self.config([{"path": a, "field": "rank"}, {"path": b, "field": "rank"}])
        self.run_pipeline(cfg)
        self.assertRanks(out, expected_ranks([(2, box(139.00, 36.00, 139.02, 36.02)),
                                              (4, box(139.01, 36.01, 139.03, 36.03))]))

    def test_low_priority_fills_only_where_normal_data_is_absent(self):
        normal = [(1, box(139.00, 36.00, 139.02, 36.02))]
        extra = [(3, box(139.01, 36.00, 139.04, 36.02)), (2, box(139.03, 36.00, 139.05, 36.03))]
        a = write_shp(self.tmp / "normal.shp", normal)
        b = write_shp(self.tmp / "extra.shp", extra)
        out, cfg = self.config([{"path": a, "field": "rank", "group": "normal"},
                                {"path": b, "field": "rank", "group": "extra", "is_extra": True}])
        self.run_pipeline(cfg)
        # 通常データのランク1は、低優先のランク3に上書きされない
        self.assertRanks(out, expected_ranks(normal, extra))

    def test_low_priority_with_many_normal_ranks(self):
        # 通常データが3ランク以上あると、低優先データは「最下位ランク」と「それより上の和」を順に引く
        normal = [(1, box(139.00, 36.00, 139.06, 36.03)), (2, box(139.01, 36.00, 139.03, 36.03)),
                  (3, box(139.02, 36.01, 139.025, 36.02)), (5, box(139.04, 36.00, 139.05, 36.02))]
        extra = [(4, box(139.005, 36.02, 139.08, 36.05)), (1, box(139.055, 36.00, 139.09, 36.025))]
        a = write_shp(self.tmp / "normal.shp", normal)
        b = write_shp(self.tmp / "extra.shp", extra)
        out, cfg = self.config([{"path": a, "field": "rank", "group": "normal"},
                                {"path": b, "field": "rank", "group": "extra", "is_extra": True}])
        self.run_pipeline(cfg)
        self.assertRanks(out, expected_ranks(normal, extra))
        # 出力は rank の昇順（以前のランク別集約と同じ並び）
        self.assertEqual(gpd.read_file(out)["rank"].tolist(), sorted(expected_ranks(normal, extra)))

    def test_output_is_2d_in_requested_crs_and_field(self):
        z_square = Polygon([(139.00, 36.00, 5), (139.02, 36.00, 5), (139.02, 36.02, 5), (139.00, 36.02, 5)])
        path = write_shp(self.tmp / "z.shp", [(2, z_square)])
        out, cfg = self.config([{"path": path, "field": "rank"}], output_field="rank")
        self.run_pipeline(cfg)
        gdf = gpd.read_file(out)
        self.assertEqual(gdf.crs.to_epsg(), PLANE_EPSG)
        self.assertIn("rank", gdf.columns)
        self.assertFalse(gdf.geometry.has_z.any(), "Z値が残っている")

    def test_clip_bounds_removes_outside(self):
        path = write_shp(self.tmp / "a.shp", [(1, box(139.00, 36.00, 139.10, 36.10))])
        clip = [139.02, 36.02, 139.05, 36.06]
        out, cfg = self.config([{"path": path, "field": "rank"}], clip_bounds=clip)
        self.run_pipeline(cfg)
        self.assertRanks(out, {1: box(*clip)})

    def test_fixed_rank_and_value_reclass(self):
        fixed = write_shp(self.tmp / "L2_3.0-5.0m.shp", [(0, box(139.00, 36.00, 139.01, 36.01))], field="dummy")
        depth = write_shp(self.tmp / "depth.shp", [(0.3, box(139.02, 36.00, 139.03, 36.01)),
                                                    (7.5, box(139.04, 36.00, 139.05, 36.01))], field="depth")
        out, cfg = self.config([
            {"path": fixed, "fixed_value": 3, "group": "fixed"},
            {"path": depth, "field": "depth", "group": "depth", "value_reclass": {"template": "depth_m"}},
        ])
        self.run_pipeline(cfg)
        self.assertRanks(out, {3: box(139.00, 36.00, 139.01, 36.01),
                               1: box(139.02, 36.00, 139.03, 36.01),
                               4: box(139.04, 36.00, 139.05, 36.01)})

    def test_invalid_input_geometry_is_repaired(self):
        bowtie = Polygon([(139.00, 36.00), (139.02, 36.02), (139.02, 36.00), (139.00, 36.02)])
        path = write_shp(self.tmp / "bowtie.shp", [(2, bowtie)])
        out, cfg = self.config([{"path": path, "field": "rank"}])
        self.run_pipeline(cfg)
        gdf = gpd.read_file(out)
        self.assertTrue(gdf.is_valid.all())
        self.assertAreaClose(rank_geometries(out)[2], shapely.make_valid(bowtie))

    def test_group_outputs_match_all_river_output_per_group(self):
        west = [(1, box(139.00, 36.00, 139.03, 36.03)), (3, box(139.01, 36.01, 139.02, 36.02))]
        east = [(2, box(139.02, 36.00, 139.05, 36.03))]
        w = write_shp(self.tmp / "west.shp", west)
        e = write_shp(self.tmp / "east.shp", east)
        out, cfg = self.config([
            {"path": w, "field": "rank", "group": "west", "output_group": "西"},
            {"path": e, "field": "rank", "group": "east", "output_group": "東"},
        ], output_group_files=True)
        self.run_pipeline(cfg)
        group_dir = Path(out).parent / f"{Path(out).stem}_by_group"
        self.assertEqual(sorted(p.stem for p in group_dir.glob("*.shp")), ["東", "西"])
        self.assertRanks(group_dir / "西.shp", expected_ranks(west))
        self.assertRanks(group_dir / "東.shp", expected_ranks(east))
        self.assertRanks(out, expected_ranks(west + east))
