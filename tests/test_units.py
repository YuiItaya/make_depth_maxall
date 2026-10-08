"""設定の正規化・値の変換・形状の補助関数を単体で確かめる。"""
import unittest

import pandas as pd
import shapely
from shapely.geometry import LineString, MultiPolygon, Point, box

from helpers import TempWorkdirTestCase, write_shp
from depth_maxall.config import normalize_config
from depth_maxall.errors import InputDataError
from depth_maxall.geospatial import clip_to_bounds, merge_disjoint_polygons, normalize_clip_bounds, polygon_parts
from depth_maxall.validation import (
    normalize_fixed_value,
    validate_output_field_name,
    validate_value_column,
)


class ConfigTest(TempWorkdirTestCase):
    def setUp(self):
        super().setUp()
        self.shp = write_shp(self.tmp / "a.shp", [(1, box(139.0, 36.0, 139.01, 36.01))])

    def test_input_names_stay_unique(self):
        # 3件目に番号を付けると A_003（番号は1始まり）になり、1件目と重なる。
        # 名前が重なると中間ファイル <name>_<rank>.gpkg が上書きされ、別河川のデータが混ざる
        config = normalize_config({"inputs": [
            {"path": self.shp, "field": "rank", "name": "A_003"},
            {"path": self.shp, "field": "rank", "name": "A"},
            {"path": self.shp, "field": "rank", "name": "A"},
            {"path": self.shp, "field": "rank", "name": "A"},
        ]})
        names = [item["name"] for item in config["inputs"]]
        self.assertEqual(len(names), len(set(names)), names)

    def test_names_are_sanitized_before_uniqueness_check(self):
        config = normalize_config({"inputs": [
            {"path": self.shp, "field": "rank", "name": "a b"},
            {"path": self.shp, "field": "rank", "name": "a_b"},
        ]})
        names = [item["name"] for item in config["inputs"]]
        self.assertEqual(len(set(names)), 2, names)

    def test_output_group_defaults_to_group(self):
        config = normalize_config({"inputs": [{"path": self.shp, "field": "rank", "group": "早川"}]})
        self.assertEqual(config["inputs"][0]["output_group"], "早川")

    def test_group_must_not_mix_normal_and_low_priority(self):
        with self.assertRaises(InputDataError):
            normalize_config({"inputs": [
                {"path": self.shp, "field": "rank", "group": "g"},
                {"path": self.shp, "field": "rank", "group": "g", "is_extra": True},
            ]})

    def test_group_must_use_one_field(self):
        with self.assertRaises(InputDataError):
            normalize_config({"inputs": [
                {"path": self.shp, "field": "rank", "group": "g"},
                {"path": self.shp, "field": "other", "group": "g"},
            ]})

    def test_missing_input_is_an_error(self):
        with self.assertRaises(InputDataError):
            normalize_config({"inputs": [{"path": str(self.tmp / "none.shp"), "field": "rank"}]})


class ValueTest(unittest.TestCase):
    def test_depth_template_boundaries(self):
        depths = [0, 0.01, 0.49, 0.5, 2.99, 3, 4.99, 5, 9.99, 10, 19.99, 20, 35]
        expected = [0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6]
        df = pd.DataFrame({"depth": depths})
        out, _, _ = validate_value_column(df, "test.shp", "depth", value_reclass={"template": "depth_m"})
        self.assertEqual(out["value"].tolist(), expected)

    def test_duration_template_boundaries(self):
        minutes = [0, 1, 719, 720, 1439, 1440, 4320, 10080, 20160, 40320]
        expected = [0, 1, 1, 2, 2, 3, 4, 5, 6, 7]
        df = pd.DataFrame({"time": minutes})
        out, _, _ = validate_value_column(df, "test.shp", "time", value_reclass={"template": "duration_min"})
        self.assertEqual(out["value"].tolist(), expected)

    def test_value_mapping(self):
        df = pd.DataFrame({"区分": ["0.5m未満", "0.5～3.0m", "3.0～5.0m"]})
        mapping = {"0.5m未満": 1, "0.5～3.0m": 2, "3.0～5.0m": 3}
        out, _, _ = validate_value_column(df, "test.shp", "区分", value_mapping=mapping)
        self.assertEqual(out["value"].tolist(), [1, 2, 3])

    def test_unmapped_value_is_an_error(self):
        df = pd.DataFrame({"区分": ["0.5m未満", "不明"]})
        with self.assertRaises(InputDataError):
            validate_value_column(df, "test.shp", "区分", value_mapping={"0.5m未満": 1})

    def test_non_integer_rank_is_an_error(self):
        df = pd.DataFrame({"rank": [1, 2.5]})
        with self.assertRaises(InputDataError):
            validate_value_column(df, "test.shp", "rank")

    def test_missing_field_is_an_error(self):
        with self.assertRaises(InputDataError):
            validate_value_column(pd.DataFrame({"rank": [1]}), "test.shp", "depth")

    def test_ksj_flood_rank_fields_are_read_by_default(self):
        # 国土数値情報 洪水浸水想定区域のランク属性は、フィールド指定なしで読み取られる
        for field, values in (("A31a_205", [1, 4]), ("A31a_105", [2, 3]), ("A31a_305", [1, 3]),
                              ("A31_205", ["1", "6"]), ("A31_105", ["2"]), ("A31_305", ["7"])):
            df = pd.DataFrame({field.replace("5", "1"): ["8303030555"] * len(values), field: values})
            out, source_field, _ = validate_value_column(df, "A31.shp")
            self.assertEqual(source_field, field)
            self.assertEqual(out["value"].tolist(), [int(v) for v in values])

    def test_ksj_non_rank_fields_are_not_read_by_default(self):
        # 家屋倒壊（種別コード）や平成24年度版（11～15の別体系）は標準では読まない
        for field in ("A31a_405", "A31_001"):
            with self.assertRaises(InputDataError, msg=field):
                validate_value_column(pd.DataFrame({field: [2]}), "A31.shp")

    def test_fixed_value(self):
        self.assertIsNone(normalize_fixed_value(""))
        self.assertEqual(normalize_fixed_value("3"), 3)
        with self.assertRaises(InputDataError):
            normalize_fixed_value("2.5")

    def test_output_field_name(self):
        self.assertEqual(validate_output_field_name("rank"), "rank")
        for bad in ("", "toolongfield", "SHAPE", "1rank", "浸水深"):
            with self.assertRaises(InputDataError, msg=bad):
                validate_output_field_name(bad)


class GeometryTest(unittest.TestCase):
    def test_polygon_parts_drops_lines_and_points(self):
        a, b = box(0, 0, 1, 1), box(2, 0, 3, 1)
        mixed = shapely.GeometryCollection([a, LineString([(1, 0), (2, 0)]), Point(5, 5), MultiPolygon([b])])
        result = polygon_parts([mixed])
        self.assertEqual(result.geom_type, "MultiPolygon")
        self.assertEqual(shapely.get_num_geometries(result), 2)
        self.assertAlmostEqual(result.area, 2.0)

    def test_polygon_parts_of_nothing_is_empty(self):
        self.assertTrue(polygon_parts([LineString([(0, 0), (1, 1)])]).is_empty)
        self.assertTrue(polygon_parts([]).is_empty)

    def test_merge_disjoint_polygons_joins_only_touching_parts(self):
        base = MultiPolygon([box(0, 0, 1, 1), box(5, 5, 6, 6)])
        addition = box(1, 0, 2, 1)  # 左の正方形に接する
        result = merge_disjoint_polygons(base, addition)
        self.assertTrue(result.is_valid)
        self.assertAlmostEqual(result.area, 3.0)
        self.assertEqual(shapely.get_num_geometries(result), 2)  # 接した2つは1つの面になる
        self.assertTrue(result.equals(shapely.union_all([base, addition])))
        self.assertTrue(merge_disjoint_polygons(None, addition).equals(addition))
        self.assertTrue(merge_disjoint_polygons(base, shapely.Polygon()).equals(base))

    def test_clip_bounds_validation(self):
        self.assertIsNone(normalize_clip_bounds(None))
        self.assertEqual(normalize_clip_bounds([1, 2, 3, 4]), (1.0, 2.0, 3.0, 4.0))
        with self.assertRaises(InputDataError):
            normalize_clip_bounds([1, 2, 3])
        with self.assertRaises(InputDataError):
            normalize_clip_bounds([3, 2, 1, 4])

    def test_clip_to_bounds(self):
        import geopandas as gpd

        gdf = gpd.GeoDataFrame({"value": [1]}, geometry=[box(139.0, 36.0, 139.1, 36.1)], crs=6668)
        clipped = clip_to_bounds(gdf, [139.05, 36.05, 139.2, 36.2])
        self.assertAlmostEqual(clipped.geometry.iloc[0].area, box(139.05, 36.05, 139.1, 36.1).area)


if __name__ == "__main__":
    unittest.main()
