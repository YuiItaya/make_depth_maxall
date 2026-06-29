from pathlib import Path

JGD2011 = 6668
RANK_EXISTENCE = [1, 2, 3, 4, 5, 6, 7]
RANK_EXISTENCE_SET = set(RANK_EXISTENCE)
DEFAULT_OUTPUT_FIELD = "rank"
DEFAULT_OUTPUT_EPSG = 6668
DEFAULT_MAP_BOUNDS_6668 = (122.0, 20.0, 154.0, 46.0)
FIELD_SAMPLE_SIZE = 200
SHAPEFILE_FIELD_NAME_LIMIT = 10
SHAPEFILE_RESERVED_FIELD_NAMES = {
    "FID",
    "OID",
    "OBJECTID",
    "SHAPE",
    "GEOMETRY",
    "GEOM",
}

OUTPUT_CRS_CHOICES = [
    ("JGD2024 緯度経度（2D互換 EPSG:6668）", 6668),
    *[
        (f"JGD2024 平面直角座標系 {zone}系（2D互換 EPSG:{6668 + index}）", 6668 + index)
        for index, zone in enumerate(
            ("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
             "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX"),
            start=1,
        )
    ],
]
OUTPUT_CRS_LABEL_TO_EPSG = dict(OUTPUT_CRS_CHOICES)
OUTPUT_CRS_EPSG_TO_LABEL = {
    epsg: label
    for label, epsg in OUTPUT_CRS_CHOICES
}

# GUIで入力ファイルを追加したとき、この順番で読み取りフィールドの初期候補を探します。
# 利用データでよく使うフィールド名が増えた場合は、ここに追記してください。
DEFAULT_VALUE_FIELD_CANDIDATES = (
    "value",
    "rank",
    "VALUE",
    "RANK",
    "浸水深ランク",
    "浸水深ﾗﾝｸ",
    "浸水時間ﾗﾝ",
    "浸水深",
    "浸水継続時間",
    "継続時間",
    "depth",
    "duration",
)

INPUT_PATH = Path('./shp')
OUTPUT_PATH = Path('./output')
SPLIT_PATH = Path('./split')
RANK_PATH = Path('./rank')
EXTRA_PATH = INPUT_PATH / 'ex'
EXTRA_SPLIT_PATH = SPLIT_PATH / 'ex'
EXTRA_RANK_PATH = RANK_PATH / 'ex'
DEFAULT_OUTPUT_FILE = OUTPUT_PATH / 'output.shp'
