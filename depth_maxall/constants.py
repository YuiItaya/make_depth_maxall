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
    # 国土数値情報 洪水浸水想定区域（A31a: 令和5年度以降の形式、A31: 令和2・3年度版）。
    # 計画規模の浸水深(10)・想定最大規模の浸水深(20)・浸水継続時間(30)のランクで、値は本ツールのrankと同じ体系。
    # 家屋倒壊等氾濫想定区域(A31a_405 等)はランクではなく種別コードのため含めない。
    # 平成24年度版(A31-12)の A31_001 は 11～15 の別体系なので含めない（変換表で読み替える）。
    "A31a_205",
    "A31a_105",
    "A31a_305",
    "A31_205",
    "A31_105",
    "A31_305",
)

VALUE_RECLASS_TEMPLATES = {
    "depth_m": {
        "label": "浸水深(m): 0, 0.5, 3, 5, 10, 20",
        "description": "0=0、0<x<0.5=1、0.5以上3未満=2、3以上5未満=3、5以上10未満=4、10以上20未満=5、20以上=6",
    },
    "duration_min": {
        "label": "浸水継続時間(分): 12h, 24h, 3日, 7日, 14日, 28日",
        "description": "0=0、0<x<720=1、720以上1440未満=2、1440以上4320未満=3、4320以上10080未満=4、10080以上20160未満=5、20160以上40320未満=6、40320以上=7",
    },
    "duration_hour": {
        "label": "浸水継続時間(時間): 12h, 24h, 3日, 7日, 14日, 28日",
        "description": "0=0、0<x<12=1、12以上24未満=2、24以上72未満=3、72以上168未満=4、168以上336未満=5、336以上672未満=6、672以上=7",
    },
    "duration_sec": {
        "label": "浸水継続時間(秒): 12h, 24h, 3日, 7日, 14日, 28日",
        "description": "0=0、0<x<43200=1、43200以上86400未満=2、86400以上259200未満=3、259200以上604800未満=4、604800以上1209600未満=5、1209600以上2419200未満=6、2419200以上=7",
    },
}
VALUE_RECLASS_TEMPLATE_CHOICES = [
    (template["label"], template_id)
    for template_id, template in VALUE_RECLASS_TEMPLATES.items()
]
VALUE_RECLASS_LABEL_TO_ID = dict(VALUE_RECLASS_TEMPLATE_CHOICES)
VALUE_RECLASS_ID_TO_LABEL = {
    template_id: template["label"]
    for template_id, template in VALUE_RECLASS_TEMPLATES.items()
}

INPUT_PATH = Path('./shp')
OUTPUT_PATH = Path('./output')
SPLIT_PATH = Path('./split')
RANK_PATH = Path('./rank')
EXTRA_PATH = INPUT_PATH / 'ex'
EXTRA_SPLIT_PATH = SPLIT_PATH / 'ex'
EXTRA_RANK_PATH = RANK_PATH / 'ex'
DEFAULT_OUTPUT_FILE = OUTPUT_PATH / 'output.shp'
