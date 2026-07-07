import re

import pandas as pd

from .constants import (
    DEFAULT_VALUE_FIELD_CANDIDATES,
    SHAPEFILE_FIELD_NAME_LIMIT,
    SHAPEFILE_RESERVED_FIELD_NAMES,
    VALUE_RECLASS_ID_TO_LABEL,
    VALUE_RECLASS_TEMPLATES,
)
from .errors import InputDataError
from .utils import format_limited_values

def validate_output_field_name(field_name):
    field_name = str(field_name or "").strip()
    upper_field_name = field_name.upper()

    if not field_name:
        raise InputDataError('警告：出力フィールド名を指定してください。')

    if len(field_name) > SHAPEFILE_FIELD_NAME_LIMIT:
        raise InputDataError(
            f'警告：Shapefileの出力フィールド名は'
            f'{SHAPEFILE_FIELD_NAME_LIMIT}文字以内にしてください（{field_name}）。'
        )

    if upper_field_name in SHAPEFILE_RESERVED_FIELD_NAMES or upper_field_name.startswith("SHAPE_"):
        raise InputDataError(
            f'警告：{field_name} はShapefileで予約済み又は予約扱いのフィールド名です。'
        )

    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', field_name):
        raise InputDataError(
            '警告：出力フィールド名は半角英字又はアンダースコアで始まる、'
            '半角英数字とアンダースコアのみの名前にしてください。'
        )

    return field_name
def get_field_name(depth_gpd, depth_shp, field_name=None):
    if field_name:
        if field_name not in depth_gpd.columns:
            raise InputDataError(
                f"警告：{depth_shp} に指定フィールド {field_name} が存在しないため、処理を終了します。"
            )
        return field_name

    for candidate in DEFAULT_VALUE_FIELD_CANDIDATES:
        if candidate in depth_gpd.columns:
            return candidate

    raise InputDataError(
        f"警告：{depth_shp} に読み取り対象フィールドが見つからないため、処理を終了します。"
    )
def normalize_value_mapping(value_mapping):
    if not value_mapping:
        return None

    if not isinstance(value_mapping, dict):
        raise InputDataError('警告：value_mapping は {元の値: rank整数} の形式で指定してください。')

    normalized = {}
    for raw_key, raw_value in value_mapping.items():
        key = str(raw_key).strip()
        if not key:
            raise InputDataError('警告：value_mapping に空の変換元値があります。')

        numeric_value = pd.to_numeric(pd.Series([raw_value]), errors="coerce").iloc[0]
        if pd.isna(numeric_value) or numeric_value % 1 != 0:
            raise InputDataError(
                f'警告：value_mapping の {key} に対応する値 {raw_value} は整数ではありません。'
            )
        normalized[key] = int(numeric_value)

    return normalized
def get_mapping_signature(value_mapping):
    if not value_mapping:
        return None
    return tuple(sorted(normalize_value_mapping(value_mapping).items()))


def normalize_value_reclass(value_reclass):
    if not value_reclass:
        return None

    if isinstance(value_reclass, str):
        template_id = value_reclass
    elif isinstance(value_reclass, dict):
        template_id = value_reclass.get("template") or value_reclass.get("id")
    else:
        raise InputDataError(
            '警告：value_reclass は {template: テンプレートID} の形式で指定してください。'
        )

    template_id = str(template_id or "").strip()
    if template_id not in VALUE_RECLASS_TEMPLATES:
        raise InputDataError(
            f'警告：value_reclass のテンプレート {template_id} は登録されていません。'
        )

    return {"template": template_id}


def get_reclass_signature(value_reclass):
    normalized = normalize_value_reclass(value_reclass)
    if not normalized:
        return None
    return normalized["template"]


def apply_value_reclass(values, value_reclass, depth_shp, source_field):
    value_reclass = normalize_value_reclass(value_reclass)
    if not value_reclass:
        return None

    template_id = value_reclass["template"]
    numeric_values = pd.to_numeric(values, errors="coerce")
    invalid_mask = values.isna() | numeric_values.isna()
    if invalid_mask.any():
        invalid_values = sorted(set(values.loc[invalid_mask].fillna("<NULL>").astype(str)))
        raise InputDataError(
            f"警告：{depth_shp} の {source_field} にテンプレート変換できない値があります"
            f"（{format_limited_values(invalid_values)}）。処理を終了します。"
        )

    def classify_depth(value):
        if value == 0:
            return 0
        if value < 0.5:
            return 1
        if value < 3:
            return 2
        if value < 5:
            return 3
        if value < 10:
            return 4
        if value < 20:
            return 5
        if value >= 20:
            return 6
        return 99

    def classify_duration(value):
        if value == 0:
            return 0
        if 0 < value < 720:
            return 1
        if 720 <= value < 1440:
            return 2
        if 1440 <= value < 4320:
            return 3
        if 4320 <= value < 10080:
            return 4
        if 10080 <= value < 20160:
            return 5
        if 20160 <= value < 40320:
            return 6
        if value >= 40320:
            return 7
        return 99

    def classify_duration_hour(value):
        if value == 0:
            return 0
        if 0 < value < 12:
            return 1
        if 12 <= value < 24:
            return 2
        if 24 <= value < 72:
            return 3
        if 72 <= value < 168:
            return 4
        if 168 <= value < 336:
            return 5
        if 336 <= value < 672:
            return 6
        if value >= 672:
            return 7
        return 99

    def classify_duration_sec(value):
        if value == 0:
            return 0
        if 0 < value < 43200:
            return 1
        if 43200 <= value < 86400:
            return 2
        if 86400 <= value < 259200:
            return 3
        if 259200 <= value < 604800:
            return 4
        if 604800 <= value < 1209600:
            return 5
        if 1209600 <= value < 2419200:
            return 6
        if value >= 2419200:
            return 7
        return 99

    if template_id == "depth_m":
        return numeric_values.map(classify_depth).astype(int)
    if template_id == "duration_min":
        return numeric_values.map(classify_duration).astype(int)
    if template_id == "duration_hour":
        return numeric_values.map(classify_duration_hour).astype(int)
    if template_id == "duration_sec":
        return numeric_values.map(classify_duration_sec).astype(int)

    raise InputDataError(
        f'警告：value_reclass のテンプレート {template_id} は処理に対応していません。'
    )


def normalize_fixed_value(fixed_value):
    if fixed_value is None or str(fixed_value).strip() == "":
        return None

    numeric_value = pd.to_numeric(pd.Series([fixed_value]), errors="coerce").iloc[0]
    if pd.isna(numeric_value) or numeric_value % 1 != 0:
        raise InputDataError(
            f'警告：固定rank {fixed_value} は整数ではありません。'
        )

    return int(numeric_value)


def validate_value_column(depth_gpd, depth_shp, field_name=None, value_mapping=None, value_reclass=None):
    source_field = get_field_name(depth_gpd, depth_shp, field_name)
    if source_field != "value":
        depth_gpd = depth_gpd.copy()
        depth_gpd["value"] = depth_gpd[source_field]

    value_reclass = normalize_value_reclass(value_reclass)
    if value_reclass:
        depth_gpd["value"] = apply_value_reclass(
            depth_gpd["value"],
            value_reclass,
            depth_shp,
            source_field,
        )
        duplicate_counts = depth_gpd["value"].value_counts()
        duplicate_values = sorted(duplicate_counts[duplicate_counts > 1].index.astype(int).tolist())
        source_label = f"{source_field} / {VALUE_RECLASS_ID_TO_LABEL[value_reclass['template']]}"
        return depth_gpd, source_label, duplicate_values

    value_mapping = normalize_value_mapping(value_mapping)
    if value_mapping:
        original_value = depth_gpd["value"]
        value_text = original_value.astype("string").str.strip()
        invalid_mask = original_value.isna() | ~value_text.isin(value_mapping)
        if invalid_mask.any():
            invalid_values = sorted(set(value_text.loc[invalid_mask].fillna("<NULL>").astype(str)))
            raise InputDataError(
                f"警告：{depth_shp} の {source_field} に変換表へ未登録の値があります"
                f"（{format_limited_values(invalid_values)}）。処理を終了します。"
            )

        depth_gpd["value"] = value_text.map(value_mapping).astype(int)
        duplicate_counts = depth_gpd["value"].value_counts()
        duplicate_values = sorted(duplicate_counts[duplicate_counts > 1].index.astype(int).tolist())
        return depth_gpd, source_field, duplicate_values

    numeric_value = pd.to_numeric(depth_gpd["value"], errors="coerce")
    invalid_mask = numeric_value.isna() | (numeric_value % 1 != 0)
    if invalid_mask.any():
        invalid_values = sorted(set(depth_gpd.loc[invalid_mask, "value"].astype(str)))
        raise InputDataError(
            f"警告：{depth_shp} の value/rank に整数化できない値があります"
            f"（{format_limited_values(invalid_values)}）。"
            "文字列の浸水深区分を使用する場合は変換表を設定してください。処理を終了します。"
        )

    depth_gpd["value"] = numeric_value.astype(int)
    duplicate_counts = depth_gpd["value"].value_counts()
    duplicate_values = sorted(duplicate_counts[duplicate_counts > 1].index.astype(int).tolist())
    return depth_gpd, source_field, duplicate_values
def validate_crs(depth_gpd, depth_shp):
    if depth_gpd.crs is None:
        raise InputDataError(
            f"警告：{depth_shp} のCRSが未設定のため、処理を終了します。"
        )
def repair_invalid_geometries(depth_gpd, depth_shp):
    invalid_mask = ~depth_gpd.geometry.is_valid
    invalid_count = int(invalid_mask.sum())

    if invalid_count == 0:
        return depth_gpd, invalid_count

    depth_gpd = depth_gpd.copy()
    depth_gpd["geometry"] = depth_gpd.geometry.make_valid()

    still_invalid_count = int((~depth_gpd.geometry.is_valid).sum())
    if still_invalid_count:
        raise InputDataError(
            f"警告：{depth_shp} に修復できない不正なジオメトリが"
            f"{still_invalid_count}件あるため、処理を終了します。"
        )

    return depth_gpd, invalid_count
