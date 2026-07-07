import contextlib
import io
import os
import warnings
from pathlib import Path

import pandas as pd
import pyogrio

from .constants import FIELD_SAMPLE_SIZE
from .constants import RANK_EXISTENCE_SET
from .errors import InputDataError
from .utils import format_limited_values
from .validation import apply_value_reclass, normalize_value_mapping, normalize_value_reclass


os.environ.setdefault("CPL_LOG", os.devnull)
pyogrio.set_gdal_config_options({"CPL_LOG": os.environ["CPL_LOG"]})


@contextlib.contextmanager
def suppress_noisy_gdal_warnings():
    old_stderr_fd = os.dup(2)
    try:
        with (
            open(os.devnull, "w") as devnull,
            warnings.catch_warnings(),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            os.dup2(devnull.fileno(), 2)
            warnings.filterwarnings("ignore", category=RuntimeWarning)
            warnings.filterwarnings("ignore", message=".*Non closed ring detected.*")
            yield
    finally:
        os.dup2(old_stderr_fd, 2)
        os.close(old_stderr_fd)


def read_geofile(path, **kwargs):
    import geopandas as gpd

    with suppress_noisy_gdal_warnings():
        return gpd.read_file(path, **kwargs)


def write_geofile(gdf, **kwargs):
    with suppress_noisy_gdal_warnings():
        return gdf.to_file(**kwargs)


def read_shapefile(shp_path, max_features=None):
    with suppress_noisy_gdal_warnings():
        return pyogrio.read_dataframe(
            shp_path,
            encoding='shift-jis',
            max_features=max_features,
            on_invalid='fix',
        )
def load_yaml_config(config_path):
    try:
        import yaml
    except ImportError as e:
        raise InputDataError(
            '警告：YAML設定を使用するには PyYAML が必要です。requirements.txt からインストールしてください。'
        ) from e

    with Path(config_path).open('r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}
def save_yaml_config(config, config_path):
    try:
        import yaml
    except ImportError as e:
        raise InputDataError(
            '警告：YAML設定を保存するには PyYAML が必要です。requirements.txt からインストールしてください。'
        ) from e

    with Path(config_path).open('w', encoding='utf-8') as f:
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)
def get_attribute_fields(shp_path):
    with suppress_noisy_gdal_warnings():
        df = pyogrio.read_dataframe(
            shp_path,
            read_geometry=False,
            max_features=1,
            encoding='shift-jis',
        )
    return list(df.columns)
def get_attribute_field_values(shp_path, field_name, max_features=FIELD_SAMPLE_SIZE):
    with suppress_noisy_gdal_warnings():
        df = pyogrio.read_dataframe(
            shp_path,
            read_geometry=False,
            columns=[field_name],
            max_features=max_features,
            encoding='shift-jis',
        )
    return df[field_name]
def get_attribute_unique_text_values(shp_path, field_name, limit=200):
    values = get_attribute_field_values(shp_path, field_name, max_features=None)
    unique_values = sorted({
        str(value).strip()
        for value in values.dropna()
        if str(value).strip()
    })
    return unique_values[:limit], len(unique_values)
def get_field_conversion_status(shp_path, field_name, value_mapping=None, value_reclass=None):
    if not field_name:
        return {
            "needs_mapping": False,
            "has_unmapped_values": False,
            "has_caution": False,
            "message": "",
        }

    value_mapping = normalize_value_mapping(value_mapping)
    value_reclass = normalize_value_reclass(value_reclass)
    values = get_attribute_field_values(shp_path, field_name)
    if values.empty:
        return {
            "needs_mapping": False,
            "has_unmapped_values": False,
            "has_caution": False,
            "message": "",
        }

    if value_reclass:
        try:
            apply_value_reclass(values, value_reclass, shp_path, field_name)
        except InputDataError as e:
            return {
                "needs_mapping": True,
                "has_unmapped_values": True,
                "has_caution": False,
                "message": str(e),
            }
        return {
            "needs_mapping": False,
            "has_unmapped_values": False,
            "has_caution": False,
            "message": "",
        }

    numeric_values = pd.to_numeric(values, errors="coerce")
    numeric_mask = values.isna() | numeric_values.notna()
    if numeric_mask.all():
        integer_rank_mask = values.isna() | (numeric_values % 1 == 0)
        if not integer_rank_mask.all() and not value_mapping:
            invalid_values = sorted(set(values.loc[~integer_rank_mask].astype(str)))
            return {
                "needs_mapping": True,
                "has_unmapped_values": True,
                "has_caution": False,
                "message": (
                    "整数rankではない数値を含みます。"
                    f"変換表を設定してください: {format_limited_values(invalid_values)}"
                ),
            }

        if value_mapping:
            text_values = values.astype("string").str.strip()
            unmapped_values = sorted(set(text_values.loc[~values.isna() & ~text_values.isin(value_mapping)].astype(str)))
            if unmapped_values:
                return {
                    "needs_mapping": True,
                    "has_unmapped_values": True,
                    "has_caution": False,
                    "message": f"変換表に未登録の値があります: {format_limited_values(unmapped_values)}",
                }
            return {
                "needs_mapping": False,
                "has_unmapped_values": False,
                "has_caution": False,
                "message": "",
            }

        non_null_text = values.dropna().astype(str).str.strip()
        has_decimal_notation = non_null_text.str.contains(r'\.\d+', regex=True).any()
        out_of_rank_values = sorted({
            int(value)
            for value in numeric_values.dropna()
            if value % 1 == 0 and int(value) not in RANK_EXISTENCE_SET
        })
        if has_decimal_notation or out_of_rank_values:
            message_parts = [
                "数値フィールドですが、rankではなく浸水深(m)等の値である可能性があります。"
            ]
            if has_decimal_notation:
                message_parts.append("例: 5.0 は rank5 ではなく 5.0m を意味する場合があります。")
            if out_of_rank_values:
                message_parts.append(
                    f"1～7以外の値があります: {format_limited_values(out_of_rank_values)}"
                )
            message_parts.append("必要に応じて変換表を設定してください。")
            return {
                "needs_mapping": False,
                "has_unmapped_values": False,
                "has_caution": True,
                "message": "".join(message_parts),
            }

        return {
            "needs_mapping": False,
            "has_unmapped_values": False,
            "has_caution": False,
            "message": "",
        }

    text_values = values.astype("string").str.strip()
    if not value_mapping:
        return {
            "needs_mapping": True,
            "has_unmapped_values": True,
            "has_caution": False,
            "message": "文字列フィールドです。変換表を設定してください。",
        }

    unmapped_values = sorted(set(text_values.loc[~values.isna() & ~text_values.isin(value_mapping)].astype(str)))
    if unmapped_values:
        return {
            "needs_mapping": True,
            "has_unmapped_values": True,
            "has_caution": False,
            "message": f"変換表に未登録の値があります: {format_limited_values(unmapped_values)}",
        }

    return {
        "needs_mapping": False,
        "has_unmapped_values": False,
        "has_caution": False,
        "message": "",
    }
