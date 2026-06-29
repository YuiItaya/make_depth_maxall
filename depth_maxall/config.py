from pathlib import Path

from .constants import (
    DEFAULT_OUTPUT_EPSG,
    DEFAULT_OUTPUT_FIELD,
    DEFAULT_OUTPUT_FILE,
    EXTRA_PATH,
    INPUT_PATH,
)
from .errors import InputDataError
from .utils import sanitize_filename
from .validation import (
    get_mapping_signature,
    normalize_fixed_value,
    normalize_value_mapping,
    validate_output_field_name,
)

def make_input_item(path, field=None, is_extra=False, name=None):
    name = name or Path(path).stem
    return {
        "path": str(Path(path)),
        "field": field,
        "is_extra": bool(is_extra),
        "name": name,
        "group": name,
    }
def build_legacy_input_items():
    input_items = [
        make_input_item(depth_shp)
        for depth_shp in INPUT_PATH.glob('*.shp')
    ]

    if EXTRA_PATH.exists():
        input_items.extend(
            make_input_item(depth_shp, is_extra=True)
            for depth_shp in EXTRA_PATH.glob('*.shp')
        )

    return input_items
def normalize_config(config, require_existing_paths=True):
    config = dict(config or {})
    processing = dict(config.get("processing") or {})
    inputs = [dict(item) for item in config.get("inputs") or []]
    output_field = validate_output_field_name(
        processing.get("output_field") or DEFAULT_OUTPUT_FIELD
    )

    if not inputs:
        raise InputDataError(
            '警告：設定ファイルに inputs が存在しないため、処理を終了します。'
        )

    normalized_inputs = []
    used_names = set()
    configured_group_fields = {}
    configured_group_extra = {}
    configured_group_mappings = {}
    for index, item in enumerate(inputs, 1):
        path_value = item.get("path")
        if not path_value:
            raise InputDataError(
                f'警告：inputs[{index}] の path が未設定のため、処理を終了します。'
            )
        path = Path(path_value)
        missing_path = not path.exists()
        if missing_path and require_existing_paths:
            raise InputDataError(
                f'警告：入力ファイル {path} が存在しないため、処理を終了します。'
            )

        name = sanitize_filename(item.get("name") or path.stem)
        if name in used_names:
            name = sanitize_filename(f"{name}_{index:03d}")
        used_names.add(name)

        group = sanitize_filename(item.get("group") or item.get("river") or name)
        field = item.get("field") or item.get("value_field")
        is_extra = bool(item.get("is_extra", False))
        fixed_value = normalize_fixed_value(
            item.get("fixed_value", item.get("fixed_rank"))
        )
        value_mapping = normalize_value_mapping(
            item.get("value_mapping", item.get("mapping"))
        )
        if field and fixed_value is None:
            existing_field = configured_group_fields.get(group)
            if existing_field and existing_field != field:
                raise InputDataError(
                    f'警告：グループ {group} 内で読み取りフィールドが一致していません'
                    f'（{existing_field}, {field}）。'
                )
            configured_group_fields[group] = field

        existing_extra = configured_group_extra.get(group)
        if existing_extra is not None and existing_extra != is_extra:
            raise InputDataError(
                f'警告：グループ {group} 内で通常/低優先の区分が一致していません。'
            )
        configured_group_extra[group] = is_extra

        if value_mapping:
            existing_mapping = configured_group_mappings.get(group)
            if existing_mapping and get_mapping_signature(existing_mapping) != get_mapping_signature(value_mapping):
                raise InputDataError(
                    f'警告：グループ {group} 内で変換表の内容が一致していません。'
                )
            configured_group_mappings[group] = value_mapping

        normalized_item = {
            "path": str(path),
            "field": field,
            "is_extra": is_extra,
            "name": name,
            "group": group,
        }
        if missing_path:
            normalized_item["missing_path"] = True
        if fixed_value is not None:
            normalized_item["fixed_value"] = fixed_value
        if value_mapping:
            normalized_item["value_mapping"] = value_mapping
        normalized_inputs.append(normalized_item)

    for item in normalized_inputs:
        group_mapping = configured_group_mappings.get(item["group"])
        if group_mapping:
            item["value_mapping"] = dict(group_mapping)

    return {
        "version": int(config.get("version", 1)),
        "processing": {
            "dissolve_input_by_value": bool(processing.get("dissolve_input_by_value", False)),
            "clip_bounds": processing.get("clip_bounds"),
            "output_path": str(processing.get("output_path") or DEFAULT_OUTPUT_FILE),
            "output_field": output_field,
            "output_epsg": int(processing.get("output_epsg") or DEFAULT_OUTPUT_EPSG),
            "keep_intermediate_files": bool(processing.get("keep_intermediate_files", False)),
        },
        "inputs": normalized_inputs,
    }
def build_legacy_config():
    return {
        "version": 1,
        "processing": {
            "dissolve_input_by_value": False,
            "clip_bounds": None,
            "output_path": str(DEFAULT_OUTPUT_FILE),
            "output_field": DEFAULT_OUTPUT_FIELD,
            "output_epsg": DEFAULT_OUTPUT_EPSG,
            "keep_intermediate_files": False,
        },
        "inputs": build_legacy_input_items(),
    }
