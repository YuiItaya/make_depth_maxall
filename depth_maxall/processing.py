import shutil
import sys
import time
from contextlib import contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely

from .config import build_legacy_config, normalize_config
from .constants import (
    DEFAULT_OUTPUT_EPSG,
    DEFAULT_OUTPUT_FIELD,
    DEFAULT_OUTPUT_FILE,
    EXTRA_PATH,
    EXTRA_RANK_PATH,
    EXTRA_SPLIT_PATH,
    INPUT_PATH,
    JGD2011,
    RANK_EXISTENCE_SET,
    RANK_PATH,
    SPLIT_PATH,
)
from .errors import InputDataError
from .geospatial import clip_to_bounds, dissolve_input_values, repair_output_geometries
from .io_utils import read_geofile, read_shapefile, read_shapefile_attributes, write_geofile
from .utils import create_directory, format_elapsed_time, format_values, sanitize_filename
from .validation import (
    normalize_fixed_value,
    repair_invalid_geometries,
    validate_crs,
    validate_value_column,
)

@contextmanager
def stage_timer(label):
    """工程の所要時間をログに出す（高速化の効果測定用）。"""
    started = time.perf_counter()
    try:
        yield
    finally:
        print(f'    [所要時間] {label}: {time.perf_counter() - started:.1f}秒', flush=True)


def process_depth_shp(
    depth_shp,
    field_name=None,
    is_extra=False,
    input_name=None,
    clip_bounds=None,
    dissolve_input_by_value=False,
    value_mapping=None,
    value_reclass=None,
    fixed_value=None,
    split_path=SPLIT_PATH,
    extra_split_path=EXTRA_SPLIT_PATH,
):
    split_path = Path(split_path)
    extra_split_path = Path(extra_split_path)
    depth_gpd = read_shapefile(depth_shp)

    fixed_value = normalize_fixed_value(fixed_value)
    if fixed_value is None:
        depth_gpd, source_field, duplicate_values = validate_value_column(
            depth_gpd,
            depth_shp,
            field_name,
            value_mapping=value_mapping,
            value_reclass=value_reclass,
        )
    else:
        depth_gpd = depth_gpd.copy()
        depth_gpd["value"] = fixed_value
        source_field = f"固定rank:{fixed_value}"
        duplicate_values = []

    validate_crs(depth_gpd, depth_shp)
    # Z値付きの入力（県その他河川など）が混ざると出力にもZが残るため、2次元に揃える
    depth_gpd = depth_gpd.set_geometry(shapely.force_2d(depth_gpd.geometry.values), crs=depth_gpd.crs)
    depth_gpd, repaired_geometry_count = repair_invalid_geometries(depth_gpd, depth_shp)
    values = sorted(depth_gpd["value"].unique().tolist())
    depth_gpd = depth_gpd.loc[:, ["value", "geometry"]].copy().to_crs(epsg=JGD2011)
    depth_gpd = clip_to_bounds(depth_gpd, clip_bounds)

    if depth_gpd.empty:
        return {
            "path": str(depth_shp),
            "field": source_field,
            "values": values,
            "duplicate_values": duplicate_values,
            "repaired_geometry_count": repaired_geometry_count,
            "empty_after_clip": True,
        }

    if dissolve_input_by_value:
        depth_gpd = dissolve_input_values(depth_gpd)

    depth_name = sanitize_filename(input_name or depth_shp.stem)
    split_path = extra_split_path if is_extra else split_path

    for value, group in depth_gpd.groupby('value'):
        write_geofile(group, filename=split_path / f'{depth_name}_{value}.gpkg', driver="GPKG", encoding="shift-jis")

    return {
        "path": str(depth_shp),
        "field": source_field,
        "values": values,
        "duplicate_values": duplicate_values,
        "repaired_geometry_count": repaired_geometry_count,
        "empty_after_clip": False,
    }
def print_final_warnings(process_reports, dissolve_input_by_value=False):
    input_values = sorted({
        value
        for report in process_reports
        for value in report["values"]
    })
    out_of_standard_values = [
        value for value in input_values
        if value not in RANK_EXISTENCE_SET
    ]

    if out_of_standard_values:
        print(
            '警告：1～7以外のvalue/rankが存在しました'
            f'（{format_values(out_of_standard_values)}）。'
        )
        processed_values = [value for value in out_of_standard_values if value != 0]
        if processed_values:
            print(
                '      value/rank=0以外は、該当するRankとして処理を継続しました。'
            )
        if 0 in out_of_standard_values:
            print('      value/rank=0は、最終出力対象から除外しました。')

    repaired_reports = [
        report for report in process_reports
        if report["repaired_geometry_count"] > 0
    ]
    if repaired_reports:
        print('警告：不正なジオメトリを検出し、修復して処理を継続しました。')
        for report in repaired_reports:
            print(
                f'      {report["path"]}: '
                f'{report["repaired_geometry_count"]}件'
            )

    if not dissolve_input_by_value:
        duplicate_reports = [
            report for report in process_reports
            if report["duplicate_values"]
        ]
        if duplicate_reports:
            print('警告：入力フィールド内に同じ整数値を持つ複数レコードがありました。')
            for report in duplicate_reports:
                print(
                    f'      {report["path"]} [{report["field"]}]: '
                    f'{format_values(report["duplicate_values"])}'
                )

    empty_reports = [
        report for report in process_reports
        if report.get("empty_after_clip")
    ]
    if empty_reports:
        print('警告：矩形範囲外のため処理対象から外れた入力ファイルがあります。')
        for report in empty_reports:
            print(f'      {report["path"]}')
def cleanup_intermediate_files(paths=None):
    for path in (paths or (SPLIT_PATH, RANK_PATH)):
        if path.exists():
            shutil.rmtree(path)


def remove_existing_output_file(output_path):
    output_path = Path(output_path)
    if output_path.suffix.lower() == ".shp":
        shapefile_suffixes = {
            ".shp",
            ".shx",
            ".dbf",
            ".prj",
            ".cpg",
            ".qix",
            ".fix",
            ".sbn",
            ".sbx",
            ".shp.xml",
        }
        for path in output_path.parent.glob(f"{output_path.stem}.*"):
            lower_name = path.name.lower()
            if path.suffix.lower() in shapefile_suffixes or lower_name.endswith(".shp.xml"):
                path.unlink()
    elif output_path.exists():
        output_path.unlink()
def get_rank_values(path: Path):
    return sorted({
        int(x.stem.rsplit('_', 1)[-1])
        for x in path.glob('*_*.gpkg')
        if int(x.stem.rsplit('_', 1)[-1]) != 0
    })
def process_shapefiles(
    input_items,
    clip_bounds=None,
    dissolve_input_by_value=False,
    split_path=SPLIT_PATH,
    extra_split_path=EXTRA_SPLIT_PATH,
):
    print('1/4_全シェープファイルをランク毎に分解中・・・')
    split_path = Path(split_path)
    extra_split_path = Path(extra_split_path)
    # 並列処理の実行
    reports = []
    total_items = len(input_items)
    progress_interval = max(1, total_items // 20)
    with ProcessPoolExecutor() as executor:
        futures = []

        for item in input_items:
            futures.append(executor.submit(
                process_depth_shp,
                Path(item["path"]),
                field_name=item.get("field"),
                is_extra=item.get("is_extra", False),
                input_name=item.get("name"),
                clip_bounds=clip_bounds,
                dissolve_input_by_value=dissolve_input_by_value,
                value_mapping=item.get("value_mapping"),
                value_reclass=item.get("value_reclass"),
                fixed_value=item.get("fixed_value"),
                split_path=split_path,
                extra_split_path=extra_split_path,
            ))

        # すべての並列タスクが完了するのを待つ
        for completed_count, future in enumerate(as_completed(futures), 1):
            reports.append(future.result())
            if (
                completed_count == 1
                or completed_count == total_items
                or completed_count % progress_interval == 0
            ):
                print(
                    f'    ランク分解進捗: {completed_count}/{total_items}件',
                    flush=True,
                )

    return reports
def process_ranked_data(
    EX,
    split_path=SPLIT_PATH,
    rank_path=RANK_PATH,
    extra_split_path=EXTRA_SPLIT_PATH,
    extra_rank_path=EXTRA_RANK_PATH,
):
    print('2/4_同一ランクを結合します。')
    split_path = Path(split_path)
    rank_path = Path(rank_path)
    extra_split_path = Path(extra_split_path)
    extra_rank_path = Path(extra_rank_path)
    RANK_set = get_rank_values(split_path)

    if not RANK_set:
        raise InputDataError(
            '警告：最終出力対象となるランクが存在しないため、処理を終了します。'
        )

    for value in RANK_set:
        print("    " + f"RANK{value}をディゾルブ中・・・")
        rank_gdfs = []
        for rank_x in split_path.glob(f'*_{value}.gpkg'):
            rank_x_gpd = read_geofile(rank_x, encoding='shift-jis')
            rank_gdfs.append(rank_x_gpd)

        RANKX_gpd = gpd.GeoDataFrame(pd.concat(rank_gdfs, ignore_index=True), crs=rank_gdfs[0].crs)
        RANKX_gpd["value"] = RANKX_gpd["value"].astype(int)
        RANKX_gpd = RANKX_gpd.dissolve()
        gpkg_file = rank_path / f'Rank_{value}.gpkg'
        write_geofile(RANKX_gpd, filename=gpkg_file, driver="GPKG", encoding="shift-jis")

    EX_RANK_set = []
    if EX:
        EX_RANK_set = process_extra_files(extra_split_path, extra_rank_path)

    return RANK_set, EX_RANK_set
def process_extra_files(extra_split_path=EXTRA_SPLIT_PATH, extra_rank_path=EXTRA_RANK_PATH):
    print("    " + '低優先ファイルを処理します。')
    extra_split_path = Path(extra_split_path)
    extra_rank_path = Path(extra_rank_path)
    EX_RANK_set = get_rank_values(extra_split_path)

    if not EX_RANK_set:
        print("    " + '低優先ファイルに最終出力対象ランクは存在しませんでした。')
        return EX_RANK_set

    for value in EX_RANK_set:
        print("    " + f"RANK{value}をディゾルブ中・・・")
        rank_gdfs = []
        for rank_x in extra_split_path.glob(f'*_{value}.gpkg'):
            rank_x_gpd = read_geofile(rank_x, encoding='shift-jis')
            rank_gdfs.append(rank_x_gpd)

        RANKX_gpd = gpd.GeoDataFrame(pd.concat(rank_gdfs, ignore_index=True), crs=rank_gdfs[0].crs)
        RANKX_gpd["value"] = RANKX_gpd["value"].astype(int)
        RANKX_gpd = RANKX_gpd.dissolve()
        gpkg_file = extra_rank_path / f'Rank_{value}.gpkg'
        write_geofile(RANKX_gpd, filename=gpkg_file, driver="GPKG", encoding="shift-jis")

    return EX_RANK_set
def generate_final_output(
    EX,
    RANK_set,
    output_path=None,
    output_field=None,
    output_epsg=None,
    rank_path=RANK_PATH,
    extra_rank_path=EXTRA_RANK_PATH,
):
    print('3/4_ランク間の重なりを判定し、重複する低ランクを削除します。')
    rank_path = Path(rank_path)
    extra_rank_path = Path(extra_rank_path)
    RANK_higher_gpd_copy = None
    dissolve_gpd = None
    output_gdfs = []

    for count, value in enumerate(reversed(RANK_set)):
        if count == 0:
            print("    " + f"RANK{value}をコピー中・・・")
            RANK_higher_gpd = read_geofile(rank_path / f'Rank_{value}.gpkg', encoding='shift-jis')
            output_gdfs.append(RANK_higher_gpd)
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop=True)
            dissolve_gpd['value'] = int(99)
        else:
            print("    " + f"RANK{value}をユニオン中・・・")
            RANKX_gpd = read_geofile(rank_path / f'Rank_{value}.gpkg', encoding='shift-jis').rename(columns={"value": f"value_{value}"})
            RANK_higher_gpd = gpd.overlay(dissolve_gpd, RANKX_gpd, how='union').fillna(0)
            RANK_higher_gpd["value"] = RANK_higher_gpd["value"].astype(int)
            RANK_higher_gpd.loc[RANK_higher_gpd['value'] == 0, 'value'] = int(f'{value}')
            del RANK_higher_gpd[f'value_{value}']
            RANK_higher_gpd = RANK_higher_gpd.dissolve(by='value').reset_index()
            dissolve_gpd = RANK_higher_gpd.dissolve().reset_index(drop=True)
            dissolve_gpd['value'] = int(99)
            RANK_higher_gpd = RANK_higher_gpd.query('not value == 99')
            output_gdfs.append(RANK_higher_gpd)

    if output_gdfs:
        RANK_higher_gpd_copy = gpd.GeoDataFrame(
            pd.concat(output_gdfs, ignore_index=True),
            crs=output_gdfs[0].crs,
        )

    # 低優先ファイルが存在した場合の追加処理
    if EX:
        print("    " + '低優先ファイルを処理します。')
        ALL_EX_RANK = [read_geofile(x, encoding='shift-jis') for x in extra_rank_path.glob('Rank_*.gpkg')]

        if not ALL_EX_RANK:
            print("    " + '出力対象となる低優先ファイルはありませんでした。')
        else:
            dissolve_gpd = RANK_higher_gpd_copy.dissolve().reset_index(drop=True)
            dissolve_gpd['value'] = int(99)

            EX_GPD = gpd.GeoDataFrame(pd.concat(ALL_EX_RANK, ignore_index=True), crs=ALL_EX_RANK[0].crs).reset_index(drop=True)
            EX_GPD["value"] = EX_GPD["value"].astype(int)
            EX_GPD_dis = EX_GPD.dissolve().reset_index(drop=True)
            EX_GPD_dis = EX_GPD_dis.rename(columns={'value': 'ex_value'})
            EX_GPD_dis['ex_value'] = int(98)
            EX_GPD_dis = gpd.overlay(dissolve_gpd, EX_GPD_dis, how='union').fillna(0)
            EX_GPD_dis = EX_GPD_dis.query('not value == 99').reset_index(drop=True)
            del EX_GPD_dis['value']

            EX_GPD = gpd.overlay(EX_GPD_dis, EX_GPD, how='union').fillna(0)
            EX_GPD["value"] = EX_GPD["value"].astype(int)
            EX_GPD = EX_GPD.query('not ex_value == 0').reset_index(drop=True)
            # 誤差により生成される可能性のあるvalue=0を削除
            EX_GPD = EX_GPD.query('not value == 0').reset_index(drop=True)

            RANK_higher_gpd_copy = gpd.GeoDataFrame(
                pd.concat([RANK_higher_gpd_copy, EX_GPD], ignore_index=True),
                crs=RANK_higher_gpd_copy.crs,
            )
            RANK_higher_gpd_copy = RANK_higher_gpd_copy.dissolve(by='value').reset_index()

            if 'ex_value' in RANK_higher_gpd_copy.columns:
                del RANK_higher_gpd_copy['ex_value']

    print('4/4_シェープファイル出力中・・・')
    output_path = Path(output_path or DEFAULT_OUTPUT_FILE)
    output_field = output_field or DEFAULT_OUTPUT_FIELD
    output_epsg = int(output_epsg or DEFAULT_OUTPUT_EPSG)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    remove_existing_output_file(output_path)

    RANK_higher_gpd_copy["value"] = RANK_higher_gpd_copy["value"].astype(int)
    RANK_higher_gpd_copy = RANK_higher_gpd_copy.to_crs(epsg=output_epsg)
    if output_field != "value":
        RANK_higher_gpd_copy = RANK_higher_gpd_copy.rename(columns={"value": output_field})

    with stage_timer('4 シェープファイル出力'):
        write_output_shapefile(RANK_higher_gpd_copy, output_path)
def write_output_shapefile(output_gdf, output_path):
    # 座標変換・ユニオンで生じた不正ジオメトリを修復してから書き出す
    output_gdf, repaired_count = repair_output_geometries(output_gdf)
    write_geofile(output_gdf, filename=output_path, driver="ESRI Shapefile", encoding="utf-8")

    # Shapefileの座標精度への丸めで再び不正になる場合があるため、読み戻して確認する
    written_gdf = read_geofile(output_path, encoding="utf-8")
    written_gdf, rewrite_count = repair_output_geometries(written_gdf)
    if rewrite_count:
        write_geofile(written_gdf, filename=output_path, driver="ESRI Shapefile", encoding="utf-8")
        written_gdf = read_geofile(output_path, encoding="utf-8")

    if repaired_count or rewrite_count:
        print("    " + f'出力ジオメトリを修復しました（{max(repaired_count, rewrite_count)}件）。')
    still_invalid_count = int((~written_gdf.geometry.is_valid).sum())
    if still_invalid_count:
        print("    " + f'警告：{output_path} に修復できない不正なジオメトリが{still_invalid_count}件残っています。')
def get_group_output_directory(output_path):
    output_path = Path(output_path or DEFAULT_OUTPUT_FILE)
    return output_path.parent / f"{output_path.stem}_by_group"
def get_group_output_path(output_path, group):
    return get_group_output_directory(output_path) / f"{sanitize_filename(group)}.shp"
def iter_output_groups(input_items):
    groups = {}
    for item in input_items:
        group = (
            item.get("output_group")
            or item.get("group")
            or item.get("name")
            or Path(item["path"]).stem
        )
        groups.setdefault(group, []).append(item)
    return groups.items()
def run_processing_pass(
    input_items,
    processing,
    output_path,
    split_path,
    rank_path,
    extra_split_path,
    extra_rank_path,
):
    has_extra = any(item.get("is_extra") for item in input_items)
    create_directory(split_path, clean=True)
    create_directory(rank_path, clean=True)

    if has_extra:
        print('低優先ファイルの存在を確認しました。')
        create_directory(extra_split_path, clean=True)
        create_directory(extra_rank_path, clean=True)

    with stage_timer('1 ランク分解'):
        process_reports = process_shapefiles(
            input_items,
            clip_bounds=processing.get("clip_bounds"),
            dissolve_input_by_value=processing.get("dissolve_input_by_value", False),
            split_path=split_path,
            extra_split_path=extra_split_path,
        )
    with stage_timer('2 同一ランク結合'):
        RANK_set, _ = process_ranked_data(
            has_extra,
            split_path=split_path,
            rank_path=rank_path,
            extra_split_path=extra_split_path,
            extra_rank_path=extra_rank_path,
        )
    with stage_timer('3-4 重なり削除・出力'):
        generate_final_output(
            has_extra,
            RANK_set,
            output_path=output_path,
            output_field=processing.get("output_field"),
            output_epsg=processing.get("output_epsg"),
            rank_path=rank_path,
            extra_rank_path=extra_rank_path,
        )
    return process_reports
def generate_group_outputs(input_items, processing):
    group_output_dir = get_group_output_directory(processing.get("output_path"))
    if group_output_dir.exists():
        shutil.rmtree(group_output_dir)
    group_output_dir.mkdir(parents=True, exist_ok=True)
    print(f'グループ別出力を作成します: {group_output_dir}')

    for group, group_items in iter_output_groups(input_items):
        group_name = sanitize_filename(group)
        print(f'グループ別出力: {group_name}')
        group_split_path = SPLIT_PATH / "_groups" / group_name / "split"
        group_rank_path = RANK_PATH / "_groups" / group_name / "rank"
        group_extra_split_path = group_split_path / "low_priority"
        group_extra_rank_path = group_rank_path / "low_priority"

        try:
            with stage_timer(f'河川別出力 {group_name}'):
                run_processing_pass(
                    group_items,
                    processing,
                    get_group_output_path(processing.get("output_path"), group_name),
                    group_split_path,
                    group_rank_path,
                    group_extra_split_path,
                    group_extra_rank_path,
                )
        except InputDataError as e:
            print(f'警告：グループ {group_name} の出力をスキップしました。{e}')
        finally:
            if not processing.get("keep_intermediate_files", False):
                cleanup_intermediate_files((group_split_path, group_rank_path))
def validate_inputs_before_cleanup(config):
    print('0/4_入力ファイルを検証中・・・', flush=True)
    resolved_group_fields = {}
    input_items = config["inputs"]
    total_items = len(input_items)
    progress_interval = max(1, total_items // 20)
    for index, item in enumerate(input_items, 1):
        depth_shp = Path(item["path"])
        if index == 1 or index == total_items or index % progress_interval == 0:
            print(
                f'    入力検証進捗: {index}/{total_items}件 ({depth_shp.name})',
                flush=True,
            )
        # 検証は列・値・CRSだけを見るので、形状は読まない
        attributes, crs = read_shapefile_attributes(depth_shp)
        fixed_value = normalize_fixed_value(item.get("fixed_value"))
        if fixed_value is None:
            _, source_field, _ = validate_value_column(
                attributes,
                depth_shp,
                item.get("field"),
                value_mapping=item.get("value_mapping"),
                value_reclass=item.get("value_reclass"),
            )
        else:
            source_field = None
        if crs is None:
            raise InputDataError(
                f"警告：{depth_shp} のCRSが未設定のため、処理を終了します。"
            )

        group = item.get("group") or item.get("name") or depth_shp.stem
        if source_field:
            existing_field = resolved_group_fields.get(group)
            if existing_field and existing_field != source_field:
                raise InputDataError(
                    f'警告：グループ {group} 内で読み取りフィールドが一致していません'
                    f'（{existing_field}, {source_field}）。'
                )
            resolved_group_fields[group] = source_field
def run_pipeline(config):
    start = time.time()
    config = normalize_config(config)
    processing = config["processing"]
    input_items = config["inputs"]

    with stage_timer('0 入力検証'):
        validate_inputs_before_cleanup(config)

    with stage_timer('全河川出力（工程1～4）'):
        process_reports = run_processing_pass(
            input_items,
            output_path=processing.get("output_path"),
            processing=processing,
            split_path=SPLIT_PATH,
            rank_path=RANK_PATH,
            extra_split_path=EXTRA_SPLIT_PATH,
            extra_rank_path=EXTRA_RANK_PATH,
        )

    if processing.get("output_group_files", False):
        with stage_timer('河川別出力（全グループ）'):
            generate_group_outputs(input_items, processing)

    print('完了しました。')
    end = time.time()
    print(format_elapsed_time(start, end))
    print_final_warnings(
        process_reports,
        dissolve_input_by_value=processing.get("dissolve_input_by_value", False),
    )

    if not processing.get("keep_intermediate_files", False):
        cleanup_intermediate_files()
        print('中間ファイルを削除しました。')
def run_legacy_pipeline():
    start = time.time()

    # 処理に用いるシェープファイルの場所
    if INPUT_PATH.exists():
        shp = list(INPUT_PATH.glob('*.shp'))
        if not shp:
            sys.exit('shpフォルダが空です。フォルダ内にシェープファイルを配置してください。')
    else:
        create_directory(INPUT_PATH, clean=False)
        sys.exit('shpフォルダ内にシェープファイルを配置してください。')

    # 処理に用いるシェープファイルの場所（低優先のもの）
    EX = list(EXTRA_PATH.glob('*.shp')) if EXTRA_PATH.exists() else []
    config = build_legacy_config()
    config["inputs"] = [
        item for item in config["inputs"]
        if Path(item["path"]).parent != EXTRA_PATH or EX
    ]
    run_pipeline(config)

    end = time.time()
    return end - start
