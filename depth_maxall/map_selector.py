from pathlib import Path

import pandas as pd
import geopandas as gpd
import pyogrio
from shapely.geometry import box

from .constants import DEFAULT_MAP_BOUNDS_6668, JGD2011
from .errors import InputDataError
from .io_utils import suppress_noisy_gdal_warnings

def get_shapefile_extent_gdf(shp_path):
    with suppress_noisy_gdal_warnings():
        info = pyogrio.read_info(
            shp_path,
            encoding='shift-jis',
            force_total_bounds=True,
        )
    crs = info.get("crs")
    total_bounds = info.get("total_bounds")
    if crs is None:
        raise InputDataError(
            f"警告：{shp_path} のCRSが未設定のため、地図に表示できません。"
        )
    if total_bounds is None:
        return None

    minx, miny, maxx, maxy = total_bounds
    if minx == maxx or miny == maxy:
        return None

    return gpd.GeoDataFrame(
        geometry=[box(minx, miny, maxx, maxy)],
        crs=crs,
    ).to_crs(epsg=3857)
def expand_bounds_to_aspect(bounds, target_aspect, padding_ratio=0.08):
    minx, miny, maxx, maxy = bounds
    width = maxx - minx
    height = maxy - miny

    if width <= 0:
        width = 1000
        minx -= width / 2
        maxx += width / 2
    if height <= 0:
        height = 1000
        miny -= height / 2
        maxy += height / 2

    current_aspect = width / height
    center_x = (minx + maxx) / 2
    center_y = (miny + maxy) / 2

    if current_aspect > target_aspect:
        height = width / target_aspect
    else:
        width = height * target_aspect

    width *= 1 + padding_ratio * 2
    height *= 1 + padding_ratio * 2

    return (
        center_x - width / 2,
        center_y - height / 2,
        center_x + width / 2,
        center_y + height / 2,
    )
def select_clip_bounds_on_map(
    input_items,
    current_bounds=None,
    progress_callback=None,
    stop_loading_callback=None,
):
    try:
        import contextily as cx
        import matplotlib.pyplot as plt
        from matplotlib import font_manager, rcParams
        from matplotlib.patches import Rectangle
        from matplotlib.widgets import Button, RectangleSelector
        from pyproj import Transformer
    except ImportError as e:
        raise InputDataError(
            '警告：地図で範囲選択を使用するには matplotlib と contextily が必要です。'
            'requirements.txt からインストールしてください。'
        ) from e

    available_fonts = {font.name for font in font_manager.fontManager.ttflist}
    for font_name in ("Yu Gothic", "Meiryo", "MS Gothic"):
        if font_name in available_fonts:
            rcParams["font.family"] = font_name
            break

    extent_gdfs = []
    stopped_loading = False
    total_items = len(input_items)
    for index, item in enumerate(input_items, 1):
        if stop_loading_callback and stop_loading_callback():
            stopped_loading = True
            break

        path = Path(item["path"])
        if progress_callback:
            progress_callback(index - 1, total_items, path, stopped_loading)

        extent_gdf = get_shapefile_extent_gdf(path)
        if extent_gdf is None or extent_gdf.empty:
            if progress_callback:
                progress_callback(index, total_items, path, stopped_loading)
            continue

        extent_gdfs.append(extent_gdf)

        if progress_callback:
            progress_callback(index, total_items, path, stopped_loading)

    if stop_loading_callback and stop_loading_callback():
        stopped_loading = True

    if not extent_gdfs and not stopped_loading:
        raise InputDataError('警告：地図に表示できる入力シェープファイルがありません。')

    map_gdf = None
    if extent_gdfs:
        map_gdf = gpd.GeoDataFrame(
            pd.concat(extent_gdfs, ignore_index=True),
            crs=extent_gdfs[0].crs,
        )

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.subplots_adjust(bottom=0.16)
    if map_gdf is None:
        ax.set_title('処理範囲をドラッグで選択し、「確定」を押してください。背景地図のみ表示しています。')
    else:
        ax.set_title(
            '処理範囲をドラッグで選択し、「確定」を押してください。'
            '赤枠は入力シェープファイルの外接範囲です。'
        )
    ax.set_axis_off()

    transformer_to_3857 = Transformer.from_crs(JGD2011, 3857, always_xy=True)
    if map_gdf is None:
        bounds_6668 = current_bounds or DEFAULT_MAP_BOUNDS_6668
        min_lon, min_lat, max_lon, max_lat = bounds_6668
        minx, miny = transformer_to_3857.transform(min_lon, min_lat)
        maxx, maxy = transformer_to_3857.transform(max_lon, max_lat)
    else:
        minx, miny, maxx, maxy = map_gdf.total_bounds

    minx, miny, maxx, maxy = expand_bounds_to_aspect(
        (minx, miny, maxx, maxy),
        target_aspect=10 / 8,
    )
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)

    overlay_artists = {"boundary": None, "error_text": None}

    def remove_basemap_artists():
        for image in list(ax.images):
            image.remove()

    def draw_overlays():
        if map_gdf is None:
            return

        if overlay_artists["boundary"] is not None:
            overlay_artists["boundary"].remove()

        overlay_artists["boundary"] = map_gdf.boundary.plot(
            ax=ax,
            linewidth=0.8,
            edgecolor='red',
            alpha=0.9,
        ).collections[-1]

    def show_basemap_error(message):
        if overlay_artists["error_text"] is not None:
            overlay_artists["error_text"].remove()
            overlay_artists["error_text"] = None

        if message:
            overlay_artists["error_text"] = ax.text(
                0.02,
                0.02,
                f'背景地図を取得できませんでした: {message}',
                transform=ax.transAxes,
                fontsize=9,
                color='black',
                bbox={'facecolor': 'white', 'alpha': 0.8, 'edgecolor': 'none'},
            )

    def reload_basemap(_event=None):
        current_xlim = ax.get_xlim()
        current_ylim = ax.get_ylim()
        remove_basemap_artists()
        show_basemap_error(None)

        try:
            cx.add_basemap(
                ax,
                source='https://cyberjapandata.gsi.go.jp/xyz/std/{z}/{x}/{y}.png',
                attribution='地理院タイル',
                reset_extent=False,
            )
        except Exception as e:
            show_basemap_error(e)

        ax.set_xlim(current_xlim)
        ax.set_ylim(current_ylim)
        draw_overlays()
        fig.canvas.draw_idle()

    selected = {"bounds_3857": None}
    selected_patch = {"patch": None}

    reload_basemap()

    def draw_selected_bounds(bounds_3857):
        if selected_patch["patch"] is not None:
            selected_patch["patch"].remove()

        x1, y1, x2, y2 = bounds_3857
        left, right = sorted((x1, x2))
        bottom, top = sorted((y1, y2))
        selected["bounds_3857"] = (left, bottom, right, top)
        selected_patch["patch"] = Rectangle(
            (left, bottom),
            right - left,
            top - bottom,
            fill=False,
            edgecolor='blue',
            linewidth=2.0,
        )
        ax.add_patch(selected_patch["patch"])
        fig.canvas.draw_idle()

    if current_bounds:
        transformer_to_3857 = Transformer.from_crs(JGD2011, 3857, always_xy=True)
        cur_minx, cur_miny, cur_maxx, cur_maxy = current_bounds
        x1, y1 = transformer_to_3857.transform(cur_minx, cur_miny)
        x2, y2 = transformer_to_3857.transform(cur_maxx, cur_maxy)
        draw_selected_bounds((x1, y1, x2, y2))

    def on_select(eclick, erelease):
        if None in (eclick.xdata, eclick.ydata, erelease.xdata, erelease.ydata):
            return
        draw_selected_bounds((eclick.xdata, eclick.ydata, erelease.xdata, erelease.ydata))

    selector = RectangleSelector(
        ax,
        on_select,
        useblit=True,
        button=[1],
        interactive=True,
        props={'facecolor': 'tab:blue', 'edgecolor': 'blue', 'alpha': 0.18, 'fill': True},
    )

    reload_ax = fig.add_axes([0.56, 0.04, 0.13, 0.05])
    confirm_ax = fig.add_axes([0.72, 0.04, 0.10, 0.05])
    cancel_ax = fig.add_axes([0.84, 0.04, 0.10, 0.05])
    reload_button = Button(reload_ax, '地図再読込')
    confirm_button = Button(confirm_ax, '確定')
    cancel_button = Button(cancel_ax, 'キャンセル')
    confirmed = {"value": False}

    def confirm(_event):
        if selected["bounds_3857"] is not None:
            confirmed["value"] = True
            plt.close(fig)

    def cancel(_event):
        confirmed["value"] = False
        selected["bounds_3857"] = None
        plt.close(fig)

    reload_button.on_clicked(reload_basemap)
    confirm_button.on_clicked(confirm)
    cancel_button.on_clicked(cancel)
    plt.show()

    # Keep widget callbacks alive until the window closes.
    _ = selector, reload_button, confirm_button, cancel_button

    if not confirmed["value"] or selected["bounds_3857"] is None:
        return None

    transformer_to_6668 = Transformer.from_crs(3857, JGD2011, always_xy=True)
    left, bottom, right, top = selected["bounds_3857"]
    lon1, lat1 = transformer_to_6668.transform(left, bottom)
    lon2, lat2 = transformer_to_6668.transform(right, top)
    min_lon, max_lon = sorted((lon1, lon2))
    min_lat, max_lat = sorted((lat1, lat2))
    return [
        round(min_lon, 8),
        round(min_lat, 8),
        round(max_lon, 8),
        round(max_lat, 8),
    ]
