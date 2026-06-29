import re
import shutil
import sys
from pathlib import Path

def install_tkinter_shutdown_warning_filter():
    original_hook = sys.unraisablehook

    def hook(unraisable):
        object_name = getattr(unraisable.object, "__qualname__", "")
        if (
            unraisable.exc_type is RuntimeError
            and "main thread is not in main loop" in str(unraisable.exc_value)
            and object_name in {"Variable.__del__", "Image.__del__"}
        ):
            return
        original_hook(unraisable)

    sys.unraisablehook = hook


install_tkinter_shutdown_warning_filter()
def format_values(values):
    return ', '.join(str(value) for value in values)
def format_limited_values(values, limit=20):
    values = list(values)
    text = format_values(values[:limit])
    if len(values) > limit:
        text += f', ... 他{len(values) - limit}件'
    return text
def sanitize_filename(value):
    value = re.sub(r'[<>:"/\\|?*\s]+', '_', str(value).strip())
    value = re.sub(r'_+', '_', value).strip('._')
    return value or 'input'
def format_elapsed_time(start, end):
    """
    スクリプトの処理時間を計算します。
    """
    elapsed_time = end - start
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    return f"経過時間：{int(hours):02d}時間{int(minutes):02d}分{seconds:.2f}秒"
def create_directory(path: Path, clean=False):
    """
    指定したパスにディレクトリを作成します。cleanがTrueの場合、既存のディレクトリを削除してから新しく作成します。
    """
    if clean and path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
