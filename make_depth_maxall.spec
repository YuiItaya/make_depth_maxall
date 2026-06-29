# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.utils.hooks import collect_all, copy_metadata


datas = []
binaries = []
hiddenimports = []

for package_name in (
    "affine",
    "contextily",
    "geopandas",
    "matplotlib",
    "mercantile",
    "numpy",
    "pandas",
    "pyogrio",
    "pyproj",
    "rasterio",
    "shapely",
    "xyzservices",
):
    package_datas, package_binaries, package_hiddenimports = collect_all(package_name)
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hiddenimports

for package_name in (
    "geopandas",
    "matplotlib",
    "pyogrio",
    "pyproj",
    "rasterio",
    "shapely",
):
    try:
        datas += copy_metadata(package_name)
    except Exception:
        pass


a = Analysis(
    ["make_depth_maxall.py"],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={
        "matplotlib": {
            "backends": ["TkAgg"],
        },
    },
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="make_depth_maxall",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="make_depth_maxall",
)
