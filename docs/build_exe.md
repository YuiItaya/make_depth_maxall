# exe化手順

このプロジェクトはPyInstallerでWindows向けexeを作成します。

GeoPandas、pyogrio、pyproj、rasterio、matplotlibなどの依存DLLとデータファイルが多いため、単一exeではなく `onedir` 形式で配布します。

## ビルド

PowerShellでプロジェクト直下に移動し、次を実行します。

```powershell
.\build_exe.ps1
```

ビルド後、次の場所にexeが作成されます。

```text
dist\make_depth_maxall\make_depth_maxall.exe
```

配布する場合は、`dist\make_depth_maxall` フォルダごと渡してください。exe単体だけでは動作しません。

## 手動ビルド

スクリプトを使わずに実行する場合:

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements-build.txt
.\venv\Scripts\python.exe -m PyInstaller --clean --noconfirm make_depth_maxall.spec
```

## 動作確認

```powershell
dist\make_depth_maxall\make_depth_maxall.exe --help
dist\make_depth_maxall\make_depth_maxall.exe
```

`--help` はCLIの確認です。引数なしで実行するとGUIが起動します。

## 既知の注意点

- 初回起動は依存ライブラリの読み込みに時間がかかる場合があります。
- 地図の背景表示にはインターネット接続が必要です。
- ウイルス対策ソフトにより、PyInstaller製exeが確認対象になることがあります。
