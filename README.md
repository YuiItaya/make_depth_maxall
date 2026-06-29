# 最大包絡シェープファイル作成

浸水深又は浸水継続時間のShapefileを入力し、重なり部分では最大rankを採用した最大包絡Shapefileを作成するツールです。
フィールド値からrankを読む方法に加えて、Shapefile単位で固定rankを指定する方法にも対応しています。

## GitHubで管理するもの

このリポジトリでは、ソースコード、依存関係、ビルド手順、操作ドキュメントを管理します。

`dist/`、`build/`、`venv/`、入力データ、処理中間ファイル、出力ファイルはGit管理しません。
exe版を配布する場合は、Gitに直接含めず、GitHub Releasesなどに `dist\make_depth_maxall` フォルダをzip化して添付してください。

## 起動

Pythonで起動する場合:

```powershell
python make_depth_maxall.py
```

exe版で起動する場合:

```powershell
dist\make_depth_maxall\make_depth_maxall.exe
```

## ドキュメント

- 操作方法: [docs/user_guide.md](docs/user_guide.md)
- exe化手順: [docs/build_exe.md](docs/build_exe.md)

## exe版の配布

exe版は `dist\make_depth_maxall` フォルダに作成されます。

配布時は `make_depth_maxall.exe` 単体ではなく、`dist\make_depth_maxall` フォルダごと渡してください。
`dist/` は生成物のため、通常はGitHubへpushしません。

## 開発者向け

依存ライブラリのインストール:

```powershell
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

exeビルド:

```powershell
.\build_exe.ps1
```
