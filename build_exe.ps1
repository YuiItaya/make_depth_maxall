$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Python = Join-Path $Root "venv\Scripts\python.exe"

if (-not (Test-Path $Python)) {
    Write-Error "venv\Scripts\python.exe が見つかりません。先に python -m venv venv と requirements.txt のインストールを行ってください。"
}

& $Python -m pip install -r (Join-Path $Root "requirements-build.txt")
if ($LASTEXITCODE -ne 0) {
    throw "ビルド依存ライブラリのインストールに失敗しました。"
}

$WorkPath = Join-Path $env:TEMP "make_depth_maxall_pyinstaller_build"
if (Test-Path $WorkPath) {
    Remove-Item -Recurse -Force $WorkPath
}

& $Python -m PyInstaller --clean --noconfirm --workpath $WorkPath (Join-Path $Root "make_depth_maxall.spec")
if ($LASTEXITCODE -ne 0) {
    throw "PyInstallerの実行に失敗しました。"
}

$Dist = Join-Path $Root "dist\make_depth_maxall"
$DistDocs = Join-Path $Dist "docs"
if (Test-Path $DistDocs) {
    Remove-Item -Recurse -Force $DistDocs
}
Copy-Item -Recurse (Join-Path $Root "docs") $DistDocs
$Readme = Join-Path $Root "README.md"
if (Test-Path $Readme) {
    Copy-Item $Readme (Join-Path $Dist "README.md") -Force
}

Write-Host ""
Write-Host "ビルド完了: dist\make_depth_maxall\make_depth_maxall.exe"
