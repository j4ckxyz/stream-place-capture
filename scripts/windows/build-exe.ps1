Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe -m pip install pyinstaller

& .\.venv\Scripts\pyinstaller.exe --clean --noconfirm stream-place-capture.spec

Write-Host "Build complete: dist\stream-place-capture.exe"
