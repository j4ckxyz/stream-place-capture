param(
  [string]$Python = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path ".venv")) {
  & $Python -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt

if (-not (Test-Path "config\streams.json")) {
  Copy-Item "config\streams.example.json" "config\streams.json"
  Write-Host "Created config\streams.json from example. Edit if needed."
}

Write-Host "Setup complete."
