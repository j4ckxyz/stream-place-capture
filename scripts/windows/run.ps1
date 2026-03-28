Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

& .\.venv\Scripts\python.exe -m stream_place_capture --config config/streams.json --gui
