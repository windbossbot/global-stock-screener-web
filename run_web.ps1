Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot

if (-not (Test-Path ".venv")) {
  python -m venv .venv
}

$py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) {
  throw "python.exe not found in .venv"
}

& $py -m pip install --upgrade pip
& $py -m pip install -r requirements.txt
& $py -m streamlit run app.py --server.headless true --server.address 127.0.0.1 --server.port 8501 --server.fileWatcherType none --runner.fastReruns true
