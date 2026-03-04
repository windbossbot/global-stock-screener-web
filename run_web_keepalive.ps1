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

while ($true) {
  Write-Host "[keepalive] streamlit starting..."
  try {
    & $py -m streamlit run app.py --server.headless true --server.address 0.0.0.0 --server.port 8502
  } catch {
    Write-Host "[keepalive] streamlit crashed: $($_.Exception.Message)"
  }
  Write-Host "[keepalive] restart in 5s"
  Start-Sleep -Seconds 5
}

