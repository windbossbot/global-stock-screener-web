Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

Ensure-AppPython
if (Test-AppHttpReady) {
  Start-Process $script:AppUrl | Out-Null
  Write-Host "[keepalive] already running: $($script:AppUrl)"
  exit 0
}
Open-AppBrowserWhenReady

while ($true) {
  Write-Host "[keepalive] streamlit starting..."
  try {
    & $script:PythonExe -m streamlit run app.py --server.headless true --server.address $script:AppHost --server.port $script:AppPort --server.fileWatcherType none --runner.fastReruns true
  } catch {
    Write-Host "[keepalive] streamlit crashed: $($_.Exception.Message)"
  }
  Write-Host "[keepalive] restart in 5s"
  Start-Sleep -Seconds 5
}

