Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

if (Test-AppHttpReady) {
  Start-Process $script:AppUrl | Out-Null
  Write-Host "[run] already running: $($script:AppUrl)"
  exit 0
}

Ensure-AppPython
Open-AppBrowserWhenReady
& $script:PythonExe -m streamlit run app.py --server.headless true --server.address $script:AppHost --server.port $script:AppPort --server.fileWatcherType none --runner.fastReruns true
