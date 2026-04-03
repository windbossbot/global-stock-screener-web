Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

if (Test-AppHttpReady) {
  Open-AppBrowser
  Write-Host "[run] already running: $($script:AppUrl)"
  exit 0
}

if (Test-AppWorkerRunning) {
  Open-AppBrowser
  Write-Host "[run] background worker already starting: $($script:AppUrl)"
  exit 0
}

$worker = Start-AppWorker
Start-Sleep -Milliseconds 700
Open-AppBrowser
Write-Host "[run] background worker started: PID=$($worker.Id) URL=$($script:AppUrl)"
