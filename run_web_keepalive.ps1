Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

if (Test-AppHttpReady) {
  Open-AppBrowser
  Write-Host "[keepalive] already running: $($script:AppUrl)"
  exit 0
}

if (Test-AppWorkerRunning) {
  Open-AppBrowser
  Write-Host "[keepalive] background worker already running: $($script:AppUrl)"
  exit 0
}

$worker = Start-AppWorker -KeepAlive
Start-Sleep -Milliseconds 700
Open-AppBrowser
Write-Host "[keepalive] hidden keepalive worker started: PID=$($worker.Id) URL=$($script:AppUrl)"

