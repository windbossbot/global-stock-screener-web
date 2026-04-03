param(
  [switch]$KeepAlive
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

Ensure-AppRuntimeDir
Set-WorkerState -ProcessId $PID -Mode ($(if ($KeepAlive) { "keepalive" } else { "single" }))
Ensure-AppPython

$launchArgs = @(
  "-m", "streamlit", "run", "app.py",
  "--server.headless", "true",
  "--server.address", $script:AppHost,
  "--server.port", "$($script:AppPort)",
  "--server.fileWatcherType", "none",
  "--runner.fastReruns", "true"
)

try {
  if ($KeepAlive) {
    while ($true) {
      try {
        & $script:PythonExe @launchArgs
      } catch {
      }
      Start-Sleep -Seconds 5
    }
  } else {
    & $script:PythonExe @launchArgs
  }
} finally {
  $pidFromFile = Get-WorkerPidFromFile
  if ($pidFromFile -eq $PID) {
    Clear-WorkerState
  }
}
