Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location -Path $PSScriptRoot
. (Join-Path $PSScriptRoot "web_local_common.ps1")

$count = Stop-AppProcess
if ($count -gt 0) {
  Write-Host "[stop] stopped $count process(es) on $($script:AppUrl)"
} else {
  Write-Host "[stop] no matching app process found on $($script:AppUrl)"
}
