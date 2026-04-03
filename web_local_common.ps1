Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:AppPort = 8501
$script:AppHost = "127.0.0.1"
$script:VenvDir = Join-Path $PSScriptRoot ".venv"
$script:PythonExe = Join-Path $script:VenvDir "Scripts\python.exe"
$script:RequirementsFile = Join-Path $PSScriptRoot "requirements.txt"
$script:RequirementsHashFile = Join-Path $script:VenvDir ".requirements.sha256"
$script:AppUrl = "http://$($script:AppHost):$($script:AppPort)"
$script:RuntimeDir = Join-Path $PSScriptRoot "_cache\\runtime"
$script:WorkerScript = Join-Path $PSScriptRoot "web_local_worker.ps1"
$script:WorkerPidFile = Join-Path $script:RuntimeDir "web_worker.pid"
$script:WorkerModeFile = Join-Path $script:RuntimeDir "web_worker.mode"
$script:WorkerStdoutLog = Join-Path $script:RuntimeDir "web_worker.stdout.log"
$script:WorkerStderrLog = Join-Path $script:RuntimeDir "web_worker.stderr.log"

function Ensure-AppRuntimeDir {
  if (-not (Test-Path $script:RuntimeDir)) {
    New-Item -ItemType Directory -Path $script:RuntimeDir -Force | Out-Null
  }
}

function Get-RequirementsHash {
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $stream = [System.IO.File]::OpenRead($script:RequirementsFile)
    try {
      $hashBytes = $sha.ComputeHash($stream)
      return ([System.BitConverter]::ToString($hashBytes)).Replace("-", "").ToLowerInvariant()
    } finally {
      $stream.Dispose()
    }
  } finally {
    $sha.Dispose()
  }
}

function Test-AppHttpReady {
  try {
    $r = Invoke-WebRequest -Uri $script:AppUrl -UseBasicParsing -TimeoutSec 2
    return ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500)
  } catch {
    return $false
  }
}

function Wait-AppReady {
  param(
    [int]$TimeoutSec = 60
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSec)
  while ((Get-Date) -lt $deadline) {
    if (Test-AppHttpReady) {
      return $true
    }
    Start-Sleep -Seconds 1
  }
  return $false
}

function Open-AppBrowser {
  Start-Process $script:AppUrl | Out-Null
}

function Get-WorkerPidFromFile {
  if (-not (Test-Path $script:WorkerPidFile)) {
    return $null
  }
  try {
    $raw = [string](Get-Content -LiteralPath $script:WorkerPidFile -Raw)
    $workerPid = 0
    if ([int]::TryParse($raw.Trim(), [ref]$workerPid)) {
      return $workerPid
    }
  } catch {
  }
  return $null
}

function Set-WorkerState {
  param(
    [int]$ProcessId,
    [string]$Mode
  )
  Ensure-AppRuntimeDir
  Set-Content -LiteralPath $script:WorkerPidFile -Value ([string]$ProcessId) -NoNewline
  Set-Content -LiteralPath $script:WorkerModeFile -Value ([string]$Mode) -NoNewline
}

function Clear-WorkerState {
  foreach ($path in @($script:WorkerPidFile, $script:WorkerModeFile)) {
    if (Test-Path $path) {
      Remove-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    }
  }
}

function Get-AppWorkerProcesses {
  Ensure-AppRuntimeDir
  $targets = @()
  $seen = [System.Collections.Generic.HashSet[int]]::new()
  $pidFromFile = Get-WorkerPidFromFile
  if ($pidFromFile) {
    try {
      $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $pidFromFile" -ErrorAction SilentlyContinue
      if ($null -ne $proc -and ([string]$proc.CommandLine -like "*web_local_worker.ps1*")) {
        $targets += $proc
        [void]$seen.Add([int]$proc.ProcessId)
      }
    } catch {
    }
  }
  try {
    $all = Get-CimInstance Win32_Process -Filter "Name = 'powershell.exe' OR Name = 'pwsh.exe'" -ErrorAction SilentlyContinue
    foreach ($proc in $all) {
      $cmd = [string]$proc.CommandLine
      if ($cmd -like "*web_local_worker.ps1*" -and $seen.Add([int]$proc.ProcessId)) {
        $targets += $proc
      }
    }
  } catch {
  }
  if (@($targets).Count -eq 0) {
    Clear-WorkerState
  }
  return $targets
}

function Test-AppWorkerRunning {
  return (@(Get-AppWorkerProcesses).Count -gt 0)
}

function Start-AppWorker {
  param(
    [switch]$KeepAlive
  )
  Ensure-AppRuntimeDir
  if (-not (Test-Path $script:WorkerScript)) {
    throw "web_local_worker.ps1 not found"
  }
  Set-Content -LiteralPath $script:WorkerStdoutLog -Value "" -NoNewline
  Set-Content -LiteralPath $script:WorkerStderrLog -Value "" -NoNewline
  $argList = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-WindowStyle", "Hidden",
    "-File", $script:WorkerScript
  )
  if ($KeepAlive) {
    $argList += "-KeepAlive"
  }
  $proc = Start-Process -FilePath "powershell.exe" -ArgumentList $argList -WindowStyle Hidden -WorkingDirectory $PSScriptRoot -PassThru -RedirectStandardOutput $script:WorkerStdoutLog -RedirectStandardError $script:WorkerStderrLog
  Set-WorkerState -ProcessId $proc.Id -Mode ($(if ($KeepAlive) { "keepalive" } else { "single" }))
  return $proc
}

function Get-AppStreamlitProcesses {
  $candidates = @()
  try {
    $connections = Get-NetTCPConnection -LocalAddress $script:AppHost -LocalPort $script:AppPort -ErrorAction SilentlyContinue
    $ownerIds = @($connections | Select-Object -ExpandProperty OwningProcess -Unique)
    foreach ($procId in $ownerIds) {
      if (-not $procId) {
        continue
      }
      $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $procId" -ErrorAction SilentlyContinue
      if ($null -eq $proc) {
        continue
      }
      $processName = ""
      try {
        $p = Get-Process -Id $procId -ErrorAction SilentlyContinue
        if ($null -ne $p) {
          $processName = [string]$p.ProcessName
        }
      } catch {
      }
      $cmd = [string]$proc.CommandLine
      $isPython = $processName -match '^python'
      $looksLikeApp = (($cmd -match 'streamlit') -and ($cmd -match 'app\.py')) -or ($isPython -and $cmd -match '--server\.port 8501')
      if ($looksLikeApp) {
        $candidates += $proc
      }
    }
  } catch {
  }
  return $candidates
}

function Stop-AppProcess {
  $stopped = [System.Collections.Generic.HashSet[int]]::new()
  $workers = Get-AppWorkerProcesses
  foreach ($proc in $workers) {
    try {
      Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
      [void]$stopped.Add([int]$proc.ProcessId)
    } catch {
    }
  }
  Start-Sleep -Milliseconds 300
  $targets = Get-AppStreamlitProcesses
  foreach ($proc in $targets) {
    try {
      Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
      [void]$stopped.Add([int]$proc.ProcessId)
    } catch {
    }
  }
  Clear-WorkerState
  return $stopped.Count
}

function Ensure-AppPython {
  if (-not (Test-Path $script:VenvDir)) {
    python -m venv $script:VenvDir
  }
  if (-not (Test-Path $script:PythonExe)) {
    throw "python.exe not found in .venv"
  }

  $currentHash = Get-RequirementsHash
  $savedHash = ""
  if (Test-Path $script:RequirementsHashFile) {
    $savedHash = [string](Get-Content -Raw $script:RequirementsHashFile)
    $savedHash = $savedHash.Trim().ToLowerInvariant()
  }

  if ($savedHash -ne $currentHash) {
    & $script:PythonExe -m pip install --upgrade pip
    & $script:PythonExe -m pip install -r $script:RequirementsFile
    Set-Content -LiteralPath $script:RequirementsHashFile -Value $currentHash -NoNewline
  }
}
