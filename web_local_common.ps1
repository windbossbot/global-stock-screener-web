Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:AppPort = 8501
$script:AppHost = "127.0.0.1"
$script:VenvDir = Join-Path $PSScriptRoot ".venv"
$script:PythonExe = Join-Path $script:VenvDir "Scripts\python.exe"
$script:RequirementsFile = Join-Path $PSScriptRoot "requirements.txt"
$script:RequirementsHashFile = Join-Path $script:VenvDir ".requirements.sha256"
$script:AppUrl = "http://$($script:AppHost):$($script:AppPort)"

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

function Open-AppBrowserWhenReady {
  Start-Job -Name "dividend-screener-open-browser" -ScriptBlock {
    param($Url)
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
      try {
        $r = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
        if ($r.StatusCode -ge 200 -and $r.StatusCode -lt 500) {
          Start-Process $Url | Out-Null
          break
        }
      } catch {
      }
      Start-Sleep -Seconds 1
    }
  } -ArgumentList $script:AppUrl | Out-Null
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
  $targets = Get-AppStreamlitProcesses
  foreach ($proc in $targets) {
    try {
      Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    } catch {
    }
  }
  return @($targets).Count
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
