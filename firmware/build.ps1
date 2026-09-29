$ErrorActionPreference = 'Stop'

$pio = Get-Command pio -ErrorAction SilentlyContinue
if ($null -eq $pio) {
    $pioExecutable = Join-Path $env:USERPROFILE '.platformio\penv\Scripts\pio.exe'
    if (-not (Test-Path -LiteralPath $pioExecutable)) {
        throw 'PlatformIO not found. Install PlatformIO IDE or run: pip install platformio'
    }
}
else {
    $pioExecutable = $pio.Source
}

$projectRoot = (Resolve-Path -LiteralPath $PSScriptRoot).Path
$buildRoot = $projectRoot
$buildAlias = $null

# Some ESP32 GCC builds on Windows cannot read response files when the project
# path contains Unicode. In that case, build through a temporary ASCII junction.
if ($projectRoot -match '[^\x00-\x7F]') {
    $tempRoot = [System.IO.Path]::GetFullPath($env:TEMP)
    $buildAlias = Join-Path $tempRoot ("iot_lab2_firmware_{0}" -f $PID)
    $aliasFullPath = [System.IO.Path]::GetFullPath($buildAlias)
    if (-not $aliasFullPath.StartsWith($tempRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw 'Temporary build path is outside TEMP.'
    }
    if (Test-Path -LiteralPath $aliasFullPath) {
        throw "Temporary build path already exists: $aliasFullPath"
    }
    New-Item -ItemType Junction -Path $aliasFullPath -Target $projectRoot | Out-Null
    $buildRoot = $aliasFullPath
}

Push-Location $buildRoot
try {
    & $pioExecutable run
    if ($LASTEXITCODE -ne 0) {
        throw "PlatformIO build failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
    if ($null -ne $buildAlias -and (Test-Path -LiteralPath $buildAlias)) {
        $aliasItem = Get-Item -LiteralPath $buildAlias
        if ($aliasItem.LinkType -ne 'Junction') {
            throw "Refusing to remove a non-junction path: $buildAlias"
        }
        [System.IO.Directory]::Delete($buildAlias)
    }
}
