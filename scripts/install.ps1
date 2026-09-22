<#
.SYNOPSIS
  Installs Norupo on Windows.

.DESCRIPTION
  Downloads the latest release for this machine's architecture, verifies its
  SHA-256 checksum, and installs `norupo.exe` and `norupo-server.exe`.

  irm https://raw.githubusercontent.com/Mahmoud-walid/Norupo-tunnel/main/scripts/install.ps1 | iex

.PARAMETER Version
  Release tag to install. Defaults to the latest release.

.PARAMETER BinDir
  Install directory. Defaults to %LOCALAPPDATA%\Programs\norupo.
#>
[CmdletBinding()]
param(
    [string]$Version = $env:NORUPO_VERSION,
    [string]$BinDir = $env:NORUPO_BIN_DIR
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$Repo = 'Mahmoud-walid/Norupo-tunnel'

# TLS 1.2 is not the default on older Windows PowerShell hosts.
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$target = switch ($env:PROCESSOR_ARCHITECTURE) {
    'AMD64' { 'x86_64-pc-windows-msvc' }
    'ARM64' { 'aarch64-pc-windows-msvc' }
    default { throw "Unsupported architecture: $($env:PROCESSOR_ARCHITECTURE)" }
}

if (-not $Version) {
    Write-Host 'Resolving the latest release ...'
    $release = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"
    $Version = $release.tag_name
}
$plainVersion = $Version -replace '^v', ''

$name = "norupo-$plainVersion-$target"
$url = "https://github.com/$Repo/releases/download/$Version/$name.zip"

$tmp = Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid().ToString())
New-Item -ItemType Directory -Path $tmp -Force | Out-Null

try {
    $archive = Join-Path $tmp "$name.zip"
    Write-Host "Downloading $name ..."
    Invoke-WebRequest -Uri $url -OutFile $archive -UseBasicParsing

    # Verify the checksum when the release publishes one.
    try {
        $expected = (Invoke-WebRequest -Uri "$url.sha256" -UseBasicParsing).Content.Trim().Split(' ')[0]
        $actual = (Get-FileHash -Algorithm SHA256 -Path $archive).Hash
        if ($expected -and ($actual -ne $expected.ToUpperInvariant())) {
            throw "Checksum mismatch: expected $expected, got $actual"
        }
        Write-Host 'Checksum ok'
    } catch [System.Net.WebException] {
        Write-Warning 'No published checksum for this asset; skipping verification'
    }

    Expand-Archive -Path $archive -DestinationPath $tmp -Force

    if (-not $BinDir) {
        $BinDir = Join-Path $env:LOCALAPPDATA 'Programs\norupo'
    }
    New-Item -ItemType Directory -Path $BinDir -Force | Out-Null

    # The archive may unpack into a versioned folder or flat, depending on how
    # it was produced; handle both.
    $source = Join-Path $tmp $name
    if (-not (Test-Path $source)) { $source = $tmp }

    foreach ($exe in @('norupo.exe', 'norupo-server.exe')) {
        Copy-Item (Join-Path $source $exe) (Join-Path $BinDir $exe) -Force
    }

    # Put it on PATH for future sessions.
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    if ($userPath -notlike "*$BinDir*") {
        [Environment]::SetEnvironmentVariable('Path', "$userPath;$BinDir", 'User')
        Write-Host "Added $BinDir to your user PATH (restart your shell to pick it up)"
    }

    Write-Host ''
    Write-Host "Installed norupo $plainVersion to $BinDir"
    Write-Host ''
    Write-Host '  norupo http 3000 --server http://your-edge:7000'
    Write-Host ''
} finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
}
