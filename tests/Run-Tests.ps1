<#
.SYNOPSIS
    Run the Horizon Books Pester test suite with formatted output.

.DESCRIPTION
    Wrapper to invoke Pester 5 with selected tags and readable console output.
    Defaults to all offline tests (excludes Integration).

.PARAMETER Tag
    Run only tests with these tags. Valid: Unit, NonRegression, TMDL, Definition,
    DataQuality, DeployScript, Integration

.PARAMETER ExcludeTag
    Exclude tests with these tags. Default: Integration

.PARAMETER WorkspaceId
    Fabric workspace GUID for Integration tests.

.PARAMETER Detailed
    Show detailed per-test output (Pester -Output Detailed).

.EXAMPLE
    .\tests\Run-Tests.ps1                           # all offline tests
    .\tests\Run-Tests.ps1 -Tag NonRegression        # non-regression only
    .\tests\Run-Tests.ps1 -Tag Integration -WorkspaceId "guid"
    .\tests\Run-Tests.ps1 -Detailed                 # verbose output
#>

param(
    [string[]]$Tag,
    [string[]]$ExcludeTag,
    [string]$WorkspaceId,
    [switch]$Detailed
)

$ErrorActionPreference = "Stop"

# Ensure Pester 5+
$pester = Get-Module Pester -ListAvailable | Sort-Object Version -Descending | Select-Object -First 1
if (-not $pester -or $pester.Version.Major -lt 5) {
    Write-Host "Installing Pester 5..." -ForegroundColor Yellow
    Install-Module Pester -Force -SkipPublisherCheck -Scope CurrentUser
}
Import-Module Pester -MinimumVersion 5.0 -Force

$testFile = Join-Path $PSScriptRoot "Deploy-HorizonBooks.Tests.ps1"

# Build container
$containerParams = @{ Path = $testFile }
if ($WorkspaceId) {
    $containerParams.Data = @{ WorkspaceId = $WorkspaceId }
}
$container = New-PesterContainer @containerParams

# Build config
$config = New-PesterConfiguration
$config.Run.Container = $container
$config.Run.PassThru  = $true
$config.Output.Verbosity = if ($Detailed) { "Detailed" } else { "Normal" }

if ($Tag) {
    $config.Filter.Tag = $Tag
}

if ($ExcludeTag) {
    $config.Filter.ExcludeTag = $ExcludeTag
}
elseif (-not $Tag) {
    # Default: exclude Integration when no specific tag requested
    $config.Filter.ExcludeTag = @("Integration")
}

# Run
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Horizon Books - Pester Test Suite" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host ""

$result = Invoke-Pester -Configuration $config

# Summary
Write-Host ""
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Results" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  Passed : $($result.PassedCount)" -ForegroundColor Green
Write-Host "  Failed : $($result.FailedCount)" -ForegroundColor $(if ($result.FailedCount -gt 0) { "Red" } else { "Green" })
Write-Host "  Skipped: $($result.SkippedCount)" -ForegroundColor Yellow
Write-Host "  Total  : $($result.TotalCount)"
Write-Host "  Time   : $([math]::Round($result.Duration.TotalSeconds, 2))s"
Write-Host ""

# Exit code for CI/CD
if ($result.FailedCount -gt 0) { exit 1 } else { exit 0 }
