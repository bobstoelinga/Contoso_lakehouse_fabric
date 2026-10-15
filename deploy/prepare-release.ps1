param(
    [Parameter(Mandatory = $true)][string]$Python,
    [string]$Project = 'contoso',
    [string]$Environment = 'dev'
)

$ErrorActionPreference = 'Stop'
if ($Project -notmatch '^[A-Za-z_][A-Za-z0-9_]*$' -or $Environment -notmatch '^[A-Za-z_][A-Za-z0-9_]*$') {
    throw 'Project and environment must be safe identifiers.'
}
$Root = Split-Path $PSScriptRoot
Push-Location $Root
$PreviousPythonPath = $env:PYTHONPATH
$PreviousEpoch = $env:SOURCE_DATE_EPOCH
try {
    $env:PYTHONPATH = Join-Path $Root 'src'
    $env:SOURCE_DATE_EPOCH = '315532800'
    $Release = Join-Path $Root "projects\$Project\release.json"
    $Binding = Join-Path $Root "environments\$Project.$Environment.json"
    & $Python -m fabric_etl validate-project $Release
    if ($LASTEXITCODE -ne 0) { throw 'Project validation failed.' }
    & $Python -m fabric_etl validate-environment $Binding
    if ($LASTEXITCODE -ne 0) { throw 'Environment validation failed.' }
    & $Python -m pip wheel (Join-Path $Root 'packaging\fabric') --no-deps --no-build-isolation --wheel-dir (Join-Path $Root 'dist')
    if ($LASTEXITCODE -ne 0) { throw 'Wheel build failed; verify the declared packaging build dependencies.' }
    $Wheel = Join-Path $Root 'dist\fabric_etl-0.1.0-py3-none-any.whl'
    if (-not (Test-Path -LiteralPath $Wheel)) { throw 'Expected version-pinned wheel was not built.' }
    $WheelHash = (Get-FileHash -LiteralPath $Wheel -Algorithm SHA256).Hash.ToLowerInvariant()
    $ReleaseId = (Get-Content -LiteralPath $Release -Raw | ConvertFrom-Json).release_id
    $BindingHash = (Get-FileHash -LiteralPath $Binding -Algorithm SHA256).Hash.ToLowerInvariant()
    $PlanName = '{0}.{1}.{2}.{3}.{4}.plan.json' -f $Project, $Environment,
        $ReleaseId.Substring(0, 12), $WheelHash.Substring(0, 12), $BindingHash.Substring(0, 12)
    $Plan = Join-Path $Root "dist\$PlanName"
    & $Python -m fabric_etl plan-release $Release --repository $Root --environment $Binding --wheel $Wheel --output $Plan
    if ($LASTEXITCODE -ne 0) { throw 'Deployment planning failed.' }
    Write-Output 'Prepared locally. No Fabric deployment, data writes or job starts performed.'
}
finally {
    $env:PYTHONPATH = $PreviousPythonPath
    $env:SOURCE_DATE_EPOCH = $PreviousEpoch
    Pop-Location
}
