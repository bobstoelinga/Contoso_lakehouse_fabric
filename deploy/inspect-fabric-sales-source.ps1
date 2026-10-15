param(
    [Parameter(Mandatory = $true)][string]$SqlClientRoot,
    [Parameter(Mandatory = $true)][string]$Receipt
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$binding = Get-Content -LiteralPath (Join-Path $root 'environments\contoso.dev.json') -Raw | ConvertFrom-Json
$connectors = Get-Content -LiteralPath (Join-Path $root 'metadata\seed\meta_source_connector.json') -Raw | ConvertFrom-Json
$source = @($connectors | Where-Object { $_.source_object_id -eq 'FABRIC_SALES.ORDER_LINES' })
if ($source.Count -ne 1 -or $source[0].request_options.source_query -ne 'SELECT * FROM SalesLT.vw_databricks_sales_order_line') {
    throw 'Expected exactly one known FABRIC_SALES source/query; refusing a different scope.'
}
$jdbc = $source[0].endpoint_url
if ($jdbc -notmatch '^jdbc:sqlserver://(?<server>[a-z0-9-]+\.database\.fabric\.microsoft\.com):1433;databaseName=(?<database>[^;]+);') {
    throw 'Fabric Sales endpoint is not the declared Fabric SQL source.'
}
$server = $Matches['server']
$database = $Matches['database']
$report = [ordered]@{
    status = 'PREFLIGHT'
    scope = 'READ_ONLY_FABRIC_SALES_SOURCE_SCHEMA'
    source_object_id = 'FABRIC_SALES.ORDER_LINES'
    server = $server
    database = $database
    identity_mode = 'DELEGATED_AZURE_CLI_SCHEMA_PROBE_ONLY'
    business_rows_extracted = $false
    connection_alias_activated = $false
    started_at_utc = [DateTime]::UtcNow.ToString('o')
}
$receiptPath = $ExecutionContext.SessionState.Path.GetUnresolvedProviderPathFromPSPath($Receipt)
if (Test-Path -LiteralPath $receiptPath) {
    throw 'Refusing to overwrite an existing source schema receipt; use a new receipt path.'
}
$parent = Split-Path -Parent $receiptPath
[IO.Directory]::CreateDirectory($parent) | Out-Null
$report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
$connection = $null
try {
    foreach ($relative in @(
        'system.runtime.compilerservices.unsafe.4.5.3\lib\net461\System.Runtime.CompilerServices.Unsafe.dll',
        'system.buffers.4.5.1\lib\net461\System.Buffers.dll',
        'system.memory.4.5.5\lib\net461\System.Memory.dll',
        'sqlclient\runtimes\win\lib\net462\Microsoft.Data.SqlClient.dll'
    )) {
        $path = Join-Path $SqlClientRoot $relative
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Previously validated SQL client dependency is missing: $relative"
        }
        [Reflection.Assembly]::LoadFrom($path) | Out-Null
    }
    $nativePath = Join-Path $SqlClientRoot 'sqlclient\lib\net462'
    $env:PATH = "$nativePath;$env:PATH"
    $tokenJson = & az account get-access-token --tenant $binding.tenant_id --resource 'https://database.windows.net/' --output json
    if ($LASTEXITCODE -ne 0) {
        throw 'Azure SQL token acquisition failed; no SQL operation was started.'
    }
    $token = ($tokenJson | ConvertFrom-Json).accessToken
    if ([string]::IsNullOrWhiteSpace($token)) {
        throw 'Azure CLI did not return an Azure SQL access token.'
    }
    $connection = New-Object Microsoft.Data.SqlClient.SqlConnection
    $builder = New-Object Microsoft.Data.SqlClient.SqlConnectionStringBuilder
    $builder['Data Source'] = "tcp:$server,1433"
    $builder['Initial Catalog'] = $database
    $builder['Encrypt'] = 'Mandatory'
    $builder['Trust Server Certificate'] = $false
    $builder['Connect Timeout'] = 30
    $builder['Application Intent'] = 'ReadOnly'
    $connection.ConnectionString = $builder.ConnectionString
    $connection.AccessToken = $token
    $connection.Open()
    $command = $connection.CreateCommand()
    $command.CommandText = 'SELECT TOP (0) * FROM SalesLT.vw_databricks_sales_order_line'
    $command.CommandTimeout = 30
    $reader = $command.ExecuteReader()
    try {
        $columns = @($reader.GetSchemaTable().Rows | ForEach-Object {
            [ordered]@{
                name = $_.ColumnName
                ordinal = [int]$_.ColumnOrdinal
                type = $reader.GetDataTypeName([int]$_.ColumnOrdinal)
                nullable = [bool]$_.AllowDBNull
                precision = $(if ($_.NumericPrecision -eq [DBNull]::Value) { $null } else { [int]$_.NumericPrecision })
                scale = $(if ($_.NumericScale -eq [DBNull]::Value) { $null } else { [int]$_.NumericScale })
            }
        })
        if ($columns.Count -eq 0) {
            throw 'Fabric Sales source returned no schema.'
        }
        if ($reader.Read()) {
            throw 'The TOP(0) schema probe unexpectedly returned business rows.'
        }
    } finally {
        $reader.Dispose()
        $command.Dispose()
    }
    $report['columns'] = $columns
    $report['status'] = 'SOURCE_SCHEMA_VERIFIED'
} catch {
    $report['status'] = 'FAILED'
    $report['error_type'] = $_.Exception.GetType().FullName
    $report['error_message'] = $_.Exception.Message
    if ($_.Exception.PSObject.Properties.Name -contains 'Number') {
        $report['sql_error_number'] = $_.Exception.Number
    }
    throw
} finally {
    if ($null -ne $connection) {
        $connection.Dispose()
    }
    $token = $null
    $tokenJson = $null
    $report['finished_at_utc'] = [DateTime]::UtcNow.ToString('o')
    $report | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
}
[ordered]@{
    status = $report.status
    scope = $report.scope
    column_count = $report.columns.Count
    business_rows_extracted = $false
    connection_alias_activated = $false
    receipt = $receiptPath
} | ConvertTo-Json
