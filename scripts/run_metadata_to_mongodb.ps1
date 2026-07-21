param(
    [switch]$AvailableNow
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Missing .env file. Copy .env.example to .env and fill the required values."
}

Get-Content -LiteralPath $envPath | ForEach-Object {
    $line = $_.Trim()
    if ($line -and -not $line.StartsWith("#") -and $line.Contains("=")) {
        $name, $value = $line.Split("=", 2)
        $value = $value.Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($name.Trim(), $value, "Process")
    }
}

$jobPath = Join-Path $projectRoot "spark_jobs\metadata_to_mongodb.py"
$arguments = @(
    "--packages",
    "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
    $jobPath
)

if ($AvailableNow) {
    $arguments += "--available-now"
}

& spark-submit @arguments
exit $LASTEXITCODE
