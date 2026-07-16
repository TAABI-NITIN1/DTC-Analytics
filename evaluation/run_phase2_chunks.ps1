# Resume Phase 2 in chunks (--limit-sessions applies to *remaining* catalog after checkpoint).
# Production VM backend (default): http://4.224.101.147:8005
# Local dev: -ApiBaseUrl http://127.0.0.1:8005 -EvalEnvironment local
param(
    [string]$ApiBaseUrl = 'http://4.224.101.147:8005',
    [string]$EvalEnvironment = 'production',
    [string]$RunId = 'eval_20260520_085108_190cf6',
    [int]$ChunkSize = 75,
    [int]$CatalogTotal = 1000,
    [string]$SessionsFile = 'evaluation/conversational_scenarios/sessions_1000.json'
)

$ErrorActionPreference = 'Stop'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
Set-Location $RepoRoot

$env:AI_ANALYST_PERSIST_OBSERVABILITY = '0'
$env:PYTHONUNBUFFERED = '1'
$env:EVAL_API_BASE_URL = $ApiBaseUrl
$env:EVAL_ENVIRONMENT = $EvalEnvironment
$env:EVAL_HEALTH_TIMEOUT_SEC = '90'
$env:EVAL_HEALTH_RETRIES = '3'
$env:AI_ANALYST_EVAL_TIMEOUT_SEC = '300'

$rollupsPath = Join-Path $RepoRoot "evaluation\artifacts\$RunId\session_rollups.jsonl"
$logStamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$logPath = Join-Path $RepoRoot "evaluation\artifacts\phase2_chunks_${EvalEnvironment}_$logStamp.log"

function Get-DoneCount {
    if (-not (Test-Path $rollupsPath)) { return 0 }
    $ids = @{}
    foreach ($line in Get-Content $rollupsPath) {
        if (-not $line.Trim()) { continue }
        try {
            $row = $line | ConvertFrom-Json
            $sid = [string]$row.session_id
            if ($sid) { $ids[$sid] = $true }
        } catch { }
    }
    return $ids.Count
}

Write-Host "Phase 2 chunked run | environment=$EvalEnvironment | api=$ApiBaseUrl | run=$RunId | chunk=$ChunkSize"

while ($true) {
    $done = Get-DoneCount
    $line = "{0} [{1}] sessions in checkpoint: {2} / {3}" -f (Get-Date -Format 'o'), $EvalEnvironment, $done, $CatalogTotal
    Write-Host $line
    try { Add-Content -Path $logPath -Value $line -ErrorAction Stop } catch { Write-Host "  [WARN] log append skipped: $_" }

    if ($done -ge $CatalogTotal) { break }

    $prevEap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    & python evaluation/run_1000_collection.py `
        --api-base-url $ApiBaseUrl `
        --run-id $RunId `
        --sessions-file $SessionsFile `
        --limit-sessions $ChunkSize `
        --resume `
        --store-full-answer `
        --use-batch-judge `
        --no-write-excel 2>&1 | Tee-Object -FilePath $logPath
    $chunkExit = $LASTEXITCODE
    $ErrorActionPreference = $prevEap
    if ($chunkExit -ne 0) {
        Write-Error "Chunk failed with exit code $LASTEXITCODE"
        exit $LASTEXITCODE
    }
}

$prevEap = $ErrorActionPreference
$ErrorActionPreference = 'Continue'
& python evaluation/run_1000_collection.py `
    --api-base-url $ApiBaseUrl `
    --run-id $RunId `
    --sessions-file $SessionsFile `
    --resume `
    --store-full-answer `
    --use-batch-judge `
    --write-excel 2>&1 | Tee-Object -FilePath $logPath -Append
$finalExit = $LASTEXITCODE
$ErrorActionPreference = $prevEap

if ($finalExit -ne 0) {
    Write-Error "Final Excel step failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}

Write-Host "Phase 2 chunked run finished ($EvalEnvironment)."
