$maxWaits = 200
$waitCount = 0

Write-Host "Monitoring pipeline progress..." -ForegroundColor Green

while ($waitCount -lt $maxWaits) {
    $waitCount++
    
    try {
        $response = Invoke-WebRequest -Uri "http://localhost:8001/api/fleet/trend?days=10" -Method Get -UseBasicParsing -ErrorAction SilentlyContinue
        
        if ($response) {
            $json = $response.Content | ConvertFrom-Json
            $dates = $json.rows | Select-Object -ExpandProperty event_date | Sort-Object | Get-Unique
            $latestDate = $dates | Select-Object -Last 1
            Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Latest: $latestDate | Check $waitCount/200" -ForegroundColor Cyan
            
            if ($latestDate -match "2026-03-1[0-9]") {
                Write-Host "Pipeline Complete! Data: $latestDate" -ForegroundColor Green
                exit 0
            }
        }
    } catch {
        Write-Host "[$(Get-Date -Format 'HH:mm:ss')] Processing... Check $waitCount/200" -ForegroundColor Yellow
    }
    
    Start-Sleep -Seconds 30
}

Write-Host "Timeout. Pipeline may still be running." -ForegroundColor Yellow
