$uri = "http://localhost:11434/api/generate"
$body = @{
    model = "deepseek-coder"
    prompt = "What is 2+2?"
    stream = $false
} | ConvertTo-Json

Write-Host "Testing Ollama at $uri"
Write-Host "Request body: $body"

try {
    $response = Invoke-WebRequest -Uri $uri -Method POST -Body $body -ContentType 'application/json' -TimeoutSec 10
    Write-Host "Ollama responded with status $($response.StatusCode)"
    Write-Host "Response: $($response.Content)"
} catch {
    Write-Host "Ollama error: $($_.Exception.Message)"
}
