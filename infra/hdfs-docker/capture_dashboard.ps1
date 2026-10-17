param(
    [Parameter(Mandatory=$true)][string]$ServingDir,
    [string]$Url = 'http://127.0.0.1:8503'
)
$ErrorActionPreference = 'Stop'
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$servingRoot = (Resolve-Path -LiteralPath $ServingDir).Path
$source = Get-Content -LiteralPath (Join-Path $servingRoot 'serving_manifest.json') -Encoding UTF8 -Raw | ConvertFrom-Json
if ($source.status -ne 'PASS') { throw 'Serving export must be PASS' }
$captureId = 'capture_' + [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$evidenceRoot = if ($source.evaluation_run_id) {
    Join-Path $repoRoot "outputs/hdfs_evaluations/$($source.evaluation_run_id)/evidence/$captureId"
} else {
    Join-Path $repoRoot "outputs/hdfs_models/$($source.model_run_id)/evidence/$captureId"
}
if (Test-Path -LiteralPath $evidenceRoot) { throw 'Evidence directory already exists' }
New-Item -ItemType Directory -Path $evidenceRoot | Out-Null
$browserProfile = Join-Path $evidenceRoot 'browser-profile'
$edgeBinary = Join-Path ${env:ProgramFiles(x86)} 'Microsoft/Edge/Application/msedge.exe'
if (-not (Test-Path -LiteralPath $edgeBinary)) { throw 'Microsoft Edge is missing; capture manually as documented' }
$script:socket = [System.Net.WebSockets.ClientWebSocket]::new()
$script:commandId = 0
$pictures = [System.Collections.Generic.List[object]]::new()
$browserProcess = $null
$status = 'FAIL'
$captureError = $null

function Invoke-Cdp([string]$Method, [hashtable]$Parameters = @{}) {
    $script:commandId++
    $currentId = $script:commandId
    $payload = @{id=$currentId; method=$Method; params=$Parameters} | ConvertTo-Json -Depth 30 -Compress
    $bytes = [Text.Encoding]::UTF8.GetBytes($payload)
    $timer = [Threading.CancellationTokenSource]::new(45000)
    try {
        $script:socket.SendAsync([ArraySegment[byte]]::new($bytes), [Net.WebSockets.WebSocketMessageType]::Text, $true, $timer.Token).GetAwaiter().GetResult() | Out-Null
        while ($true) {
            $buffer = [byte[]]::new(1048576)
            $message = [IO.MemoryStream]::new()
            try {
                do {
                    $received = $script:socket.ReceiveAsync([ArraySegment[byte]]::new($buffer), $timer.Token).GetAwaiter().GetResult()
                    if ($received.MessageType -eq [Net.WebSockets.WebSocketMessageType]::Close) { throw 'Browser closed the debugging session' }
                    $message.Write($buffer, 0, $received.Count)
                } until ($received.EndOfMessage)
                $reply = [Text.Encoding]::UTF8.GetString($message.ToArray()) | ConvertFrom-Json
            } finally { $message.Dispose() }
            if ($reply.id -eq $currentId) {
                if ($reply.error) { throw ($reply.error | ConvertTo-Json -Compress) }
                return $reply.result
            }
        }
    } finally { $timer.Dispose() }
}

function Invoke-JavaScript([string]$Expression) {
    $answer = Invoke-Cdp 'Runtime.evaluate' @{expression=$Expression; returnByValue=$true; awaitPromise=$true}
    if ($answer.exceptionDetails) { throw ($answer.exceptionDetails | ConvertTo-Json -Depth 10) }
    return $answer.result.value
}

function Wait-MainText([string]$Text) {
    $encoded = ConvertTo-Json -InputObject $Text -Compress
    for ($attempt = 0; $attempt -lt 90; $attempt++) {
        $present = Invoke-JavaScript "(document.querySelector('[data-testid=stMain]')?.innerText || '').includes($encoded)"
        if ($present) { Start-Sleep -Milliseconds 800; return }
        Start-Sleep -Milliseconds 500
    }
    throw "Page did not render expected text: $Text"
}

function Select-Page([string]$Name) {
    $encoded = ConvertTo-Json -InputObject $Name -Compress
    $clicked = Invoke-JavaScript "(()=>{const label=[...document.querySelectorAll('[data-testid=stSidebar] label')].find(x=>x.innerText.trim()===$encoded); if(!label)return false; label.click(); return true;})()"
    if (-not $clicked) { throw "Missing navigation: $Name" }
    Wait-MainText $Name
}

function Select-Demo([string]$Prefix, [string]$Expected) {
    $encoded = ConvertTo-Json -InputObject $Prefix -Compress
    if ($source.evaluation_run_id) {
        $chosen = $false
        for ($attempt = 0; $attempt -lt 90 -and -not $chosen; $attempt++) {
            $chosen = Click-DomElement "[...document.querySelectorAll('[data-testid=stMain] [data-testid=stRadio] label')].find(x=>x.innerText.trim().startsWith($encoded))"
            if (-not $chosen) { Start-Sleep -Milliseconds 500 }
        }
        if (-not $chosen) { throw "Missing real demo radio: $Prefix" }
    } else {
    $opened = $false
    for ($attempt = 0; $attempt -lt 90 -and -not $opened; $attempt++) {
        $opened = Click-DomElement "[...document.querySelectorAll('[data-testid=stSelectbox]')].find(x=>x.innerText.includes('Giỏ minh họa từ dữ liệu thật'))?.querySelector('button[aria-haspopup=listbox], [data-baseweb=select]')"
        if (-not $opened) { Start-Sleep -Milliseconds 500 }
    }
    if (-not $opened) { throw 'Missing demo selector' }
    Start-Sleep -Milliseconds 400
    $encoded = ConvertTo-Json -InputObject $Prefix -Compress
    $chosen = $false
    for ($attempt = 0; $attempt -lt 40 -and -not $chosen; $attempt++) {
        $chosen = Click-DomElement "[...document.querySelectorAll('[role=option]')].find(x=>x.innerText.trim().startsWith($encoded))"
        if (-not $chosen) { Start-Sleep -Milliseconds 500 }
    }
    if (-not $chosen) { throw "Missing real demo case: $Prefix" }
    }
    Start-Sleep -Milliseconds 700
    $applied = Click-DomElement "[...document.querySelectorAll('button')].find(x=>x.innerText.trim()==='Áp dụng giỏ minh họa')"
    if (-not $applied) { throw 'Missing apply-demo button' }
    Wait-MainText $Expected
}

function Click-DomElement([string]$Expression) {
    $position = Invoke-JavaScript "(()=>{const node=$Expression; if(!node)return null; node.scrollIntoView({block:'center',behavior:'instant'}); const r=node.getBoundingClientRect(); return {x:r.x+r.width/2,y:r.y+r.height/2};})()"
    if ($null -eq $position) { return $false }
    Start-Sleep -Milliseconds 150
    Invoke-Cdp 'Input.dispatchMouseEvent' @{type='mouseMoved';x=$position.x;y=$position.y} | Out-Null
    Invoke-Cdp 'Input.dispatchMouseEvent' @{type='mousePressed';x=$position.x;y=$position.y;button='left';clickCount=1} | Out-Null
    Invoke-Cdp 'Input.dispatchMouseEvent' @{type='mouseReleased';x=$position.x;y=$position.y;button='left';clickCount=1} | Out-Null
    return $true
}

function Save-Screenshot([string]$Name) {
    Invoke-JavaScript "(()=>{const main=document.querySelector('[data-testid=stMain]'); if(main)main.scrollTop=0; return true;})()" | Out-Null
    Start-Sleep -Milliseconds 700
    $errors = Invoke-JavaScript "[...document.querySelectorAll('[data-testid=stException],[data-testid=stAlertContainer]')].filter(x=>x.innerText.includes('Traceback')).map(x=>x.innerText)"
    if ($errors.Count -gt 0) { throw 'UI has a runtime traceback' }
    $layout = Invoke-Cdp 'Page.getLayoutMetrics'
    $height = [Math]::Min(3500, [Math]::Max(1000, $layout.cssContentSize.height))
    $shot = Invoke-Cdp 'Page.captureScreenshot' @{format='png'; captureBeyondViewport=$true; clip=@{x=0;y=0;width=1440;height=$height;scale=1}}
    $filename = $Name + '.png'
    [IO.File]::WriteAllBytes((Join-Path $evidenceRoot $filename), [Convert]::FromBase64String($shot.data))
    $pictures.Add(@{file=$filename; url=$Url; export_id=$source.export_id; publication_id=$source.publication_id; captured_at=[DateTime]::UtcNow.ToString('o'); kind='real browser screenshot'; max_height=3500})
}

try {
    $arguments = @('--headless=new', '--window-size=1440,1000', '--disable-gpu', '--no-first-run', '--no-default-browser-check', '--remote-debugging-port=0', ('--user-data-dir="' + $browserProfile + '"'), 'about:blank')
    $browserProcess = Start-Process -FilePath $edgeBinary -ArgumentList $arguments -WindowStyle Hidden -PassThru
    $portFile = Join-Path $browserProfile 'DevToolsActivePort'
    for ($attempt = 0; $attempt -lt 60 -and -not (Test-Path -LiteralPath $portFile); $attempt++) { Start-Sleep -Milliseconds 500 }
    if (-not (Test-Path -LiteralPath $portFile)) { throw 'Headless Edge did not expose a debugging port' }
    $debugPort = (Get-Content -LiteralPath $portFile)[0]
    $targets = Invoke-RestMethod "http://127.0.0.1:$debugPort/json"
    $target = $targets | Where-Object type -eq 'page' | Select-Object -First 1
    $script:socket.ConnectAsync([Uri]$target.webSocketDebuggerUrl, [Threading.CancellationToken]::None).GetAwaiter().GetResult() | Out-Null
    Invoke-Cdp 'Page.enable' | Out-Null
    Invoke-Cdp 'Emulation.setDeviceMetricsOverride' @{width=1440;height=1000;deviceScaleFactor=1;mobile=$false} | Out-Null
    Invoke-Cdp 'Page.navigate' @{url=$Url} | Out-Null
    if ($source.evaluation_run_id) { Wait-MainText 'Đánh giá độc lập basket completion' }
    else { Wait-MainText 'Mô hình pilot HDFS' }
    Wait-MainText $source.export_id
    Save-Screenshot '01_overview'
    Select-Page 'Hành vi mua sắm'
    Save-Screenshot '02_behavior'
    Select-Page 'Luật kết hợp'
    Save-Screenshot '03_rules'
    Select-Page 'Gợi ý sản phẩm'
    if ($source.evaluation_run_id) { Select-Demo 'Luật' 'sản phẩm • Hybrid' }
    else { Select-Demo 'Luật' 'sản phẩm từ luật kết hợp.' }
    Save-Screenshot '04_rule_recommendation'
    Select-Demo 'Fallback' 'Fallback: không có luật khớp'
    Save-Screenshot '05_fallback'
    Select-Page 'Hiệu năng hệ thống'
    Save-Screenshot '06_performance'
    $status = 'PASS'
} catch {
    $captureError = $_.Exception.Message
    try {
        $dom = Invoke-JavaScript "document.querySelector('[data-testid=stMain]')?.innerHTML || document.body.innerHTML"
        [IO.File]::WriteAllText((Join-Path $evidenceRoot 'runtime_debug.html'), $dom, [Text.Encoding]::UTF8)
    } catch { }
    throw
} finally {
    $manifest = @{status=$status; capture_id=$captureId; evaluation_run_id=$source.evaluation_run_id; model_run_id=$source.model_run_id; etl_run_id=$source.etl_run_id; publication_id=$source.publication_id; export_id=$source.export_id; created_at=[DateTime]::UtcNow.ToString('o'); browser='Microsoft Edge headless via CDP'; images=$pictures.ToArray(); error=$captureError}
    $manifest | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath (Join-Path $evidenceRoot 'evidence_manifest.json') -Encoding UTF8
    if ($script:socket.State -eq [Net.WebSockets.WebSocketState]::Open) {
        try { Invoke-Cdp 'Browser.close' | Out-Null } catch { }
    }
    $script:socket.Dispose()
    if ($browserProcess -and -not $browserProcess.HasExited) { Stop-Process -Id $browserProcess.Id -ErrorAction SilentlyContinue }
    Write-Output "EVIDENCE=$evidenceRoot"
}
