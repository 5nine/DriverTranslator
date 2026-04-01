param(
    [Parameter(Mandatory = $true)]
    [string[]]$DecoderIps,

    [int]$Port = 50002,

    [int]$DurationMinutes = 30,

    [int]$PollSeconds = 10,

    [string]$Command = "?",

    [int]$ConnectTimeoutMs = 2000,

    [int]$ReadIdleMs = 350,

    [int]$ReadMaxMs = 3000,

    [string]$OutputDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Expand-ListInput {
    param([string[]]$Items)
    $out = New-Object System.Collections.Generic.List[string]
    foreach ($item in ($Items | Where-Object { $_ -ne $null })) {
        foreach ($part in ($item -split ",")) {
            $v = $part.Trim()
            if (-not [string]::IsNullOrWhiteSpace($v)) {
                [void]$out.Add($v)
            }
        }
    }
    return @($out.ToArray())
}

function Convert-ToEscapedText {
    param([byte[]]$Bytes)
    if ($null -eq $Bytes -or $Bytes.Length -eq 0) { return "" }
    $sb = New-Object System.Text.StringBuilder
    foreach ($b in $Bytes) {
        switch ($b) {
            13 { [void]$sb.Append('\r') }
            10 { [void]$sb.Append('\n') }
            9 { [void]$sb.Append('\t') }
            default {
                if ($b -ge 32 -and $b -le 126) {
                    [void]$sb.Append([char]$b)
                } else {
                    [void]$sb.Append(('\x{0:X2}' -f $b))
                }
            }
        }
    }
    return $sb.ToString()
}

function Read-AllAvailable {
    param(
        [Parameter(Mandatory = $true)] [System.Net.Sockets.NetworkStream]$Stream,
        [int]$IdleMs = 350,
        [int]$MaxMs = 3000
    )

    $buffer = New-Object byte[] 8192
    $ms = New-Object System.IO.MemoryStream
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $lastDataAt = [System.Diagnostics.Stopwatch]::StartNew()
    $hadAny = $false

    while ($sw.ElapsedMilliseconds -lt $MaxMs) {
        if ($Stream.DataAvailable) {
            $n = $Stream.Read($buffer, 0, $buffer.Length)
            if ($n -gt 0) {
                $ms.Write($buffer, 0, $n)
                $hadAny = $true
                $lastDataAt.Restart()
                continue
            }
            break
        }
        if ($hadAny -and $lastDataAt.ElapsedMilliseconds -ge $IdleMs) {
            break
        }
        Start-Sleep -Milliseconds 20
    }
    return ,$ms.ToArray()
}

function Connect-Amx {
    param(
        [Parameter(Mandatory = $true)] [string]$Ip,
        [int]$Port = 50002,
        [int]$TimeoutMs = 2000
    )
    $client = New-Object System.Net.Sockets.TcpClient
    $ar = $client.BeginConnect($Ip, $Port, $null, $null)
    if (-not $ar.AsyncWaitHandle.WaitOne($TimeoutMs)) {
        $client.Close()
        throw "Connect timeout to $Ip`:$Port after ${TimeoutMs}ms"
    }
    $client.EndConnect($ar)
    return $client
}

$DecoderIps = Expand-ListInput -Items $DecoderIps
if ($DecoderIps.Count -eq 0) { throw "No decoder IPs supplied." }
if ($DurationMinutes -le 0) { throw "DurationMinutes must be > 0." }
if ($PollSeconds -le 0) { throw "PollSeconds must be > 0." }
if ([string]::IsNullOrWhiteSpace($Command)) { throw "Command must not be empty." }
if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $PSScriptRoot "amx-captures"
}
if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
}

$durationSeconds = $DurationMinutes * 60
$startAt = Get-Date
$endAt = $startAt.AddSeconds($durationSeconds)

Write-Host "=========================================="
Write-Host "AMX Persistent Soak Test"
Write-Host "=========================================="
Write-Host "Decoders : $($DecoderIps -join ', ')"
Write-Host "Command  : $Command"
Write-Host "Duration : $DurationMinutes minutes"
Write-Host "Poll     : every $PollSeconds second(s)"
Write-Host "Output   : $OutputDir"
Write-Host ""
[void](Read-Host "Press ENTER to start soak test")

$rows = New-Object System.Collections.Generic.List[object]
$clients = @{}
foreach ($ip in $DecoderIps) { $clients[$ip] = $null }

function Close-Client([System.Net.Sockets.TcpClient]$Client) {
    if ($null -ne $Client) {
        try { $Client.Close() } catch {}
    }
}

$cycle = 0
try {
    while ((Get-Date) -lt $endAt) {
        $cycle++
        foreach ($ip in $DecoderIps) {
            $ts = (Get-Date).ToString("o")
            $client = $clients[$ip]
            try {
                if ($null -eq $client -or -not $client.Connected) {
                    $client = Connect-Amx -Ip $ip -Port $Port -TimeoutMs $ConnectTimeoutMs
                    $clients[$ip] = $client
                }

                $stream = $client.GetStream()
                $wire = [System.Text.Encoding]::ASCII.GetBytes($Command + "`r")
                $stream.Write($wire, 0, $wire.Length)
                $stream.Flush()
                [byte[]]$respBytes = Read-AllAvailable -Stream $stream -IdleMs $ReadIdleMs -MaxMs $ReadMaxMs

                $rows.Add([PSCustomObject]@{
                    Timestamp = $ts
                    Cycle = $cycle
                    Mode = "persistent_soak"
                    Ip = $ip
                    Command = $Command
                    SentHex = (($wire | ForEach-Object { '{0:X2}' -f $_ }) -join ' ')
                    SentEscaped = (Convert-ToEscapedText -Bytes $wire)
                    ResponseLen = $respBytes.Length
                    ResponseHex = (($respBytes | ForEach-Object { '{0:X2}' -f $_ }) -join ' ')
                    ResponseEscaped = (Convert-ToEscapedText -Bytes $respBytes)
                    ResponseBase64 = [Convert]::ToBase64String($respBytes)
                    Error = $null
                }) | Out-Null
            } catch {
                $rows.Add([PSCustomObject]@{
                    Timestamp = $ts
                    Cycle = $cycle
                    Mode = "persistent_soak"
                    Ip = $ip
                    Command = $Command
                    SentHex = $null
                    SentEscaped = $null
                    ResponseLen = 0
                    ResponseHex = ""
                    ResponseEscaped = ""
                    ResponseBase64 = ""
                    Error = $_.Exception.Message
                }) | Out-Null
                Close-Client $client
                $clients[$ip] = $null
            }
        }

        $nextTick = (Get-Date).AddSeconds($PollSeconds)
        while ((Get-Date) -lt $nextTick -and (Get-Date) -lt $endAt) {
            $remaining = $endAt - (Get-Date)
            $status = ("`rTime remaining: {0:hh\:mm\:ss} | Cycle: {1}" -f $remaining, $cycle)
            Write-Host -NoNewline $status
            Start-Sleep -Seconds 1
        }
    }
} finally {
    Write-Host ""
    foreach ($ip in $DecoderIps) {
        Close-Client $clients[$ip]
    }
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$jsonPath = Join-Path $OutputDir ("amx-soak-{0}.json" -f $stamp)
$csvPath = Join-Path $OutputDir ("amx-soak-{0}.csv" -f $stamp)

$rows | ConvertTo-Json -Depth 6 | Set-Content -Path $jsonPath -Encoding UTF8
$rows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Soak test complete."
Write-Host "JSON: $jsonPath"
Write-Host "CSV : $csvPath"
Write-Host "Rows: $($rows.Count)"

