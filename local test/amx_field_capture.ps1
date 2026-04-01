param(
    [Parameter(Mandatory = $true)]
    [string[]]$DecoderIps,

    [int]$Port = 50002,

    [int]$ConnectTimeoutMs = 2000,

    [int]$ReadIdleMs = 350,

    [int]$ReadMaxMs = 3000,

    [string[]]$Commands = @("?", "getStatus"),

    [switch]$RunPersistent,

    [switch]$RunNonPersistent,

    [switch]$ProbeSingleConnection,

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

$DecoderIps = Expand-ListInput -Items $DecoderIps
$Commands = Expand-ListInput -Items $Commands

if ($DecoderIps.Count -eq 0) {
    throw "No decoder IPs supplied. Use -DecoderIps with one or more IP addresses."
}
if ($Commands.Count -eq 0) {
    throw "No commands supplied. Use -Commands with one or more AMX commands."
}

if (-not $RunPersistent -and -not $RunNonPersistent) {
    $RunPersistent = $true
    $RunNonPersistent = $true
}

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $OutputDir = Join-Path $PSScriptRoot "amx-captures"
}

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
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

function Send-AndCapture {
    param(
        [Parameter(Mandatory = $true)] [System.Net.Sockets.TcpClient]$Client,
        [Parameter(Mandatory = $true)] [string]$Command,
        [int]$ReadIdleMs = 350,
        [int]$ReadMaxMs = 3000
    )

    $stream = $Client.GetStream()
    $wire = [System.Text.Encoding]::ASCII.GetBytes($Command + "`r")
    $stream.Write($wire, 0, $wire.Length)
    $stream.Flush()

    [byte[]]$respBytes = Read-AllAvailable -Stream $stream -IdleMs $ReadIdleMs -MaxMs $ReadMaxMs
    [PSCustomObject]@{
        Command = $Command
        SentHex = (($wire | ForEach-Object { '{0:X2}' -f $_ }) -join ' ')
        SentEscaped = (Convert-ToEscapedText -Bytes $wire)
        ResponseBytes = $respBytes
        ResponseLen = $respBytes.Length
        ResponseHex = (($respBytes | ForEach-Object { '{0:X2}' -f $_ }) -join ' ')
        ResponseEscaped = (Convert-ToEscapedText -Bytes $respBytes)
        ResponseBase64 = [Convert]::ToBase64String($respBytes)
    }
}

function Test-SecondConnectionWhileOpen {
    param(
        [Parameter(Mandatory = $true)] [string]$Ip,
        [int]$Port = 50002,
        [int]$TimeoutMs = 1200
    )
    try {
        $c = Connect-Amx -Ip $Ip -Port $Port -TimeoutMs $TimeoutMs
        try {
            $ok = $c.Connected
        } finally {
            $c.Close()
        }
        return [PSCustomObject]@{ SecondConnectionSucceeded = [bool]$ok; Error = $null }
    } catch {
        return [PSCustomObject]@{ SecondConnectionSucceeded = $false; Error = $_.Exception.Message }
    }
}

function Run-NonPersistent {
    param(
        [string]$Ip,
        [string[]]$Commands,
        [int]$Port,
        [int]$ConnectTimeoutMs,
        [int]$ReadIdleMs,
        [int]$ReadMaxMs
    )

    $rows = @()
    foreach ($cmd in $Commands) {
        $client = $null
        try {
            $client = Connect-Amx -Ip $Ip -Port $Port -TimeoutMs $ConnectTimeoutMs
            $capture = Send-AndCapture -Client $client -Command $cmd -ReadIdleMs $ReadIdleMs -ReadMaxMs $ReadMaxMs
            $rows += [PSCustomObject]@{
                Mode = "non_persistent"
                Ip = $Ip
                Command = $capture.Command
                SentHex = $capture.SentHex
                SentEscaped = $capture.SentEscaped
                ResponseLen = $capture.ResponseLen
                ResponseHex = $capture.ResponseHex
                ResponseEscaped = $capture.ResponseEscaped
                ResponseBase64 = $capture.ResponseBase64
                Error = $null
            }
        } catch {
            $rows += [PSCustomObject]@{
                Mode = "non_persistent"
                Ip = $Ip
                Command = $cmd
                SentHex = $null
                SentEscaped = $null
                ResponseLen = 0
                ResponseHex = ""
                ResponseEscaped = ""
                ResponseBase64 = ""
                Error = $_.Exception.Message
            }
        } finally {
            if ($null -ne $client) { $client.Close() }
        }
    }
    return $rows
}

function Run-Persistent {
    param(
        [string]$Ip,
        [string[]]$Commands,
        [int]$Port,
        [int]$ConnectTimeoutMs,
        [int]$ReadIdleMs,
        [int]$ReadMaxMs,
        [switch]$ProbeSingleConnection
    )

    $rows = @()
    $client = $null
    try {
        $client = Connect-Amx -Ip $Ip -Port $Port -TimeoutMs $ConnectTimeoutMs

        if ($ProbeSingleConnection) {
            $probe = Test-SecondConnectionWhileOpen -Ip $Ip -Port $Port -TimeoutMs 1200
            $rows += [PSCustomObject]@{
                Mode = "persistent_probe"
                Ip = $Ip
                Command = "<probe second connection while first open>"
                SentHex = $null
                SentEscaped = $null
                ResponseLen = 0
                ResponseHex = ""
                ResponseEscaped = ""
                ResponseBase64 = ""
                Error = $probe.Error
                SecondConnectionSucceeded = $probe.SecondConnectionSucceeded
            }
        }

        foreach ($cmd in $Commands) {
            try {
                $capture = Send-AndCapture -Client $client -Command $cmd -ReadIdleMs $ReadIdleMs -ReadMaxMs $ReadMaxMs
                $rows += [PSCustomObject]@{
                    Mode = "persistent"
                    Ip = $Ip
                    Command = $capture.Command
                    SentHex = $capture.SentHex
                    SentEscaped = $capture.SentEscaped
                    ResponseLen = $capture.ResponseLen
                    ResponseHex = $capture.ResponseHex
                    ResponseEscaped = $capture.ResponseEscaped
                    ResponseBase64 = $capture.ResponseBase64
                    Error = $null
                }
            } catch {
                $rows += [PSCustomObject]@{
                    Mode = "persistent"
                    Ip = $Ip
                    Command = $cmd
                    SentHex = $null
                    SentEscaped = $null
                    ResponseLen = 0
                    ResponseHex = ""
                    ResponseEscaped = ""
                    ResponseBase64 = ""
                    Error = $_.Exception.Message
                }
            }
        }
    } catch {
        $rows += [PSCustomObject]@{
            Mode = "persistent"
            Ip = $Ip
            Command = "<connect>"
            SentHex = $null
            SentEscaped = $null
            ResponseLen = 0
            ResponseHex = ""
            ResponseEscaped = ""
            ResponseBase64 = ""
            Error = $_.Exception.Message
        }
    } finally {
        if ($null -ne $client) { $client.Close() }
    }

    return $rows
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$allRows = @()

Write-Host "AMX field capture starting..."
Write-Host "OutputDir: $OutputDir"
Write-Host "Decoders: $($DecoderIps -join ', ')"
Write-Host "Commands: $($Commands -join ' | ')"
Write-Host "Modes: $(
    @(
        if ($RunNonPersistent) { 'non_persistent' }
        if ($RunPersistent) { 'persistent' }
    ) -join ', '
)"

foreach ($ip in $DecoderIps) {
    if ($RunNonPersistent) {
        $allRows += Run-NonPersistent -Ip $ip -Commands $Commands -Port $Port -ConnectTimeoutMs $ConnectTimeoutMs -ReadIdleMs $ReadIdleMs -ReadMaxMs $ReadMaxMs
    }
    if ($RunPersistent) {
        $allRows += Run-Persistent -Ip $ip -Commands $Commands -Port $Port -ConnectTimeoutMs $ConnectTimeoutMs -ReadIdleMs $ReadIdleMs -ReadMaxMs $ReadMaxMs -ProbeSingleConnection:$ProbeSingleConnection
    }
}

$jsonPath = Join-Path $OutputDir ("amx-capture-{0}.json" -f $stamp)
$csvPath = Join-Path $OutputDir ("amx-capture-{0}.csv" -f $stamp)

$allRows | ConvertTo-Json -Depth 6 | Set-Content -Path $jsonPath -Encoding UTF8
$allRows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8

Write-Host ""
Write-Host "Done."
Write-Host "JSON: $jsonPath"
Write-Host "CSV : $csvPath"
Write-Host "Rows: $($allRows.Count)"

