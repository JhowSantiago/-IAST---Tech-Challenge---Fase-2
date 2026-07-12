# Sobe Kafka local em modo KRaft (sem Zookeeper, sem Docker).
# Usa unidade virtual K: para evitar limite de linha de comando no Windows.
# Uso: powershell -ExecutionPolicy Bypass -File scripts/subir_kafka_local.ps1

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$KafkaDir = Join-Path $Root ".kafka"
$KafkaVersion = "3.7.0"
$KafkaName = "kafka_2.13-$KafkaVersion"
$KafkaHome = Join-Path $KafkaDir $KafkaName
$Archive = Join-Path $KafkaDir "$KafkaName.tgz"
$Url = "https://archive.apache.org/dist/kafka/$KafkaVersion/$KafkaName.tgz"
$LogDir = Join-Path $KafkaDir "kraft-logs"
$MetaFile = Join-Path $KafkaDir "kraft-meta.properties"
$ServerProps = Join-Path $KafkaDir "server-kraft.properties"
$Drive = "K:"

function Test-Port($Port) {
    return (Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue).TcpTestSucceeded
}

if (-not (Test-Path $KafkaHome)) {
    New-Item -ItemType Directory -Force -Path $KafkaDir | Out-Null
    Write-Host "Baixando Kafka $KafkaVersion..."
    Invoke-WebRequest -Uri $Url -OutFile $Archive
    tar -xzf $Archive -C $KafkaDir
}

$env:KAFKA_HEAP_OPTS = "-Xmx512M -Xms512M"

if (-not (Test-Path $ServerProps)) {
    $template = Get-Content (Join-Path $KafkaHome "config\kraft\server.properties") -Raw
    $logPath = ($LogDir -replace '\\', '/')
    $template = $template -replace "log.dirs=/tmp/kraft-combined-logs", "log.dirs=$logPath"
    Set-Content -Path $ServerProps -Value $template -Encoding UTF8
}

subst $Drive $KafkaDir 2>$null | Out-Null

if (-not (Test-Path $MetaFile)) {
    $uuid = & "$Drive\kafka_2.13-$KafkaVersion\bin\windows\kafka-storage.bat" random-uuid
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    & "$Drive\kafka_2.13-$KafkaVersion\bin\windows\kafka-storage.bat" format -t $uuid -c "$Drive\server-kraft.properties"
    Set-Content -Path $MetaFile -Value "formatted=$uuid"
}

if (-not (Test-Port 9092)) {
    Write-Host "Iniciando Kafka (KRaft)..."
    Start-Process -FilePath "cmd.exe" -ArgumentList @(
        "/c",
        "set KAFKA_HEAP_OPTS=-Xmx512M -Xms512M && $Drive\kafka_2.13-$KafkaVersion\bin\windows\kafka-server-start.bat $Drive\server-kraft.properties"
    ) -WindowStyle Hidden

    for ($i = 1; $i -le 30; $i++) {
        if (Test-Port 9092) {
            Write-Host "Kafka disponível em localhost:9092"
            exit 0
        }
        Start-Sleep -Seconds 2
    }
}

if (Test-Port 9092) {
    Write-Host "Kafka já em execução em localhost:9092"
    exit 0
}

Write-Host "Falha ao subir Kafka"
exit 1
