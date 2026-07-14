param(
    [string]$ComposeDir = (Get-Location).Path,
    [string]$Topic = "powietrze-slownik-stacje",
    [string]$BootstrapServer = "kafka-1:9092",
    [string]$DictionaryPath = "$PSScriptRoot\air_stations.json"
)

$stations = Get-Content -Raw -LiteralPath $DictionaryPath | ConvertFrom-Json
$lines = foreach ($station in $stations) {
    $json = $station | ConvertTo-Json -Compress
    "$($station.stationId)|$json"
}

$lines | docker compose --project-directory $ComposeDir exec -T kafka-1 `
    /opt/kafka/bin/kafka-console-producer.sh `
    --topic $Topic `
    --bootstrap-server $BootstrapServer `
    --property parse.key=true `
    --property key.separator="|"
