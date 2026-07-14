# Powietrze Streams App


## Tworzenie tematów 

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic powietrze-alarmy --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 1
```

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic powietrze-odczyty --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 1
```

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --topic powietrze-slownik-stacje --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 1 --config cleanup.policy=compact
```

Wyniki Poziomu 1 zapisywane są do tematu `powietrze-poziom1`, a drugiego do `powietrze-poziom2`.

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic powietrze-poziom1 --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 1 --config cleanup.policy=compact,delete
```

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic powietrze-poziom2 --bootstrap-server kafka-1:9092 --replication-factor 3 --partitions 1 --config cleanup.policy=compact,delete
```

## Odczyt wyników Poziomu 1

```powershell
docker compose exec -it kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic powietrze-poziom1 --from-beginning --property print.key=true --property key.separator=" | "
```
## Ładowanie słownika

```powershell
.\scripts\load-air-stations.ps1
```

Konfiguracja znajduje sie w `src/main/resources/application.properties`

##  Konfiguracja ujścia

### Utworzenie użyttkownika i bazy danych 

```powershell
docker exec -it mysql mysql -uroot -ppassword
```

```powershell
CREATE USER 'streamuser'@'%' IDENTIFIED BY 'stream'; 
CREATE DATABASE IF NOT EXISTS streamdb CHARACTER SET utf8; 
GRANT ALL ON streamdb.* TO 'streamuser'@'%'; 
exit;
```

### Utworzenie tabeli 

```powershell
docker exec -it mysql mysql -u streamuser -pstream streamdb
```

```powershell
CREATE TABLE powietrze_poziom2 (
    report_date DATE NOT NULL,
    station_type VARCHAR(64) NOT NULL,
    wind_sector VARCHAR(1) NOT NULL,
    pm25_sum DOUBLE NOT NULL,
    pm25_count BIGINT NOT NULL,
    avg_pm25 DOUBLE NOT NULL,
    boundary_layer_sum DOUBLE NOT NULL,
    boundary_layer_count BIGINT NOT NULL,
    avg_boundary_layer_height_m DOUBLE NOT NULL,
    station_hll_state TEXT NOT NULL,
    station_count_estimated BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
       ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, station_type, wind_sector)
);
```

### Sterowniki JDBC i Kafka Connect

```powershell
Copy-Item .\src\main\java\com\example\bigdata\powietrze\streams\connect\connect-jdbc-sink.properties ..\..\BigData26\shared_workspace\ -Force
Copy-Item .\src\main\java\com\example\bigdata\powietrze\streams\connect\connect-standalone.properties ..\..\BigData26\shared_workspace\ -Force
```

```powershell
docker exec --workdir /home/appuser -it kafka-1 bash
```

```powershell
wget -P /opt/kafka/libs https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.5.0/mysql-connector-j-9.5.0.jar
mkdir -p /opt/kafka/plugin
wget -P /opt/kafka/plugin https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.8.2/kafka-connect-jdbc-10.8.2.jar
export PATH=$PATH:/opt/kafka/bin
connect-standalone.sh /opt/workspace/connect-standalone.properties /opt/workspace/connect-jdbc-sink.properties
```

## Usuwanie stanu
```powershell
docker compose --profile jupyter --profile flink --profile spark --profile kafka down -v 
```