package com.example.bigdata.powietrze.streams;

import com.example.bigdata.powietrze.streams.model.*;
import com.example.bigdata.powietrze.streams.serde.JsonPOJOSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.InputStream;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PowietrzeStreamsApp {
    public static void main(String[] args) throws Exception {
        Properties appCfg = new Properties();
        try (InputStream is = PowietrzeStreamsApp.class.getResourceAsStream("/application.properties")) {
            if (is != null) {
                appCfg.load(is);
            }
        }

        for (String arg : args) {
            if (arg.startsWith("--")) {
                String[] parts = arg.substring(2).split("=", 2);
                if (parts.length == 2) {
                    appCfg.setProperty(parts[0], parts[1]);
                }
            }
        }

        String bootstrapServers = appCfg.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String inputTopic = appCfg.getProperty("kafka.topic.powietrze", "powietrze-odczyty");
        String stationsTopic = appCfg.getProperty("kafka.topic.air.stations", "powietrze-slownik-stacje");
        String level1Topic = appCfg.getProperty("kafka.topic.level1", "powietrze-poziom1");
        String level2Topic = appCfg.getProperty("kafka.topic.level2", "powietrze-poziom2");
        String alertsTopic = appCfg.getProperty("kafka.topic.alerts", "powietrze-alarmy");
        String applicationId = appCfg.getProperty("streams.application.id", "powietrze-streams-app");
        String commitIntervalMs = appCfg.getProperty("streams.commit.interval.ms", "10000");
        String offsetReset = appCfg.getProperty("streams.auto.offset.reset", "earliest");
        String processingGuarantee = appCfg.getProperty("streams.processing.guarantee", "exactly_once_v2");
        String isolationLevel = appCfg.getProperty("streams.isolation.level", "read_committed");
        long windowSizeMs = Long.parseLong(appCfg.getProperty("streams.window.size.ms", "60000"));
        long graceMs = Long.parseLong(appCfg.getProperty("streams.window.grace.ms", "35000"));
        double unhealthyPm25Threshold = Double.parseDouble(appCfg.getProperty("alerts.pm25.unhealthy.threshold", "55"));
        double veryUnhealthyPm25Threshold = Double.parseDouble(appCfg.getProperty("alerts.pm25.very.unhealthy.threshold", "150"));
        long counterAlertWindowMs = Long.parseLong(appCfg.getProperty("alerts.counter.window.ms", "300000"));
        long counterAlertGraceMs = Long.parseLong(appCfg.getProperty("alerts.counter.grace.ms", "30000"));
        long counterAlertMinCount = Long.parseLong(appCfg.getProperty("alerts.counter.min.count", "3"));
        int hllLog2m = Integer.parseInt(appCfg.getProperty("level2.hll.log2m", "12"));
        if (hllLog2m < 4 || hllLog2m > 30) {
            throw new IllegalArgumentException("level2.hll.log2m must be between 4 and 30");
        }


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
                AirQualityTimestampExtractor.class);


        final Serde<AirQualityEvent> airQualitySerde = new JsonPOJOSerde<>(AirQualityEvent.class);
        final Serde<AirStation> airStationSerde = new JsonPOJOSerde<>(AirStation.class);
        final Serde<Level1Aggregate> level1AggregateSerde = new JsonPOJOSerde<>(Level1Aggregate.class);
        final Serde<EnrichedLevel1Result> enrichedLevel1ResultSerde = new JsonPOJOSerde<>(EnrichedLevel1Result.class);
        final Serde<Level2Key> level2KeySerde = new JsonPOJOSerde<>(Level2Key.class);
        final Serde<Level2Aggregate> level2AggregateSerde = new JsonPOJOSerde<>(Level2Aggregate.class);
        final Serde<AirQualityAlarm> airQualityAlarmSerde = new JsonPOJOSerde<>(AirQualityAlarm.class);
        final Serde<CounterAlertAccumulator> counterAlertAccumulatorSerde =
                new JsonPOJOSerde<>(CounterAlertAccumulator.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, AirQualityEvent> events = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), airQualitySerde)
        );

        KStream<String, AirQualityAlarm> immediateAlerts = events
                .filter((key, event) -> event.getPm25() > veryUnhealthyPm25Threshold)
                .map((key, event) -> KeyValue.pair(
                        immediateAlarmKey(event),
                        AirQualityAlarm.immediate(event, veryUnhealthyPm25Threshold)
                ));

        immediateAlerts.to(alertsTopic, Produced.with(Serdes.String(), airQualityAlarmSerde));

        KStream<String, AirQualityEvent> counterAlertCandidates = events
                .filter((key, event) ->
                        event.getStationId() != null
                                && !event.getStationId().isBlank()
                                && event.getPm25() > unhealthyPm25Threshold
                )
                .selectKey((key, event) -> event.getStationId());

        KStream<String, AirQualityAlarm> counterAlerts = counterAlertCandidates
                .groupByKey(Grouped.with(Serdes.String(), airQualitySerde))
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(
                        Duration.ofMillis(counterAlertWindowMs),
                        Duration.ofMillis(counterAlertGraceMs)
                ))
                .aggregate(
                        CounterAlertAccumulator::new,
                        (stationId, event, accumulator) -> accumulator.add(event),
                        Materialized.with(Serdes.String(), counterAlertAccumulatorSerde)
                )
                .toStream()
                .filter((windowedStationId, accumulator) ->
                        accumulator.getMatchingCount() >= counterAlertMinCount)
                .map((windowedStationId, accumulator) -> KeyValue.pair(
                        counterAlarmKey(windowedStationId),
                        AirQualityAlarm.counter(
                                windowedStationId.key(),
                                windowedStationId.window().startTime(),
                                windowedStationId.window().endTime(),
                                accumulator.getLastMatchingEventTimestamp(),
                                accumulator.getMatchingCount(),
                                accumulator.getMaxPm25(),
                                unhealthyPm25Threshold,
                                accumulator.getMatchingPm25Values()
                        )
                ));

        counterAlerts.to(alertsTopic, Produced.with(Serdes.String(), airQualityAlarmSerde));

        KTable<Windowed<String>, Level1Aggregate> level1Table = events
                .groupBy(
                        (key, event) -> event.getStationId() + "|" + windSector(event.getWindDirectionDeg()),
                        Grouped.with(Serdes.String(), airQualitySerde)
                )
                .windowedBy(TimeWindows.ofSizeAndGrace(
                        Duration.ofMillis(windowSizeMs),
                        Duration.ofMillis(graceMs)
                ))
                .aggregate(
                        () -> new Level1Aggregate(0, 0.0, 0.0, Double.NEGATIVE_INFINITY, 0.0),
                        (aggKey, event, aggregate) -> aggregate.add(event),
                        Materialized.with(Serdes.String(), level1AggregateSerde)
                );

        KStream<String, Level1Result> level1LiveResults = level1Table
                .toStream()
                .map((windowedKey, aggregate) -> toLevel1Record(windowedKey, aggregate));

        KStream<String, Level1Result> level1FinalResults = level1Table
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .map((windowedKey, aggregate) -> toLevel1Record(windowedKey, aggregate));

        GlobalKTable<String, AirStation> stations = builder.globalTable(
                stationsTopic,
                Consumed.with(Serdes.String(), airStationSerde)
        );

        KStream<String, EnrichedLevel1Result> enrichedLevel1Results = level1LiveResults.leftJoin(
                stations,
                (key, result) -> result.getStationId(),
                EnrichedLevel1Result::from
        );

        KStream<String, String> enrichedLevel1Text = enrichedLevel1Results
                .mapValues(PowietrzeStreamsApp::formatOutputLine);

        enrichedLevel1Text.peek((key, line) -> System.out.println(line));

        enrichedLevel1Text.to(level1Topic, Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, EnrichedLevel1Result> enrichedLevel1FinalResults = level1FinalResults.leftJoin(
                stations,
                (key, result) -> result.getStationId(),
                EnrichedLevel1Result::from
        );

        KStream<Level2Key, EnrichedLevel1Result> level2Input = enrichedLevel1FinalResults
                .map((key, result) -> KeyValue.pair(
                        Level2Key.from(result),
                        result
                ));

        KTable<Level2Key, Level2Aggregate> level2Aggregates = level2Input
                .groupByKey(Grouped.with(level2KeySerde, enrichedLevel1ResultSerde))
                .aggregate(
                        () -> new Level2Aggregate(hllLog2m),
                        (key, result, aggregate) -> aggregate.add(result),
                        Materialized.with(level2KeySerde, level2AggregateSerde)
                );

        KStream<Level2Key, Level2Result> level2Results = level2Aggregates
                .toStream()
                .map((key, aggregate) -> KeyValue.pair(key, Level2Result.from(key, aggregate)));

        level2Results.peek((key, result) -> System.out.println(result));
        KStream<String, String> level2SinkRecords = level2Results.map((key, result) ->
                KeyValue.pair(
                        toConnectKeyJson(key),
                        toConnectValueJson(result)
                )
        );

        level2SinkRecords.to(level2Topic, Produced.with(Serdes.String(), Serdes.String()));

        try (KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
            CountDownLatch latch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                streams.close();
                latch.countDown();
            }));

            streams.start();
            latch.await();
        }
    }

    private static String formatOutputLine(EnrichedLevel1Result result) {
        return String.format(
                "stationId=%s stationName=%s district=%s stationType=%s windowStart=%s windowEnd=%s count=%d avgPm25=%.2f avgNo2=%.2f maxPm10=%.2f avgBoundaryLayerHeightM=%.2f",
                result.getStationId(),
                result.getStationName(),
                result.getDistrict(),
                result.getStationType(),
                result.getWindowStart(),
                result.getWindowEnd(),
                result.getCount(),
                result.getAvgPm25(),
                result.getAvgNo2(),
                result.getMaxPm10(),
                result.getAvgBoundaryLayerHeightM()
        );
    }

    private static KeyValue<String, Level1Result> toLevel1Record(Windowed<String> windowedKey, Level1Aggregate aggregate) {
        String[] keyParts = windowedKey.key().split("\\|", 2);
        String stationId = keyParts[0];
        String windSector = keyParts.length > 1 ? keyParts[1] : "UNKNOWN";

        Level1Result result = Level1Result.builder()
                .stationId(stationId)
                .windSector(windSector)
                .windowStart(windowedKey.window().startTime())
                .windowEnd(windowedKey.window().endTime())
                .count(aggregate.getCount())
                .avgPm25(aggregate.getSumPm25() / aggregate.getCount())
                .avgNo2(aggregate.getSumNo2() / aggregate.getCount())
                .maxPm10(aggregate.getMaxPm10())
                .avgBoundaryLayerHeightM(aggregate.getSumBoundaryLayerHeightM() / aggregate.getCount())
                .build();

        String outputKey = stationId + "|" + windSector + "|" + windowedKey.window().startTime() + "|" + windowedKey.window().endTime();
        return KeyValue.pair(outputKey, result);
    }

    private static String immediateAlarmKey(AirQualityEvent event) {
        return "IMMEDIATE_PM25_VERY_UNHEALTHY|"
                + event.getStationId()
                + "|"
                + event.getTimestamp();
    }

    private static String counterAlarmKey(Windowed<String> windowedStationId) {
        return "COUNTER_PM25_UNHEALTHY_5MIN|"
                + windowedStationId.key()
                + "|"
                + windowedStationId.window().startTime()
                + "|"
                + windowedStationId.window().endTime();
    }

    private static String windSector(double windDirectionDeg) {
        if (windDirectionDeg >= 315 || windDirectionDeg < 45) {
            return "N";
        } else if (windDirectionDeg < 135) {
            return "E";
        } else if (windDirectionDeg < 225) {
            return "S";
        } else {
            return "W";
        }
    }

    private static final ObjectMapper CONNECT_MAPPER = new ObjectMapper();

    private static String toConnectKeyJson(Level2Key key) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("report_date", key.getReportDate().toString());
        payload.put("station_type", key.getStationType());
        payload.put("wind_sector", key.getWindSector());

        return connectEnvelope(
                "com.example.bigdata.powietrze.level2.key",
                List.of(
                        field("report_date", "string"),
                        field("station_type", "string"),
                        field("wind_sector", "string")
                ),
                payload
        );
    }

    private static String toConnectValueJson(Level2Result result) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("report_date", result.getReportDate().toString());
        payload.put("station_type", result.getStationType());
        payload.put("wind_sector", result.getWindSector());
        payload.put("pm25_sum", result.getPm25Sum());
        payload.put("pm25_count", result.getPm25Count());
        payload.put("avg_pm25", result.getAvgPm25());
        payload.put("boundary_layer_sum", result.getBoundaryLayerSum());
        payload.put("boundary_layer_count", result.getBoundaryLayerCount());
        payload.put("avg_boundary_layer_height_m", result.getAvgBoundaryLayerHeightM());
        payload.put("station_hll_state", result.getStationHllState());
        payload.put("station_count_estimated", result.getStationCountEstimated());

        return connectEnvelope(
                "com.example.bigdata.powietrze.level2.value",
                List.of(
                        field("report_date", "string"),
                        field("station_type", "string"),
                        field("wind_sector", "string"),
                        field("pm25_sum", "double"),
                        field("pm25_count", "int64"),
                        field("avg_pm25", "double"),
                        field("boundary_layer_sum", "double"),
                        field("boundary_layer_count", "int64"),
                        field("avg_boundary_layer_height_m", "double"),
                        field("station_hll_state", "string"),
                        field("station_count_estimated", "int64")
                ),
                payload
        );
    }

    private static Map<String, Object> field(String name, String type) {
        Map<String, Object> field = new LinkedHashMap<>();
        field.put("field", name);
        field.put("type", type);
        field.put("optional", false);
        return field;
    }

    private static String connectEnvelope(String schemaName, List<Map<String, Object>> fields, Map<String, Object> payload) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "struct");
        schema.put("optional", false);
        schema.put("name", schemaName);
        schema.put("fields", fields);

        Map<String, Object> root = new LinkedHashMap<>();
        root.put("schema", schema);
        root.put("payload", payload);

        try {
            return CONNECT_MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot build Kafka Connect JSON envelope", e);
        }
    }
}
