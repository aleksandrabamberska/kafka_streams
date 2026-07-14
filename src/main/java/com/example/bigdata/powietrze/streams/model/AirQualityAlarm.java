package com.example.bigdata.powietrze.streams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AirQualityAlarm {
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant triggerEventTimestamp;

    private String alarmType;
    private String stationId;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant eventTimestamp;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowStart;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowEnd;

    private Double pm25;
    private Double maxPm25;
    private List<Double> matchingPm25Values;
    private Double threshold;
    private Long count;
    private String message;

    public static AirQualityAlarm immediate(AirQualityEvent event, double threshold) {
        return AirQualityAlarm.builder()
                .alarmType("IMMEDIATE_PM25_VERY_UNHEALTHY")
                .stationId(event.getStationId())
                .eventTimestamp(event.getTimestamp())
                .triggerEventTimestamp(event.getTimestamp())
                .pm25(event.getPm25())
                .threshold(threshold)
                .message("Single PM2.5 reading exceeded very unhealthy threshold")
                .build();
    }

    public static AirQualityAlarm counter(
            String stationId,
            Instant windowStart,
            Instant windowEnd,
            Instant triggerEventTimestamp,
            long count,
            double maxPm25,
            double threshold,
            List<Double> matchingPm25Values
    ) {
        return AirQualityAlarm.builder()
                .alarmType("COUNTER_PM25_UNHEALTHY_5MIN")
                .stationId(stationId)
                .eventTimestamp(triggerEventTimestamp)
                .triggerEventTimestamp(triggerEventTimestamp)
                .windowStart(windowStart)
                .windowEnd(windowEnd)
                .count(count)
                .maxPm25(maxPm25)
                .threshold(threshold)
                .matchingPm25Values(matchingPm25Values)
                .message("At least 3 PM2.5 readings exceeded unhealthy threshold in a 5-minute window")
                .build();
    }
}