package com.example.bigdata.powietrze.streams.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AirQualityEvent {
    private String stationId;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    private double pm25;
    private double pm10;
    private double no2;
    private double o3;
    private double co;
    private double so2;
    private double benzene;
    private double temperatureC;
    private double humidityPercent;
    private double windSpeedMs;
    private double windDirectionDeg;
    private double boundaryLayerHeightM;
}
