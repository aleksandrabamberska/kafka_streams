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
public class Level1Result {
    private String stationId;
    private String windSector;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowStart;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant windowEnd;

    private long count;
    private double avgPm25;
    private double avgNo2;
    private double maxPm10;
    private double avgBoundaryLayerHeightM;
}