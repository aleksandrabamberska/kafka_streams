package com.example.bigdata.powietrze.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Level2Result {
    private LocalDate reportDate;
    private String stationType;
    private String windSector;

    private double pm25Sum;
    private long pm25Count;
    private double avgPm25;

    private double boundaryLayerSum;
    private long boundaryLayerCount;
    private double avgBoundaryLayerHeightM;

    private String stationHllState;
    private long stationCountEstimated;

    public static Level2Result from(Level2Key key, Level2Aggregate aggregate) {
        return Level2Result.builder()
                .reportDate(key.getReportDate())
                .stationType(key.getStationType())
                .windSector(key.getWindSector())
                .pm25Sum(aggregate.getPm25Sum())
                .pm25Count(aggregate.getPm25Count())
                .avgPm25(aggregate.getPm25Count() > 0 ? aggregate.getPm25Sum() / aggregate.getPm25Count() : 0.0)
                .boundaryLayerSum(aggregate.getBoundaryLayerSum())
                .boundaryLayerCount(aggregate.getBoundaryLayerCount())
                .avgBoundaryLayerHeightM(
                        aggregate.getBoundaryLayerCount() > 0
                                ? aggregate.getBoundaryLayerSum() / aggregate.getBoundaryLayerCount()
                                : 0.0
                )
                .stationHllState(aggregate.getStationHllState())
                .stationCountEstimated(aggregate.getStationCountEstimated())
                .build();
    }
}