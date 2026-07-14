package com.example.bigdata.powietrze.streams.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedLevel1Result {
    private String stationId;
    private String stationName;
    private String district;
    private String stationType;
    private String windSector;

    private Instant windowStart;
    private Instant windowEnd;

    private long count;
    private double avgPm25;
    private double avgNo2;
    private double maxPm10;
    private double avgBoundaryLayerHeightM;

    public static EnrichedLevel1Result from(Level1Result result, AirStation station) {
        return EnrichedLevel1Result.builder()
                .stationId(result.getStationId())
                .stationName(station != null ? station.getName() : "UNKNOWN")
                .district(station != null ? station.getDistrict() : "UNKNOWN")
                .stationType(station != null ? station.getStationType() : "UNKNOWN")
                .windSector(result.getWindSector())
                .windowStart(result.getWindowStart())
                .windowEnd(result.getWindowEnd())
                .count(result.getCount())
                .avgPm25(result.getAvgPm25())
                .avgNo2(result.getAvgNo2())
                .maxPm10(result.getMaxPm10())
                .avgBoundaryLayerHeightM(result.getAvgBoundaryLayerHeightM())
                .build();
    }
}