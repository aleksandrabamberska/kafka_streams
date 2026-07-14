package com.example.bigdata.powietrze.streams.model;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.Base64;

@Data
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Level2Aggregate {
    private double pm25Sum;
    private long pm25Count;

    private double boundaryLayerSum;
    private long boundaryLayerCount;

    private String stationHllState;
    private int hllLog2m;

    public Level2Aggregate(int hllLog2m) {
        this.hllLog2m = hllLog2m;
    }

    public Level2Aggregate add(EnrichedLevel1Result result) {
        pm25Sum += result.getAvgPm25() * result.getCount();
        pm25Count += result.getCount();

        boundaryLayerSum += result.getAvgBoundaryLayerHeightM() * result.getCount();
        boundaryLayerCount += result.getCount();

        HyperLogLog stationSketch = restoreStationSketch();
        stationSketch.offer(result.getStationId());

        try {
            this.stationHllState = Base64.getEncoder().encodeToString(stationSketch.getBytes());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize HyperLogLog state", e);
        }

        return this;
    }

    public long getStationCountEstimated() {
        return restoreStationSketch().cardinality();
    }

    private HyperLogLog restoreStationSketch() {
        if (stationHllState == null || stationHllState.isBlank()) {
            return new HyperLogLog(hllLog2m);
        }

        try {
            byte[] serialized = Base64.getDecoder().decode(stationHllState);
            return HyperLogLog.Builder.build(serialized);
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalStateException("Cannot deserialize HyperLogLog state", e);
        }
    }
}