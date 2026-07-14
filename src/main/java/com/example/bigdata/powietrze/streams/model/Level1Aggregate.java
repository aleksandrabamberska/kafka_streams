package com.example.bigdata.powietrze.streams.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Level1Aggregate {
    private long count;
    private double sumPm25;
    private double sumNo2;
    private double maxPm10;
    private double sumBoundaryLayerHeightM;

    public Level1Aggregate add(AirQualityEvent event) {
        count++;
        sumPm25 += event.getPm25();
        sumNo2 += event.getNo2();
        maxPm10 = Math.max(maxPm10, event.getPm10());
        sumBoundaryLayerHeightM += event.getBoundaryLayerHeightM();
        return this;
    }
}