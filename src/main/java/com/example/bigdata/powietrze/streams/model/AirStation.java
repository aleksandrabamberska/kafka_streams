package com.example.bigdata.powietrze.streams.model;

import lombok.Data;

@Data
public class AirStation {
    private String stationId;
    private String name;
    private double lat;
    private double lon;
    private String district;
    private String stationType;
}
