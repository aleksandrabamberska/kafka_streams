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
public class Level2Key {
    private LocalDate reportDate;
    private String stationType;
    private String windSector;

    public static Level2Key from(EnrichedLevel1Result result) {
        return Level2Key.builder()
                .reportDate(result.getWindowStart().atZone(java.time.ZoneOffset.UTC).toLocalDate())
                .stationType(result.getStationType())
                .windSector(result.getWindSector())
                .build();
    }
}