package com.example.bigdata.powietrze.streams.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class CounterAlertAccumulator {
    private long matchingCount;
    private double maxPm25;
    private Instant lastMatchingEventTimestamp;
    private List<Double> matchingPm25Values = new ArrayList<>();

    public CounterAlertAccumulator add(AirQualityEvent event) {
        matchingCount++;
        maxPm25 = Math.max(maxPm25, event.getPm25());
        matchingPm25Values.add(event.getPm25());

        if (lastMatchingEventTimestamp == null || event.getTimestamp().isAfter(lastMatchingEventTimestamp)) {
            lastMatchingEventTimestamp = event.getTimestamp();
        }

        return this;
    }

    public long getMatchingCount() {
        return matchingCount;
    }

    public void setMatchingCount(long matchingCount) {
        this.matchingCount = matchingCount;
    }

    public double getMaxPm25() {
        return maxPm25;
    }

    public void setMaxPm25(double maxPm25) {
        this.maxPm25 = maxPm25;
    }

    public Instant getLastMatchingEventTimestamp() {
        return lastMatchingEventTimestamp;
    }

    public void setLastMatchingEventTimestamp(Instant lastMatchingEventTimestamp) {
        this.lastMatchingEventTimestamp = lastMatchingEventTimestamp;
    }

    public List<Double> getMatchingPm25Values() {
        return matchingPm25Values;
    }

    public void setMatchingPm25Values(List<Double> matchingPm25Values) {
        this.matchingPm25Values = matchingPm25Values;
    }
}