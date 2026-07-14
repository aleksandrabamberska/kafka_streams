package com.example.bigdata.powietrze.streams;

import com.example.bigdata.powietrze.streams.model.AirQualityEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class AirQualityTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        long timestamp = -1;

        final AirQualityEvent event = (AirQualityEvent) record.value();
        if (event != null && event.getTimestamp() != null) {
            timestamp = event.getTimestamp().toEpochMilli();
        }

        if (timestamp < 0) {
            if (previousTimestamp >= 0) {
                return previousTimestamp;
            } else {
                return System.currentTimeMillis();
            }
        }

        return timestamp;
    }
}
