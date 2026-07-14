package com.example.bigdata.powietrze.streams.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class JsonPOJOSerde<T> implements Serde<T> {
    private final Class<T> tClass;

    public JsonPOJOSerde(Class<T> cls) {
        this.tClass = cls;
    }

    @Override
    public Serializer<T> serializer() {
        final Serializer<T> s = new JsonPOJOSerializer<>();
        return s;
    }

    @Override
    public Deserializer<T> deserializer() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Deserializer<T> d = new JsonPOJODeserializer<>();
        serdeProps.put("JsonPOJOClass", tClass);
        d.configure(serdeProps, false);
        return d;
    }
}
