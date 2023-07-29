package wiadrodanych.streams.models.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import wiadrodanych.streams.models.modules.MongoToQuestModule;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonPOJOSerializer<T> implements Serializer<T> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJOSerializer() {
    }
    
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        objectMapper.registerModule(new MongoToQuestModule());   
    }

    @Override
    public byte[] serialize(String topic, T data) {
        if (data == null)
            return null;

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

}