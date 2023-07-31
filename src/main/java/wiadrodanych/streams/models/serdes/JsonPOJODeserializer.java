package wiadrodanych.streams.models.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Base64;
import java.util.Map;


public class JsonPOJODeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> tClass;

    /**
     * Default constructor needed by Kafka
     */
    public JsonPOJODeserializer() {
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        tClass = (Class<T>) props.get("JsonPOJOClass");
        // objectMapper.registerModule(new MongoToQuestModule());        
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        T data;
        try {            
            try {
                data = objectMapper.readValue(bytes, tClass);       
            } catch (Exception e) {
                String value = new String(bytes);
            
                // Remove unnecessary quotes of BsonValues converted to Strings.
                // Such as BsonBinary base64 string representations
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                // byte[] rawBytes = new Base64Codec().decode(value);
                byte[] rawBytes = Base64.getDecoder().decode(value);

                data = objectMapper.readValue(rawBytes, tClass);
            }    
        } catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }

    @Override
    public void close() {

    }
}