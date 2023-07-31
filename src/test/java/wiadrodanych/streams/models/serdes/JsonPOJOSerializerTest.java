package wiadrodanych.streams.models.serdes;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import de.undercouch.bson4jackson.types.ObjectId;
import de.undercouch.bson4jackson.types.Timestamp;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class JsonPOJOSerializerTest {    
    
    private static Serializer<JsonNode> getSerializer() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Serializer<JsonNode> jsonSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", JsonNode.class);
        jsonSerializer.configure(serdeProps, true);
        return jsonSerializer;
    }
   
    public JsonNode fromBytes(byte[] bytes) throws StreamReadException, DatabindException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(bytes, JsonNode.class);
    }


    @Test
    public void inputJsonNodeToJsonStringSerializerWorks(){
        Serializer<JsonNode> serializer = getSerializer();
        
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        ObjectNode document = nodeFactory.objectNode();
        ObjectId objectid = new ObjectId(1415909410, 9127271, 12582588, (short) 777);
        Timestamp timestamp = new Timestamp(1415909410, 9127271);
        Date date = new Date(2147483647);
        document.setAll(Map.of(
         "_id", nodeFactory.pojoNode(objectid),
        "Timestamp", nodeFactory.pojoNode(timestamp),
        "Date", nodeFactory.pojoNode(date),
        "isBoolean",nodeFactory.booleanNode(true)
        ));

        byte[] bytes =  serializer.serialize(null, document);

        JsonNode serializedDocument;
        try {
            serializedDocument = fromBytes(bytes);
        }
        catch (Exception e) {
            throw new SerializationException(e);
        }


        Assert.assertEquals(2147483647, serializedDocument.path("Date").asInt());
        
        // boolean is string (todo: confirm nested)
        Assert.assertTrue(serializedDocument.path("isBoolean") instanceof TextNode);
        Assert.assertEquals("true", serializedDocument.path("isBoolean").asText());

    }
}
