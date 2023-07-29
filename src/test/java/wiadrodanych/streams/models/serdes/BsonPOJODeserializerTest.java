package wiadrodanych.streams.models.serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.POJONode;

import de.undercouch.bson4jackson.types.ObjectId;
import de.undercouch.bson4jackson.types.Timestamp;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;


public class BsonPOJODeserializerTest {    

    private static Deserializer<JsonNode> getDeserializer() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Deserializer<JsonNode> jsonDeserializer = new BsonPOJODeserializer<>();
        serdeProps.put("BsonPOJOClass", JsonNode.class);
        jsonDeserializer.configure(serdeProps, true);
        return jsonDeserializer;
    }
    
    private static byte[] toBytes(Document document) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        new DocumentCodec().encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        byte[] bytes = outputBuffer.toByteArray();
        outputBuffer.close();

        return bytes;
    }
    

    @Test
    public void inputBsonStringToJsonNodeDeserializerWorks(){
        Deserializer<JsonNode> deserializer = getDeserializer();

        String rawRecord = "{\"_id\":{\"$oid\": \"54651022bffebc03098b4567\"},\"Date\":{\"$date\":2147483647000},\"Timestamp\":{\"$timestamp\":{\"t\":1690275046, \"i\":1}}, \"isBoolean\": true, \"Lon\": 21.043399, \"VehicleNumber\": \"1042\", \"Time\": \"2020-04-24 21:14:34\", \"Lat\": 52.26617, \"Brigade\": \"04\"}";
        Document document = Document.parse(rawRecord);
        JsonNode record =  deserializer.deserialize(null, toBytes(document));

        // objectid
        POJONode objectIdNode = (POJONode) record.path("_id");
        ObjectId objectId = (ObjectId) objectIdNode.getPojo();
        Assert.assertTrue(objectId instanceof ObjectId);
        Assert.assertEquals(1415909410, objectId.getTimestamp());
        
        // date
        POJONode dateNode = (POJONode) record.path("Date");
        Date date = (Date) dateNode.getPojo();
        Assert.assertTrue(date instanceof Date);
        Assert.assertEquals("Tue Jan 19 03:14:07 GMT 2038", date.toString());

        // timestamp
        POJONode timestampNode = (POJONode) record.path("Timestamp");
        Timestamp timestamp = (Timestamp) timestampNode.getPojo();
        Assert.assertTrue(timestamp instanceof Timestamp);
        Assert.assertEquals(1690275046, timestamp.getTime());
        Assert.assertEquals(1, timestamp.getInc());

        // boolean
        Assert.assertTrue(record.path("isBoolean") instanceof BooleanNode);
        Assert.assertEquals(true, record.path("isBoolean").asBoolean());

        // others todo...
        Assert.assertEquals(21.043399,record.path("Lon").asDouble(),0.0001);
        Assert.assertEquals(52.26617,record.path("Lat").asDouble(),0.0001);
        Assert.assertEquals("04",record.path("Brigade").asText());
        Assert.assertEquals("1042",record.path("VehicleNumber").asText());
    }
}
