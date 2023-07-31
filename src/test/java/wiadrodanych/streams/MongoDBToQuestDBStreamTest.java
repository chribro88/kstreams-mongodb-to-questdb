package wiadrodanych.streams;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import wiadrodanych.streams.models.serdes.JsonPOJODeserializer;
import wiadrodanych.utils.BsonToBinary;


public class MongoDBToQuestDBStreamTest {
    TopologyTestDriver testDriver;
    Map<String, Object> serdeProps = new HashMap<>();
    
    StringSerializer stringSerializer = new StringSerializer();
    StringDeserializer stringDeserializer = new StringDeserializer();
    ByteArraySerializer byteArraySerializer = new ByteArraySerializer();
    JsonPOJODeserializer<JsonNode> jsonPojoDeserializer = new JsonPOJODeserializer<JsonNode>();
    
    private TestInputTopic<byte[],  byte[]> inputTopic;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopic;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicRoot;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicArrayInt;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicObject;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicArrayObject;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicNestedObject;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicNestedObjectKey;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicNestedObjectAndString;
    private TestOutputTopic<JsonNode,  JsonNode> outputTopicNestedObjectAndStringKey;

    private static final ObjectMapper mapper = new ObjectMapper();

    private Document keyDocument;
    private Document valueDocument;
    private byte[] keyBytes;
    private byte[] valueBytes;

    private MongoDBToQuestDBStream mongoToQbeStream;
    
    private List<JsonNode> output;
    private List<JsonNode> outputRoot;
    private List<JsonNode> outputArrayInt;
    private List<JsonNode> outputObject;
    private List<JsonNode> outputArrayObject;
    private List<JsonNode> outputNestedObject;
    private List<JsonNode> outputNestedObjectKey;
    private List<JsonNode> outputNestedObjectAndString;
    private List<JsonNode> outputNestedObjectAndStringKey;

    
    @Before
    public void prepareTes() {
        prepareTopologyTestDriver();
        prepareTestInputs();
    }
    
    public void prepareTestInputs() {
        String keyRecord = "{\"_id\":{\"data\":\"82635019A0000000012B042C0100296E5A1004AB1154ACACD849A48C61756D70D3B21F463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F69640064635019A078BE67426D7CF4D2000004\"}}";
        String valueRecord = "{\"schema\":null,\"payload\":{\"_id\":{\"data\":\"82635019A0000000012B042C0100296E5A1004AB1154ACACD849A48C61756D70D3B21F463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F69640064635019A078BE67426D7CF4D2000004\"},\"operationType\":\"insert\",\"clusterTime\":{\"$timestamp\":{\"t\":86400,\"i\":1}},\"walltime\":{\"$date\":86400000},\"fullDocument\":{\"_id\":{\"documentId\":\"648fe4fe4328e69b8c2d6566\"},\"documentId\":\"648fe4fe4328e69b8c2d6566\",\"timestamp\":86400000,\"isBoolean\":true,\"notBoolean\":false,\"string\":\"hello world\",\"arrayInt\":[0,1,2,3,4],\"object\":{\"key\":\"value0\"},\"arrayObject\":[{\"key\":\"value1\"},{\"key\":\"value2\"}],\"nestedObject\":{\"key\":{\"nestedKey\":\"value0\"}},\"nestedObjectAndString\":{\"key\":{\"nestedKey\":\"value0\"},\"string\":\"foo\"}},\"ns\":{\"db\":\"test\",\"coll\":\"example\"},\"documentKey\":{\"_id\":{\"documentId\":\"648fe4fe4328e69b8c2d6566\"}}}}";

        keyDocument = Document.parse(keyRecord);
        valueDocument = Document.parse(valueRecord);

        keyBytes = BsonToBinary.toBytes(keyDocument);
        valueBytes = BsonToBinary.toBytes(valueDocument);
        
        inputTopic.pipeInput(keyBytes, valueBytes);
        
        output = outputTopic.readValuesToList();
        outputRoot = outputTopicRoot.readValuesToList();
        outputArrayInt = outputTopicArrayInt.readValuesToList();
        outputObject = outputTopicObject.readValuesToList();
        outputArrayObject = outputTopicArrayObject.readValuesToList();
        outputNestedObject = outputTopicNestedObject.readValuesToList();
        outputNestedObjectKey = outputTopicNestedObjectKey.readValuesToList();
        outputNestedObjectAndString = outputTopicNestedObjectAndString.readValuesToList();
        outputNestedObjectAndStringKey = outputTopicNestedObjectAndStringKey.readValuesToList();
        
    }
    
    public void prepareTopologyTestDriver() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "SerDeJsonStreamTest");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "doesnt-matter:1337");

        serdeProps.put("JsonPOJOClass", JsonNode.class);
        jsonPojoDeserializer.configure(serdeProps, true);

        mongoToQbeStream = new MongoDBToQuestDBStream();
        Topology topology = mongoToQbeStream.createTopology();
        testDriver = new TopologyTestDriver(topology, config);
        inputTopic = testDriver.createInputTopic(mongoToQbeStream.inputTopic, byteArraySerializer, byteArraySerializer);
        outputTopic = testDriver.createOutputTopic("etl.mongodb.changeEvents", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicRoot = testDriver.createOutputTopic("etl.test.example", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicArrayInt = testDriver.createOutputTopic("etl.test.example.arrayInt", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicObject = testDriver.createOutputTopic("etl.test.example.object", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicArrayObject = testDriver.createOutputTopic("etl.test.example.arrayObject", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicNestedObject = testDriver.createOutputTopic("etl.test.example.nestedObject", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicNestedObjectKey = testDriver.createOutputTopic("etl.test.example.nestedObject.key", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicNestedObjectAndString = testDriver.createOutputTopic("etl.test.example.nestedObjectAndString", jsonPojoDeserializer, jsonPojoDeserializer);
        outputTopicNestedObjectAndStringKey = testDriver.createOutputTopic("etl.test.example.nestedObjectAndString.key", jsonPojoDeserializer, jsonPojoDeserializer);
    }

    @After
    public void closeTestDriver() {
        testDriver.close();
    }

    @Test
    public void streamRootDuality() { 
        Document input = getPointer(valueDocument, "/payload/fullDocument", Document.class);
        List<JsonNode> outputRecords = outputRoot;


        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(1, outputRecords.size()); 

        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);        
    }

    @Test
    public void streamArrayIntDuality() {
        ArrayList inputArrayInt = getPointer(valueDocument, "/payload/fullDocument/arrayInt", ArrayList.class);
        Assert.assertEquals(inputArrayInt.size(), outputArrayInt.size());

        for (int i = 0; i < inputArrayInt.size(); i++) {
            JsonNode outputElementArrayInt = outputArrayInt.get(i).at("/document");
            Assert.assertEquals(inputArrayInt.get(i), outputElementArrayInt.at("/arrayInt").asInt());
        }
        
    }

    @Test
    public void streamObjectDuality() {
        Document input = getPointer(valueDocument, "/payload/fullDocument/object", Document.class);
        List<JsonNode> outputRecords = outputObject;

        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(1, outputRecords.size()); 

        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);
    }

    @Test
    public void streamArrayObjectDuality() {
        ArrayList inputArray = getPointer(valueDocument, "/payload/fullDocument/arrayObject", ArrayList.class);
        List<JsonNode> outputRecords = outputArrayObject;

        Assert.assertEquals(inputArray.size(), outputRecords.size());

        for (int i = 0; i < inputArray.size(); i++) {
            
            Document inputElementObject = (Document) inputArray.get(i);
            Document filterInput = filterOuterValues(inputElementObject);

            JsonNode outputElementObject = outputRecords.get(i).at("/document");
            objectDuality(filterInput, outputElementObject);
        }
        
    }

    @Test
    public void streamNestedObjectDuality() {
        Document input = getPointer(valueDocument, "/payload/fullDocument/nestedObject", Document.class);
        List<JsonNode> outputRecords = outputNestedObject;
        
        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(0, outputRecords.size()); 

        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);
    }

    @Test
    public void streamNestedObjectKeyDuality() {
        Document input = getPointer(valueDocument, "/payload/fullDocument/nestedObject/key", Document.class);
        List<JsonNode> outputRecords = outputNestedObjectKey;
        
        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(1, outputRecords.size()); 

        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);
    }

    @Test
    public void streamNestedObjectAndStringDuality() {
        Document input = getPointer(valueDocument, "/payload/fullDocument/nestedObjectAndString", Document.class);
        List<JsonNode> outputRecords = outputNestedObjectAndString;

        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(1, outputRecords.size());

        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);
    }

    @Test
    public void streamNestedObjectAndStringKeyDuality() {
        Document input = getPointer(valueDocument, "/payload/fullDocument/nestedObjectAndString/key", Document.class);
        List<JsonNode> outputRecords = outputNestedObjectAndStringKey;
        
        Document filterInput = filterOuterValues(input);
        
        Assert.assertEquals(1, outputRecords.size());
        
        if (filterInput.isEmpty()) {
            return;
        }

        JsonNode output = outputRecords.get(0).at("/document");
        objectDuality(filterInput, output);
    }

    private void objectDuality(Document input, JsonNode output) {
        for (Map.Entry<String, Object> en : input.entrySet()) {
            String key = en.getKey();
            Object value = en.getValue();

            // check value is same (casting to same class)
            Assert.assertEquals(value, getValue(output.at("/"+key), value.getClass()));
            
            // check class is the same (except boolean now string)
            if (value instanceof Boolean) {
                Assert.assertEquals(String.class, getValue(output.at("/"+key), Object.class).getClass());
            } else {
                Assert.assertEquals(value.getClass(), getValue(output.at("/"+key), Object.class).getClass());
            }
        }
    }
    
    private <T> T getPointer(Document document, String jsonPointer, Class<T> targetType) {
        if (document == null || jsonPointer == null || jsonPointer.isEmpty()) {
            return null;
        }

        if (jsonPointer.startsWith("/")) {
            jsonPointer = jsonPointer.substring(1, jsonPointer.length());
        }
        String[] parts = jsonPointer.split("/");
        Object value = document;

        for (String part : parts) {
            if (!part.isEmpty()) {
                if (value instanceof Document) {
                    value = ((Document) value).get(part);
                } else {
                    return null; // Invalid path, or the value is not a Document
                }
            }
        }

        if (targetType.isInstance(value)) {
            return targetType.cast(value);
        }

        return null;
    }
    
    private Document filterOuterValues(Document document) {
        Document result = new Document();
        
        for (String key : document.keySet()) {
            Object value = document.get(key);
            
            if (!(value instanceof Document) && !(value instanceof ArrayList)) {
                result.put(key, value);
            }
        }

        return result;
    }

    private <T> T getValue(JsonNode jsonNode, Class<T> targetType) {
        return mapper.convertValue(jsonNode, targetType);
    }

}
 