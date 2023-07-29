package wiadrodanych.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import com.fasterxml.jackson.databind.JsonNode;

import wiadrodanych.streams.models.serdes.BsonPOJODeserializer;
import wiadrodanych.streams.models.serdes.JsonPOJOSerializer;
import wiadrodanych.utils.RecordParser;
import wiadrodanych.utils.EnvTools;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MongoDBToQuestDBStream {

    public String inputTopic;
    public String outputTopic;

    public MongoDBToQuestDBStream() {
        inputTopic = EnvTools.getEnvValue(EnvTools.INPUT_TOPIC, "mongodb1.database_name.collection_name");
        outputTopic = EnvTools.getEnvValue(EnvTools.OUTPUT_TOPIC_PREFIX, "etl.database_name.collection_name");
    }

    public static void main(String[] args) throws Exception {
        Properties props = createProperties();
        
        MongoDBToQuestDBStream serDeJsonStream = new MongoDBToQuestDBStream();

        final KafkaStreams streams = new KafkaStreams(serDeJsonStream.createTopology(), props);
        
        StreamsUncaughtExceptionHandler customExceptionHandler = (exception) -> {
            // Handle the exception based on your custom logic.
            // You can log the exception or take appropriate action based on the specific exception.
            System.err.println("oh no! error! " + exception.getMessage());
        
            // You need to return an appropriate StreamThreadExceptionResponse depending on your requirements.
            // For this example, let's return REPLACE_THREAD as the default response.
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        };
        
        streams.setUncaughtExceptionHandler(customExceptionHandler);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }


    public Topology createTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        final RecordParser parser = new RecordParser();
        final Serde<JsonNode> jsonSerde = createJsonSerde();       
    

        final KStream<JsonNode, JsonNode> recordListings = builder.stream(inputTopic, Consumed.with(jsonSerde, jsonSerde));
        final KStream<JsonNode, JsonNode> routeNestedJson = recordListings.flatMapValues((key, value)  -> parser.createIndividualRecords(value));

        routeNestedJson.to((key, value, recordContext) -> {
            JsonNode topic = value.at("/topic");
            if (topic.isMissingNode()) {
              return outputTopic + value.at("/documentJson/path").textValue().replaceAll("\\/\\-?\\d+", "").replace('/', '.');
            }
            return topic.textValue();
            
          }, Produced.with(jsonSerde, jsonSerde));

        return builder.build();
    }

    private static Serde<JsonNode> createJsonSerde() {
        Map<String, Object> serdeProps = new HashMap<>();
        final Deserializer<JsonNode> jsonDeserializer = new BsonPOJODeserializer<>();
        serdeProps.put("BsonPOJOClass", JsonNode.class);
        jsonDeserializer.configure(serdeProps, false);
    
        final Serializer<JsonNode> jsonSerializer = new JsonPOJOSerializer<>();
        serdeProps.put("JsonPOJOClass", JsonNode.class);
        jsonSerializer.configure(serdeProps, false);
    
        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
      }


    private static Properties createProperties() {
        Properties props = new Properties();
        String appIdConfig = EnvTools.getEnvValue(EnvTools.APPLICATION_ID_CONFIG, "kstreams-mongo2qdb-stream"); // wiaderko-ztm-stream
        String clientIdConfig = EnvTools.getEnvValue(EnvTools.CLIENT_ID_CONFIG, "kstreams-mongo2qdb-stream-client1");
        String bootstrapServersConfig =  EnvTools.getEnvValue(EnvTools.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        String autoOffset = EnvTools.getEnvValue(EnvTools.AUTO_OFFSET_RESET, "latest");
        
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appIdConfig);
        
        // assuming auto-generated client id if none
        if (clientIdConfig != null) {
            props.put(StreamsConfig.CLIENT_ID_CONFIG, clientIdConfig);
        }
        
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,autoOffset);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        return props;
        
    }


}
