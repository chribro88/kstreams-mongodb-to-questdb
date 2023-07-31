package wiadrodanych.utils;

import java.io.IOException;

import org.apache.kafka.common.errors.SerializationException;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BsonToBinary {
    private static final Codec<Document> DOCUMENT_CODEC = new DocumentCodec();
    
    public static byte[] toBytes(Document document) {
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        DOCUMENT_CODEC.encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());

        byte[] bytes = outputBuffer.toByteArray();
        outputBuffer.close();

        return bytes;
    }

    public static Object fromBytes(byte[] bytes, Class<?> cls) {
        ObjectMapper mapper = new ObjectMapper();
        Object data;
        try {
            data = mapper.readValue(bytes, cls);
        }
        catch (Exception e) {
            throw new SerializationException(e);
        }

        return data;
    }
}
