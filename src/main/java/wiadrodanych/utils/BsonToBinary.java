package wiadrodanych.utils;
// https://gist.github.com/Koboo/ebd7c6802101e1a941ef31baca04113d
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;

public class BsonToBinary {

    private static final BsonDocumentCodec DOCUMENT_CODEC = new BsonDocumentCodec();

    public static byte[] toBytes(BsonDocument document) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(buffer);
        new BsonDocumentCodec().encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
        byte[] bytes = buffer.toByteArray();
        buffer.close();
        return bytes;
    }

    public static BsonDocument toDocument(byte[] bytes) {
        BsonBinaryReader reader = new BsonBinaryReader(ByteBuffer.wrap(bytes));
        BsonDocument document = DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
        reader.close();
        return document;
    }
}