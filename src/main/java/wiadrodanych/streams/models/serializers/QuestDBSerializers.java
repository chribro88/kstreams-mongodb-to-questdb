package wiadrodanych.streams.models.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.BooleanSerializer;
import com.fasterxml.jackson.databind.ser.std.NonTypedScalarSerializerBase;

import wiadrodanych.streams.models.serializers.ObjectNodeBooleanStringSerializer;


/**
 * BSON serializers
 * @author James Roper
 * @since 2.0
 */
public class QuestDBSerializers extends SimpleSerializers {
    // private static final long serialVersionUID = -1327629614239143170L;

    /**
     * Default constructor
     */
    public QuestDBSerializers() {
        addSerializer(ObjectNode.class, new ObjectNodeBooleanStringSerializer());
    }
}
