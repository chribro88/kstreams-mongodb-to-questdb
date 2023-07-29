package wiadrodanych.streams.models.deserializers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;


/**
 * BSON deserializers
 * @author Michel Kraemer
 * @since 2.3.2
 */
public class QuestDBDeserializers extends SimpleDeserializers {
    // private static final long serialVersionUID = 261492073508673840L;

    /**
     * Default constructor
     */
    public QuestDBDeserializers() {
        addDeserializer(JsonNode.class, new JsonNodeBooleanStringDeserializer());
    }
}
