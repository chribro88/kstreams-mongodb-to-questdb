package wiadrodanych.streams.models.serializers;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class ObjectNodeBooleanStringSerializer extends JsonSerializer<ObjectNode> {  
    public static Map<String, JsonNode> kids;        
    
    public ObjectNodeBooleanStringSerializer() {
        super();
    }

    // override serialize() method of JsonSerializer  
    @Override  
    public void serialize(ObjectNode obj, JsonGenerator g, SerializerProvider provider)  
    throws IOException 
    {
        kids = new ObjectMapper().convertValue(obj, new TypeReference<Map<String, JsonNode>>(){});
        if (provider != null) {
            boolean trimEmptyArray = !provider.isEnabled(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS);
            boolean skipNulls = !provider.isEnabled(JsonNodeFeature.WRITE_NULL_PROPERTIES);
            if (trimEmptyArray || skipNulls) {
                g.writeStartObject(obj);
                serializeFilteredContents(g, provider, trimEmptyArray, skipNulls);
                g.writeEndObject();
                return;
            }
        }
        g.writeStartObject(obj);
        for (Map.Entry<String, JsonNode> en : kids.entrySet()) {
            JsonNode value = en.getValue();
            g.writeFieldName(en.getKey());
            if (value.isBoolean()) {
                value = JsonNodeFactory.instance.textNode(value.asText());
            }
            value.serialize(g, provider);
        }
        g.writeEndObject();
    }

    // cant call protected serializeFilteredContents() of ObjectNode
    protected void serializeFilteredContents(final JsonGenerator g, final SerializerProvider provider,
        final boolean trimEmptyArray, final boolean skipNulls)
    throws IOException
    {
        for (Map.Entry<String, JsonNode> en : kids.entrySet()) {
            // 17-Feb-2009, tatu: Can we trust that all nodes will always
            //   extend BaseJsonNode? Or if not, at least implement
            //   JsonSerializable? Let's start with former, change if
            //   we must.
            BaseJsonNode value = (BaseJsonNode) en.getValue();

            // as per [databind#867], see if WRITE_EMPTY_JSON_ARRAYS feature is disabled,
            // if the feature is disabled, then should not write an empty array
            // to the output, so continue to the next element in the iteration
            if (trimEmptyArray && value.isArray() && value.isEmpty(provider)) {
            continue;
            }
            if (skipNulls && value.isNull()) {
                continue;
            }
            g.writeFieldName(en.getKey());
            value.serialize(g, provider);
        }
    }   
}  