package wiadrodanych.streams.models;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module.SetupContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BaseJsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.databind.Module;

import wiadrodanych.streams.models.deserializers.QdbNodeDeserializer;
import wiadrodanych.streams.models.deserializers.QuestDBDeserializers;
import wiadrodanych.streams.models.serializers.QuestDBSerializers;

// import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Map;

public class test {
    private static JsonNodeFactory nodeFactory;
    public static void main(String[] args) throws Exception {
        readAndWrite();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonFactory jsonFactory = new JsonFactory();
        StringWriter stringWriter = new StringWriter();
        JsonGenerator jsonGenerator = jsonFactory.createGenerator(stringWriter);
        jsonGenerator.enable(JsonGenerator.Feature.WRITE_NUMBERS_AS_STRINGS);

        SimpleModule simpleModule = new SimpleModule("BooleanModule");
        simpleModule.addSerializer(BooleanNode.class, new BooleanSerializer());
        simpleModule.addSerializer(Boolean.class, new BooleanSerializer2());
        simpleModule.addSerializer(boolean.class, new BooleanSerializer2());
        objectMapper.registerModule(simpleModule);

        objectMapper.configOverride(BooleanNode.class).setFormat(JsonFormat.Value.forShape(JsonFormat.Shape.STRING));
         objectMapper.configOverride(Boolean.class).setFormat(JsonFormat.Value.forShape(JsonFormat.Shape.STRING));
        objectMapper.configOverride(boolean.class).setFormat(JsonFormat.Value.forShape(JsonFormat.Shape.STRING));

        String jsonStr = "{\"a\":true, \"b\":1}";
        JsonNode employee = new ObjectMapper().readValue(jsonStr, JsonNode.class);
        objectMapper.writeValue(jsonGenerator, employee);
        System.out.println(stringWriter);
        JsonParser jsonParser = jsonFactory.createParser(stringWriter.toString());
        JsonNode value = objectMapper.readValue(jsonParser, JsonNode.class);
        System.out.print(value);
    }

    public static class BooleanSerializer extends JsonSerializer<BooleanNode> {
        @Override
        public void serialize(BooleanNode value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException, JsonGenerationException {
            if (value.asBoolean()) {
                jgen.writeString("true");
            } else {
                jgen.writeString("false");
            }
        }
    }

    public static class BooleanSerializer2 extends JsonSerializer<Boolean> {
        @Override
        public void serialize(Boolean value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException, JsonGenerationException {
            if (value) {
                jgen.writeString("true");
            } else {
                jgen.writeString("false");
            }
        }
    }

    // create custom serializer NumericStringSerializer by using JsonSerializer  
    public static class NumericStringSerializer extends JsonSerializer<ObjectNode> {  
            public NumericStringSerializer() {
                super();
            }
            // override serialize() method of JsonSerializer  
            @Override  
            public void serialize(ObjectNode obj, JsonGenerator g, SerializerProvider provider)  
            throws IOException 
            {
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
                Map<String, JsonNode> kids = new ObjectMapper().convertValue(obj, new TypeReference<Map<String, JsonNode>>(){});
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

            protected void serializeFilteredContents(final JsonGenerator g, final SerializerProvider provider,
                final boolean trimEmptyArray, final boolean skipNulls)
            throws IOException
            {
            for (Map.Entry<String, JsonNode> en : _children.entrySet()) {
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

    public static class SimplePojo {

        @JsonProperty
        @JsonSerialize(using=NumericBooleanSerializer.class)
        @JsonDeserialize(using=NumericBooleanDeserializer.class)
        Boolean bool;
    }

    public static class NumericBooleanSerializer extends JsonSerializer<Boolean> {

        @Override
        public void serialize(Boolean bool, JsonGenerator generator, SerializerProvider provider) throws IOException, JsonProcessingException {
            generator.writeString(bool ? "1" : "0");
        }   
    }

    public static class NumericBooleanDeserializer extends JsonNodeDeserializer {
        public NumericBooleanDeserializer() {
            super();
        }
        @Override
        public JsonNode deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            final CustomContainerStack stack = new CustomContainerStack();
            final JsonNodeFactory nodeF = ctxt.getNodeFactory();
            switch (p.currentTokenId()) {
            case JsonTokenId.ID_START_OBJECT:
                return _customDeserializeContainerNoRecursion(p, ctxt, nodeF, stack, nodeF.objectNode());
            case JsonTokenId.ID_END_OBJECT:
                return nodeF.objectNode();
            // case JsonTokenId.ID_START_ARRAY:
            //     return super._deserializeContainerNoRecursion(p, ctxt, nodeF, stack, nodeF.arrayNode());
            // case JsonTokenId.ID_FIELD_NAME:
            //     return super._deserializeObjectAtName(p, ctxt, nodeF, stack);
            default:
            }
            return super._deserializeAnyScalar(p, ctxt);
        }  
        // @Override


        // @Override
        protected final ContainerNode<?> _customDeserializeContainerNoRecursion(JsonParser p, DeserializationContext ctxt,
            JsonNodeFactory nodeFactory, CustomContainerStack stack, final ContainerNode<?> root)
            throws IOException
        {
            ContainerNode<?> curr = root;
            final int intCoercionFeats = ctxt.getDeserializationFeatures() & F_MASK_INT_COERCIONS;

            outer_loop:
            do {
                if (curr instanceof ObjectNode) {
                    ObjectNode currObject = (ObjectNode) curr;
                    String propName = p.nextFieldName();

                    objectLoop:
                    for (; propName != null; propName = p.nextFieldName()) {
                        JsonNode value;
                        JsonToken t = p.nextToken();
                        if (t == null) { // unexpected end-of-input (or bad buffering?)
                            t = JsonToken.NOT_AVAILABLE; // to trigger an exception
                        }
                        switch (t.id()) {
                        case JsonTokenId.ID_START_OBJECT:
                            {
                                ObjectNode newOb = nodeFactory.objectNode();
                                JsonNode old = currObject.replace(propName, newOb);
                                if (old != null) {
                                    _handleDuplicateField(p, ctxt, nodeFactory,
                                            propName, currObject, old, newOb);
                                }
                                stack.push(curr);
                                curr = currObject = newOb;
                                // We can actually take a short-cut with nested Objects...
                                continue objectLoop;
                            }
                        case JsonTokenId.ID_START_ARRAY:
                            {
                                ArrayNode newOb = nodeFactory.arrayNode();
                                JsonNode old = currObject.replace(propName, newOb);
                                if (old != null) {
                                    _handleDuplicateField(p, ctxt, nodeFactory,
                                            propName, currObject, old, newOb);
                                }
                                stack.push(curr);
                                curr = newOb;
                            }
                            continue outer_loop;
                        case JsonTokenId.ID_STRING:
                            value = nodeFactory.textNode(p.getText());
                            break;
                        case JsonTokenId.ID_NUMBER_INT:
                            value = _fromInt(p, intCoercionFeats, nodeFactory);
                            break;
                        case JsonTokenId.ID_NUMBER_FLOAT:
                            value = _fromFloat(p, ctxt, nodeFactory);
                            break;
                        case JsonTokenId.ID_TRUE:
                            value = nodeFactory.textNode("true");
                            break;
                        case JsonTokenId.ID_FALSE:
                            value = nodeFactory.textNode("false");
                            break;
                        case JsonTokenId.ID_NULL:
                            // 20-Mar-2022, tatu: [databind#3421] Allow skipping `null`s from JSON
                            if (!ctxt.isEnabled(JsonNodeFeature.READ_NULL_PROPERTIES)) {
                                continue;
                            }
                            value = nodeFactory.nullNode();
                            break;
                        default:
                            value = _deserializeRareScalar(p, ctxt);
                        }
                        JsonNode old = currObject.replace(propName, value);
                        if (old != null) {
                            _handleDuplicateField(p, ctxt, nodeFactory,
                                    propName, currObject, old, value);
                        }
                    }
                    // reached not-property-name, should be END_OBJECT (verify?)
                } else {
                    // Otherwise we must have an array
                    final ArrayNode currArray = (ArrayNode) curr;

                    arrayLoop:
                    while (true) {
                        JsonToken t = p.nextToken();
                        if (t == null) { // unexpected end-of-input (or bad buffering?)
                            t = JsonToken.NOT_AVAILABLE; // to trigger an exception
                        }
                        switch (t.id()) {
                        case JsonTokenId.ID_START_OBJECT:
                            stack.push(curr);
                            curr = nodeFactory.objectNode();
                            currArray.add(curr);
                            continue outer_loop;
                        case JsonTokenId.ID_START_ARRAY:
                            stack.push(curr);
                            curr = nodeFactory.arrayNode();
                            currArray.add(curr);
                            continue outer_loop;
                        case JsonTokenId.ID_END_ARRAY:
                            break arrayLoop;
                        case JsonTokenId.ID_STRING:
                            currArray.add(nodeFactory.textNode(p.getText()));
                            continue arrayLoop;
                        case JsonTokenId.ID_NUMBER_INT:
                            currArray.add(_fromInt(p, intCoercionFeats, nodeFactory));
                            continue arrayLoop;
                        case JsonTokenId.ID_NUMBER_FLOAT:
                            currArray.add(_fromFloat(p, ctxt, nodeFactory));
                            continue arrayLoop;
                        case JsonTokenId.ID_TRUE:
                            currArray.add(nodeFactory.booleanNode(true));
                            continue arrayLoop;
                        case JsonTokenId.ID_FALSE:
                            currArray.add(nodeFactory.booleanNode(false));
                            continue arrayLoop;
                        case JsonTokenId.ID_NULL:
                            currArray.add(nodeFactory.nullNode());
                            continue arrayLoop;
                        default:
                            currArray.add(_deserializeRareScalar(p, ctxt));
                            continue arrayLoop;
                        }
                    }
                    // Reached end of array (or input), so...
                }

                // Either way, Object or Array ended, return up nesting level:
                curr = stack.popOrNull();
            } while (curr != null);
            return root;
        }

        @SuppressWarnings("rawtypes")
        final static class CustomContainerStack
        {
            private ContainerNode[] _stack;
            private int _top, _end;

            public CustomContainerStack() { }

            // Not used yet but useful for limits (fail at [some high depth])
            public int size() { return _top; }

            public void push(ContainerNode node)
            {
                if (_top < _end) {
                    _stack[_top++] = node; // lgtm [java/dereferenced-value-may-be-null]
                    return;
                }
                if (_stack == null) {
                    _end = 10;
                    _stack = new ContainerNode[_end];
                } else {
                    // grow by 50%, for most part
                    _end += Math.min(4000, Math.max(20, _end>>1));
                    _stack = Arrays.copyOf(_stack, _end);
                }
                _stack[_top++] = node;
            }

            public ContainerNode popOrNull() {
                if (_top == 0) {
                    return null;
                }
                // note: could clean up stack but due to usage pattern, should not make
                // any difference -- all nodes joined during and after construction and
                // after construction the whole stack is discarded
                return _stack[--_top];
            }
        }
    }

    public static void readAndWrite() throws JsonParseException, JsonMappingException, IOException {
        // JsonMapper mapper = JsonMapper.builder()
        // .nodeFactory(new JsonNodeFactory(true) {
        //     @Override
        //     public BooleanNode booleanNode(boolean v) {
        //         return v ? BooleanNode.getTrue() : BooleanNode.getFalse();
        //     }
        // })
        // .build();
        
        
        ObjectMapper mapper = new ObjectMapper();
        final SimpleModule sm = new CusModule();
        mapper.registerModule(sm);
        // read it
        JsonNode sp = mapper.readValue("{\"foo\":true, \"bar\":false, \"baz\":[1,2,3,4], \"qux\":{\"quux\":\"corge\"},\"grault\":[{\"quux\":\"corge\"},{\"quux\":\"garply\"}]}", JsonNode.class);
        // assertThat(sp.bool, is(false));

        // write it
        byte[] bytes = mapper.writeValueAsBytes(sp);
        new String(bytes);
        // assertThat(writer.toString(), is("{\"bool\":\"0\"}"));
    }

    public static class CusDeser extends SimpleDeserializers {
        public CusDeser() {
            addDeserializer(JsonNode.class, new NumericBooleanDeserializer() );
        }
    
    }

    public static class CusSer extends SimpleSerializers {
        public CusSer() {
            addSerializer(ObjectNode.class, new NumericStringSerializer() );
        }
    
    }

    public static class CusModule extends SimpleModule {
    @Override
    public String getModuleName() {
        return "CusModule";
    }

    @Override
    public Version version() {
        return new Version(1, 0, 0, "", null, null);
    }

    @Override
    public void setupModule(SetupContext context) {
        context.addSerializers(new CusSer());
        // context.addDeserializers(new CusDeser());
    }

    }
}

