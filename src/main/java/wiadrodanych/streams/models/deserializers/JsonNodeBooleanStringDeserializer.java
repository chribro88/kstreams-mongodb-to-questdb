package wiadrodanych.streams.models.deserializers;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.cfg.JsonNodeFeature;
import com.fasterxml.jackson.databind.deser.std.JsonNodeDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;


public class JsonNodeBooleanStringDeserializer  extends JsonNodeDeserializer {
        public JsonNodeBooleanStringDeserializer () {
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
