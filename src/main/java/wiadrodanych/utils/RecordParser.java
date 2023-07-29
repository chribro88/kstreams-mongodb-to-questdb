package wiadrodanych.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JsonPointer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordParser {
    public static final String DOCUMENT_BEFORE_FIELD = "fullDocumentBeforeChange";
    public static final String DOCUMENT_AFTER_FIELD = "fullDocument";
    public static final String DOCUMENT_UPDATE_FIELD = "updateDescription";
    public static final JsonPointer DOCUMENT_BEFORE_POINTER = JsonPointer.compile("/"+DOCUMENT_BEFORE_FIELD);
    public static final JsonPointer DOCUMENT_AFTER_POINTER = JsonPointer.compile("/"+DOCUMENT_AFTER_FIELD);
    public static final JsonPointer DOCUMENT_UPDATE_POINTER = JsonPointer.compile("/"+DOCUMENT_UPDATE_FIELD);
    public static final JsonPointer UPDATED_FIELDS_POINTER = JsonPointer.compile("/"+DOCUMENT_UPDATE_FIELD + "/updatedFields");
    public static final String DOCUMENT_KEY = "networkId";
    public static final String DOCUMENT_TIMESTAMP = "_accessedDate";
    
    private JsonNodeFactory nodeFactory;

    public RecordParser() {
        // Initialize the JsonNodeFactory when the class is instantiated.
        this.nodeFactory = JsonNodeFactory.instance;
    }

    public List<JsonNode> createIndividualRecords(JsonNode value) {
        // initialise records
        List<JsonNode> individualRecords = new ArrayList<>();

        //get value payload
        ObjectNode payload = (ObjectNode) (
            value.at("/schema").isMissingNode() 
            ? value 
            : value.get("payload")
            );
         
        // prepare the payload
        ObjectNode documents = removeAndFetchDocumentStates(payload);
        JsonNode updatedFields = documents.get("updatedFields");
        JsonNode documentBefore = documents.get(DOCUMENT_BEFORE_FIELD);
        // we are only doing this once 
        // but could do for every node to easily see when fields in certain nodes are updated by adding the node path
        // this will give us last accessed date even if no updates
        individualRecords.add(createChangeEventPayload(payload)); 
        
        if (updatedFields.isEmpty()) {
            return individualRecords;
        }

        // get records from nested json
        individualRecords.addAll(objectToRecords(payload, updatedFields, documentBefore, 2));
    
        return individualRecords;
    }

    private List<JsonNode> objectToRecords(ObjectNode payload, JsonNode updatedFields, JsonNode documentBefore, int level) {
        // https://crosscuttingconcerns.com/JSON-Data-Modeling-RDBMS-Users
        
        // initialise the records
        List<JsonNode> individualRecords = new ArrayList<>(); 
        ObjectNode recordFields = nodeFactory.objectNode();

        // json depth
        AtomicInteger level_ = new AtomicInteger(level); 
    
        // initialise the number of traversable child nodes
        AtomicInteger numChildNodes = new AtomicInteger(0);
        AtomicInteger numRecordFields = new AtomicInteger(0);
    
        // get and set document ids
        JsonNode documentKey  = payload.at("/documentKey/_id");
        JsonNode documentTimestamp  = payload.at("/documentTimestamp");

        recordFields.setAll((ObjectNode) documentKey);
        recordFields.setAll((ObjectNode) documentTimestamp);

        // variant 2
        if (documentBefore == null) {
          documentBefore = updatedFields;
        }
    
        // loop through fields
        Iterator<Map.Entry<String, JsonNode>> fields = documentBefore.fields();
    
        // loop thru fields
        fields.forEachRemaining((Map.Entry<String, JsonNode> entry) -> {
          // field name
          String fieldName = entry.getKey();
    
          // field node
          JsonNode fieldNodeBefore = entry.getValue();
          JsonNode fieldNode = updatedFields.path(fieldName);
          
          // node type
          String fieldNodeBeforeType = fieldNodeBefore.getNodeType().toString();
          String fieldNodeType = fieldNode.getNodeType().toString();
    
          switch (fieldNodeType) {
            // Assumption: add previous (valid) value for this table only.
            // if object or array missing (i.e. unchanged) then previous record will still be valid in qbe
            case "MISSING": {
              if (fieldNodeBeforeType != "OBJECT" && fieldNodeBeforeType != "ARRAY") {
                recordFields.set(fieldName, fieldNodeBefore);
              }
              
              break;
            }
            // Assumption: Relationship is 1-to-1 
            case "OBJECT": 
            //  Assumption: Relationship is 1-to-many
            case "ARRAY": {
              // increment traversable count
              numChildNodes.incrementAndGet();
    
              // create new payload for field node
              ObjectNode childPayload = getFieldPayload(payload, fieldName);

              List<JsonNode> recordsFromFieldList;
              if (fieldNodeType == "OBJECT") {
                
                // get records from nested json
                recordsFromFieldList = objectToRecords(childPayload, fieldNode, fieldNodeBefore, level_.get()+1);
              } 
              else  {
                // get records from nested array
                // dont send fieldNodeBeforeChange (if element changed entire array should be there, plus previous index not always same)
                recordsFromFieldList = arrayToRecords(fieldName, fieldNode, childPayload, level);
              }
              
              individualRecords.addAll(recordsFromFieldList);
              
              break;
            }
            default: {
              // increment traversable count
              numRecordFields.incrementAndGet();
              // JsonNode fieldNodeBeforeChange = newPayloadDocument.get(fieldName);
              recordFields.set(fieldName, fieldNode);
            }
          } 
        });
    
        // recordFields.setAll((ObjectNode) documentKey);
        // recordFields.setAll((ObjectNode) documentTimestamp);
    
        payload.set("document", recordFields);

        if (numRecordFields.get() > 0){
          individualRecords.add(payload);
        }
        
        return individualRecords;
      }
    

      private List<JsonNode> arrayToRecords(String arrayName, JsonNode arrayNode, ObjectNode arrayPayload, int level) {
        // todo.. accessedDate no passed to object node. need to check how payloadDocument initialised
        
        // initialise list of records values
        List<JsonNode> individualRecords = new ArrayList<JsonNode>();
    
        // if array is empty need to return null for questdb fields
        if (arrayNode.isMissingNode() || arrayNode.size() == 0) {
            // todo: modularize
            // create new payload for field node with index -1
            // any columns in table will be null
            ObjectNode elementPayload = getFieldPayload(arrayPayload, "-1");   
            ObjectNode recordFields = nodeFactory.objectNode();
        
            // set the required fields for questdb
            recordFields.setAll((ObjectNode) arrayPayload.at("/documentKey/_id"));
            recordFields.setAll((ObjectNode) arrayPayload.at("/documentTimestamp"));
            recordFields.put("__index", -1); // set to -1 as not sure if null is a good fk / join on
            
            elementPayload.set("document", recordFields);
            individualRecords.add(elementPayload);
            return individualRecords;
        }
        
        //alternative but no index?
        AtomicInteger index = new AtomicInteger(0);
    
        arrayNode.elements().forEachRemaining((elementNode) -> {
            // todo: modularize
            // create new payload for field node with index
            ObjectNode elementPayload = getFieldPayload(arrayPayload, index.toString());
            ObjectNode recordFields = nodeFactory.objectNode();
        
            // set the required fields for questdb
            recordFields.setAll((ObjectNode) arrayPayload.at("/documentKey/_id"));
            recordFields.setAll((ObjectNode) arrayPayload.at("/documentTimestamp"));
            recordFields.put("__index", index.get());
            
            // node type of the element
            String elementNodeType = elementNode.getNodeType().toString();
    
            switch (elementNodeType) {
                // [{foo:"bar"}, {foo:"baz"}]
                case "OBJECT": {
                    // we need an index in both the array table and element table, basically like a composite key (ts, id, index)
                    ((ObjectNode) elementNode).put("__index", index.get());
        
                    // get records from nested json
                    List<JsonNode> newFieldPayloadList = objectToRecords(elementPayload, ((ObjectNode) elementNode), null, level +1);
                    individualRecords.addAll(newFieldPayloadList);
        
                    break;
                }
                // [["foo","bar"], ["baz", "quux"]]
                case "ARRAY": {
                    // todo: .. 
                    break;
                }
                // ["foo", "bar", "baz", "quux"]
                default: {
                    recordFields.set(arrayName, elementNode);
                    // add to payload and record list
                    elementPayload.set("document", recordFields);
                    individualRecords.add(elementPayload);    
                }
            };

            index.incrementAndGet();
        });
    
        return individualRecords;
    }
    

    public JsonNode createChangeEventPayload(JsonNode valuePayload)  {
        ObjectNode changeEventPayload = nodeFactory.objectNode();
        ObjectNode changeEventDocument = nodeFactory.objectNode();

        changeEventPayload.put("topic", "etl.mongodb.changeEvents");
        
        changeEventDocument.setAll(Map.of(
            "documentTimestamp", valuePayload.at("/documentTimestamp/"+DOCUMENT_TIMESTAMP),
            "documentKey", valuePayload.at("/documentKey/_id/"+DOCUMENT_KEY),
            "operationType", valuePayload.at("/operationType"),
            "wallTime", valuePayload.at("/wallTime"),
            "clusterTime", valuePayload.at("/clusterTime")
            ));
        changeEventDocument.setAll((ObjectNode) valuePayload.at("/ns"));
        changeEventPayload.set("document", changeEventDocument);  
        
        return changeEventPayload;
    }

    
    private static ObjectNode getFieldPayload(JsonNode payloadNode, String fieldName) {
        ObjectNode newFieldPayload = payloadNode.deepCopy();
        
          // initialise json node and path
        // ObjectNode jsonNode = JsonNodeFactory.instance.objectNode();
        ObjectNode jsonNode  = (ObjectNode) newFieldPayload.get("documentJson");
        jsonNode.put("node", fieldName);
        jsonNode.put("path", jsonNode.get("path").textValue() + "/" + fieldName);
        newFieldPayload.set("documentJson", jsonNode);
    
        return newFieldPayload;
    }
    

    private void addPayloadTimestamp(ObjectNode valuePayload, JsonNode payloadDocument) {
        ObjectNode documentTimestamp = nodeFactory.objectNode();
        documentTimestamp.set(DOCUMENT_TIMESTAMP, payloadDocument.path(DOCUMENT_TIMESTAMP));
        valuePayload.set("documentTimestamp", documentTimestamp);
    }


    private void addPayloadJsonPath(ObjectNode  valuePayload) {
        // initialise json node and path
        ObjectNode documentJson = nodeFactory.objectNode();
        documentJson.put("node", "root");
        documentJson.put("path", "");
        valuePayload.set("documentJson", documentJson);
    }


    private ObjectNode removeAndFetchDocumentStates(ObjectNode payload)  {
        ObjectNode documentBeforeAndUpdated = nodeFactory.objectNode();
        JsonNode document = nodeFactory.objectNode();
        
        JsonNode documentBefore = payload.remove(DOCUMENT_BEFORE_FIELD);
        JsonNode documentAfter = payload.remove(DOCUMENT_AFTER_FIELD);
        JsonNode updateDescription = payload.remove(DOCUMENT_UPDATE_FIELD);
        
        JsonNode operationType = payload.get("operationType");
        
        // get the document
        switch (operationType.asText()) {
          case "update": {
            document = updateDescription.get("updatedFields");
            break;
          }
          case "insert": {
            document =  documentAfter;
            break;
          }
          default: {}
        };
        
        addPayloadTimestamp(payload, document);
        addPayloadJsonPath(payload);
        removeDocumentIDs((ObjectNode) document);
        removeDocumentIDs((ObjectNode) documentBefore);

        documentBeforeAndUpdated.setAll(Map.of(
            DOCUMENT_BEFORE_FIELD, documentBefore,
            "updatedFields", document
        ));

        return documentBeforeAndUpdated;
    }


    public static void removeDocumentIDs(ObjectNode document)  {
        // remove id and timestamp from document from document
        document.remove("_id");
        document.remove(DOCUMENT_KEY);
        document.remove(DOCUMENT_TIMESTAMP);
    }
}