package wiadrodanych.streams.models.modules;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;

import de.undercouch.bson4jackson.deserializers.BsonDeserializers;
import wiadrodanych.streams.models.serializers.QuestDBSerializers;

/**
 * Module that configures Jackson to be able to correctly handle all BSON types
 * plus Boolean to Symbol for QuestDB
 * @author James Roper
 * @since 1.3
 */
public class MongoToQuestModule extends Module {
    @Override
    public String getModuleName() {
        return "MongoToQuestModule";
    }

    @Override
    public Version version() {
        return new Version(1, 0, 0, "", null, null);
    }

    @Override
    public void setupModule(SetupContext context) {
        context.addSerializers(new QuestDBSerializers());
        context.addDeserializers(new BsonDeserializers());
    }
}
