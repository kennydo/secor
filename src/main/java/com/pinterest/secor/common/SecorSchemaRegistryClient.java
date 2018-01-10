package com.pinterest.secor.common;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class SecorSchemaRegistryClient {

    private static final Logger LOG = LoggerFactory.getLogger(SecorSchemaRegistryClient.class);

    private KafkaAvroDecoder decoder;
    private final static Map<String, Schema> schemas = new ConcurrentHashMap<>();

    public SecorSchemaRegistryClient(SecorConfig config) {
        try {
            Properties props = new Properties();
            props.put("schema.registry.url", config.getSchemaRegistryUrl());
            CachedSchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrl(), 30);
            decoder = new KafkaAvroDecoder(schemaRegistryClient);
        } catch (Exception e){
            LOG.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }
    }

    public GenericRecord decodeMessage(String topic, byte[] message) {
        GenericRecord record = (GenericRecord) decoder.fromBytes(message);
        Schema schema = record.getSchema();
        schemas.putIfAbsent(topic, schema);
        return record;
    }

    public Schema getSchema(String topic) {
        Schema schema = schemas.get(topic);
        if (schema == null) {
            throw new IllegalStateException("Avro schema not found for topic " + topic);
        }
        return schema;
    }
}
