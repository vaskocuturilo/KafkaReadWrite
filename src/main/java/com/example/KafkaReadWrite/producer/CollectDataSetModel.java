package com.example.KafkaReadWrite.producer;

public record CollectDataSetModel(String schemaName, String schemaVersion, String recordKey, String recordValue) {
}
