package com.fersal.kafka.listener.kafkalistener.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;

@Service
@Slf4j
public class FileTopicConsumer {
    private static final String PAYLOAD = "payload";

    @Value("${kafka-topic-json}")
    private String jsonTopic;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public FileTopicConsumer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(
            topics = "${kafka-topic-text-file}",
            groupId = "${kafka-group-id}",
            containerFactory = "textListenerContainerFactory"
    )
    public void handleTextFileMessage(final String message) {
        parseMessageMap(message)
                .flatMap(this::getMessagePayload)
                .flatMap(this::getTransaction)
                .ifPresent(transaction -> kafkaTemplate.send(jsonTopic, transaction));

    }

    private Optional<Transaction> getTransaction(final String textFileLine) {
        return Optional.of(textFileLine)
                .map(fileLine -> fileLine.split("\\|"))
                .map(this::buildTransaction);
    }

    private Transaction buildTransaction(final String[] fileLine) {
        return Transaction.builder()
                .transactionId(Long.parseLong(fileLine[0]))
                .currency(fileLine[1])
                .value(fileLine[2])
                .build();
    }

    private Optional<String> getMessagePayload(Map<String, String> msgMap) {
        return Optional.ofNullable(msgMap.get(PAYLOAD));
    }

    private Optional<Map<String, String>> parseMessageMap(final String message) {
        try {
            return Optional.of(new ObjectMapper().readValue(message, Map.class));
        } catch (JsonProcessingException e) {
            log.error("Error while mapping Text file message", e);
            return Optional.empty();
        }
    }
}
