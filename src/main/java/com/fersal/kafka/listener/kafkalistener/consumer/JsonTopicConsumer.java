package com.fersal.kafka.listener.kafkalistener.consumer;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import com.fersal.kafka.listener.kafkalistener.model.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
public class JsonTopicConsumer {
    private final TransactionRepository repository;

    public JsonTopicConsumer(TransactionRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            topics = "${kafka-topic-json}",
            groupId = "${kafka-group-id}",
            containerFactory = "jsonListenerContainerFactory"
    )
    public void jsonListener(final Transaction transaction) {
        Optional.of(transaction)
                .map(repository::save)
                .ifPresent(savedTransaction -> log.info("Handled and committed transaction, id: " + transaction.getTransactionId()));
    }
}
