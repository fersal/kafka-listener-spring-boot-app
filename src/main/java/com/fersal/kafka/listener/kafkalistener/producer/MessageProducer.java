package com.fersal.kafka.listener.kafkalistener.producer;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import com.fersal.kafka.listener.kafkalistener.model.TransactionRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.text.DecimalFormat;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Slf4j
@Service
public class MessageProducer implements CommandLineRunner {
    private static final int DEFAULT_MESSAGE_COUNT = 1000;
    private static final int RANDOMIZER_MIN = 100;
    private static final int RANDOMIZER_MAX = 1000;
    private static final Random RANDOMIZER = new Random(RANDOMIZER_MIN);
    private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("0.00");

    @Value("${kafka-topic-json}")
    private String jsonTopic;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TransactionRepository repository;

    public MessageProducer(KafkaTemplate<String, Object> kafkaTemplate, TransactionRepository repository) {
        this.kafkaTemplate = kafkaTemplate;
        this.repository = repository;
    }

    @Override
    public void run(String... args) {
        publishMessages(DEFAULT_MESSAGE_COUNT);
    }

    String publishMessages(final int messageCount) {
        buildMessages(messageCount).forEach(transaction -> kafkaTemplate.send(jsonTopic, transaction));
        return "Published " + DEFAULT_MESSAGE_COUNT + " messages to JSON topic";
    }

    private Stream<Transaction> buildMessages(final int messageCount) {
        AtomicLong lastTransactionID = new AtomicLong(getLastTransactionIdFromDatabase());
        Stream.Builder<Transaction> transactionBuilder = Stream.builder();
        IntStream.range(0, messageCount)
                .forEach(unused ->
                        transactionBuilder.add(
                                Transaction.builder()
                                        .transactionId(lastTransactionID.getAndIncrement())
                                        .currency("USD")
                                        .value(getRandomAmountValue())
                                        .build()
                        )
                );

        return transactionBuilder.build();
    }

    private String getRandomAmountValue() {
        return String.valueOf(
                DECIMAL_FORMATTER.format(
                        RANDOMIZER_MIN + (RANDOMIZER_MAX - RANDOMIZER_MIN) * RANDOMIZER.nextDouble()
                )
        );
    }

    private long getLastTransactionIdFromDatabase() {
        return Optional.ofNullable(repository.findLastTransaction()).map(id -> id + 1L).orElse(1L);
    }
}
