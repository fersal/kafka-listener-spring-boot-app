package com.fersal.kafka.listener.kafkalistener;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import com.fersal.kafka.listener.kafkalistener.model.TransactionRepository;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;

@Profile("test")
@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka
@TestPropertySource(properties = { "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}" })
public class JsonTopicConsumerTest {
    private static final String TOPIC = "json-topic";
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @MockBean
    private TransactionRepository repository;

    private Producer<String, Transaction> producer;

    @Before
    public void setUp() {
        kafkaListenerEndpointRegistry.getListenerContainers()
                .forEach(messageListener -> ContainerTestUtils.waitForAssignment(messageListener, 1));

        Map<String, Object> producerConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<String, Transaction>(producerConfig, new StringSerializer(), new JsonSerializer<>()).createProducer();
    }

    @Test
    public void messageReceivedIsSavedToDatabase() {
        //arrange
        ProducerRecord<String, Transaction> message = new ProducerRecord<>(TOPIC, buildTestTransaction());
        producer.send(message);

        //assert
        Mockito.verify(repository, Mockito.atLeastOnce()).save(Mockito.any(Transaction.class));
    }

    private Transaction buildTestTransaction() {
        return Transaction.builder()
                .transactionId(12345L)
                .value("45.69")
                .currency("USD")
                .build();
    }
}