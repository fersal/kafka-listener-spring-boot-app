package com.fersal.kafka.listener.kafkalistener;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext
@EmbeddedKafka
//@EmbeddedKafka(topics = {"json-topic-test"},brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
//@EmbeddedKafka(topics = {"json-topic-test"}, partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class MessageProducerTest {
    private static final String TOPIC = "json-topic";
    private static final String CURRENCY = "USD";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private BlockingDeque<ConsumerRecord<String, Transaction>> messages;
    private KafkaMessageListenerContainer<String, Transaction> container;
    private Producer<String, Transaction> producer;

    @Before
    public void setUp() {
        //Consumer setup
        Map<String, Object> consumerConfig = new HashMap<>(KafkaTestUtils.consumerProps("group", "false", embeddedKafkaBroker));
        DefaultKafkaConsumerFactory<String, Transaction> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig, new StringDeserializer(), new JsonDeserializer<>(Transaction.class));
        ContainerProperties containerProperties = new ContainerProperties(TOPIC);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        messages = new LinkedBlockingDeque<>();
        container.setupMessageListener((MessageListener<String, Transaction>) messages::add);
        container.start();
        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());

        //Producer setup
        Map<String, Object> producerConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<String, Transaction>(producerConfig, new StringSerializer(), new JsonSerializer<>()).createProducer();
    }

    @After
    public void teardown() {
        container.stop();
    }

    @Test
    public void oneThousandMessagesSentUponStartup() throws InterruptedException {
        //arrange
        ProducerRecord<String, Transaction> message = new ProducerRecord<>(TOPIC, buildTestTransaction());
        producer.send(message);

        //act
        ConsumerRecord<String, Transaction> result = messages.poll(2, TimeUnit.SECONDS);

        //assert
        Assert.assertTrue(
                Optional.ofNullable(result)
                        .map(ConsumerRecord::value)
                        .map(testTransaction -> {
                            Assert.assertEquals(CURRENCY, testTransaction.getCurrency());
                            Assert.assertTrue(testTransaction.getTransactionId() > 0);
                            Assert.assertFalse(testTransaction.getValue().isEmpty());

                            return true;
                        })
                        .orElseThrow()
        );
    }

    private Transaction buildTestTransaction() {
        return Transaction.builder()
                .transactionId(12345L)
                .value("45.69")
                .currency("USD")
                .build();
    }
}