package com.fersal.kafka.listener.kafkalistener;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
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
import org.springframework.kafka.core.KafkaTemplate;
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
public class FileTopicConsumerTest {
    private static final String TOPIC = "text-file-topic";
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;
    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    private Producer<String, String> producer;

    @Before
    public void setUp() {
        kafkaListenerEndpointRegistry.getListenerContainers()
                .forEach(messageListener -> ContainerTestUtils.waitForAssignment(messageListener, 1));

        Map<String, Object> producerConfig = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        producer = new DefaultKafkaProducerFactory<>(producerConfig, new StringSerializer(), new StringSerializer()).createProducer();
    }

    @Test
    public void messageReceivedIsSavedToDatabase() {
        //arrange
        ProducerRecord<String, String> message = new ProducerRecord<>(TOPIC, buildFileLineString());
        producer.send(message);

        //assert
        Mockito.verify(kafkaTemplate, Mockito.atLeastOnce()).send(Mockito.any(), Mockito.any(Transaction.class));
    }

    private String buildFileLineString() {
        return "3939393|USD|381";
    }
}