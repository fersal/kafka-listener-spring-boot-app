package com.fersal.kafka.listener.kafkalistener.config;

import com.fersal.kafka.listener.kafkalistener.model.Transaction;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {
    @Value("${kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka-group-id}")
    private String groupId;

    /**
     * Consumer Config Factory for String key / String value messages
     *
     * @return String key / String value
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> textListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringDefaultKafkaConsumerFactory());
        return factory;
    }

    private ConsumerFactory<String, String> stringDefaultKafkaConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getDefaultKafkaConfig(), new StringDeserializer(), new StringDeserializer());
    }

    /**
     * Consumer Config Factory for Transaction objects that is serialized to JSON.
     * @return String key / Transaction value
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Transaction> jsonListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Transaction> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(jsonDefaultKafkaConsumerFactory());
        return factory;
    }

    private DefaultKafkaConsumerFactory<String, Transaction> jsonDefaultKafkaConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(getDefaultKafkaConfig(), new StringDeserializer(), new JsonDeserializer<>(Transaction.class));
    }

    private Map<String, Object> getDefaultKafkaConfig() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        return properties;
    }
}
