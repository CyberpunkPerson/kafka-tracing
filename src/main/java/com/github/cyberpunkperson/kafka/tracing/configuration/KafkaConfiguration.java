package com.github.cyberpunkperson.kafka.tracing.configuration;

import com.github.cyberpunkperson.kafka.tracing.configuration.KafkaProperties.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.UUID.randomUUID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

@Configuration
class KafkaConfiguration {

    @Bean
    @ConfigurationProperties("kafka.sources.dc")
    KafkaProperties dcKafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    ConsumerFactory<String, byte[]> dcConsumerFactory() {
        KafkaProperties properties = dcKafkaProperties();
        KafkaConsumer consumer = properties.getConsumer();
        Map<String, Object> configuration = new HashMap<>(consumer.getProperties());
        configuration.put(BOOTSTRAP_SERVERS_CONFIG, properties.getUrl());
        configuration.put(GROUP_ID_CONFIG, "%s.%s".formatted(consumer.getGroupId(), randomUUID()));
        configuration.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuration.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configuration);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> dcContainer() {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> container = new ConcurrentKafkaListenerContainerFactory<>();
        container.setConsumerFactory(dcConsumerFactory());
        return container;
    }

    @Bean
    @ConfigurationProperties("kafka.sources.marvel")
    KafkaProperties marvelKafkaProperties() {
        return new KafkaProperties();
    }

    @Bean
    ConsumerFactory<String, byte[]> marvelConsumerFactory() {
        KafkaProperties properties = marvelKafkaProperties();
        KafkaConsumer consumer = properties.getConsumer();
        Map<String, Object> configuration = new HashMap<>(consumer.getProperties());
        configuration.put(BOOTSTRAP_SERVERS_CONFIG, properties.getUrl());
        configuration.put(GROUP_ID_CONFIG, "%s.%s".formatted(consumer.getGroupId(), randomUUID()));
        configuration.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configuration.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(configuration);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, byte[]> marvelContainer() {
        ConcurrentKafkaListenerContainerFactory<String, byte[]> container = new ConcurrentKafkaListenerContainerFactory<>();
        container.setConsumerFactory(marvelConsumerFactory());
        return container;
    }

    @Bean
    ProducerFactory<String, byte[]> kafkaDefaultProducerFactory(KafkaProperties kafkaDefaultProperties) {
        Map<String, Object> configuration = new HashMap<>(kafkaDefaultProperties.getProducer());
        configuration.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaDefaultProperties.getUrl());
        configuration.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        configuration.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new DefaultKafkaProducerFactory<>(configuration);
    }

    @Bean
    KafkaTemplate<String, byte[]> kafkaDefaultTemplate(ProducerFactory<String, byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConfigurationProperties("kafka.default")
    KafkaProperties kafkaDefaultProperties() {
        return new KafkaProperties();
    }
}
