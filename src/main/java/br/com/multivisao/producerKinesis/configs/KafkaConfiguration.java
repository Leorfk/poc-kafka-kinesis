package br.com.multivisao.producerKinesis.configs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Properties;
@Configuration
public class KafkaConfiguration {

    @Value("${kafka.bootstrap-servers}")
    private String broker;
    @Value("${kafka.producer.key-serializer-class}")
    private String keySerializer;
    @Value("${kafka.producer.value-serializer-class}")
    private String valueSerializer;
    @Value("${kafka.consumer.key-deserializer-class}")
    private String keyDeserializer;
    @Value("${kafka.consumer.value-deserializer-class}")
    private String valueDeserializer;
    @Value("${kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public Properties producerConfig(){
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return properties;
    }

    @Bean
    public Properties consumerConfig(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }
}
