package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KafkaConfiguration;
import br.com.multivisao.producerKinesis.dtos.ClientDTO;
import br.com.multivisao.producerKinesis.models.Client;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Service
public class KafkaProducerService {

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Value("${kafka.topics.order}")
    private String topic;

    public void produceMessage(ClientDTO clientDto) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String,String>(kafkaConfiguration.producerConfig());
        var client = toDomain(clientDto);
        var record = new ProducerRecord<>(topic, client.getId(), client.toJson());
        Callback callback = (data, e) -> {
            if (e!=null){
                e.printStackTrace();
                return;
            }
            System.out.println("Mensagem enviada com sucesso para o " +
                    "tópico: " + data.topic() +
                    ":::partition " + data.partition() +
                    "/ offset: " + data.offset() +
                    "timestamp" + data.timestamp());
        };
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", client.getEmail(), client.toJson());
        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
    }

    private Client toDomain(ClientDTO clientDTO){
        Client client = new Client();
        client.setName(clientDTO.getName());
        client.setAge(clientDTO.getAge());
        client.setId(UUID.randomUUID().toString());
        client.setEmail(clientDTO.getEmail());
        return client;
    }
}
