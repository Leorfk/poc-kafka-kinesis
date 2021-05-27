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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Service
public class KafkaService {

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Value("${kafka.producer.topic}")
    private String topic;
    @Value("${kafka.consumer.topics.order}")
    String orderTopic;
    @Value("${kafka.consumer.topics.email}")
    String emailTopic;

    public void produceMessage(ClientDTO clientDto) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String,String>(kafkaConfiguration.producerConfig());
        var client = toDomain(clientDto);
        var record = new ProducerRecord<String,String>(topic, client.getId(), client.toJson());
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

    public void consumeMessageOrders(){
        var consumer = new KafkaConsumer<String,String>(kafkaConfiguration.consumerConfig());
        consumer.subscribe(Collections.singleton(orderTopic));
        readMessages(consumer, orderTopic);
    }

    public void consumeMessageEmails(){
        var consumer = new KafkaConsumer<String,String>(kafkaConfiguration.consumerConfig());
        consumer.subscribe(Collections.singleton(emailTopic));
        readMessages(consumer, emailTopic);
    }

    public void consumeMessagelogs(){
        var consumer = new KafkaConsumer<String,String>(kafkaConfiguration.consumerConfig());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        readMessages(consumer, "Logs");
    }

    private Client toDomain(ClientDTO clientDTO){
        Client client = new Client();
        client.setName(clientDTO.getName());
        client.setAge(clientDTO.getAge());
        client.setId(UUID.randomUUID().toString());
        client.setEmail(clientDTO.getEmail());
        return client;
    }

    private void readMessages(KafkaConsumer<String, String> consumer, String topic) {
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()){
                for (var record: records) {
                    System.out.println("=====================================================================");
                    System.out.println("Lendo as mensagens do tópico: " + topic);
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Processado!!!!");
                }
                System.out.println("Não tem nada aqui");
            }
        }
    }
}
