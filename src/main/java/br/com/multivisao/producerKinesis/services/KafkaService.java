package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KafkaConfiguration;
import br.com.multivisao.producerKinesis.dtos.ClientDTO;
import br.com.multivisao.producerKinesis.models.Client;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaService {

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Value("${kafka.producer.topic}")
    private String topic;
    @Value("${kafka.consumer.topics}")
    List<String> topics;

    public void produceMessage(ClientDTO clientDto) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String,String>(kafkaConfiguration.producerConfig());
        var client = toDomain(clientDto);
        var record = new ProducerRecord<String,String>(topic, client.getId(), client.toJson());
        producer.send(record, (data, e) -> {
            if (e!=null){
                e.printStackTrace();
                return;
            }
            System.out.println("Mensagem enviada com sucesso para o " +
                    "tópico: " + data.topic() +
                    ":::partition " + data.partition() +
                    "/ offset: " + data.offset() +
                    "timestamp" + data.timestamp());
        }).get();
    }

    public void consumeMessage(){
        var consumer = new KafkaConsumer<String,String>(kafkaConfiguration.consumerConfig());
        consumer.subscribe(topics);
        while(true){
            var records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty()){
                System.out.println("Não tem nada aqui");
            }
            for (var record: records) {
                System.out.println("=====================================================================================");
                System.out.println("Lendo as mensagens");
                System.out.println(record.key());
                System.out.println(record.value());
                System.out.println(record.partition());
                System.out.println(record.offset());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Processado!!!!");
            }
        }
    }

    private Client toDomain(ClientDTO clientDTO){
        Client client = new Client();
        client.setName(clientDTO.getName());
        client.setAge(clientDTO.getAge());
        client.setId(UUID.randomUUID().toString());
        return client;
    }
}
