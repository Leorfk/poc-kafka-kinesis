package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KafkaConfiguration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
public class KafkaService {

    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Value("${kafka.producer.topic}")
    private String topic;
    @Value("${kafka.consumer.topics}")
    List<String> topics;

    public void produceMessage(String message){
        System.out.println(topic);
        var producer = new KafkaProducer<String,String>(kafkaConfiguration.producerConfig());
        var record = new ProducerRecord<String,String>(topic, message, message);
        producer.send(record);
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
}
