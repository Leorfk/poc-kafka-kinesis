package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.configs.KafkaConfiguration;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaDispatcher {

    @Autowired
    private KafkaConfiguration configuration;
    private KafkaProducer<String, String> producer;

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
         this.producer = new KafkaProducer<>(configuration.producerConfig());
        var record = new ProducerRecord<>(topic, key, value);
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
        producer.send(record, callback).get();
    }
}
