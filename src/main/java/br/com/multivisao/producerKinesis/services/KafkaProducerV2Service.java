package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.services.kafka.KafkaDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaProducerV2Service {

    @Autowired
    private KafkaDispatcher dispatcher;

    @Value("${kafka.topics.order}")
    private String orderTopic;

    @Value("${kafka.topics.email}")
    private String emailTopic;

    public void produce(){
        var key = UUID.randomUUID().toString();
        var value = "24r358t4erhg7e89fw0,32t74839rw,4y5t3ret";
        try {
            dispatcher.send(emailTopic, key, value);
            dispatcher.send(orderTopic, key, value);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
