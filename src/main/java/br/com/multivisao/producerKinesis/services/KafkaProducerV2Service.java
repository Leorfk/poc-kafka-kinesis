package br.com.multivisao.producerKinesis.services;

import br.com.multivisao.producerKinesis.models.Order;
import br.com.multivisao.producerKinesis.services.kafka.KafkaDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaProducerV2Service {

    @Autowired
    private KafkaDispatcher<Order> orderDispatcher;

    @Autowired
    private KafkaDispatcher<String> emailDispatcher;

    @Value("${kafka.topics.order}")
    private String orderTopic;

    @Value("${kafka.topics.email}")
    private String emailTopic;

    public void produce() throws ExecutionException, InterruptedException {
        var userId = UUID.randomUUID().toString();
        var orderId = UUID.randomUUID().toString();
        var amount = new BigDecimal(Math.random() * 5000 + 1);
        /*TODO: refatorar esse método para que receba valores de maneira dinâmica*/
        var order = new Order(userId, orderId, amount);

        orderDispatcher.send(emailTopic, userId, order);
        var email = "Fez a boa";
        emailDispatcher.send(orderTopic, userId, email);
    }
}
