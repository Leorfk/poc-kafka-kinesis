package br.com.multivisao.producerKinesis.consumers;

import br.com.multivisao.producerKinesis.services.kafka.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class FraudDetectorService {

    public static void main(String[] args) throws IOException {
        var fraudService = new FraudDetectorService();
        try(var service = new KafkaConsumerService(
                FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse)){
            service.run();
        }
    }

    public void parse(ConsumerRecord<String, String> record){
        System.out.println("------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }
}
