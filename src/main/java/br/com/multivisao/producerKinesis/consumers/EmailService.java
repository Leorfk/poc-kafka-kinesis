package br.com.multivisao.producerKinesis.consumers;

import br.com.multivisao.producerKinesis.services.kafka.KafkaConsumerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class EmailService {

    public static void main(String[] args) throws IOException {
        var emailService = new EmailService();
        try(var kafkaService = new KafkaConsumerService(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse)) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // ignoring
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
