package br.com.multivisao.producerKinesis.controllers;

import br.com.multivisao.producerKinesis.configs.KinesisConfiguration;
import br.com.multivisao.producerKinesis.dtos.ClientDTO;
import br.com.multivisao.producerKinesis.models.Client;
import br.com.multivisao.producerKinesis.services.KafkaService;
import br.com.multivisao.producerKinesis.services.KinesisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("kafka")
public class KafkaController {

    @Autowired
    private KafkaService kafkaProducerService;

    @PostMapping("produce")
    public ResponseEntity<String> produce(@RequestBody ClientDTO message){
        try {
            kafkaProducerService.produceMessage(message);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().body("Recurso criado");
    }

    @GetMapping("consume")
    public ResponseEntity<String> consume(){
        kafkaProducerService.consumeMessage();
        return ResponseEntity.ok().body("mensagens lidas");
    }
}
