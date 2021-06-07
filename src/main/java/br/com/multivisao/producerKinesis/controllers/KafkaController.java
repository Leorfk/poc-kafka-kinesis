package br.com.multivisao.producerKinesis.controllers;

import br.com.multivisao.producerKinesis.dtos.ClientDTO;
import br.com.multivisao.producerKinesis.services.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("kafka")
public class KafkaController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("produce")
    public ResponseEntity<String> produce(@RequestBody ClientDTO message){
        try {
            for (var i = 0; i < 10; i++)
                kafkaProducerService.produceMessage(message);
        } catch (ExecutionException | InterruptedException e) {
            e.printStackTrace();
        }
        return ResponseEntity.ok().body("Recurso criado");
    }
}
