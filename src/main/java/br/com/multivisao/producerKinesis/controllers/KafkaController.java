package br.com.multivisao.producerKinesis.controllers;

import br.com.multivisao.producerKinesis.services.KafkaService;
import br.com.multivisao.producerKinesis.services.KinesisProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;

@RestController
@RequestMapping("/api")
public class KafkaController {

    @Autowired
    private KafkaService kafkaProducerService;

    @Autowired
    private KinesisProducerService kinesisProducerService;

    @PostMapping("/produce")
    public ResponseEntity<String> produce(@RequestBody String message){
        kafkaProducerService.produceMessage(message);
        return ResponseEntity.ok().body("Recurso criado");
    }

    @GetMapping("/consume")
    public ResponseEntity<String> consume(){
        kafkaProducerService.consumeMessage();
        return ResponseEntity.ok().body("mensagens lidas");
    }

    @GetMapping("kinesis")
    public void xama(){
        try {
            kinesisProducerService.produce();
        } catch (UnsupportedEncodingException e) {
            System.out.println(e.getMessage());
        }
    }

    @GetMapping("teste")
    public void mama(){
        kinesisProducerService.mamus();
    }
}
