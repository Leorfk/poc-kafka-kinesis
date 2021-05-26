package br.com.multivisao.producerKinesis.controllers;

import br.com.multivisao.producerKinesis.configs.KinesisConfiguration;
import br.com.multivisao.producerKinesis.services.KafkaService;
import br.com.multivisao.producerKinesis.services.KinesisService;
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
    private KinesisConfiguration kinesisConfiguration;

    @Autowired
    private KinesisService kinesisService;

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

    @PostMapping("funciona")
    public void kinesisVai(@RequestParam String id, @RequestBody String dado){
        try {
            kinesisService.putRecords(dado, id, "xap", kinesisConfiguration.createProducer());
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
