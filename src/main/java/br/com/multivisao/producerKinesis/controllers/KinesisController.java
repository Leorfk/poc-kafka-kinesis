package br.com.multivisao.producerKinesis.controllers;

import br.com.multivisao.producerKinesis.services.KinesisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;

@RestController
@RequestMapping("kinesis")
public class KinesisController {

    @Autowired
    private KinesisService kinesisService;

    @PostMapping("produce")
    public void producer(@RequestParam String id, @RequestBody String dado){
            kinesisService.putRecords(dado, id);
    }
}
